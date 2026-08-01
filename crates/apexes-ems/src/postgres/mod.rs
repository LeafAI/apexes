//! [`PostgresEms`]: the crash-recoverable `Ems` implementation used
//! ONLY by `apexes-live-trading` (never backtest/paper, which use
//! `crate::in_memory::InMemoryEms` instead -- see this crate's `lib.rs`
//! docs). Paces TWAP/VWAP/Iceberg by real background `tokio::spawn`
//! tasks using `tokio::time::sleep` (unlike `InMemoryEms`'s
//! virtual-clock pacing via `Ems::on_market_event`); POV is paced by
//! `on_market_event`'s `MarketEvent::Trade` observations, same as
//! `InMemoryEms`, since POV is inherently event-driven regardless of
//! virtual vs. real clock.
//!
//! Crash recovery: every parent order and every submitted child order
//! is persisted to PostgreSQL (`parent_orders` /
//! `parent_order_children`, see [`schema`]). `PostgresEms::new` picks
//! up every parent order still in `Scheduling` status from a previous
//! process and resumes its background scheduling loop from wherever
//! `filled_quantity` last left off -- mirroring
//! `apexes_oms::OrderManager::new`'s open-order recovery pattern.

mod order_manager_sink;
mod schema;

pub use order_manager_sink::OrderManagerChildOrderSink;
pub use schema::PARENT_ORDERS_TABLE_DDL;

use std::collections::HashMap;
use std::sync::Arc;

use apexes_core::{AssetClass, MarketEvent, Side, Venue};
use apexes_strategy::{ParentOrderId, ParentOrderStatus, VenueScorer};
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::PgPool;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::child_order_sink::{ChildOrderIntent, ChildOrderSink};
use crate::ems::{AlgoOrderRequest, Ems};
use crate::error::EmsError;
use crate::volume_curve_provider::VolumeCurveProvider;
use schema::{AlgoSpecJson, PARENT_ORDER_CHILDREN_TABLE_DDL, ParentOrderRow, RoutingJson};

pub struct PostgresEms {
    pool: PgPool,
    sink: Arc<dyn ChildOrderSink>,
    /// Retained for constructor-signature parity with
    /// `InMemoryEms::new` and for future SOR/split routing support --
    /// this task's initial scope submits every child order with
    /// `OrderRouting::Fixed` semantics (see `in_memory::routing`'s
    /// note), so neither field is read yet.
    #[allow(dead_code)]
    scorer: Arc<dyn VenueScorer>,
    #[allow(dead_code)]
    estimated_fee_bps: HashMap<AssetClass, Decimal>,
    volume_curve_provider: Option<Arc<dyn VolumeCurveProvider>>,
    /// Handles to per-parent-order background scheduling tasks (TWAP/
    /// VWAP/Iceberg only -- POV has no background task, it reacts to
    /// `on_market_event` calls instead), so `cancel_parent_order` can
    /// abort the right task.
    background_tasks: Mutex<HashMap<ParentOrderId, tokio::task::JoinHandle<()>>>,
}

/// The subset of `PostgresEms`'s state its background scheduling loops
/// need, cloned into each spawned task. `Ems::submit_algo_order` only
/// has `&self` (the trait signature provides no `Arc<Self>`), so the
/// spawned task cannot capture an `Arc<PostgresEms>` -- it captures
/// this cheap-to-clone handle instead (`PgPool` is internally
/// `Arc`-backed, the rest are `Arc`s/`Option<Arc>`).
#[derive(Clone)]
struct SchedulingDeps {
    pool: PgPool,
    sink: Arc<dyn ChildOrderSink>,
    volume_curve_provider: Option<Arc<dyn VolumeCurveProvider>>,
}

impl PostgresEms {
    /// Connects to PostgreSQL at `url`, ensures both tables exist, then
    /// recovers every `Scheduling`-status parent order left over from a
    /// previous process (crash recovery) by re-spawning its background
    /// scheduling task from wherever `filled_quantity` last left off.
    pub async fn new(
        url: &str,
        sink: Arc<dyn ChildOrderSink>,
        scorer: Arc<dyn VenueScorer>,
        estimated_fee_bps: HashMap<AssetClass, Decimal>,
        volume_curve_provider: Option<Arc<dyn VolumeCurveProvider>>,
    ) -> Result<Arc<Self>, EmsError> {
        let pool = PgPool::connect(url)
            .await
            .map_err(|error| EmsError::ConnectionFailed {
                reason: error.to_string(),
            })?;
        sqlx::query(PARENT_ORDERS_TABLE_DDL).execute(&pool).await?;
        sqlx::query(PARENT_ORDER_CHILDREN_TABLE_DDL)
            .execute(&pool)
            .await?;

        let ems = Arc::new(Self {
            pool,
            sink,
            scorer,
            estimated_fee_bps,
            volume_curve_provider,
            background_tasks: Mutex::new(HashMap::new()),
        });

        ems.recover_scheduling_parent_orders().await?;

        Ok(ems)
    }

    fn scheduling_deps(&self) -> SchedulingDeps {
        SchedulingDeps {
            pool: self.pool.clone(),
            sink: Arc::clone(&self.sink),
            volume_curve_provider: self.volume_curve_provider.clone(),
        }
    }

    async fn recover_scheduling_parent_orders(&self) -> Result<(), EmsError> {
        let rows: Vec<ParentOrderRow> =
            sqlx::query_as("SELECT * FROM parent_orders WHERE status = 'Scheduling'")
                .fetch_all(&self.pool)
                .await?;

        for row in rows {
            let parent_id = row.parent_id();
            let remaining = row.total_quantity - row.filled_quantity;
            if remaining <= Decimal::ZERO {
                continue;
            }
            tracing::info!(
                parent_id = ?parent_id,
                remaining = %remaining,
                "PostgresEms: recovering in-flight parent order from a previous process"
            );
            self.spawn_scheduling_task(parent_id, &row).await;
        }
        Ok(())
    }

    /// Spawns a background `tokio::spawn` task that paces TWAP/VWAP/
    /// Iceberg child-order submission using REAL wall-clock timers
    /// (`tokio::time::sleep`), independent of `on_market_event`. `Pov`
    /// requests get NO background task (POV is driven entirely by
    /// `on_market_event`'s `MarketEvent::Trade` handling instead, see
    /// `process_pov_on_trade`) -- this method returns immediately
    /// without spawning anything for a `Pov` parent order.
    async fn spawn_scheduling_task(&self, parent_id: ParentOrderId, row: &ParentOrderRow) {
        let algo_spec: apexes_strategy::ExecutionAlgoSpec = match serde_json::from_value::<
            AlgoSpecJson,
        >(row.algo_spec.clone())
        {
            Ok(json) => json.into(),
            Err(error) => {
                tracing::error!(?error, parent_id = ?parent_id, "PostgresEms: failed to deserialize algo_spec, cannot schedule");
                return;
            }
        };

        if matches!(algo_spec, apexes_strategy::ExecutionAlgoSpec::Pov { .. }) {
            // No background task -- handled entirely by on_market_event.
            return;
        }

        let deps = self.scheduling_deps();
        let handle = tokio::spawn(async move {
            if let Err(error) = deps.run_scheduling_loop(parent_id, algo_spec).await {
                tracing::error!(?error, parent_id = ?parent_id, "PostgresEms: background scheduling task ended with an error");
            }
        });

        self.background_tasks.lock().await.insert(parent_id, handle);
    }

    /// POV's entire scheduling is driven here, from `on_market_event`,
    /// not from a background task (see `spawn_scheduling_task`'s doc
    /// comment).
    async fn process_pov_on_trade(&self, trade: &apexes_core::TradeEvent) -> Result<(), EmsError> {
        let rows: Vec<ParentOrderRow> = sqlx::query_as(
            "SELECT * FROM parent_orders WHERE status = 'Scheduling' AND symbol = $1",
        )
        .bind(&trade.symbol)
        .fetch_all(&self.pool)
        .await?;

        let deps = self.scheduling_deps();
        for row in rows {
            let Ok(spec_json) = serde_json::from_value::<AlgoSpecJson>(row.algo_spec.clone())
            else {
                continue;
            };
            let apexes_strategy::ExecutionAlgoSpec::Pov { participation_rate } = spec_json.into()
            else {
                continue;
            };
            let parent_id = row.parent_id();
            let remaining = row.total_quantity - row.filled_quantity;
            if remaining <= Decimal::ZERO {
                deps.mark_completed_if_fully_submitted(parent_id).await?;
                continue;
            }
            // Cap at the remaining unsubmitted quantity, mirroring
            // `InMemoryEms`'s POV (see `ExecutionAlgoSpec::Pov`'s doc).
            let quantity = (trade.quantity * participation_rate).min(remaining);
            if quantity > Decimal::ZERO {
                deps.submit_and_record_child(parent_id, &row, quantity)
                    .await?;
                deps.mark_completed_if_fully_submitted(parent_id).await?;
            }
        }
        Ok(())
    }
}

impl SchedulingDeps {
    /// The real-clock-paced scheduling loop for TWAP/VWAP/Iceberg,
    /// running in its own spawned task for the lifetime of one parent
    /// order. Reuses `crate::in_memory`'s scheduling DECISIONS
    /// conceptually (same slice-quantity/interval math as
    /// `InMemoryEms`'s TWAP, same curve-lookup math as its VWAP, same
    /// resting-child-poll-and-refill logic as its Iceberg), but drives
    /// them from `tokio::time::sleep` between iterations instead of
    /// `Ems::on_market_event` calls, and persists every state change to
    /// `parent_orders`/`parent_order_children` instead of an in-process
    /// `Mutex<HashMap<..>>`.
    async fn run_scheduling_loop(
        &self,
        parent_id: ParentOrderId,
        algo_spec: apexes_strategy::ExecutionAlgoSpec,
    ) -> Result<(), EmsError> {
        match algo_spec {
            apexes_strategy::ExecutionAlgoSpec::Twap { duration, slices } => {
                self.run_twap_loop(parent_id, duration, slices).await
            }
            apexes_strategy::ExecutionAlgoSpec::Vwap {
                duration,
                lookback_days,
            } => self.run_vwap_loop(parent_id, duration, lookback_days).await,
            apexes_strategy::ExecutionAlgoSpec::Iceberg { visible_qty } => {
                self.run_iceberg_loop(parent_id, visible_qty).await
            }
            // Unreachable -- guarded in spawn_scheduling_task.
            apexes_strategy::ExecutionAlgoSpec::Pov { .. } => Ok(()),
        }
    }

    async fn run_twap_loop(
        &self,
        parent_id: ParentOrderId,
        duration: std::time::Duration,
        slices: u32,
    ) -> Result<(), EmsError> {
        let slices = slices.max(1);
        let interval = duration / slices;
        loop {
            let Some(row) = self.load_parent_order(parent_id).await? else {
                return Ok(());
            };
            if row.status != "Scheduling" {
                return Ok(());
            }
            let remaining = row.total_quantity - row.filled_quantity;
            if remaining <= Decimal::ZERO {
                return self.mark_completed_if_fully_submitted(parent_id).await;
            }
            // Sized off `remaining` (not a fixed `slices` iteration
            // count) so a crash-recovered parent order only submits
            // what it has NOT already submitted.
            let slice_quantity = (row.total_quantity / Decimal::from(slices)).min(remaining);
            self.submit_and_record_child(parent_id, &row, slice_quantity)
                .await?;
            tokio::time::sleep(interval).await;
        }
    }

    async fn run_iceberg_loop(
        &self,
        parent_id: ParentOrderId,
        visible_qty: Decimal,
    ) -> Result<(), EmsError> {
        loop {
            let Some(row) = self.load_parent_order(parent_id).await? else {
                return Ok(());
            };
            if row.status != "Scheduling" {
                return Ok(());
            }
            let remaining = row.total_quantity - row.filled_quantity;
            if remaining <= Decimal::ZERO {
                return self.mark_completed_if_fully_submitted(parent_id).await;
            }
            let slice = visible_qty.min(remaining);
            let child_order_id = self.submit_and_record_child(parent_id, &row, slice).await?;

            // Poll every 500ms until the resting child reaches a
            // terminal state before submitting the next slice --
            // matches InMemoryEms::Iceberg's own "wait for terminal,
            // then refill" contract, just paced by real sleeps instead
            // of virtual-clock on_market_event calls.
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let status = self.sink.order_status(child_order_id).await?;
                let terminal = matches!(
                    status.map(|order| order.status),
                    Some(
                        apexes_core::OrderStatus::Filled
                            | apexes_core::OrderStatus::Cancelled
                            | apexes_core::OrderStatus::Rejected
                    )
                );
                if terminal {
                    break;
                }
            }
        }
    }

    async fn run_vwap_loop(
        &self,
        parent_id: ParentOrderId,
        duration: std::time::Duration,
        lookback_days: u32,
    ) -> Result<(), EmsError> {
        let Some(row) = self.load_parent_order(parent_id).await? else {
            return Ok(());
        };
        let venue = parse_venue(&row.venue)?;
        let Some(provider) = &self.volume_curve_provider else {
            return self
                .fail_parent_order(
                    parent_id,
                    "no VolumeCurveProvider configured for this PostgresEms instance".to_string(),
                )
                .await;
        };
        let curve = match provider
            .intraday_volume_curve(&row.symbol, venue, lookback_days)
            .await
        {
            Ok(curve) => curve,
            Err(error) => {
                return self.fail_parent_order(parent_id, error.to_string()).await;
            }
        };

        let started_at = Utc::now();
        let poll_interval =
            std::time::Duration::from_millis((duration.as_millis() / 100).max(200) as u64);
        // Seed from the persisted filled fraction so a crash-recovered
        // parent order does not re-submit what it already submitted.
        let mut last_target_fraction = if row.total_quantity > Decimal::ZERO {
            row.filled_quantity / row.total_quantity
        } else {
            Decimal::ZERO
        };

        loop {
            let Some(row) = self.load_parent_order(parent_id).await? else {
                return Ok(());
            };
            if row.status != "Scheduling" {
                return Ok(());
            }
            let elapsed = Utc::now().signed_duration_since(started_at);
            let target_fraction = crate::in_memory::target_fraction_for_elapsed(&curve, elapsed);
            if target_fraction > last_target_fraction {
                let increment = row.total_quantity * (target_fraction - last_target_fraction);
                if increment > Decimal::ZERO {
                    self.submit_and_record_child(parent_id, &row, increment)
                        .await?;
                }
                last_target_fraction = target_fraction;
            }
            if target_fraction >= Decimal::ONE {
                return self.mark_completed_if_fully_submitted(parent_id).await;
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    async fn load_parent_order(
        &self,
        parent_id: ParentOrderId,
    ) -> Result<Option<ParentOrderRow>, EmsError> {
        let row: Option<ParentOrderRow> =
            sqlx::query_as("SELECT * FROM parent_orders WHERE id = $1")
                .bind(parent_id.0)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row)
    }

    /// Submits one child-order slice via `self.sink`, records it in
    /// `parent_order_children`, and updates
    /// `parent_orders.filled_quantity` -- the persisted equivalent of
    /// `InMemoryEms`'s in-memory `allocation.submitted_quantity += slice`
    /// bookkeeping.
    async fn submit_and_record_child(
        &self,
        parent_id: ParentOrderId,
        row: &ParentOrderRow,
        quantity: Decimal,
    ) -> Result<apexes_core::OrderId, EmsError> {
        let venue = parse_venue(&row.venue)?;
        let side = parse_side(&row.side)?;

        let child_order_id = self
            .sink
            .submit_child_order(ChildOrderIntent {
                symbol: row.symbol.clone(),
                venue,
                // See in_memory/routing.rs's Fixed-routing note (Task
                // 13) -- the same initial-scope limitation applies here.
                asset_class: AssetClass::Spot,
                side,
                quantity,
            })
            .await?;

        let child_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM parent_order_children WHERE parent_order_id = $1",
        )
        .bind(parent_id.0)
        .fetch_one(&self.pool)
        .await?;
        let sequence_number =
            i32::try_from(child_count).map_err(|error| EmsError::QueryFailed {
                reason: format!("child sequence number overflow: {error}"),
            })?;

        sqlx::query(
            "INSERT INTO parent_order_children (parent_order_id, child_order_id, sequence_number, created_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(parent_id.0)
        .bind(child_order_id.0)
        .bind(sequence_number)
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "UPDATE parent_orders SET filled_quantity = filled_quantity + $2, updated_at = $3 WHERE id = $1",
        )
        .bind(parent_id.0)
        .bind(quantity)
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;

        Ok(child_order_id)
    }

    async fn mark_completed_if_fully_submitted(
        &self,
        parent_id: ParentOrderId,
    ) -> Result<(), EmsError> {
        let Some(row) = self.load_parent_order(parent_id).await? else {
            return Ok(());
        };
        if row.filled_quantity >= row.total_quantity && row.status == "Scheduling" {
            sqlx::query(
                "UPDATE parent_orders SET status = 'Completed', updated_at = $2 WHERE id = $1",
            )
            .bind(parent_id.0)
            .bind(Utc::now())
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn fail_parent_order(
        &self,
        parent_id: ParentOrderId,
        reason: String,
    ) -> Result<(), EmsError> {
        sqlx::query(
            "UPDATE parent_orders SET status = 'Failed', failure_reason = $2, updated_at = $3 WHERE id = $1",
        )
        .bind(parent_id.0)
        .bind(&reason)
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl Ems for PostgresEms {
    async fn submit_algo_order(
        &self,
        request: AlgoOrderRequest,
    ) -> Result<ParentOrderId, EmsError> {
        let parent_id = ParentOrderId(Uuid::new_v4());
        let now = Utc::now();
        let algo_spec_json = serde_json::to_value(AlgoSpecJson::from(&request.execution_algo))
            .map_err(|error| EmsError::QueryFailed {
                reason: error.to_string(),
            })?;
        let routing_json =
            serde_json::to_value(RoutingJson::from(&request.routing)).map_err(|error| {
                EmsError::QueryFailed {
                    reason: error.to_string(),
                }
            })?;

        sqlx::query(
            "INSERT INTO parent_orders (id, symbol, venue, side, total_quantity, filled_quantity, algo_spec, routing, status, failure_reason, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'Scheduling', NULL, $9, $9)",
        )
        .bind(parent_id.0)
        .bind(&request.symbol)
        .bind(format!("{:?}", request.venue))
        .bind(format!("{:?}", request.side))
        .bind(request.quantity)
        .bind(Decimal::ZERO)
        .bind(algo_spec_json)
        .bind(routing_json)
        .bind(now)
        .execute(&self.pool)
        .await?;

        let row: ParentOrderRow = sqlx::query_as("SELECT * FROM parent_orders WHERE id = $1")
            .bind(parent_id.0)
            .fetch_one(&self.pool)
            .await?;

        self.spawn_scheduling_task(parent_id, &row).await;

        Ok(parent_id)
    }

    async fn parent_order_status(
        &self,
        id: ParentOrderId,
    ) -> Result<Option<ParentOrderStatus>, EmsError> {
        let row: Option<ParentOrderRow> =
            sqlx::query_as("SELECT * FROM parent_orders WHERE id = $1")
                .bind(id.0)
                .fetch_optional(&self.pool)
                .await?;
        row.map(|row| row.parse_status()).transpose()
    }

    async fn cancel_parent_order(&self, id: ParentOrderId) -> Result<(), EmsError> {
        sqlx::query("UPDATE parent_orders SET status = 'Cancelled', updated_at = $2 WHERE id = $1")
            .bind(id.0)
            .bind(Utc::now())
            .execute(&self.pool)
            .await?;

        if let Some(handle) = self.background_tasks.lock().await.remove(&id) {
            handle.abort();
        }

        let child_ids: Vec<(uuid::Uuid,)> = sqlx::query_as(
            "SELECT child_order_id FROM parent_order_children WHERE parent_order_id = $1",
        )
        .bind(id.0)
        .fetch_all(&self.pool)
        .await?;
        for (child_uuid,) in child_ids {
            self.sink
                .cancel_child_order(apexes_core::OrderId(child_uuid))
                .await?;
        }
        Ok(())
    }

    async fn on_market_event(&self, event: &MarketEvent) -> Result<(), EmsError> {
        // TWAP/VWAP/Iceberg are paced by their own background tasks
        // (spawned in submit_algo_order/recover_scheduling_parent_orders),
        // independent of this call -- only POV reacts here, since POV is
        // inherently trade-driven regardless of virtual vs. real clock
        // (see this module's docs).
        if let MarketEvent::Trade(trade) = event {
            self.process_pov_on_trade(trade).await?;
        }
        Ok(())
    }
}

fn parse_venue(s: &str) -> Result<Venue, EmsError> {
    match s {
        "Sse" => Ok(Venue::Sse),
        "Szse" => Ok(Venue::Szse),
        "Hkex" => Ok(Venue::Hkex),
        "Nasdaq" => Ok(Venue::Nasdaq),
        "Nyse" => Ok(Venue::Nyse),
        "Binance" => Ok(Venue::Binance),
        "Okx" => Ok(Venue::Okx),
        "Coinbase" => Ok(Venue::Coinbase),
        other => Err(EmsError::QueryFailed {
            reason: format!("unknown persisted Venue: {other:?}"),
        }),
    }
}

fn parse_side(s: &str) -> Result<Side, EmsError> {
    match s {
        "Buy" => Ok(Side::Buy),
        "Sell" => Ok(Side::Sell),
        other => Err(EmsError::QueryFailed {
            reason: format!("unknown persisted Side: {other:?}"),
        }),
    }
}
