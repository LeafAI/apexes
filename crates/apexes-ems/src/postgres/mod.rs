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

mod dispatch_inspection;
mod fill_attribution;
mod order_manager_sink;
mod schema;
mod state;

pub use crate::ems::{ChildAssociation, FillParentEvidence};
pub use dispatch_inspection::{DispatchEvidence, inspect_pending_dispatch};
pub use fill_attribution::inspect_fill_parent;
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
use state::SchedulingState;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
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
    state: Arc<SchedulingState>,
    pov_tx: mpsc::Sender<PovWork>,
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
    state: Arc<SchedulingState>,
}

struct PovWork {
    trade: apexes_core::TradeEvent,
    parents: Vec<ParentOrderId>,
    enqueued: tokio::time::Instant,
}

const POV_QUEUE_CAPACITY: usize = 256;
const HEALTH_INTERVAL: Duration = Duration::from_secs(2);
const DB_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_POV_AGE: Duration = Duration::from_secs(2);

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
        Self::new_with_kill_switch(
            url,
            sink,
            scorer,
            estimated_fee_bps,
            volume_curve_provider,
            None,
        )
        .await
    }

    /// The live runner shares its OMS kill switch with EMS. A latched
    /// scheduling fault requires restart/reconciliation, not auto-resume.
    pub async fn new_with_kill_switch(
        url: &str,
        sink: Arc<dyn ChildOrderSink>,
        scorer: Arc<dyn VenueScorer>,
        estimated_fee_bps: HashMap<AssetClass, Decimal>,
        volume_curve_provider: Option<Arc<dyn VolumeCurveProvider>>,
        kill_switch: Option<Arc<apexes_oms::KillSwitch>>,
    ) -> Result<Arc<Self>, EmsError> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(DB_TIMEOUT)
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query("SET statement_timeout = '2000ms'")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .map_err(|error| EmsError::ConnectionFailed {
                reason: error.to_string(),
            })?;
        sqlx::query(PARENT_ORDERS_TABLE_DDL).execute(&pool).await?;
        sqlx::query("ALTER TABLE parent_orders ADD COLUMN IF NOT EXISTS dispatch_pending BOOLEAN NOT NULL DEFAULT FALSE")
            .execute(&pool).await?;
        sqlx::query("ALTER TABLE parent_orders ADD COLUMN IF NOT EXISTS intent_scope UUID, ADD COLUMN IF NOT EXISTS intent_ordinal NUMERIC(20,0), ADD COLUMN IF NOT EXISTS pending_dispatch_id UUID")
            .execute(&pool).await?;
        sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS idx_parent_scoped_intent ON parent_orders(intent_scope,intent_ordinal) WHERE intent_scope IS NOT NULL AND intent_ordinal IS NOT NULL")
            .execute(&pool).await?;
        sqlx::query(PARENT_ORDER_CHILDREN_TABLE_DDL)
            .execute(&pool)
            .await?;
        sqlx::query("ALTER TABLE parent_order_children ADD COLUMN IF NOT EXISTS dispatch_id UUID")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS idx_parent_children_dispatch ON parent_order_children(dispatch_id) WHERE dispatch_id IS NOT NULL")
            .execute(&pool).await?;

        let state = Arc::new(SchedulingState::new(kill_switch));
        let (pov_tx, pov_rx) = mpsc::channel(POV_QUEUE_CAPACITY);
        let ems = Arc::new(Self {
            pool,
            sink,
            scorer,
            estimated_fee_bps,
            volume_curve_provider,
            background_tasks: Mutex::new(HashMap::new()),
            state,
            pov_tx,
        });

        ems.recover_scheduling_parent_orders().await?;
        Self::spawn_pov_worker(&ems, pov_rx);
        Self::spawn_health_monitor(&ems);
        Ok(ems)
    }

    /// Read-only lookup of a persisted strategy parent identity.
    pub async fn parent_for_intent(
        &self,
        scope: Uuid,
        id: apexes_strategy::IntentId,
    ) -> Result<Option<ParentOrderId>, EmsError> {
        if scope.is_nil() || id.0 == 0 {
            return Err(EmsError::ParentAdmissionRejected {
                reason: "invalid scoped parent intent".into(),
            });
        }
        let id: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM parent_orders WHERE intent_scope=$1 AND intent_ordinal=$2",
        )
        .bind(scope)
        .bind(Decimal::from(id.0))
        .fetch_optional(&self.pool)
        .await?;
        Ok(id.map(ParentOrderId))
    }

    /// Exact pending dispatch identity, not evidence of successful submission.
    pub async fn pending_dispatch(&self, parent: ParentOrderId) -> Result<Option<Uuid>, EmsError> {
        let id: Option<Option<Uuid>> = sqlx::query_scalar(
            "SELECT pending_dispatch_id FROM parent_orders WHERE id=$1 AND dispatch_pending",
        )
        .bind(parent.0)
        .fetch_optional(&self.pool)
        .await?;
        Ok(id.flatten())
    }

    fn scheduling_deps(&self) -> SchedulingDeps {
        SchedulingDeps {
            pool: self.pool.clone(),
            sink: Arc::clone(&self.sink),
            volume_curve_provider: self.volume_curve_provider.clone(),
            state: Arc::clone(&self.state),
        }
    }

    async fn recover_scheduling_parent_orders(&self) -> Result<(), EmsError> {
        let rows: Vec<ParentOrderRow> = sqlx::query_as("SELECT * FROM parent_orders")
            .fetch_all(&self.pool)
            .await?;

        // Validate the entire recovery set before spawning any task.
        if rows.iter().any(|row| row.dispatch_pending) {
            self.state.trip("unresolved EMS child dispatch at startup");
            return Err(EmsError::SchedulingSuspended);
        }
        for row in &rows {
            if row.status == "Scheduling" {
                self.sink
                    .verify_parent(
                        row.parent_id(),
                        &row.symbol,
                        parse_side(&row.side)?,
                        row.total_quantity,
                        row.filled_quantity,
                    )
                    .await?;
            } else {
                self.sink.release_parent(row.parent_id()).await?;
            }
            self.state.register(row)?;
        }
        let mut recover = Vec::new();
        for row in rows {
            if row.status != "Scheduling" {
                continue;
            }
            let parent_id = row.parent_id();
            let remaining = row.total_quantity - row.filled_quantity;
            if remaining <= Decimal::ZERO {
                self.scheduling_deps()
                    .mark_completed_if_fully_submitted(parent_id)
                    .await?;
                continue;
            }
            tracing::info!(
                parent_id = ?parent_id,
                remaining = %remaining,
                "PostgresEms: recovering in-flight parent order from a previous process"
            );
            recover.push(row);
        }
        for row in recover {
            self.spawn_scheduling_task(row.parent_id(), &row).await;
        }
        Ok(())
    }

    /// Spawns a background `tokio::spawn` task that paces TWAP/VWAP/
    /// Iceberg child-order submission using REAL wall-clock timers
    /// (`tokio::time::sleep`), independent of `on_market_event`. `Pov`
    /// requests get NO per-parent task (POV uses the bounded worker
    /// fed by `on_market_event`) -- this method returns immediately
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
            if !deps.state.wait_startup_ready().await {
                return;
            }
            if deps
                .run_scheduling_loop(parent_id, algo_spec)
                .await
                .is_err()
            {
                tracing::error!(parent_id = ?parent_id, "PostgresEms: background scheduling task ended with an error");
                deps.state.trip("EMS background scheduling failed");
            }
        });

        self.background_tasks.lock().await.insert(parent_id, handle);
    }

    fn spawn_pov_worker(ems: &Arc<Self>, mut rx: mpsc::Receiver<PovWork>) {
        let weak = Arc::downgrade(ems);
        tokio::spawn(async move {
            while let Some(work) = rx.recv().await {
                let Some(ems) = weak.upgrade() else {
                    return;
                };
                if !ems.state.allowed() {
                    continue;
                }
                if work.enqueued.elapsed() > MAX_POV_AGE {
                    ems.state.trip("EMS POV queue stale");
                    continue;
                }
                if ems.process_pov(work).await.is_err() {
                    ems.state.trip("EMS POV scheduling failed");
                }
            }
        });
    }

    fn spawn_health_monitor(ems: &Arc<Self>) {
        let weak = Arc::downgrade(ems);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(HEALTH_INTERVAL).await;
                let Some(ems) = weak.upgrade() else {
                    return;
                };
                if !ems.state.healthy() {
                    ems.state.trip("EMS reconciliation required");
                    continue;
                }
                let result =
                    tokio::time::timeout(DB_TIMEOUT, sqlx::query("SELECT 1").execute(&ems.pool))
                        .await;
                if !matches!(result, Ok(Ok(_))) {
                    ems.state.trip("EMS database unavailable");
                }
            }
        });
    }

    async fn process_pov(&self, work: PovWork) -> Result<(), EmsError> {
        let deps = self.scheduling_deps();
        for parent_id in work.parents {
            if !self.state.allowed() {
                break;
            }
            if !self.state.is_active(parent_id) {
                continue;
            }
            let Some(row) = deps.load_parent_order(parent_id).await? else {
                return Err(EmsError::ParentOrderNotFound { id: parent_id });
            };
            if row.status != "Scheduling" {
                self.state.set_status(parent_id, row.parse_status()?);
                continue;
            }
            let spec: apexes_strategy::ExecutionAlgoSpec =
                serde_json::from_value::<AlgoSpecJson>(row.algo_spec.clone())
                    .map_err(|_| EmsError::SchedulingSuspended)?
                    .into();
            let apexes_strategy::ExecutionAlgoSpec::Pov { participation_rate } = spec else {
                continue;
            };
            let remaining = row.total_quantity - row.filled_quantity;
            let quantity = (work.trade.quantity * participation_rate).min(remaining);
            if quantity > Decimal::ZERO {
                deps.submit_and_record_child(parent_id, &row, quantity)
                    .await?;
            }
            deps.mark_completed_if_fully_submitted(parent_id).await?;
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
        if !self.state.allowed() {
            return Err(EmsError::SchedulingSuspended);
        }
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
        if !self.state.allowed() {
            return Err(EmsError::SchedulingSuspended);
        }
        // A committed reservation prevents another dispatcher (including
        // after restart) from treating an interrupted child as unsent.
        let dispatch_id = Uuid::new_v4();
        let reserved = sqlx::query("UPDATE parent_orders SET dispatch_pending = TRUE, pending_dispatch_id = $3 WHERE id = $1 AND status = 'Scheduling' AND NOT dispatch_pending AND filled_quantity + $2 <= total_quantity")
            .bind(parent_id.0).bind(quantity).bind(dispatch_id).execute(&self.pool).await?;
        if reserved.rows_affected() != 1 || !self.state.allowed() {
            return Err(EmsError::SchedulingSuspended);
        }

        // `_fills` is deliberately discarded here: this sink is always
        // `OrderManagerChildOrderSink`, which returns an empty `Vec` by
        // design -- PostgresEms's crash-recoverable fill-tracking
        // happens entirely through `OrderManager`'s own persisted
        // state (`apply_exchange_update`/`reconcile()`), not through
        // this return value.
        let (child_order_id, _fills) = self
            .sink
            .submit_dispatched_child(parent_id, dispatch_id, ChildOrderIntent {
                symbol: row.symbol.clone(),
                venue,
                // See in_memory/routing.rs's Fixed-routing note (Task
                // 13) -- the same initial-scope limitation applies here.
                asset_class: AssetClass::Spot,
                side,
                quantity,
                // Known limitation: the `parent_orders` table has no
                // leverage/margin_mode columns, so `PostgresEms` cannot
                // thread a leveraged parent order's values onto its
                // child orders -- a `Some(leverage)` AlgoOrderRequest
                // submitted here silently degrades to spot accounting,
                // the same behavior as before those fields existed.
                // Persisting them requires a schema change (new columns
                // + INSERT/SELECT round-trip), out of scope for the
                // margin-monitoring fix wave -- `InMemoryEms` (the
                // backtest/paper path the regression coverage targets)
                // threads them correctly.
                leverage: None,
                margin_mode: None,
            })
            .await
            .inspect_err(|_| tracing::info!(target: "apexes::decision", stage = "ems_child_failed", parent_id = %parent_id.0, reason = "child_submission_failed"))?;
        // Record the accepted child even if the subsequent persistence fails.
        tracing::info!(target: "apexes::decision", stage = "ems_child_accepted", parent_id = %parent_id.0, order_id = %child_order_id.0);

        // Child link and submitted quantity are committed together.
        let mut tx = self.pool.begin().await?;
        let child_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM parent_order_children WHERE parent_order_id = $1",
        )
        .bind(parent_id.0)
        .fetch_one(&mut *tx)
        .await?;
        let sequence_number =
            i32::try_from(child_count).map_err(|error| EmsError::QueryFailed {
                reason: format!("child sequence number overflow: {error}"),
            })?;

        sqlx::query(
            "INSERT INTO parent_order_children (parent_order_id, child_order_id, sequence_number, created_at, dispatch_id) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(parent_id.0)
        .bind(child_order_id.0)
        .bind(sequence_number)
        .bind(Utc::now())
        .bind(dispatch_id)
        .execute(&mut *tx)
        .await?;

        let updated = sqlx::query(
            "UPDATE parent_orders SET filled_quantity = filled_quantity + $2, updated_at = $3, dispatch_pending = FALSE, pending_dispatch_id = NULL WHERE id = $1 AND dispatch_pending AND pending_dispatch_id = $4",
        )
        .bind(parent_id.0)
        .bind(quantity)
        .bind(Utc::now())
        .bind(dispatch_id)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(EmsError::SchedulingSuspended);
        }
        tx.commit().await?;

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
            let updated = sqlx::query(
                "UPDATE parent_orders SET status = 'Completed', updated_at = $2 WHERE id = $1 AND status = 'Scheduling' AND NOT dispatch_pending",
            )
            .bind(parent_id.0)
            .bind(Utc::now())
            .execute(&self.pool)
            .await?;
            if updated.rows_affected() == 1 {
                self.state
                    .set_status(parent_id, ParentOrderStatus::Completed);
                self.sink.release_parent(parent_id).await?;
            }
        }
        Ok(())
    }

    async fn fail_parent_order(
        &self,
        parent_id: ParentOrderId,
        reason: String,
    ) -> Result<(), EmsError> {
        let updated = sqlx::query(
            "UPDATE parent_orders SET status = 'Failed', failure_reason = $2, updated_at = $3 WHERE id = $1 AND status = 'Scheduling' AND NOT dispatch_pending",
        )
        .bind(parent_id.0)
        .bind(&reason)
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() == 1 {
            self.state
                .set_status(parent_id, ParentOrderStatus::Failed { reason });
            self.sink.release_parent(parent_id).await?;
        }
        Ok(())
    }
}

impl PostgresEms {
    async fn submit_parent(
        &self,
        request: AlgoOrderRequest,
        link: Option<(Uuid, apexes_strategy::IntentId)>,
    ) -> Result<ParentOrderId, EmsError> {
        if !self.state.allowed() {
            return Err(EmsError::SchedulingSuspended);
        }
        if request.leverage.is_some() {
            // Known limitation (see `submit_and_record_child`): the
            // `parent_orders` table does not persist
            // leverage/margin_mode, so this parent order's child orders
            // will be spot-accounted. Warn loudly rather than silently
            // degrade -- persisting them requires a schema change, out
            // of scope for the margin-monitoring fix wave.
            tracing::warn!(
                symbol = %request.symbol,
                leverage = %request.leverage.unwrap_or(Decimal::ZERO),
                "PostgresEms: leveraged parent order received, but leverage/margin_mode \
                 are not persisted to parent_orders -- child orders will be spot-accounted"
            );
        }
        if let Some((scope, id)) = link {
            if scope.is_nil() || id.0 == 0 {
                return Err(EmsError::ParentAdmissionRejected {
                    reason: "invalid scoped parent intent".into(),
                });
            }
            let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM parent_orders WHERE intent_scope=$1 AND intent_ordinal=$2)").bind(scope).bind(Decimal::from(id.0)).fetch_one(&self.pool).await?;
            if exists {
                return Err(EmsError::ParentAdmissionRejected {
                    reason: "scoped parent intent already exists".into(),
                });
            }
        }
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

        // The OMS allocation is durable before the parent can schedule.
        // A crash between these records leaves an orphan lock, never
        // an unfunded scheduling parent. Audit is required to release it.
        self.sink.reserve_parent(parent_id, &request).await?;
        let row: ParentOrderRow = sqlx::query_as(
            "INSERT INTO parent_orders (id, symbol, venue, side, total_quantity, filled_quantity, algo_spec, routing, status, failure_reason, created_at, updated_at, intent_scope, intent_ordinal)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'Scheduling', NULL, $9, $9, $10, $11) RETURNING *",
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
        .bind(link.map(|(scope,_)|scope))
        .bind(link.map(|(_,id)|Decimal::from(id.0)))
        .fetch_one(&self.pool)
        .await?;

        self.state.register(&row)?;
        self.spawn_scheduling_task(parent_id, &row).await;

        Ok(parent_id)
    }
}

#[async_trait]
impl Ems for PostgresEms {
    async fn submit_algo_order(
        &self,
        request: AlgoOrderRequest,
    ) -> Result<ParentOrderId, EmsError> {
        self.submit_correlated_algo_order(request, None).await
    }

    async fn submit_correlated_algo_order(
        &self,
        request: AlgoOrderRequest,
        link: Option<(Uuid, apexes_strategy::IntentId)>,
    ) -> Result<ParentOrderId, EmsError> {
        if !self.state.trading_ready() {
            return Err(EmsError::SchedulingSuspended);
        }
        let result = self.submit_parent(request, link).await;
        if result
            .as_ref()
            .is_err_and(|e| !matches!(e, EmsError::ParentAdmissionRejected { .. }))
        {
            self.state.trip("EMS parent persistence failed");
        }
        result
    }

    async fn inspect_live_fill_parents(
        &self,
        ids: &[apexes_core::FillId],
    ) -> Result<Vec<FillParentEvidence>, EmsError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            fill_attribution::inspect_fill_parents(&self.pool, ids),
        )
        .await
        .map_err(|_| EmsError::QueryFailed {
            reason: "live fill attribution snapshot timed out".into(),
        })?
    }

    async fn inspect_live_terminal_parents(
        &self,
        ids: &[apexes_core::OrderId],
    ) -> Result<Vec<FillParentEvidence>, EmsError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            fill_attribution::inspect_terminal_parents(&self.pool, ids),
        )
        .await
        .map_err(|_| EmsError::QueryFailed {
            reason: "live terminal attribution snapshot timed out".into(),
        })?
    }

    async fn parent_order_status(
        &self,
        id: ParentOrderId,
    ) -> Result<Option<ParentOrderStatus>, EmsError> {
        // Every recovered/new parent is cached; polling the trading loop
        // never performs a database read for a known parent.
        if let Some(status) = self.state.status(id) {
            return Ok(Some(status));
        }
        Ok(None)
    }

    async fn cancel_parent_order(&self, id: ParentOrderId) -> Result<(), EmsError> {
        let result = sqlx::query("UPDATE parent_orders SET status = 'Cancelled', updated_at = $2 WHERE id = $1 AND NOT dispatch_pending")
            .bind(id.0).bind(Utc::now()).execute(&self.pool).await;
        match result {
            Ok(result) if result.rows_affected() == 1 => {
                self.state.set_status(id, ParentOrderStatus::Cancelled)
            }
            _ => {
                self.state.trip("EMS cancellation requires reconciliation");
                return Err(EmsError::SchedulingSuspended);
            }
        }

        if let Some(handle) = self.background_tasks.lock().await.remove(&id) {
            handle.abort();
        }

        let child_ids: Vec<(uuid::Uuid,)> = sqlx::query_as(
            "SELECT child_order_id FROM parent_order_children WHERE parent_order_id = $1",
        )
        .bind(id.0)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| {
            self.state
                .trip("EMS child lookup failed during cancellation");
            EmsError::from(error)
        })?;
        for (child_uuid,) in child_ids {
            self.sink
                .cancel_child_order(apexes_core::OrderId(child_uuid))
                .await
                .inspect_err(|_| self.state.trip("EMS child cancellation failed"))?;
        }
        self.sink
            .release_parent(id)
            .await
            .inspect_err(|_| self.state.trip("EMS parent allocation release failed"))?;
        Ok(())
    }

    async fn on_market_event(
        &self,
        event: &MarketEvent,
    ) -> Result<Vec<apexes_core::Fill>, EmsError> {
        // TWAP/VWAP/Iceberg are paced by their own background tasks
        // (spawned in submit_algo_order/recover_scheduling_parent_orders),
        // independent of this call -- only POV reacts here, since POV is
        // inherently trade-driven regardless of virtual vs. real clock
        // (see this module's docs). This implementation never returns
        // any fills here (see this trait method's own doc comment on
        // `Ems` for why: `PostgresEms`'s `ChildOrderSink` is always
        // `OrderManagerChildOrderSink`, whose fills surface later via
        // `OrderManager::apply_exchange_update`/`reconcile()`, not
        // synchronously here).
        if !self.state.trading_ready() {
            return Ok(Vec::new());
        }
        if let MarketEvent::Trade(trade) = event {
            let parents = self.state.active_for(trade.venue, &trade.symbol);
            if !parents.is_empty()
                && self
                    .pov_tx
                    .try_send(PovWork {
                        trade: trade.clone(),
                        parents,
                        enqueued: tokio::time::Instant::now(),
                    })
                    .is_err()
            {
                self.state.trip("EMS POV queue overflow or worker stopped");
            }
        }
        Ok(Vec::new())
    }
}

impl Drop for PostgresEms {
    fn drop(&mut self) {
        for (_, handle) in self.background_tasks.get_mut().drain() {
            handle.abort();
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::OrderId;
    use apexes_strategy::{ExecutionAlgoSpec, OrderRouting, PriceAndFeeScorer};
    use rust_decimal_macros::dec;

    struct NoDispatch;
    #[async_trait]
    impl ChildOrderSink for NoDispatch {
        async fn submit_child_order(
            &self,
            _: ChildOrderIntent,
        ) -> Result<(OrderId, Vec<apexes_core::Fill>), EmsError> {
            panic!("unexpected child dispatch")
        }
        async fn cancel_child_order(&self, _: OrderId) -> Result<(), EmsError> {
            Ok(())
        }
        async fn order_status(&self, _: OrderId) -> Result<Option<apexes_core::Order>, EmsError> {
            Ok(None)
        }
    }

    #[tokio::test]
    #[ignore]
    async fn idle_and_unmatched_events_need_no_database_and_outage_latches_halt() {
        let base_url = "postgres://apexes:apexes@localhost:5432/apexes";
        let pool = PgPool::connect(base_url).await.unwrap();
        // SQL identifier is generated exclusively from a fixed prefix and UUID hex.
        let schema = format!("ems_test_{}", Uuid::new_v4().simple());
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&pool)
            .await
            .unwrap();
        let url = format!("{base_url}?options[search_path]={schema}");
        let ks = Arc::new(apexes_oms::KillSwitch::new());
        let ems = PostgresEms::new_with_kill_switch(
            &url,
            Arc::new(NoDispatch),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
            Some(ks.clone()),
        )
        .await
        .unwrap();
        let id = ems
            .submit_algo_order(AlgoOrderRequest {
                symbol: "BTCUSDT".into(),
                venue: Venue::Binance,
                side: Side::Buy,
                quantity: dec!(1),
                execution_algo: ExecutionAlgoSpec::Pov {
                    participation_rate: dec!(0.5),
                },
                routing: OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            })
            .await
            .unwrap();
        ems.pool.close().await;
        let event = MarketEvent::Trade(apexes_core::TradeEvent {
            symbol: "ETHUSDT".into(),
            venue: Venue::Binance,
            price: dec!(100),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        for _ in 0..1000 {
            assert!(ems.on_market_event(&event).await.unwrap().is_empty());
            assert_eq!(
                ems.parent_order_status(id).await.unwrap(),
                Some(ParentOrderStatus::Scheduling)
            );
        }
        assert!(
            !ks.is_engaged(),
            "idle event path must not query closed pool"
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while !ks.is_engaged() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!ems.state.healthy());
        ks.release();
        ems.on_market_event(&event).await.unwrap();
        assert!(
            ks.is_engaged(),
            "operator release cannot clear EMS fault latch"
        );
        drop(ems);
        sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&pool)
            .await
            .unwrap();
    }
}
