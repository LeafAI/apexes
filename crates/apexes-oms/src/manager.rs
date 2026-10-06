//! [`OrderManager`]: the business-logic layer -- risk checks, gateway
//! submission, status tracking, and reconnect reconciliation. Holds an
//! in-memory `HashMap<OrderId, Order>` snapshot of open orders (mirroring
//! `apexes_backtest::PortfolioLedger`'s in-memory-plus-persistence
//! pattern), rebuilt from `OrderStore::get_open_orders` on construction
//! so a process restart recovers its view of in-flight orders without
//! waiting for the next `reconcile()` call.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use apexes_core::registry::Registry;
use apexes_core::{AssetClass, Fill, FillId, Order, OrderId, OrderStatus};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{OrderIntent, Position};
use chrono::Utc;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::error::OmsError;
use crate::gateway::{ExecutionGateway, GatewayOutcome};
use crate::store::OrderStore;

pub struct OrderManager {
    store: OrderStore,
    gateway: Arc<dyn ExecutionGateway>,
    risk_checker: Option<Arc<CompositeRiskChecker>>,
    open_orders: HashMap<OrderId, Order>,
}

impl OrderManager {
    /// Constructs a new manager, rebuilding `open_orders` from
    /// `store.get_open_orders()` -- the "recover in-flight order state
    /// after a process restart" half of reconnect reconciliation;
    /// `reconcile()` is the other half (comparing that recovered state
    /// against the exchange's current truth). `gateway` (exactly one
    /// `Arc<dyn ExecutionGateway>`) is looked up via `registry.one()` --
    /// a missing or ambiguous (2+) registration is a hard error, since
    /// an `OrderManager` cannot function without exactly one gateway.
    /// `risk_checker` (`Arc<CompositeRiskChecker>`) is looked up via
    /// `registry.try_one()` -- absent is valid and means "no risk
    /// checking configured", matching this method's previous `None`
    /// parameter semantics exactly.
    pub async fn new(store: OrderStore, registry: &Registry) -> Result<Self, OmsError> {
        let gateway = registry.one::<Arc<dyn ExecutionGateway>>()?;
        let risk_checker = registry.try_one::<Arc<CompositeRiskChecker>>()?;

        let open_orders_vec = store.get_open_orders().await?;
        let open_orders = open_orders_vec
            .into_iter()
            .map(|order| (order.id, order))
            .collect();
        Ok(Self {
            store,
            gateway,
            risk_checker,
            open_orders,
        })
    }

    /// Submits a new order:
    /// 1. Runs `risk_checker` (if configured) against `intent`; returns
    ///    `Err(OmsError::RiskCheckFailed)` without creating any order if
    ///    it fails. `positions`/`balances` are caller-supplied snapshots
    ///    passed straight through to `risk_checker.check(..)` --
    ///    `OrderManager` does not own position/balance tracking itself
    ///    (that is sub-project #11's scope).
    /// 2. Constructs an `Order` (status `New`, `exchange_order_id:
    ///    None`), persists it via `store.upsert_order` BEFORE calling
    ///    the gateway (a local record the gateway never accepted is
    ///    recoverable via reconciliation or manual review; a
    ///    gateway-accepted order with no local record is not).
    /// 3. Calls `gateway.submit_order`:
    ///    - `Accepted { exchange_order_id }`: updates the order's
    ///      `exchange_order_id` field, re-persists, inserts into
    ///      `open_orders`, returns `Ok(order_id)`.
    ///    - `Rejected { reason }`: transitions the order to `Rejected`,
    ///      re-persists, does NOT insert into `open_orders`, returns
    ///      `Err(OmsError::OrderRejected { reason })`.
    ///    - `Uncertain { reason }`: leaves the order's status as `New`
    ///      (already persisted), inserts into `open_orders` anyway (it
    ///      is genuinely uncertain whether it's live), logs a
    ///      `tracing::warn!`, and returns `Ok(order_id)` -- only
    ///      `reconcile()` will later resolve the true status.
    pub async fn submit_order(
        &mut self,
        intent: OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Result<OrderId, OmsError> {
        if let Some(checker) = &self.risk_checker {
            let empty_open_orders = HashMap::new();
            if let Err(violations) =
                checker.check(&intent, positions, balances, &empty_open_orders, Utc::now())
            {
                return Err(OmsError::RiskCheckFailed { violations });
            }
        }

        let now = Utc::now();
        let mut order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: intent.symbol,
            venue: intent.venue,
            asset_class: intent.asset_class,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: intent.price,
            stop_price: intent.stop_price,
            leverage: intent.leverage,
            margin_mode: intent.margin_mode,
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
        self.store.upsert_order(&order).await?;

        match self.gateway.submit_order(&order).await {
            GatewayOutcome::Accepted { exchange_order_id } => {
                order.exchange_order_id = Some(exchange_order_id);
                order.updated_at = Utc::now();
                self.store.upsert_order(&order).await?;
                let order_id = order.id;
                self.open_orders.insert(order_id, order);
                Ok(order_id)
            }
            GatewayOutcome::Rejected { reason } => {
                order.status = OrderStatus::Rejected;
                order.updated_at = Utc::now();
                self.store.upsert_order(&order).await?;
                Err(OmsError::OrderRejected { reason })
            }
            GatewayOutcome::Uncertain { reason } => {
                tracing::warn!(
                    order_id = ?order.id,
                    reason,
                    "order submission outcome uncertain; deferring to reconcile()"
                );
                let order_id = order.id;
                self.open_orders.insert(order_id, order);
                Ok(order_id)
            }
        }
    }

    /// Cancels an existing order:
    /// 1. Looks up `order_id` in `open_orders`; returns
    ///    `Err(OmsError::OrderNotFound)` if absent, or
    ///    `Err(OmsError::OrderNotCancellable { current_status })` if its
    ///    status isn't `New`/`PartiallyFilled`.
    /// 2. Checks `order.exchange_order_id`: if `None` (this order's
    ///    original `submit_order` call returned `Uncertain` and has not
    ///    yet been resolved by `reconcile()`), returns
    ///    `Err(OmsError::ExchangeOrderIdUnknown)` rather than calling
    ///    the gateway.
    /// 3. Otherwise calls `gateway.cancel_order`. Same three-way
    ///    `GatewayOutcome` handling as `submit_order`: `Accepted` ->
    ///    mark `Cancelled`, remove from `open_orders`; `Rejected` ->
    ///    return `Err` with the reason, order status unchanged;
    ///    `Uncertain` -> log and leave status unchanged, defer to
    ///    `reconcile()`.
    pub async fn cancel_order(&mut self, order_id: OrderId) -> Result<(), OmsError> {
        let order = self
            .open_orders
            .get(&order_id)
            .ok_or(OmsError::OrderNotFound { order_id })?;

        if !matches!(
            order.status,
            OrderStatus::New | OrderStatus::PartiallyFilled
        ) {
            return Err(OmsError::OrderNotCancellable {
                order_id,
                current_status: order.status,
            });
        }

        if order.exchange_order_id.is_none() {
            return Err(OmsError::ExchangeOrderIdUnknown { order_id });
        }
        let order_snapshot = order.clone();

        match self.gateway.cancel_order(&order_snapshot).await {
            GatewayOutcome::Accepted { .. } => {
                let mut order = self.open_orders.remove(&order_id).unwrap();
                order.status = OrderStatus::Cancelled;
                order.updated_at = Utc::now();
                self.store.upsert_order(&order).await?;
                Ok(())
            }
            GatewayOutcome::Rejected { reason } => Err(OmsError::OrderRejected { reason }),
            GatewayOutcome::Uncertain { reason } => {
                tracing::warn!(
                    order_id = ?order_id,
                    reason,
                    "order cancellation outcome uncertain; deferring to reconcile()"
                );
                Ok(())
            }
        }
    }

    /// Applies one authoritative exchange-reported status update to a
    /// single local order -- the shared core both `reconcile()`'s two
    /// passes (below) and live push-based updates (Binance User Data
    /// Stream, via `apexes-gateway-binance`'s `ExchangeUpdate` stream)
    /// call, so this overwrite/persist/fill-fetch logic exists in
    /// exactly one place.
    ///
    /// Unconditionally overwrites local status with `status` if it
    /// differs from the order's current status (same
    /// authoritative-exchange rationale as this method's callers --
    /// bypassing `OrderStatus::can_transition_to`'s normal-operation
    /// validation is intentional: a transition it would reject during
    /// live operation reflects a real event that happened while
    /// disconnected or between poll intervals, not a bug). If
    /// `exchange_order_id` is `Some` and differs from what's currently
    /// stored, populates/updates it.
    ///
    /// If either the status or `exchange_order_id` changed, persists
    /// the order via `store.upsert_order`. If the status transition
    /// lands on `PartiallyFilled` or `Filled` (and actually changed --
    /// a repeated push of the same already-applied status is a no-op),
    /// calls `gateway.query_fills_for_order`, deduplicates each
    /// returned `ExchangeFill` against `OrderStore` via
    /// `exchange_trade_id`, persists any genuinely-new ones, and
    /// returns them. If the new status is terminal (`Filled`,
    /// `Cancelled`, `Rejected`), removes the order from `open_orders`.
    ///
    /// Returns `Ok(new_fills)` -- empty if the order wasn't found
    /// locally (logs `tracing::error!`), if nothing changed, or if no
    /// new fills were found beyond what was already recorded.
    pub async fn apply_exchange_update(
        &mut self,
        client_order_id: OrderId,
        exchange_order_id: Option<String>,
        status: OrderStatus,
    ) -> Result<Vec<Fill>, OmsError> {
        let Some(previous_status) = self
            .open_orders
            .get(&client_order_id)
            .map(|order| order.status)
        else {
            tracing::error!(
                order_id = ?client_order_id,
                "apply_exchange_update: exchange reports an order we have no local record of"
            );
            return Ok(Vec::new());
        };

        let status_changed = previous_status != status;
        let mut exchange_order_id_changed = false;

        if let Some(order) = self.open_orders.get_mut(&client_order_id) {
            if exchange_order_id.is_some() && order.exchange_order_id != exchange_order_id {
                order.exchange_order_id = exchange_order_id;
                exchange_order_id_changed = true;
            }
            if status_changed {
                order.status = status;
                order.updated_at = Utc::now();
            }
        }

        if !status_changed && !exchange_order_id_changed {
            return Ok(Vec::new());
        }

        let order_snapshot = self.open_orders.get(&client_order_id).unwrap().clone();
        self.store.upsert_order(&order_snapshot).await?;

        let mut new_fills = Vec::new();
        if status_changed
            && matches!(status, OrderStatus::PartiallyFilled | OrderStatus::Filled)
            && order_snapshot.exchange_order_id.is_some()
        {
            match self.gateway.query_fills_for_order(&order_snapshot).await {
                Ok(exchange_fills) => {
                    for exchange_fill in exchange_fills {
                        let fill = Fill {
                            id: FillId(Uuid::new_v4()),
                            order_id: client_order_id,
                            symbol: order_snapshot.symbol.clone(),
                            venue: order_snapshot.venue,
                            side: order_snapshot.side,
                            price: exchange_fill.price,
                            quantity: exchange_fill.quantity,
                            fee: exchange_fill.fee,
                            fee_currency: exchange_fill.fee_currency.clone(),
                            filled_at: exchange_fill.filled_at,
                            // Binance's fill/trade responses don't repeat an
                            // order's leverage, so thread it from the order
                            // snapshot instead -- without this, a leveraged
                            // position opened via run_live_trading would
                            // silently degrade to spot accounting the moment
                            // its fills came back from the exchange.
                            leverage: order_snapshot.leverage,
                            margin_mode: order_snapshot.margin_mode,
                        };
                        let inserted = self
                            .store
                            .insert_fill(&fill, &exchange_fill.exchange_trade_id)
                            .await?;
                        if inserted {
                            new_fills.push(fill);
                        }
                    }
                }
                Err(reason) => {
                    tracing::warn!(
                        order_id = ?client_order_id,
                        reason,
                        "apply_exchange_update: query_fills_for_order failed; fills for this transition may be recovered on a later call"
                    );
                }
            }
        }

        // Keep `filled_quantity` in sync with the fills actually
        // recorded for this transition, mirroring
        // `apexes_backtest::unified_loop::run_unified_event_loop`'s
        // equivalent increment -- without this, `filled_quantity` would stay at
        // its `submit_order`-time value of zero for the entire life of
        // every live-traded order, even after Binance reports
        // PartiallyFilled/Filled, silently misrepresenting the
        // persisted `orders` row to any future consumer of this field.
        if !new_fills.is_empty()
            && let Some(order) = self.open_orders.get_mut(&client_order_id)
        {
            order.filled_quantity += new_fills.iter().map(|fill| fill.quantity).sum::<Decimal>();
            order.updated_at = Utc::now();
            self.store
                .upsert_order(self.open_orders.get(&client_order_id).unwrap())
                .await?;
        }

        if matches!(
            status,
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
        ) {
            self.open_orders.remove(&client_order_id);
        }

        Ok(new_fills)
    }

    /// Reconnect reconciliation, in two passes, both delegating the
    /// actual per-order state application to `apply_exchange_update`
    /// (above):
    ///
    /// **Pass 1** -- for each distinct `asset_class` among locally-open
    /// orders, calls `gateway.query_all_open_orders(asset_class)` (real
    /// exchange APIs scope "list open orders" to one API family, e.g.
    /// Binance's spot vs. futures open-orders endpoints are entirely
    /// separate -- this is why the call happens once per asset class
    /// rather than once for the whole account). For every
    /// `(client_order_id, exchange_order_id, exchange_status)` triple
    /// returned, applies it via `apply_exchange_update`.
    ///
    /// **Pass 2** -- `query_all_open_orders` is documented to report
    /// ONLY non-terminal orders, so it can never surface a `Filled`/
    /// `Cancelled`/`Rejected` transition on its own. For every local
    /// order still `New`/`PartiallyFilled` that pass 1 did NOT report
    /// (and that has a known `exchange_order_id`), this pass calls
    /// `gateway.query_order_status` individually (which DOES report
    /// terminal statuses) and applies the result the same way. A local
    /// order with no `exchange_order_id` yet, not reported by pass 1
    /// either, cannot be resolved via the current `ExecutionGateway`
    /// interface -- logged via `tracing::warn!`, left unchanged.
    ///
    /// Returns one `(OrderId, Vec<Fill>)` entry for every order whose
    /// status actually changed OR that produced new fills, so a caller
    /// (e.g. `apexes-live-trading`'s event loop) can both log/alert on
    /// what changed and apply any new `Fill`s to its own portfolio
    /// tracking.
    pub async fn reconcile(&mut self) -> Result<Vec<(OrderId, Vec<Fill>)>, OmsError> {
        let asset_classes: HashSet<AssetClass> = self
            .open_orders
            .values()
            .map(|order| order.asset_class)
            .collect();

        let mut all_exchange_orders: Vec<(OrderId, Option<String>, OrderStatus)> = Vec::new();
        for asset_class in &asset_classes {
            let exchange_orders = self
                .gateway
                .query_all_open_orders(*asset_class)
                .await
                .map_err(|reason| OmsError::QueryFailed { reason })?;
            all_exchange_orders.extend(exchange_orders);
        }

        let reported_client_ids: HashSet<OrderId> =
            all_exchange_orders.iter().map(|(id, _, _)| *id).collect();

        let mut results: Vec<(OrderId, Vec<Fill>)> = Vec::new();

        // Pass 1: orders the exchange still considers open (or has just
        // transitioned between non-terminal states).
        for (client_order_id, exchange_order_id, exchange_status) in all_exchange_orders {
            let Some(previous_status) = self
                .open_orders
                .get(&client_order_id)
                .map(|order| order.status)
            else {
                tracing::error!(
                    order_id = ?client_order_id,
                    "reconcile: exchange reports an open order we have no local record of"
                );
                continue;
            };

            let new_fills = self
                .apply_exchange_update(client_order_id, exchange_order_id, exchange_status)
                .await?;
            if previous_status != exchange_status || !new_fills.is_empty() {
                results.push((client_order_id, new_fills));
            }
        }

        // Pass 2: local orders still New/PartiallyFilled that the
        // exchange did NOT report as open in pass 1 -- the primary way
        // a Filled/Cancelled/Rejected transition is ever discovered.
        let still_locally_open: Vec<OrderId> = self
            .open_orders
            .iter()
            .filter(|(id, order)| {
                !reported_client_ids.contains(*id)
                    && matches!(
                        order.status,
                        OrderStatus::New | OrderStatus::PartiallyFilled
                    )
            })
            .map(|(id, _)| *id)
            .collect();

        for client_order_id in still_locally_open {
            let Some(order) = self.open_orders.get(&client_order_id) else {
                continue;
            };
            let previous_status = order.status;
            let Some(exchange_order_id) = order.exchange_order_id.clone() else {
                tracing::warn!(
                    order_id = ?client_order_id,
                    "reconcile: local order still open with no known exchange_order_id and not reported as open by the exchange -- cannot resolve"
                );
                continue;
            };
            let order_snapshot = order.clone();

            match self.gateway.query_order_status(&order_snapshot).await {
                Ok(Some(status)) => {
                    let new_fills = self
                        .apply_exchange_update(client_order_id, Some(exchange_order_id), status)
                        .await?;
                    if previous_status != status || !new_fills.is_empty() {
                        results.push((client_order_id, new_fills));
                    }
                }
                Ok(None) => {
                    tracing::error!(
                        order_id = ?client_order_id,
                        exchange_order_id,
                        "reconcile: exchange has no record at all of an order we believe is open"
                    );
                }
                Err(reason) => {
                    tracing::warn!(
                        order_id = ?client_order_id,
                        reason,
                        "reconcile: query_order_status failed for this order; will retry on next reconcile() call"
                    );
                }
            }
        }

        Ok(results)
    }

    pub fn open_orders(&self) -> &HashMap<OrderId, Order> {
        &self.open_orders
    }

    /// Queries the exchange's open orders for each of `asset_classes`
    /// and returns the client order IDs we have NO local record of
    /// (orphans: e.g. placed manually or by another process).
    /// `reconcile()` only logs these and only looks at asset classes
    /// with local open orders, so startup recovery calls this
    /// explicitly to refuse to start on an account it cannot account for.
    pub async fn unknown_exchange_orders(
        &self,
        asset_classes: &[AssetClass],
    ) -> Result<Vec<OrderId>, OmsError> {
        let mut unknown = Vec::new();
        for asset_class in asset_classes {
            let exchange_orders = self
                .gateway
                .query_all_open_orders(*asset_class)
                .await
                .map_err(|reason| OmsError::QueryFailed { reason })?;
            unknown.extend(
                exchange_orders
                    .into_iter()
                    .map(|(id, _, _)| id)
                    .filter(|id| !self.open_orders.contains_key(id)),
            );
        }
        Ok(unknown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_gateway::MockExecutionGateway;
    use apexes_core::{AssetClass, MarginMode, OrderType, Side, Venue};
    use apexes_risk::{PreTradeCheck, RiskViolation};
    use chrono::DateTime;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    const POSTGRES_URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

    /// Production code uses the process-wide global `Registry`
    /// (`apexes_core::registry::global()`); this crate's tests each
    /// construct their OWN isolated `Registry::new()` instead. This is
    /// required, not just a style preference: each test needs its own
    /// fresh `MockExecutionGateway` registered so it can inspect that
    /// specific instance's exchange state afterward -- a shared global
    /// registry would mean only the first test to register ever "wins"
    /// (later `register_or_skip` calls silently no-op, so later tests
    /// would observe the FIRST test's stale gateway instead of their
    /// own), breaking test isolation regardless of `--test-threads`.
    fn registry_with_gateway(gateway: Arc<dyn ExecutionGateway>) -> Registry {
        let registry = Registry::new();
        registry
            .register::<Arc<dyn ExecutionGateway>, ()>((), gateway)
            .unwrap();
        registry
    }

    /// Wraps a `PreTradeCheck` that always rejects, for testing
    /// `submit_order`'s risk-check-failure path without depending on
    /// `apexes-risk`'s concrete checkers.
    struct AlwaysRejectCheck;

    impl PreTradeCheck for AlwaysRejectCheck {
        fn check(
            &self,
            intent: &OrderIntent,
            _positions: &HashMap<String, Position>,
            _balances: &HashMap<String, Decimal>,
            _open_orders: &HashMap<OrderId, Order>,
            _now: DateTime<Utc>,
        ) -> Result<(), Vec<RiskViolation>> {
            Err(vec![RiskViolation::SymbolBlacklisted {
                symbol: intent.symbol.clone(),
            }])
        }
    }

    fn sample_intent() -> OrderIntent {
        OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    /// Deletes all rows from both tables before a test runs, so tests
    /// sharing one live PostgreSQL database don't see leftover state
    /// from earlier tests (e.g. `OrderManager::new`'s
    /// `get_open_orders()` recovery would otherwise pick up unrelated
    /// New-status orders from a previous test run).
    async fn clean_db(store: &OrderStore) {
        // OrderStore has no public "delete everything" method (by
        // design -- production code should never truncate order
        // history). Test-only cleanup goes directly through sqlx.
        // OrderStore does not expose its pool, so this helper
        // reconnects independently for the sole purpose of clearing
        // state; production code never does this.
        let pool = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
        sqlx::query("TRUNCATE fills, orders")
            .execute(&pool)
            .await
            .unwrap();
        let _ = store; // parameter kept for call-site symmetry/clarity
    }

    async fn fresh_manager(gateway: Arc<dyn ExecutionGateway>) -> OrderManager {
        let store = OrderStore::new(POSTGRES_URL).await.unwrap();
        clean_db(&store).await;
        let registry = registry_with_gateway(gateway);
        OrderManager::new(store, &registry).await.unwrap()
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_with_failing_risk_check_creates_no_order() {
        let store = OrderStore::new(POSTGRES_URL).await.unwrap();
        clean_db(&store).await;
        let gateway = Arc::new(MockExecutionGateway::new());
        let registry = registry_with_gateway(gateway);
        registry
            .register::<Arc<CompositeRiskChecker>, ()>(
                (),
                Arc::new(
                    CompositeRiskChecker::new().with_pre_trade_check(Box::new(AlwaysRejectCheck)),
                ),
            )
            .unwrap();
        let mut manager = OrderManager::new(store, &registry).await.unwrap();

        let result = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await;

        assert!(matches!(result, Err(OmsError::RiskCheckFailed { .. })));
        assert!(manager.open_orders().is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_accepted_by_gateway_appears_in_open_orders_with_exchange_id() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(gateway).await;

        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        let order = manager.open_orders().get(&order_id).unwrap();
        assert!(order.exchange_order_id.is_some());
        assert_eq!(order.status, OrderStatus::New);
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_threads_leverage_and_margin_mode_onto_constructed_order() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(gateway).await;
        let mut intent = sample_intent();
        intent.leverage = Some(dec!(10));
        intent.margin_mode = Some(MarginMode::Isolated);

        let order_id = manager
            .submit_order(intent, &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        let order = manager.open_orders().get(&order_id).unwrap();
        assert_eq!(order.leverage, Some(dec!(10)));
        assert_eq!(order.margin_mode, Some(MarginMode::Isolated));
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_rejected_by_gateway_is_not_in_open_orders() {
        let gateway = Arc::new(MockExecutionGateway::new());
        gateway.program_next_submit_outcome(GatewayOutcome::Rejected {
            reason: "insufficient balance".to_string(),
        });
        let mut manager = fresh_manager(gateway).await;

        let result = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await;

        assert!(matches!(result, Err(OmsError::OrderRejected { .. })));
        assert!(manager.open_orders().is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_uncertain_outcome_stays_open_with_no_exchange_id() {
        let gateway = Arc::new(MockExecutionGateway::new());
        gateway.program_next_submit_outcome(GatewayOutcome::Uncertain {
            reason: "connection reset".to_string(),
        });
        let mut manager = fresh_manager(gateway).await;

        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        let order = manager.open_orders().get(&order_id).unwrap();
        assert_eq!(order.exchange_order_id, None);
        assert_eq!(order.status, OrderStatus::New);
    }

    #[tokio::test]
    #[ignore]
    async fn cancel_order_not_found_returns_error() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(gateway).await;

        let result = manager.cancel_order(OrderId(Uuid::new_v4())).await;
        assert!(matches!(result, Err(OmsError::OrderNotFound { .. })));
    }

    #[tokio::test]
    #[ignore]
    async fn cancel_order_with_unknown_exchange_id_is_rejected_without_calling_gateway() {
        let gateway = Arc::new(MockExecutionGateway::new());
        gateway.program_next_submit_outcome(GatewayOutcome::Uncertain {
            reason: "timeout".to_string(),
        });
        let mut manager = fresh_manager(gateway).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        let result = manager.cancel_order(order_id).await;
        assert!(matches!(
            result,
            Err(OmsError::ExchangeOrderIdUnknown { .. })
        ));
    }

    #[tokio::test]
    #[ignore]
    async fn cancel_order_accepted_removes_it_from_open_orders() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(gateway).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        manager.cancel_order(order_id).await.unwrap();

        assert!(manager.open_orders().get(&order_id).is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_pass_one_overwrites_local_status_for_a_reported_open_order() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            manager.open_orders().get(&order_id).unwrap().status,
            OrderStatus::New
        );

        // PartiallyFilled is NON-terminal, so it still appears in
        // query_all_open_orders -- this exercises pass 1.
        gateway.set_exchange_state(order_id, OrderStatus::PartiallyFilled);

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, order_id);
        assert!(changed[0].1.is_empty());
        assert_eq!(
            manager.open_orders().get(&order_id).unwrap().status,
            OrderStatus::PartiallyFilled
        );
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_pass_two_discovers_a_terminal_status_via_query_order_status() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        // Filled is TERMINAL -- query_all_open_orders will no longer
        // report this order (by MockExecutionGateway's own filtering,
        // matching the real ExecutionGateway contract), so pass 1 alone
        // cannot see this. Only pass 2's query_order_status call can.
        gateway.set_exchange_state(order_id, OrderStatus::Filled);

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, order_id);
        assert!(manager.open_orders().get(&order_id).is_none());

        let persisted = manager.store.get_order(order_id).await.unwrap().unwrap();
        assert_eq!(persisted.status, OrderStatus::Filled);
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_resolves_a_previously_uncertain_order_via_pass_one() {
        let gateway = Arc::new(MockExecutionGateway::new());
        gateway.program_next_submit_outcome(GatewayOutcome::Uncertain {
            reason: "timeout".to_string(),
        });
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            manager
                .open_orders()
                .get(&order_id)
                .unwrap()
                .exchange_order_id,
            None
        );

        // The exchange actually accepted the order despite our local
        // Uncertain result, and it is still open (non-terminal) --
        // this is reported via query_all_open_orders (pass 1), which
        // is why force_register_exchange_order (not set_exchange_state,
        // which requires a prior submit_order through this mock) is
        // used here.
        gateway.force_register_exchange_order(
            order_id,
            "EX-999",
            OrderStatus::PartiallyFilled,
            AssetClass::Spot,
        );

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, order_id);
        let order = manager.open_orders().get(&order_id).unwrap();
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.exchange_order_id, Some("EX-999".to_string()));
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_leaves_terminal_orders_unaffected_when_not_reported_by_exchange() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
        manager.cancel_order(order_id).await.unwrap();

        // Cancelled order is terminal and no longer in open_orders;
        // reconcile should not error or resurrect it.
        let changed = manager.reconcile().await.unwrap();
        assert!(!changed.iter().any(|(id, _)| *id == order_id));
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_pass_two_fetches_and_persists_fills_on_fill_transition() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        gateway.set_exchange_state(order_id, OrderStatus::Filled);
        gateway.set_fills_for_order(
            order_id,
            vec![crate::gateway::ExchangeFill {
                exchange_trade_id: "T1".to_string(),
                price: dec!(65000),
                quantity: dec!(1),
                fee: dec!(1.5),
                fee_currency: Some("USDT".to_string()),
                filled_at: Utc::now(),
            }],
        );

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, order_id);
        assert_eq!(changed[0].1.len(), 1);
        assert_eq!(changed[0].1[0].quantity, dec!(1));

        let persisted_fills = manager.store.get_fills_for_order(order_id).await.unwrap();
        assert_eq!(persisted_fills.len(), 1);

        // Regression check for the final whole-branch review's Important
        // finding: filled_quantity must reflect the fill that just
        // transitioned this order to Filled, both in the persisted row
        // and in the in-memory snapshot (the order is removed from
        // open_orders once Filled, so check the persisted row).
        let persisted_order = manager.store.get_order(order_id).await.unwrap().unwrap();
        assert_eq!(persisted_order.filled_quantity, dec!(1));

        gateway.set_exchange_state(order_id, OrderStatus::Filled);
        let changed_again = manager.reconcile().await.unwrap();
        assert!(changed_again.is_empty());
        let persisted_fills_again = manager.store.get_fills_for_order(order_id).await.unwrap();
        assert_eq!(persisted_fills_again.len(), 1);
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_threads_leverage_and_margin_mode_from_order_onto_constructed_fills() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let mut intent = sample_intent();
        intent.leverage = Some(dec!(10));
        intent.margin_mode = Some(MarginMode::Isolated);
        let order_id = manager
            .submit_order(intent, &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        gateway.set_exchange_state(order_id, OrderStatus::Filled);
        gateway.set_fills_for_order(
            order_id,
            vec![crate::gateway::ExchangeFill {
                exchange_trade_id: "T1".to_string(),
                price: dec!(65000),
                quantity: dec!(1),
                fee: dec!(1.5),
                fee_currency: Some("USDT".to_string()),
                filled_at: Utc::now(),
            }],
        );

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].1.len(), 1);
        assert_eq!(changed[0].1[0].leverage, Some(dec!(10)));
        assert_eq!(changed[0].1[0].margin_mode, Some(MarginMode::Isolated));
    }
}
