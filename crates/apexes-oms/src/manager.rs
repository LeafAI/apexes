//! [`OrderManager`]: the business-logic layer -- risk checks, gateway
//! submission, status tracking, and reconnect reconciliation. Holds an
//! in-memory `HashMap<OrderId, Order>` snapshot of open orders (mirroring
//! `apexes_backtest::PortfolioLedger`'s in-memory-plus-persistence
//! pattern), rebuilt from `OrderStore::get_open_orders` on construction
//! so a process restart recovers its view of in-flight orders without
//! waiting for the next `reconcile()` call.

use std::collections::HashMap;
use std::sync::Arc;

use apexes_core::{Order, OrderId, OrderStatus};
use apexes_risk::PreTradeCheck;
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
    risk_checker: Option<Box<dyn PreTradeCheck>>,
    open_orders: HashMap<OrderId, Order>,
}

impl OrderManager {
    /// Constructs a new manager, rebuilding `open_orders` from
    /// `store.get_open_orders()` -- the "recover in-flight order state
    /// after a process restart" half of reconnect reconciliation;
    /// `reconcile()` is the other half (comparing that recovered state
    /// against the exchange's current truth).
    pub async fn new(
        store: OrderStore,
        gateway: Arc<dyn ExecutionGateway>,
        risk_checker: Option<Box<dyn PreTradeCheck>>,
    ) -> Result<Self, OmsError> {
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

        let exchange_order_id = order
            .exchange_order_id
            .clone()
            .ok_or(OmsError::ExchangeOrderIdUnknown { order_id })?;

        match self.gateway.cancel_order(&exchange_order_id).await {
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

    /// Reconnect reconciliation, in two passes:
    ///
    /// **Pass 1** -- calls `gateway.query_all_open_orders()`, and for
    /// every `(client_order_id, exchange_order_id, exchange_status)`
    /// triple returned, looks up the matching local order BY
    /// `client_order_id` (our own `OrderId` -- NOT `exchange_order_id`,
    /// which may still be unknown locally for `Uncertain`-outcome
    /// orders). If found: sets `order.exchange_order_id` (populating it
    /// if it was `None`) and OVERWRITES the local status with the
    /// exchange's reported status UNCONDITIONALLY -- bypassing
    /// `OrderStatus::can_transition_to`'s validation entirely, per this
    /// design's explicit decision that the exchange is authoritative
    /// during reconciliation. If NOT found (a `client_order_id` the
    /// exchange reports that we have no local record of at all), logs a
    /// `tracing::error!` and skips it.
    ///
    /// **Pass 2** -- `query_all_open_orders` is documented to report
    /// ONLY non-terminal orders, so it can never surface a `Filled`/
    /// `Cancelled`/`Rejected` transition on its own. For every local
    /// order still `New`/`PartiallyFilled` that pass 1 did NOT see
    /// (and that has a known `exchange_order_id`), this pass calls
    /// `gateway.query_order_status` individually (which DOES report
    /// terminal statuses) and applies the same unconditional-overwrite
    /// treatment. A local order with no `exchange_order_id` yet (an
    /// unresolved `Uncertain` submission) and not reported by pass 1
    /// either cannot be resolved via the current `ExecutionGateway`
    /// interface -- logged via `tracing::warn!`, left unchanged.
    ///
    /// Every status change (either pass) is persisted via
    /// `store.upsert_order` and reflected in `open_orders`. Returns the
    /// list of `OrderId`s whose status actually changed.
    pub async fn reconcile(&mut self) -> Result<Vec<OrderId>, OmsError> {
        let exchange_orders = self
            .gateway
            .query_all_open_orders()
            .await
            .map_err(|reason| OmsError::QueryFailed { reason })?;

        let mut changed = Vec::new();
        let reported_client_ids: std::collections::HashSet<OrderId> =
            exchange_orders.iter().map(|(id, _, _)| *id).collect();

        // First pass: orders the exchange still considers open (or has
        // just transitioned between non-terminal states). Matched by
        // OUR client order ID, never by exchange_order_id -- see
        // ExecutionGateway::query_all_open_orders's docs for why.
        for (client_order_id, exchange_order_id, exchange_status) in exchange_orders {
            match self.open_orders.get_mut(&client_order_id) {
                Some(order) => {
                    let status_changed = order.status != exchange_status;
                    if exchange_order_id.is_some() {
                        order.exchange_order_id = exchange_order_id;
                    }
                    if status_changed {
                        order.status = exchange_status;
                        order.updated_at = Utc::now();
                        self.store.upsert_order(order).await?;
                        changed.push(client_order_id);
                    }
                }
                None => {
                    tracing::error!(
                        order_id = ?client_order_id,
                        "reconcile: exchange reports an open order we have no local record of"
                    );
                }
            }
        }

        // Second pass: local orders still New/PartiallyFilled that the
        // exchange did NOT report as open in the first pass. This is
        // NOT an edge case -- it is the primary way a Filled/Cancelled/
        // Rejected transition is ever discovered, since
        // query_all_open_orders structurally never reports terminal
        // orders. For each such local order that HAS an
        // exchange_order_id, query it individually (query_order_status
        // DOES return terminal statuses).
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
            let exchange_order_id = self
                .open_orders
                .get(&client_order_id)
                .and_then(|order| order.exchange_order_id.clone());

            let Some(exchange_order_id) = exchange_order_id else {
                // Uncertain submission that was never accepted into the
                // exchange's open-order list either -- nothing to query
                // by yet. Log and move on; this specific combination
                // (Uncertain + never became visible as open + went
                // straight to a terminal state) cannot be resolved with
                // the current ExecutionGateway interface.
                tracing::warn!(
                    order_id = ?client_order_id,
                    "reconcile: local order still open with no known exchange_order_id and not reported as open by the exchange -- cannot resolve"
                );
                continue;
            };

            match self.gateway.query_order_status(&exchange_order_id).await {
                Ok(Some(status)) => {
                    if let Some(order) = self.open_orders.get_mut(&client_order_id)
                        && order.status != status
                    {
                        order.status = status;
                        order.updated_at = Utc::now();
                        self.store.upsert_order(order).await?;
                        changed.push(client_order_id);
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

        // Remove any orders that reconciliation moved to a terminal
        // status from the open-orders map (mirroring submit_order/
        // cancel_order's own bookkeeping).
        self.open_orders.retain(|_, order| {
            matches!(
                order.status,
                OrderStatus::New | OrderStatus::PartiallyFilled
            )
        });

        Ok(changed)
    }

    pub fn open_orders(&self) -> &HashMap<OrderId, Order> {
        &self.open_orders
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_gateway::MockExecutionGateway;
    use apexes_core::{AssetClass, OrderType, Side, Venue};
    use apexes_risk::RiskViolation;
    use chrono::DateTime;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    const POSTGRES_URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

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
        OrderManager::new(store, gateway, None).await.unwrap()
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_with_failing_risk_check_creates_no_order() {
        let store = OrderStore::new(POSTGRES_URL).await.unwrap();
        clean_db(&store).await;
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = OrderManager::new(store, gateway, Some(Box::new(AlwaysRejectCheck)))
            .await
            .unwrap();

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

        assert_eq!(changed, vec![order_id]);
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

        assert_eq!(changed, vec![order_id]);
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
        gateway.force_register_exchange_order(order_id, "EX-999", OrderStatus::PartiallyFilled);

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed, vec![order_id]);
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
        assert!(!changed.contains(&order_id));
    }
}
