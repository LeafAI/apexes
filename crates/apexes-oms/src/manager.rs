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

    /// Reconnect reconciliation: calls
    /// `gateway.query_all_open_orders()`, and for every
    /// `(client_order_id, exchange_order_id, exchange_status)` triple
    /// returned, looks up the matching local order BY `client_order_id`
    /// (our own `OrderId` -- NOT `exchange_order_id`, which may still be
    /// unknown locally for `Uncertain`-outcome orders). If found: sets
    /// `order.exchange_order_id` (populating it if it was `None`) and
    /// OVERWRITES the local status with the exchange's reported status
    /// UNCONDITIONALLY -- bypassing `OrderStatus::can_transition_to`'s
    /// validation entirely, per this design's explicit decision that
    /// the exchange is authoritative during reconciliation. If NOT
    /// found (a `client_order_id` the exchange reports that we have no
    /// local record of at all), logs a `tracing::error!` and skips it.
    /// Local orders NOT present in the exchange's open-order list are
    /// left unchanged if already terminal, or logged as a discrepancy
    /// (`tracing::warn!`) if the local status is still `New`/
    /// `PartiallyFilled`. Every status change is persisted via
    /// `store.upsert_order` and reflected in `open_orders`. Returns the
    /// list of `OrderId`s whose status actually changed.
    pub async fn reconcile(&mut self) -> Result<Vec<OrderId>, OmsError> {
        let exchange_orders = self
            .gateway
            .query_all_open_orders()
            .await
            .map_err(|reason| OmsError::QueryFailed { reason })?;

        let mut changed = Vec::new();
        let exchange_order_ids: std::collections::HashSet<OrderId> =
            exchange_orders.iter().map(|(id, _, _)| *id).collect();

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

        for (local_id, local_order) in self.open_orders.iter() {
            if !exchange_order_ids.contains(local_id)
                && matches!(
                    local_order.status,
                    OrderStatus::New | OrderStatus::PartiallyFilled
                )
            {
                tracing::warn!(
                    order_id = ?local_id,
                    "reconcile: local order still open but exchange no longer reports it as open"
                );
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
