//! [`OrderManagerChildOrderSink`]: adapts `apexes_oms::OrderManager` to
//! `ChildOrderSink`, so `PostgresEms` can submit child orders through
//! the SAME `OrderManager` a live-trading strategy's direct orders also
//! go through (one shared order book / one shared PostgreSQL `orders`
//! table, not two independent order-management paths).
//!
//! Known, deliberate limitation (documented in the Task 16 plan entry,
//! not silently worked around): `ChildOrderSink::submit_child_order`
//! has no `positions`/`balances` parameters, but
//! legacy risk checks require them. Managed Spot children now use
//! the OMS's fresh wallet, durable owned quantity and shared reservation
//! gate, and must match their already-reserved dispatch quantity exactly.
//! The remaining caller-snapshot-dependent equity floor is not supplied
//! by the EMS adapter; it can refuse children conservatively. A future iteration could
//! thread live position/balance snapshots into `ChildOrderSink`, but
//! that would require either giving `apexes-ems` a
//! `PortfolioLedger`-shaped dependency (a potential dependency cycle,
//! since `apexes-backtest` defines `PortfolioLedger`) or restructuring
//! `ChildOrderSink`'s signature; out of scope here.
//!
//! `submit_child_order` always returns an EMPTY `Vec<Fill>` alongside
//! the `OrderId`: `OrderManager::submit_order` never fills
//! synchronously -- those fills surface later through the caller's own
//! `OrderManager::apply_exchange_update`/`reconcile()` handling, same
//! as any other live order.

use std::sync::Arc;

use apexes_core::{Order, OrderId};
use apexes_oms::OrderManager;
use apexes_strategy::OrderIntent;
use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::child_order_sink::{ChildOrderIntent, ChildOrderSink};
use crate::error::EmsError;

pub struct OrderManagerChildOrderSink {
    order_manager: Arc<Mutex<OrderManager>>,
}

impl OrderManagerChildOrderSink {
    pub fn new(order_manager: Arc<Mutex<OrderManager>>) -> Self {
        Self { order_manager }
    }
}

#[async_trait]
impl ChildOrderSink for OrderManagerChildOrderSink {
    async fn submit_child_order(
        &self,
        intent: ChildOrderIntent,
    ) -> Result<(OrderId, Vec<apexes_core::Fill>), EmsError> {
        let order_intent = OrderIntent {
            symbol: intent.symbol,
            venue: intent.venue,
            asset_class: intent.asset_class,
            side: intent.side,
            order_type: apexes_core::OrderType::Market,
            quantity: intent.quantity,
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: intent.leverage,
            margin_mode: intent.margin_mode,
        };
        let mut order_manager = self.order_manager.lock().await;
        let order_id = order_manager
            .submit_child_order(order_intent)
            .await
            .map_err(|error| EmsError::ChildOrderSubmissionFailed {
                parent_id: apexes_strategy::ParentOrderId(uuid::Uuid::nil()),
                reason: error.to_string(),
            })?;
        // OrderManager's own fills arrive later via
        // apply_exchange_update/reconcile(), never synchronously from
        // submit_order -- always an empty Vec here, by design (see
        // this module's doc comment for why this is correct, not
        // a shortcut).
        Ok((order_id, Vec::new()))
    }

    async fn cancel_child_order(&self, order_id: OrderId) -> Result<(), EmsError> {
        let mut order_manager = self.order_manager.lock().await;
        match order_manager.cancel_order(order_id).await {
            Ok(()) => Ok(()),
            // Matches ChildOrderSink's documented "cancelling an
            // already-terminal order is a no-op success" contract --
            // OrderNotCancellable means the order already reached a
            // terminal state, which is not an error from this trait's
            // perspective.
            Err(apexes_oms::OmsError::OrderNotCancellable { .. }) => Ok(()),
            Err(error) => Err(EmsError::ChildOrderSubmissionFailed {
                parent_id: apexes_strategy::ParentOrderId(uuid::Uuid::nil()),
                reason: error.to_string(),
            }),
        }
    }

    async fn order_status(&self, order_id: OrderId) -> Result<Option<Order>, EmsError> {
        let order_manager = self.order_manager.lock().await;
        Ok(order_manager.open_orders().get(&order_id).cloned())
    }
}
