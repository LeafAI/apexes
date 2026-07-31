//! [`ExecutionGateway`]: the abstraction `OrderManager` submits/cancels/
//! queries orders through, decoupling order-lifecycle management from
//! any specific exchange's API. Sub-project #8 (first live gateway)
//! implements this trait against a real exchange; this crate ships only
//! [`crate::mock_gateway::MockExecutionGateway`] for testing.

use apexes_core::{Order, OrderId, OrderStatus};
use async_trait::async_trait;

/// The outcome of a `submit_order`/`cancel_order` call, distinguishing
/// three cases `OrderManager` must handle differently:
/// - `Accepted`: the exchange confirmed the action.
/// - `Rejected`: the exchange explicitly refused (e.g. insufficient
///   balance, invalid parameters) -- authoritative, no ambiguity.
/// - `Uncertain`: the call failed in a way that does NOT confirm
///   whether the exchange actually processed it (e.g. network timeout,
///   connection dropped mid-request) -- the order may have gone through
///   despite the local failure. `OrderManager` must NOT assume failure
///   in this case; it leaves the order's status unchanged and defers to
///   a later `reconcile()` call to discover the true state.
#[derive(Debug, Clone, PartialEq)]
pub enum GatewayOutcome {
    Accepted { exchange_order_id: String },
    Rejected { reason: String },
    Uncertain { reason: String },
}

#[async_trait]
pub trait ExecutionGateway: Send + Sync {
    /// Submits `order` (already persisted locally with status `New` by
    /// the caller before this is invoked -- see
    /// `OrderManager::submit_order`) to the exchange. Implementations
    /// MUST pass `order.id` to the exchange as a client order ID
    /// (alongside whatever exchange-native order ID the exchange
    /// generates), so `query_all_open_orders` can later report it back
    /// -- this is what lets `reconcile()` match exchange reports to
    /// local orders even when `exchange_order_id` was never learned
    /// (an `Uncertain` outcome from this call).
    async fn submit_order(&self, order: &Order) -> GatewayOutcome;

    /// Cancels an already-submitted order, identified by its
    /// `exchange_order_id` (the caller -- `OrderManager` -- guarantees
    /// this is `Some` before calling, since `OrderManager::cancel_order`
    /// refuses to call this method for orders where it's still `None`).
    async fn cancel_order(&self, exchange_order_id: &str) -> GatewayOutcome;

    /// Queries the exchange's current authoritative status for a single
    /// order. Returns `Ok(None)` if the exchange has no record of this
    /// order ID at all (distinct from "query failed" -- that's `Err`).
    async fn query_order_status(
        &self,
        exchange_order_id: &str,
    ) -> Result<Option<OrderStatus>, String>;

    /// Queries the exchange for every currently open (not Filled/
    /// Cancelled/Rejected) order across the whole account -- used by
    /// `OrderManager::reconcile` after a reconnect.
    ///
    /// Returns `(client_order_id, exchange_order_id, status)` triples.
    /// `client_order_id` is OUR locally-assigned `OrderId` (`Order.id`),
    /// not the exchange's own identifier -- see `submit_order`'s docs
    /// for why implementations must echo it back here.
    async fn query_all_open_orders(
        &self,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String>;
}
