//! [`Ems`]: submit/query/cancel algorithmically-scheduled "parent"
//! orders. Two implementations: [`crate::in_memory::InMemoryEms`]
//! (backtest/paper) and [`crate::postgres::PostgresEms`] (live
//! trading) -- see this crate's `lib.rs` docs.

use apexes_core::{MarketEvent, Side, Venue};
use apexes_strategy::{ExecutionAlgoSpec, OrderRouting, ParentOrderId, ParentOrderStatus};
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::error::EmsError;

/// A request to schedule an algorithmically-paced "parent" order.
/// Constructed by a caller (`run_event_loop`/`run_live_trading`, Task
/// 17) from an `OrderIntent` whose `execution_algo` is `Some(..)`.
#[derive(Debug, Clone, PartialEq)]
pub struct AlgoOrderRequest {
    pub symbol: String,
    pub venue: Venue,
    pub side: Side,
    pub quantity: Decimal,
    pub execution_algo: ExecutionAlgoSpec,
    pub routing: OrderRouting,
}

#[async_trait]
pub trait Ems: Send + Sync {
    /// Schedules `request`, returning a `ParentOrderId` immediately;
    /// actual child-order submission happens asynchronously afterward,
    /// paced per `request.execution_algo` (see each implementation's
    /// own docs for its pacing mechanism).
    async fn submit_algo_order(&self, request: AlgoOrderRequest)
    -> Result<ParentOrderId, EmsError>;

    /// Returns the current aggregate status of a parent order, or
    /// `Ok(None)` if unknown to this `Ems` instance.
    async fn parent_order_status(
        &self,
        id: ParentOrderId,
    ) -> Result<Option<ParentOrderStatus>, EmsError>;

    /// Cancels a parent order: stops scheduling any not-yet-submitted
    /// slices, and cancels any still-active child orders already
    /// submitted (via the implementation's own `ChildOrderSink`/
    /// `OrderManager`).
    async fn cancel_parent_order(&self, id: ParentOrderId) -> Result<(), EmsError>;

    /// Notifies this `Ems` of a new `MarketEvent` observed by the
    /// caller's event loop. `InMemoryEms` uses this event's timestamp
    /// to drive virtual-clock-paced scheduling (TWAP/VWAP/Iceberg) and
    /// `MarketEvent::Trade.quantity` to drive POV participation.
    /// `PostgresEms` ignores everything except `MarketEvent::Trade`
    /// (used for its own POV scheduling only -- TWAP/VWAP/Iceberg are
    /// paced by real background timers there, independent of this
    /// call). Callers (Task 17) call this once per event, for every
    /// event, regardless of whether any parent orders are currently
    /// scheduling.
    async fn on_market_event(&self, event: &MarketEvent) -> Result<(), EmsError>;
}
