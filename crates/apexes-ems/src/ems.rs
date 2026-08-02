//! [`Ems`]: submit/query/cancel algorithmically-scheduled "parent"
//! orders. Two implementations: [`crate::in_memory::InMemoryEms`]
//! (backtest/paper) and [`crate::postgres::PostgresEms`] (live
//! trading) -- see this crate's `lib.rs` docs.

use apexes_core::{Fill, MarginMode, MarketEvent, Side, Venue};
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
    /// `None`: spot parent order -- every child order is spot (the
    /// behavior before this field existed). `Some(leverage)`: every
    /// child order this parent is sliced into carries this leverage,
    /// so the resulting child fills open/add to a leveraged perpetual
    /// position instead of a spot position.
    pub leverage: Option<Decimal>,
    /// Only read when `leverage.is_some()`; `None` defaults to
    /// `MarginMode::Isolated` downstream (the safer default -- an
    /// isolated position's liquidation cannot cascade into other
    /// positions).
    pub margin_mode: Option<MarginMode>,
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
    /// caller's event loop, returning every `Fill` that happened
    /// synchronously as a result of any child-order slice submitted
    /// during this call (aggregated across every parent order this
    /// call advanced, in submission order) -- callers apply these
    /// exactly like any other `Fill`s (ledger/tracker/report), the
    /// same way they already apply direct-strategy order fills.
    /// `InMemoryEms` uses this event's timestamp to drive
    /// virtual-clock-paced scheduling (TWAP/VWAP/Iceberg) and
    /// `MarketEvent::Trade.quantity` to drive POV participation, and
    /// returns real fills from its `MatchingEngineChildOrderSink`.
    /// `PostgresEms` ignores everything except `MarketEvent::Trade`
    /// (used for its own POV scheduling only -- TWAP/VWAP/Iceberg are
    /// paced by real background timers there, independent of this
    /// call) and always returns an empty `Vec` -- its `ChildOrderSink`
    /// (`OrderManagerChildOrderSink`) never fills synchronously; those
    /// fills surface later through the caller's own
    /// `OrderManager::apply_exchange_update`/`reconcile()` handling,
    /// same as any other live order. Callers (Task 17) call this once
    /// per event, for every event, regardless of whether any parent
    /// orders are currently scheduling.
    async fn on_market_event(&self, event: &MarketEvent) -> Result<Vec<Fill>, EmsError>;
}
