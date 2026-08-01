//! [`ChildOrderSink`]: the abstraction `Ems` implementations submit
//! algorithmically-scheduled child orders through, decoupling EMS
//! scheduling logic from where those child orders actually go.
//! `apexes-ems` itself does not depend on `apexes-backtest` (see this
//! crate's `lib.rs` docs for why), so `InMemoryEms` cannot hold an
//! `apexes_backtest::MatchingEngine` directly -- `apexes-backtest`
//! instead implements this trait as a thin adapter over its own
//! `MatchingEngine` (Task 15) and passes that adapter into
//! `InMemoryEms::new`. `PostgresEms` (Task 16) submits child orders via
//! `apexes_oms::OrderManager`, which ALREADY implements this trait's
//! shape closely enough that a small adapter (also Task 16) suffices
//! there too -- `apexes-ems` depends directly on `apexes-oms`, so no
//! cycle exists on that side.

use apexes_core::{AssetClass, Fill, Order, OrderId, Side, Venue};
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::error::EmsError;

/// A single child order an `Ems` implementation wants submitted.
/// Deliberately much narrower than `apexes_strategy::OrderIntent`
/// (`OrderIntent` carries `execution_algo`/`routing`, which never apply
/// recursively to a child order -- a child order is always submitted
/// immediately, at market, to one fixed venue).
#[derive(Debug, Clone, PartialEq)]
pub struct ChildOrderIntent {
    pub symbol: String,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub side: Side,
    pub quantity: Decimal,
}

#[async_trait]
pub trait ChildOrderSink: Send + Sync {
    /// Submits `intent` for immediate execution, returning the
    /// resulting `OrderId` and any `Fill`s that happened synchronously
    /// as part of submission (e.g. a Market order against a live local
    /// order book fills immediately; an order routed through a real
    /// broker/exchange gateway does NOT, and returns an empty `Vec` --
    /// its fills, if any, arrive later through that sink's own
    /// separate fill-reporting mechanism, e.g. `apexes_oms::OrderManager`'s
    /// `apply_exchange_update`/`reconcile()`).
    async fn submit_child_order(
        &self,
        intent: ChildOrderIntent,
    ) -> Result<(OrderId, Vec<Fill>), EmsError>;

    /// Cancels a previously-submitted child order, if still active.
    /// Implementations should treat cancelling an already-terminal
    /// order as a no-op success, not an error (mirrors
    /// `apexes_backtest::MatchingEngine`'s and
    /// `apexes_oms::OrderManager`'s existing idempotent-cancel
    /// conventions where applicable).
    async fn cancel_child_order(&self, order_id: OrderId) -> Result<(), EmsError>;

    /// Returns the current state of a previously-submitted child order,
    /// or `Ok(None)` if this sink has no record of it.
    async fn order_status(&self, order_id: OrderId) -> Result<Option<Order>, EmsError>;
}
