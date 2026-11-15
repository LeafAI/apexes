//! [`PreTradeCheck`]: called after a `Strategy::on_market_event` call
//! produces an `OrderIntent`, before that intent is submitted to a
//! matching engine (backtest) or OMS (future live trading). Every
//! implementation must be side-effect-free with respect to the
//! *decision* it returns -- i.e. calling `check` twice with identical
//! inputs must return identical results -- even though implementations
//! like `CircuitBreaker` do carry internal mutable state that `on_fill`
//! (via `InTradeMonitor`) updates between calls.

use apexes_core::{Order, OrderId};
use apexes_strategy::{OrderIntent, Position};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::error::RiskViolation;

pub trait PreTradeCheck: Send + Sync {
    /// Returns `Ok(())` if `intent` passes this check, or
    /// `Err(violations)` with every rule this single check instance
    /// found violated (a single `PreTradeCheck` implementation may
    /// itself detect more than one violation, e.g. `LimitChecker`
    /// checking both quantity AND notional in one call).
    fn check(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        open_orders: &HashMap<OrderId, Order>,
        now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>>;

    /// Called once when an order that passed every check was actually
    /// accepted for execution. Stateful limits (orders and volume per day)
    /// advance here and ONLY here.
    ///
    /// `check` must not change state: the same checker is consulted by more
    /// than one layer for the same order, and an order the exchange then
    /// rejects never used up any allowance. Counting inside `check` made each
    /// order count twice and let rejected orders exhaust the daily cap.
    fn on_order_accepted(&self, _intent: &OrderIntent, _now: DateTime<Utc>) {}
}
