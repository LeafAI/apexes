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

pub trait PreTradeCheck: Send {
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
}
