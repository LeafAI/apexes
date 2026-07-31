//! Apexes risk engine: pluggable pre-trade (`PreTradeCheck`) and
//! in-trade (`InTradeMonitor`) risk controls, shared across backtest,
//! paper, and live trading per the project's non-negotiable
//! backtest/live logic-parity requirement. Covers `docs/FEATURES.md`
//! rows 6.1 (pre-trade: limits, black/white lists, self-trade
//! prevention) and 6.2 (in-trade: position-limit circuit breaker).

pub mod error;
pub mod in_trade;
pub mod pre_trade;

pub use error::{CircuitBreakerScope, RiskViolation};
pub use in_trade::InTradeMonitor;
pub use pre_trade::PreTradeCheck;
