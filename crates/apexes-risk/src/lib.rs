//! Apexes risk engine: pluggable pre-trade (`PreTradeCheck`) and
//! in-trade (`InTradeMonitor`) risk controls, shared across backtest,
//! paper, and live trading per the project's non-negotiable
//! backtest/live logic-parity requirement. Covers `docs/FEATURES.md`
//! rows 6.1 (pre-trade: limits, black/white lists, self-trade
//! prevention) and 6.2 (in-trade: position-limit circuit breaker).

pub mod circuit_breaker;
pub mod composite;
pub mod error;
pub mod in_trade;
pub mod limits;
pub mod lists;
pub mod margin_monitor;
pub mod position_monitor;
pub mod pre_trade;
pub mod self_trade;

pub use circuit_breaker::{
    CircuitBreaker, CircuitBreakerInTradeAdapter, CircuitBreakerPreTradeAdapter,
};
pub use composite::CompositeRiskChecker;
pub use error::{CircuitBreakerScope, RiskViolation};
pub use in_trade::InTradeMonitor;
pub use limits::{LimitChecker, RiskLimits};
pub use lists::{ListMode, SymbolVenueList};
pub use margin_monitor::{
    MarginMonitor, MarginMonitorPositionAdapter, MarginMonitorPreTradeAdapter,
};
pub use position_monitor::PositionMonitor;
pub use pre_trade::PreTradeCheck;
pub use self_trade::SelfTradePreventer;
