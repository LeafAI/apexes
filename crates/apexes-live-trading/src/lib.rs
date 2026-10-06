//! Apexes live-trading event loop (sub-project #8): exchange-agnostic
//! orchestration driving `Strategy` -> `OrderManager` against live
//! market data and gateway push/poll updates. Structurally parallel to
//! `apexes-paper::run_paper_trading`, but sources order state from a
//! real `apexes_oms::ExecutionGateway`/`OrderManager` instead of a
//! local `apexes_backtest::MatchingEngine` -- fills are real exchange
//! executions, not simulated ones.
//!
//! This crate depends on `apexes-backtest` only for
//! `apexes_backtest::PortfolioLedger` (a generic "rebuild positions/
//! balances from a stream of `Fill`s" component with no
//! backtest-specific logic) -- reused here as `run_live_trading`'s
//! temporary position/balance tracking, pending sub-project #11
//! (account & fund management), which will supersede it.

pub mod order_executor_adapter;
pub mod runner;
pub mod startup_recovery;

pub use order_executor_adapter::{
    LiveLiquidationPolicy, LiveSupplementalFillSource, OrderManagerExecutor,
};
pub use runner::{LiveTradingConfig, LiveTradingReport, run_live_trading};
pub use startup_recovery::{
    ExchangeSnapshot, RecoveredState, RecoveryError, RecoveryOrders, recover_live_state,
};
