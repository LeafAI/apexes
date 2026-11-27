//! Apexes backtest engine: tick-level historical replay, Level-2-depth
//! matching simulator, cost model, multi-asset portfolio ledger, and
//! performance/risk-metrics reporting.
//!
//! The [`Strategy`] trait defined in this crate is the shared interface
//! that future live-trading sub-projects (#6 Paper Trading, #7 OMS, #8 live
//! crypto gateway) will implement against unchanged, per the project's
//! non-negotiable backtest/live logic-parity requirement.
//!
//! [`unified_loop`] holds the single shared core `run_backtest`,
//! `apexes-paper`'s `run_paper_trading` and `apexes-live-trading`'s
//! `run_live_trading` all drive -- it is `pub` (so those crates can
//! reach it) but deliberately not re-exported here; `run_backtest`
//! remains this crate's primary public entry point.

pub mod error;

pub mod collector_receipts;
pub mod collector_replay;
pub mod collector_segments;
pub mod control;
mod diagnostics;
pub mod evaluation;
pub mod historical_dataset;
pub mod history;
pub mod order_log;
pub mod source_coverage;
pub mod source_identity;

pub mod ems_adapter;

pub mod order_executor_adapter;

// Populated in Task 3.
pub mod market_data_source;

// Populated in Task 4.
pub mod order_book;

// Populated in Task 5.
pub mod cost_model;

pub mod depth_policy;
pub mod depth_ranges;
pub mod execution_policy;
pub use depth_policy::{DepthSequence, SimulatedDepthPolicy};
pub mod event_latency;
pub mod latency;
pub use event_latency::{ChildLatencyReport, EventLatencyPolicy, EventLatencyReport};
pub use execution_policy::SimulatedExecution;
pub mod mark_price_source;

// Populated in Task 7.
pub mod ledger;
pub mod spot_accounting;

// Populated in Tasks 8-11 (walk_book, MatchingEngine core, limit orders, Stop/StopLimit).
pub mod matching_engine;
pub mod queue_calibration;

// Populated in Task 12.
pub mod clock;

// Populated in Task 13.
pub mod performance;

// Populated in Task 14.
pub mod replay_identity;
pub mod report;
pub mod runner;

pub mod unified_loop;

pub use apexes_strategy::{OrderIntent, OrderRouting, Position, Strategy, StrategyContext};
pub use clock::ReplaySpeed;
pub use control::{ControlHandle, OpenOrderView, SessionCommand, SessionIdentity, SessionSnapshot};
pub use cost_model::{
    BinanceSpotVipFeeSchedule, CostModel, FeeRates, FeeSchedule, FillLiquidity, FlatFeeSchedule,
    SimulatedFeeReport, SimulatedFees,
};
pub use error::ApexesError;
pub use evaluation::{EvaluationSummary, EvaluationTracker, LimitSpec, LimitUsage};
pub use latency::LatencyModel;
pub use ledger::PortfolioLedger;
pub use mark_price_source::StorageReaderMarkPriceProvider;
pub use market_data_source::{
    HistoricalMarketDataSource, InMemoryMarketDataSource, StorageMarketDataSource,
};
pub use matching_engine::MatchingEngine;
pub use order_log::{OrderLog, OrderRecord};
pub use performance::{PerformanceSummary, PerformanceTracker};
pub use report::BacktestReport;
pub use runner::{BacktestConfig, run_backtest};

mod stable_order;

pub mod execution_performance;

pub mod benchmark;
pub mod drawdown_duration;

pub mod funding_attribution;
