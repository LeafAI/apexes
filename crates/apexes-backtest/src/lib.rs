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

pub mod ems_adapter;

pub mod order_executor_adapter;

// Populated in Task 3.
pub mod market_data_source;

// Populated in Task 4.
pub mod order_book;

// Populated in Task 5.
pub mod cost_model;

pub mod latency;
pub mod mark_price_source;

// Populated in Task 7.
pub mod ledger;

// Populated in Tasks 8-11 (walk_book, MatchingEngine core, limit orders, Stop/StopLimit).
pub mod matching_engine;

// Populated in Task 12.
pub mod clock;

// Populated in Task 13.
pub mod performance;

// Populated in Task 14.
pub mod report;
pub mod runner;

pub mod unified_loop;

pub use apexes_strategy::{OrderIntent, OrderRouting, Position, Strategy, StrategyContext};
pub use clock::ReplaySpeed;
pub use cost_model::{
    BinanceSpotVipFeeSchedule, CostModel, FeeSchedule, FillLiquidity, FlatFeeSchedule,
};
pub use error::ApexesError;
pub use latency::LatencyModel;
pub use ledger::PortfolioLedger;
pub use mark_price_source::StorageReaderMarkPriceProvider;
pub use market_data_source::{
    HistoricalMarketDataSource, InMemoryMarketDataSource, StorageMarketDataSource,
};
pub use matching_engine::MatchingEngine;
pub use performance::{PerformanceSummary, PerformanceTracker};
pub use report::BacktestReport;
pub use runner::{BacktestConfig, run_backtest};
