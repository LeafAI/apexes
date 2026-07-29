//! Apexes backtest engine: tick-level historical replay, Level-2-depth
//! matching simulator, cost model, multi-asset portfolio ledger, and
//! performance/risk-metrics reporting.
//!
//! The [`Strategy`] trait defined in this crate is the shared interface
//! that future live-trading sub-projects (#6 Paper Trading, #7 OMS, #8 live
//! crypto gateway) will implement against unchanged, per the project's
//! non-negotiable backtest/live logic-parity requirement.

pub mod error;

// Populated in Task 3.
pub mod market_data_source;

// Populated in Task 4.
pub mod order_book;

// Populated in Task 5.
pub mod cost_model;

// Populated in Task 6.
pub mod strategy;

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
