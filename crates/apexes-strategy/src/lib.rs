//! Apexes strategy SDK: the shared `Strategy` trait interface (implemented
//! identically by the backtest engine and all future live-trading
//! sub-projects, per the project's non-negotiable backtest/live logic-parity
//! requirement), plus `DriftScriptStrategy`, a `Strategy` implementation
//! that runs a user-authored Drift Script (`.drift`) file as the strategy's
//! decision logic.

pub mod strategy;

pub use strategy::{OrderIntent, Position, Strategy, StrategyContext};
