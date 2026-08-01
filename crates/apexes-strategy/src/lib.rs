//! Apexes strategy SDK: the shared `Strategy` trait interface (implemented
//! identically by the backtest engine and all future live-trading
//! sub-projects, per the project's non-negotiable backtest/live logic-parity
//! requirement), plus `DriftScriptStrategy`, a `Strategy` implementation
//! that runs a user-authored Drift Script (`.drift`) file as the strategy's
//! decision logic.

pub mod drift_dsl;
pub mod error;
pub mod strategy;
pub mod venue_scorer;

pub use error::StrategyError;
pub use strategy::{
    ExecutionAlgoSpec, OrderIntent, OrderRouting, ParentOrderId, ParentOrderStatus, Position,
    Strategy, StrategyContext,
};
pub use venue_scorer::{PriceAndFeeScorer, VenueMarketState, VenueScorer};
