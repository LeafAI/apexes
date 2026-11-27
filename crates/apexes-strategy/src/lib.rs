//! Apexes strategy SDK: the shared `Strategy` trait interface (implemented
//! identically by the backtest engine and all future live-trading
//! sub-projects, per the project's non-negotiable backtest/live logic-parity
//! requirement), plus `DriftScriptStrategy`, a `Strategy` implementation
//! that runs a user-authored Drift Script (`.drift`) file as the strategy's
//! decision logic.

pub mod drift_dsl;
pub mod error;
pub mod margin_tier;
pub mod mark_price_provider;
pub mod order_executor;
pub mod strategy;
pub mod venue_scorer;

pub use apexes_core::MarginMode;
pub use error::StrategyError;
pub use margin_tier::{MarginTier, MarginTierTable, PositionMarginStatus};
pub use mark_price_provider::MarkPriceProvider;
pub use order_executor::{
    LiquidationPolicy, LiquidationReason, OrderExecutor, SupplementalFillSource,
};
pub use strategy::{
    ExecutionAlgoSpec, OrderIntent, OrderRouting, ParentOrderId, ParentOrderStatus, Position,
    Strategy, StrategyContext,
};
pub use venue_scorer::{PriceAndFeeScorer, VenueMarketState, VenueScorer};

pub mod symbol_state;

pub mod event_time;

pub mod context_market;
pub use context_market::InstrumentContext;

pub mod book_context;
pub use book_context::{BookIntegrity, BookObservation};

pub mod cost_context;
pub use cost_context::{CostObservation, FeeRole};
