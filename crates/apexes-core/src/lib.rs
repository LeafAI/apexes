//! Apexes core types: shared `Instrument`, `Order`, `Fill`, `MarketEvent`,
//! and `MarketDataProvider` used across backtest, paper, and live trading.
//!
//! This crate contains only data types and trait definitions — no gateway
//! implementations, no network I/O, no business logic beyond the
//! `OrderStatus` state machine.

pub mod error;
pub mod instrument;
pub mod order;

pub use error::ApexesError;
pub use instrument::{AssetClass, Instrument, MarketType, Venue};
pub use order::OrderStatus;
