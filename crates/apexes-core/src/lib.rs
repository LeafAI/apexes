//! Apexes core types: shared `Instrument`, `Order`, `Fill`, `MarketEvent`,
//! and `MarketDataProvider` used across backtest, paper, and live trading.
//!
//! This crate contains only data types and trait definitions — no gateway
//! implementations, no network I/O, no business logic beyond the
//! `OrderStatus` state machine.

pub mod error;
pub mod fill;
pub mod instrument;
pub mod market_data;
pub mod order;

pub use error::ApexesError;
pub use fill::{Fill, FillId};
pub use instrument::{AssetClass, Instrument, MarketType, Venue};
pub use market_data::{
    Bar, BarInterval, DepthUpdate, MarketDataProvider, MarketEvent, PriceLevel, Tick, TradeEvent,
};
pub use order::{Order, OrderId, OrderStatus, OrderType, Side};
