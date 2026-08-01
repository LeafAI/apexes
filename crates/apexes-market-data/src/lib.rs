//! Apexes market data ingestion: real `apexes_core::MarketDataProvider`
//! implementations. This crate wires exchange-specific WebSocket feeds
//! (via `barter-data`) into Apexes's normalized `MarketEvent` model.

pub mod bar_aggregator;
pub mod depth_converter;
pub mod depth_task;
pub mod ingestion;
pub mod mark_price_task;
pub mod provider;
pub mod symbol;
pub mod ticker_task;
pub mod trade_task;

pub use provider::BinanceMarketDataProvider;
