//! Apexes market data ingestion: real `apexes_core::MarketDataProvider`
//! implementations. This crate wires exchange-specific WebSocket feeds
//! (via `barter-data`) into Apexes's normalized `MarketEvent` model.

pub mod bar_aggregator;
pub mod depth_converter;
pub mod provider;

pub use provider::BinanceMarketDataProvider;
