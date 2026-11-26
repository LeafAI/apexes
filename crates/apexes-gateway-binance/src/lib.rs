//! Real Binance `ExecutionGateway` implementation (sub-project #8):
//! REST order submission/cancellation/querying across Binance Spot and
//! USDS-M Perpetual Futures, request signing, and a User Data Stream
//! WebSocket client for push-based order/fill updates. Implements
//! `apexes_oms::ExecutionGateway` -- see that trait's docs and
//! `docs/superpowers/specs/2026-08-01-apexes-gateway-binance-design.md`
//! for the full design, including why this crate does NOT depend on
//! `barter-execution` (its Binance client module is an empty, private
//! stub as of the version available when this crate was written).

pub mod auth;
pub mod config;
pub mod error;
pub mod gateway;
pub mod rest;
pub mod signing;
pub mod user_data_stream;
pub mod ws_user_stream;

pub use apexes_core::binance_network::{BinanceEndpoints, BinanceNetwork, ParseNetworkError};
pub use config::BinanceGatewayConfig;
pub use error::BinanceApiError;
pub use gateway::{BinanceExecutionGateway, SpotBalance};
pub use rest::{ApiFamily, BinanceRestClient};
pub use user_data_stream::UserDataStreamHandle;
pub use ws_user_stream::SpotStreamConfig;
