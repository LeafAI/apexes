//! [`BinanceGatewayConfig`]: configuration for [`crate::gateway::BinanceExecutionGateway`].
//! Deliberately has no `Default` impl and no environment-variable or
//! config-file loading -- per this sub-project's design decision, the
//! caller (test code today; a future CLI/UI tool in a later
//! sub-project) constructs this explicitly. This crate never reads
//! `std::env` in production code.

/// Configuration for [`crate::gateway::BinanceExecutionGateway`].
///
/// `spot_base_url`/`futures_base_url` are overridable (not hardcoded to
/// `https://api.binance.com`/`https://fapi.binance.com`) so tests can
/// point them at a local `wiremock` server or Binance's public testnet.
#[derive(Debug, Clone)]
pub struct BinanceGatewayConfig {
    pub api_key: String,
    pub api_secret: String,
    pub spot_base_url: String,
    pub futures_base_url: String,
    pub recv_window_ms: u64,
}
