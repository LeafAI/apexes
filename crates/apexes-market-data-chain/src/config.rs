//! [`EthRpcConfig`]: configuration for [`crate::eth_rpc_provider::EthRpcProvider`].
//! Never reads `std::env` in production code, matching
//! `apexes-gateway-binance`'s established convention -- the host/port
//! are always caller-supplied, never a hardcoded default pointing at
//! any specific provider (including `publicnode.com`, which is
//! documented in this crate's `lib.rs` as the endpoint this was
//! developed and tested against, but never hardcoded as a default).

#[derive(Debug, Clone)]
pub struct EthRpcConfig {
    /// e.g. "https://ethereum-rpc.publicnode.com"
    pub http_url: String,
    /// e.g. "wss://ethereum-rpc.publicnode.com"
    pub ws_url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_is_constructible_and_cloneable() {
        let config = EthRpcConfig {
            http_url: "http://127.0.0.1:8545".to_string(),
            ws_url: "ws://127.0.0.1:8546".to_string(),
        };
        let cloned = config.clone();
        assert_eq!(config.http_url, cloned.http_url);
        assert_eq!(config.ws_url, cloned.ws_url);
    }
}
