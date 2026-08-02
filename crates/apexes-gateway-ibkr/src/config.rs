//! [`IbkrGatewayConfig`]: configuration for
//! [`crate::gateway::IbkrExecutionGateway`]. Default port values per
//! IBKR's publicly documented conventions: TWS paper trading 7497,
//! TWS live 7496, IB Gateway paper trading 4002, IB Gateway live 4001.
//! This crate never reads `std::env` in production code, matching
//! `apexes-gateway-binance`'s established convention.

#[derive(Debug, Clone)]
pub struct IbkrGatewayConfig {
    pub host: String,
    pub port: u16,
    pub client_id: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_is_constructible_and_cloneable() {
        let config = IbkrGatewayConfig {
            host: "127.0.0.1".to_string(),
            port: 7497,
            client_id: 1,
        };
        let cloned = config.clone();
        assert_eq!(config.host, cloned.host);
        assert_eq!(config.port, cloned.port);
        assert_eq!(config.client_id, cloned.client_id);
    }
}
