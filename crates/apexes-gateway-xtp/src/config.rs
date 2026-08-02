//! [`XtpGatewayConfig`]: configuration for [`crate::gateway::XtpExecutionGateway`].
//! Field names are best-effort inferred from the publicly-available
//! `ztsec/xtp_api_java` documentation -- account_key in particular is
//! a placeholder name that may not match XTP's actual credential
//! terminology once real account access is available. This crate
//! never reads `std::env` in production code, matching
//! `apexes-gateway-binance`'s established convention.

#[derive(Debug, Clone)]
pub struct XtpGatewayConfig {
    pub client_id: u8,
    pub account_key: String,
    pub trade_server_ip: String,
    pub trade_server_port: u16,
    pub quote_server_ip: String,
    pub quote_server_port: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_is_constructible_and_cloneable() {
        let config = XtpGatewayConfig {
            client_id: 1,
            account_key: "test-key".to_string(),
            trade_server_ip: "127.0.0.1".to_string(),
            trade_server_port: 6001,
            quote_server_ip: "127.0.0.1".to_string(),
            quote_server_port: 6002,
        };
        let cloned = config.clone();
        assert_eq!(config.client_id, cloned.client_id);
        assert_eq!(config.account_key, cloned.account_key);
    }
}
