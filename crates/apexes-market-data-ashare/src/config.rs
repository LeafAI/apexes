//! [`EastmoneyConfig`]: configuration for
//! [`crate::eastmoney_provider::EastmoneyMarketDataProvider`]. Never
//! reads `std::env` in production code. The base URL is always
//! caller-supplied, never hardcoded -- this crate was developed and
//! tested against `https://push2.eastmoney.com` (verified during
//! design research to be a genuinely public API requiring no key),
//! but that specific host is documented here, not compiled in as a
//! default.

#[derive(Debug, Clone)]
pub struct EastmoneyConfig {
    /// e.g. "https://push2.eastmoney.com"
    pub base_url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_is_constructible_and_cloneable() {
        let config = EastmoneyConfig {
            base_url: "http://127.0.0.1:8080".to_string(),
        };
        let cloned = config.clone();
        assert_eq!(config.base_url, cloned.base_url);
    }
}
