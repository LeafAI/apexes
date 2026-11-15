//! [`LaunchRequest`]: what an operator asks for, validated before any
//! process is started, and turned into the engine's [`LaunchConfig`].

use std::collections::HashMap;
use std::time::Duration;

use apexes_core::binance_network::BinanceNetwork;
use apexes_core::{AssetClass, Instrument, MarketType, Venue};
use apexes_engine::LiveRiskLimits;
use apexes_engine::launch_config::{LaunchConfig, SessionMode};
use apexes_strategy_loader::StrategyRef;
use rust_decimal::Decimal;

use crate::error::RequestError;
use crate::paths::SessionId;

/// The word an operator must type to start a mainnet session.
pub const MAINNET_CONFIRMATION: &str = "MAINNET";

/// Quote currencies the market-data feed can split a symbol into.
const SUPPORTED_QUOTES: [&str; 3] = ["USDT", "USDC", "BUSD"];

#[derive(Debug, Clone)]
pub struct LaunchRequest {
    pub network: BinanceNetwork,
    /// Must equal [`MAINNET_CONFIRMATION`] when `network` is mainnet;
    /// ignored for testnet.
    pub mainnet_confirmation: String,
    /// A name, not a secret: resolved to `APEXES_CRED_<REF>_<NETWORK>_*`.
    pub credentials_ref: String,
    pub strategy: StrategyRef,
    /// Binance Spot symbols such as `BTCUSDT`.
    pub symbols: Vec<String>,
    pub risk_limits: LiveRiskLimits,
    pub ems_postgres_url: String,
    pub stale_data_timeout_secs: Option<u64>,
    pub snapshot_min_interval: Duration,
    /// `None`: generate one (`testnet-1a2b3c4d`).
    pub session_id: Option<String>,
}

/// A request that passed validation, with its resolved session id.
#[derive(Debug, Clone)]
pub struct ValidRequest {
    pub request: LaunchRequest,
    pub session_id: SessionId,
}

impl LaunchRequest {
    pub fn validate(self) -> Result<ValidRequest, RequestError> {
        if self.network == BinanceNetwork::Mainnet
            && self.mainnet_confirmation.trim() != MAINNET_CONFIRMATION
        {
            return Err(RequestError::MainnetNotConfirmed);
        }
        if self.symbols.is_empty() {
            return Err(RequestError::NoInstruments);
        }
        for symbol in &self.symbols {
            check_symbol(symbol)?;
        }
        check_credentials_ref(&self.credentials_ref)?;
        let postgres = self.ems_postgres_url.trim();
        if !(postgres.starts_with("postgres://") || postgres.starts_with("postgresql://")) {
            return Err(RequestError::BadPostgresUrl);
        }
        let session_id = match &self.session_id {
            Some(id) => SessionId::parse(id)?,
            None => SessionId::generate(self.network),
        };
        self.risk_limits
            .validate()
            .map_err(|error| RequestError::Config(error.to_string()))?;
        Ok(ValidRequest {
            request: self,
            session_id,
        })
    }
}

fn check_symbol(symbol: &str) -> Result<(), RequestError> {
    let well_formed = (5..=20).contains(&symbol.len())
        && symbol
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit());
    if !well_formed {
        return Err(RequestError::BadSymbol {
            symbol: symbol.to_string(),
        });
    }
    split_symbol(symbol)
        .map(|_| ())
        .ok_or_else(|| RequestError::UnsupportedQuote {
            symbol: symbol.to_string(),
        })
}

fn check_credentials_ref(value: &str) -> Result<(), RequestError> {
    let ok = !value.is_empty()
        && value.len() <= 40
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if ok {
        Ok(())
    } else {
        Err(RequestError::BadCredentialsRef {
            value: value.to_string(),
        })
    }
}

/// `BTCUSDT` -> `("BTC", "USDT")`.
pub fn split_symbol(symbol: &str) -> Option<(String, String)> {
    SUPPORTED_QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}

/// Builds a Binance Spot [`Instrument`]. The tick and lot sizes are
/// placeholders (as in the gateway): nothing in the live path reads them,
/// and the real values come from the exchange when an order is sent.
pub fn spot_instrument(symbol: &str) -> Result<Instrument, RequestError> {
    let (base, quote) = split_symbol(symbol).ok_or_else(|| RequestError::UnsupportedQuote {
        symbol: symbol.to_string(),
    })?;
    Instrument::new(
        symbol,
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some(base),
        Some(quote),
        Decimal::new(1, 2),
        Decimal::new(1, 4),
        Decimal::ONE,
    )
    .map_err(|error| RequestError::Config(format!("{symbol}: {error}")))
}

impl ValidRequest {
    /// The engine's launch config. It carries no secret: the credentials
    /// reference is a name the engine resolves from its own environment.
    pub fn launch_config(&self, gateway_port: u16) -> Result<LaunchConfig, RequestError> {
        let request = &self.request;
        let instruments = request
            .symbols
            .iter()
            .map(|symbol| spot_instrument(symbol))
            .collect::<Result<Vec<_>, _>>()?;
        let config = LaunchConfig {
            session_id: self.session_id.to_string(),
            mode: SessionMode::Live {
                network: request.network,
                risk_limits: request.risk_limits.clone(),
                credentials_ref: request.credentials_ref.clone(),
                auto_derisk_live: false,
                reconcile_interval: Duration::from_secs(30),
                ems_postgres_url: request.ems_postgres_url.trim().to_string(),
                stale_data_timeout_secs: request.stale_data_timeout_secs,
            },
            strategy_ref: request.strategy.clone(),
            instruments,
            initial_balances: HashMap::new(),
            cpu_cores: None,
            snapshot_min_interval: request.snapshot_min_interval,
            margin_tiers: HashMap::new(),
            market_condition_config: None,
            risk_free_return: Decimal::ZERO,
            gateway_url: format!("ws://127.0.0.1:{gateway_port}/market"),
        };
        config
            .validate()
            .map_err(|error| RequestError::Config(error.to_string()))?;
        Ok(config)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::path::PathBuf;

    pub(crate) fn request(network: BinanceNetwork) -> LaunchRequest {
        LaunchRequest {
            network,
            mainnet_confirmation: String::new(),
            credentials_ref: "binance-main".to_string(),
            strategy: StrategyRef::DriftScript {
                path: PathBuf::from("/s/strategy.drift"),
            },
            symbols: vec!["BTCUSDT".to_string()],
            risk_limits: LiveRiskLimits {
                max_order_notional: dec!(200),
                max_order_quantity: dec!(0.002),
                max_position_per_symbol: dec!(0.005),
                max_daily_trade_count: 50,
                min_account_equity: None,
                max_daily_volume: None,
            },
            ems_postgres_url: "postgres://apexes:apexes@localhost:5432/apexes".to_string(),
            stale_data_timeout_secs: Some(30),
            snapshot_min_interval: Duration::from_millis(500),
            session_id: Some("s-1".to_string()),
        }
    }

    #[test]
    fn a_complete_testnet_request_validates_without_any_confirmation() {
        assert!(request(BinanceNetwork::Testnet).validate().is_ok());
    }

    #[test]
    fn mainnet_needs_the_exact_word_typed() {
        for wrong in ["", "mainnet", "Mainnet", "yes", "MAINNET!"] {
            let mut r = request(BinanceNetwork::Mainnet);
            r.mainnet_confirmation = wrong.to_string();
            assert_eq!(
                r.validate().unwrap_err(),
                RequestError::MainnetNotConfirmed,
                "{wrong:?}"
            );
        }
        let mut r = request(BinanceNetwork::Mainnet);
        r.mainnet_confirmation = "MAINNET".to_string();
        assert!(r.validate().is_ok());
        let mut r = request(BinanceNetwork::Mainnet);
        r.mainnet_confirmation = "  MAINNET  ".to_string();
        assert!(
            r.validate().is_ok(),
            "surrounding spaces from a text box are tolerated"
        );
    }

    #[test]
    fn symbols_are_checked() {
        let mut r = request(BinanceNetwork::Testnet);
        r.symbols = vec![];
        assert_eq!(r.validate().unwrap_err(), RequestError::NoInstruments);
        for bad in ["btcusdt", "BTC-USDT", "BTC", "BTCUSDT; rm -rf", "../X"] {
            let mut r = request(BinanceNetwork::Testnet);
            r.symbols = vec![bad.to_string()];
            assert!(
                matches!(r.validate().unwrap_err(), RequestError::BadSymbol { .. }),
                "{bad}"
            );
        }
        let mut r = request(BinanceNetwork::Testnet);
        r.symbols = vec!["BTCEUR".to_string()];
        assert!(matches!(
            r.validate().unwrap_err(),
            RequestError::UnsupportedQuote { .. }
        ));
        let mut r = request(BinanceNetwork::Testnet);
        r.symbols = vec!["USDT".to_string()]; // no base
        assert!(r.validate().is_err());
    }

    #[test]
    fn credentials_refs_and_postgres_urls_are_checked() {
        for bad in ["", "Has Space", "UPPER", "a/b", "a_b", &"x".repeat(41)] {
            let mut r = request(BinanceNetwork::Testnet);
            r.credentials_ref = bad.to_string();
            assert!(
                matches!(
                    r.validate().unwrap_err(),
                    RequestError::BadCredentialsRef { .. }
                ),
                "{bad:?}"
            );
        }
        let mut r = request(BinanceNetwork::Testnet);
        r.ems_postgres_url = "mysql://x".to_string();
        assert_eq!(r.validate().unwrap_err(), RequestError::BadPostgresUrl);
    }

    #[test]
    fn invalid_risk_limits_are_refused_before_any_process_work() {
        let mut r = request(BinanceNetwork::Testnet);
        r.risk_limits.max_order_quantity = dec!(0);
        assert!(
            matches!(r.validate().unwrap_err(), RequestError::Config(m) if m.contains("max_order_quantity"))
        );
    }

    #[test]
    fn an_unsafe_session_id_is_refused_and_a_missing_one_is_generated() {
        let mut r = request(BinanceNetwork::Testnet);
        r.session_id = Some("../x".to_string());
        assert!(matches!(
            r.validate().unwrap_err(),
            RequestError::BadSessionId { .. }
        ));
        let mut r = request(BinanceNetwork::Testnet);
        r.session_id = None;
        assert!(
            r.validate()
                .unwrap()
                .session_id
                .as_str()
                .starts_with("testnet-")
        );
    }

    #[test]
    fn the_launch_config_is_live_on_the_requested_network_and_carries_no_secret() {
        let valid = request(BinanceNetwork::Testnet).validate().unwrap();
        let config = valid.launch_config(9742).unwrap();
        assert_eq!(config.gateway_url, "ws://127.0.0.1:9742/market");
        assert_eq!(config.session_id, "s-1");
        match &config.mode {
            SessionMode::Live {
                network,
                credentials_ref,
                auto_derisk_live,
                ..
            } => {
                assert_eq!(*network, BinanceNetwork::Testnet);
                assert_eq!(credentials_ref, "binance-main");
                assert!(!auto_derisk_live);
            }
            other => panic!("expected live, got {other:?}"),
        }
        assert_eq!(config.instruments.len(), 1);
        assert_eq!(config.instruments[0].asset_class, AssetClass::Spot);
        assert_eq!(config.instruments[0].venue, Venue::Binance);
        let json = serde_json::to_string(&config).unwrap();
        assert!(
            !json.to_lowercase().contains("secret") && !json.to_lowercase().contains("api_key"),
            "{json}"
        );
        // And it round-trips through the engine's own parser + validation.
        let parsed: LaunchConfig = serde_json::from_str(&json).unwrap();
        parsed.validate().unwrap();
    }

    #[test]
    fn split_symbol_handles_each_supported_quote() {
        assert_eq!(
            split_symbol("ETHUSDC"),
            Some(("ETH".to_string(), "USDC".to_string()))
        );
        assert_eq!(
            split_symbol("BTCBUSD"),
            Some(("BTC".to_string(), "BUSD".to_string()))
        );
        assert_eq!(split_symbol("BTCEUR"), None);
    }
}
