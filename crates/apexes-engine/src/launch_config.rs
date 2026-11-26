//! [`LaunchConfig`]: the JSON configuration `apexes-ui` hands to a
//! spawned `apexes-engine` process (design spec §四.2) -- one process
//! per trading session. Carries everything Task 7's `main.rs` needs to
//! construct whichever of `apexes_backtest::BacktestConfig`/
//! `apexes_paper::PaperTradingConfig`/
//! `apexes_live_trading::LiveTradingConfig` the selected [`SessionMode`]
//! requires.
//!
//! Field-by-field provenance against the three real config structs
//! (`crates/apexes-backtest/src/runner.rs`,
//! `crates/apexes-paper/src/runner.rs`,
//! `crates/apexes-live-trading/src/runner.rs`), re-verified against
//! their current shape as of this task:
//! - `instruments`/`initial_balances`/`margin_tiers`/
//!   `market_condition_config`/`risk_free_return`: present verbatim on
//!   ALL THREE configs -- carried once here, `main.rs` fans them out.
//! - `SessionMode::Backtest.date_range`: `BacktestConfig.date_range`'s
//!   real type is `std::ops::Range<NaiveDate>` (NOT a `(NaiveDate,
//!   NaiveDate)` tuple) -- matched exactly here.
//! - `SessionMode::Backtest.clickhouse_url`: new, added by Task 7 --
//!   `run_backtest` takes `impl HistoricalMarketDataSource`, not a
//!   live stream, so backtest mode never connects to `gateway_url`;
//!   `main.rs` instead constructs one
//!   `apexes_backtest::StorageMarketDataSource` per instrument
//!   directly from `apexes-storage`, which needs its own ClickHouse
//!   URL. This is a real, additive gap fix to Task 6's original shape
//!   (no ClickHouse URL field existed anywhere on `LaunchConfig`
//!   before this task) -- not a breaking change, since no real caller
//!   ever constructed a `SessionMode::Backtest` value before Task 7.
//! - `SessionMode::Live.credentials_ref`/`auto_derisk_live`/
//!   `reconcile_interval`/`ems_postgres_url`: all four map directly to
//!   `LiveTradingConfig` fields of the same name/purpose (the first
//!   three by direct field match; `credentials_ref` itself is NOT a
//!   `LiveTradingConfig` field -- it is resolved by `apexes-engine`'s
//!   own `main.rs` into gateway credentials read from env/local config,
//!   design spec §4.4 -- included here so `main.rs` knows which
//!   credentials to resolve).
//! - F03.01: simulated fees are configurable; live uses actual exchange
//!   commissions. Replay speed, queue factor and latency are unchanged.
//! - `snapshot_min_interval`: new, not present on any of the three
//!   configs -- consumed by Task 7's `main.rs` to construct the
//!   `ControlHandle` (design spec §五), not passed through to any of
//!   the three configs themselves.
//! - `cpu_cores`/`gateway_url`: new, `apexes-engine`-process-level
//!   concerns (CPU affinity; which `apexes-market-data-gateway`
//!   `/market` WS URL to subscribe to) with no equivalent field on any
//!   of the three configs.

use std::collections::HashMap;
use std::ops::Range;
use std::time::Duration;

use apexes_core::{AssetClass, Instrument, Venue};
use apexes_gateway_binance::BinanceNetwork;
use apexes_risk::MarketConditionConfig;
use apexes_strategy::MarginTierTable;
use apexes_strategy_loader::StrategyRef;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::risk_limits::{LiveRiskLimits, RiskLimitsError};

/// Which of the three trading modes this session runs, plus the
/// mode-specific parameters each one needs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionMode {
    /// Drives `apexes_backtest::run_backtest`.
    Backtest {
        /// Matches `BacktestConfig.date_range`'s real type exactly
        /// (`std::ops::Range<NaiveDate>`, not a tuple).
        date_range: Range<NaiveDate>,
        /// ClickHouse URL (e.g. `http://localhost:8123`) `main.rs`
        /// connects `apexes_storage::StorageReader` to, to construct
        /// one `apexes_backtest::StorageMarketDataSource` per
        /// instrument in `LaunchConfig.instruments` (design spec §4.2
        /// re-verification against `run_backtest`'s real signature:
        /// `run_backtest` takes `impl HistoricalMarketDataSource`, NOT
        /// a live stream -- backtest mode reads history directly from
        /// `apexes-storage`, bypassing `LaunchConfig.gateway_url`'s
        /// live gateway entirely). Added by Task 7 -- this field was
        /// genuinely missing from Task 6's original `LaunchConfig`
        /// shape (no ClickHouse URL existed anywhere on this struct),
        /// a real gap rather than an intentional omission, since
        /// nothing before this task exercised `SessionMode::Backtest`
        /// against a real caller.
        clickhouse_url: String,
    },
    /// Drives `apexes_paper::run_paper_trading`.
    Paper,
    /// Drives `apexes_live_trading::run_live_trading`.
    Live {
        /// Which Binance Spot system this session trades on. REQUIRED, no
        /// default: a config that omits it fails to parse, so mainnet can
        /// never be selected by omission. The market-data gateway must be
        /// running on the same network (checked at connect time).
        network: BinanceNetwork,
        /// Pre-trade limits. REQUIRED: a live engine used to run with no
        /// risk checker at all.
        risk_limits: LiveRiskLimits,
        /// A reference (e.g. `"binance-main"`), NOT the credentials
        /// themselves -- credentials never cross a process boundary
        /// over the network. `apexes-engine`'s own `main.rs` resolves
        /// this into actual gateway credentials by reading env vars /
        /// a local config file (design spec §4.4).
        credentials_ref: String,
        /// Maps directly to `LiveTradingConfig.auto_derisk_live`.
        auto_derisk_live: bool,
        /// Maps directly to `LiveTradingConfig.reconcile_interval`.
        #[serde(with = "duration_millis")]
        reconcile_interval: Duration,
        /// Maps directly to `LiveTradingConfig.ems_postgres_url`.
        ems_postgres_url: String,
        /// Engage the kill switch automatically if no market event
        /// arrives for this many seconds. `None` (default) disables.
        #[serde(default)]
        stale_data_timeout_secs: Option<u64>,
    },
}

/// The JSON configuration `apexes-ui` hands to a spawned
/// `apexes-engine` process (design spec §四.2). See this module's doc
/// comment for the full field-by-field provenance against
/// `BacktestConfig`/`PaperTradingConfig`/`LiveTradingConfig`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchConfig {
    /// A caller-generated (by `apexes-ui`, BEFORE spawning this
    /// process) unique identifier for this session, used ONLY to name
    /// this engine process's PID file: `~/.apexes/sessions/<session_id>.pid`
    /// (Task 8, design spec §6.2/§4). Added by Task 8 -- deliberately
    /// NOT engine-generated (e.g. a UUID minted at startup, or derived
    /// from the ephemeral port `/session/ws` happens to bind): the
    /// whole point of a predictable PID-file path is that
    /// `apexes-ui`'s future supervisor (Task 9) needs to know it
    /// BEFORE spawning the process, so it can poll for the file's
    /// existence/read it back without any out-of-band communication
    /// (stdout scraping, a second file, etc.) from the child. Since
    /// `apexes-ui` already constructs the whole `LaunchConfig` before
    /// spawning, having it mint this identifier (e.g.
    /// `uuid::Uuid::new_v4().to_string()`) and pass it through here is
    /// the simplest mechanism that is genuinely predictable by the
    /// caller.
    pub session_id: String,
    pub mode: SessionMode,
    pub strategy_ref: StrategyRef,
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    /// CPU cores to pin this process to (design spec §3.3), if any.
    /// `None` leaves CPU affinity unset (today's implicit behavior for
    /// every process in this workspace).
    pub cpu_cores: Option<Vec<usize>>,
    /// Minimum interval between successive `SessionSnapshot` pushes
    /// over `/session/ws` (design spec §五's `ControlHandle`).
    #[serde(with = "duration_millis")]
    pub snapshot_min_interval: Duration,
    pub margin_tiers: HashMap<String, MarginTierTable>,
    pub market_condition_config: Option<MarketConditionConfig>,
    pub risk_free_return: Decimal,
    /// Optional in legacy JSON: zero rates are explicitly labeled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub simulated_fees: Option<apexes_backtest::SimulatedFees>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub simulated_execution: Option<apexes_backtest::SimulatedExecution>,
    /// `apexes-market-data-gateway`'s `/market` WS URL this engine
    /// subscribes to for live market data (backtest mode ignores this
    /// -- historical replay reads from `apexes-storage` instead).
    pub gateway_url: String,
}

/// Why a [`LaunchConfig`] was refused before any process work started.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum LaunchConfigError {
    #[error(transparent)]
    RiskLimits(#[from] RiskLimitsError),
    #[error("invalid simulated fees: {0}")]
    SimulatedFees(String),
    #[error("invalid simulated execution: {0}")]
    SimulatedExecution(String),
    #[error("live sessions need at least one instrument")]
    NoInstruments,
    #[error(
        "live sessions are Binance Spot only; instrument {symbol} is {venue:?}/{asset_class:?}"
    )]
    NotBinanceSpot {
        symbol: String,
        venue: Venue,
        asset_class: AssetClass,
    },
}

impl LaunchConfig {
    /// Checks the parts of a config that serde cannot: live risk limits are
    /// positive, and every live instrument is a Binance Spot instrument.
    /// Backtest and paper configs are not constrained here.
    pub fn validate(&self) -> Result<(), LaunchConfigError> {
        if let Some(fees) = &self.simulated_fees {
            if matches!(self.mode, SessionMode::Live { .. }) {
                return Err(LaunchConfigError::SimulatedFees(
                    "live sessions use actual exchange fees".into(),
                ));
            }
            fees.validate(&self.instruments)
                .map_err(LaunchConfigError::SimulatedFees)?;
        }
        if let Some(policy) = &self.simulated_execution {
            if matches!(self.mode, SessionMode::Live { .. }) {
                return Err(LaunchConfigError::SimulatedExecution(
                    "live uses actual exchange execution".into(),
                ));
            }
            policy
                .validate(&self.instruments)
                .map_err(LaunchConfigError::SimulatedExecution)?;
        }
        let SessionMode::Live { risk_limits, .. } = &self.mode else {
            return Ok(());
        };
        risk_limits.validate()?;
        if self.instruments.is_empty() {
            return Err(LaunchConfigError::NoInstruments);
        }
        for instrument in &self.instruments {
            if instrument.venue != Venue::Binance || instrument.asset_class != AssetClass::Spot {
                return Err(LaunchConfigError::NotBinanceSpot {
                    symbol: instrument.symbol.clone(),
                    venue: instrument.venue,
                    asset_class: instrument.asset_class,
                });
            }
        }
        Ok(())
    }

    pub fn cost_model(&self) -> apexes_backtest::CostModel {
        self.simulated_fees.clone().unwrap_or_default().cost_model()
    }

    /// What this session is, as shown in the operator console. Built from
    /// the config the engine actually runs, so the banner cannot disagree
    /// with the real network.
    pub fn identity(&self) -> apexes_backtest::SessionIdentity {
        let (mode, network) = match &self.mode {
            SessionMode::Backtest { .. } => ("backtest", None),
            SessionMode::Paper => ("paper", None),
            SessionMode::Live { network, .. } => ("live", Some(network.to_string())),
        };
        let strategy = match &self.strategy_ref {
            StrategyRef::DriftScript { path } => path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| path.display().to_string()),
            StrategyRef::RustNative { name } => name.clone(),
        };
        apexes_backtest::SessionIdentity {
            session_id: self.session_id.clone(),
            network,
            mode: mode.to_string(),
            strategy,
            fee_label: Some(if mode == "live" {
                "Exchange fees".to_string()
            } else {
                let mut model = self.cost_model();
                model.freeze_fees(&self.instruments).map_or_else(
                    |_| "Fees unknown".to_string(),
                    |report| report.label().to_string(),
                )
            }),
        }
    }

    /// Limits the evaluation view reports utilisation against: only live
    /// sessions carry them.
    pub fn limit_spec(&self) -> apexes_backtest::LimitSpec {
        match &self.mode {
            SessionMode::Live { risk_limits, .. } => apexes_backtest::LimitSpec {
                max_position_per_symbol: Some(risk_limits.max_position_per_symbol),
                max_daily_fills: Some(risk_limits.max_daily_trade_count),
            },
            _ => apexes_backtest::LimitSpec {
                max_position_per_symbol: None,
                max_daily_fills: None,
            },
        }
    }

    /// `snapshot_min_interval`'s default per design spec §4.2/§五.
    pub fn default_snapshot_min_interval() -> Duration {
        Duration::from_millis(500)
    }
}

/// Serializes/deserializes a `Duration` as whole milliseconds (a plain
/// JSON number), since `serde`'s own built-in `Duration` impl
/// serializes as a `{secs, nanos}` struct -- a plain millisecond count
/// is simpler for `apexes-ui`'s launcher UI to produce/display.
mod duration_millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(value.as_millis() as u64)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, MarketType, Venue};
    use rust_decimal_macros::dec;
    use std::path::PathBuf;

    fn sample_instrument() -> Instrument {
        Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            Some("BTC".to_string()),
            Some("USDT".to_string()),
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap()
    }

    fn sample_backtest_config() -> LaunchConfig {
        LaunchConfig {
            session_id: "test-session-backtest".to_string(),
            mode: SessionMode::Backtest {
                date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
                    ..NaiveDate::from_ymd_opt(2026, 2, 1).unwrap(),
                clickhouse_url: "http://localhost:8123".to_string(),
            },
            strategy_ref: StrategyRef::DriftScript {
                path: PathBuf::from("strategies/example.drift"),
            },
            instruments: vec![sample_instrument()],
            initial_balances: HashMap::from([("USDT".to_string(), dec!(10000))]),
            cpu_cores: Some(vec![0, 1]),
            snapshot_min_interval: LaunchConfig::default_snapshot_min_interval(),
            margin_tiers: HashMap::new(),
            market_condition_config: None,
            risk_free_return: dec!(0.02),
            simulated_fees: None,
            simulated_execution: None,
            gateway_url: "ws://localhost:9001/market".to_string(),
        }
    }

    fn sample_live_config() -> LaunchConfig {
        LaunchConfig {
            session_id: "test-session-live".to_string(),
            mode: SessionMode::Live {
                network: BinanceNetwork::Testnet,
                risk_limits: crate::risk_limits::LiveRiskLimits {
                    max_order_notional: dec!(100),
                    max_order_quantity: dec!(0.01),
                    max_position_per_symbol: dec!(0.05),
                    max_daily_trade_count: 50,
                    min_account_equity: None,
                    max_daily_volume: None,
                },
                credentials_ref: "binance-main".to_string(),
                auto_derisk_live: false,
                reconcile_interval: Duration::from_secs(30),
                ems_postgres_url: "postgres://localhost/apexes_ems".to_string(),
                stale_data_timeout_secs: Some(20),
            },
            strategy_ref: StrategyRef::RustNative {
                name: "example".to_string(),
            },
            instruments: vec![sample_instrument()],
            initial_balances: HashMap::from([("USDT".to_string(), dec!(10000))]),
            cpu_cores: None,
            snapshot_min_interval: LaunchConfig::default_snapshot_min_interval(),
            margin_tiers: HashMap::new(),
            market_condition_config: None,
            risk_free_return: dec!(0.02),
            simulated_fees: None,
            simulated_execution: None,
            gateway_url: "ws://localhost:9001/market".to_string(),
        }
    }

    /// `deploy/launch-config.example.json` is what the container
    /// deployment mounts; keep it parseable as the types evolve.
    #[test]
    fn deploy_example_launch_config_parses_as_a_live_session() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../deploy/launch-config.example.json"
        );
        let json = std::fs::read_to_string(path).unwrap();
        let config: LaunchConfig = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            config.mode,
            SessionMode::Live {
                network: BinanceNetwork::Testnet,
                ..
            }
        ));
        config
            .validate()
            .expect("the shipped example must validate");
        assert!(matches!(
            config.strategy_ref,
            StrategyRef::DriftScript { .. }
        ));
        assert!(!config.instruments.is_empty());
    }

    #[test]
    fn bounded_execution_config_round_trips_and_live_refuses_it() {
        use apexes_backtest::SimulatedExecution;
        let mut config = sample_backtest_config();
        let old: LaunchConfig =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        assert!(old.simulated_execution.is_none());
        let policy = SimulatedExecution::BoundedSpotIoc {
            rules: std::collections::BTreeMap::from([(
                "BTCUSDT".into(),
                apexes_core::SymbolRules {
                    quantity_step: dec!(0.001),
                    min_quantity: dec!(0.001),
                    max_quantity: dec!(100),
                    price_tick: dec!(0.01),
                    min_notional: dec!(5),
                },
            )]),
            max_reference_age_ms: 500,
        };
        config.simulated_execution = Some(policy.clone());
        let roundtrip: LaunchConfig =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        assert_eq!(roundtrip.simulated_execution, Some(policy.clone()));
        roundtrip.validate().unwrap();
        config.instruments[0].venue = Venue::Okx;
        assert!(config.validate().is_err());
        let mut live = sample_live_config();
        live.simulated_execution = Some(policy);
        assert!(matches!(
            live.validate(),
            Err(LaunchConfigError::SimulatedExecution(_))
        ));
    }

    #[test]
    fn simulated_fee_config_validates_round_trips_and_labels_legacy_zero() {
        use apexes_backtest::{FillLiquidity, SimulatedFees};
        let mut config = sample_backtest_config();
        let legacy: LaunchConfig =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        assert!(legacy.simulated_fees.is_none());
        assert_eq!(legacy.identity().fee_label.as_deref(), Some("ZERO FEES"));
        config.simulated_fees = Some(SimulatedFees::Flat {
            maker_bps: dec!(5),
            taker_bps: dec!(10),
        });
        let roundtrip: LaunchConfig =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        roundtrip.validate().unwrap();
        assert_eq!(roundtrip.simulated_fees, config.simulated_fees);
        assert_eq!(
            roundtrip.identity().fee_label.as_deref(),
            Some("Modeled fees")
        );
        assert_eq!(
            roundtrip
                .cost_model()
                .fee_for_fill(Venue::Binance, FillLiquidity::Maker, dec!(1000)),
            dec!(0.5)
        );
        assert_eq!(
            roundtrip
                .cost_model()
                .fee_for_fill(Venue::Binance, FillLiquidity::Taker, dec!(1000)),
            dec!(1)
        );
        for bad in [
            SimulatedFees::Flat {
                maker_bps: dec!(-1),
                taker_bps: dec!(10),
            },
            SimulatedFees::Flat {
                maker_bps: dec!(1),
                taker_bps: dec!(10000),
            },
            SimulatedFees::BinanceSpotVip { tier: 10 },
        ] {
            config.simulated_fees = Some(bad);
            assert!(config.validate().is_err());
        }
        config.simulated_fees = Some(SimulatedFees::BinanceSpotVip { tier: 4 });
        config.validate().unwrap();
        assert_eq!(
            config
                .cost_model()
                .fee_for_fill(Venue::Binance, FillLiquidity::Taker, dec!(10000)),
            dec!(5.2)
        );
        config.instruments[0].venue = Venue::Okx;
        assert!(config.validate().is_err());
    }

    #[test]
    fn live_configuration_cannot_override_authoritative_fees() {
        let mut config = sample_live_config();
        assert_eq!(
            config.identity().fee_label.as_deref(),
            Some("Exchange fees")
        );
        config.simulated_fees = Some(apexes_backtest::SimulatedFees::Zero);
        assert!(matches!(
            config.validate(),
            Err(LaunchConfigError::SimulatedFees(_))
        ));
    }

    #[test]
    fn backtest_launch_config_round_trips_through_json() {
        let config = sample_backtest_config();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: LaunchConfig = serde_json::from_str(&json).unwrap();

        match deserialized.mode {
            SessionMode::Backtest {
                date_range,
                clickhouse_url,
            } => {
                assert_eq!(date_range, config_date_range(&config));
                assert_eq!(clickhouse_url, "http://localhost:8123");
            }
            _ => panic!("expected SessionMode::Backtest"),
        }
        assert_eq!(deserialized.gateway_url, config.gateway_url);
        assert_eq!(
            deserialized.snapshot_min_interval,
            config.snapshot_min_interval
        );
    }

    #[test]
    fn live_launch_config_round_trips_through_json() {
        let config = sample_live_config();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: LaunchConfig = serde_json::from_str(&json).unwrap();

        match deserialized.mode {
            SessionMode::Live {
                network,
                risk_limits,
                credentials_ref,
                auto_derisk_live,
                reconcile_interval,
                ems_postgres_url,
                stale_data_timeout_secs,
            } => {
                assert_eq!(network, BinanceNetwork::Testnet);
                assert_eq!(risk_limits.max_daily_trade_count, 50);
                assert_eq!(stale_data_timeout_secs, Some(20));
                assert_eq!(credentials_ref, "binance-main");
                assert!(!auto_derisk_live);
                assert_eq!(reconcile_interval, Duration::from_secs(30));
                assert_eq!(ems_postgres_url, "postgres://localhost/apexes_ems");
            }
            _ => panic!("expected SessionMode::Live"),
        }
    }

    fn live_json_without(field: &str) -> String {
        let mut value = serde_json::to_value(sample_live_config()).unwrap();
        let live = value["mode"]["Live"].as_object_mut().unwrap();
        assert!(
            live.remove(field).is_some(),
            "{field} not present to remove"
        );
        value.to_string()
    }

    #[test]
    fn a_live_config_without_network_does_not_parse() {
        let error = serde_json::from_str::<LaunchConfig>(&live_json_without("network"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("network"), "{error}");
    }

    #[test]
    fn a_live_config_without_risk_limits_does_not_parse() {
        let error = serde_json::from_str::<LaunchConfig>(&live_json_without("risk_limits"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("risk_limits"), "{error}");
    }

    #[test]
    fn a_valid_live_config_validates() {
        assert!(sample_live_config().validate().is_ok());
    }

    #[test]
    fn validation_rejects_zero_risk_limits() {
        let mut config = sample_live_config();
        if let SessionMode::Live { risk_limits, .. } = &mut config.mode {
            risk_limits.max_order_notional = dec!(0);
        }
        assert!(matches!(
            config.validate(),
            Err(LaunchConfigError::RiskLimits(_))
        ));
    }

    #[test]
    fn validation_rejects_non_spot_and_empty_instrument_lists() {
        let mut config = sample_live_config();
        config.instruments.clear();
        assert_eq!(config.validate(), Err(LaunchConfigError::NoInstruments));

        let mut config = sample_live_config();
        config.instruments = vec![
            Instrument::new(
                "BTCUSDT",
                MarketType::Crypto,
                Venue::Binance,
                AssetClass::Perpetual,
                Some("BTC".to_string()),
                Some("USDT".to_string()),
                dec!(0.01),
                dec!(0.0001),
                dec!(1),
            )
            .unwrap(),
        ];
        assert!(matches!(
            config.validate(),
            Err(LaunchConfigError::NotBinanceSpot { .. })
        ));
    }

    #[test]
    fn identity_reports_the_live_network_and_strategy_file_name() {
        let mut config = sample_live_config();
        config.strategy_ref = StrategyRef::DriftScript {
            path: PathBuf::from("/etc/apexes/strategy.drift"),
        };
        let identity = config.identity();
        assert_eq!(identity.network.as_deref(), Some("testnet"));
        assert_eq!(identity.mode, "live");
        assert_eq!(identity.strategy, "strategy.drift");
        assert_eq!(identity.session_id, "test-session-live");
    }

    #[test]
    fn identity_has_no_network_for_sessions_that_touch_no_exchange() {
        assert_eq!(sample_backtest_config().identity().network, None);
        assert_eq!(sample_backtest_config().identity().mode, "backtest");
    }

    #[test]
    fn limit_spec_comes_from_live_risk_limits_only() {
        let live = sample_live_config().limit_spec();
        assert_eq!(live.max_position_per_symbol, Some(dec!(0.05)));
        assert_eq!(live.max_daily_fills, Some(50));
        assert_eq!(sample_backtest_config().limit_spec().max_daily_fills, None);
    }

    #[test]
    fn backtest_configs_are_not_constrained_by_live_validation() {
        assert!(sample_backtest_config().validate().is_ok());
    }

    fn config_date_range(config: &LaunchConfig) -> Range<NaiveDate> {
        match &config.mode {
            SessionMode::Backtest { date_range, .. } => date_range.clone(),
            _ => panic!("expected SessionMode::Backtest"),
        }
    }
}
