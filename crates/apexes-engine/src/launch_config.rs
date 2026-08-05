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
//! - `cost_model`/`speed`/`queue_position_factor` (`BacktestConfig`),
//!   `cost_model`/`latency_model` (`PaperTradingConfig`), `cost_model`
//!   (`LiveTradingConfig`): deliberately NOT carried in `LaunchConfig`.
//!   These are engine-internal simulation-fidelity knobs, not
//!   session-launch parameters a UI user picks -- Task 7's `main.rs`
//!   constructs sensible fixed defaults for these directly, matching
//!   this brief's own field list (which also omits them).
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

use apexes_core::Instrument;
use apexes_risk::MarketConditionConfig;
use apexes_strategy::MarginTierTable;
use apexes_strategy_loader::StrategyRef;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

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
    },
}

/// The JSON configuration `apexes-ui` hands to a spawned
/// `apexes-engine` process (design spec §四.2). See this module's doc
/// comment for the full field-by-field provenance against
/// `BacktestConfig`/`PaperTradingConfig`/`LiveTradingConfig`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchConfig {
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
    /// `apexes-market-data-gateway`'s `/market` WS URL this engine
    /// subscribes to for live market data (backtest mode ignores this
    /// -- historical replay reads from `apexes-storage` instead).
    pub gateway_url: String,
}

impl LaunchConfig {
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
            gateway_url: "ws://localhost:9001/market".to_string(),
        }
    }

    fn sample_live_config() -> LaunchConfig {
        LaunchConfig {
            mode: SessionMode::Live {
                credentials_ref: "binance-main".to_string(),
                auto_derisk_live: false,
                reconcile_interval: Duration::from_secs(30),
                ems_postgres_url: "postgres://localhost/apexes_ems".to_string(),
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
            gateway_url: "ws://localhost:9001/market".to_string(),
        }
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
                credentials_ref,
                auto_derisk_live,
                reconcile_interval,
                ems_postgres_url,
            } => {
                assert_eq!(credentials_ref, "binance-main");
                assert!(!auto_derisk_live);
                assert_eq!(reconcile_interval, Duration::from_secs(30));
                assert_eq!(ems_postgres_url, "postgres://localhost/apexes_ems");
            }
            _ => panic!("expected SessionMode::Live"),
        }
    }

    fn config_date_range(config: &LaunchConfig) -> Range<NaiveDate> {
        match &config.mode {
            SessionMode::Backtest { date_range, .. } => date_range.clone(),
            _ => panic!("expected SessionMode::Backtest"),
        }
    }
}
