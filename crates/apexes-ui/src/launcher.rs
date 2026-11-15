//! Session launcher: the form an operator fills in, and the rules that turn
//! it into a validated [`LaunchRequest`]. No rendering and no I/O, so every
//! rule is unit-testable; `crate::launcher_view` draws it.
//!
//! Defaults are chosen for safety: Testnet, small order and position caps,
//! and mainnet needs the word `MAINNET` typed.

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use apexes_core::binance_network::BinanceNetwork;
use apexes_engine::LiveRiskLimits;
use apexes_strategy_loader::StrategyRef;
use apexes_supervisor::{LaunchRequest, MAINNET_CONFIRMATION, SessionInfo};
use rust_decimal::Decimal;

/// Smallest order value Binance Spot accepts (the NOTIONAL filter), in the
/// quote currency. Most USDT pairs use 5; a pair that needs more is rejected
/// by the exchange with a clear message.
pub const EXCHANGE_MIN_ORDER_VALUE: Decimal = Decimal::from_parts(5, 0, 0, false, 0);

/// Order store for launcher-started sessions (see the field default).
pub const LIVE_DATABASE_URL: &str = "postgres://apexes:apexes@localhost:5432/apexes_live";

/// How long without any market data before the engine halts trading itself.
/// The Binance testnet is a quiet, flaky feed (silences of a minute or more
/// and abrupt disconnects were observed), so a mainnet-style 30 s would halt
/// healthy testnet sessions; mainnet is a busy feed where 30 s is already a
/// long silence.
pub const STALE_DATA_SECS_TESTNET: u64 = 90;
pub const STALE_DATA_SECS_MAINNET: u64 = 30;

fn default_stale_secs(network: BinanceNetwork) -> u64 {
    match network {
        BinanceNetwork::Testnet => STALE_DATA_SECS_TESTNET,
        BinanceNetwork::Mainnet => STALE_DATA_SECS_MAINNET,
    }
}

/// A problem with one field, shown next to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldProblem {
    pub field: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LauncherForm {
    pub network: BinanceNetwork,
    /// Only consulted for mainnet.
    pub mainnet_confirmation: String,
    pub credentials_ref: String,
    /// Directory scanned for `*.drift` strategies.
    pub strategies_dir: String,
    pub strategy_path: String,
    /// Comma-separated, e.g. `BTCUSDT, ETHUSDT`.
    pub symbols: String,
    pub max_order_notional: String,
    pub max_order_quantity: String,
    pub max_position_per_symbol: String,
    pub max_daily_trade_count: String,
    pub stale_data_timeout_secs: String,
    pub postgres_url: String,
    /// Blank: generated.
    pub session_id: String,
}

impl Default for LauncherForm {
    fn default() -> Self {
        Self {
            network: BinanceNetwork::Testnet,
            mainnet_confirmation: String::new(),
            credentials_ref: "binance-main".to_string(),
            strategies_dir: "deploy".to_string(),
            strategy_path: "deploy/strategy.example.drift".to_string(),
            symbols: "BTCUSDT".to_string(),
            // The value cap is what protects across symbols; the base-unit caps
            // are deliberately generous so one default suits BTC and ETH alike.
            max_order_notional: "100".to_string(),
            max_order_quantity: "1".to_string(),
            max_position_per_symbol: "1".to_string(),
            max_daily_trade_count: "50".to_string(),
            stale_data_timeout_secs: STALE_DATA_SECS_TESTNET.to_string(),
            // A database of its own: the test suites TRUNCATE `apexes` and leave
            // mock orders in it, which a live engine would adopt as its own
            // open orders. `scripts/run-testnet.sh` creates this database.
            postgres_url: LIVE_DATABASE_URL.to_string(),
            session_id: String::new(),
        }
    }
}

fn decimal(field: &'static str, text: &str, problems: &mut Vec<FieldProblem>) -> Decimal {
    match Decimal::from_str(text.trim()) {
        Ok(value) => value,
        Err(_) => {
            problems.push(FieldProblem {
                field,
                message: format!("{text:?} is not a number"),
            });
            Decimal::ZERO
        }
    }
}

fn whole(field: &'static str, text: &str, problems: &mut Vec<FieldProblem>) -> u64 {
    match text.trim().parse::<u64>() {
        Ok(value) => value,
        Err(_) => {
            problems.push(FieldProblem {
                field,
                message: format!("{text:?} is not a whole number"),
            });
            0
        }
    }
}

impl LauncherForm {
    /// Switching network clears the typed mainnet confirmation, so a word
    /// typed earlier can never carry over to a later, different launch.
    pub fn set_network(&mut self, network: BinanceNetwork) {
        if network != self.network {
            // Follow the network's default only if the operator has not
            // chosen their own value.
            if self.stale_data_timeout_secs == default_stale_secs(self.network).to_string() {
                self.stale_data_timeout_secs = default_stale_secs(network).to_string();
            }
            self.network = network;
            self.mainnet_confirmation.clear();
        }
    }

    pub fn symbol_list(&self) -> Vec<String> {
        self.symbols
            .split(',')
            .map(|s| s.trim().to_uppercase())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Builds the request, or lists every problem found (not just the
    /// first). The supervisor validates again before starting anything.
    pub fn to_request(&self) -> Result<LaunchRequest, Vec<FieldProblem>> {
        let mut problems = Vec::new();
        let max_order_notional = decimal(
            "max_order_notional",
            &self.max_order_notional,
            &mut problems,
        );
        let max_order_quantity = decimal(
            "max_order_quantity",
            &self.max_order_quantity,
            &mut problems,
        );
        let max_position_per_symbol = decimal(
            "max_position_per_symbol",
            &self.max_position_per_symbol,
            &mut problems,
        );
        let daily = whole(
            "max_daily_trade_count",
            &self.max_daily_trade_count,
            &mut problems,
        );
        let stale = whole(
            "stale_data_timeout_secs",
            &self.stale_data_timeout_secs,
            &mut problems,
        );

        if self.strategy_path.trim().is_empty() {
            problems.push(FieldProblem {
                field: "strategy_path",
                message: "choose a strategy file".to_string(),
            });
        }
        if self.network == BinanceNetwork::Mainnet
            && self.mainnet_confirmation.trim() != MAINNET_CONFIRMATION
        {
            problems.push(FieldProblem {
                field: "mainnet_confirmation",
                message: format!("type {MAINNET_CONFIRMATION} to trade real funds"),
            });
        }
        if stale == 0
            && !problems
                .iter()
                .any(|p| p.field == "stale_data_timeout_secs")
        {
            problems.push(FieldProblem {
                field: "stale_data_timeout_secs",
                message: "must be at least 1 second (the watchdog halts trading when data stops)"
                    .to_string(),
            });
        }

        // Binance refuses orders worth less than its minimum (5 USDT on Spot).
        // A value cap below that means every order is refused: say so here
        // instead of letting a session start and reject all of its orders.
        if max_order_notional > Decimal::ZERO && max_order_notional < EXCHANGE_MIN_ORDER_VALUE {
            problems.push(FieldProblem {
                field: "max_order_notional",
                message: format!(
                    "below the exchange's minimum order value ({EXCHANGE_MIN_ORDER_VALUE} USDT): every order would be refused"
                ),
            });
        }

        let risk_limits = LiveRiskLimits {
            max_order_notional,
            max_order_quantity,
            max_position_per_symbol,
            max_daily_trade_count: u32::try_from(daily).unwrap_or(u32::MAX),
            min_account_equity: None,
            max_daily_volume: None,
        };
        if problems.is_empty()
            && let Err(error) = risk_limits.validate()
        {
            problems.push(FieldProblem {
                field: "risk_limits",
                message: error.to_string(),
            });
        }
        if !problems.is_empty() {
            return Err(problems);
        }

        let session_id = self.session_id.trim();
        Ok(LaunchRequest {
            network: self.network,
            mainnet_confirmation: self.mainnet_confirmation.clone(),
            credentials_ref: self.credentials_ref.trim().to_string(),
            strategy: StrategyRef::DriftScript {
                path: PathBuf::from(self.strategy_path.trim()),
            },
            symbols: self.symbol_list(),
            risk_limits,
            ems_postgres_url: self.postgres_url.trim().to_string(),
            stale_data_timeout_secs: Some(stale),
            snapshot_min_interval: Duration::from_millis(500),
            session_id: (!session_id.is_empty()).then(|| session_id.to_string()),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum LaunchStatus {
    #[default]
    Idle,
    /// A launch is in progress (it can take a minute: the engine reconciles
    /// with the exchange before it is considered started).
    Launching,
    Failed(String),
    Launched {
        session_id: String,
    },
}

#[derive(Debug, Default)]
pub struct LauncherState {
    pub form: LauncherForm,
    pub status: LaunchStatus,
    pub sessions: Vec<SessionInfo>,
    /// Name of a credential variable the supervisor is missing for the
    /// selected network, or `None` when both are present.
    pub missing_credential: Option<String>,
    /// Strategy files found in `form.strategies_dir`.
    pub available_strategies: Vec<PathBuf>,
    pub notice: Option<String>,
}

impl LauncherState {
    pub fn busy(&self) -> bool {
        self.status == LaunchStatus::Launching
    }

    /// Whether the Launch button may be pressed.
    pub fn can_launch(&self) -> bool {
        !self.busy() && self.missing_credential.is_none() && self.form.to_request().is_ok()
    }

    /// Why Launch is disabled, for the tooltip. `None` when it is enabled.
    pub fn launch_blocker(&self) -> Option<String> {
        if self.busy() {
            return Some("a launch is already in progress".to_string());
        }
        if let Some(var) = &self.missing_credential {
            return Some(format!(
                "set {var} (and its _SECRET) in the environment this app runs in"
            ));
        }
        self.form
            .to_request()
            .err()
            .and_then(|problems| problems.into_iter().next())
            .map(|p| p.message)
    }

    pub fn refresh_strategies(&mut self) {
        let dir = PathBuf::from(self.form.strategies_dir.trim());
        self.available_strategies = apexes_strategy_loader::list_available_strategies(&dir)
            .into_iter()
            .filter_map(|r| match r {
                StrategyRef::DriftScript { path } => Some(path),
                StrategyRef::RustNative { .. } => None,
            })
            .collect();
        self.available_strategies.sort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn problem_fields(form: &LauncherForm) -> Vec<&'static str> {
        form.to_request()
            .err()
            .map(|p| p.into_iter().map(|p| p.field).collect())
            .unwrap_or_default()
    }

    #[test]
    fn the_defaults_are_testnet_small_and_valid() {
        let form = LauncherForm::default();
        assert_eq!(form.network, BinanceNetwork::Testnet);
        let request = form.to_request().expect("defaults must be launchable");
        assert_eq!(request.network, BinanceNetwork::Testnet);
        assert_eq!(request.symbols, vec!["BTCUSDT".to_string()]);
        assert_eq!(request.risk_limits.max_order_notional, dec!(100));
        assert_eq!(request.risk_limits.max_order_quantity, dec!(1));
        assert_eq!(request.stale_data_timeout_secs, Some(90));
        assert!(request.session_id.is_none(), "generated by the supervisor");
    }

    #[test]
    fn mainnet_cannot_be_launched_without_the_typed_word() {
        let mut form = LauncherForm::default();
        form.set_network(BinanceNetwork::Mainnet);
        assert_eq!(problem_fields(&form), vec!["mainnet_confirmation"]);
        form.mainnet_confirmation = "mainnet".to_string();
        assert!(!problem_fields(&form).is_empty(), "case matters");
        form.mainnet_confirmation = "MAINNET".to_string();
        assert!(form.to_request().is_ok());
    }

    #[test]
    fn switching_networks_clears_the_confirmation() {
        let mut form = LauncherForm::default();
        form.set_network(BinanceNetwork::Mainnet);
        form.mainnet_confirmation = "MAINNET".to_string();
        form.set_network(BinanceNetwork::Testnet);
        form.set_network(BinanceNetwork::Mainnet);
        assert_eq!(
            form.mainnet_confirmation, "",
            "a word typed earlier must not carry over"
        );
        // Re-selecting the same network keeps what was typed.
        form.mainnet_confirmation = "MAIN".to_string();
        form.set_network(BinanceNetwork::Mainnet);
        assert_eq!(form.mainnet_confirmation, "MAIN");
    }

    #[test]
    fn every_bad_field_is_reported_not_just_the_first() {
        let form = LauncherForm {
            max_order_notional: "abc".to_string(),
            max_order_quantity: "".to_string(),
            max_daily_trade_count: "-3".to_string(),
            stale_data_timeout_secs: "x".to_string(),
            strategy_path: "  ".to_string(),
            ..LauncherForm::default()
        };
        let fields = problem_fields(&form);
        for expected in [
            "max_order_notional",
            "max_order_quantity",
            "max_daily_trade_count",
            "stale_data_timeout_secs",
            "strategy_path",
        ] {
            assert!(
                fields.contains(&expected),
                "{expected} missing from {fields:?}"
            );
        }
    }

    #[test]
    fn zero_and_negative_limits_are_refused() {
        for (field, value) in [
            ("qty", "0"),
            ("notional", "-1"),
            ("position", "0"),
            ("trades", "0"),
        ] {
            let mut form = LauncherForm::default();
            match field {
                "qty" => form.max_order_quantity = value.to_string(),
                "notional" => form.max_order_notional = value.to_string(),
                "position" => form.max_position_per_symbol = value.to_string(),
                _ => form.max_daily_trade_count = value.to_string(),
            }
            assert!(form.to_request().is_err(), "{field}={value}");
        }
    }

    #[test]
    fn a_value_cap_below_the_exchange_minimum_is_refused_with_the_reason() {
        let form = LauncherForm {
            max_order_notional: "4".to_string(),
            ..LauncherForm::default()
        };
        let problems = form.to_request().unwrap_err();
        let message = &problems
            .iter()
            .find(|p| p.field == "max_order_notional")
            .unwrap()
            .message;
        assert!(message.contains("minimum order value"), "{message}");
        let ok = LauncherForm {
            max_order_notional: "5".to_string(),
            ..LauncherForm::default()
        };
        assert!(ok.to_request().is_ok(), "exactly the minimum is allowed");
    }

    #[test]
    fn the_default_limits_suit_both_btc_and_eth_at_the_demo_order_size() {
        // The demo strategy holds 20 USDT: 0.0003 BTC or 0.0066 ETH.
        let limits = LauncherForm::default().to_request().unwrap().risk_limits;
        for (price, base_qty) in [(dec!(65000), dec!(0.0003)), (dec!(3000), dec!(0.0066))] {
            assert!(base_qty <= limits.max_order_quantity);
            assert!(base_qty <= limits.max_position_per_symbol);
            assert!(base_qty * price <= limits.max_order_notional);
        }
    }

    #[test]
    fn the_stale_data_timeout_follows_the_network_unless_the_operator_chose_one() {
        let mut form = LauncherForm::default();
        assert_eq!(form.stale_data_timeout_secs, "90", "testnet is tolerant");
        form.set_network(BinanceNetwork::Mainnet);
        assert_eq!(form.stale_data_timeout_secs, "30", "mainnet is strict");
        form.set_network(BinanceNetwork::Testnet);
        assert_eq!(form.stale_data_timeout_secs, "90");

        // A value the operator typed is theirs: switching never overwrites it.
        form.stale_data_timeout_secs = "45".to_string();
        form.set_network(BinanceNetwork::Mainnet);
        assert_eq!(form.stale_data_timeout_secs, "45");
    }

    #[test]
    fn live_sessions_do_not_share_the_test_suites_database() {
        let url = LauncherForm::default().postgres_url;
        assert!(url.ends_with("/apexes_live"), "{url}");
        assert!(
            !url.ends_with("/apexes"),
            "the test suites TRUNCATE `apexes`"
        );
    }

    #[test]
    fn a_zero_stale_data_timeout_is_refused_because_it_would_disable_the_watchdog() {
        let form = LauncherForm {
            stale_data_timeout_secs: "0".to_string(),
            ..LauncherForm::default()
        };
        assert_eq!(problem_fields(&form), vec!["stale_data_timeout_secs"]);
    }

    #[test]
    fn symbols_are_split_trimmed_and_upper_cased() {
        let form = LauncherForm {
            symbols: " btcusdt , ETHUSDT,, ".to_string(),
            ..LauncherForm::default()
        };
        assert_eq!(
            form.symbol_list(),
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
        );
    }

    #[test]
    fn a_typed_session_id_is_passed_through_and_blank_means_generate() {
        let form = LauncherForm {
            session_id: "  my-session ".to_string(),
            ..LauncherForm::default()
        };
        assert_eq!(
            form.to_request().unwrap().session_id.as_deref(),
            Some("my-session")
        );
    }

    #[test]
    fn launch_is_blocked_while_busy_or_credentials_are_missing_and_says_why() {
        let mut state = LauncherState::default();
        assert!(state.can_launch());
        assert_eq!(state.launch_blocker(), None);

        state.missing_credential = Some("APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY".to_string());
        assert!(!state.can_launch());
        assert!(
            state
                .launch_blocker()
                .unwrap()
                .contains("APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY")
        );

        state.missing_credential = None;
        state.status = LaunchStatus::Launching;
        assert!(!state.can_launch(), "no double-launch");
        assert!(state.launch_blocker().unwrap().contains("in progress"));
    }

    #[test]
    fn launch_is_blocked_by_an_invalid_form_with_the_first_problem_as_the_reason() {
        let mut state = LauncherState::default();
        state.form.max_order_quantity = "nope".to_string();
        assert!(!state.can_launch());
        assert!(state.launch_blocker().unwrap().contains("not a number"));
    }

    #[test]
    fn strategies_in_the_directory_are_listed_sorted() {
        let dir =
            std::env::temp_dir().join(format!("apexes-ui-strat-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("b.drift"), "").unwrap();
        std::fs::write(dir.join("a.drift"), "").unwrap();
        std::fs::write(dir.join("notes.txt"), "").unwrap();
        let mut state = LauncherState::default();
        state.form.strategies_dir = dir.to_string_lossy().into_owned();
        state.refresh_strategies();
        let names: Vec<_> = state
            .available_strategies
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, ["a.drift", "b.drift"]);
        std::fs::remove_dir_all(dir).ok();
    }
}
