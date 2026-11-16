//! Headless tests of the theme: switching changes what is drawn, status
//! colours follow the theme, and the console renders in both. The PNG
//! renders (`APEXES_SHOTS=<dir>`) are for a human to look at.

use std::collections::HashMap;

use apexes_backtest::{EvaluationSummary, LimitUsage, SessionIdentity, SessionSnapshot};
use apexes_engine::{EngineStatus, SessionMessage};
use apexes_ui::chrome::Chrome;
use apexes_ui::client::ConnectionStatus;
use apexes_ui::evaluation_view::{Severity, environment_banner};
use apexes_ui::operator::OperatorState;
use apexes_ui::operator_view::{OperatorApp, StandaloneConsole};
use apexes_ui::theme::{Palette, ThemeChoice};
use chrono::{Duration, Utc};
use egui_kittest::Harness;
use egui_kittest::kittest::Queryable;
use rust_decimal_macros::dec;

fn snapshot(network: &str) -> SessionSnapshot {
    snapshot_spanning(network, Duration::minutes(40), 40)
}

/// `points` equity samples spread evenly over `span`, ending now.
fn snapshot_spanning(network: &str, span: Duration, points: i64) -> SessionSnapshot {
    let start = Utc::now() - span;
    let equity_history = (0..points)
        .map(|i| {
            let wiggle = rust_decimal::Decimal::from((i * 7 % 13) - 4);
            (
                start + span / points as i32 * i as i32,
                dec!(10000) + wiggle * dec!(3),
            )
        })
        .collect();
    SessionSnapshot {
        identity: Some(SessionIdentity {
            session_id: "s-20260101".to_string(),
            network: Some(network.to_string()),
            mode: "live".to_string(),
            strategy: "example".to_string(),
        }),
        evaluation: Some(EvaluationSummary {
            start_equity: dec!(10000),
            total_pnl: dec!(-12.5),
            realized_pnl: dec!(3),
            strategy_pnl: dec!(-12.5),
            holdings_pnl: dec!(0),
            holdings_value_at_start: dec!(0),
            unrealized_pnl: dec!(-15.5),
            current_drawdown: dec!(0.0125),
            max_drawdown: dec!(0.04),
            strategy_current_drawdown: dec!(0.0125),
            strategy_max_drawdown: dec!(0.04),
            gross_exposure_fraction: dec!(0.2),
            fees_paid: HashMap::from([("USDT".to_string(), dec!(0.4))]),
            trade_count: 6,
            turnover: dec!(120),
            time_in_market_fraction: dec!(0.5),
            limit_usage: vec![
                LimitUsage {
                    name: "daily orders".to_string(),
                    used: dec!(2),
                    limit: dec!(100),
                },
                LimitUsage {
                    name: "order notional".to_string(),
                    used: dec!(60),
                    limit: dec!(100),
                },
                LimitUsage {
                    name: "position".to_string(),
                    used: dec!(95),
                    limit: dec!(100),
                },
            ],
        }),
        equity_history,
        recent_orders: Vec::new(),
        positions: HashMap::new(),
        balances: HashMap::from([("USDT".to_string(), dec!(9987.5))]),
        equity: dec!(9987.5),
        realized_pnl: HashMap::new(),
        margin_status: HashMap::new(),
        params: HashMap::from([("notional".to_string(), dec!(20))]),
        paused: false,
        open_orders: Vec::new(),
        timestamp: Utc::now(),
    }
}

fn state(network: &str, halted: bool) -> OperatorState {
    let mut s = OperatorState::default();
    s.set_connection(ConnectionStatus::Connected);
    s.apply(SessionMessage::Status(EngineStatus {
        actor: "operator".to_string(),
        role: "operator".to_string(),
        halted,
        halt_reason: halted.then(|| "stale market data".to_string()),
        flatten_requested: false,
    }));
    s.apply(SessionMessage::Snapshot(snapshot(network)));
    s
}

type Console = Harness<'static, StandaloneConsole>;

fn console(choice: ThemeChoice, state: OperatorState) -> Console {
    let mut chrome = Chrome::load(None);
    chrome.set(choice);
    Harness::builder()
        .with_size(egui::vec2(900.0, 820.0))
        .build_eframe(move |_cc| StandaloneConsole::new(OperatorApp::new_for_test(state), chrome))
}

fn render_to(dir: &str, name: &str, harness: &mut Console) {
    let image = harness.render().expect("render");
    std::fs::create_dir_all(dir).expect("shots dir");
    image.save(format!("{dir}/{name}.png")).expect("save png");
}

#[test]
fn the_console_follows_the_chosen_theme() {
    for (choice, theme) in [
        (ThemeChoice::Light, egui::Theme::Light),
        (ThemeChoice::Dark, egui::Theme::Dark),
    ] {
        let mut h = console(choice, state("testnet", false));
        h.step();
        h.step();
        assert_eq!(h.ctx.theme(), theme, "{choice:?}");
        assert!(
            h.query_all_by_label_contains("TESTNET").next().is_some(),
            "{choice:?}: the environment banner is still shown"
        );
    }
}

#[test]
fn the_theme_shortcut_cycles_and_applies() {
    let mut h = console(ThemeChoice::Light, state("testnet", false));
    h.step();
    assert_eq!(h.ctx.theme(), egui::Theme::Light);
    h.key_press_modifiers(
        egui::Modifiers::COMMAND | egui::Modifiers::SHIFT,
        egui::Key::L,
    );
    h.step();
    h.step();
    assert_eq!(h.ctx.theme(), egui::Theme::Dark, "Light -> Dark");
}

#[test]
fn status_colours_differ_between_themes_but_keep_their_meaning() {
    let light = Palette::for_theme(egui::Theme::Light);
    let dark = Palette::for_theme(egui::Theme::Dark);
    for severity in [Severity::Ok, Severity::Warn, Severity::Danger] {
        assert_ne!(
            severity.color(&light),
            severity.color(&dark),
            "{severity:?}"
        );
    }
    let banner = environment_banner(snapshot("mainnet").identity.as_ref());
    assert!(banner.text.contains("MAINNET"));
}

#[test]
fn a_halted_mainnet_console_renders_in_both_themes_without_panicking() {
    for choice in [ThemeChoice::Light, ThemeChoice::Dark] {
        let mut h = console(choice, state("mainnet", true));
        h.step();
        assert!(h.query_all_by_label_contains("HALTED").next().is_some());
        assert!(h.query_all_by_label_contains("MAINNET").next().is_some());
    }
}

/// The chart's x-axis in a real window, for a human to look at: a quarter of
/// an hour (seconds on the axis), an afternoon (hours and minutes), and a
/// span across local midnight (the date appears at the day change).
#[test]
#[ignore = "writes PNG files for manual inspection"]
fn renders_the_time_axis_at_three_scales() {
    let dir = std::env::var("APEXES_SHOTS").unwrap_or_else(|_| "/tmp/shots".to_string());
    for (name, span, points) in [
        ("axis-12min", Duration::minutes(12), 60),
        ("axis-5h", Duration::hours(5), 120),
        ("axis-30h", Duration::hours(30), 150),
    ] {
        let mut state = OperatorState::default();
        state.set_connection(ConnectionStatus::Connected);
        state.apply(SessionMessage::Snapshot(snapshot_spanning(
            "testnet", span, points,
        )));
        let mut h = console(ThemeChoice::Light, state);
        h.step();
        h.step();
        render_to(&dir, name, &mut h);
    }
}

/// Writes PNGs for a human to inspect. Run with
/// `APEXES_SHOTS=/tmp/shots cargo test -p apexes-ui --test themes -- renders --ignored`.
#[test]
#[ignore = "writes PNG files for manual inspection"]
fn renders_screenshots() {
    let dir = std::env::var("APEXES_SHOTS").unwrap_or_else(|_| "/tmp/shots".to_string());
    for (name, choice, network, halted) in [
        (
            "console-testnet-light",
            ThemeChoice::Light,
            "testnet",
            false,
        ),
        ("console-testnet-dark", ThemeChoice::Dark, "testnet", false),
        (
            "console-mainnet-halted-light",
            ThemeChoice::Light,
            "mainnet",
            true,
        ),
        (
            "console-mainnet-halted-dark",
            ThemeChoice::Dark,
            "mainnet",
            true,
        ),
    ] {
        let mut h = console(choice, state(network, halted));
        h.step();
        h.step();
        render_to(&dir, name, &mut h);
    }
}
