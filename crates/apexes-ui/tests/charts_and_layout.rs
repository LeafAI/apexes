//! The chart selector, the "pricing…" equity, and the two-column card layout,
//! driven through the real console widgets in a headless window.

use std::collections::HashMap;

use apexes_backtest::{EvaluationSummary, LimitUsage, SessionCommand, SessionSnapshot};
use apexes_engine::{EngineStatus, SessionMessage};
use apexes_ui::chart_view::ChartView;
use apexes_ui::client::ConnectionStatus;
use apexes_ui::operator::OperatorState;
use apexes_ui::operator_view::show;
use chrono::{DateTime, Duration, Utc};
use egui_kittest::Harness;
use egui_kittest::kittest::Queryable;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

type Fixture = (OperatorState, Vec<SessionCommand>);
type Series = Vec<(DateTime<Utc>, Decimal)>;

/// `n` one-minute points around `start`. `wobble` shapes the walk (its period
/// and size differ per symbol, so lines in "All" do not sit on top of each
/// other) and `drift` is the net move over the whole span, as a fraction.
fn series(start: Decimal, wobble: i64, drift: Decimal, n: i64) -> Series {
    let t0 = Utc::now() - Duration::minutes(n);
    (0..n)
        .map(|i| {
            let wave = Decimal::from((i * wobble) % 11 - 5) / dec!(2000);
            let trend = drift * Decimal::from(i) / Decimal::from(n);
            (
                t0 + Duration::minutes(i),
                start * (Decimal::ONE + wave + trend),
            )
        })
        .collect()
}

fn evaluation() -> EvaluationSummary {
    EvaluationSummary {
        start_equity: dec!(77000),
        total_pnl: dec!(5),
        realized_pnl: dec!(0),
        strategy_pnl: dec!(5),
        holdings_pnl: dec!(0),
        holdings_value_at_start: dec!(0),
        unrealized_pnl: dec!(5),
        current_drawdown: dec!(0),
        max_drawdown: dec!(0),
        strategy_current_drawdown: dec!(0),
        strategy_max_drawdown: dec!(0),
        gross_exposure_fraction: dec!(0.1),
        fees_paid: HashMap::new(),
        trade_count: 3,
        turnover: dec!(100),
        time_in_market_fraction: dec!(0.2),
        limit_usage: vec![LimitUsage {
            name: "daily orders".to_string(),
            used: dec!(2),
            limit: dec!(100),
        }],
    }
}

fn snapshot(symbols: &[&str], priced: bool) -> SessionSnapshot {
    let price_history = symbols
        .iter()
        .map(|s| {
            let (base, wobble, drift) = match *s {
                "BTCUSDT" => (dec!(65000), 3, dec!(0.004)),
                "ETHUSDT" => (dec!(1800), 5, dec!(-0.006)),
                _ => (dec!(590), 7, dec!(0.001)),
            };
            (s.to_string(), series(base, wobble, drift, 60))
        })
        .collect();
    SessionSnapshot {
        identity: None,
        evaluation: priced.then(evaluation),
        equity_history: series(dec!(77000), 3, dec!(0.0006), 60),
        recent_orders: Vec::new(),
        price_history,
        equity_priced: priced,
        positions: HashMap::new(),
        balances: HashMap::from([
            ("USDT".to_string(), dec!(9940)),
            ("BNB".to_string(), dec!(1)),
        ]),
        equity: dec!(76773),
        realized_pnl: HashMap::new(),
        margin_status: HashMap::new(),
        params: HashMap::from([("threshold".to_string(), dec!(5))]),
        paused: false,
        open_orders: Vec::new(),
        timestamp: Utc::now(),
    }
}

fn state(snapshot: SessionSnapshot) -> OperatorState {
    let mut s = OperatorState::default();
    s.set_connection(ConnectionStatus::Connected);
    s.apply(SessionMessage::Status(EngineStatus {
        actor: "alice".to_string(),
        role: "operator".to_string(),
        halted: false,
        halt_reason: None,
        flatten_requested: false,
    }));
    s.apply(SessionMessage::Snapshot(snapshot));
    s
}

fn harness(state: OperatorState, width: f32) -> Harness<'static, Fixture> {
    Harness::builder()
        .with_size(egui::vec2(width, 1500.0))
        .build_ui_state(
            |ui, (state, sent): &mut Fixture| sent.extend(show(ui, state)),
            (state, Vec::new()),
        )
}

fn has_text(h: &Harness<'static, Fixture>, text: &str) -> bool {
    h.query_all_by_label_contains(text).next().is_some()
}

fn rect_of(h: &Harness<'static, Fixture>, label: &str) -> egui::Rect {
    h.get_by_label(label).rect()
}

// ---- chart selector ----------------------------------------------------

#[test]
fn a_multi_symbol_session_offers_equity_each_symbol_and_all() {
    let mut h = harness(
        state(snapshot(&["BTCUSDT", "ETHUSDT", "BNBUSDT"], true)),
        1180.0,
    );
    h.run();
    for label in ["Equity", "BNBUSDT", "BTCUSDT", "ETHUSDT", "All"] {
        assert!(
            h.query_all_by_label(label).next().is_some(),
            "selector is missing {label}"
        );
    }
}

#[test]
fn a_single_symbol_session_has_no_all_option() {
    let mut h = harness(state(snapshot(&["BTCUSDT"], true)), 1180.0);
    h.run();
    assert!(h.query_all_by_label("All").next().is_none());
    assert!(h.query_all_by_label("BTCUSDT").next().is_some());
}

#[test]
fn choosing_a_symbol_switches_the_chart_to_its_price_and_the_choice_is_kept() {
    let mut h = harness(
        state(snapshot(&["BTCUSDT", "ETHUSDT", "BNBUSDT"], true)),
        1180.0,
    );
    h.run();
    assert_eq!(h.state().0.chart, ChartView::Equity);
    assert!(has_text(&h, "account value"));
    h.get_by_label("ETHUSDT").click();
    h.run();
    assert_eq!(h.state().0.chart, ChartView::Symbol("ETHUSDT".into()));
    assert!(has_text(&h, "last trade price"));
    assert!(!has_text(&h, "account value"));
}

#[test]
fn all_says_it_is_percent_change_so_symbols_of_different_price_are_comparable() {
    let mut h = harness(
        state(snapshot(&["BTCUSDT", "ETHUSDT", "BNBUSDT"], true)),
        1180.0,
    );
    h.run();
    h.get_by_label("All").click();
    h.run();
    assert_eq!(h.state().0.chart, ChartView::All);
    assert!(has_text(&h, "percent change"));
}

/// A selection that no longer exists (a session that shrank, an older engine)
/// must not leave an empty chart: it falls back to equity.
#[test]
fn a_stale_selection_falls_back_to_equity_instead_of_an_empty_chart() {
    let mut s = state(snapshot(&["BTCUSDT"], true));
    s.chart = ChartView::All;
    let mut h = harness(s, 1180.0);
    h.run();
    assert_eq!(h.state().0.chart, ChartView::Equity);
    assert!(has_text(&h, "account value"));
}

#[test]
fn a_symbol_that_has_not_traded_yet_says_it_is_waiting_not_blank() {
    let mut snap = snapshot(&["BTCUSDT"], true);
    snap.price_history.insert("BNBUSDT".into(), Vec::new());
    let mut s = state(snap);
    s.chart = ChartView::Symbol("BNBUSDT".into());
    let mut h = harness(s, 1180.0);
    h.run();
    // Not offered while it has no price, so the chart is equity.
    assert_eq!(h.state().0.chart, ChartView::Equity);
    assert!(h.query_all_by_label("BNBUSDT").next().is_none());
}

// ---- pricing -----------------------------------------------------------

#[test]
fn equity_says_pricing_instead_of_a_wrong_number_while_a_holding_has_no_price() {
    let mut h = harness(state(snapshot(&["BTCUSDT"], false)), 1180.0);
    h.run();
    assert!(
        has_text(&h, "pricing"),
        "the equity tile says it is pricing"
    );
    assert!(
        !has_text(&h, "76773.00"),
        "the placeholder-valued equity must not be shown"
    );
}

#[test]
fn while_pricing_the_empty_details_card_blames_the_missing_price_not_the_engine() {
    let mut h = harness(state(snapshot(&["BTCUSDT"], false)), 1180.0);
    h.run();
    assert!(has_text(&h, "waiting for a price of a held asset before"));
    assert!(!has_text(&h, "from this engine"));
}

#[test]
fn a_priced_session_shows_the_equity_number() {
    let mut h = harness(state(snapshot(&["BTCUSDT"], true)), 1180.0);
    h.run();
    assert!(has_text(&h, "76773.00"));
    assert!(!has_text(&h, "pricing"));
}

// ---- layout ------------------------------------------------------------

/// Left column: details, then positions and open orders. Right column: risk
/// limits, then balances.
#[test]
fn details_and_positions_are_on_the_left_and_limits_and_balances_on_the_right() {
    let mut h = harness(state(snapshot(&["BTCUSDT"], true)), 1180.0);
    h.run();
    let details = rect_of(&h, "Details");
    let positions = rect_of(&h, "Positions");
    let limits = rect_of(&h, "Risk limits");
    let balances = rect_of(&h, "Balances");

    assert!(
        (details.min.x - positions.min.x).abs() < 2.0,
        "details and positions share the left column"
    );
    assert!(
        (limits.min.x - balances.min.x).abs() < 2.0,
        "limits and balances share the right column"
    );
    assert!(
        limits.min.x > details.max.x - 1.0 || limits.min.x > details.min.x + 300.0,
        "the right column is to the right of the left: {details:?} {limits:?}"
    );
    assert!(positions.min.y > details.min.y, "positions under details");
    assert!(balances.min.y > limits.min.y, "balances under limits");
    assert!(
        (details.min.y - limits.min.y).abs() < 2.0,
        "the two columns start on the same row"
    );
}

#[test]
fn a_narrow_window_stacks_every_card_in_one_column_losing_none() {
    let mut h = harness(state(snapshot(&["BTCUSDT"], true)), 600.0);
    h.run();
    let xs: Vec<f32> = ["Details", "Positions", "Risk limits", "Balances"]
        .iter()
        .map(|l| rect_of(&h, l).min.x)
        .collect();
    assert!(
        xs.iter().all(|x| (x - xs[0]).abs() < 2.0),
        "stacked in one column: {xs:?}"
    );
}

/// The columns must not overlap each other at any width the layout splits at.
#[test]
fn the_two_columns_never_overlap() {
    for width in [720.0, 900.0, 1180.0, 1800.0] {
        let mut h = harness(state(snapshot(&["BTCUSDT"], true)), width);
        h.run();
        let positions = rect_of(&h, "Positions");
        let limits = rect_of(&h, "Risk limits");
        assert!(
            limits.min.x >= positions.min.x + 200.0,
            "width {width}: right column starts at {} left at {}",
            limits.min.x,
            positions.min.x
        );
        assert!(limits.max.x <= width + 1.0, "width {width}: {limits:?}");
    }
}

#[test]
fn nothing_the_old_layout_showed_is_lost() {
    let mut h = harness(state(snapshot(&["BTCUSDT", "ETHUSDT"], true)), 1180.0);
    h.run();
    for text in [
        "Details",
        "Positions",
        "Open orders",
        "Risk limits",
        "Balances",
        "daily orders",
        "HALT TRADING",
        "Strategy parameters",
    ] {
        assert!(has_text(&h, text), "missing {text}");
    }
}

#[test]
fn without_evaluation_data_the_left_column_says_so_and_balances_remain() {
    let mut snap = snapshot(&["BTCUSDT"], true);
    snap.evaluation = None;
    let mut h = harness(state(snap), 1180.0);
    h.run();
    assert!(has_text(&h, "no evaluation data"));
    assert!(has_text(&h, "Balances"));
    assert!(has_text(&h, "Positions"));
}

// ---- reachability ------------------------------------------------------

#[test]
fn halt_stays_on_screen_at_the_smallest_window_with_the_new_layout() {
    let mut h = Harness::builder()
        .with_size(egui::vec2(760.0, 520.0))
        .build_ui_state(
            |ui, (state, sent): &mut Fixture| sent.extend(show(ui, state)),
            (
                state(snapshot(&["BTCUSDT", "ETHUSDT", "BNBUSDT"], true)),
                Vec::new(),
            ),
        );
    h.run();
    let halt = h.get_by_label("HALT TRADING").rect();
    assert!(halt.min.y >= 0.0 && halt.max.y <= 520.0, "{halt:?}");
}

/// Looking at it: `APEXES_SHOTS=/tmp/shots cargo test -p apexes-ui --test
/// charts_and_layout -- renders --ignored`.
#[test]
#[ignore = "writes PNG files for manual inspection"]
fn renders_charts_and_layout() {
    use apexes_ui::chrome::Chrome;
    use apexes_ui::operator_view::{OperatorApp, StandaloneConsole};
    use apexes_ui::theme::ThemeChoice;
    let dir = std::env::var("APEXES_SHOTS").unwrap_or_else(|_| "/tmp/shots".to_string());
    std::fs::create_dir_all(&dir).expect("shots dir");
    let cases = [
        (
            "equity",
            ChartView::Equity,
            ThemeChoice::Light,
            [1500.0, 1250.0],
            true,
        ),
        (
            "btc",
            ChartView::Symbol("BTCUSDT".into()),
            ThemeChoice::Light,
            [1500.0, 1250.0],
            true,
        ),
        (
            "all-light",
            ChartView::All,
            ThemeChoice::Light,
            [1500.0, 1250.0],
            true,
        ),
        (
            "all-dark",
            ChartView::All,
            ThemeChoice::Dark,
            [1500.0, 1250.0],
            true,
        ),
        (
            "pricing",
            ChartView::Equity,
            ThemeChoice::Light,
            [1500.0, 1250.0],
            false,
        ),
        (
            "narrow-all",
            ChartView::All,
            ThemeChoice::Light,
            [760.0, 1700.0],
            true,
        ),
    ];
    for (name, view, theme, size, priced) in cases {
        let mut s = state(snapshot(&["BTCUSDT", "ETHUSDT", "BNBUSDT"], priced));
        s.chart = view;
        let mut chrome = Chrome::load(None);
        chrome.set(theme);
        let mut h = Harness::builder()
            .with_size(egui::vec2(size[0], size[1]))
            .build_eframe(move |_cc| StandaloneConsole::new(OperatorApp::new_for_test(s), chrome));
        h.step();
        h.step();
        let image = h.render().expect("render");
        image
            .save(format!("{dir}/charts-{name}.png"))
            .expect("save");
    }
}
