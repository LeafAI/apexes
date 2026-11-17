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

/// A session that holds a position and has an order working, so a render shows
/// the positions card with content in it (a flat account shows only "flat").
fn holding_snapshot() -> SessionSnapshot {
    let mut snap = snapshot(&["BTCUSDT", "ETHUSDT", "BNBUSDT"], true);
    snap.positions.insert(
        "BTCUSDT".to_string(),
        apexes_strategy::Position {
            symbol: "BTCUSDT".to_string(),
            quantity: dec!(0.12345),
            average_entry_price: dec!(65432.1),
            leverage: None,
            margin_mode: None,
            allocated_margin: dec!(0),
        },
    );
    snap.open_orders = vec![apexes_backtest::OpenOrderView {
        id: "o1".to_string(),
        symbol: "ETHUSDT".to_string(),
        side: "Buy".to_string(),
        order_type: "Limit".to_string(),
        quantity: dec!(0.5),
        filled_quantity: dec!(0.1),
        price: Some(dec!(1795.5)),
        status: "PartiallyFilled".to_string(),
        created_at: Utc::now(),
    }];
    snap
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

/// Wide window: the right column is the orders panel and holds, top to bottom,
/// Recent orders, Positions (with Open orders), Risk limits, Balances. The main
/// column keeps Details and the parameters.
#[test]
fn the_right_column_runs_orders_then_positions_then_limits_then_balances() {
    let mut h = harness(state(snapshot(&["BTCUSDT"], true)), 1180.0);
    h.run();
    let orders = rect_of(&h, "Recent orders");
    let positions = rect_of(&h, "Positions");
    let open = rect_of(&h, "Open orders (0)");
    let limits = rect_of(&h, "Risk limits");
    let balances = rect_of(&h, "Balances");
    let details = rect_of(&h, "Details");

    for (name, r) in [
        ("Positions", positions),
        ("Open orders", open),
        ("Risk limits", limits),
        ("Balances", balances),
    ] {
        assert!(
            (r.min.x - orders.min.x).abs() < 2.0,
            "{name} shares Recent orders' column: {r:?} vs {orders:?}"
        );
    }
    assert!(
        positions.min.y > orders.min.y,
        "positions under the orders list"
    );
    assert!(
        open.min.y > positions.min.y,
        "open orders are in the positions card"
    );
    assert!(
        limits.min.y > open.min.y,
        "limits under positions (and its open orders)"
    );
    assert!(balances.min.y > limits.min.y, "balances under limits");
    assert!(
        orders.min.x > details.min.x + 300.0,
        "the right column is to the right of the main one: {details:?} {orders:?}"
    );
}

/// Positions left the main column, so Details is the only card above the
/// parameters there and nothing of the right column is beside it.
#[test]
fn positions_are_no_longer_in_the_main_column() {
    let mut h = harness(state(snapshot(&["BTCUSDT"], true)), 1180.0);
    h.run();
    let details = rect_of(&h, "Details");
    let positions = rect_of(&h, "Positions");
    assert!(
        positions.min.x > details.min.x + 300.0,
        "positions are in the right column: {details:?} {positions:?}"
    );
    let params = rect_of(&h, "Strategy parameters");
    assert!(
        (params.min.x - details.min.x).abs() < 2.0 && params.min.y > details.min.y,
        "the main column is Details, then the parameters"
    );
}

/// The positions table has six columns; the side panel is narrower than the
/// body was, so the card must still hold them without running past its edge.
#[test]
fn a_position_row_fits_inside_the_side_panel() {
    let mut snap = snapshot(&["BTCUSDT"], true);
    snap.positions.insert(
        "BTCUSDT".to_string(),
        apexes_strategy::Position {
            symbol: "BTCUSDT".to_string(),
            quantity: dec!(0.12345),
            average_entry_price: dec!(65432.1),
            leverage: Some(dec!(3)),
            margin_mode: None,
            allocated_margin: dec!(0),
        },
    );
    for width in [1100.0, 1180.0, 1500.0] {
        let mut h = harness(state(snap.clone()), width);
        h.run();
        let orders = rect_of(&h, "Recent orders");
        for label in [
            "Symbol",
            "Qty",
            "Entry",
            "Leverage",
            "Margin ratio",
            "Liq. price",
        ] {
            let r = rect_of(&h, label);
            assert!(
                r.max.x <= width + 1.0,
                "width {width}: header {label} runs off the window: {r:?}"
            );
            assert!(
                r.min.x >= orders.min.x - 1.0,
                "{label} starts inside the column"
            );
        }
    }
}

/// No side panel in a narrow window, so the cards keep their own two columns
/// there (limits and balances on the right of the body, orders below).
#[test]
fn without_the_side_panel_limits_and_balances_stay_in_the_bodys_right_column() {
    let mut h = harness(state(snapshot(&["BTCUSDT"], true)), 900.0);
    h.run();
    let limits = rect_of(&h, "Risk limits");
    let balances = rect_of(&h, "Balances");
    let positions = rect_of(&h, "Positions");
    let orders = rect_of(&h, "Recent orders");
    assert!((limits.min.x - balances.min.x).abs() < 2.0);
    assert!(limits.min.x > positions.min.x + 200.0);
    assert!(
        orders.min.y > positions.min.y,
        "orders panel is below the cards"
    );
}

/// The orders panel is taller now (orders, limits, balances) and is a fixed
/// full-height column, so it must scroll rather than cut Balances off.
#[test]
fn a_full_orders_list_does_not_cut_off_balances() {
    use apexes_backtest::OrderRecord;
    let mut snap = snapshot(&["BTCUSDT"], true);
    snap.recent_orders = (0..30)
        .map(|i| OrderRecord {
            id: format!("o{i}"),
            at: Utc::now() - Duration::seconds(i),
            symbol: "BTCUSDT".to_string(),
            side: if i % 2 == 0 { "Buy" } else { "Sell" }.to_string(),
            order_type: "Market".to_string(),
            quantity: dec!(0.001),
            limit_price: None,
            filled_quantity: dec!(0.001),
            avg_fill_price: Some(dec!(65000)),
            status: "Filled".to_string(),
            detail: None,
        })
        .collect();
    let mut h = Harness::builder()
        .with_size(egui::vec2(1180.0, 700.0))
        .build_ui_state(
            |ui, (state, sent): &mut Fixture| sent.extend(show(ui, state)),
            (state(snap), Vec::new()),
        );
    h.run();
    assert!(
        has_text(&h, "Balances"),
        "Balances is still part of the panel"
    );
    assert!(has_text(&h, "Risk limits"));
    assert!(
        h.query_all_by_label_contains("HALT TRADING")
            .next()
            .is_some(),
        "HALT stays reachable"
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
        let details = rect_of(&h, "Details");
        let limits = rect_of(&h, "Risk limits");
        // Wide: limits are in the orders panel, right of the whole main
        // column. Narrow: they are the body's right column, right of Positions.
        let left_edge_of_right = if width >= 1100.0 {
            details.min.x + 300.0
        } else {
            rect_of(&h, "Positions").min.x + 200.0
        };
        assert!(
            limits.min.x >= left_edge_of_right,
            "width {width}: right column starts at {} (needs {left_edge_of_right})",
            limits.min.x,
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

// ---- no separator rule beside the orders panel -------------------------

/// The pixel column just right of the main column's cards, where the side
/// panel's full-height rule used to be drawn, must be plain page background.
#[test]
fn no_full_height_rule_is_drawn_between_the_cards_and_the_orders_panel() {
    use apexes_ui::chrome::Chrome;
    use apexes_ui::operator_view::{OperatorApp, StandaloneConsole};
    use apexes_ui::theme::ThemeChoice;
    let mut chrome = Chrome::load(None);
    chrome.set(ThemeChoice::Light);
    let s = state(snapshot(&["BTCUSDT"], true));
    let mut h = Harness::builder()
        .with_size(egui::vec2(1500.0, 1250.0))
        .build_eframe(move |_cc| StandaloneConsole::new(OperatorApp::new_for_test(s), chrome));
    h.step();
    h.step();
    let image = h.render().expect("render");
    // Below every card (the parameters card ends near y = 1000), where only the
    // page background and the separator could be.
    let (x_range, y) = (960..1010u32, 1150u32);
    let page = *image.get_pixel(20, y);
    let rule = x_range
        .filter(|x| *image.get_pixel(*x, y) != page)
        .collect::<Vec<_>>();
    assert!(rule.is_empty(), "a vertical rule is drawn at x = {rule:?}");
}

/// Renders the whole window as `scripts/run-testnet.sh` shows it: tab bar,
/// page and a console in it. The standalone-console renders above lack the tab
/// bar, whose rule sits right above the cards. Returns the PNG's path.
fn render_shell(theme: apexes_ui::theme::ThemeChoice, size: [f32; 2], name: &str) -> String {
    render_shell_at(theme, size, 1.0, name)
}

/// A rendered window as plain data: width, height, and one RGBA per pixel.
struct Shot {
    width: u32,
    height: u32,
    pixels: Vec<[u8; 4]>,
}

fn render_shell_at(
    theme: apexes_ui::theme::ThemeChoice,
    size: [f32; 2],
    pixels_per_point: f32,
    name: &str,
) -> String {
    render_shell_hover(theme, size, pixels_per_point, None, name).0
}

fn render_shell_hover(
    theme: apexes_ui::theme::ThemeChoice,
    size: [f32; 2],
    pixels_per_point: f32,
    hover: Option<[f32; 2]>,
    name: &str,
) -> (String, Shot) {
    use apexes_ui::chrome::Chrome;
    use apexes_ui::operator_view::OperatorApp;
    use apexes_ui::shell_app::ShellApp;
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let home = std::env::temp_dir().join(format!(
        "apexes-shellshot-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&home).expect("home");
    let mut chrome = Chrome::load(None);
    chrome.set(theme);
    let handle = runtime.handle().clone();
    let supervisor = apexes_supervisor::Supervisor::new(&home);
    let mut s = state(holding_snapshot());
    s.engine_url = "test".to_string();
    let mut h = Harness::builder()
        .with_size(egui::vec2(size[0], size[1]))
        .with_pixels_per_point(pixels_per_point)
        .build_eframe(move |_cc| {
            let mut app = ShellApp::with_chrome(supervisor, handle, chrome);
            app.open_console_with_state("testnet-demo", OperatorApp::new_for_test(s));
            app
        });
    h.step();
    h.step();
    if let Some(at) = hover {
        h.hover_at(egui::pos2(at[0], at[1]));
        h.step();
        h.step();
    }
    let image = h.render().expect("render");
    let dir = std::env::var("APEXES_SHOTS").unwrap_or_else(|_| "/tmp/shots".to_string());
    std::fs::create_dir_all(&dir).expect("shots dir");
    let path = format!("{dir}/charts-shell-{name}.png");
    image.save(&path).expect("save");
    std::fs::remove_dir_all(&home).ok();
    drop(h);
    drop(runtime);
    let (width, height) = (image.width(), image.height());
    let pixels = image.pixels().map(|p| p.0).collect();
    (
        path,
        Shot {
            width,
            height,
            pixels,
        },
    )
}

#[test]
#[ignore = "writes PNG files for manual inspection"]
fn renders_the_real_shell_window() {
    use apexes_ui::theme::ThemeChoice;
    for (name, theme) in [("light", ThemeChoice::Light), ("dark", ThemeChoice::Dark)] {
        println!("{}", render_shell(theme, [1180.0, 760.0], name));
    }
    println!(
        "{}",
        render_shell_at(ThemeChoice::Light, [1500.0, 1000.0], 2.0, "retina")
    );
    println!(
        "{}",
        render_shell_at(ThemeChoice::Light, [1500.0, 1000.0], 1.0, "large")
    );
    // The narrowest window that still gets the side panel.
    println!(
        "{}",
        render_shell_at(ThemeChoice::Light, [1130.0, 900.0], 1.0, "narrowest-side")
    );
    // The mouse resting on the gap between the cards and the orders panel,
    // then over the main column.
    for (name, at) in [
        ("hover-edge", [998.0, 600.0]),
        ("hover-body", [700.0, 600.0]),
        ("hover-orders", [1250.0, 500.0]),
    ] {
        println!(
            "{}",
            render_shell_hover(ThemeChoice::Light, [1500.0, 1000.0], 1.0, Some(at), name).0
        );
    }
}

/// With the mouse over the page (where an operator's mouse nearly always is) no
/// tall bar may appear. egui draws the scroll bar's handle there when the
/// content is taller than the window, and it read as a stray vertical line.
#[test]
fn the_mouse_over_the_page_draws_no_full_height_bar() {
    use apexes_ui::theme::ThemeChoice;
    let (_, rest) = render_shell_hover(ThemeChoice::Light, [1500.0, 1000.0], 1.0, None, "t-rest");
    let (_, over) = render_shell_hover(
        ThemeChoice::Light,
        [1500.0, 1000.0],
        1.0,
        Some([700.0, 600.0]),
        "t-over",
    );
    let (w, h) = (rest.width as usize, rest.height as usize);
    let tall: Vec<usize> = (0..w)
        .filter(|x| {
            (0..h)
                .filter(|y| rest.pixels[y * w + x] != over.pixels[y * w + x])
                .count()
                > h / 3
        })
        .collect();
    assert!(
        tall.is_empty(),
        "hovering the page draws a tall bar at x = {tall:?}"
    );
}

/// The side panel is a fixed-default, resizable column. Content (a wide open
/// orders table) must not widen it past its default, and it must not creep
/// wider frame after frame.
#[test]
fn the_side_panel_keeps_its_width_with_a_wide_table_and_does_not_grow() {
    let width_of = |snap: SessionSnapshot, frames: usize| {
        let mut h = harness(state(snap), 1500.0);
        h.step();
        h.step();
        let first = rect_of(&h, "Recent orders").min.x;
        for _ in 0..frames {
            h.step();
        }
        (first, rect_of(&h, "Recent orders").min.x)
    };
    let (empty_first, empty_last) = width_of(snapshot(&["BTCUSDT"], true), 20);
    let (full_first, full_last) = width_of(holding_snapshot(), 20);
    assert_eq!(empty_first, empty_last, "an empty panel settles");
    assert_eq!(full_first, full_last, "a panel with tables settles");
    assert!(
        (full_last - empty_last).abs() < 2.0,
        "content must not change the column's width: {empty_last} vs {full_last}"
    );
}
