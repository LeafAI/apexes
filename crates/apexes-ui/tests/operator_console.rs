//! Headless `egui_kittest` tests that drive the real operator-console
//! widgets: what an operator sees and what their clicks produce.

use std::collections::HashMap;

use apexes_backtest::{OpenOrderView, SessionCommand, SessionSnapshot};
use apexes_engine::{EngineStatus, SessionMessage};
use apexes_ui::client::ConnectionStatus;
use apexes_ui::operator::OperatorState;
use apexes_ui::operator_view::show;
use chrono::Utc;
use egui_kittest::Harness;
use egui_kittest::kittest::{NodeT, Queryable};
use rust_decimal_macros::dec;

type Fixture = (OperatorState, Vec<SessionCommand>);

fn snapshot() -> SessionSnapshot {
    let mut positions = HashMap::new();
    positions.insert(
        "BTCUSDT".to_string(),
        apexes_strategy::Position {
            symbol: "BTCUSDT".to_string(),
            quantity: dec!(0.5),
            average_entry_price: dec!(65000),
            leverage: Some(dec!(3)),
            margin_mode: None,
            allocated_margin: dec!(0),
        },
    );
    SessionSnapshot {
        identity: None,
        evaluation: None,
        equity_history: Vec::new(),
        recent_orders: Vec::new(),
        price_history: Default::default(),
        equity_priced: true,
        positions,
        balances: HashMap::from([("USDT".to_string(), dec!(1234.5))]),
        equity: dec!(10500),
        realized_pnl: HashMap::from([("BTCUSDT".to_string(), dec!(12.5))]),
        margin_status: HashMap::new(),
        params: HashMap::from([("threshold".to_string(), dec!(5))]),
        paused: false,
        open_orders: vec![OpenOrderView {
            id: "id-1".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: "Buy".to_string(),
            order_type: "Limit".to_string(),
            quantity: dec!(1),
            filled_quantity: dec!(0.25),
            price: Some(dec!(64000)),
            status: "PartiallyFilled".to_string(),
            created_at: Utc::now(),
        }],
        timestamp: Utc::now(),
    }
}

fn status(role: &str, halted: bool) -> EngineStatus {
    EngineStatus {
        actor: "alice".to_string(),
        role: role.to_string(),
        halted,
        halt_reason: halted.then(|| "operator console".to_string()),
        flatten_requested: false,
        health_state: None,
        health_reason: None,
        resume_blocked: false,
    }
}

fn state(role: &str, halted: bool) -> OperatorState {
    let mut s = OperatorState::default();
    s.set_connection(ConnectionStatus::Connected);
    s.apply(SessionMessage::Status(status(role, halted)));
    s.apply(SessionMessage::Snapshot(snapshot()));
    s
}

/// The window a real operator has: the app's default size, not egui_kittest's
/// 800x600, which is smaller than the minimum the app allows. The console is a
/// scrolling page and a test must not depend on what happens to fit a toy window.
const WINDOW: [f32; 2] = [1180.0, 1500.0];

fn harness(state: OperatorState) -> Harness<'static, Fixture> {
    Harness::builder()
        .with_size(egui::vec2(WINDOW[0], WINDOW[1]))
        .build_ui_state(
            |ui, (state, sent): &mut Fixture| {
                sent.extend(show(ui, state));
            },
            (state, Vec::new()),
        )
}

#[test]
fn renders_positions_orders_balances_and_params() {
    let mut h = harness(state("operator", false));
    h.run();
    for text in [
        "TRADING",
        "alice (operator)",
        "BTCUSDT",
        "PartiallyFilled",
        "USDT",
        "threshold",
        "Open orders (1)",
    ] {
        // At least one widget shows this text.
        assert!(
            h.query_all_by_label_contains(text).next().is_some(),
            "missing {text:?} in the rendered console"
        );
    }
}

#[test]
fn halt_is_a_single_click() {
    let mut h = harness(state("operator", false));
    h.run();
    h.get_by_label("HALT TRADING").click();
    h.run();
    let (state, sent) = h.state();
    assert_eq!(
        sent,
        &vec![SessionCommand::Halt {
            reason: "operator console".to_string(),
            flatten: false
        }]
    );
    assert!(state.pending.is_none());
}

#[test]
fn flatten_needs_a_second_deliberate_click_and_cancel_sends_nothing() {
    let mut h = harness(state("operator", false));
    h.run();

    h.get_by_label("Halt + flatten...").click();
    h.run();
    assert!(h.state().1.is_empty(), "nothing is sent by the first click");
    assert!(
        h.query_all_by_label_contains("CLOSE all open futures positions")
            .next()
            .is_some(),
        "the consequences are spelled out"
    );

    h.get_by_label("Cancel").click();
    h.run();
    assert!(h.state().1.is_empty());

    h.get_by_label("Halt + flatten...").click();
    h.run();
    h.get_by_label("Confirm").click();
    h.run();
    assert_eq!(
        h.state().1,
        vec![SessionCommand::Halt {
            reason: "operator console (flatten)".to_string(),
            flatten: true
        }]
    );
}

#[test]
fn when_halted_the_console_says_so_and_offers_resume_not_halt() {
    let mut h = harness(state("operator", true));
    h.run();
    assert!(
        h.query_all_by_label_contains("TRADING HALTED")
            .next()
            .is_some(),
        "the halt banner is shown"
    );
    assert!(h.query_by_label("HALT TRADING").is_none());

    h.get_by_label("Resume trading...").click();
    h.run();
    assert!(h.state().1.is_empty(), "resuming needs confirmation");
    h.get_by_label("Confirm").click();
    h.run();
    assert_eq!(h.state().1, vec![SessionCommand::ResumeTrading]);
}

#[test]
fn a_viewer_sees_everything_but_every_control_is_disabled() {
    let mut h = harness(state("viewer", false));
    h.run();
    assert!(h.query_all_by_label_contains("BTCUSDT").next().is_some());
    for label in [
        "HALT TRADING",
        "Halt + flatten...",
        "Pause strategy",
        "Stop session...",
    ] {
        let button = h.get_by_label(label);
        assert!(
            button.accesskit_node().is_disabled(),
            "{label} must be disabled for a viewer"
        );
        button.click(); // even a forced click must do nothing
    }
    h.run();
    assert!(h.state().1.is_empty());
}

#[test]
fn a_disconnected_console_warns_and_disables_controls() {
    let mut s = state("operator", false);
    s.set_connection(ConnectionStatus::Reconnecting);
    let mut h = harness(s);
    h.run();
    assert!(
        h.query_all_by_label_contains("NO CONNECTION")
            .next()
            .is_some(),
        "stale state must be flagged"
    );
    assert!(
        h.get_by_label("HALT TRADING")
            .accesskit_node()
            .is_disabled()
    );
}

#[test]
fn editing_a_parameter_sends_set_param() {
    let mut h = harness(state("operator", false));
    h.run();
    // Type into the parameter's edit box, then press Set.
    h.state_mut()
        .0
        .param_edits
        .insert("threshold".to_string(), "7.5".to_string());
    h.run();
    h.get_by_label("Set").click();
    h.run();
    assert_eq!(
        h.state().1,
        vec![SessionCommand::SetParam {
            name: "threshold".to_string(),
            value: dec!(7.5)
        }]
    );
}

fn identity(mode: &str, network: Option<&str>) -> apexes_backtest::SessionIdentity {
    apexes_backtest::SessionIdentity {
        session_id: "s-42".to_string(),
        network: network.map(str::to_string),
        mode: mode.to_string(),
        strategy: "demo.drift".to_string(),
    }
}

fn evaluation() -> apexes_backtest::EvaluationSummary {
    apexes_backtest::EvaluationSummary {
        start_equity: dec!(10000),
        total_pnl: dec!(500),
        realized_pnl: dec!(120),
        strategy_pnl: dec!(500),
        holdings_pnl: dec!(0),
        holdings_value_at_start: dec!(0),
        unrealized_pnl: dec!(380),
        current_drawdown: dec!(0.02),
        max_drawdown: dec!(0.05),
        strategy_current_drawdown: dec!(0.02),
        strategy_max_drawdown: dec!(0.05),
        gross_exposure_fraction: dec!(0.3),
        fees_paid: HashMap::from([("USDT".to_string(), dec!(1.25))]),
        trade_count: 7,
        turnover: dec!(4200),
        time_in_market_fraction: dec!(0.6),
        limit_usage: vec![apexes_backtest::LimitUsage {
            name: "position per symbol".to_string(),
            used: dec!(0.04),
            limit: dec!(0.05),
        }],
    }
}

fn history() -> Vec<(chrono::DateTime<Utc>, rust_decimal::Decimal)> {
    let now = Utc::now();
    (0..30)
        .map(|i| {
            (
                now + chrono::Duration::seconds(i),
                dec!(10000) + rust_decimal::Decimal::from(i * 10),
            )
        })
        .collect()
}

fn state_with(snapshot: SessionSnapshot) -> OperatorState {
    let mut s = OperatorState::default();
    s.set_connection(ConnectionStatus::Connected);
    s.apply(SessionMessage::Status(status("operator", false)));
    s.apply(SessionMessage::Snapshot(snapshot));
    s
}

fn has_text(h: &Harness<'static, Fixture>, text: &str) -> bool {
    h.query_all_by_label_contains(text).next().is_some()
}

#[test]
fn a_testnet_session_is_labelled_testnet_with_virtual_funds() {
    let mut snap = snapshot();
    snap.identity = Some(identity("live", Some("testnet")));
    let mut h = harness(state_with(snap));
    h.run();
    assert!(has_text(&h, "TESTNET"));
    assert!(has_text(&h, "virtual funds"));
    assert!(has_text(&h, "demo.drift"));
    assert!(!has_text(&h, "REAL FUNDS"));
}

#[test]
fn a_mainnet_session_is_labelled_real_funds() {
    let mut snap = snapshot();
    snap.identity = Some(identity("live", Some("mainnet")));
    let mut h = harness(state_with(snap));
    h.run();
    assert!(has_text(&h, "MAINNET"));
    assert!(has_text(&h, "REAL FUNDS"));
}

#[test]
fn an_engine_that_reports_no_identity_is_flagged_not_assumed_safe() {
    // `snapshot()` carries `identity: None`, like an older engine would.
    let mut h = harness(state_with(snapshot()));
    h.run();
    assert!(has_text(&h, "ENVIRONMENT UNKNOWN"));
    assert!(!has_text(&h, "TESTNET"));
}

#[test]
fn the_evaluation_view_shows_metrics_limits_and_the_timeline() {
    let mut snap = snapshot();
    snap.identity = Some(identity("live", Some("testnet")));
    snap.evaluation = Some(evaluation());
    snap.equity_history = history();
    let mut h = harness(state_with(snap));
    h.run();
    for text in [
        "Equity",
        "Total PnL",
        "+500",
        "Drawdown (max)",
        "5%",
        "Gross exposure",
        "Fees paid",
        "1.25 USDT",
        "Risk limits",
        "position per symbol",
    ] {
        assert!(
            has_text(&h, text),
            "missing {text:?} in the evaluation view"
        );
    }
    assert!(!has_text(&h, "collecting equity history"));
}

#[test]
fn a_session_with_no_history_yet_says_so_instead_of_drawing_nothing() {
    let mut snap = snapshot();
    snap.evaluation = Some(evaluation());
    let mut h = harness(state_with(snap));
    h.run();
    assert!(has_text(&h, "collecting equity history"));
    assert!(has_text(&h, "Total PnL"));
}

#[test]
fn an_engine_without_evaluation_data_does_not_break_the_console() {
    let mut snap = snapshot();
    snap.equity_history = history();
    let mut h = harness(state_with(snap));
    h.run();
    assert!(has_text(&h, "no evaluation data"));
    assert!(
        has_text(&h, "BTCUSDT"),
        "the rest of the console still renders"
    );
}

// ---- What the screenshot showed -------------------------------------------

fn closed_btc_position() -> apexes_strategy::Position {
    apexes_strategy::Position {
        symbol: "BTCUSDT".to_string(),
        quantity: dec!(0.00000000),
        average_entry_price: dec!(64657.62000000),
        leverage: None,
        margin_mode: None,
        allocated_margin: dec!(0),
    }
}

/// The session from the screenshot: 1.0006 BTC held since before it started,
/// a flat strategy position, and BTC down about 272.
fn screenshot_snapshot() -> SessionSnapshot {
    let mut snap = snapshot();
    snap.identity = Some(identity("live", Some("testnet")));
    snap.positions = HashMap::from([("BTCUSDT".to_string(), closed_btc_position())]);
    snap.open_orders = Vec::new();
    snap.balances = HashMap::from([
        ("BTC".to_string(), dec!(1.00060000)),
        (
            "USDT".to_string(),
            rust_decimal::Decimal::from_str_exact("9940.9058680000000000").unwrap(),
        ),
    ]);
    snap.equity = dec!(74078.28);
    snap.realized_pnl = HashMap::from([("BTCUSDT".to_string(), dec!(-0.0727))]);
    let mut e = evaluation();
    e.start_equity = dec!(74350.26);
    e.total_pnl = dec!(-271.99);
    e.realized_pnl = dec!(-0.0727);
    e.strategy_pnl = dec!(-0.08);
    e.holdings_pnl = dec!(-271.91);
    e.holdings_value_at_start = dec!(64423);
    e.unrealized_pnl = dec!(-0.01);
    e.current_drawdown = dec!(0.0092);
    e.max_drawdown = dec!(0.0116);
    e.strategy_current_drawdown = dec!(0);
    e.strategy_max_drawdown = dec!(0.000001);
    e.gross_exposure_fraction = dec!(0.87);
    e.fees_paid = HashMap::from([
        ("BTC".to_string(), dec!(0.00000000)),
        ("USDT".to_string(), dec!(0.00000000)),
    ]);
    snap.evaluation = Some(e);
    snap
}

#[test]
fn a_closed_position_is_flat_not_a_row_with_a_stale_entry_price() {
    let mut h = harness(state_with(screenshot_snapshot()));
    h.run();
    assert!(
        h.query_by_label("flat").is_some(),
        "the positions section says flat"
    );
    assert!(
        !has_text(&h, "64657.62"),
        "the old entry price of a closed position is not shown"
    );
}

#[test]
fn balances_show_only_the_digits_that_mean_something() {
    let mut h = harness(state_with(screenshot_snapshot()));
    h.run();
    assert!(has_text(&h, "9940.905868"), "USDT, trailing zeros dropped");
    assert!(has_text(&h, "1.0006"));
    assert!(
        !has_text(&h, "9940.9058680000000000"),
        "the decimal's internal scale must not leak into the display"
    );
    assert!(!has_text(&h, "1.00060000"));
}

#[test]
fn zero_fees_read_as_a_single_zero_not_a_list_of_zero_currencies() {
    let mut h = harness(state_with(screenshot_snapshot()));
    h.run();
    assert!(has_text(&h, "Fees paid"));
    assert!(
        !has_text(&h, "0.00000000 BTC"),
        "no `0.00000000 BTC, 0.00000000 USDT`"
    );
}

#[test]
fn the_market_move_on_held_assets_is_labelled_as_such_and_apart_from_the_strategy() {
    let mut h = harness(state_with(screenshot_snapshot()));
    h.run();
    for text in [
        "Account PnL",
        "-271.99",
        "from holdings",
        "-271.91",
        "Strategy PnL",
        "-0.08",
    ] {
        assert!(has_text(&h, text), "missing {text:?}");
    }
}

#[test]
fn the_drawdown_shown_is_the_strategys_not_the_markets() {
    let mut h = harness(state_with(screenshot_snapshot()));
    h.run();
    assert!(
        !has_text(&h, "1.16%"),
        "the 1.16% was BTC's price swing on a holding the strategy never traded"
    );
}

#[test]
fn an_open_position_is_still_shown_with_clean_numbers() {
    let mut snap = screenshot_snapshot();
    snap.positions = HashMap::from([(
        "BTCUSDT".to_string(),
        apexes_strategy::Position {
            quantity: dec!(0.00070000),
            ..closed_btc_position()
        },
    )]);
    let mut h = harness(state_with(snap));
    h.run();
    assert!(has_text(&h, "0.0007"));
    assert!(has_text(&h, "64657.62"));
    assert!(
        h.query_by_label("flat").is_none(),
        "a held position is not reported as flat"
    );
}

// ---- Recent orders panel ---------------------------------------------------

fn order(
    secs_ago: i64,
    side: &str,
    status: &str,
    detail: Option<&str>,
) -> apexes_backtest::OrderRecord {
    apexes_backtest::OrderRecord {
        id: format!("o{secs_ago}{side}"),
        at: Utc::now() - chrono::Duration::seconds(secs_ago),
        symbol: "BTCUSDT".to_string(),
        side: side.to_string(),
        order_type: "Market".to_string(),
        quantity: dec!(0.00070000),
        limit_price: None,
        filled_quantity: if status == "Filled" {
            dec!(0.0007)
        } else {
            dec!(0)
        },
        avg_fill_price: (status == "Filled").then_some(dec!(64401.85000000)),
        status: status.to_string(),
        detail: detail.map(str::to_string),
    }
}

fn state_with_orders(orders: Vec<apexes_backtest::OrderRecord>) -> OperatorState {
    let mut snap = screenshot_snapshot();
    snap.recent_orders = orders;
    state_with(snap)
}

fn wide_harness(state: OperatorState, width: f32) -> Harness<'static, Fixture> {
    Harness::builder()
        .with_size(egui::vec2(width, 900.0))
        .build_ui_state(
            |ui, (state, sent): &mut Fixture| sent.extend(show(ui, state)),
            (state, Vec::new()),
        )
}

#[test]
fn the_first_order_and_every_one_after_it_are_listed_newest_first() {
    let orders = vec![
        order(20, "Sell", "Filled", None),
        order(60, "Buy", "Filled", None),
    ];
    let mut h = wide_harness(state_with_orders(orders), 1180.0);
    h.run();
    assert!(has_text(&h, "Recent orders"));
    assert!(has_text(&h, "2 orders, 2 filled"));
    assert!(has_text(&h, "64401.85"), "the fill price is shown");
    assert!(
        has_text(&h, "0.0007"),
        "the quantity, without trailing zeros"
    );
}

#[test]
fn a_refused_order_shows_its_reason_in_the_panel() {
    let orders = vec![order(
        10,
        "Buy",
        "Rejected",
        Some("order notional 45.6 exceeds per-order notional limit 40"),
    )];
    let mut h = wide_harness(state_with_orders(orders), 1180.0);
    h.run();
    assert!(has_text(&h, "Rejected"));
    assert!(has_text(&h, "exceeds per-order notional limit 40"));
}

#[test]
fn with_no_orders_yet_the_panel_says_what_it_is_for() {
    let mut h = wide_harness(state_with_orders(Vec::new()), 1180.0);
    h.run();
    assert!(has_text(&h, "Recent orders"));
    assert!(has_text(&h, "no orders yet"));
    assert!(has_text(&h, "appear here"));
}

#[test]
fn the_panel_is_present_in_a_wide_window_and_in_a_narrow_one() {
    for width in [1180.0, 760.0] {
        let mut h = wide_harness(
            state_with_orders(vec![order(5, "Buy", "Filled", None)]),
            width,
        );
        h.run();
        assert!(has_text(&h, "Recent orders"), "missing at width {width}");
        assert!(
            has_text(&h, "HALT TRADING"),
            "HALT must stay reachable at width {width}"
        );
    }
}

/// The panel once grew by a few pixels on every frame because each row asked
/// for slightly more than the width it was given. A resizable panel must settle.
#[test]
fn the_panel_settles_and_does_not_grow_frame_after_frame() {
    let orders: Vec<_> = (0..12)
        .map(|i| {
            order(
                10 + i,
                if i % 2 == 0 { "Buy" } else { "Sell" },
                "Filled",
                None,
            )
        })
        .collect();
    let mut h = wide_harness(state_with_orders(orders), 1180.0);
    for _ in 0..3 {
        h.step();
    }
    let x_of_panel = |h: &Harness<'static, Fixture>| h.get_by_label("Recent orders").rect().min.x;
    let settled = x_of_panel(&h);
    for _ in 0..20 {
        h.step();
    }
    assert_eq!(
        x_of_panel(&h),
        settled,
        "the panel moved across frames, so it is still growing"
    );
}

/// `n` orders, newest first, each a second apart. The oldest is on `ZZZUSDT`
/// so it can be told apart from the rest.
fn many_orders(n: i64) -> Vec<apexes_backtest::OrderRecord> {
    (0..n)
        .map(|i| {
            let mut o = order(
                10 + i,
                if i % 2 == 0 { "Buy" } else { "Sell" },
                "Filled",
                None,
            );
            if i == n - 1 {
                o.symbol = "ZZZUSDT".to_string();
            }
            o
        })
        .collect()
}

/// The y of the card under the orders panel's list: it moves down if the list
/// grows, so it measures the list's height without naming any widget.
fn y_of_card_under_orders(h: &Harness<'static, Fixture>) -> f32 {
    h.get_by_label("Positions").rect().min.y
}

/// The list is about ten orders tall however many there are: 45 orders take
/// no more room than 10, and fewer orders take less.
#[test]
fn the_orders_list_is_ten_rows_tall_however_many_orders_there_are() {
    let y = |n: i64| {
        let mut h = wide_harness(state_with_orders(many_orders(n)), 1180.0);
        h.run();
        y_of_card_under_orders(&h)
    };
    let (three, ten, forty_five, hundred) = (y(3), y(10), y(45), y(100));
    assert!(three < ten, "fewer orders take less room: {three} vs {ten}");
    assert!(
        (ten - forty_five).abs() < 2.0 && (ten - hundred).abs() < 2.0,
        "the list is capped at ten rows: 10 -> {ten}, 45 -> {forty_five}, 100 -> {hundred}"
    );
}

/// "Ten rows" is the visible height: the eleventh order starts past the end of
/// the box and the oldest is far past it, so the box has to scroll to reach them.
#[test]
fn only_ten_orders_are_visible_and_the_rest_are_reached_by_scrolling() {
    let mut h = wide_harness(state_with_orders(many_orders(45)), 1180.0);
    h.run();
    let oldest = h.get_by_label("ZZZUSDT");
    let before = oldest.rect().min.y;
    let box_bottom = y_of_card_under_orders(&h);
    assert!(
        before > box_bottom,
        "the oldest order is below the list's visible end: {before} vs {box_bottom}"
    );
    oldest.scroll_to_me();
    h.run();
    let after = h.get_by_label("ZZZUSDT").rect().min.y;
    assert!(
        after < box_bottom && after < before,
        "scrolling brings the oldest order into the box: {before} -> {after} (box ends {box_bottom})"
    );
}

/// The column names stay put while the list scrolls (they are above it, not in it).
#[test]
fn the_column_headings_stay_in_place_while_the_orders_scroll() {
    let mut h = wide_harness(state_with_orders(many_orders(45)), 1180.0);
    h.run();
    let head = h.get_by_label("Status").rect().min.y;
    h.get_by_label("ZZZUSDT").scroll_to_me();
    h.run();
    assert_eq!(h.get_by_label("Status").rect().min.y, head);
}

/// A refused order carries a reason line under it, so ten of those are taller
/// than ten plain rows. The box is sized from real heights, so it still shows
/// ten orders and never cuts the tenth one in half.
#[test]
fn ten_refused_orders_get_a_taller_box_than_ten_plain_ones_and_none_is_cut() {
    let refused: Vec<_> = (0..45)
        .map(|i| {
            order(
                10 + i,
                "Buy",
                "Rejected",
                Some(
                    "quantity 0.0004 is below the 0.001 step: it rounds to nothing (minimum 0.001)",
                ),
            )
        })
        .collect();
    let mut h = wide_harness(state_with_orders(refused), 1180.0);
    h.run();
    let tall = y_of_card_under_orders(&h);
    let mut h = wide_harness(state_with_orders(many_orders(45)), 1180.0);
    h.run();
    let plain = y_of_card_under_orders(&h);
    assert!(tall > plain + 40.0, "reasons add height: {tall} vs {plain}");
}

/// Every recorded order can be reached, not only the newest thirty the old
/// "show older" button stopped at.
#[test]
fn all_one_hundred_recorded_orders_are_reachable() {
    let mut h = wide_harness(state_with_orders(many_orders(100)), 1180.0);
    h.run();
    assert!(has_text(&h, "100 orders"));
    h.get_by_label("ZZZUSDT").scroll_to_me();
    h.run();
    assert!(has_text(&h, "ZZZUSDT"));
}

// ---- Reachability at the smallest window the app allows -------------------

fn small_window(state: OperatorState) -> Harness<'static, Fixture> {
    // chrome::MIN_WINDOW_SIZE: the smallest window the app lets an operator make.
    Harness::builder()
        .with_size(egui::vec2(760.0, 520.0))
        .build_ui_state(
            |ui, (state, sent): &mut Fixture| sent.extend(show(ui, state)),
            (state, Vec::new()),
        )
}

/// At the smallest window the page scrolls, but HALT is in the fixed top part:
/// it must be on screen without scrolling, always.
#[test]
fn halt_is_visible_without_scrolling_at_the_smallest_window() {
    let mut h = small_window(state("operator", false));
    h.run();
    let halt = h.get_by_label("HALT TRADING").rect();
    assert!(
        halt.min.y >= 0.0 && halt.max.y <= 520.0,
        "HALT is on screen: {halt:?}"
    );
    assert!(halt.min.x >= 0.0 && halt.max.x <= 760.0, "{halt:?}");
}

/// Everything below the fixed top scrolls: the parameter Set button is
/// reachable by scrolling to it, and clicking it works.
#[test]
fn a_parameter_can_still_be_set_at_the_smallest_window_by_scrolling() {
    let mut h = small_window(state("operator", false));
    h.run();
    h.state_mut()
        .0
        .param_edits
        .insert("threshold".to_string(), "7.5".to_string());
    h.run();
    h.get_by_label("Set").scroll_to_me();
    h.run();
    h.get_by_label("Set").click();
    h.run();
    assert_eq!(
        h.state().1,
        vec![SessionCommand::SetParam {
            name: "threshold".to_string(),
            value: dec!(7.5)
        }]
    );
}
