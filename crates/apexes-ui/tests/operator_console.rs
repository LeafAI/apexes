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
    }
}

fn state(role: &str, halted: bool) -> OperatorState {
    let mut s = OperatorState::default();
    s.set_connection(ConnectionStatus::Connected);
    s.apply(SessionMessage::Status(status(role, halted)));
    s.apply(SessionMessage::Snapshot(snapshot()));
    s
}

fn harness(state: OperatorState) -> Harness<'static, Fixture> {
    Harness::new_ui_state(
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
