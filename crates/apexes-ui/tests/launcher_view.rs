//! Headless `egui_kittest` tests of the launcher: what the operator sees and
//! what their clicks produce.

use apexes_core::binance_network::BinanceNetwork;
use apexes_supervisor::{SessionId, SessionInfo};
use apexes_ui::launcher::LauncherState;
use apexes_ui::launcher_view::{LauncherAction, show};
use egui_kittest::Harness;
use egui_kittest::kittest::Queryable;

type Fixture = (LauncherState, Option<String>, Vec<LauncherAction>);

fn session(id: &str, alive: bool, network: BinanceNetwork) -> SessionInfo {
    SessionInfo {
        id: SessionId::parse(id).unwrap(),
        pid: 10,
        port: 20,
        network: Some(network),
        alive,
        control_url: "ws://127.0.0.1:20/session/ws".to_string(),
        token: Some("t".repeat(32)),
        log_path: "/tmp/x.log".into(),
    }
}

fn harness(state: LauncherState, pending: Option<&str>) -> Harness<'static, Fixture> {
    Harness::new_ui_state(
        |ui, (state, pending, sent): &mut Fixture| {
            sent.extend(show(ui, state, pending.as_deref()));
        },
        (state, pending.map(str::to_string), Vec::new()),
    )
}

fn has(h: &Harness<'static, Fixture>, text: &str) -> bool {
    h.query_all_by_label_contains(text).next().is_some()
}

#[test]
fn the_default_form_is_testnet_and_launch_is_enabled() {
    let mut h = harness(LauncherState::default(), None);
    h.run();
    assert!(has(&h, "Launch on TESTNET"));
    assert!(!has(&h, "Type MAINNET to confirm"));
    h.get_by_label("Launch on TESTNET").click();
    h.run();
    assert_eq!(h.state().2, vec![LauncherAction::Launch]);
}

#[test]
fn mainnet_shows_a_real_funds_warning_and_a_disabled_button_until_confirmed() {
    let mut state = LauncherState::default();
    state.form.set_network(BinanceNetwork::Mainnet);
    let mut h = harness(state, None);
    h.run();
    assert!(has(&h, "real funds"));
    assert!(has(&h, "Type MAINNET to confirm"));
    assert!(has(&h, "Launch on MAINNET"));
    h.get_by_label_contains("Launch on MAINNET").click();
    h.run();
    assert!(h.state().2.is_empty(), "a disabled button sends nothing");
}

#[test]
fn a_missing_credential_is_named_and_blocks_launch() {
    let state = LauncherState {
        missing_credential: Some("APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY".to_string()),
        ..LauncherState::default()
    };
    let mut h = harness(state, None);
    h.run();
    assert!(has(&h, "APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY"));
    h.get_by_label("Launch on TESTNET").click();
    h.run();
    assert!(h.state().2.is_empty());
}

#[test]
fn an_invalid_field_is_shown_and_blocks_launch() {
    let mut state = LauncherState::default();
    state.form.max_order_quantity = "lots".to_string();
    let mut h = harness(state, None);
    h.run();
    assert!(has(&h, "max_order_quantity"));
    h.get_by_label("Launch on TESTNET").click();
    h.run();
    assert!(h.state().2.is_empty());
}

#[test]
fn a_failed_launch_shows_the_reason() {
    let state = LauncherState {
        status: apexes_ui::launcher::LaunchStatus::Failed(
            "engine exited during startup: recovery refused".to_string(),
        ),
        ..LauncherState::default()
    };
    let mut h = harness(state, None);
    h.run();
    assert!(has(&h, "Launch failed"));
    assert!(has(&h, "recovery refused"));
}

#[test]
fn sessions_show_their_network_and_state_and_offer_the_right_buttons() {
    let state = LauncherState {
        sessions: vec![
            session("live-a", true, BinanceNetwork::Testnet),
            session("old-b", false, BinanceNetwork::Mainnet),
        ],
        ..LauncherState::default()
    };
    let mut h = harness(state, None);
    h.run();
    assert!(has(&h, "live-a") && has(&h, "old-b"));
    assert!(has(&h, "TESTNET") && has(&h, "MAINNET"));
    assert!(has(&h, "running") && has(&h, "stopped"));
    assert!(has(&h, "Open console"));
    assert!(has(&h, "Stop"));
    assert!(has(&h, "Remove files"));

    h.get_by_label("Open console").click();
    h.run();
    assert_eq!(
        h.state().2,
        vec![LauncherAction::Connect {
            session_id: "live-a".to_string()
        }]
    );
}

#[test]
fn stopping_needs_a_second_deliberate_click() {
    let state = LauncherState {
        sessions: vec![session("live-a", true, BinanceNetwork::Testnet)],
        ..LauncherState::default()
    };

    // First click only asks.
    let mut h = harness(state, None);
    h.run();
    h.get_by_label("Stop").click();
    h.run();
    assert_eq!(
        h.state().2,
        vec![LauncherAction::RequestStop {
            session_id: "live-a".to_string()
        }]
    );

    // With the confirmation showing, Yes sends the stop.
    let state = LauncherState {
        sessions: vec![session("live-a", true, BinanceNetwork::Testnet)],
        ..LauncherState::default()
    };
    let mut h = harness(state, Some("live-a"));
    h.run();
    assert!(has(&h, "Stop this session?"));
    h.get_by_label("Yes, stop").click();
    h.run();
    assert_eq!(
        h.state().2,
        vec![LauncherAction::ConfirmStop {
            session_id: "live-a".to_string()
        }]
    );
}

#[test]
fn a_session_without_a_saved_token_cannot_be_opened() {
    let mut info = session("foreign", true, BinanceNetwork::Testnet);
    info.token = None;
    let state = LauncherState {
        sessions: vec![info],
        ..LauncherState::default()
    };
    let mut h = harness(state, None);
    h.run();
    assert!(has(&h, "no saved token"));
    h.get_by_label("Open console").click();
    h.run();
    assert!(h.state().2.is_empty());
}

#[test]
fn an_empty_session_list_says_so() {
    let mut h = harness(LauncherState::default(), None);
    h.run();
    assert!(has(&h, "No sessions found"));
}
