//! The operator console's client against the REAL engine router
//! (`apexes_engine::build_app_secured`): authentication, roles, snapshots
//! with open orders, halt/resume reaching the kill switch, and the
//! "never replay stale commands" guarantee.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use apexes_backtest::{OpenOrderView, SessionCommand, SessionSnapshot};
use apexes_engine::{ServerSecurity, build_app_secured};
use apexes_oms::KillSwitch;
use apexes_security::TokenSet;
use apexes_ui::client::ConnectionStatus;
use apexes_ui::operator::{OperatorAction, OperatorState, Requested};
use apexes_ui::operator_client::{OperatorClientConfig, run};
use chrono::Utc;
use rust_decimal_macros::dec;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

const ALICE: &str = "alice-operator-token-0001";
const BOB: &str = "bob-viewer-token-000002";

fn snapshot() -> SessionSnapshot {
    SessionSnapshot {
        identity: None,
        evaluation: None,
        equity_history: Vec::new(),
        positions: HashMap::new(),
        balances: HashMap::from([("USDT".to_string(), dec!(500))]),
        equity: dec!(500),
        realized_pnl: HashMap::new(),
        margin_status: HashMap::new(),
        params: HashMap::from([("threshold".to_string(), dec!(5))]),
        paused: false,
        open_orders: vec![OpenOrderView {
            id: "o-1".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: "Buy".to_string(),
            order_type: "Limit".to_string(),
            quantity: dec!(1),
            filled_quantity: dec!(0),
            price: Some(dec!(50)),
            status: "New".to_string(),
            created_at: Utc::now(),
        }],
        timestamp: Utc::now(),
    }
}

/// Keeps the loop-side channel halves alive for the life of the test.
struct Engine {
    kill_switch: Arc<KillSwitch>,
    _snapshot_tx: watch::Sender<SessionSnapshot>,
    _command_rx: mpsc::UnboundedReceiver<SessionCommand>,
    _stop_rx: tokio::sync::oneshot::Receiver<()>,
}

fn engine_router() -> (axum::Router, Engine) {
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (snapshot_tx, snapshot_rx) = watch::channel(snapshot());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let kill_switch = Arc::new(KillSwitch::new());
    let security = ServerSecurity {
        tokens: Some(Arc::new(
            TokenSet::from_spec(&format!("alice:operator:{ALICE},bob:viewer:{BOB}")).unwrap(),
        )),
        audit: None,
    };
    let app = build_app_secured(
        command_tx,
        snapshot_rx,
        stop_tx,
        Some(Arc::clone(&kill_switch)),
        security,
    );
    (
        app,
        Engine {
            kill_switch,
            _snapshot_tx: snapshot_tx,
            _command_rx: command_rx,
            _stop_rx: stop_rx,
        },
    )
}

struct Console {
    state: OperatorState,
    events: mpsc::UnboundedReceiver<apexes_engine::SessionMessage>,
    status: watch::Receiver<ConnectionStatus>,
    commands: mpsc::UnboundedSender<SessionCommand>,
}

fn start_console(port: u16, token: Option<&str>) -> Console {
    let (event_tx, events) = mpsc::unbounded_channel();
    let (status_tx, status) = watch::channel(ConnectionStatus::default());
    let (commands, command_rx) = mpsc::unbounded_channel();
    tokio::spawn(run(
        OperatorClientConfig {
            url: format!("ws://127.0.0.1:{port}/session/ws"),
            token: token.map(str::to_string),
        },
        event_tx,
        status_tx,
        command_rx,
    ));
    Console {
        state: OperatorState::default(),
        events,
        status,
        commands,
    }
}

impl Console {
    /// Folds incoming events/connection changes into the state until `done`.
    async fn pump_until(&mut self, what: &str, done: impl Fn(&OperatorState) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            self.state.set_connection(*self.status.borrow());
            while let Ok(message) = self.events.try_recv() {
                self.state.apply(message);
            }
            if done(&self.state) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn send(&self, command: SessionCommand) {
        self.commands.send(command).unwrap();
    }
}

async fn serve(app: axum::Router) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    port
}

#[tokio::test]
async fn an_operator_sees_the_session_and_halting_reaches_the_kill_switch() {
    let (app, engine) = engine_router();
    let port = serve(app).await;
    let mut console = start_console(port, Some(ALICE));

    console
        .pump_until("identity + first snapshot", |s| {
            s.can_command() && s.snapshot.is_some()
        })
        .await;
    let snapshot = console.state.snapshot.as_ref().unwrap();
    assert_eq!(
        snapshot.open_orders.len(),
        1,
        "open orders reach the console"
    );
    assert_eq!(snapshot.params["threshold"], dec!(5));
    assert_eq!(console.state.status.as_ref().unwrap().actor, "alice");
    assert!(!console.state.halted());

    // One click -> Halt -> kill switch -> Status back to the console.
    let Requested::Send(command) = console.state.request(OperatorAction::Halt) else {
        panic!("halt must be sendable immediately");
    };
    console.send(command);
    console
        .pump_until("halt to be reflected", |s| s.halted())
        .await;
    assert!(engine.kill_switch.is_engaged());

    // Resume needs confirmation, then releases the switch.
    assert_eq!(
        console.state.request(OperatorAction::ResumeTrading),
        Requested::NeedsConfirmation
    );
    let confirmed = console.state.confirm().expect("confirmed");
    console.send(confirmed);
    console
        .pump_until("resume to be reflected", |s| !s.halted())
        .await;
    assert!(!engine.kill_switch.is_engaged());
}

#[tokio::test]
async fn a_viewer_sees_the_session_but_cannot_command_even_if_it_tries() {
    let (app, engine) = engine_router();
    let port = serve(app).await;
    let mut console = start_console(port, Some(BOB));

    console
        .pump_until("viewer identity", |s| {
            s.status.as_ref().is_some_and(|st| st.role == "viewer") && s.snapshot.is_some()
        })
        .await;
    assert!(!console.state.can_command());
    // The console itself refuses...
    assert!(matches!(
        console.state.request(OperatorAction::Halt),
        Requested::Refused(_)
    ));
    // ...and so does the engine if a modified client sends one anyway.
    console.send(SessionCommand::Halt {
        reason: "bypassing the UI".to_string(),
        flatten: false,
    });
    console
        .pump_until("engine refusal", |s| {
            s.notice.as_deref().is_some_and(|n| n.contains("viewer"))
        })
        .await;
    assert!(!engine.kill_switch.is_engaged());
}

#[tokio::test]
async fn a_wrong_token_never_connects() {
    let (app, _engine) = engine_router();
    let port = serve(app).await;
    let mut console = start_console(port, Some("not-a-valid-token-at-all"));

    tokio::time::sleep(Duration::from_millis(800)).await;
    console.state.set_connection(*console.status.borrow());
    while let Ok(m) = console.events.try_recv() {
        console.state.apply(m);
    }
    assert!(!console.state.connected());
    assert!(console.state.snapshot.is_none());
}

#[tokio::test]
async fn commands_clicked_while_the_engine_is_unreachable_are_never_replayed() {
    // Reserve a port, then release it so nothing is listening yet.
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = reserved.local_addr().unwrap().port();
    drop(reserved);

    let mut console = start_console(port, Some(ALICE));
    tokio::time::sleep(Duration::from_millis(100)).await; // first connect attempt failed
    // A stale click against a dead link...
    console.send(SessionCommand::Halt {
        reason: "stale click".to_string(),
        flatten: true,
    });

    // ...then the engine comes up on that port.
    let (app, engine) = engine_router();
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    console
        .pump_until("reconnect", |s| s.can_command() && s.snapshot.is_some())
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !engine.kill_switch.is_engaged(),
        "a command issued while disconnected must not fire after reconnect"
    );
    assert!(!engine.kill_switch.flatten_requested());
}
