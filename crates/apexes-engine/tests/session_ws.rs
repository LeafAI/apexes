//! Integration test for `apexes_engine::build_app` / `GET /session/ws`
//! (Task 8): a real `127.0.0.1:0` bind, a real `tokio-tungstenite`
//! client, verifying both directions of the protocol:
//!
//! - Engine -> UI: a `watch::Sender<SessionSnapshot>` update (the SAME
//!   kind of push `ControlHandle::maybe_push_snapshot` performs inside
//!   the unified loop) is observed by the client as a
//!   `SessionMessage::Snapshot` frame.
//! - UI -> Engine: a `SessionMessage::Command` frame sent by the
//!   client is observed, in order, on the `mpsc::UnboundedReceiver`
//!   side of the exact `command_tx` handed to `build_app` -- the SAME
//!   sender `ControlHandle::drain_commands` (Task 2, `apexes-backtest`,
//!   `pub(crate)` and therefore not directly callable from this crate)
//!   reads from every tick inside the real unified loop. Verifying the
//!   command lands on this receiver, in the shape `drain_commands`
//!   expects, is the correct boundary for this task to test: Task 2's
//!   own unit tests (`apexes-backtest/src/control.rs`) already cover
//!   `drain_commands` actually applying a received `SetParam` to a
//!   `Strategy`.
//!
//! Deliberately does NOT go through `apexes_backtest::ControlHandle`
//! itself (`ControlHandle::new` returns the handle holding the
//! `command_rx`/`snapshot_tx` halves privately -- there is no public
//! way to peek into it from outside `apexes-backtest`) -- this test
//! instead constructs its own `mpsc`/`watch` channel pair directly,
//! passing the same-shaped halves `build_app` expects, which is
//! exactly what `run_inner` does when it calls `build_control_handle`
//! and keeps the `Sender`/`Receiver` halves for `build_app`.

use apexes_backtest::{SessionCommand, SessionSnapshot};
use apexes_engine::SessionMessage;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

fn empty_snapshot() -> SessionSnapshot {
    SessionSnapshot {
        identity: None,
        evaluation: None,
        equity_history: Vec::new(),
        recent_orders: Vec::new(),
        price_history: Default::default(),
        equity_priced: true,
        positions: HashMap::new(),
        balances: HashMap::new(),
        equity: Decimal::ZERO,
        realized_pnl: HashMap::new(),
        margin_status: HashMap::new(),
        params: HashMap::new(),
        paused: false,
        open_orders: Vec::new(),
        timestamp: Utc::now(),
    }
}

#[tokio::test]
async fn session_ws_forwards_snapshots_out_and_commands_in() {
    let (command_tx, mut command_rx) = mpsc::unbounded_channel::<SessionCommand>();
    let (snapshot_tx, snapshot_rx) = watch::channel(empty_snapshot());
    let (stop_tx, _stop_rx) = tokio::sync::oneshot::channel();

    let app = apexes_engine::build_app(command_tx, snapshot_rx, stop_tx);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let (mut ws_stream, _response) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/session/ws"))
            .await
            .expect("client failed to connect to /session/ws");

    // Engine -> UI: push a new snapshot with a distinguishable value
    // and assert the client receives it, correctly enveloped.
    let mut pushed_snapshot = empty_snapshot();
    pushed_snapshot.equity = dec!(1234.5);
    snapshot_tx
        .send(pushed_snapshot.clone())
        .expect("snapshot receiver should still be alive (server task holds it)");

    // The engine also sends a `Status` frame on connect; wait for the Snapshot.
    let snapshot = loop {
        let received = tokio::time::timeout(Duration::from_secs(5), ws_stream.next())
            .await
            .expect("timed out waiting for Snapshot frame")
            .expect("stream ended unexpectedly")
            .expect("received a WS error frame");
        let Message::Text(text) = received else {
            panic!("expected a Text frame, got {received:?}");
        };
        let message: SessionMessage =
            serde_json::from_str(&text).expect("failed to parse SessionMessage JSON");
        match message {
            // The current (initial) snapshot is sent on connect; keep reading
            // until the one we pushed arrives.
            SessionMessage::Snapshot(snapshot) if snapshot.equity == dec!(1234.5) => {
                break snapshot;
            }
            SessionMessage::Snapshot(_) | SessionMessage::Status(_) => continue,
            other => panic!("unexpected frame {other:?}"),
        }
    };
    assert_eq!(snapshot.equity, dec!(1234.5));

    // UI -> Engine: send a SetParam command and assert it lands on the
    // receiver side of the exact `command_tx` given to `build_app`.
    let command_message = SessionMessage::Command(SessionCommand::SetParam {
        name: "threshold".to_string(),
        value: dec!(42),
    });
    let json = serde_json::to_string(&command_message).unwrap();
    ws_stream
        .send(Message::Text(json.into()))
        .await
        .expect("failed to send Command frame");

    let received_command = tokio::time::timeout(Duration::from_secs(5), command_rx.recv())
        .await
        .expect("timed out waiting for forwarded SessionCommand")
        .expect("command_rx closed unexpectedly");
    match received_command {
        SessionCommand::SetParam { name, value } => {
            assert_eq!(name, "threshold");
            assert_eq!(value, dec!(42));
        }
        other => panic!("expected SetParam, got {other:?}"),
    }
}

/// Task 8b: a `SessionCommand::Stop` frame must ALSO fire the stop
/// `oneshot` handed into `build_app` -- not just land on `command_tx`
/// -- so `take_until(stop_signal)` in `run_paper_trading`/
/// `run_live_trading` unblocks a STALLED market-event stream
/// immediately, rather than waiting for `ControlHandle::
/// drain_commands` to next be polled (which only happens when a new
/// tick arrives). This test asserts the real `oneshot::Receiver` side
/// resolves shortly after a real client sends `Stop` over a real WS
/// connection -- proving the wiring described in `server.rs`'s
/// top-level doc comment actually works end-to-end.
#[tokio::test]
async fn session_ws_stop_command_also_fires_the_stop_oneshot() {
    let (command_tx, mut command_rx) = mpsc::unbounded_channel::<SessionCommand>();
    let (_snapshot_tx, snapshot_rx) = watch::channel(empty_snapshot());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();

    let app = apexes_engine::build_app(command_tx, snapshot_rx, stop_tx);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let (mut ws_stream, _response) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/session/ws"))
            .await
            .expect("client failed to connect to /session/ws");

    let stop_message = SessionMessage::Command(SessionCommand::Stop);
    let json = serde_json::to_string(&stop_message).unwrap();
    ws_stream
        .send(Message::Text(json.into()))
        .await
        .expect("failed to send Stop Command frame");

    // The stop oneshot must resolve promptly -- this is the whole
    // point of the fix: it must NOT require any further market event
    // or channel activity to unblock.
    tokio::time::timeout(Duration::from_secs(5), stop_rx)
        .await
        .expect("timed out waiting for the stop oneshot to resolve after sending Stop")
        .expect("stop oneshot sender was dropped without sending");

    // Stop must ALSO still land on command_tx as before (so
    // ControlHandle::drain_commands still sees it, e.g. if the loop
    // wasn't actually stalled).
    let received_command = tokio::time::timeout(Duration::from_secs(5), command_rx.recv())
        .await
        .expect("timed out waiting for forwarded Stop SessionCommand")
        .expect("command_rx closed unexpectedly");
    assert!(matches!(received_command, SessionCommand::Stop));
}

/// A second `Stop` (from the same client re-sending, simulating what
/// two connected clients both sending `Stop` would also produce on
/// the shared sender) must not panic or hang the connection -- the
/// oneshot sender is already consumed after the first `Stop`, and the
/// second send must be a harmless no-op.
#[tokio::test]
async fn session_ws_duplicate_stop_commands_do_not_panic() {
    let (command_tx, mut command_rx) = mpsc::unbounded_channel::<SessionCommand>();
    let (_snapshot_tx, snapshot_rx) = watch::channel(empty_snapshot());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();

    let app = apexes_engine::build_app(command_tx, snapshot_rx, stop_tx);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let (mut ws_stream, _response) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/session/ws"))
            .await
            .expect("client failed to connect to /session/ws");

    let stop_message = SessionMessage::Command(SessionCommand::Stop);
    let json = serde_json::to_string(&stop_message).unwrap();
    for _ in 0..2 {
        ws_stream
            .send(Message::Text(json.clone().into()))
            .await
            .expect("failed to send Stop Command frame");
    }

    tokio::time::timeout(Duration::from_secs(5), stop_rx)
        .await
        .expect("timed out waiting for the stop oneshot to resolve")
        .expect("stop oneshot sender was dropped without sending");

    for _ in 0..2 {
        let received_command = tokio::time::timeout(Duration::from_secs(5), command_rx.recv())
            .await
            .expect("timed out waiting for forwarded Stop SessionCommand")
            .expect("command_rx closed unexpectedly");
        assert!(matches!(received_command, SessionCommand::Stop));
    }
}

#[tokio::test]
async fn halt_and_resume_commands_engage_and_release_the_kill_switch_without_a_loop() {
    // Nothing consumes `command_rx` (the receiver is held but never
    // drained), modelling a trading loop stalled on a dead market feed:
    // the kill switch must still engage.
    let (command_tx, _command_rx) = mpsc::unbounded_channel::<SessionCommand>();
    let (_snapshot_tx, snapshot_rx) = watch::channel(empty_snapshot());
    let (stop_tx, _stop_rx) = tokio::sync::oneshot::channel();
    let kill_switch = std::sync::Arc::new(apexes_oms::KillSwitch::new());

    let app = apexes_engine::build_app_with_kill_switch(
        command_tx,
        snapshot_rx,
        stop_tx,
        Some(std::sync::Arc::clone(&kill_switch)),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let (mut ws_stream, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/session/ws"))
            .await
            .unwrap();

    let send = |c: SessionCommand| serde_json::to_string(&SessionMessage::Command(c)).unwrap();
    ws_stream
        .send(Message::Text(
            send(SessionCommand::Halt {
                reason: "operator".to_string(),
                flatten: true,
            })
            .into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), kill_switch.wait_engaged())
        .await
        .expect("kill switch was not engaged by Halt");
    assert_eq!(kill_switch.reason().as_deref(), Some("operator"));
    assert!(
        kill_switch.flatten_requested(),
        "a Halt with flatten set must request a flatten"
    );

    ws_stream
        .send(Message::Text(send(SessionCommand::ResumeTrading).into()))
        .await
        .unwrap();
    for _ in 0..50 {
        if !kill_switch.is_engaged() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("kill switch was not released by ResumeTrading");
}

/// Minimal HTTP/1.0 GET over a raw socket (no HTTP client dev-dependency).
async fn http_get(port: u16, path: &str) -> (u16, String) {
    http_get_with(port, path, None).await
}

async fn http_get_with(port: u16, path: &str, bearer: Option<&str>) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let auth = bearer
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.0\r\nHost: localhost\r\n{auth}\r\n").as_bytes())
        .await
        .unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let status = raw.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = raw.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

#[tokio::test]
async fn metrics_health_and_readiness_endpoints_reflect_kill_switch_state() {
    let (command_tx, _command_rx) = mpsc::unbounded_channel::<SessionCommand>();
    let (_snapshot_tx, snapshot_rx) = watch::channel(empty_snapshot());
    let (stop_tx, _stop_rx) = tokio::sync::oneshot::channel();
    let kill_switch = std::sync::Arc::new(apexes_oms::KillSwitch::new());
    let app = apexes_engine::build_app_with_kill_switch(
        command_tx,
        snapshot_rx,
        stop_tx,
        Some(std::sync::Arc::clone(&kill_switch)),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let (status, body) = http_get(port, "/healthz").await;
    assert_eq!(status, 200);
    assert!(body.contains("\"status\":\"ok\""), "{body}");
    assert_eq!(http_get(port, "/readyz").await.0, 200);

    let (status, body) = http_get(port, "/metrics").await;
    assert_eq!(status, 200);
    assert!(body.contains("apexes_kill_switch_engaged"), "{body}");

    kill_switch.engage("maintenance");
    let (status, body) = http_get(port, "/healthz").await;
    assert_eq!(status, 200, "liveness stays 200 while halted");
    assert!(body.contains("\"status\":\"halted\"") && body.contains("maintenance"));
    assert_eq!(http_get(port, "/readyz").await.0, 503);
}

async fn spawn_secured_app(
    security: apexes_engine::ServerSecurity,
) -> (
    u16,
    std::sync::Arc<apexes_oms::KillSwitch>,
    impl Sized, // keeps the loop-side channel halves alive
) {
    let (command_tx, command_rx) = mpsc::unbounded_channel::<SessionCommand>();
    let (snapshot_tx, snapshot_rx) = watch::channel(empty_snapshot());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let kill_switch = std::sync::Arc::new(apexes_oms::KillSwitch::new());
    let app = apexes_engine::build_app_secured(
        command_tx,
        snapshot_rx,
        stop_tx,
        Some(std::sync::Arc::clone(&kill_switch)),
        security,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (port, kill_switch, (command_rx, snapshot_tx, stop_rx))
}

#[tokio::test]
async fn secured_endpoint_rejects_unauthenticated_clients_and_audits_commands() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let dir = std::env::temp_dir().join(format!("apexes-engine-audit-{}", std::process::id()));
    let audit_path = dir.join("audit.jsonl");
    let security = apexes_engine::ServerSecurity {
        tokens: Some(std::sync::Arc::new(apexes_security::TokenSet::single(
            "ctl",
        ))),
        audit: Some(std::sync::Arc::new(
            apexes_security::AuditLog::open(&audit_path).unwrap(),
        )),
    };
    let (port, kill_switch, _keepalive) = spawn_secured_app(security).await;
    let url = format!("ws://127.0.0.1:{port}/session/ws");

    // No token / wrong token: handshake refused with 401.
    let err = tokio_tungstenite::connect_async(&url).await.unwrap_err();
    assert!(
        matches!(&err, tokio_tungstenite::tungstenite::Error::Http(r) if r.status() == 401),
        "{err:?}"
    );
    let mut wrong = url.as_str().into_client_request().unwrap();
    wrong
        .headers_mut()
        .insert("Authorization", "Bearer nope".parse().unwrap());
    assert!(tokio_tungstenite::connect_async(wrong).await.is_err());

    // /metrics is protected; /healthz is not.
    assert_eq!(http_get(port, "/metrics").await.0, 401);
    assert_eq!(http_get(port, "/healthz").await.0, 200);

    // Correct token: connects, and a Halt both takes effect and is audited.
    let mut good = url.as_str().into_client_request().unwrap();
    good.headers_mut()
        .insert("Authorization", "Bearer ctl".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(good).await.unwrap();
    let halt = SessionMessage::Command(SessionCommand::Halt {
        reason: "audit-me".to_string(),
        flatten: false,
    });
    ws.send(Message::Text(serde_json::to_string(&halt).unwrap().into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), kill_switch.wait_engaged())
        .await
        .expect("halt not applied");

    let records: Vec<serde_json::Value> = std::fs::read_to_string(&audit_path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let actions: Vec<&str> = records
        .iter()
        .map(|r| r["action"].as_str().unwrap())
        .collect();
    assert!(actions.contains(&"unauthorized_request"), "{actions:?}");
    assert!(actions.contains(&"ws_connect"), "{actions:?}");
    let command = records.iter().find(|r| r["action"] == "command").unwrap();
    assert_eq!(command["actor"], "shared");
    assert!(command.to_string().contains("audit-me"));
    // The secret itself is never written.
    assert!(
        !std::fs::read_to_string(&audit_path)
            .unwrap()
            .contains("ctl\"")
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The exact frames documented in docs/RUNBOOK.md.
#[test]
fn runbook_halt_and_resume_frames_parse() {
    let halt: SessionMessage =
        serde_json::from_str(r#"{"type":"Command","Halt":{"reason":"operator: investigating"}}"#)
            .unwrap();
    assert!(matches!(
        halt,
        SessionMessage::Command(SessionCommand::Halt { ref reason, flatten: false }) if reason.starts_with("operator")
    ));
    let resume: SessionMessage =
        serde_json::from_str(r#"{"type":"Command","ResumeTrading":null}"#).unwrap();
    assert!(matches!(
        resume,
        SessionMessage::Command(SessionCommand::ResumeTrading)
    ));
}

// ---- per-user identity and roles -------------------------------------

async fn next_status(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> apexes_engine::EngineStatus {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timed out waiting for a Status frame")
            .expect("stream ended")
            .expect("ws error");
        if let Message::Text(text) = frame
            && let SessionMessage::Status(status) = serde_json::from_str(&text).unwrap()
        {
            return status;
        }
    }
}

async fn connect_as(
    port: u16,
    token: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut request = format!("ws://127.0.0.1:{port}/session/ws")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Authorization", format!("Bearer {token}").parse().unwrap());
    tokio_tungstenite::connect_async(request).await.unwrap().0
}

#[tokio::test]
async fn viewers_can_watch_but_not_command_and_actions_are_attributed_to_people() {
    const ALICE: &str = "alice-operator-token-0001";
    const BOB: &str = "bob-viewer-token-000002";
    let dir = std::env::temp_dir().join(format!("apexes-engine-roles-{}", std::process::id()));
    let audit_path = dir.join("audit.jsonl");
    let security = apexes_engine::ServerSecurity {
        tokens: Some(std::sync::Arc::new(
            apexes_security::TokenSet::from_spec(&format!(
                "alice:operator:{ALICE},bob:viewer:{BOB}"
            ))
            .unwrap(),
        )),
        audit: Some(std::sync::Arc::new(
            apexes_security::AuditLog::open(&audit_path).unwrap(),
        )),
    };
    let (port, kill_switch, _keepalive) = spawn_secured_app(security).await;

    // Each connection is told who it is and what it may do.
    let mut bob = connect_as(port, BOB).await;
    let status = next_status(&mut bob).await;
    assert_eq!(
        (status.actor.as_str(), status.role.as_str()),
        ("bob", "viewer")
    );
    assert!(!status.halted);

    // A viewer's Halt is refused: nothing happens, and the viewer is told.
    let halt = SessionMessage::Command(SessionCommand::Halt {
        reason: "viewer tries".to_string(),
        flatten: false,
    });
    bob.send(Message::Text(serde_json::to_string(&halt).unwrap().into()))
        .await
        .unwrap();
    let refusal = loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), bob.next())
            .await
            .expect("no refusal received")
            .unwrap()
            .unwrap();
        if let Message::Text(text) = frame
            && let SessionMessage::Error { message } = serde_json::from_str(&text).unwrap()
        {
            break message;
        }
    };
    assert!(
        refusal.contains("bob") && refusal.contains("viewer"),
        "{refusal}"
    );
    assert!(
        !kill_switch.is_engaged(),
        "a viewer must not be able to halt"
    );

    // The operator can; the status then reaches the viewer too.
    let mut alice = connect_as(port, ALICE).await;
    let status = next_status(&mut alice).await;
    assert_eq!(
        (status.actor.as_str(), status.role.as_str()),
        ("alice", "operator")
    );
    let halt = SessionMessage::Command(SessionCommand::Halt {
        reason: "alice halts".to_string(),
        flatten: true,
    });
    alice
        .send(Message::Text(serde_json::to_string(&halt).unwrap().into()))
        .await
        .unwrap();
    let seen_by_bob = loop {
        let s = next_status(&mut bob).await;
        if s.halted {
            break s;
        }
    };
    assert_eq!(seen_by_bob.halt_reason.as_deref(), Some("alice halts"));
    assert!(seen_by_bob.flatten_requested);

    // The audit trail names people, and records the refused attempt.
    let records: Vec<serde_json::Value> = std::fs::read_to_string(&audit_path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let find = |action: &str, actor: &str| {
        records
            .iter()
            .any(|r| r["action"] == action && r["actor"] == actor)
    };
    assert!(find("ws_connect", "bob") && find("ws_connect", "alice"));
    assert!(find("command_denied", "bob"));
    assert!(find("command", "alice"));
    assert!(!find("command", "bob"));
    let text = std::fs::read_to_string(&audit_path).unwrap();
    assert!(
        !text.contains(ALICE) && !text.contains(BOB),
        "secrets in audit log"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn viewers_can_scrape_metrics_and_unknown_tokens_are_rejected() {
    const BOB: &str = "bob-viewer-token-000002";
    let security = apexes_engine::ServerSecurity {
        tokens: Some(std::sync::Arc::new(
            apexes_security::TokenSet::from_spec(&format!("bob:viewer:{BOB}")).unwrap(),
        )),
        audit: None,
    };
    let (port, _ks, _keepalive) = spawn_secured_app(security).await;

    assert_eq!(http_get(port, "/metrics").await.0, 401, "no token");
    assert_eq!(
        http_get_with(port, "/metrics", Some("wrong-token-wrong-token"))
            .await
            .0,
        401,
        "unknown token"
    );
    let (status, body) = http_get_with(port, "/metrics", Some(BOB)).await;
    assert_eq!(status, 200, "a viewer may scrape metrics");
    assert!(body.contains("apexes_kill_switch_engaged"));
}

#[tokio::test]
async fn a_late_joining_client_gets_the_current_snapshot_immediately() {
    let (command_tx, _command_rx) = mpsc::unbounded_channel::<SessionCommand>();
    // The snapshot is already in the watch channel BEFORE anyone connects and
    // never changes again (a stalled engine): it must still be delivered.
    let mut already_there = empty_snapshot();
    already_there.equity = dec!(777);
    let (_snapshot_tx, snapshot_rx) = watch::channel(already_there);
    let (stop_tx, _stop_rx) = tokio::sync::oneshot::channel();
    let app = apexes_engine::build_app(command_tx, snapshot_rx, stop_tx);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/session/ws"))
        .await
        .unwrap();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("a late joiner never received the current snapshot")
            .unwrap()
            .unwrap();
        if let Message::Text(text) = frame
            && let SessionMessage::Snapshot(snapshot) = serde_json::from_str(&text).unwrap()
        {
            assert_eq!(snapshot.equity, dec!(777));
            return;
        }
    }
}

#[tokio::test]
async fn managed_health_is_visible_without_ticks_and_resume_is_denied() {
    let (command_tx, mut command_rx) = mpsc::unbounded_channel::<SessionCommand>();
    let (_snapshot_tx, snapshot_rx) = watch::channel(empty_snapshot());
    let (stop_tx, _stop_rx) = tokio::sync::oneshot::channel();
    let ks = std::sync::Arc::new(apexes_oms::KillSwitch::managed());
    let app = apexes_engine::build_app_with_kill_switch(
        command_tx,
        snapshot_rx,
        stop_tx,
        Some(ks.clone()),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/session/ws"))
        .await
        .unwrap();
    assert_eq!(
        next_status(&mut ws).await.health_state.as_deref(),
        Some("Initializing")
    );
    ks.begin_reconciliation();
    assert_eq!(
        next_status(&mut ws).await.health_state.as_deref(),
        Some("Reconciling")
    );
    ks.startup_ready();
    assert_eq!(
        next_status(&mut ws).await.health_state.as_deref(),
        Some("Running")
    );
    ks.fault("database unavailable");
    let status = next_status(&mut ws).await;
    assert_eq!(status.health_state.as_deref(), Some("Degraded"));
    assert!(status.resume_blocked && status.halted);
    ks.cancel_sweep_complete();
    assert_eq!(
        next_status(&mut ws).await.health_state.as_deref(),
        Some("Halted")
    );
    ws.send(Message::Text(
        serde_json::to_string(&SessionMessage::Command(SessionCommand::ResumeTrading))
            .unwrap()
            .into(),
    ))
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Message::Text(text) = ws.next().await.unwrap().unwrap()
                && let SessionMessage::Error { message } = serde_json::from_str(&text).unwrap()
            {
                assert!(message.contains("reconciliation and restart"));
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(ks.is_engaged());
    assert!(command_rx.try_recv().is_err());
    task.abort();
}
