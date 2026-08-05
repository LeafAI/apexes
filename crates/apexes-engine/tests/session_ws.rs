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
        positions: HashMap::new(),
        balances: HashMap::new(),
        equity: Decimal::ZERO,
        realized_pnl: HashMap::new(),
        margin_status: HashMap::new(),
        params: HashMap::new(),
        paused: false,
        timestamp: Utc::now(),
    }
}

#[tokio::test]
async fn session_ws_forwards_snapshots_out_and_commands_in() {
    let (command_tx, mut command_rx) = mpsc::unbounded_channel::<SessionCommand>();
    let (snapshot_tx, snapshot_rx) = watch::channel(empty_snapshot());

    let app = apexes_engine::build_app(command_tx, snapshot_rx);
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
        SessionMessage::Snapshot(snapshot) => assert_eq!(snapshot.equity, dec!(1234.5)),
        SessionMessage::Command(_) => panic!("expected a Snapshot frame"),
    }

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
