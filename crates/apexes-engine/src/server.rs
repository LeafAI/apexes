//! `axum` app exposing `GET /session/ws` -- design spec §6.2. Unlike
//! `apexes-market-data-gateway`'s read-only `/market` endpoint, this
//! endpoint is genuinely bidirectional: on upgrade, each connection
//! spawns two independent tasks:
//!
//! 1. A "snapshot forwarder" task: watches
//!    `watch::Receiver<SessionSnapshot>` for changes and forwards each
//!    new value out as a `SessionMessage::Snapshot` JSON text frame.
//! 2. A "command reader" task: reads incoming WS text frames, parses
//!    each as `SessionMessage::Command`, and forwards the inner
//!    `SessionCommand` into the shared
//!    `mpsc::UnboundedSender<SessionCommand>` -- the SAME sender the
//!    trading-loop task's `ControlHandle` receives from. Multiple
//!    connected clients can all send commands, all landing in the one
//!    session's command queue in arrival order at the channel -- this
//!    is a deliberate simplification (see this module's own top-level
//!    doc note in the design spec discussion), not a multi-client
//!    command-ordering guarantee beyond that.
//!
//! Both tasks run until either the WS connection closes or their
//! respective channel is exhausted/closed, at which point the OTHER
//! task is aborted too (`tokio::select!` over both, rather than two
//! independently-`tokio::spawn`ed tasks nobody joins) -- this avoids
//! leaking a task that would otherwise run forever after its peer
//! half of the connection is already gone.

use crate::protocol::SessionMessage;
use apexes_backtest::{SessionCommand, SessionSnapshot};
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use futures::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};

#[derive(Clone)]
struct AppState {
    command_tx: mpsc::UnboundedSender<SessionCommand>,
    snapshot_rx: watch::Receiver<SessionSnapshot>,
}

/// Builds the `axum::Router` `apexes-engine`'s `main.rs` serves.
/// Callers are responsible for binding a listener and calling
/// `axum::serve` themselves -- this function's only job is producing
/// the router, so it stays testable without a real network bind
/// (mirroring `apexes-market-data-gateway`'s own `build_app`
/// precedent).
pub fn build_app(
    command_tx: mpsc::UnboundedSender<SessionCommand>,
    snapshot_rx: watch::Receiver<SessionSnapshot>,
) -> Router {
    let state = AppState {
        command_tx,
        snapshot_rx,
    };
    Router::new()
        .route("/session/ws", get(session_ws_handler))
        .with_state(state)
}

async fn session_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut snapshot_rx = state.snapshot_rx;
    let command_tx = state.command_tx;

    loop {
        tokio::select! {
            changed = snapshot_rx.changed() => {
                if changed.is_err() {
                    tracing::info!(
                        "apexes-engine server: snapshot watch channel closed (trading loop \
                         ended), closing /session/ws connection"
                    );
                    break;
                }
                let snapshot = snapshot_rx.borrow().clone();
                if !send_snapshot(&mut sender, &snapshot).await {
                    break;
                }
            }
            incoming = receiver.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if !forward_command(&text, &command_tx) {
                            // Not a fatal error for this connection --
                            // an unparseable frame is logged and
                            // skipped, matching
                            // `apexes-market-data-gateway`'s own
                            // "skip, don't disconnect" convention for
                            // malformed individual frames.
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        tracing::info!(
                            "apexes-engine server: client closed /session/ws connection"
                        );
                        break;
                    }
                    Some(Ok(_)) => {
                        // Binary/Ping/Pong frames: nothing to do.
                    }
                    Some(Err(error)) => {
                        tracing::warn!(
                            ?error,
                            "apexes-engine server: /session/ws read error, closing connection"
                        );
                        break;
                    }
                }
            }
        }
    }

    let _ = sender.send(Message::Close(None)).await;
}

/// Serializes `snapshot` as a `SessionMessage::Snapshot` JSON text
/// frame and sends it. Returns `false` if the send failed (the client
/// disconnected), in which case the caller should stop forwarding to
/// this socket.
async fn send_snapshot(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    snapshot: &SessionSnapshot,
) -> bool {
    let message = SessionMessage::Snapshot(snapshot.clone());
    let json = match serde_json::to_string(&message) {
        Ok(json) => json,
        Err(error) => {
            tracing::warn!(
                ?error,
                "apexes-engine server: failed to serialize SessionSnapshot, skipping"
            );
            return true;
        }
    };
    if sender.send(Message::Text(json.into())).await.is_err() {
        tracing::info!("apexes-engine server: client disconnected, stopping snapshot forwarder");
        return false;
    }
    true
}

/// Parses `text` as a `SessionMessage` and, if it is a `Command`
/// variant, forwards the inner `SessionCommand` into `command_tx`.
/// Returns `false` if parsing failed or the frame was not a `Command`
/// (logged, not fatal to the connection).
fn forward_command(text: &str, command_tx: &mpsc::UnboundedSender<SessionCommand>) -> bool {
    let message: SessionMessage = match serde_json::from_str(text) {
        Ok(message) => message,
        Err(error) => {
            tracing::warn!(
                ?error,
                "apexes-engine server: failed to parse incoming /session/ws frame as \
                 SessionMessage, skipping"
            );
            return false;
        }
    };
    match message {
        SessionMessage::Command(command) => {
            if command_tx.send(command).is_err() {
                tracing::warn!(
                    "apexes-engine server: command_tx receiver dropped (trading loop ended), \
                     dropping command"
                );
                return false;
            }
            true
        }
        SessionMessage::Snapshot(_) => {
            tracing::warn!(
                "apexes-engine server: received a Snapshot frame on /session/ws (clients should \
                 only send Command frames), ignoring"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_backtest::ControlHandle;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use std::collections::HashMap;
    use std::time::Duration;

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

    #[test]
    fn build_app_does_not_panic_and_returns_a_router() {
        let (_handle, command_tx, snapshot_rx) =
            ControlHandle::new(Duration::from_millis(500), empty_snapshot());
        let _router = build_app(command_tx, snapshot_rx);
    }
}
