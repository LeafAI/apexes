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
//!
//! A received `SessionCommand::Stop` is handled TWICE, deliberately:
//! it is forwarded into `command_tx` as usual (so `ControlHandle::
//! drain_commands` still observes it and sets `stop_requested` the
//! normal way, the next time the unified loop's `market_events` arm
//! runs), AND it also fires the shared stop `oneshot` (`stop_tx`)
//! that `run_paper_trading`/`run_live_trading` gate their own
//! `take_until(stop_signal)` event stream on. The oneshot path is
//! what makes `Stop` unblock a STALLED market-event stream
//! immediately, rather than sitting queued until the next tick
//! happens to arrive.

use crate::protocol::{EngineStatus, SessionMessage};
use apexes_backtest::{SessionCommand, SessionSnapshot};
use apexes_oms::KillSwitch;
use apexes_security::{AuditLog, Identity, Role, TokenSet};
use axum::Extension;
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot, watch};

/// The shared stop-signal sender: `oneshot::Sender<()>::send` consumes
/// the sender and can only ever be called once, but multiple WS
/// connections (or repeated `Stop` frames on the same connection) may
/// all race to trigger it. Wrapping it in `Arc<Mutex<Option<..>>>`
/// and `.take()`-ing it inside the lock makes every send-after-the-
/// first a harmless no-op (the `Option` is already `None`) instead of
/// a panic.
type SharedStopSender = Arc<Mutex<Option<oneshot::Sender<()>>>>;

#[derive(Clone)]
struct AppState {
    command_tx: mpsc::UnboundedSender<SessionCommand>,
    snapshot_rx: watch::Receiver<SessionSnapshot>,
    stop_tx: SharedStopSender,
    kill_switch: Option<Arc<KillSwitch>>,
    security: ServerSecurity,
}

/// Authentication and audit configuration for the HTTP/WS surface.
/// `Default` is open and unaudited (tests, backtest/paper on loopback).
#[derive(Clone, Default)]
pub struct ServerSecurity {
    /// When set, `/session/ws` and `/metrics` require
    /// `Authorization: Bearer <token>` matching one of the named tokens;
    /// the caller's name and role come from the matching entry.
    /// `/healthz` and `/readyz` stay open for supervisors and carry no
    /// sensitive data.
    pub tokens: Option<Arc<TokenSet>>,
    /// When set, every connection, rejected request and control command is
    /// appended to it.
    pub audit: Option<Arc<AuditLog>>,
}

impl ServerSecurity {
    fn audit(&self, actor: &str, action: &str, detail: serde_json::Value) {
        if let Some(audit) = &self.audit {
            audit.record(actor, action, detail);
        }
    }
}

/// Identity assumed when the engine runs without tokens.
fn unauthenticated_identity() -> Identity {
    Identity {
        name: "unauthenticated".to_string(),
        role: Role::Operator,
    }
}

async fn require_auth(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    let identity = match &state.security.tokens {
        None => unauthenticated_identity(),
        Some(tokens) => match tokens.identify_headers(request.headers()) {
            Some(identity) => identity.clone(),
            None => {
                state.security.audit(
                    "anonymous",
                    "unauthorized_request",
                    serde_json::json!({ "path": request.uri().path() }),
                );
                return apexes_security::unauthorized();
            }
        },
    };
    request.extensions_mut().insert(identity);
    next.run(request).await
}

pub fn build_app(
    command_tx: mpsc::UnboundedSender<SessionCommand>,
    snapshot_rx: watch::Receiver<SessionSnapshot>,
    stop_tx: oneshot::Sender<()>,
) -> Router {
    build_app_with_kill_switch(command_tx, snapshot_rx, stop_tx, None)
}

/// Like [`build_app`], but `Halt`/`ResumeTrading` commands also
/// engage/release `kill_switch` immediately, on the connection task
/// itself -- not via the command channel, whose consumer (the trading
/// loop) only drains it on market ticks.
pub fn build_app_with_kill_switch(
    command_tx: mpsc::UnboundedSender<SessionCommand>,
    snapshot_rx: watch::Receiver<SessionSnapshot>,
    stop_tx: oneshot::Sender<()>,
    kill_switch: Option<Arc<KillSwitch>>,
) -> Router {
    build_app_secured(
        command_tx,
        snapshot_rx,
        stop_tx,
        kill_switch,
        ServerSecurity::default(),
    )
}

/// Full constructor: like [`build_app_with_kill_switch`] plus
/// authentication and audit logging per [`ServerSecurity`].
pub fn build_app_secured(
    command_tx: mpsc::UnboundedSender<SessionCommand>,
    snapshot_rx: watch::Receiver<SessionSnapshot>,
    stop_tx: oneshot::Sender<()>,
    kill_switch: Option<Arc<KillSwitch>>,
    security: ServerSecurity,
) -> Router {
    let state = AppState {
        command_tx,
        snapshot_rx,
        stop_tx: Arc::new(Mutex::new(Some(stop_tx))),
        kill_switch,
        security,
    };
    let protected = Router::new()
        .route("/session/ws", get(session_ws_handler))
        .route("/metrics", get(metrics_handler))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_auth,
        ));
    let open = Router::new()
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler));
    protected.merge(open).with_state(state)
}

/// Prometheus scrape endpoint.
async fn metrics_handler() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        apexes_telemetry::render(),
    )
}

fn health_body(state: &AppState) -> (bool, serde_json::Value) {
    let halt_reason = state
        .kill_switch
        .as_ref()
        .filter(|ks| ks.is_engaged())
        .map(|ks| ks.reason().unwrap_or_default());
    let halted = halt_reason.is_some();
    let phase = state.kill_switch.as_ref().and_then(|ks| ks.phase());
    let blocked = state
        .kill_switch
        .as_ref()
        .is_some_and(|ks| ks.blocks_submissions());
    (
        blocked,
        serde_json::json!({
            "status": if halted { "halted" } else if blocked { "not_ready" } else { "ok" },
            "halt_reason": halt_reason,
            "health_state": phase.map(|p| p.as_str()),
            "health_reason": state.kill_switch.as_ref().and_then(|ks| ks.health_reason()),
        }),
    )
}

/// Liveness: 200 whenever the process is serving, with halt state in the
/// body (a halted engine is alive and must not be restarted by a
/// supervisor, which would only lose the halt).
async fn healthz_handler(State(state): State<AppState>) -> impl IntoResponse {
    let (_, body) = health_body(&state);
    (StatusCode::OK, axum::Json(body))
}

/// Readiness: 503 while managed startup is incomplete or trading is halted.
async fn readyz_handler(State(state): State<AppState>) -> impl IntoResponse {
    let (halted, body) = health_body(&state);
    let code = if halted {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    (code, axum::Json(body))
}

async fn session_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state, identity))
}

/// How often connection-level status (halt state) is re-checked.
const STATUS_POLL: std::time::Duration = std::time::Duration::from_millis(500);

fn engine_status(identity: &Identity, kill_switch: Option<&KillSwitch>) -> EngineStatus {
    let halted = kill_switch.is_some_and(KillSwitch::is_engaged);
    EngineStatus {
        actor: identity.name.clone(),
        role: identity.role.as_str().to_string(),
        halted,
        halt_reason: kill_switch.filter(|_| halted).and_then(KillSwitch::reason),
        flatten_requested: kill_switch.is_some_and(KillSwitch::flatten_requested),
        health_state: kill_switch
            .and_then(KillSwitch::phase)
            .map(|phase| phase.as_str().to_string()),
        health_reason: kill_switch.and_then(KillSwitch::health_reason),
        resume_blocked: kill_switch.is_some_and(|ks| ks.phase().is_some()),
    }
}

async fn send_message(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    message: &SessionMessage,
) -> bool {
    let Ok(json) = serde_json::to_string(message) else {
        return true;
    };
    sender.send(Message::Text(json.into())).await.is_ok()
}

async fn handle_socket(socket: WebSocket, state: AppState, identity: Identity) {
    let (mut sender, mut receiver) = socket.split();
    let mut snapshot_rx = state.snapshot_rx;
    let command_tx = state.command_tx;
    let stop_tx = state.stop_tx;
    let kill_switch = state.kill_switch;
    let security = state.security;
    let actor = identity.name.clone();
    security.audit(
        &actor,
        "ws_connect",
        serde_json::json!({ "role": identity.role.as_str() }),
    );

    // A client that connects LATE (typically during an incident, to an engine
    // whose feed has stalled and so pushes no new snapshots) must still see
    // the last known state straight away, not only after the next change.
    // `borrow_and_update` also marks it seen so it is not sent twice.
    let current = snapshot_rx.borrow_and_update().clone();
    if !send_snapshot(&mut sender, &current).await {
        return;
    }

    let mut status_tick = tokio::time::interval(STATUS_POLL);
    let mut last_status: Option<EngineStatus> = None;

    loop {
        tokio::select! {
            _ = status_tick.tick() => {
                // First tick fires immediately: the client learns its
                // identity/role and the halt state on connect.
                let status = engine_status(&identity, kill_switch.as_deref());
                if last_status.as_ref() != Some(&status) {
                    if !send_message(&mut sender, &SessionMessage::Status(status.clone())).await {
                        break;
                    }
                    last_status = Some(status);
                }
            }
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
                        // A skipped frame is not fatal to the connection
                        // (matching `apexes-market-data-gateway`'s "skip,
                        // don't disconnect" convention); a denied command
                        // is reported back to this client only.
                        if let Forwarded::Denied(message) = forward_command(
                            &text,
                            &command_tx,
                            &stop_tx,
                            kill_switch.as_deref(),
                            &security,
                            &identity,
                        )
                        .await
                            && !send_message(&mut sender, &SessionMessage::Error { message }).await
                        {
                            break;
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
/// If the command is `SessionCommand::Stop`, also fires `stop_tx`
/// (once, across however many connections/`Stop` frames actually
/// arrive -- see `SharedStopSender`'s own doc comment) so a stalled
/// market-event stream unblocks immediately rather than waiting for
/// `ControlHandle::drain_commands` to next be polled. Returns `false`
/// if parsing failed or the frame was not a `Command` (logged, not
/// fatal to the connection).
/// What became of one incoming frame.
enum Forwarded {
    /// Parsed and acted on.
    Applied,
    /// Unparseable, not a command, or the loop is gone: logged, ignored.
    Skipped,
    /// The caller's role may not send commands.
    Denied(String),
}

async fn forward_command(
    text: &str,
    command_tx: &mpsc::UnboundedSender<SessionCommand>,
    stop_tx: &SharedStopSender,
    kill_switch: Option<&KillSwitch>,
    security: &ServerSecurity,
    identity: &Identity,
) -> Forwarded {
    let actor = identity.name.as_str();
    let message: SessionMessage = match serde_json::from_str(text) {
        Ok(message) => message,
        Err(error) => {
            tracing::warn!(
                ?error,
                "apexes-engine server: failed to parse incoming /session/ws frame as \
                 SessionMessage, skipping"
            );
            return Forwarded::Skipped;
        }
    };
    match message {
        SessionMessage::Command(command) => {
            if !identity.role.can_command() {
                security.audit(
                    actor,
                    "command_denied",
                    serde_json::to_value(&command).unwrap_or(serde_json::Value::Null),
                );
                return Forwarded::Denied(format!(
                    "{actor} is a {} and may not send commands",
                    identity.role.as_str()
                ));
            }
            let is_stop = matches!(command, SessionCommand::Stop);
            // Audited BEFORE it takes effect, so the record exists even if
            // acting on it crashes the process.
            security.audit(
                actor,
                "command",
                serde_json::to_value(&command).unwrap_or(serde_json::Value::Null),
            );
            // Engage/release BEFORE forwarding: the halt must take effect
            // even if the trading loop is stalled or already gone.
            match (&command, kill_switch) {
                (SessionCommand::Halt { reason, flatten }, Some(ks)) => {
                    // Request first: the runner reads it when `engage` wakes it.
                    if *flatten {
                        ks.request_flatten();
                    }
                    ks.engage(reason.clone());
                }
                (SessionCommand::ResumeTrading, Some(ks)) => {
                    if let Err(reason) = ks.try_release() {
                        security.audit(
                            actor,
                            "command_denied",
                            serde_json::json!({"command":"ResumeTrading","reason":reason}),
                        );
                        return Forwarded::Denied(reason.into());
                    }
                }
                (SessionCommand::Halt { .. } | SessionCommand::ResumeTrading, None) => {
                    tracing::warn!(
                        "apexes-engine server: halt/resume received but no kill switch is configured (non-live session)"
                    );
                }
                _ => {}
            }
            if command_tx.send(command).is_err() {
                tracing::warn!(
                    "apexes-engine server: command_tx receiver dropped (trading loop ended), \
                     dropping command"
                );
                return Forwarded::Skipped;
            }
            if is_stop {
                fire_stop_signal(stop_tx).await;
            }
            Forwarded::Applied
        }
        SessionMessage::Snapshot(_) | SessionMessage::Status(_) | SessionMessage::Error { .. } => {
            tracing::warn!(
                "apexes-engine server: received an engine->client frame on /session/ws (clients \
                 should only send Command frames), ignoring"
            );
            Forwarded::Skipped
        }
    }
}

/// Sends on `stop_tx` exactly once across however many times this is
/// called (from one or many connections). Subsequent calls, once the
/// inner `Option` is already `None`, are a harmless no-op -- `oneshot
/// ::Sender::send` cannot be called twice, so the `Option::take()`
/// under the lock is what makes repeated `Stop` commands safe.
async fn fire_stop_signal(stop_tx: &SharedStopSender) {
    let mut guard = stop_tx.lock().await;
    if let Some(sender) = guard.take() {
        // The receiver may already be gone (e.g. the trading loop
        // already ended on its own) -- that's not an error here, just
        // means the stop signal has nothing left to unblock.
        let _ = sender.send(());
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

    #[test]
    fn build_app_does_not_panic_and_returns_a_router() {
        let (_handle, command_tx, snapshot_rx) =
            ControlHandle::new(Duration::from_millis(500), empty_snapshot());
        let (stop_tx, _stop_rx) = oneshot::channel();
        let _router = build_app(command_tx, snapshot_rx, stop_tx);
    }

    #[tokio::test]
    async fn fire_stop_signal_is_a_harmless_no_op_when_called_more_than_once() {
        let (stop_tx, stop_rx) = oneshot::channel();
        let shared: SharedStopSender = Arc::new(Mutex::new(Some(stop_tx)));

        // First call: actually sends, unblocking `stop_rx`.
        fire_stop_signal(&shared).await;
        tokio::time::timeout(Duration::from_secs(1), stop_rx)
            .await
            .expect("timed out waiting for the stop oneshot to resolve")
            .expect("stop oneshot sender was dropped without sending");

        // Second and third calls (simulating a repeated Stop from the
        // same client, or a second client also sending Stop): must not
        // panic -- the `Option` is already `None`.
        fire_stop_signal(&shared).await;
        fire_stop_signal(&shared).await;
    }
}
