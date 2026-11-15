//! [`GatewayMarketDataProvider`]: adapts
//! `apexes-market-data-gateway`'s `GET /market` WebSocket endpoint
//! (row 8.3's minimum-viable message-bus gateway) into an
//! `apexes_core::MarketDataProvider`, so `main.rs` can wrap it in a
//! real `apexes_message_bus::MarketEventBus` for both `SessionMode::Paper`
//! (`run_paper_trading` takes `&MarketEventBus` directly) and
//! `SessionMode::Live` (`run_live_trading` looks up
//! `Arc<MarketEventBus>` instances from its `Registry`, keyed by
//! `Venue` -- see `crate::main`'s Live-mode wiring for how this
//! adapter's single instance gets registered under every venue
//! present in `LaunchConfig.instruments`).
//!
//! Deliberately NOT `apexes_message_bus::MarketEventBus`'s own
//! internal upstream-connecting logic (that connects to a real
//! exchange provider, e.g. `BinanceMarketDataProvider`) -- this
//! adapter instead connects to ANOTHER WebSocket server (the gateway
//! process) acting as a relay, mirroring `apexes-ui`'s existing
//! `client.rs` (13a) connect-and-deserialize-`MarketEvent`-frames
//! logic almost exactly (same reconnect-with-backoff shape), since the
//! gateway's `/market` wire protocol is identical to `apexes-ui`'s own
//! `/ws` protocol (bare `serde_json`-serialized `MarketEvent` frames,
//! no envelope -- see `apexes-market-data-gateway`'s `protocol.rs`).

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent};
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Tracks the exponential-backoff delay across reconnect attempts,
/// identical in shape to `apexes-ui`'s own `BackoffState`
/// (`crates/apexes-ui/src/client.rs`) -- not reused directly since
/// `apexes-engine` does not depend on `apexes-ui` (and should not:
/// `apexes-ui` spawns/supervises `apexes-engine`, never the reverse).
struct BackoffState {
    current: Duration,
}

impl BackoffState {
    fn new() -> Self {
        Self {
            current: INITIAL_BACKOFF,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current * 2).min(MAX_BACKOFF);
        delay
    }

    fn reset(&mut self) {
        self.current = INITIAL_BACKOFF;
    }
}

/// Adapts one gateway `/market` WebSocket connection into an
/// `apexes_core::MarketDataProvider`. `subscribe`'s `instruments`
/// parameter is intentionally ignored: the underlying gateway process
/// is already configured (via its own `--instruments` CLI flag) with
/// whatever fixed instrument set it broadcasts, and this adapter's one
/// WS connection carries all of it regardless of what any individual
/// caller passes here -- matching `MarketEventBus::subscribe`'s own
/// documented behavior of calling the wrapped provider's `subscribe`
/// exactly once (on the first subscribe call transitioning
/// `NotConnected` -> `Connected`), so this parameter is in practice
/// only ever read by that one caller anyway.
pub struct GatewayMarketDataProvider {
    gateway_url: String,
    token: Option<String>,
}

impl GatewayMarketDataProvider {
    pub fn new(gateway_url: impl Into<String>) -> Self {
        Self {
            gateway_url: gateway_url.into(),
            token: None,
        }
    }

    /// Sends `Authorization: Bearer <token>` when connecting (for
    /// gateways started with `APEXES_GATEWAY_TOKEN`). `None`/empty: no header.
    pub fn with_token(mut self, token: Option<String>) -> Self {
        self.token = token.filter(|t| !t.trim().is_empty());
        self
    }
}

#[async_trait]
impl MarketDataProvider for GatewayMarketDataProvider {
    async fn subscribe(
        &self,
        _instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run_connection_loop(
            self.gateway_url.clone(),
            self.token.clone(),
            tx,
        ));

        // Adapts the `mpsc::UnboundedReceiver` into a `Stream` via
        // `futures::stream::unfold` rather than pulling in
        // `tokio-stream`'s `UnboundedReceiverStream` wrapper as a new
        // dependency purely for this one call site.
        let stream = stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        });
        Ok(Box::pin(stream))
    }
}

/// Connects, attaching the bearer token header when configured. The error is
/// a plain string: callers only log it, and `tungstenite::Error` is large
/// enough to trip `clippy::result_large_err`.
async fn connect(
    url: &str,
    token: Option<&str>,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut request = url.into_client_request().map_err(|e| e.to_string())?;
    if let Some(token) = token {
        request.headers_mut().insert(
            "Authorization",
            apexes_security::bearer_header_value(token)
                .parse()
                .map_err(|_| "invalid characters in gateway token".to_string())?,
        );
    }
    tokio_tungstenite::connect_async(request)
        .await
        .map(|(socket, _response)| socket)
        .map_err(|e| e.to_string())
}

/// Runs forever: connects to `url`, forwards parsed `MarketEvent`s
/// into `tx`, and reconnects with backoff on any error or unexpected
/// close. Returns only if `tx` is closed (every receiver -- ultimately
/// `MarketEventBus`'s own internal forwarding task -- was dropped).
async fn run_connection_loop(
    url: String,
    token: Option<String>,
    tx: mpsc::UnboundedSender<MarketEvent>,
) {
    let mut backoff = BackoffState::new();
    loop {
        match connect(&url, token.as_deref()).await {
            Ok(socket) => {
                tracing::info!(%url, "apexes-engine gateway_client: connected");
                backoff.reset();
                let (_write, mut read) = socket.split();
                loop {
                    match read.next().await {
                        Some(Ok(Message::Text(text))) => {
                            match serde_json::from_str::<MarketEvent>(&text) {
                                Ok(event) => {
                                    if tx.send(event).is_err() {
                                        tracing::info!(
                                            "apexes-engine gateway_client: output channel closed, stopping"
                                        );
                                        return;
                                    }
                                }
                                Err(error) => {
                                    tracing::warn!(
                                        ?error,
                                        "apexes-engine gateway_client: failed to parse MarketEvent JSON, skipping frame"
                                    );
                                }
                            }
                        }
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            tracing::warn!(
                                ?error,
                                "apexes-engine gateway_client: WebSocket error, reconnecting"
                            );
                            break;
                        }
                        None => {
                            tracing::warn!(
                                "apexes-engine gateway_client: WebSocket stream ended, reconnecting"
                            );
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "apexes-engine gateway_client: failed to connect, retrying"
                );
            }
        }
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_state_doubles_each_call_and_caps_at_max_backoff() {
        let mut backoff = BackoffState::new();
        assert_eq!(backoff.next_delay(), Duration::from_millis(500));
        assert_eq!(backoff.next_delay(), Duration::from_millis(1000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(2000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(4000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(8000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(16000));
        assert_eq!(backoff.next_delay(), Duration::from_secs(30));
        assert_eq!(backoff.next_delay(), Duration::from_secs(30));
    }

    #[test]
    fn backoff_state_reset_returns_to_the_initial_delay() {
        let mut backoff = BackoffState::new();
        let _ = backoff.next_delay();
        let _ = backoff.next_delay();
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(500));
    }
}
