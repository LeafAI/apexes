//! WebSocket client task: connects to the local server task (Task 2)
//! over `ws://127.0.0.1:<port>/ws`, deserializes each incoming JSON
//! text frame as a `MarketEvent`, and forwards successfully-parsed
//! events into an `mpsc` channel. Reconnects with exponential backoff
//! on any error or unexpected close (mirroring
//! `apexes-market-data/src/mark_price_task.rs`'s existing
//! hand-rolled-reconnect loop shape -- connect, read-loop-until-error,
//! sleep, retry -- but with backoff instead of that file's fixed
//! delay). A malformed/unparseable JSON frame is logged and skipped,
//! not treated as a connection error.

use apexes_core::MarketEvent;
use futures::StreamExt;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionStatus {
    #[default]
    Disconnected,
    Connected,
    Reconnecting,
}

/// Tracks the exponential-backoff delay across reconnect attempts:
/// starts at `INITIAL_BACKOFF`, doubles on each consecutive call to
/// `next_delay`, caps at `MAX_BACKOFF`, and resets back to
/// `INITIAL_BACKOFF` the next time `reset` is called (on a successful
/// connection).
pub struct BackoffState {
    current: Duration,
}

impl Default for BackoffState {
    fn default() -> Self {
        Self::new()
    }
}

impl BackoffState {
    pub fn new() -> Self {
        Self {
            current: INITIAL_BACKOFF,
        }
    }

    /// Returns the delay to sleep before the next reconnect attempt,
    /// then doubles the internal state (capped at `MAX_BACKOFF`) for
    /// the following call.
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current * 2).min(MAX_BACKOFF);
        delay
    }

    pub fn reset(&mut self) {
        self.current = INITIAL_BACKOFF;
    }
}

/// Runs forever: connects to `url`, forwards parsed `MarketEvent`s
/// into `tx`, publishes connection state transitions to `status`, and
/// reconnects with backoff on any error. Returns only if `tx` is
/// closed (the receiving side -- the `egui` app -- was dropped).
pub async fn run(
    url: String,
    tx: mpsc::UnboundedSender<MarketEvent>,
    status: watch::Sender<ConnectionStatus>,
) {
    let mut backoff = BackoffState::new();
    loop {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((socket, _response)) => {
                tracing::info!(%url, "apexes-ui client: connected");
                let _ = status.send(ConnectionStatus::Connected);
                backoff.reset();
                let (_write, mut read) = socket.split();
                loop {
                    match read.next().await {
                        Some(Ok(Message::Text(text))) => {
                            match serde_json::from_str::<MarketEvent>(&text) {
                                Ok(event) => {
                                    if tx.send(event).is_err() {
                                        tracing::info!(
                                            "apexes-ui client: output channel closed, stopping"
                                        );
                                        return;
                                    }
                                }
                                Err(error) => {
                                    tracing::warn!(
                                        ?error,
                                        "apexes-ui client: failed to parse MarketEvent JSON, skipping frame"
                                    );
                                }
                            }
                        }
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            tracing::warn!(
                                ?error,
                                "apexes-ui client: WebSocket error, reconnecting"
                            );
                            break;
                        }
                        None => {
                            tracing::warn!(
                                "apexes-ui client: WebSocket stream ended, reconnecting"
                            );
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(?error, "apexes-ui client: failed to connect, retrying");
            }
        }
        let _ = status.send(ConnectionStatus::Reconnecting);
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_delay_doubles_each_call_and_caps_at_max_backoff() {
        let mut backoff = BackoffState::new();
        assert_eq!(backoff.next_delay(), Duration::from_millis(500));
        assert_eq!(backoff.next_delay(), Duration::from_millis(1000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(2000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(4000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(8000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(16000));
        assert_eq!(backoff.next_delay(), Duration::from_secs(30)); // capped
        assert_eq!(backoff.next_delay(), Duration::from_secs(30)); // stays capped
    }

    #[test]
    fn reset_returns_the_delay_sequence_to_the_initial_value() {
        let mut backoff = BackoffState::new();
        let _ = backoff.next_delay();
        let _ = backoff.next_delay();
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(500));
    }

    #[test]
    fn connection_status_default_is_disconnected() {
        assert_eq!(ConnectionStatus::default(), ConnectionStatus::Disconnected);
    }
}
