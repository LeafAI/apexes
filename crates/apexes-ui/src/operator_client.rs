//! Connection from the operator console to a running engine's
//! `/session/ws`: authenticates with a bearer token, forwards engine
//! messages to the UI, sends the operator's commands, and reconnects with
//! the same backoff the market-data client uses.
//!
//! Safety property: commands are only ever sent over a connection that is
//! currently up. Anything queued while disconnected is DISCARDED on
//! reconnect, so a click made against a dead link can never fire later
//! against a different engine state (a stale "resume trading" is the
//! dangerous case).

use apexes_backtest::SessionCommand;
use apexes_engine::SessionMessage;
use futures::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::client::{BackoffState, ConnectionStatus};

#[derive(Debug, Clone)]
pub struct OperatorClientConfig {
    /// `ws://host:port/session/ws`
    pub url: String,
    /// Bearer token (any role); `None` for an engine running without auth.
    pub token: Option<String>,
}

fn build_request(
    config: &OperatorClientConfig,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, String> {
    let mut request = config
        .url
        .as_str()
        .into_client_request()
        .map_err(|e| format!("bad engine url {:?}: {e}", config.url))?;
    if let Some(token) = &config.token {
        request.headers_mut().insert(
            "Authorization",
            apexes_security::bearer_header_value(token)
                .parse()
                .map_err(|_| "token contains characters not allowed in a header".to_string())?,
        );
    }
    Ok(request)
}

/// Encodes a command exactly as the engine expects it.
pub fn command_frame(command: SessionCommand) -> String {
    serde_json::to_string(&SessionMessage::Command(command))
        .expect("SessionMessage serialization cannot fail")
}

/// Runs until `events`' receiver is dropped.
pub async fn run(
    config: OperatorClientConfig,
    events: mpsc::UnboundedSender<SessionMessage>,
    status: watch::Sender<ConnectionStatus>,
    mut commands: mpsc::UnboundedReceiver<SessionCommand>,
) {
    let mut backoff = BackoffState::new();
    loop {
        // Anything clicked while we were down is stale: never replay it.
        while commands.try_recv().is_ok() {}

        match build_request(&config) {
            Err(reason) => {
                tracing::error!(reason, "operator client: cannot build request");
                let _ = status.send(ConnectionStatus::Disconnected);
                return; // configuration error: retrying cannot help
            }
            Ok(request) => match tokio_tungstenite::connect_async(request).await {
                Ok((socket, _)) => {
                    tracing::info!(url = %config.url, "operator client: connected");
                    backoff.reset();
                    let _ = status.send(ConnectionStatus::Connected);
                    let (mut write, mut read) = socket.split();
                    loop {
                        tokio::select! {
                            frame = read.next() => match frame {
                                Some(Ok(Message::Text(text))) => {
                                    match serde_json::from_str::<SessionMessage>(&text) {
                                        Ok(message) => {
                                            if events.send(message).is_err() {
                                                return;
                                            }
                                        }
                                        Err(error) => tracing::warn!(?error, "operator client: unparseable frame, skipping"),
                                    }
                                }
                                Some(Ok(_)) => {}
                                Some(Err(error)) => {
                                    tracing::warn!(?error, "operator client: socket error, reconnecting");
                                    break;
                                }
                                None => {
                                    tracing::warn!("operator client: engine closed the connection, reconnecting");
                                    break;
                                }
                            },
                            command = commands.recv() => match command {
                                Some(command) => {
                                    if write.send(Message::Text(command_frame(command).into())).await.is_err() {
                                        break;
                                    }
                                }
                                None => return, // UI is gone
                            },
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(?error, "operator client: connect failed, retrying");
                }
            },
        }
        let _ = status.send(ConnectionStatus::Reconnecting);
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_carries_the_bearer_token() {
        let request = build_request(&OperatorClientConfig {
            url: "ws://127.0.0.1:9100/session/ws".into(),
            token: Some("tok-tok-tok-tok-tok".into()),
        })
        .unwrap();
        assert_eq!(
            request.headers().get("Authorization").unwrap(),
            "Bearer tok-tok-tok-tok-tok"
        );
        let none = build_request(&OperatorClientConfig {
            url: "ws://127.0.0.1:9100/session/ws".into(),
            token: None,
        })
        .unwrap();
        assert!(none.headers().get("Authorization").is_none());
    }

    #[test]
    fn bad_urls_and_tokens_are_rejected_up_front() {
        assert!(
            build_request(&OperatorClientConfig {
                url: "not a url".into(),
                token: None
            })
            .is_err()
        );
        assert!(
            build_request(&OperatorClientConfig {
                url: "ws://x/session/ws".into(),
                token: Some("bad\ntoken".into())
            })
            .is_err()
        );
    }

    #[test]
    fn command_frames_match_the_engine_wire_format() {
        let json = command_frame(SessionCommand::Halt {
            reason: "r".into(),
            flatten: true,
        });
        assert_eq!(
            json,
            r#"{"type":"Command","Halt":{"reason":"r","flatten":true}}"#
        );
        assert_eq!(
            command_frame(SessionCommand::ResumeTrading),
            r#"{"type":"Command","ResumeTrading":null}"#
        );
    }
}
