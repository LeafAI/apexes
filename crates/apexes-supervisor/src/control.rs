//! A one-shot client for an engine's `/session/ws`: just enough to send a
//! `Stop`. The live view of a session belongs to the operator console; this
//! exists so the supervisor can stop a session cleanly without a console.

use std::time::Duration;

use apexes_backtest::SessionCommand;
use apexes_engine::SessionMessage;
use futures::SinkExt;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::error::SupervisorError;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

fn unreachable(reason: impl std::fmt::Display) -> SupervisorError {
    SupervisorError::ControlUnreachable {
        reason: reason.to_string(),
    }
}

/// The exact frame the engine expects for a command.
pub fn stop_frame() -> String {
    serde_json::to_string(&SessionMessage::Command(SessionCommand::Stop))
        .unwrap_or_else(|_| r#"{"type":"Command","Stop":null}"#.to_string())
}

/// Connects to `url`, sends `Stop`, and closes. Blocks the calling thread.
pub fn send_stop(url: &str, token: Option<&str>) -> Result<(), SupervisorError> {
    let mut request = url
        .into_client_request()
        .map_err(|error| unreachable(format!("bad control url {url:?}: {error}")))?;
    if let Some(token) = token {
        request.headers_mut().insert(
            "Authorization",
            apexes_security::bearer_header_value(token)
                .parse()
                .map_err(|_| unreachable("token contains characters not allowed in a header"))?,
        );
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(unreachable)?;
    runtime.block_on(async move {
        let (mut socket, _) =
            tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(request))
                .await
                .map_err(|_| unreachable("timed out connecting"))?
                .map_err(unreachable)?;
        socket
            .send(Message::Text(stop_frame().into()))
            .await
            .map_err(unreachable)?;
        // Closing flushes the frame before the socket goes away.
        let _ = socket.close(None).await;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stop_frame_is_what_the_engine_parses() {
        let frame = stop_frame();
        let parsed: SessionMessage = serde_json::from_str(&frame).expect("engine-parsable");
        assert!(
            matches!(parsed, SessionMessage::Command(SessionCommand::Stop)),
            "{frame}"
        );
    }

    #[test]
    fn an_unreachable_endpoint_is_an_error_not_a_hang() {
        // Port 1 on loopback refuses immediately.
        let error = send_stop("ws://127.0.0.1:1/session/ws", None).unwrap_err();
        assert!(
            matches!(error, SupervisorError::ControlUnreachable { .. }),
            "{error:?}"
        );
    }

    #[test]
    fn a_bad_url_is_reported() {
        assert!(matches!(
            send_stop("not a url", None).unwrap_err(),
            SupervisorError::ControlUnreachable { .. }
        ));
    }
}
