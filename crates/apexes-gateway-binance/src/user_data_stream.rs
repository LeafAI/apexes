//! Binance User Data Stream: push-based order/fill updates delivered
//! over a WebSocket, keyed by a `listenKey` obtained via REST and
//! renewed periodically (Binance requires renewal within 60 minutes of
//! the last renewal or the key expires and the stream disconnects).
//! Spot and USDS-M Futures each have their OWN separate User Data
//! Stream (separate listenKey-issuing endpoints, separate WebSocket
//! hosts) -- `start_user_data_stream` subscribes to both and merges
//! their events into one output stream.

use apexes_core::{OrderId, OrderStatus};
use apexes_oms::ExchangeUpdate;
use futures::stream::{self, Stream, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::rest::{ApiFamily, BinanceRestClient};

/// Keeps the listenKey renewal and WebSocket-reading background tasks
/// alive for both the Spot and Futures streams. Dropping this aborts
/// all four tasks immediately -- callers must keep this alive for as
/// long as they want the User Data Stream to keep running.
pub struct UserDataStreamHandle {
    _spot_socket: tokio::task::JoinHandle<()>,
    _spot_renewal: tokio::task::JoinHandle<()>,
    _futures_socket: tokio::task::JoinHandle<()>,
    _futures_renewal: tokio::task::JoinHandle<()>,
}

impl Drop for UserDataStreamHandle {
    fn drop(&mut self) {
        self._spot_socket.abort();
        self._spot_renewal.abort();
        self._futures_socket.abort();
        self._futures_renewal.abort();
    }
}

async fn obtain_listen_key(rest: &BinanceRestClient, family: ApiFamily) -> Result<String, String> {
    let path = match family {
        ApiFamily::Spot => "/api/v3/userDataStream",
        ApiFamily::Futures => "/fapi/v1/listenKey",
    };
    // NOTE: this endpoint is documented by Binance as NOT requiring a
    // signature (only the API key header) -- signed_request always
    // signs, which Binance tolerates (extra params are ignored) per
    // community-verified behavior, so reusing signed_request here is
    // safe and avoids a second request-building code path.
    let (status, body) = rest.signed_request("POST", family, path, "").await?;
    if !status.is_success() {
        return Err(format!("failed to obtain listenKey: HTTP {status}: {body}"));
    }

    #[derive(Deserialize)]
    struct ListenKeyResponse {
        #[serde(rename = "listenKey")]
        listen_key: String,
    }
    let parsed: ListenKeyResponse = serde_json::from_str(&body)
        .map_err(|error| format!("parse error: {error}; body={body}"))?;
    Ok(parsed.listen_key)
}

async fn renew_listen_key(
    rest: &BinanceRestClient,
    family: ApiFamily,
    listen_key: &str,
) -> Result<(), String> {
    let path = match family {
        ApiFamily::Spot => "/api/v3/userDataStream",
        ApiFamily::Futures => "/fapi/v1/listenKey",
    };
    let params = format!("listenKey={listen_key}");
    let (status, body) = rest.signed_request("PUT", family, path, &params).await?;
    if !status.is_success() {
        return Err(format!("failed to renew listenKey: HTTP {status}: {body}"));
    }
    Ok(())
}

fn websocket_host(family: ApiFamily) -> &'static str {
    match family {
        ApiFamily::Spot => "wss://stream.binance.com:9443/ws",
        ApiFamily::Futures => "wss://fstream.binance.com/ws",
    }
}

/// Parses one raw WebSocket text message into an `ExchangeUpdate`, or
/// `None` if the message is a User Data Stream event type this gateway
/// doesn't need to act on (Binance's User Data Stream carries several
/// event types -- `outboundAccountPosition`, `balanceUpdate`, etc. --
/// only `executionReport` (spot) and `ORDER_TRADE_UPDATE` (futures)
/// carry order status changes).
fn parse_event(raw: &str) -> Option<ExchangeUpdate> {
    let value: Value = serde_json::from_str(raw).ok()?;
    let event_type = value.get("e")?.as_str()?;

    match event_type {
        "executionReport" => {
            let client_order_id = value.get("c")?.as_str()?.parse::<uuid::Uuid>().ok()?;
            let exchange_order_id = value.get("i")?.as_i64()?.to_string();
            let status = parse_binance_status(value.get("X")?.as_str()?)?;
            Some(ExchangeUpdate {
                client_order_id: OrderId(client_order_id),
                exchange_order_id: Some(exchange_order_id),
                status,
            })
        }
        "ORDER_TRADE_UPDATE" => {
            let order = value.get("o")?;
            let client_order_id = order.get("c")?.as_str()?.parse::<uuid::Uuid>().ok()?;
            let exchange_order_id = order.get("i")?.as_i64()?.to_string();
            let status = parse_binance_status(order.get("X")?.as_str()?)?;
            Some(ExchangeUpdate {
                client_order_id: OrderId(client_order_id),
                exchange_order_id: Some(exchange_order_id),
                status,
            })
        }
        _ => None,
    }
}

fn parse_binance_status(status: &str) -> Option<OrderStatus> {
    match status {
        "NEW" => Some(OrderStatus::New),
        "PARTIALLY_FILLED" => Some(OrderStatus::PartiallyFilled),
        "FILLED" => Some(OrderStatus::Filled),
        "CANCELED" | "EXPIRED" => Some(OrderStatus::Cancelled),
        "REJECTED" => Some(OrderStatus::Rejected),
        // Statuses outside this gateway's current OrderStatus mapping
        // (e.g. spot's EXPIRED_IN_MATCH, futures' NEW_INSURANCE /
        // NEW_ADL) intentionally drop the whole event rather than
        // guessing a mapping -- logged so a silent gap is diagnosable
        // in production rather than invisible.
        other => {
            tracing::debug!(
                status = other,
                "unmapped Binance order status; event dropped"
            );
            None
        }
    }
}

/// Starts both the Spot and Futures User Data Streams, spawning a
/// renewal background task for each (every 30 minutes -- well within
/// Binance's 60-minute expiry window), and returns a merged stream of
/// `ExchangeUpdate`s from both plus a handle that keeps renewal alive.
pub async fn start_user_data_stream(
    rest: BinanceRestClient,
) -> Result<(UserDataStreamHandle, impl Stream<Item = ExchangeUpdate>), String> {
    let (tx, rx) = mpsc::unbounded_channel();

    let spot_listen_key = obtain_listen_key(&rest, ApiFamily::Spot).await?;
    let futures_listen_key = obtain_listen_key(&rest, ApiFamily::Futures).await?;

    let (spot_socket, spot_renewal) =
        spawn_stream_tasks(rest.clone(), ApiFamily::Spot, spot_listen_key, tx.clone());
    let (futures_socket, futures_renewal) =
        spawn_stream_tasks(rest.clone(), ApiFamily::Futures, futures_listen_key, tx);

    let handle = UserDataStreamHandle {
        _spot_socket: spot_socket,
        _spot_renewal: spot_renewal,
        _futures_socket: futures_socket,
        _futures_renewal: futures_renewal,
    };

    Ok((handle, tokio_stream_from_receiver(rx)))
}

fn tokio_stream_from_receiver(
    rx: mpsc::UnboundedReceiver<ExchangeUpdate>,
) -> impl Stream<Item = ExchangeUpdate> {
    stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| (update, rx))
    })
}

/// Spawns two independently-tracked background tasks for `family`: one
/// that connects the WebSocket using `listen_key` and forwards parsed
/// `ExchangeUpdate`s to `tx`, and one that renews `listen_key` every 30
/// minutes for as long as it runs. Both `JoinHandle`s are returned so
/// `UserDataStreamHandle`'s `Drop` impl can abort both -- a listenKey
/// renewal task spawned *inside* (nested under) the socket task, with
/// its `JoinHandle` discarded, would keep running forever after the
/// outer task is aborted, since aborting a task does not recursively
/// abort tasks it spawned.
fn spawn_stream_tasks(
    rest: BinanceRestClient,
    family: ApiFamily,
    listen_key: String,
    tx: mpsc::UnboundedSender<ExchangeUpdate>,
) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
    let renewal_rest = rest.clone();
    let renewal_key = listen_key.clone();
    let renewal_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30 * 60));
        loop {
            interval.tick().await;
            if let Err(reason) = renew_listen_key(&renewal_rest, family, &renewal_key).await {
                tracing::warn!(reason, ?family, "failed to renew Binance listenKey");
            }
        }
    });

    let socket_task = tokio::spawn(async move {
        let url = format!("{}/{listen_key}", websocket_host(family));
        loop {
            match tokio_tungstenite::connect_async(&url).await {
                Ok((ws_stream, _)) => {
                    let (_, mut read) = ws_stream.split();
                    while let Some(message) = read.next().await {
                        match message {
                            Ok(Message::Text(text)) => {
                                if let Some(update) = parse_event(&text)
                                    && tx.send(update).is_err()
                                {
                                    return; // receiver dropped, stop this task
                                }
                            }
                            Ok(_) => {}
                            Err(error) => {
                                tracing::warn!(?error, ?family, "User Data Stream WebSocket error");
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        ?family,
                        "failed to connect User Data Stream WebSocket"
                    );
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    (socket_task, renewal_task)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BinanceGatewayConfig;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn rest_against(mock_server: &MockServer) -> BinanceRestClient {
        BinanceRestClient::new(BinanceGatewayConfig {
            api_key: "test-api-key".to_string(),
            api_secret: "test-api-secret".to_string(),
            spot_base_url: mock_server.uri(),
            futures_base_url: mock_server.uri(),
            recv_window_ms: 5000,
        })
    }

    #[tokio::test]
    async fn obtain_listen_key_sends_post_and_parses_the_key() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/v3/userDataStream"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "listenKey": "spot-key-123"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let rest = rest_against(&mock_server);
        let key = obtain_listen_key(&rest, ApiFamily::Spot).await.unwrap();

        assert_eq!(key, "spot-key-123");
    }

    #[tokio::test]
    async fn obtain_listen_key_uses_futures_path_for_futures_family() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/fapi/v1/listenKey"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "listenKey": "futures-key-456"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let rest = rest_against(&mock_server);
        let key = obtain_listen_key(&rest, ApiFamily::Futures).await.unwrap();

        assert_eq!(key, "futures-key-456");
    }

    #[tokio::test]
    async fn obtain_listen_key_surfaces_an_error_on_non_success_status() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/v3/userDataStream"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "code": -2015,
                "msg": "Invalid API-key"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let rest = rest_against(&mock_server);
        let result = obtain_listen_key(&rest, ApiFamily::Spot).await;

        assert!(result.is_err());
    }

    // Regression test for the Task 17 review's Critical finding:
    // `renew_listen_key` calls `signed_request("PUT", ...)`, which
    // would fail with "unsupported HTTP method: PUT" before any
    // network call if `signed_request` didn't support PUT. This test
    // asserts the real PUT request actually reaches the mock server
    // with the correct path and `listenKey` param, which fails loudly
    // if PUT support regresses.
    #[tokio::test]
    async fn renew_listen_key_sends_put_with_the_listen_key_param() {
        let mock_server = MockServer::start().await;

        Mock::given(method("PUT"))
            .and(path("/api/v3/userDataStream"))
            .and(query_param("listenKey", "spot-key-123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&mock_server)
            .await;

        let rest = rest_against(&mock_server);
        renew_listen_key(&rest, ApiFamily::Spot, "spot-key-123")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn renew_listen_key_uses_futures_path_for_futures_family() {
        let mock_server = MockServer::start().await;

        Mock::given(method("PUT"))
            .and(path("/fapi/v1/listenKey"))
            .and(query_param("listenKey", "futures-key-456"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&mock_server)
            .await;

        let rest = rest_against(&mock_server);
        renew_listen_key(&rest, ApiFamily::Futures, "futures-key-456")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn renew_listen_key_surfaces_an_error_on_non_success_status() {
        let mock_server = MockServer::start().await;

        Mock::given(method("PUT"))
            .and(path("/api/v3/userDataStream"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "code": -1125,
                "msg": "This listenKey does not exist."
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let rest = rest_against(&mock_server);
        let result = renew_listen_key(&rest, ApiFamily::Spot, "expired-key").await;

        assert!(result.is_err());
    }

    #[test]
    fn parses_spot_execution_report_into_exchange_update() {
        let order_id = uuid::Uuid::new_v4();
        let raw = serde_json::json!({
            "e": "executionReport",
            "c": order_id.to_string(),
            "i": 12345,
            "X": "PARTIALLY_FILLED"
        })
        .to_string();

        let update = parse_event(&raw).unwrap();
        assert_eq!(update.client_order_id, OrderId(order_id));
        assert_eq!(update.exchange_order_id, Some("12345".to_string()));
        assert_eq!(update.status, OrderStatus::PartiallyFilled);
    }

    #[test]
    fn parses_futures_order_trade_update_into_exchange_update() {
        let order_id = uuid::Uuid::new_v4();
        let raw = serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "o": {
                "c": order_id.to_string(),
                "i": 999,
                "X": "FILLED"
            }
        })
        .to_string();

        let update = parse_event(&raw).unwrap();
        assert_eq!(update.client_order_id, OrderId(order_id));
        assert_eq!(update.exchange_order_id, Some("999".to_string()));
        assert_eq!(update.status, OrderStatus::Filled);
    }

    #[test]
    fn ignores_unrecognized_event_types() {
        let raw = serde_json::json!({
            "e": "outboundAccountPosition"
        })
        .to_string();

        assert!(parse_event(&raw).is_none());
    }

    #[test]
    fn ignores_unparseable_json() {
        assert!(parse_event("not json").is_none());
    }
}
