//! Binance **Spot** user-data stream over the WebSocket API.
//!
//! Binance removed the listenKey stream (`POST /api/v3/userDataStream` has
//! answered HTTP 410 Gone since 2026-02-20). The replacement is a
//! subscription on the WebSocket API: connect to the network's `ws-api`
//! host and send `userDataStream.subscribe.signature`, a request carrying a
//! signature over `apiKey`, `recvWindow` and `timestamp`. It accepts any
//! key type (HMAC here), so no Ed25519 session logon is needed.
//!
//! Pushed events arrive wrapped: `{"subscriptionId": 0, "event": {...}}`,
//! with the same field names as before inside `event`. Two details differ
//! from the old stream and are handled in [`parse_frame`]:
//! - for a `CANCELED` report `c` is the id of the *cancel request* and the
//!   id of the order that was cancelled is in `C`;
//! - `serverShutdown` and `eventStreamTerminated` end the subscription, so
//!   the stream reconnects (with a fresh signature) on either.
//!
//! The reconnect, resync and idle-timeout behaviour mirrors the Futures
//! stream in `user_data_stream`: any gap means pushed updates may have been
//! missed, so every (re)subscribe notifies the consumer to reconcile.

use std::sync::Arc;
use std::time::Duration;

use apexes_oms::ExchangeUpdate;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc};
use tokio_tungstenite::tungstenite::Message;

use crate::auth::ParamSigner;
use crate::rest::{ApiFamily, BinanceRestClient};
use crate::user_data_stream::{StreamTimings, backoff_delay, parse_binance_status};

pub type WsApiSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// How long connecting and subscribing may take before it counts as failed.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const SUBSCRIBE_METHOD: &str = "userDataStream.subscribe.signature";

/// Everything needed to open (and re-open) the subscription.
#[derive(Clone)]
pub struct SpotStreamConfig {
    /// `wss://ws-api.<host>/ws-api/v3`, from `BinanceNetwork::endpoints()`.
    pub url: String,
    pub signer: Arc<dyn ParamSigner>,
    pub recv_window_ms: u64,
}

impl std::fmt::Debug for SpotStreamConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpotStreamConfig")
            .field("url", &self.url)
            .field("recv_window_ms", &self.recv_window_ms)
            .finish_non_exhaustive()
    }
}

/// What one pushed frame means to the stream.
#[derive(Debug, PartialEq)]
pub enum Frame {
    Update(ExchangeUpdate),
    /// The subscription is over; reconnect and resubscribe.
    Reconnect(&'static str),
    Ignore,
}

/// Builds the subscribe request. Public so tests (and the smoke script's
/// Rust twin) can check the exact wire shape.
pub fn subscribe_request(
    signer: &dyn ParamSigner,
    recv_window_ms: u64,
    timestamp_ms: i64,
    request_id: &str,
) -> Result<String, String> {
    let api_key = signer.api_key();
    // The key is signed as-is, so anything needing escaping would sign a
    // different string than the server verifies. Binance keys are
    // alphanumeric; refuse anything else instead of sending a bad signature.
    if api_key.is_empty() || !api_key.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err("the API key must be alphanumeric".to_string());
    }
    // Parameters sorted by name, as Binance specifies for signing.
    let payload = format!("apiKey={api_key}&recvWindow={recv_window_ms}&timestamp={timestamp_ms}");
    let signature = signer.sign(&payload)?;
    Ok(json!({
        "id": request_id,
        "method": SUBSCRIBE_METHOD,
        "params": {
            "apiKey": api_key,
            "recvWindow": recv_window_ms,
            "timestamp": timestamp_ms,
            "signature": signature,
        },
    })
    .to_string())
}

/// Reads the verdict on a subscribe request: `Ok` for status 200, otherwise
/// the exchange's own code and message (which never contain our secret).
fn check_subscribe_reply(reply: &Value) -> Result<(), String> {
    let status = reply.get("status").and_then(Value::as_i64).unwrap_or(0);
    if status == 200 {
        return Ok(());
    }
    let error = reply.get("error");
    let code = error.and_then(|e| e.get("code")).and_then(Value::as_i64);
    let message = error
        .and_then(|e| e.get("msg"))
        .and_then(Value::as_str)
        .unwrap_or("no error message");
    Err(format!(
        "user-data subscription rejected: HTTP-style status {status}, Binance code {}: {message}",
        code.map_or_else(|| "none".to_string(), |c| c.to_string())
    ))
}

/// Connects and subscribes once. The returned socket is ready to deliver
/// events.
pub async fn connect_and_subscribe(config: &SpotStreamConfig) -> Result<WsApiSocket, String> {
    connect_and_subscribe_with_clock(config, None).await
}

// The gateway always provides its shared REST clock. Keep the public
// low-level helper compatible for callers that supply their own local clock.
pub(crate) async fn connect_and_subscribe_with_clock(
    config: &SpotStreamConfig,
    rest: Option<&BinanceRestClient>,
) -> Result<WsApiSocket, String> {
    let mut retried = false;
    loop {
        let mut rejected_timestamp = false;
        let result = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
            let (mut socket, _) = tokio_tungstenite::connect_async(config.url.as_str())
                .await
                .map_err(|error| format!("could not connect to {}: {error}", config.url))?;
            let timestamp_ms = match rest {
                Some(rest) => rest
                    .signed_timestamp(ApiFamily::Spot)
                    .await
                    .map_err(|reason| format!("Spot subscription clock unavailable: {reason}"))?,
                None => chrono::Utc::now().timestamp_millis(),
            };
            let request_id = uuid::Uuid::new_v4().to_string();
            let request = subscribe_request(
                config.signer.as_ref(),
                config.recv_window_ms,
                timestamp_ms,
                &request_id,
            )?;
            socket
                .send(Message::Text(request.into()))
                .await
                .map_err(|error| format!("could not send the subscribe request: {error}"))?;
            loop {
                match socket.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let Ok(reply) = serde_json::from_str::<Value>(&text) else {
                            continue;
                        };
                        if reply.get("id").and_then(Value::as_str) == Some(request_id.as_str()) {
                            if reply.get("status").and_then(Value::as_i64) == Some(400)
                                && reply
                                    .get("error")
                                    .and_then(|e| e.get("code"))
                                    .and_then(Value::as_i64)
                                    == Some(-1021)
                                && let Some(rest) = rest
                            {
                                rest.invalidate_clock(ApiFamily::Spot).await;
                                rejected_timestamp = true;
                            }
                            return check_subscribe_reply(&reply).map(|()| socket);
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        return Err(format!("socket error while subscribing: {error}"));
                    }
                    None => return Err("the connection closed while subscribing".to_string()),
                }
            }
        })
        .await
        .map_err(|_| format!("timed out after {HANDSHAKE_TIMEOUT:?} connecting and subscribing"))?;
        if rejected_timestamp && !retried {
            retried = true;
            tracing::warn!(
                "Spot subscription rejected timestamp; refreshing and reconnecting once"
            );
            continue;
        }
        return result;
    }
}

fn parse_execution_report(event: &Value) -> Option<ExchangeUpdate> {
    // `C` is the id of the order being cancelled on a cancel report and is
    // empty on every other report; `c` is then the cancel request's own id.
    let order_ref = event
        .get("C")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .or_else(|| event.get("c").and_then(Value::as_str))?;
    let client_order_id = order_ref.parse::<uuid::Uuid>().ok()?;
    let exchange_order_id = event.get("i")?.as_i64()?.to_string();
    let status = parse_binance_status(event.get("X")?.as_str()?)?;
    Some(ExchangeUpdate {
        client_order_id: apexes_core::OrderId(client_order_id),
        exchange_order_id: Some(exchange_order_id),
        status,
    })
}

/// Classifies one text frame from the socket.
pub fn parse_frame(raw: &str) -> Frame {
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        return Frame::Ignore;
    };
    let Some(event) = value.get("event") else {
        return Frame::Ignore;
    };
    match event.get("e").and_then(Value::as_str) {
        Some("executionReport") => {
            parse_execution_report(event).map_or(Frame::Ignore, Frame::Update)
        }
        Some("serverShutdown") => Frame::Reconnect("the server is shutting down"),
        Some("eventStreamTerminated") => Frame::Reconnect("the event stream was terminated"),
        _ => Frame::Ignore,
    }
}

/// Runs the subscription for the life of the process (until the receiver is
/// dropped). `first` is a socket the caller already subscribed, so startup
/// fails fast on bad credentials without subscribing twice.
pub(crate) async fn run_ws_stream(
    config: SpotStreamConfig,
    first: Option<WsApiSocket>,
    tx: mpsc::UnboundedSender<ExchangeUpdate>,
    resync: Arc<Notify>,
    timings: StreamTimings,
    rest: Option<BinanceRestClient>,
    health: Option<Arc<apexes_oms::KillSwitch>>,
) {
    let mut first = first;
    let mut failures = 0u32;
    loop {
        let mut socket = match first.take() {
            Some(socket) => socket,
            None => match connect_and_subscribe_with_clock(&config, rest.as_ref()).await {
                Ok(socket) => socket,
                Err(reason) => {
                    if let Some(ks) = &health {
                        ks.fault("exchange user-data connection unavailable");
                    }
                    tracing::warn!(
                        reason,
                        "user-data stream: could not (re)subscribe; will retry"
                    );
                    tokio::time::sleep(backoff_delay(&timings, failures)).await;
                    failures = failures.saturating_add(1);
                    continue;
                }
            },
        };
        failures = 0;
        tracing::info!("Spot user-data stream subscribed (WebSocket API)");
        let telemetry = apexes_telemetry::metrics();
        telemetry
            .stream_connected
            .with_label_values(&["spot"])
            .set(1.0);
        telemetry
            .stream_reconnects
            .with_label_values(&["spot"])
            .inc();
        // Anything pushed while we were away is gone: have the consumer
        // reconcile right away.
        resync.notify_one();

        loop {
            match tokio::time::timeout(timings.idle_timeout, socket.next()).await {
                Err(_) => {
                    tracing::warn!("user-data stream idle timeout; reconnecting");
                    break;
                }
                Ok(None) => {
                    tracing::warn!("user-data stream closed; reconnecting");
                    break;
                }
                Ok(Some(Err(error))) => {
                    tracing::warn!(?error, "user-data stream socket error; reconnecting");
                    break;
                }
                Ok(Some(Ok(Message::Text(text)))) => match parse_frame(&text) {
                    Frame::Update(update) => {
                        if tx.send(update).is_err() {
                            return; // receiver dropped: the session is over
                        }
                    }
                    Frame::Reconnect(reason) => {
                        tracing::warn!(
                            reason,
                            "user-data stream ended by the exchange; reconnecting"
                        );
                        break;
                    }
                    Frame::Ignore => {}
                },
                Ok(Some(Ok(Message::Close(_)))) => {
                    tracing::warn!("user-data stream received Close; reconnecting");
                    break;
                }
                Ok(Some(Ok(_))) => {}
            }
        }
        if let Some(ks) = &health {
            ks.fault("exchange user-data stream disconnected");
        }
        telemetry
            .stream_connected
            .with_label_values(&["spot"])
            .set(0.0);
        tokio::time::sleep(backoff_delay(&timings, 0)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::HmacSigner;
    use apexes_core::{OrderId, OrderStatus};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

    const KEY: &str = "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A";
    const SECRET: &str = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";

    fn signer() -> Arc<dyn ParamSigner> {
        Arc::new(HmacSigner::new(KEY, SECRET))
    }

    fn envelope(event: Value) -> String {
        json!({"subscriptionId": 0, "event": event}).to_string()
    }

    fn report(client_id: &str, order_id: i64, status: &str) -> Value {
        json!({"e": "executionReport", "c": client_id, "C": "", "i": order_id, "X": status, "x": status})
    }

    // ---- request shape -------------------------------------------------

    #[test]
    fn the_subscribe_request_is_signed_over_the_sorted_params() {
        let request = subscribe_request(
            &HmacSigner::new(KEY, SECRET),
            5000,
            1_770_736_694_138,
            "id-1",
        )
        .expect("request");
        let value: Value = serde_json::from_str(&request).unwrap();
        assert_eq!(value["method"], "userDataStream.subscribe.signature");
        assert_eq!(value["id"], "id-1");
        let params = &value["params"];
        assert_eq!(params["apiKey"], KEY);
        assert_eq!(params["recvWindow"], 5000);
        assert_eq!(params["timestamp"], 1_770_736_694_138i64);
        // The signature is HMAC-SHA256 over exactly `apiKey&recvWindow&timestamp`.
        let expected = crate::signing::sign_query_string(
            SECRET,
            &format!("apiKey={KEY}&recvWindow=5000&timestamp=1770736694138"),
        );
        assert_eq!(params["signature"], expected);
        assert!(
            !request.contains(SECRET),
            "the secret must never be on the wire"
        );
    }

    #[test]
    fn a_key_that_would_need_escaping_is_refused_not_mis_signed() {
        for bad in ["", "has space", "a&b=c", "key\u{e9}"] {
            let error = subscribe_request(&HmacSigner::new(bad, SECRET), 5000, 1, "x").unwrap_err();
            assert!(error.contains("alphanumeric"), "{bad:?}: {error}");
            assert!(!error.contains(SECRET));
        }
    }

    #[test]
    fn a_signer_failure_is_reported_without_leaking_anything() {
        struct Failing;
        impl ParamSigner for Failing {
            fn api_key(&self) -> &str {
                "abc123"
            }
            fn sign(&self, _: &str) -> Result<String, String> {
                Err("agent refused".to_string())
            }
        }
        assert_eq!(
            subscribe_request(&Failing, 5000, 1, "x").unwrap_err(),
            "agent refused"
        );
    }

    #[test]
    fn replies_are_judged_by_status_and_carry_the_exchanges_message() {
        assert!(
            check_subscribe_reply(&json!({"status": 200, "result": {"subscriptionId": 0}})).is_ok()
        );
        let error = check_subscribe_reply(
            &json!({"status": 401, "error": {"code": -2015, "msg": "Invalid API-key, IP, or permissions for action."}}),
        )
        .unwrap_err();
        assert!(
            error.contains("-2015") && error.contains("Invalid API-key"),
            "{error}"
        );
        assert!(check_subscribe_reply(&json!({"nonsense": true})).is_err());
    }

    // ---- frame parsing (shapes captured from the real testnet) ---------

    #[test]
    fn a_new_order_report_becomes_an_update_for_that_order() {
        let id = uuid::Uuid::new_v4();
        let frame = envelope(report(&id.to_string(), 14_228_644, "NEW"));
        assert_eq!(
            parse_frame(&frame),
            Frame::Update(ExchangeUpdate {
                client_order_id: OrderId(id),
                exchange_order_id: Some("14228644".to_string()),
                status: OrderStatus::New,
            })
        );
    }

    #[test]
    fn a_cancel_report_is_keyed_by_the_cancelled_order_not_the_cancel_request() {
        let order = uuid::Uuid::new_v4();
        let cancel_request = uuid::Uuid::new_v4();
        let frame = envelope(json!({
            "e": "executionReport",
            "c": cancel_request.to_string(),   // id of the cancel request
            "C": order.to_string(),            // id of the order that was cancelled
            "i": 7, "X": "CANCELED", "x": "CANCELED",
        }));
        match parse_frame(&frame) {
            Frame::Update(update) => {
                assert_eq!(
                    update.client_order_id,
                    OrderId(order),
                    "must be the original order"
                );
                assert_eq!(update.status, OrderStatus::Cancelled);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn fills_and_rejections_map_to_their_statuses() {
        let id = uuid::Uuid::new_v4().to_string();
        for (raw, expected) in [
            ("PARTIALLY_FILLED", OrderStatus::PartiallyFilled),
            ("FILLED", OrderStatus::Filled),
            ("REJECTED", OrderStatus::Rejected),
            ("EXPIRED", OrderStatus::Cancelled),
        ] {
            match parse_frame(&envelope(report(&id, 1, raw))) {
                Frame::Update(u) => assert_eq!(u.status, expected, "{raw}"),
                other => panic!("{raw}: {other:?}"),
            }
        }
    }

    #[test]
    fn the_exchange_ending_the_subscription_means_reconnect() {
        assert!(matches!(
            parse_frame(&envelope(json!({"e": "serverShutdown", "E": 1, "T": 2}))),
            Frame::Reconnect(_)
        ));
        assert!(matches!(
            parse_frame(&envelope(json!({"e": "eventStreamTerminated", "E": 1}))),
            Frame::Reconnect(_)
        ));
    }

    #[test]
    fn other_events_and_junk_are_ignored() {
        for raw in [
            envelope(json!({"e": "outboundAccountPosition", "B": []})),
            envelope(json!({"e": "balanceUpdate", "a": "BTC", "d": "1"})),
            // The OLD un-enveloped shape must not be mistaken for an event.
            report(&uuid::Uuid::new_v4().to_string(), 1, "NEW").to_string(),
            "not json".to_string(),
            "{}".to_string(),
            // Not a UUID client id (an order placed by hand): ignored, not fatal.
            envelope(report("manual-order", 1, "NEW")),
            // Unmapped status.
            envelope(report(
                &uuid::Uuid::new_v4().to_string(),
                1,
                "EXPIRED_IN_MATCH",
            )),
        ] {
            assert_eq!(parse_frame(&raw), Frame::Ignore, "{raw}");
        }
    }

    // ---- a fake Binance WebSocket API ----------------------------------

    /// What the fake server does on one connection.
    #[derive(Clone)]
    enum Behaviour {
        /// Accept the subscription, push `frames`, then hold the line open.
        Accept(Vec<String>),
        /// Accept, push `frames`, then close.
        AcceptThenClose(Vec<String>),
        /// Reject the subscription with this status/code.
        Reject(i64, i64),
        /// Accept, then stay silent.
        Silent,
    }

    struct FakeWsApi {
        url: String,
        connections: Arc<AtomicUsize>,
        requests: Arc<std::sync::Mutex<Vec<Value>>>,
    }

    /// Connection N (0-based) follows `script[N]`; later ones follow the last.
    async fn fake_ws_api(script: Vec<Behaviour>) -> FakeWsApi {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let connections = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (count, seen) = (Arc::clone(&connections), Arc::clone(&requests));
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    return;
                };
                let n = count.fetch_add(1, Ordering::SeqCst);
                let behaviour = script.get(n).or(script.last()).cloned().unwrap();
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    let Ok(mut ws) = tokio_tungstenite::accept_async(tcp).await else {
                        return;
                    };
                    let Some(Ok(Message::Text(text))) = ws.next().await else {
                        return;
                    };
                    let request: Value = serde_json::from_str(&text).unwrap();
                    let id = request["id"].clone();
                    seen.lock().unwrap().push(request);
                    if let Behaviour::Reject(status, code) = behaviour {
                        let reply = json!({"id": id, "status": status,
                            "error": {"code": code, "msg": "Invalid API-key, IP, or permissions for action."}});
                        let _ = ws.send(Message::Text(reply.to_string().into())).await;
                        return;
                    }
                    let reply = json!({"id": id, "status": 200, "result": {"subscriptionId": 0}});
                    let _ = ws.send(Message::Text(reply.to_string().into())).await;
                    let close = matches!(behaviour, Behaviour::AcceptThenClose(_));
                    match behaviour {
                        Behaviour::Accept(frames) | Behaviour::AcceptThenClose(frames) => {
                            for frame in frames {
                                let _ = ws.send(Message::Text(frame.into())).await;
                            }
                            if close {
                                let _ = ws.close(None).await;
                            } else {
                                futures::future::pending::<()>().await;
                            }
                        }
                        _ => futures::future::pending::<()>().await,
                    }
                });
            }
        });
        FakeWsApi {
            url: format!("ws://127.0.0.1:{port}/ws-api/v3"),
            connections,
            requests,
        }
    }

    fn config(url: &str) -> SpotStreamConfig {
        SpotStreamConfig {
            url: url.to_string(),
            signer: signer(),
            recv_window_ms: 5000,
        }
    }

    fn fast() -> StreamTimings {
        StreamTimings {
            renew_every: Duration::from_secs(3600),
            idle_timeout: Duration::from_secs(3600),
            reconnect_base: Duration::from_millis(10),
            reconnect_max: Duration::from_millis(50),
        }
    }

    async fn next_update(rx: &mut mpsc::UnboundedReceiver<ExchangeUpdate>) -> ExchangeUpdate {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for an update")
            .expect("channel closed")
    }

    // ---- connect / subscribe -------------------------------------------

    #[tokio::test]
    async fn a_valid_subscription_is_made_with_a_correct_signature() {
        let server = fake_ws_api(vec![Behaviour::Accept(vec![])]).await;
        connect_and_subscribe(&config(&server.url))
            .await
            .expect("subscribed");

        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let params = &requests[0]["params"];
        let payload = format!(
            "apiKey={KEY}&recvWindow=5000&timestamp={}",
            params["timestamp"]
        );
        assert_eq!(
            params["signature"],
            crate::signing::sign_query_string(SECRET, &payload),
            "the server can verify the signature with the shared secret"
        );
    }

    #[tokio::test]
    async fn gateway_subscription_uses_synchronized_clock_and_retries_timestamp_rejection() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let http = MockServer::start().await;
        for (stamp, first) in [(1_800_000_000_000i64, true), (1_800_000_010_000i64, false)] {
            let mock = Mock::given(method("GET"))
                .and(path("/api/v3/time"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(json!({"serverTime": stamp})),
                )
                .expect(1);
            if first {
                mock.up_to_n_times(1).mount(&http).await;
            } else {
                mock.mount(&http).await;
            }
        }
        let rest = BinanceRestClient::new(crate::config::BinanceGatewayConfig {
            api_key: KEY.into(),
            api_secret: SECRET.into(),
            spot_base_url: http.uri(),
            futures_base_url: http.uri(),
            recv_window_ms: 5000,
        });
        let server = fake_ws_api(vec![
            Behaviour::Reject(400, -1021),
            Behaviour::Accept(vec![]),
        ])
        .await;
        connect_and_subscribe_with_clock(&config(&server.url), Some(&rest))
            .await
            .unwrap();
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for (request, expected) in requests
            .iter()
            .zip([1_800_000_000_000i64, 1_800_000_010_000i64])
        {
            let params = &request["params"];
            let timestamp = params["timestamp"].as_i64().unwrap();
            assert!((expected..expected + 1000).contains(&timestamp));
            let payload = format!("apiKey={KEY}&recvWindow=5000&timestamp={timestamp}");
            assert_eq!(
                params["signature"],
                crate::signing::sign_query_string(SECRET, &payload)
            );
        }
    }

    #[tokio::test]
    async fn a_rejected_subscription_fails_startup_with_the_exchanges_reason() {
        let server = fake_ws_api(vec![Behaviour::Reject(401, -2015)]).await;
        let error = connect_and_subscribe(&config(&server.url))
            .await
            .unwrap_err();
        assert!(
            error.contains("-2015") && error.contains("Invalid API-key"),
            "{error}"
        );
        assert!(!error.contains(SECRET));
    }

    #[tokio::test]
    async fn an_unreachable_endpoint_is_a_clear_error() {
        let error = connect_and_subscribe(&config("ws://127.0.0.1:1/ws-api/v3"))
            .await
            .unwrap_err();
        assert!(error.contains("could not connect"), "{error}");
    }

    // ---- streaming and recovery ----------------------------------------

    #[tokio::test]
    async fn updates_are_delivered_and_every_subscribe_signals_a_resync() {
        let order = uuid::Uuid::new_v4();
        let server = fake_ws_api(vec![Behaviour::Accept(vec![
            envelope(report(&order.to_string(), 1, "NEW")),
            envelope(json!({"e": "outboundAccountPosition", "B": []})),
            envelope(report(&order.to_string(), 1, "FILLED")),
        ])])
        .await;
        let cfg = config(&server.url);
        let first = connect_and_subscribe(&cfg).await.unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let resync = Arc::new(Notify::new());
        let task = tokio::spawn(run_ws_stream(
            cfg,
            Some(first),
            tx,
            Arc::clone(&resync),
            fast(),
            None,
            None,
        ));

        tokio::time::timeout(Duration::from_secs(5), resync.notified())
            .await
            .expect("resync on subscribe");
        assert_eq!(next_update(&mut rx).await.status, OrderStatus::New);
        assert_eq!(
            next_update(&mut rx).await.status,
            OrderStatus::Filled,
            "non-order events are skipped"
        );
        task.abort();
    }

    #[tokio::test]
    async fn a_dropped_connection_resubscribes_with_a_fresh_signature_and_resyncs() {
        let a = uuid::Uuid::new_v4();
        let b = uuid::Uuid::new_v4();
        let server = fake_ws_api(vec![
            Behaviour::AcceptThenClose(vec![envelope(report(&a.to_string(), 1, "NEW"))]),
            Behaviour::Accept(vec![envelope(report(&b.to_string(), 2, "NEW"))]),
        ])
        .await;
        let cfg = config(&server.url);
        let first = connect_and_subscribe(&cfg).await.unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let resync = Arc::new(Notify::new());
        let task = tokio::spawn(run_ws_stream(
            cfg,
            Some(first),
            tx,
            Arc::clone(&resync),
            fast(),
            None,
            None,
        ));

        tokio::time::timeout(Duration::from_secs(5), resync.notified())
            .await
            .expect("first resync");
        assert_eq!(next_update(&mut rx).await.client_order_id, OrderId(a));
        tokio::time::timeout(Duration::from_secs(5), resync.notified())
            .await
            .expect("resync after reconnect");
        assert_eq!(next_update(&mut rx).await.client_order_id, OrderId(b));
        assert_eq!(server.connections.load(Ordering::SeqCst), 2);

        let requests = server.requests.lock().unwrap();
        assert_ne!(
            requests[0]["params"]["timestamp"], requests[1]["params"]["timestamp"],
            "each subscribe is freshly timestamped and signed (a replayed signature would be rejected)"
        );
        task.abort();
    }

    #[tokio::test]
    async fn the_exchange_announcing_shutdown_triggers_a_reconnect() {
        let server = fake_ws_api(vec![
            Behaviour::Accept(vec![envelope(
                json!({"e": "serverShutdown", "E": 1, "T": 2}),
            )]),
            Behaviour::Accept(vec![]),
        ])
        .await;
        let cfg = config(&server.url);
        let first = connect_and_subscribe(&cfg).await.unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(run_ws_stream(
            cfg,
            Some(first),
            tx,
            Arc::new(Notify::new()),
            fast(),
            None,
            None,
        ));
        for _ in 0..100 {
            if server.connections.load(Ordering::SeqCst) >= 2 {
                task.abort();
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("no reconnect after serverShutdown");
    }

    #[tokio::test]
    async fn a_failing_resubscribe_is_retried_with_backoff_not_fatal() {
        let server = fake_ws_api(vec![
            Behaviour::AcceptThenClose(vec![]),
            Behaviour::Reject(400, -1021), // clock skew, say
            Behaviour::Reject(400, -1021),
            Behaviour::Accept(vec![]),
        ])
        .await;
        let cfg = config(&server.url);
        let first = connect_and_subscribe(&cfg).await.unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        let resync = Arc::new(Notify::new());
        let task = tokio::spawn(run_ws_stream(
            cfg,
            Some(first),
            tx,
            Arc::clone(&resync),
            fast(),
            None,
            None,
        ));
        // first subscribe, then after two rejections a successful third
        tokio::time::timeout(Duration::from_secs(5), resync.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), resync.notified())
            .await
            .expect("must recover after failed resubscribes");
        assert!(server.connections.load(Ordering::SeqCst) >= 4);
        task.abort();
    }

    #[tokio::test]
    async fn a_silent_connection_is_recycled_after_the_idle_timeout() {
        let server = fake_ws_api(vec![Behaviour::Silent]).await;
        let cfg = config(&server.url);
        let first = connect_and_subscribe(&cfg).await.unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        let timings = StreamTimings {
            idle_timeout: Duration::from_millis(100),
            ..fast()
        };
        let task = tokio::spawn(run_ws_stream(
            cfg,
            Some(first),
            tx,
            Arc::new(Notify::new()),
            timings,
            None,
            None,
        ));
        for _ in 0..100 {
            if server.connections.load(Ordering::SeqCst) >= 2 {
                task.abort();
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("silent socket was never recycled");
    }

    #[tokio::test]
    async fn dropping_the_consumer_ends_the_task() {
        let order = uuid::Uuid::new_v4();
        let server = fake_ws_api(vec![Behaviour::Accept(vec![envelope(report(
            &order.to_string(),
            1,
            "NEW",
        ))])])
        .await;
        let cfg = config(&server.url);
        let first = connect_and_subscribe(&cfg).await.unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let task = tokio::spawn(run_ws_stream(
            cfg,
            Some(first),
            tx,
            Arc::new(Notify::new()),
            fast(),
            None,
            None,
        ));
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the task must stop once nobody is listening")
            .unwrap();
    }
    #[tokio::test]
    async fn managed_exchange_disconnect_latches_session_even_after_resubscribe() {
        let server = fake_ws_api(vec![
            Behaviour::AcceptThenClose(vec![]),
            Behaviour::Accept(vec![]),
        ])
        .await;
        let cfg = config(&server.url);
        let first = connect_and_subscribe(&cfg).await.unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        let resync = Arc::new(Notify::new());
        let ks = Arc::new(apexes_oms::KillSwitch::managed());
        ks.begin_reconciliation();
        ks.startup_ready();
        let task = tokio::spawn(run_ws_stream(
            cfg,
            Some(first),
            tx,
            resync.clone(),
            fast(),
            None,
            Some(ks.clone()),
        ));
        tokio::time::timeout(Duration::from_secs(5), ks.wait_engaged())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while server.connections.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(ks.phase(), Some(apexes_oms::SessionPhase::Degraded));
        assert!(ks.blocks_submissions());
        assert!(ks.try_release().is_err());
        task.abort();
    }
}
