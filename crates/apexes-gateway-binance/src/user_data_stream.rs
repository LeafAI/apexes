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
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc};
use tokio_tungstenite::tungstenite::Message;

use crate::rest::{ApiFamily, BinanceRestClient};

/// Keeps the per-family stream tasks alive (each owns its own listenKey
/// lifecycle: obtain, renew, re-obtain on every reconnect). Dropping this
/// aborts both tasks immediately -- callers must keep it alive for as long
/// as they want the User Data Stream to keep running.
pub struct UserDataStreamHandle {
    _spot: tokio::task::JoinHandle<()>,
    _futures: Option<tokio::task::JoinHandle<()>>,
    resync: Arc<Notify>,
}

impl UserDataStreamHandle {
    /// Notified after every (re)connection of either stream. A websocket
    /// gap means pushed updates may have been missed, so consumers should
    /// run a full `OrderManager::reconcile()` on each notification rather
    /// than wait for the next poll. Uses `notify_one` semantics: a
    /// connection that happened before anyone waits is not lost.
    pub fn resync_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.resync)
    }
}

impl Drop for UserDataStreamHandle {
    fn drop(&mut self) {
        self._spot.abort();
        if let Some(futures) = &self._futures {
            futures.abort();
        }
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
    // Safe to retry: while a key is active Binance returns the same key.
    let (status, body) = rest
        .signed_request_idempotent("POST", family, path, "")
        .await?;
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

fn futures_websocket_host() -> &'static str {
    "wss://fstream.binance.com/ws"
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

pub(crate) fn parse_binance_status(status: &str) -> Option<OrderStatus> {
    match status {
        "NEW" => Some(OrderStatus::New),
        "PARTIALLY_FILLED" => Some(OrderStatus::PartiallyFilled),
        "FILLED" => Some(OrderStatus::Filled),
        "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" => Some(OrderStatus::Cancelled),
        "REJECTED" => Some(OrderStatus::Rejected),
        // Statuses outside this gateway's current OrderStatus mapping
        // (e.g. futures' NEW_INSURANCE / NEW_ADL) intentionally drop the whole event rather than
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

/// Timing knobs for one stream task; injectable so tests run in
/// milliseconds.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StreamTimings {
    /// listenKey keep-alive period (Binance expires keys after 60 min).
    pub(crate) renew_every: Duration,
    /// Reconnect if no frame (data OR ping) arrives for this long.
    pub(crate) idle_timeout: Duration,
    pub(crate) reconnect_base: Duration,
    pub(crate) reconnect_max: Duration,
}

impl Default for StreamTimings {
    fn default() -> Self {
        Self {
            renew_every: Duration::from_secs(30 * 60),
            // Spot pings every 20s and Futures every 3 min; any frame counts.
            idle_timeout: Duration::from_secs(10 * 60),
            reconnect_base: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
        }
    }
}

/// Starts the user-data streams and returns a merged stream of
/// `ExchangeUpdate`s plus a handle that keeps the tasks alive and exposes
/// [`UserDataStreamHandle::resync_notifier`].
///
/// Spot uses the WebSocket-API subscription (`crate::ws_user_stream`):
/// Binance removed the Spot listenKey stream. USDS-M Futures still uses its
/// listenKey stream, and is only started when `with_futures` is set.
///
/// The Spot subscription is made eagerly, so bad credentials, a wrong
/// network or a clock problem fail startup with the exchange's own message;
/// afterwards the stream task recovers on its own.
pub async fn start_user_data_stream(
    rest: BinanceRestClient,
    spot: crate::ws_user_stream::SpotStreamConfig,
    with_futures: bool,
) -> Result<(UserDataStreamHandle, impl Stream<Item = ExchangeUpdate>), String> {
    start_user_data_stream_guarded(rest, spot, with_futures, None).await
}

pub(crate) async fn start_user_data_stream_guarded(
    rest: BinanceRestClient,
    spot: crate::ws_user_stream::SpotStreamConfig,
    with_futures: bool,
    health: Option<Arc<apexes_oms::KillSwitch>>,
) -> Result<(UserDataStreamHandle, impl Stream<Item = ExchangeUpdate>), String> {
    let (tx, rx) = mpsc::unbounded_channel();
    let resync = Arc::new(Notify::new());

    let first = crate::ws_user_stream::connect_and_subscribe_with_clock(&spot, Some(&rest)).await?;
    if with_futures {
        obtain_listen_key(&rest, ApiFamily::Futures).await?;
    }

    let spot_task = tokio::spawn(crate::ws_user_stream::run_ws_stream(
        spot,
        Some(first),
        tx.clone(),
        Arc::clone(&resync),
        StreamTimings::default(),
        Some(rest.clone()),
        health.clone(),
    ));
    let futures = with_futures.then(|| {
        tokio::spawn(run_stream_task(
            rest,
            ApiFamily::Futures,
            futures_websocket_host().to_string(),
            tx,
            Arc::clone(&resync),
            StreamTimings::default(),
            health,
        ))
    });

    Ok((
        UserDataStreamHandle {
            _spot: spot_task,
            _futures: futures,
            resync,
        },
        tokio_stream_from_receiver(rx),
    ))
}

fn tokio_stream_from_receiver(
    rx: mpsc::UnboundedReceiver<ExchangeUpdate>,
) -> impl Stream<Item = ExchangeUpdate> {
    stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| (update, rx))
    })
}

pub(crate) fn backoff_delay(timings: &StreamTimings, failures: u32) -> Duration {
    timings
        .reconnect_base
        .saturating_mul(1u32 << failures.min(16))
        .min(timings.reconnect_max)
}

/// One family's whole lifecycle, forever (until the receiver is dropped):
/// obtain a listenKey -> connect -> notify resync -> read frames while
/// renewing the key -> on ANY failure (socket error/close, idle timeout,
/// failed renewal) start over with a freshly obtained key. Re-obtaining
/// on every reconnect is what recovers from an expired/revoked key:
/// Binance returns the existing key while it is still valid and issues a
/// new one otherwise.
async fn run_stream_task(
    rest: BinanceRestClient,
    family: ApiFamily,
    ws_base: String,
    tx: mpsc::UnboundedSender<ExchangeUpdate>,
    resync: Arc<Notify>,
    timings: StreamTimings,
    health: Option<Arc<apexes_oms::KillSwitch>>,
) {
    let mut failures = 0u32;
    loop {
        let listen_key = match obtain_listen_key(&rest, family).await {
            Ok(key) => key,
            Err(reason) => {
                if let Some(ks) = &health {
                    ks.fault("exchange user-data connection unavailable");
                }
                tracing::warn!(reason, ?family, "failed to obtain listenKey; will retry");
                tokio::time::sleep(backoff_delay(&timings, failures)).await;
                failures = failures.saturating_add(1);
                continue;
            }
        };
        let url = format!("{ws_base}/{listen_key}");
        let (ws_stream, _) = match tokio_tungstenite::connect_async(&url).await {
            Ok(connected) => connected,
            Err(error) => {
                tracing::warn!(
                    ?error,
                    ?family,
                    "failed to connect User Data Stream WebSocket"
                );
                tokio::time::sleep(backoff_delay(&timings, failures)).await;
                failures = failures.saturating_add(1);
                continue;
            }
        };
        failures = 0;
        tracing::info!(?family, "User Data Stream connected");
        let family_label = match family {
            ApiFamily::Spot => "spot",
            ApiFamily::Futures => "futures",
        };
        let telemetry = apexes_telemetry::metrics();
        telemetry
            .stream_connected
            .with_label_values(&[family_label])
            .set(1.0);
        telemetry
            .stream_reconnects
            .with_label_values(&[family_label])
            .inc();
        // Updates pushed while we were disconnected are gone: have the
        // consumer reconcile immediately.
        resync.notify_one();

        let (_, mut read) = ws_stream.split();
        let mut renew = tokio::time::interval_at(
            tokio::time::Instant::now() + timings.renew_every,
            timings.renew_every,
        );
        loop {
            tokio::select! {
                frame = tokio::time::timeout(timings.idle_timeout, read.next()) => match frame {
                    Err(_) => {
                        tracing::warn!(?family, "User Data Stream idle timeout; reconnecting");
                        break;
                    }
                    Ok(None) => {
                        tracing::warn!(?family, "User Data Stream closed; reconnecting");
                        break;
                    }
                    Ok(Some(Err(error))) => {
                        tracing::warn!(?error, ?family, "User Data Stream WebSocket error; reconnecting");
                        break;
                    }
                    Ok(Some(Ok(Message::Text(text)))) => {
                        if let Some(update) = parse_event(&text)
                            && tx.send(update).is_err()
                        {
                            return; // receiver dropped, stop this task
                        }
                    }
                    Ok(Some(Ok(Message::Close(_)))) => {
                        tracing::warn!(?family, "User Data Stream received Close; reconnecting");
                        break;
                    }
                    Ok(Some(Ok(_))) => {}
                },
                _ = renew.tick() => {
                    if let Err(reason) = renew_listen_key(&rest, family, &listen_key).await {
                        tracing::warn!(reason, ?family, "failed to renew listenKey; re-establishing stream");
                        break;
                    }
                }
            }
        }
        telemetry
            .stream_connected
            .with_label_values(&[family_label])
            .set(0.0);
        if let Some(ks) = &health {
            ks.fault("exchange user-data stream disconnected");
        }
        tokio::time::sleep(backoff_delay(&timings, 0)).await;
    }
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

    async fn mount_clock(server: &MockServer) {
        for time_path in ["/api/v3/time", "/fapi/v1/time"] {
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .and(wiremock::matchers::path(time_path))
                .respond_with(|_: &wiremock::Request| {
                    wiremock::ResponseTemplate::new(200).set_body_json(
                        serde_json::json!({"serverTime": chrono::Utc::now().timestamp_millis()}),
                    )
                })
                .with_priority(1)
                .mount(server)
                .await;
        }
    }

    #[tokio::test]
    async fn obtain_listen_key_sends_post_and_parses_the_key() {
        let mock_server = MockServer::start().await;
        mount_clock(&mock_server).await;

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
        mount_clock(&mock_server).await;

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
        mount_clock(&mock_server).await;

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
        mount_clock(&mock_server).await;

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
        mount_clock(&mock_server).await;

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
        mount_clock(&mock_server).await;

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

    // ---- reconnect / listenKey-recovery behaviour -------------------

    use tokio::net::TcpListener;
    use tokio::sync::mpsc::UnboundedReceiver;

    fn fast_timings() -> StreamTimings {
        StreamTimings {
            renew_every: Duration::from_secs(3600),
            idle_timeout: Duration::from_secs(3600),
            reconnect_base: Duration::from_millis(10),
            reconnect_max: Duration::from_millis(50),
        }
    }

    fn exec_report(order_id: i64) -> String {
        serde_json::json!({
            "e": "executionReport",
            "c": uuid::Uuid::new_v4().to_string(),
            "i": order_id,
            "X": "NEW",
        })
        .to_string()
    }

    /// Fake Binance WS: connection N (1-based) sends `scripts[N-1]`
    /// frames; with `close_after == true` it then closes, else it holds
    /// the connection open. Returns (port, connection counter).
    async fn fake_ws_server(
        scripts: Vec<(Vec<String>, bool)>,
    ) -> (u16, Arc<std::sync::atomic::AtomicUsize>) {
        use futures::SinkExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&count);
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let (frames, close_after) = scripts
                    .get(n)
                    .cloned()
                    .unwrap_or_else(|| (Vec::new(), false));
                tokio::spawn(async move {
                    let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
                    for frame in frames {
                        ws.send(Message::Text(frame.into())).await.unwrap();
                    }
                    if close_after {
                        let _ = ws.close(None).await;
                    } else {
                        futures::future::pending::<()>().await;
                    }
                });
            }
        });
        (port, count)
    }

    async fn listen_key_mock(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/api/v3/userDataStream"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "listenKey": "key"
            })))
            .mount(server)
            .await;
    }

    async fn recv(rx: &mut UnboundedReceiver<ExchangeUpdate>) -> ExchangeUpdate {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for update")
            .expect("channel closed")
    }

    #[tokio::test]
    async fn reconnects_with_a_fresh_listen_key_and_signals_resync_each_time() {
        let server = MockServer::start().await;
        mount_clock(&server).await;
        listen_key_mock(&server).await;
        let (port, connections) = fake_ws_server(vec![
            (vec![exec_report(1)], true), // sends one update then drops
            (vec![exec_report(2)], false),
        ])
        .await;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let resync = Arc::new(Notify::new());
        let task = tokio::spawn(run_stream_task(
            rest_against(&server),
            ApiFamily::Spot,
            format!("ws://127.0.0.1:{port}/ws"),
            tx,
            Arc::clone(&resync),
            fast_timings(),
            None,
        ));

        // First connection: resync + update 1.
        tokio::time::timeout(Duration::from_secs(5), resync.notified())
            .await
            .expect("no resync after first connect");
        assert_eq!(recv(&mut rx).await.exchange_order_id.as_deref(), Some("1"));
        // Server dropped us: reconnect -> another resync + update 2.
        tokio::time::timeout(Duration::from_secs(5), resync.notified())
            .await
            .expect("no resync after reconnect");
        assert_eq!(recv(&mut rx).await.exchange_order_id.as_deref(), Some("2"));

        assert_eq!(connections.load(std::sync::atomic::Ordering::SeqCst), 2);
        let key_requests = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.method.as_str() == "POST")
            .count();
        assert_eq!(
            key_requests, 2,
            "a listenKey must be (re)obtained per connect"
        );
        task.abort();
    }

    #[tokio::test]
    async fn failed_keepalive_forces_a_reconnect_with_a_new_key() {
        let server = MockServer::start().await;
        mount_clock(&server).await;
        listen_key_mock(&server).await;
        Mock::given(method("PUT"))
            .and(path("/api/v3/userDataStream"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "code": -1125, "msg": "This listenKey does not exist."
            })))
            .mount(&server)
            .await;
        let (port, connections) = fake_ws_server(vec![]).await; // holds every connection open

        let (tx, _rx) = mpsc::unbounded_channel();
        let timings = StreamTimings {
            renew_every: Duration::from_millis(50),
            ..fast_timings()
        };
        let task = tokio::spawn(run_stream_task(
            rest_against(&server),
            ApiFamily::Spot,
            format!("ws://127.0.0.1:{port}/ws"),
            tx,
            Arc::new(Notify::new()),
            timings,
            None,
        ));

        for _ in 0..100 {
            if connections.load(std::sync::atomic::Ordering::SeqCst) >= 2 {
                task.abort();
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("stream never reconnected after the keep-alive failed");
    }

    #[tokio::test]
    async fn idle_socket_is_dropped_and_reconnected() {
        let server = MockServer::start().await;
        mount_clock(&server).await;
        listen_key_mock(&server).await;
        let (port, connections) = fake_ws_server(vec![]).await; // silent connections

        let (tx, _rx) = mpsc::unbounded_channel();
        let timings = StreamTimings {
            idle_timeout: Duration::from_millis(100),
            ..fast_timings()
        };
        let task = tokio::spawn(run_stream_task(
            rest_against(&server),
            ApiFamily::Spot,
            format!("ws://127.0.0.1:{port}/ws"),
            tx,
            Arc::new(Notify::new()),
            timings,
            None,
        ));
        for _ in 0..100 {
            if connections.load(std::sync::atomic::Ordering::SeqCst) >= 2 {
                task.abort();
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("silent socket was never recycled");
    }
}
