//! Shared REST plumbing: signed-request construction, routing between
//! Binance's Spot and USDS-M Futures API families based on
//! `apexes_core::AssetClass`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use apexes_core::AssetClass;
use chrono::Utc;
use serde::Deserialize;
use tokio::time::Instant;

use crate::config::BinanceGatewayConfig;
use crate::error::BinanceApiError;
use crate::signing::sign_query_string;

/// Which Binance API family a request targets. Binance Spot and
/// USDS-M Perpetual Futures are entirely separate REST APIs (different
/// base URLs, different endpoint paths, some different parameter
/// semantics) -- this crate routes every request through one of the
/// two based on the `Order`/query's `AssetClass`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiFamily {
    Spot,
    Futures,
}

impl From<AssetClass> for ApiFamily {
    fn from(asset_class: AssetClass) -> Self {
        match asset_class {
            AssetClass::Spot => ApiFamily::Spot,
            AssetClass::Perpetual => ApiFamily::Futures,
            other => panic!(
                "BinanceExecutionGateway only supports Spot and Perpetual orders, got {other:?}"
            ),
        }
    }
}

/// Retry/backoff and rate-limit behaviour of [`BinanceRestClient`].
///
/// Only idempotent requests are retried (`GET`, `PUT` listenKey
/// keep-alive, and the listenKey `POST` via
/// [`BinanceRestClient::signed_request_idempotent`]): retrying an order
/// `POST`/`DELETE` after an ambiguous failure could double-submit, so
/// those surface the failure and let `OrderManager::reconcile` resolve it.
/// An explicit HTTP 400 / Binance -1021 timestamp rejection instead
/// permits one fresh synchronization and retry.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Extra attempts after the first for idempotent requests.
    pub max_retries: u32,
    /// First backoff delay; doubles each retry up to `max_delay`.
    pub base_delay: Duration,
    pub max_delay: Duration,
    /// Longest rate-limit wait honoured (from `Retry-After`). Longer
    /// bans are not waited out: the request fails instead.
    pub rate_limit_cap: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(5),
            rate_limit_cap: Duration::from_secs(30),
        }
    }
}

/// Used when a 418 (IP ban) carries no `Retry-After`.
const DEFAULT_BAN: Duration = Duration::from_secs(60);
/// Used when a 429 carries no `Retry-After`.
const DEFAULT_RATE_LIMIT_WAIT: Duration = Duration::from_secs(1);

/// A hostname can resolve to several addresses of which one is dead (the
/// Binance testnet does this: one of four addresses black-holes connections
/// and a default client then waits ~30s). The connect timeout is divided
/// across a host's addresses, so a dead one fails fast and the next is tried.
/// Requests are signed with a timestamp taken *before* connecting and valid
/// for `recvWindow` (5s), so waiting out a dead address would also turn into
/// a `-1021` rejection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(4);
/// Upper bound for a whole request, so a stalled server cannot hang a caller.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const CLOCK_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const MAX_CLOCK_RTT: Duration = Duration::from_secs(1);
const CLOCK_FAILURE_COOLDOWN: Duration = Duration::from_secs(1);

/// A server timestamp anchored to the midpoint of a monotonic RTT sample.
/// Wall-clock corrections after sampling cannot move the signed clock.
#[derive(Clone, Copy)]
struct ClockSample {
    server_ms: i64,
    midpoint: Instant,
    sampled_at: Instant,
}

impl ClockSample {
    fn timestamp(self, now: Instant) -> i64 {
        self.server_ms.saturating_add(
            i64::try_from(now.saturating_duration_since(self.midpoint).as_millis())
                .unwrap_or(i64::MAX),
        )
    }
}

#[derive(Default)]
struct ClockState {
    sample: Option<ClockSample>,
    retry_after: Option<Instant>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServerTime {
    server_time: i64,
}

fn build_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .build()
        .unwrap_or_else(|error| {
            tracing::warn!(
                ?error,
                "could not build the tuned HTTP client; using defaults"
            );
            reqwest::Client::new()
        })
}

/// Thin wrapper around a `reqwest::Client` plus the config needed to
/// build and sign Binance requests. `BinanceExecutionGateway` (Task 16)
/// holds one of these. Besides "build the URL, sign the query string,
/// send the request" it applies [`RetryConfig`] and a shared rate-limit
/// gate: after a 429/418 every request (from every clone of this
/// client) is held back until the server-indicated time. Idempotent
/// requests wait it out (up to `rate_limit_cap`); order-changing
/// requests fail fast with a synthetic 429 so they are never sent into
/// a ban.
#[derive(Clone)]
pub struct BinanceRestClient {
    http: reqwest::Client,
    config: BinanceGatewayConfig,
    retry: RetryConfig,
    /// Requests are held back until this instant (set by 429/418).
    blocked_until: Arc<Mutex<Option<Instant>>>,
    spot_clock: Arc<tokio::sync::Mutex<ClockState>>,
    futures_clock: Arc<tokio::sync::Mutex<ClockState>>,
}

impl BinanceRestClient {
    /// Non-reversible identifier for binding a local inventory database to
    /// the REST credential. Never log this value or send it to clients.
    pub(crate) fn credential_fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(self.config.api_key.as_bytes()))
    }

    pub fn new(config: BinanceGatewayConfig) -> Self {
        Self {
            http: build_http_client(),
            config,
            retry: RetryConfig::default(),
            blocked_until: Arc::new(Mutex::new(None)),
            spot_clock: Arc::new(tokio::sync::Mutex::new(ClockState::default())),
            futures_clock: Arc::new(tokio::sync::Mutex::new(ClockState::default())),
        }
    }

    pub fn with_retry_config(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// One unsigned GET of a public market-data endpoint. Sends no API key and
    /// no signature: the data is public, so the credentials stay out of it.
    /// `query` is an already URL-encoded `key=value` string.
    pub async fn public_get(&self, path: &str, query: &str) -> Result<String, String> {
        let url = format!("{}{path}?{query}", self.base_url(ApiFamily::Spot));
        let response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|error| format!("request failed: {error}"))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| format!("failed to read response body: {error}"))?;
        if status.is_success() {
            Ok(body)
        } else {
            Err(format!("HTTP {status}: {body}"))
        }
    }

    fn base_url(&self, family: ApiFamily) -> &str {
        match family {
            ApiFamily::Spot => &self.config.spot_base_url,
            ApiFamily::Futures => &self.config.futures_base_url,
        }
    }

    /// Builds the common `timestamp`/`recvWindow` query-string prefix
    /// every signed Binance endpoint requires, appends `extra_params`
    /// (already URL-encoded key=value pairs joined by `&`, caller's
    /// responsibility), computes the HMAC signature over the full
    /// resulting string, and appends `&signature=<hex>`.
    fn build_signed_query(&self, extra_params: &str, timestamp: i64) -> String {
        let base_query = if extra_params.is_empty() {
            format!(
                "timestamp={timestamp}&recvWindow={}",
                self.config.recv_window_ms
            )
        } else {
            format!(
                "{extra_params}&timestamp={timestamp}&recvWindow={}",
                self.config.recv_window_ms
            )
        };
        let signature = sign_query_string(&self.config.api_secret, &base_query);
        format!("{base_query}&signature={signature}")
    }

    /// Sends a signed request to `family`'s base URL + `path`, with
    /// `extra_params` as the unsigned portion of the query string (see
    /// `build_signed_query`). `method` is `"GET"`, `"POST"`, `"PUT"`, or
    /// `"DELETE"` per Binance's documented HTTP method per endpoint
    /// (`PUT` is used by the User Data Stream listenKey-renewal
    /// endpoints). Returns the raw response body text plus HTTP status
    /// code -- callers (`gateway.rs`) are responsible for parsing the
    /// body into the expected success shape or `BinanceApiError`, and
    /// for mapping both into `GatewayOutcome`/`Result<_, String>`
    /// per-call, since each endpoint's success shape differs.
    ///
    /// `GET` and `PUT` are retried per [`RetryConfig`]; `POST`/`DELETE`
    /// are sent at most once except one retry after an explicit HTTP 400
    /// Binance -1021 timestamp rejection (the request did not execute).
    pub async fn signed_request(
        &self,
        method: &str,
        family: ApiFamily,
        path: &str,
        extra_params: &str,
    ) -> Result<(reqwest::StatusCode, String), String> {
        let idempotent = matches!(method, "GET" | "PUT");
        self.request_with_policy(method, family, path, extra_params, idempotent)
            .await
    }

    /// Like [`Self::signed_request`] but retried regardless of method.
    /// Only for endpoints that are safe to repeat (the listenKey `POST`
    /// returns the existing key if one is active).
    pub async fn signed_request_idempotent(
        &self,
        method: &str,
        family: ApiFamily,
        path: &str,
        extra_params: &str,
    ) -> Result<(reqwest::StatusCode, String), String> {
        self.request_with_policy(method, family, path, extra_params, true)
            .await
    }

    fn gate_remaining(&self) -> Option<Duration> {
        let guard = self.blocked_until.lock().unwrap();
        guard
            .map(|until| until.saturating_duration_since(Instant::now()))
            .filter(|d| !d.is_zero())
    }

    fn set_gate(&self, wait: Duration) {
        let until = Instant::now() + wait;
        let mut guard = self.blocked_until.lock().unwrap();
        if guard.is_none_or(|current| until > current) {
            *guard = Some(until);
        }
    }

    fn backoff(&self, attempt: u32) -> Duration {
        self.retry
            .base_delay
            .saturating_mul(1u32 << attempt.min(16))
            .min(self.retry.max_delay)
    }

    fn rate_limited_response(wait: Duration) -> (reqwest::StatusCode, String) {
        (
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            format!(
                r#"{{"code":-1003,"msg":"client-side rate-limit gate: request not sent, retry after {}s"}}"#,
                wait.as_secs().max(1)
            ),
        )
    }

    fn clock(&self, family: ApiFamily) -> &tokio::sync::Mutex<ClockState> {
        match family {
            ApiFamily::Spot => &self.spot_clock,
            ApiFamily::Futures => &self.futures_clock,
        }
    }

    /// Refresh once per family across all clones. This lock is only for
    /// clock sampling, not the OMS or a general request lock.
    pub(crate) async fn signed_timestamp(&self, family: ApiFamily) -> Result<i64, &'static str> {
        if !(1..=60_000).contains(&self.config.recv_window_ms) {
            return Err("invalid_recv_window");
        }
        let mut clock = self.clock(family).lock().await;
        let now = Instant::now();
        if let Some(sample) = clock.sample
            && now.saturating_duration_since(sample.sampled_at) < CLOCK_REFRESH_INTERVAL
        {
            return Ok(sample.timestamp(now));
        }
        if self.gate_remaining().is_some() {
            return Err("rate_limited");
        }
        if clock.retry_after.is_some_and(|until| now < until) {
            return Err("time_refresh_cooldown");
        }
        match self.sample_clock(family).await {
            Ok(sample) => {
                clock.sample = Some(sample);
                clock.retry_after = None;
                Ok(sample.timestamp(Instant::now()))
            }
            Err(reason) => {
                clock.sample = None;
                clock.retry_after = Some(Instant::now() + CLOCK_FAILURE_COOLDOWN);
                Err(reason)
            }
        }
    }

    async fn sample_clock(&self, family: ApiFamily) -> Result<ClockSample, &'static str> {
        let path = match family {
            ApiFamily::Spot => "/api/v3/time",
            ApiFamily::Futures => "/fapi/v1/time",
        };
        let start = Instant::now();
        let response = self
            .http
            .get(format!("{}{path}", self.base_url(family)))
            .timeout(MAX_CLOCK_RTT)
            .send()
            .await
            .map_err(|_| "time_request_failed")?;
        let status = response.status();
        apexes_telemetry::metrics().count_rest_status(status.as_u16());
        if status.as_u16() == 429 || status.as_u16() == 418 {
            let wait = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(if status.as_u16() == 418 {
                    DEFAULT_BAN
                } else {
                    DEFAULT_RATE_LIMIT_WAIT
                });
            self.set_gate(wait);
            return Err("rate_limited");
        }
        if !status.is_success() {
            return Err("time_http_error");
        }
        let time: ServerTime = response.json().await.map_err(|_| "invalid_time_response")?;
        let end = Instant::now();
        let rtt = end.saturating_duration_since(start);
        let max_rtt = MAX_CLOCK_RTT.min(Duration::from_millis(self.config.recv_window_ms / 2));
        if rtt > max_rtt || time.server_time <= 0 {
            return Err("unreliable_time_sample");
        }
        let sample = ClockSample {
            server_ms: time.server_time,
            midpoint: start + rtt / 2,
            sampled_at: end,
        };
        let timestamp = sample.timestamp(end);
        tracing::info!(
            ?family,
            rtt_ms = rtt.as_millis() as u64,
            offset_ms = timestamp.saturating_sub(Utc::now().timestamp_millis()),
            "Binance server clock synchronized"
        );
        Ok(sample)
    }

    pub(crate) async fn invalidate_clock(&self, family: ApiFamily) {
        self.clock(family).lock().await.sample = None;
    }

    fn timestamp_rejected(status: reqwest::StatusCode, body: &str) -> bool {
        status == reqwest::StatusCode::BAD_REQUEST
            && serde_json::from_str::<BinanceApiError>(body).is_ok_and(|e| e.code == -1021)
    }

    fn clock_unavailable_response(reason: &'static str) -> (reqwest::StatusCode, String) {
        // No signed request was sent. Surface an explicit local rejection
        // rather than making the OMS reconcile a nonexistent order.
        (reqwest::StatusCode::BAD_REQUEST,
            serde_json::json!({"code": -1021, "msg": format!("client clock unavailable: {reason}; signed request not sent")}).to_string())
    }

    async fn request_with_policy(
        &self,
        method: &str,
        family: ApiFamily,
        path: &str,
        extra_params: &str,
        idempotent: bool,
    ) -> Result<(reqwest::StatusCode, String), String> {
        let mut attempt = 0u32;
        let mut timestamp_retry_used = false;
        loop {
            if let Some(wait) = self.gate_remaining() {
                if !idempotent || wait > self.retry.rate_limit_cap {
                    return Ok(Self::rate_limited_response(wait));
                }
                tokio::time::sleep(wait).await;
            }

            let timestamp = match self.signed_timestamp(family).await {
                Ok(timestamp) => timestamp,
                Err(reason) => {
                    tracing::warn!(
                        ?family,
                        reason,
                        "Binance signed request not sent: clock unavailable"
                    );
                    if let Some(wait) = self.gate_remaining() {
                        return Ok(Self::rate_limited_response(wait));
                    }
                    return Ok(Self::clock_unavailable_response(reason));
                }
            };
            // Another clone can have engaged the gate during sampling.
            if let Some(wait) = self.gate_remaining() {
                if !idempotent || wait > self.retry.rate_limit_cap {
                    return Ok(Self::rate_limited_response(wait));
                }
                tokio::time::sleep(wait).await;
                continue;
            }
            match self
                .send_once(method, family, path, extra_params, timestamp)
                .await
            {
                Ok((status, body, retry_after)) => {
                    if Self::timestamp_rejected(status, &body) {
                        self.invalidate_clock(family).await;
                        if !timestamp_retry_used {
                            timestamp_retry_used = true;
                            tracing::warn!(
                                ?family,
                                path,
                                "Binance rejected timestamp; refreshing clock and retrying once"
                            );
                            continue;
                        }
                    }
                    let limited =
                        status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.as_u16() == 418;
                    if limited {
                        let default = if status.as_u16() == 418 {
                            DEFAULT_BAN
                        } else {
                            DEFAULT_RATE_LIMIT_WAIT
                        };
                        let wait = retry_after.unwrap_or(default);
                        tracing::warn!(%status, wait_secs = wait.as_secs(), path, "Binance rate limit: gating requests");
                        self.set_gate(wait);
                        // A 418 ban is not retried in-line: the gate holds
                        // everything back for the ban duration instead.
                        if idempotent && status.as_u16() != 418 && attempt < self.retry.max_retries
                        {
                            attempt += 1;
                            continue;
                        }
                    } else if status.is_server_error()
                        && idempotent
                        && attempt < self.retry.max_retries
                    {
                        let delay = self.backoff(attempt);
                        tracing::warn!(%status, ?delay, path, "Binance server error; retrying idempotent request");
                        attempt += 1;
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Ok((status, body));
                }
                Err(error) => {
                    if idempotent && attempt < self.retry.max_retries {
                        let delay = self.backoff(attempt);
                        tracing::warn!(%error, ?delay, path, "Binance request failed; retrying idempotent request");
                        attempt += 1;
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }

    /// One HTTP attempt with a freshly signed timestamp.
    async fn send_once(
        &self,
        method: &str,
        family: ApiFamily,
        path: &str,
        extra_params: &str,
        timestamp: i64,
    ) -> Result<(reqwest::StatusCode, String, Option<Duration>), String> {
        let query = self.build_signed_query(extra_params, timestamp);
        let url = format!("{}{path}?{query}", self.base_url(family));

        let request = match method {
            "GET" => self.http.get(&url),
            "POST" => self.http.post(&url),
            "PUT" => self.http.put(&url),
            "DELETE" => self.http.delete(&url),
            other => return Err(format!("unsupported HTTP method: {other}")),
        }
        .header("X-MBX-APIKEY", &self.config.api_key);

        let response = request
            .send()
            .await
            .map_err(|error| format!("request failed: {}", error.without_url()))?;
        let status = response.status();
        apexes_telemetry::metrics().count_rest_status(status.as_u16());
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        let body = response
            .text()
            .await
            .map_err(|error| format!("failed to read response body: {}", error.without_url()))?;
        Ok((status, body, retry_after))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn client(server: &MockServer) -> BinanceRestClient {
        BinanceRestClient::new(BinanceGatewayConfig {
            api_key: "k".to_string(),
            api_secret: "s".to_string(),
            spot_base_url: server.uri(),
            futures_base_url: server.uri(),
            recv_window_ms: 5000,
        })
        .with_retry_config(RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(20),
            rate_limit_cap: Duration::from_secs(5),
        })
    }

    async fn received(server: &MockServer) -> usize {
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| !r.url.path().ends_with("/time"))
            .count()
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
    async fn get_is_retried_on_server_error_until_success() {
        let server = MockServer::start().await;
        mount_clock(&server).await;
        Mock::given(method("GET"))
            .and(path("/x"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/x"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&server)
            .await;

        let (status, body) = client(&server)
            .signed_request("GET", ApiFamily::Spot, "/x", "")
            .await
            .unwrap();
        assert!(status.is_success());
        assert_eq!(body, "ok");
        assert_eq!(received(&server).await, 3);
    }

    #[tokio::test]
    async fn get_retry_is_bounded() {
        let server = MockServer::start().await;
        mount_clock(&server).await;
        Mock::given(path("/x"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let (status, _) = client(&server)
            .signed_request("GET", ApiFamily::Spot, "/x", "")
            .await
            .unwrap();
        assert_eq!(status.as_u16(), 500);
        assert_eq!(received(&server).await, 4); // 1 + max_retries
    }

    #[tokio::test]
    async fn order_post_is_never_retried() {
        let server = MockServer::start().await;
        mount_clock(&server).await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let (status, _) = client(&server)
            .signed_request("POST", ApiFamily::Spot, "/order", "symbol=X")
            .await
            .unwrap();
        assert_eq!(status.as_u16(), 503);
        assert_eq!(received(&server).await, 1);
    }

    #[tokio::test]
    async fn rate_limit_with_retry_after_is_waited_out_for_gets() {
        let server = MockServer::start().await;
        mount_clock(&server).await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "1"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&server)
            .await;

        let started = std::time::Instant::now();
        let (status, _) = client(&server)
            .signed_request("GET", ApiFamily::Spot, "/x", "")
            .await
            .unwrap();
        assert!(status.is_success());
        assert!(started.elapsed() >= Duration::from_millis(900));
        assert_eq!(received(&server).await, 2);
    }

    #[tokio::test]
    async fn order_requests_fail_fast_while_gated_and_are_not_sent() {
        let server = MockServer::start().await;
        mount_clock(&server).await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(418).insert_header("Retry-After", "120"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let c = client(&server);

        let (status, _) = c
            .signed_request("GET", ApiFamily::Spot, "/x", "")
            .await
            .unwrap();
        assert_eq!(status.as_u16(), 418);
        assert_eq!(received(&server).await, 1);

        // Gate is shared across clones and blocks the order POST locally.
        let (status, body) = c
            .clone()
            .signed_request("POST", ApiFamily::Spot, "/order", "symbol=X")
            .await
            .unwrap();
        assert_eq!(status.as_u16(), 429);
        assert!(body.contains("-1003"));
        assert_eq!(received(&server).await, 1, "POST must not reach the server");
    }

    #[tokio::test]
    async fn transport_errors_are_retried_for_gets_only() {
        // Nothing listens on this port.
        let c = BinanceRestClient::new(BinanceGatewayConfig {
            api_key: "k".to_string(),
            api_secret: "s".to_string(),
            spot_base_url: "http://127.0.0.1:1".to_string(),
            futures_base_url: "http://127.0.0.1:1".to_string(),
            recv_window_ms: 5000,
        })
        .with_retry_config(RetryConfig {
            max_retries: 2,
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(10),
            rate_limit_cap: Duration::from_secs(1),
        });
        // A prior successful synchronization: exercise the signed
        // request's transport retry, not the preflight time endpoint.
        let now = Instant::now();
        c.spot_clock.lock().await.sample = Some(ClockSample {
            server_ms: Utc::now().timestamp_millis(),
            midpoint: now,
            sampled_at: now,
        });
        let started = std::time::Instant::now();
        assert!(
            c.signed_request("GET", ApiFamily::Spot, "/x", "")
                .await
                .is_err()
        );
        // 2 backoffs of >= 5ms and >= 10ms happened.
        assert!(started.elapsed() >= Duration::from_millis(15));
    }

    #[tokio::test]
    async fn explicit_timestamp_rejection_refreshes_and_retries_the_same_order_once() {
        let server = MockServer::start().await;
        let old_time = 1_800_000_000_000i64;
        let corrected_time = old_time + 10_000;
        Mock::given(method("GET"))
            .and(path("/api/v3/time"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"serverTime": old_time})),
            )
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v3/time"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"serverTime": corrected_time})),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/order"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_json(serde_json::json!({"code": -1021, "msg": "outside recvWindow"})),
            )
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/order"))
            .respond_with(ResponseTemplate::new(200).set_body_string("accepted"))
            .expect(1)
            .mount(&server)
            .await;
        let c = client(&server);
        let (status, body) = c
            .signed_request(
                "POST",
                ApiFamily::Spot,
                "/order",
                "symbol=ETHUSDT&newClientOrderId=same-id",
            )
            .await
            .unwrap();
        assert!(status.is_success());
        assert_eq!(body, "accepted");
        let requests = server.received_requests().await.unwrap();
        let orders: Vec<_> = requests
            .iter()
            .filter(|r| r.url.path() == "/order")
            .collect();
        assert_eq!(orders.len(), 2);
        for (request, expected) in orders.iter().zip([old_time, corrected_time]) {
            let query = request.url.query().unwrap();
            let params: std::collections::HashMap<_, _> = request.url.query_pairs().collect();
            let timestamp = params["timestamp"].parse::<i64>().unwrap();
            assert!((expected..expected + 1000).contains(&timestamp));
            assert_eq!(params["newClientOrderId"], "same-id");
            let (unsigned, sig) = query.rsplit_once("&signature=").unwrap();
            assert_eq!(sig, sign_query_string("s", unsigned));
        }
        for request in requests.iter().filter(|r| r.url.path().ends_with("/time")) {
            assert!(!request.headers.contains_key("x-mbx-apikey"));
            assert!(request.url.query().is_none());
        }
    }

    #[tokio::test]
    async fn timestamp_retries_are_bounded_and_ambiguous_responses_are_not_retried() {
        for (code, expected) in [(400, 2), (503, 1)] {
            let server = MockServer::start().await;
            mount_clock(&server).await;
            Mock::given(method("POST"))
                .and(path("/order"))
                .respond_with(
                    ResponseTemplate::new(code)
                        .set_body_json(serde_json::json!({"code": -1021, "msg": "timestamp"})),
                )
                .expect(expected)
                .mount(&server)
                .await;
            let (status, _) = client(&server)
                .signed_request(
                    "POST",
                    ApiFamily::Spot,
                    "/order",
                    "newClientOrderId=same-id",
                )
                .await
                .unwrap();
            assert_eq!(status.as_u16(), code);
            assert_eq!(received(&server).await, expected as usize);
        }
    }

    #[tokio::test]
    async fn time_samples_are_shared_across_clones_and_isolated_by_api_family() {
        let server = MockServer::start().await;
        for (time_path, stamp) in [
            ("/api/v3/time", 1_800_000_000_000i64),
            ("/fapi/v1/time", 1_800_000_010_000i64),
        ] {
            Mock::given(method("GET"))
                .and(path(time_path))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({"serverTime": stamp})),
                )
                .expect(1)
                .mount(&server)
                .await;
        }
        let c = client(&server);
        let clone = c.clone();
        let (a, b) = tokio::join!(
            c.signed_timestamp(ApiFamily::Spot),
            clone.signed_timestamp(ApiFamily::Spot)
        );
        let a = a.unwrap();
        let b = b.unwrap();
        assert!((1_800_000_000_000..1_800_000_001_000).contains(&a));
        assert!((a - 1000..a + 1000).contains(&b));
        let future = c.signed_timestamp(ApiFamily::Futures).await.unwrap();
        assert!((1_800_000_010_000..1_800_000_011_000).contains(&future));
    }

    #[tokio::test]
    async fn expired_clock_is_refreshed_before_signing_again() {
        let server = MockServer::start().await;
        let c = client(&server);
        let now = Instant::now();
        c.spot_clock.lock().await.sample = Some(ClockSample {
            server_ms: 1,
            midpoint: now - CLOCK_REFRESH_INTERVAL,
            sampled_at: now - CLOCK_REFRESH_INTERVAL,
        });
        Mock::given(method("GET"))
            .and(path("/api/v3/time"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"serverTime": 1_800_000_000_000i64})),
            )
            .expect(1)
            .mount(&server)
            .await;
        assert!(c.signed_timestamp(ApiFamily::Spot).await.unwrap() >= 1_800_000_000_000);
    }

    #[tokio::test]
    async fn missing_invalid_or_slow_time_preflight_never_sends_an_order() {
        for template in [
            ResponseTemplate::new(503),
            ResponseTemplate::new(200).set_body_string("not json"),
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"serverTime": -1})),
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"serverTime": 1_800_000_000_000i64}))
                .set_delay(Duration::from_millis(1200)),
        ] {
            let server = MockServer::start().await;
            Mock::given(path("/api/v3/time"))
                .respond_with(template)
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(200))
                .expect(0)
                .mount(&server)
                .await;
            let (status, body) = client(&server)
                .signed_request("POST", ApiFamily::Spot, "/order", "")
                .await
                .unwrap();
            assert_eq!(status.as_u16(), 400);
            assert!(body.contains("signed request not sent"));
        }
    }

    #[tokio::test]
    async fn time_endpoint_rate_limit_blocks_signed_requests_across_clones() {
        let server = MockServer::start().await;
        Mock::given(path("/api/v3/time"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "120"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let c = client(&server);
        for _ in 0..2 {
            let (status, _) = c
                .clone()
                .signed_request("POST", ApiFamily::Spot, "/order", "")
                .await
                .unwrap();
            assert_eq!(status.as_u16(), 429);
        }
    }
    #[tokio::test]
    async fn failed_time_preflight_is_throttled_across_clones() {
        let server = MockServer::start().await;
        Mock::given(path("/api/v3/time"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let c = client(&server);
        for _ in 0..3 {
            let (status, body) = c
                .clone()
                .signed_request("POST", ApiFamily::Spot, "/order", "")
                .await
                .unwrap();
            assert_eq!(status.as_u16(), 400);
            assert!(body.contains("signed request not sent"));
        }
    }
}
