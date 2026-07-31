//! Shared REST plumbing: signed-request construction, routing between
//! Binance's Spot and USDS-M Futures API families based on
//! `apexes_core::AssetClass`.

use apexes_core::AssetClass;
use chrono::Utc;

use crate::config::BinanceGatewayConfig;
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

/// Thin wrapper around a `reqwest::Client` plus the config needed to
/// build and sign Binance requests. `BinanceExecutionGateway` (Task 16)
/// holds one of these; this type has no `ExecutionGateway`-specific
/// logic itself, only "build the URL, sign the query string, send the
/// request" mechanics shared by every endpoint.
#[derive(Clone)]
pub struct BinanceRestClient {
    http: reqwest::Client,
    config: BinanceGatewayConfig,
}

impl BinanceRestClient {
    pub fn new(config: BinanceGatewayConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            config,
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
    fn build_signed_query(&self, extra_params: &str) -> String {
        let timestamp = Utc::now().timestamp_millis();
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
    /// `build_signed_query`). `method` is `"GET"`, `"POST"`, or
    /// `"DELETE"` per Binance's documented HTTP method per endpoint.
    /// Returns the raw response body text plus HTTP status code --
    /// callers (`gateway.rs`) are responsible for parsing the body into
    /// the expected success shape or `BinanceApiError`, and for mapping
    /// both into `GatewayOutcome`/`Result<_, String>` per-call, since
    /// each endpoint's success shape differs.
    pub async fn signed_request(
        &self,
        method: &str,
        family: ApiFamily,
        path: &str,
        extra_params: &str,
    ) -> Result<(reqwest::StatusCode, String), String> {
        let query = self.build_signed_query(extra_params);
        let url = format!("{}{path}?{query}", self.base_url(family));

        let request = match method {
            "GET" => self.http.get(&url),
            "POST" => self.http.post(&url),
            "DELETE" => self.http.delete(&url),
            other => return Err(format!("unsupported HTTP method: {other}")),
        }
        .header("X-MBX-APIKEY", &self.config.api_key);

        let response = request
            .send()
            .await
            .map_err(|error| format!("request failed: {error}"))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| format!("failed to read response body: {error}"))?;
        Ok((status, body))
    }
}
