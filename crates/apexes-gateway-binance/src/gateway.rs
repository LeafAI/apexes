//! [`BinanceExecutionGateway`]: implements `apexes_oms::ExecutionGateway`
//! against real Binance Spot and USDS-M Perpetual Futures REST APIs.

use apexes_core::SymbolRules;
use apexes_core::{
    AssetClass, FundMovement, FundMovementStatus, MarginMode, Order, OrderId, OrderStatus, WalletId,
};
use apexes_oms::{ExchangeFill, ExecutionGateway, GatewayOutcome};
use apexes_strategy::{OrderIntent, Position};

#[path = "spot_preflight.rs"]
mod spot_preflight;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::config::BinanceGatewayConfig;
use crate::error::BinanceApiError;
use crate::rest::{ApiFamily, BinanceRestClient};
use apexes_core::binance_network::BinanceNetwork;

pub struct BinanceExecutionGateway {
    rest: BinanceRestClient,
    /// When set, no Futures endpoint is ever contacted: balances come
    /// from Spot only, positions are empty, and the user-data stream
    /// covers Spot only. Required for the Spot Test Network (which has
    /// no Futures API) and sensible for any Spot-only session.
    spot_only: bool,
    /// WebSocket-API endpoint the Spot user-data stream subscribes on.
    spot_ws_api: String,
    /// Signs the stream's subscribe request. Built from the API key and
    /// secret by default; replaceable (see [`Self::with_signer`]).
    signer: std::sync::Arc<dyn crate::auth::ParamSigner>,
    recv_window_ms: u64,
    /// Per-symbol leverage/margin-mode last successfully set on the
    /// exchange -- avoids redundant `POST /fapi/v1/leverage`/
    /// `marginType` calls when consecutive orders for the same symbol
    /// use the same settings (Binance requires calling these BEFORE
    /// order submission, but not on every single order if unchanged).
    leverage_cache: Mutex<HashMap<String, (Decimal, MarginMode)>>,
}

impl BinanceExecutionGateway {
    pub fn new(config: BinanceGatewayConfig) -> Self {
        let signer: std::sync::Arc<dyn crate::auth::ParamSigner> = std::sync::Arc::new(
            crate::auth::HmacSigner::new(config.api_key.clone(), config.api_secret.clone()),
        );
        let recv_window_ms = config.recv_window_ms;
        Self {
            rest: BinanceRestClient::new(config),
            spot_only: false,
            spot_ws_api: BinanceNetwork::Mainnet.endpoints().spot_ws_api.to_string(),
            signer,
            recv_window_ms,
            leverage_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Replaces how the user-data subscription is signed, for example with
    /// an Ed25519 signer whose private key lives in an agent. REST requests
    /// keep using the HMAC secret in the config.
    pub fn with_signer(mut self, signer: std::sync::Arc<dyn crate::auth::ParamSigner>) -> Self {
        self.signer = signer;
        self
    }

    /// Points the user-data subscription at a specific WebSocket-API URL
    /// (tests use a local server).
    pub fn with_ws_api_url(mut self, url: impl Into<String>) -> Self {
        self.spot_ws_api = url.into();
        self
    }

    /// A Spot-only gateway for `network`: REST and user-data-stream
    /// hosts both come from [`BinanceNetwork::endpoints`], and no
    /// Futures endpoint is ever contacted (the Futures base URL is left
    /// empty so an accidental Futures call fails loudly).
    pub fn for_network(network: BinanceNetwork, api_key: String, api_secret: String) -> Self {
        Self::for_network_with_recv_window(network, api_key, api_secret, 5000)
    }

    /// Same Spot-only network routing with an explicit receive window.
    /// Signed requests refuse invalid windows (outside 1..=60,000 ms).
    pub fn for_network_with_recv_window(
        network: BinanceNetwork,
        api_key: String,
        api_secret: String,
        recv_window_ms: u64,
    ) -> Self {
        let endpoints = network.endpoints();
        let mut gateway = Self::new(BinanceGatewayConfig {
            api_key,
            api_secret,
            spot_base_url: endpoints.spot_rest.to_string(),
            futures_base_url: String::new(),
            recv_window_ms,
        })
        .spot_only();
        gateway.spot_ws_api = endpoints.spot_ws_api.to_string();
        gateway
    }

    /// Restricts this gateway to Spot. See the `spot_only` field.
    pub fn spot_only(mut self) -> Self {
        self.spot_only = true;
        self
    }

    /// Ensures `symbol` is set to `leverage`/`margin_mode` on the
    /// exchange BEFORE an order using them is submitted -- Binance
    /// requires `POST /fapi/v1/marginType` and `POST /fapi/v1/leverage`
    /// as separate, prior calls (not part of the order-submission
    /// request itself). Skips both calls entirely if the cache already
    /// reflects the requested values (no-op for repeated same-leverage
    /// orders). Returns `Err(reason)` if either call fails -- the
    /// caller (`submit_order`) treats this as `GatewayOutcome::Rejected`,
    /// never attempting the order submission itself.
    async fn ensure_leverage_and_margin_mode(
        &self,
        symbol: &str,
        leverage: Decimal,
        margin_mode: MarginMode,
    ) -> Result<(), String> {
        {
            let cache = self
                .leverage_cache
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if cache.get(symbol) == Some(&(leverage, margin_mode)) {
                return Ok(());
            }
        }

        let margin_type_str = match margin_mode {
            MarginMode::Isolated => "ISOLATED",
            MarginMode::Cross => "CROSSED",
        };
        let margin_type_params = format!("symbol={symbol}&marginType={margin_type_str}");
        let (status, body) = self
            .rest
            .signed_request(
                "POST",
                ApiFamily::Futures,
                "/fapi/v1/marginType",
                &margin_type_params,
            )
            .await?;
        // Binance returns an error code -4046 ("No need to change margin
        // type") when the requested mode is already active -- treat
        // this specific case as success, not a failure, since it means
        // the desired end state is already true.
        if !status.is_success() && !body.contains("-4046") {
            return Err(format!(
                "failed to set margin type for {symbol}: HTTP {status}: {body}"
            ));
        }

        let leverage_params = format!("symbol={symbol}&leverage={leverage}");
        let (status, body) = self
            .rest
            .signed_request(
                "POST",
                ApiFamily::Futures,
                "/fapi/v1/leverage",
                &leverage_params,
            )
            .await?;
        if !status.is_success() {
            return Err(format!(
                "failed to set leverage for {symbol}: HTTP {status}: {body}"
            ));
        }

        self.leverage_cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(symbol.to_string(), (leverage, margin_mode));
        Ok(())
    }

    fn order_path(family: ApiFamily) -> &'static str {
        match family {
            ApiFamily::Spot => "/api/v3/order",
            ApiFamily::Futures => "/fapi/v1/order",
        }
    }

    fn open_orders_path(family: ApiFamily) -> &'static str {
        match family {
            ApiFamily::Spot => "/api/v3/openOrders",
            ApiFamily::Futures => "/fapi/v1/openOrders",
        }
    }

    fn my_trades_path(family: ApiFamily) -> &'static str {
        match family {
            ApiFamily::Spot => "/api/v3/myTrades",
            ApiFamily::Futures => "/fapi/v1/userTrades",
        }
    }

    /// Maps a raw `(status, body)` REST response into `GatewayOutcome`,
    /// per this crate's design (`docs/superpowers/specs/2026-08-01-apexes-gateway-binance-design.md`
    /// §六): HTTP success + parseable order JSON -> `Accepted`; HTTP
    /// 4xx with an allowlisted explicit refusal -> `Rejected`.
    /// Unknown execution, duplicate client IDs, unrecognized errors,
    /// malformed responses, and HTTP 5xx remain `Uncertain`.
    fn map_order_response(
        status: reqwest::StatusCode,
        body: &str,
    ) -> Result<OrderResponseBody, GatewayOutcome> {
        if status.is_success() {
            match serde_json::from_str::<OrderResponseBody>(body) {
                Ok(parsed) => Ok(parsed),
                Err(error) => Err(GatewayOutcome::Uncertain {
                    reason: format!("could not parse success response body: {error}; body={body}"),
                }),
            }
        } else if status.is_client_error() {
            match serde_json::from_str::<BinanceApiError>(body) {
                Ok(api_error)
                    if matches!(
                        api_error.code,
                        -1003
                            | -1013
                            | -1015
                            | -1021
                            | -1022
                            | -2011
                            | -2014
                            | -2015
                            | -2019
                            | -2021
                            | -2022
                            | -1136..=-1100
                    ) || (api_error.code == -2010
                        && !api_error.msg.to_ascii_lowercase().contains("duplicate")) =>
                {
                    Err(GatewayOutcome::Rejected {
                        reason: format!("{} (code {})", api_error.msg, api_error.code),
                    })
                }
                Ok(api_error) => Err(GatewayOutcome::Uncertain {
                    reason: format!("unconfirmed exchange execution (code {})", api_error.code),
                }),
                Err(_) => Err(GatewayOutcome::Uncertain {
                    reason: format!("HTTP {status} with unparseable error body: {body}"),
                }),
            }
        } else {
            Err(GatewayOutcome::Uncertain {
                reason: format!("HTTP {status}: {body}"),
            })
        }
    }

    /// Raw open-order entries for `asset_class`'s API family.
    async fn fetch_open_order_entries(
        &self,
        asset_class: AssetClass,
    ) -> Result<Vec<OpenOrderEntry>, String> {
        let family = ApiFamily::from(asset_class);
        // A Spot-only gateway has no Futures endpoint, so it has no Futures
        // orders to report: that is an honest "none", not an error. (Startup
        // recovery asks about every asset class the engine supports.)
        if self.spot_only && family == ApiFamily::Futures {
            return Ok(Vec::new());
        }
        let (status, body) = self
            .rest
            .signed_request("GET", family, Self::open_orders_path(family), "")
            .await?;
        if !status.is_success() {
            return Err(format!("HTTP {status}: {body}"));
        }
        serde_json::from_str(&body).map_err(|error| format!("parse error: {error}; body={body}"))
    }

    /// Starts the Binance user-data stream: Spot over the WebSocket API,
    /// plus Futures (listenKey) unless the gateway is Spot-only. Returns a
    /// handle that must be kept alive for as long as the stream should keep
    /// running, plus the merged update stream. See
    /// `crate::ws_user_stream` and `crate::user_data_stream`.
    pub async fn start_user_data_stream(
        &self,
    ) -> Result<
        (
            crate::user_data_stream::UserDataStreamHandle,
            impl futures::Stream<Item = apexes_oms::ExchangeUpdate> + use<>,
        ),
        String,
    > {
        self.start_user_data_stream_with_health(None).await
    }

    pub async fn start_user_data_stream_with_health(
        &self,
        health: Option<std::sync::Arc<apexes_oms::KillSwitch>>,
    ) -> Result<
        (
            crate::user_data_stream::UserDataStreamHandle,
            impl futures::Stream<Item = apexes_oms::ExchangeUpdate> + use<>,
        ),
        String,
    > {
        crate::user_data_stream::start_user_data_stream_guarded(
            self.rest.clone(),
            crate::ws_user_stream::SpotStreamConfig {
                url: self.spot_ws_api.clone(),
                signer: std::sync::Arc::clone(&self.signer),
                recv_window_ms: self.recv_window_ms,
            },
            !self.spot_only,
            health,
        )
        .await
    }
}

impl BinanceExecutionGateway {
    /// Public direct-pair conversion from the last fully closed minute
    /// ending no later than the requested instant. No credentials or
    /// signed endpoint, no forward-looking partial candle.
    pub async fn fee_conversion(
        &self,
        asset: &str,
        quote: &str,
        at: DateTime<Utc>,
    ) -> Result<apexes_oms::FeeConversion, String> {
        if asset == quote
            || [asset, quote].iter().any(|s| {
                s.is_empty()
                    || s.len() > 24
                    || !s
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
            })
        {
            return Err("invalid fee conversion pair".into());
        }
        let symbol = format!("{asset}{quote}");
        let info = self
            .rest
            .public_get("/api/v3/exchangeInfo", &format!("symbol={symbol}"))
            .await?;
        let info: serde_json::Value =
            serde_json::from_str(&info).map_err(|_| "invalid conversion pair metadata")?;
        let matches = info["symbols"].as_array().is_some_and(|rows| {
            rows.iter().any(|row| {
                row["symbol"].as_str() == Some(symbol.as_str())
                    && row["baseAsset"].as_str() == Some(asset)
                    && row["quoteAsset"].as_str() == Some(quote)
                    && row["isSpotTradingAllowed"].as_bool() == Some(true)
            })
        });
        if !matches {
            return Err("direct spot fee conversion pair unavailable".into());
        }
        let end = at.timestamp_millis();
        let start = end.saturating_sub(300_000);
        let body = self
            .rest
            .public_get(
                "/api/v3/klines",
                &format!("symbol={symbol}&interval=1m&startTime={start}&endTime={end}&limit=6"),
            )
            .await?;
        let rows: Vec<Vec<serde_json::Value>> =
            serde_json::from_str(&body).map_err(|_| "invalid fee conversion candles")?;
        let mut best: Option<apexes_oms::FeeConversion> = None;
        for row in rows {
            let (Some(open), Some(close), Some(price)) = (
                row.first().and_then(|v| v.as_i64()),
                row.get(6).and_then(|v| v.as_i64()),
                row.get(4)
                    .and_then(|v| v.as_str())
                    .and_then(|v| v.parse::<Decimal>().ok()),
            ) else {
                return Err("invalid fee conversion candle".into());
            };
            if close.checked_sub(open) != Some(59_999)
                || open.rem_euclid(60_000) != 0
                || price <= Decimal::ZERO
            {
                return Err("invalid fee conversion candle range or price".into());
            }
            let Some(observed_at) = DateTime::from_timestamp_millis(close) else {
                continue;
            };
            if observed_at > at || at - observed_at > chrono::TimeDelta::minutes(5) {
                continue;
            }
            if best.as_ref().is_none_or(|b| observed_at > b.observed_at) {
                best = Some(apexes_oms::FeeConversion {
                    asset: asset.into(),
                    quote: quote.into(),
                    observed_at,
                    rate: price,
                });
            }
        }
        best.ok_or_else(|| "no causal closed conversion candle".into())
    }

    /// The latest traded price of each symbol, from the public ticker.
    ///
    /// Used once at session start to value every held asset before its first
    /// live trade arrives (a quiet market can take minutes). A symbol the
    /// exchange refuses, answers garbled, or prices at zero or below is left
    /// out and logged; the caller values it from its first trade instead.
    pub async fn last_prices(&self, symbols: &[String]) -> HashMap<String, Decimal> {
        let lookups = symbols
            .iter()
            .map(|symbol| async move { (symbol.clone(), self.last_price(symbol).await) });
        let mut prices = HashMap::new();
        for (symbol, outcome) in futures::future::join_all(lookups).await {
            match outcome {
                Ok(price) => {
                    prices.insert(symbol, price);
                }
                Err(error) => {
                    tracing::warn!(%symbol, %error, "no start price; valued from its first trade");
                }
            }
        }
        prices
    }

    /// Each symbol's trading rules (quantity step and bounds, price tick,
    /// minimum order value) from the public `exchangeInfo`.
    ///
    /// Read once at session start so the engine can make an order valid
    /// before sending it, instead of learning a symbol's step from a -1013
    /// rejection. A symbol whose rules cannot be had is left out and logged:
    /// its orders then go unconformed and the exchange decides, as before.
    pub async fn symbol_rules(&self, symbols: &[String]) -> HashMap<String, SymbolRules> {
        let lookups = symbols
            .iter()
            .map(|symbol| async move { (symbol.clone(), self.symbol_rule(symbol).await) });
        let mut rules = HashMap::new();
        for (symbol, outcome) in futures::future::join_all(lookups).await {
            match outcome {
                Ok(found) => {
                    rules.insert(symbol, found);
                }
                Err(error) => {
                    tracing::warn!(%symbol, %error, "no trading rules; the exchange will judge its orders");
                }
            }
        }
        rules
    }

    async fn symbol_rule(&self, symbol: &str) -> Result<SymbolRules, String> {
        let body = self
            .rest
            .public_get("/api/v3/exchangeInfo", &format!("symbol={symbol}"))
            .await?;
        parse_symbol_rules(&body, symbol)
    }

    async fn last_price(&self, symbol: &str) -> Result<Decimal, String> {
        #[derive(Deserialize)]
        struct Ticker {
            price: Decimal,
        }
        let body = self
            .rest
            .public_get("/api/v3/ticker/price", &format!("symbol={symbol}"))
            .await?;
        let ticker: Ticker =
            serde_json::from_str(&body).map_err(|error| format!("unreadable price: {error}"))?;
        if ticker.price <= Decimal::ZERO {
            return Err(format!("non-positive price {}", ticker.price));
        }
        Ok(ticker.price)
    }
}

/// An exchange Spot wallet balance, retaining the reservation distinction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SpotBalance {
    pub free: Decimal,
    pub locked: Decimal,
}

impl BinanceExecutionGateway {
    /// Local persistence binding only; the API key itself never leaves
    /// the gateway through this interface.
    pub fn credential_fingerprint(&self) -> String {
        self.rest.credential_fingerprint()
    }

    pub async fn spot_balances(&self) -> Result<HashMap<String, SpotBalance>, String> {
        let (status, body) = self
            .rest
            .signed_request("GET", ApiFamily::Spot, "/api/v3/account", "")
            .await?;
        if !status.is_success() {
            return Err(format!("spot balances: HTTP {status}"));
        }
        let spot: SpotAccountResponse =
            serde_json::from_str(&body).map_err(|_| "invalid spot balance response".to_string())?;
        let mut balances = HashMap::new();
        for entry in spot.balances {
            if entry.free < Decimal::ZERO
                || entry.locked < Decimal::ZERO
                || balances
                    .insert(
                        entry.asset,
                        SpotBalance {
                            free: entry.free,
                            locked: entry.locked,
                        },
                    )
                    .is_some()
            {
                return Err("invalid or duplicate spot wallet balance".into());
            }
        }
        Ok(balances)
    }

    /// Queries real Spot (`GET /api/v3/account`) and USDS-M Futures
    /// (`GET /fapi/v2/balance`) balances, merging both wallets into a
    /// single asset-symbol -> total-quantity map (matching
    /// `PortfolioLedger::balances`'s "one map, not split by wallet"
    /// convention). Spot's `free`+`locked` are summed (both are
    /// genuinely-held balance, just some of it reserved by open
    /// orders); the same asset present in both wallets is summed
    /// across them.
    pub async fn account_balances(&self) -> Result<HashMap<String, Decimal>, String> {
        let mut totals: HashMap<String, Decimal> = HashMap::new();

        for (asset, entry) in self.spot_balances().await? {
            totals.insert(asset, entry.free + entry.locked);
        }

        if self.spot_only {
            return Ok(totals);
        }

        let (status, body) = self
            .rest
            .signed_request("GET", ApiFamily::Futures, "/fapi/v2/balance", "")
            .await?;
        if !status.is_success() {
            return Err(format!("account_balances (futures): HTTP {status}: {body}"));
        }
        let futures: Vec<FuturesBalanceEntry> = serde_json::from_str(&body).map_err(|error| {
            format!("account_balances (futures): parse error: {error}; body={body}")
        })?;
        for entry in futures {
            *totals.entry(entry.asset).or_insert(Decimal::ZERO) += entry.balance;
        }

        Ok(totals)
    }

    /// Queries real USDS-M Futures positions (`GET
    /// /fapi/v2/positionRisk`), mapping each non-zero entry to
    /// `apexes_strategy::Position`. Zero-quantity entries (a symbol
    /// Binance always reports even with no open position) are omitted
    /// entirely -- matching `PortfolioLedger::positions()`'s convention
    /// of only containing symbols with an actual net position.
    pub async fn account_positions(&self) -> Result<HashMap<String, Position>, String> {
        if self.spot_only {
            return Ok(HashMap::new());
        }
        let (status, body) = self
            .rest
            .signed_request("GET", ApiFamily::Futures, "/fapi/v2/positionRisk", "")
            .await?;
        if !status.is_success() {
            return Err(format!("account_positions: HTTP {status}: {body}"));
        }
        let entries: Vec<PositionRiskEntry> = serde_json::from_str(&body)
            .map_err(|error| format!("account_positions: parse error: {error}; body={body}"))?;

        let mut positions = HashMap::new();
        for entry in entries {
            if entry.position_amt.is_zero() {
                continue;
            }
            let margin_mode = if entry.margin_type == "isolated" {
                MarginMode::Isolated
            } else {
                MarginMode::Cross
            };
            positions.insert(
                entry.symbol.clone(),
                Position {
                    symbol: entry.symbol,
                    quantity: entry.position_amt,
                    average_entry_price: entry.entry_price,
                    leverage: Some(entry.leverage),
                    margin_mode: Some(margin_mode),
                    allocated_margin: entry.isolated_margin,
                },
            );
        }
        Ok(positions)
    }

    fn wallet_transfer_type(from: WalletId, to: WalletId) -> Result<&'static str, String> {
        match (from, to) {
            (WalletId::Spot, WalletId::UsdsFutures) => Ok("MAIN_UMFUTURE"),
            (WalletId::UsdsFutures, WalletId::Spot) => Ok("UMFUTURE_MAIN"),
            (WalletId::Spot, WalletId::Spot) | (WalletId::UsdsFutures, WalletId::UsdsFutures) => {
                Err("internal_transfer: from_wallet and to_wallet must differ".to_string())
            }
        }
    }

    /// Real wallet-to-wallet transfer (`POST /sapi/v1/asset/transfer`)
    /// within the SAME Binance account -- e.g. Spot <-> USDS-M
    /// Futures. Never transfers funds out of the account (no
    /// withdrawal support -- see this crate's design constraints).
    pub async fn internal_transfer(
        &self,
        from_wallet: WalletId,
        to_wallet: WalletId,
        asset: &str,
        amount: Decimal,
    ) -> Result<(), String> {
        let transfer_type = Self::wallet_transfer_type(from_wallet, to_wallet)?;
        let params = format!("type={transfer_type}&asset={asset}&amount={amount}");
        let (status, body) = self
            .rest
            .signed_request("POST", ApiFamily::Spot, "/sapi/v1/asset/transfer", &params)
            .await?;
        if !status.is_success() {
            return Err(format!("internal_transfer: HTTP {status}: {body}"));
        }
        Ok(())
    }

    /// Maps Binance's deposit-status integer codes to
    /// `FundMovementStatus`. Per Binance's documented deposit status
    /// codes: `0` = pending, `1` = success (completed), `6` = credited
    /// but the underlying blockchain transaction is unconfirmed
    /// (treated as `Pending` here -- not yet safely spendable);
    /// anything else maps to `Failed` (conservative default -- an
    /// unrecognized code should never silently read as `Completed`).
    fn deposit_status(code: i32) -> FundMovementStatus {
        match code {
            1 => FundMovementStatus::Completed,
            0 | 6 => FundMovementStatus::Pending,
            _ => FundMovementStatus::Failed,
        }
    }

    /// Maps Binance's withdrawal-status integer codes to
    /// `FundMovementStatus`. Per Binance's documented withdrawal status
    /// codes: `6` = completed; `0`/`1`/`2`/`4`/`5` = various in-flight
    /// states (all `Pending`); `3` = rejected, `7` = cancelled -- both
    /// `Failed` (did not result in a successful withdrawal).
    fn withdrawal_status(code: i32) -> FundMovementStatus {
        match code {
            6 => FundMovementStatus::Completed,
            3 | 7 => FundMovementStatus::Failed,
            _ => FundMovementStatus::Pending,
        }
    }

    /// Read-only query of real deposit history (`GET
    /// /sapi/v1/capital/deposit/hisrec`). This crate never implements
    /// deposit/withdrawal INITIATION -- only history queries, per this
    /// plan's explicit scope constraint.
    pub async fn deposit_history(&self) -> Result<Vec<FundMovement>, String> {
        let (status, body) = self
            .rest
            .signed_request(
                "GET",
                ApiFamily::Spot,
                "/sapi/v1/capital/deposit/hisrec",
                "",
            )
            .await?;
        if !status.is_success() {
            return Err(format!("deposit_history: HTTP {status}: {body}"));
        }
        let entries: Vec<DepositHistoryEntry> = serde_json::from_str(&body)
            .map_err(|error| format!("deposit_history: parse error: {error}; body={body}"))?;
        Ok(entries
            .into_iter()
            .map(|entry| FundMovement {
                asset: entry.coin,
                amount: entry.amount,
                status: Self::deposit_status(entry.status),
                timestamp: Utc
                    .timestamp_millis_opt(entry.insert_time)
                    .single()
                    .unwrap_or_else(Utc::now),
                exchange_tx_id: entry.id,
            })
            .collect())
    }

    /// Read-only query of real withdrawal history (`GET
    /// /sapi/v1/capital/withdraw/history`). Binance reports
    /// `applyTime` as a `"YYYY-MM-DD HH:MM:SS"` string (UTC) for this
    /// endpoint (unlike deposit history's millisecond epoch) --
    /// parsed accordingly; an unparseable timestamp falls back to
    /// `Utc::now()` rather than failing the whole call (matching
    /// `deposit_history`'s same fallback for a symmetrical, deliberately
    /// forgiving parsing policy on this best-effort historical field).
    pub async fn withdrawal_history(&self) -> Result<Vec<FundMovement>, String> {
        let (status, body) = self
            .rest
            .signed_request(
                "GET",
                ApiFamily::Spot,
                "/sapi/v1/capital/withdraw/history",
                "",
            )
            .await?;
        if !status.is_success() {
            return Err(format!("withdrawal_history: HTTP {status}: {body}"));
        }
        let entries: Vec<WithdrawHistoryEntry> = serde_json::from_str(&body)
            .map_err(|error| format!("withdrawal_history: parse error: {error}; body={body}"))?;
        Ok(entries
            .into_iter()
            .map(|entry| {
                let timestamp =
                    chrono::NaiveDateTime::parse_from_str(&entry.apply_time, "%Y-%m-%d %H:%M:%S")
                        .map(|naive| naive.and_utc())
                        .unwrap_or_else(|_| Utc::now());
                FundMovement {
                    asset: entry.coin,
                    amount: entry.amount,
                    status: Self::withdrawal_status(entry.status),
                    timestamp,
                    exchange_tx_id: entry.id,
                }
            })
            .collect())
    }
}

/// Binance's order-response JSON shape (both submit and cancel
/// responses share this shape for the fields this gateway needs).
#[derive(Debug, Deserialize)]
struct OrderResponseBody {
    #[serde(rename = "orderId")]
    order_id: i64,
}

/// Binance's open-order-list entry shape (`GET /api/v3/openOrders` /
/// `GET /fapi/v1/openOrders`), only the fields this gateway needs.
#[derive(Debug, Deserialize)]
struct OpenOrderEntry {
    #[serde(rename = "orderId")]
    order_id: i64,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    status: String,
}

/// Binance's trade-list entry shape (`GET /api/v3/myTrades` /
/// `GET /fapi/v1/userTrades`), only the fields this gateway needs.
#[derive(Debug, Deserialize)]
struct TradeEntry {
    id: i64,
    price: String,
    qty: String,
    commission: String,
    #[serde(rename = "commissionAsset")]
    commission_asset: Option<String>,
    time: i64,
}

/// Binance's Spot account response shape (`GET /api/v3/account`), only
/// the fields this gateway needs.
#[derive(Debug, Deserialize)]
struct SpotAccountResponse {
    balances: Vec<SpotBalanceEntry>,
}

/// One entry of `SpotAccountResponse::balances`.
#[derive(Debug, Deserialize)]
struct SpotBalanceEntry {
    asset: String,
    free: Decimal,
    locked: Decimal,
}

/// One entry of the USDS-M Futures balance list (`GET /fapi/v2/balance`).
#[derive(Debug, Deserialize)]
struct FuturesBalanceEntry {
    asset: String,
    balance: Decimal,
}

/// One entry of the USDS-M Futures position-risk list (`GET
/// /fapi/v2/positionRisk`), only the fields this gateway needs.
#[derive(Debug, Deserialize)]
struct PositionRiskEntry {
    symbol: String,
    #[serde(rename = "positionAmt")]
    position_amt: Decimal,
    #[serde(rename = "entryPrice")]
    entry_price: Decimal,
    leverage: Decimal,
    #[serde(rename = "marginType")]
    margin_type: String,
    #[serde(rename = "isolatedMargin")]
    isolated_margin: Decimal,
}

/// One entry of the deposit-history list (`GET
/// /sapi/v1/capital/deposit/hisrec`), only the fields this gateway needs.
#[derive(Debug, Deserialize)]
struct DepositHistoryEntry {
    id: String,
    amount: Decimal,
    coin: String,
    status: i32,
    #[serde(rename = "insertTime")]
    insert_time: i64,
}

/// One entry of the withdrawal-history list (`GET
/// /sapi/v1/capital/withdraw/history`), only the fields this gateway
/// needs. Binance reports `applyTime` as a `"YYYY-MM-DD HH:MM:SS"`
/// string (UTC) for this endpoint, unlike deposit history's millisecond
/// epoch -- hence `String`, parsed separately in `withdrawal_history`.
#[derive(Debug, Deserialize)]
struct WithdrawHistoryEntry {
    id: String,
    amount: Decimal,
    coin: String,
    status: i32,
    #[serde(rename = "applyTime")]
    apply_time: String,
}

/// Binance's definitive "no such order" answer (`-2013`), returned with
/// HTTP 400.
/// Reads one symbol's `SymbolRules` out of an `exchangeInfo` response. A filter
/// the symbol does not list leaves that rule at zero (not enforced).
fn parse_symbol_rules(body: &str, symbol: &str) -> Result<SymbolRules, String> {
    #[derive(Deserialize)]
    struct Info {
        symbols: Vec<Entry>,
    }
    #[derive(Deserialize)]
    struct Entry {
        symbol: String,
        filters: Vec<serde_json::Value>,
    }
    let info: Info =
        serde_json::from_str(body).map_err(|error| format!("unreadable exchangeInfo: {error}"))?;
    let entry = info
        .symbols
        .into_iter()
        .find(|entry| entry.symbol == symbol)
        .ok_or_else(|| format!("{symbol} is not in the exchange's symbol list"))?;
    let field = |filter: &str, name: &str| -> Option<Decimal> {
        entry
            .filters
            .iter()
            .find(|f| f["filterType"] == filter)
            .and_then(|f| f[name].as_str())
            .and_then(|text| text.parse::<Decimal>().ok())
    };
    let zero = Decimal::ZERO;
    Ok(SymbolRules {
        quantity_step: field("LOT_SIZE", "stepSize").unwrap_or(zero),
        min_quantity: field("LOT_SIZE", "minQty").unwrap_or(zero),
        max_quantity: field("LOT_SIZE", "maxQty").unwrap_or(zero),
        price_tick: field("PRICE_FILTER", "tickSize").unwrap_or(zero),
        min_notional: field("NOTIONAL", "minNotional")
            .or_else(|| field("MIN_NOTIONAL", "minNotional"))
            .unwrap_or(zero),
    })
}

/// Most decimal places Binance accepts in a quantity or price field.
const MAX_WIRE_DECIMALS: u32 = 8;

/// `value` as Binance wants it on the wire: plain decimal notation, no
/// exponent, trailing zeros trimmed, at most [`MAX_WIRE_DECIMALS`] places.
///
/// Rounds to that precision, and refuses a value that is zero (or negative)
/// once rounded, because an order for "nothing" is a strategy bug the
/// exchange would reject anyway, with a worse message. The exchange's own
/// LOT_SIZE and PRICE_FILTER still decide whether the rounded value is valid
/// for the symbol.
fn wire_decimal(value: Decimal) -> Result<String, String> {
    let rounded = value.round_dp(MAX_WIRE_DECIMALS).normalize();
    if rounded <= Decimal::ZERO {
        return Err(format!(
            "rounds to {rounded} at {MAX_WIRE_DECIMALS} decimal places; the order is too small to send"
        ));
    }
    Ok(rounded.to_string())
}

fn is_order_does_not_exist(body: &str) -> bool {
    serde_json::from_str::<BinanceApiError>(body).is_ok_and(|e| e.code == -2013)
}

/// Validate the response identity before accepting a client-ID lookup.
fn parse_client_identity(body: &str, order: &Order) -> Result<(String, OrderStatus), String> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        order_id: i64,
        client_order_id: String,
        symbol: String,
        status: String,
    }
    let parsed: Response = serde_json::from_str(body)
        .map_err(|_| "invalid client-order lookup response".to_owned())?;
    if parsed.order_id <= 0
        || parsed.symbol != order.symbol
        || parsed.client_order_id != order.id.0.to_string()
        || order
            .exchange_order_id
            .as_ref()
            .is_some_and(|id| *id != parsed.order_id.to_string())
    {
        return Err("client-order lookup response identity mismatch".into());
    }
    Ok((
        parsed.order_id.to_string(),
        parse_binance_status(&parsed.status)?,
    ))
}

fn parse_binance_status(status: &str) -> Result<OrderStatus, String> {
    match status {
        "NEW" => Ok(OrderStatus::New),
        "PARTIALLY_FILLED" => Ok(OrderStatus::PartiallyFilled),
        "FILLED" => Ok(OrderStatus::Filled),
        "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" => Ok(OrderStatus::Cancelled),
        "REJECTED" => Ok(OrderStatus::Rejected),
        other => Err(format!("unknown Binance order status: {other}")),
    }
}

/// Parses one `query_all_open_orders` response entry into the
/// `(client_order_id, exchange_order_id, status)` triple the trait
/// contract requires. Factored out of `query_all_open_orders` so a
/// single unparseable entry can be skipped-and-logged by the caller
/// rather than failing the entire batch (see that method's doc
/// comment for why: `reconcile()`'s safety-net purpose depends on
/// one bad entry never hiding every other order in the same response).
fn parse_open_order_entry(
    entry: &OpenOrderEntry,
) -> Result<(OrderId, Option<String>, OrderStatus), String> {
    let client_order_id = entry.client_order_id.parse::<uuid::Uuid>().map_err(|error| {
        format!(
            "clientOrderId {:?} is not a valid UUID (was it set by something other than this gateway?): {error}",
            entry.client_order_id
        )
    })?;
    let status = parse_binance_status(&entry.status)?;
    Ok((
        OrderId(client_order_id),
        Some(entry.order_id.to_string()),
        status,
    ))
}

fn parse_decimal_field(value: &str, field: &str) -> Result<Decimal, String> {
    value
        .parse::<Decimal>()
        .map_err(|error| format!("could not parse {field} {value:?} as Decimal: {error}"))
}

fn millis_to_datetime(millis: i64) -> Result<DateTime<Utc>, String> {
    Utc.timestamp_millis_opt(millis)
        .single()
        .ok_or_else(|| format!("could not convert {millis} to a valid timestamp"))
}

impl BinanceExecutionGateway {
    async fn submit_order_wire(&self, order: &Order, immediate_or_cancel: bool) -> GatewayOutcome {
        if let (Some(leverage), Some(margin_mode)) = (order.leverage, order.margin_mode)
            && let Err(reason) = self
                .ensure_leverage_and_margin_mode(&order.symbol, leverage, margin_mode)
                .await
        {
            return GatewayOutcome::Rejected {
                reason: format!("leverage/margin mode setup failed: {reason}"),
            };
        }

        let family = ApiFamily::from(order.asset_class);
        let side = match order.side {
            apexes_core::Side::Buy => "BUY",
            apexes_core::Side::Sell => "SELL",
        };
        // Spot and Futures use different order-type strings for stop
        // orders: Spot's `STOP_LOSS`/`STOP_LOSS_LIMIT` don't exist on
        // Futures, which instead uses `STOP_MARKET`/`STOP`. Sending the
        // Spot names to Futures (or vice versa) would be rejected by
        // Binance. Unlike Spot's `STOP_LOSS_LIMIT`, Futures' `STOP`
        // does not require `timeInForce` (Binance defaults to GTC).
        let order_type = match (family, order.order_type) {
            (_, apexes_core::OrderType::Market) => "MARKET",
            (_, apexes_core::OrderType::Limit) => "LIMIT",
            (ApiFamily::Spot, apexes_core::OrderType::Stop) => "STOP_LOSS",
            (ApiFamily::Futures, apexes_core::OrderType::Stop) => "STOP_MARKET",
            (ApiFamily::Spot, apexes_core::OrderType::StopLimit) => "STOP_LOSS_LIMIT",
            (ApiFamily::Futures, apexes_core::OrderType::StopLimit) => "STOP",
        };

        // Binance accepts at most 20 decimal places and refuses anything else
        // with a -1100 "Illegal characters" error. A quantity or price that
        // went through floating point can carry dozens of digits of residue,
        // so send them as plain, bounded decimals.
        let quantity = match wire_decimal(order.quantity) {
            Ok(quantity) => quantity,
            Err(reason) => {
                return GatewayOutcome::Rejected {
                    reason: format!("invalid order quantity {}: {reason}", order.quantity),
                };
            }
        };
        let mut params = format!(
            "symbol={}&side={side}&type={order_type}&quantity={quantity}&newClientOrderId={}",
            order.symbol, order.id.0
        );
        if let Some(price) = order.price {
            let price = match wire_decimal(price) {
                Ok(price) => price,
                Err(reason) => {
                    return GatewayOutcome::Rejected {
                        reason: format!("invalid order price {price}: {reason}"),
                    };
                }
            };
            params.push_str(&format!("&price={price}"));
            let needs_time_in_force = matches!(
                (family, order.order_type),
                (_, apexes_core::OrderType::Limit)
                    | (ApiFamily::Spot, apexes_core::OrderType::StopLimit)
            );
            if needs_time_in_force {
                params.push_str(if immediate_or_cancel {
                    "&timeInForce=IOC"
                } else {
                    "&timeInForce=GTC"
                });
            }
        }
        if let Some(stop_price) = order.stop_price {
            params.push_str(&format!("&stopPrice={stop_price}"));
        }

        let response = self
            .rest
            .signed_request("POST", family, Self::order_path(family), &params)
            .await;

        match response {
            Ok((status, body)) => match Self::map_order_response(status, &body) {
                Ok(parsed) => GatewayOutcome::Accepted {
                    exchange_order_id: parsed.order_id.to_string(),
                },
                Err(outcome) => outcome,
            },
            Err(reason) => GatewayOutcome::Uncertain { reason },
        }
    }
}

#[async_trait]
impl ExecutionGateway for BinanceExecutionGateway {
    async fn spot_preflight(
        &self,
        intent: &OrderIntent,
    ) -> Result<apexes_oms::SpotPreflight, String> {
        self.check_spot_intent(intent).await
    }
    async fn spot_sell_inventory(&self, symbol: &str) -> Result<(String, Decimal), String> {
        if symbol.is_empty()
            || !symbol
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        {
            return Err("invalid spot symbol".into());
        }
        let body = self
            .rest
            .public_get("/api/v3/exchangeInfo", &format!("symbol={symbol}"))
            .await?;
        let info: serde_json::Value =
            serde_json::from_str(&body).map_err(|_| "invalid spot metadata")?;
        let row = info["symbols"]
            .as_array()
            .and_then(|rows| {
                rows.iter().find(|r| {
                    r["symbol"].as_str() == Some(symbol)
                        && r["isSpotTradingAllowed"].as_bool() == Some(true)
                })
            })
            .ok_or("spot instrument unavailable")?;
        let base = row["baseAsset"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or("missing base asset")?
            .to_owned();
        let wallet = self.spot_balances().await?;
        Ok((
            base.clone(),
            wallet.get(&base).map_or(Decimal::ZERO, |b| b.free),
        ))
    }
    async fn submit_order(&self, order: &Order) -> GatewayOutcome {
        self.submit_order_wire(order, false).await
    }

    async fn submit_bounded_spot_buy(&self, order: &Order, cap: Decimal) -> GatewayOutcome {
        if order.asset_class != apexes_core::AssetClass::Spot
            || order.venue != apexes_core::Venue::Binance
            || order.side != apexes_core::Side::Buy
            || order.order_type != apexes_core::OrderType::Market
            || order.price.is_some()
            || order.stop_price.is_some()
            || order.leverage.is_some()
            || order.margin_mode.is_some()
            || cap <= Decimal::ZERO
            || order.quantity <= Decimal::ZERO
            || wire_decimal(cap)
                .ok()
                .and_then(|s| s.parse::<Decimal>().ok())
                != Some(cap)
            || wire_decimal(order.quantity)
                .ok()
                .and_then(|s| s.parse::<Decimal>().ok())
                != Some(order.quantity)
        {
            return GatewayOutcome::Rejected {
                reason: "invalid bounded Spot buy".into(),
            };
        }
        let mut wire_order = order.clone();
        wire_order.order_type = apexes_core::OrderType::Limit;
        wire_order.price = Some(cap);
        self.submit_order_wire(&wire_order, true).await
    }

    async fn cancel_order(&self, order: &Order) -> GatewayOutcome {
        let family = ApiFamily::from(order.asset_class);
        let Some(exchange_order_id) = &order.exchange_order_id else {
            return GatewayOutcome::Rejected {
                reason: "cannot cancel an order with no known exchange_order_id".to_string(),
            };
        };
        let params = format!("symbol={}&orderId={exchange_order_id}", order.symbol);

        let response = self
            .rest
            .signed_request("DELETE", family, Self::order_path(family), &params)
            .await;

        match response {
            Ok((status, body)) => match Self::map_order_response(status, &body) {
                Ok(parsed) => GatewayOutcome::Accepted {
                    exchange_order_id: parsed.order_id.to_string(),
                },
                Err(outcome) => outcome,
            },
            Err(reason) => GatewayOutcome::Uncertain { reason },
        }
    }

    async fn query_order_status(&self, order: &Order) -> Result<Option<OrderStatus>, String> {
        let family = ApiFamily::from(order.asset_class);
        let Some(exchange_order_id) = &order.exchange_order_id else {
            return Ok(None);
        };
        let params = format!("symbol={}&orderId={exchange_order_id}", order.symbol);

        let (status, body) = self
            .rest
            .signed_request("GET", family, Self::order_path(family), &params)
            .await?;

        if status == reqwest::StatusCode::NOT_FOUND || is_order_does_not_exist(&body) {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(format!("HTTP {status}: {body}"));
        }

        #[derive(Deserialize)]
        struct SingleOrderStatus {
            status: String,
        }
        let parsed: SingleOrderStatus = serde_json::from_str(&body)
            .map_err(|error| format!("parse error: {error}; body={body}"))?;
        Ok(Some(parse_binance_status(&parsed.status)?))
    }

    async fn query_order_execution(
        &self,
        order: &Order,
    ) -> Result<apexes_oms::OrderExecutionSnapshot, String> {
        let family = ApiFamily::from(order.asset_class);
        let exchange_id = order
            .exchange_order_id
            .as_deref()
            .ok_or("missing exchange order ID")?;
        let params = format!("symbol={}&orderId={exchange_id}", order.symbol);
        let (status, body) = self
            .rest
            .signed_request("GET", family, Self::order_path(family), &params)
            .await?;
        if !status.is_success() {
            return Err(format!("order execution snapshot: HTTP {status}"));
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Execution {
            order_id: i64,
            symbol: String,
            status: String,
            executed_qty: Decimal,
        }
        let parsed: Execution =
            serde_json::from_str(&body).map_err(|_| "invalid execution snapshot".to_string())?;
        if parsed.order_id.to_string() != exchange_id
            || parsed.symbol != order.symbol
            || parsed.executed_qty < Decimal::ZERO
        {
            return Err("order execution snapshot identity or quantity mismatch".into());
        }
        Ok(apexes_oms::OrderExecutionSnapshot {
            status: parse_binance_status(&parsed.status)?,
            executed_quantity: parsed.executed_qty,
        })
    }

    /// Looks the order up by `origClientOrderId` (our `Order.id`, which
    /// `submit_order` sent as `newClientOrderId`). Binance answers
    /// `-2013 Order does not exist` for an order it never received.
    async fn query_order_by_client_id(
        &self,
        order: &Order,
    ) -> Result<Option<(String, OrderStatus)>, String> {
        let family = ApiFamily::from(order.asset_class);
        let params = format!("symbol={}&origClientOrderId={}", order.symbol, order.id.0);
        let (status, body) = self
            .rest
            .signed_request("GET", family, Self::order_path(family), &params)
            .await?;
        if status == reqwest::StatusCode::BAD_REQUEST && is_order_does_not_exist(&body) {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(format!("HTTP {status}: {body}"));
        }

        parse_client_identity(&body, order).map(Some)
    }

    async fn query_all_open_orders(
        &self,
        asset_class: AssetClass,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
        let entries = self.fetch_open_order_entries(asset_class).await?;

        // A single entry this gateway can't parse (a non-UUID
        // clientOrderId from an order placed by something other than
        // this gateway -- e.g. manually via Binance's web/app UI on the
        // same account -- or a Binance order-status string outside our
        // 5-variant mapping, e.g. PENDING_CANCEL) must not poison every
        // OTHER entry in this response: reconcile() is the safety-net
        // poll specifically meant to catch what a WS disconnect missed,
        // and losing visibility into every other open order in this
        // asset class over one unrelated/unmapped entry would defeat
        // that purpose exactly when it matters most. Skip and log the
        // offending entry instead of failing the whole batch, matching
        // `user_data_stream::parse_binance_status`'s existing
        // skip-and-log behavior for the same class of problem.
        Ok(entries
            .into_iter()
            .filter_map(|entry| match parse_open_order_entry(&entry) {
                Ok(parsed) => Some(parsed),
                Err(reason) => {
                    tracing::warn!(
                        reason,
                        client_order_id = entry.client_order_id,
                        "skipping unparseable open-order entry"
                    );
                    None
                }
            })
            .collect())
    }

    async fn count_foreign_open_orders(&self, asset_class: AssetClass) -> Result<usize, String> {
        // "Foreign" = a clientOrderId that is not one of ours (a UUID). An
        // entry with a UUID but an unmapped status is still our own order.
        Ok(self
            .fetch_open_order_entries(asset_class)
            .await?
            .iter()
            .filter(|entry| entry.client_order_id.parse::<uuid::Uuid>().is_err())
            .count())
    }

    async fn query_fills_for_order(&self, order: &Order) -> Result<Vec<ExchangeFill>, String> {
        let family = ApiFamily::from(order.asset_class);
        let Some(exchange_order_id) = &order.exchange_order_id else {
            return Ok(Vec::new());
        };
        let params = format!("symbol={}&orderId={exchange_order_id}", order.symbol);
        let (status, body) = self
            .rest
            .signed_request("GET", family, Self::my_trades_path(family), &params)
            .await?;
        if !status.is_success() {
            return Err(format!("HTTP {status}: {body}"));
        }
        let entries: Vec<TradeEntry> = serde_json::from_str(&body)
            .map_err(|error| format!("parse error: {error}; body={body}"))?;

        entries
            .into_iter()
            .map(|entry| {
                Ok(ExchangeFill {
                    exchange_trade_id: entry.id.to_string(),
                    price: parse_decimal_field(&entry.price, "price")?,
                    quantity: parse_decimal_field(&entry.qty, "qty")?,
                    fee: parse_decimal_field(&entry.commission, "commission")?,
                    fee_currency: entry.commission_asset,
                    filled_at: millis_to_datetime(entry.time)?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod wire_decimal_tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn wire_decimals_are_short_plain_and_never_zero() {
        assert_eq!(wire_decimal(dec!(0.0066)).unwrap(), "0.0066");
        assert_eq!(wire_decimal(dec!(1.50000)).unwrap(), "1.5");
        assert_eq!(wire_decimal(dec!(65447.01)).unwrap(), "65447.01");
        assert_eq!(wire_decimal(dec!(100)).unwrap(), "100");
        // The exact residue that the real testnet rejected with -1100.
        let residue: Decimal = "0.000000000000000001734723476".parse().unwrap();
        assert!(
            wire_decimal(residue).is_err(),
            "rounds to nothing: refuse, do not send"
        );
        // Long fractions are cut to what the exchange takes, in plain notation.
        let long: Decimal = "0.0109000000000000000000000001".parse().unwrap();
        let text = wire_decimal(long).unwrap();
        assert_eq!(text, "0.0109");
        assert!(!text.contains('e') && !text.contains('E'));
        assert!(wire_decimal(dec!(0)).is_err());
        assert!(wire_decimal(dec!(-1)).is_err());
    }
}
