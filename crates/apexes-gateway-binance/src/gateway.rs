//! [`BinanceExecutionGateway`]: implements `apexes_oms::ExecutionGateway`
//! against real Binance Spot and USDS-M Perpetual Futures REST APIs.

use apexes_core::{AssetClass, MarginMode, Order, OrderId, OrderStatus};
use apexes_oms::{ExchangeFill, ExecutionGateway, GatewayOutcome};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::config::BinanceGatewayConfig;
use crate::error::BinanceApiError;
use crate::rest::{ApiFamily, BinanceRestClient};

pub struct BinanceExecutionGateway {
    rest: BinanceRestClient,
    /// Per-symbol leverage/margin-mode last successfully set on the
    /// exchange -- avoids redundant `POST /fapi/v1/leverage`/
    /// `marginType` calls when consecutive orders for the same symbol
    /// use the same settings (Binance requires calling these BEFORE
    /// order submission, but not on every single order if unchanged).
    leverage_cache: Mutex<HashMap<String, (Decimal, MarginMode)>>,
}

impl BinanceExecutionGateway {
    pub fn new(config: BinanceGatewayConfig) -> Self {
        Self {
            rest: BinanceRestClient::new(config),
            leverage_cache: Mutex::new(HashMap::new()),
        }
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
    /// 4xx + a parseable `{"code": <negative>, "msg": ...}` body ->
    /// `Rejected` (an explicit business-logic refusal); anything else
    /// (network failure already surfaced as `Err` by `signed_request`,
    /// HTTP 5xx, or an unparseable body) -> `Uncertain`.
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
                Ok(api_error) => Err(GatewayOutcome::Rejected {
                    reason: format!("{} (code {})", api_error.msg, api_error.code),
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

    /// Starts the Binance User Data Stream (both Spot and Futures),
    /// returning a handle that must be kept alive for as long as the
    /// stream should keep running, plus the merged update stream
    /// itself. See `crate::user_data_stream` for details.
    pub async fn start_user_data_stream(
        &self,
    ) -> Result<
        (
            crate::user_data_stream::UserDataStreamHandle,
            impl futures::Stream<Item = apexes_oms::ExchangeUpdate>,
        ),
        String,
    > {
        crate::user_data_stream::start_user_data_stream(self.rest.clone()).await
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

fn parse_binance_status(status: &str) -> Result<OrderStatus, String> {
    match status {
        "NEW" => Ok(OrderStatus::New),
        "PARTIALLY_FILLED" => Ok(OrderStatus::PartiallyFilled),
        "FILLED" => Ok(OrderStatus::Filled),
        "CANCELED" | "EXPIRED" => Ok(OrderStatus::Cancelled),
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

#[async_trait]
impl ExecutionGateway for BinanceExecutionGateway {
    async fn submit_order(&self, order: &Order) -> GatewayOutcome {
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

        let mut params = format!(
            "symbol={}&side={side}&type={order_type}&quantity={}&newClientOrderId={}",
            order.symbol, order.quantity, order.id.0
        );
        if let Some(price) = order.price {
            params.push_str(&format!("&price={price}"));
            let needs_time_in_force = matches!(
                (family, order.order_type),
                (_, apexes_core::OrderType::Limit)
                    | (ApiFamily::Spot, apexes_core::OrderType::StopLimit)
            );
            if needs_time_in_force {
                params.push_str("&timeInForce=GTC");
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

        if status == reqwest::StatusCode::NOT_FOUND {
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

    async fn query_all_open_orders(
        &self,
        asset_class: AssetClass,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
        let family = ApiFamily::from(asset_class);
        let (status, body) = self
            .rest
            .signed_request("GET", family, Self::open_orders_path(family), "")
            .await?;
        if !status.is_success() {
            return Err(format!("HTTP {status}: {body}"));
        }
        let entries: Vec<OpenOrderEntry> = serde_json::from_str(&body)
            .map_err(|error| format!("parse error: {error}; body={body}"))?;

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
