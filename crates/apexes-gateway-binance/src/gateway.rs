//! [`BinanceExecutionGateway`]: implements `apexes_oms::ExecutionGateway`
//! against real Binance Spot and USDS-M Perpetual Futures REST APIs.

use apexes_core::{AssetClass, Order, OrderId, OrderStatus};
use apexes_oms::{ExchangeFill, ExecutionGateway, GatewayOutcome};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::config::BinanceGatewayConfig;
use crate::error::BinanceApiError;
use crate::rest::{ApiFamily, BinanceRestClient};

pub struct BinanceExecutionGateway {
    rest: BinanceRestClient,
}

impl BinanceExecutionGateway {
    pub fn new(config: BinanceGatewayConfig) -> Self {
        Self {
            rest: BinanceRestClient::new(config),
        }
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
        let family = ApiFamily::from(order.asset_class);
        let side = match order.side {
            apexes_core::Side::Buy => "BUY",
            apexes_core::Side::Sell => "SELL",
        };
        let order_type = match order.order_type {
            apexes_core::OrderType::Market => "MARKET",
            apexes_core::OrderType::Limit => "LIMIT",
            apexes_core::OrderType::Stop => "STOP_LOSS",
            apexes_core::OrderType::StopLimit => "STOP_LOSS_LIMIT",
        };

        let mut params = format!(
            "symbol={}&side={side}&type={order_type}&quantity={}&newClientOrderId={}",
            order.symbol, order.quantity, order.id.0
        );
        if let Some(price) = order.price {
            params.push_str(&format!("&price={price}"));
            if order.order_type == apexes_core::OrderType::Limit {
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

        entries
            .into_iter()
            .map(|entry| {
                let client_order_id = entry
                    .client_order_id
                    .parse::<uuid::Uuid>()
                    .map_err(|error| {
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
            })
            .collect()
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
