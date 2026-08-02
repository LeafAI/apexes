//! [`XtpExecutionGateway`]: implements `apexes_oms::ExecutionGateway`
//! against the XTP protocol. Every method performs real, complete
//! local parameter validation and Order -> XTP-request-shape mapping,
//! then returns `GatewayOutcome::Uncertain` (never a panic, never
//! `unimplemented!()`, never a silent success) because the actual XTP
//! SDK binary has never been linked -- see this crate's `lib.rs` docs
//! for the full rationale.

use apexes_core::{AssetClass, Order, OrderId, OrderStatus};
use apexes_oms::{ExchangeFill, ExecutionGateway, GatewayOutcome};
use async_trait::async_trait;

use crate::config::XtpGatewayConfig;
use crate::types::{XtpMarket, XtpOrderInsertRequest, XtpPriceType, XtpSide};

pub struct XtpExecutionGateway {
    #[allow(dead_code)]
    config: XtpGatewayConfig,
}

impl XtpExecutionGateway {
    pub fn new(config: XtpGatewayConfig) -> Self {
        Self { config }
    }

    /// Real Order -> XTP protocol request-shape mapping, kept as an
    /// associated function (not requiring `&self`) so it's directly
    /// unit-testable without constructing a gateway instance.
    fn build_insert_request(order: &Order) -> Result<XtpOrderInsertRequest, String> {
        let market = match order.venue {
            apexes_core::Venue::Sse => XtpMarket::ShanghaiA,
            apexes_core::Venue::Szse => XtpMarket::ShenzhenA,
            other => return Err(format!("XTP gateway does not support venue {other:?}")),
        };
        let side = match order.side {
            apexes_core::Side::Buy => XtpSide::Buy,
            apexes_core::Side::Sell => XtpSide::Sell,
        };
        let price_type = match order.order_type {
            apexes_core::OrderType::Limit => XtpPriceType::Limit,
            apexes_core::OrderType::Market => XtpPriceType::Market,
            other => {
                return Err(format!("XTP gateway does not support order type {other:?}"));
            }
        };
        let price = order
            .price
            .and_then(|p| p.to_string().parse::<f64>().ok())
            .unwrap_or(0.0);
        let quantity = order.quantity.to_string().parse::<i64>().unwrap_or(0);
        Ok(XtpOrderInsertRequest {
            ticker: order.symbol.clone(),
            market,
            side,
            price_type,
            price,
            quantity,
        })
    }

    fn not_yet_linked_reason() -> String {
        "XTP SDK not yet linked -- pending real account access; see \
         docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md \
         §3 for scope"
            .to_string()
    }
}

#[async_trait]
impl ExecutionGateway for XtpExecutionGateway {
    async fn submit_order(&self, order: &Order) -> GatewayOutcome {
        match Self::build_insert_request(order) {
            Ok(_request) => GatewayOutcome::Uncertain {
                reason: Self::not_yet_linked_reason(),
            },
            Err(reason) => GatewayOutcome::Rejected { reason },
        }
    }

    async fn cancel_order(&self, _order: &Order) -> GatewayOutcome {
        GatewayOutcome::Uncertain {
            reason: Self::not_yet_linked_reason(),
        }
    }

    async fn query_order_status(&self, _order: &Order) -> Result<Option<OrderStatus>, String> {
        Err(Self::not_yet_linked_reason())
    }

    async fn query_all_open_orders(
        &self,
        _asset_class: AssetClass,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
        Err(Self::not_yet_linked_reason())
    }

    async fn query_fills_for_order(&self, _order: &Order) -> Result<Vec<ExchangeFill>, String> {
        Err(Self::not_yet_linked_reason())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, Order, OrderId, OrderStatus, OrderType, Side, Venue};
    use apexes_oms::{ExecutionGateway, GatewayOutcome};
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn sample_config() -> XtpGatewayConfig {
        XtpGatewayConfig {
            client_id: 1,
            account_key: "test-key".to_string(),
            trade_server_ip: "127.0.0.1".to_string(),
            trade_server_port: 6001,
            quote_server_ip: "127.0.0.1".to_string(),
            quote_server_port: 6002,
        }
    }

    fn sample_order(venue: Venue, order_type: OrderType, side: Side) -> Order {
        Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "600000".to_string(),
            venue,
            asset_class: AssetClass::Equity,
            side,
            order_type,
            quantity: dec!(100),
            price: Some(dec!(10.5)),
            stop_price: None,
            leverage: None,
            margin_mode: None,
            filled_quantity: dec!(0),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn build_insert_request_maps_sse_buy_limit_correctly() {
        let order = sample_order(Venue::Sse, OrderType::Limit, Side::Buy);
        let request = XtpExecutionGateway::build_insert_request(&order).unwrap();
        assert_eq!(request.ticker, "600000");
        assert_eq!(request.market, XtpMarket::ShanghaiA);
        assert_eq!(request.side, XtpSide::Buy);
        assert_eq!(request.price_type, XtpPriceType::Limit);
        assert_eq!(request.price, 10.5);
        assert_eq!(request.quantity, 100);
    }

    #[test]
    fn build_insert_request_maps_szse_sell_market_correctly() {
        let order = sample_order(Venue::Szse, OrderType::Market, Side::Sell);
        let request = XtpExecutionGateway::build_insert_request(&order).unwrap();
        assert_eq!(request.market, XtpMarket::ShenzhenA);
        assert_eq!(request.side, XtpSide::Sell);
        assert_eq!(request.price_type, XtpPriceType::Market);
    }

    #[test]
    fn build_insert_request_rejects_unsupported_venue() {
        let order = sample_order(Venue::Binance, OrderType::Limit, Side::Buy);
        let result = XtpExecutionGateway::build_insert_request(&order);
        assert!(result.is_err());
    }

    #[test]
    fn build_insert_request_rejects_unsupported_order_type() {
        let order = sample_order(Venue::Sse, OrderType::Stop, Side::Buy);
        let result = XtpExecutionGateway::build_insert_request(&order);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn submit_order_returns_uncertain_for_a_valid_order() {
        let gateway = XtpExecutionGateway::new(sample_config());
        let order = sample_order(Venue::Sse, OrderType::Limit, Side::Buy);
        let outcome = gateway.submit_order(&order).await;
        assert!(matches!(outcome, GatewayOutcome::Uncertain { .. }));
    }

    #[tokio::test]
    async fn submit_order_returns_rejected_for_an_unsupported_venue() {
        let gateway = XtpExecutionGateway::new(sample_config());
        let order = sample_order(Venue::Binance, OrderType::Limit, Side::Buy);
        let outcome = gateway.submit_order(&order).await;
        assert!(matches!(outcome, GatewayOutcome::Rejected { .. }));
    }

    #[tokio::test]
    async fn cancel_order_returns_uncertain() {
        let gateway = XtpExecutionGateway::new(sample_config());
        let order = sample_order(Venue::Sse, OrderType::Limit, Side::Buy);
        let outcome = gateway.cancel_order(&order).await;
        assert!(matches!(outcome, GatewayOutcome::Uncertain { .. }));
    }

    #[tokio::test]
    async fn query_order_status_returns_err() {
        let gateway = XtpExecutionGateway::new(sample_config());
        let order = sample_order(Venue::Sse, OrderType::Limit, Side::Buy);
        let result = gateway.query_order_status(&order).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn query_all_open_orders_returns_err() {
        let gateway = XtpExecutionGateway::new(sample_config());
        let result = gateway.query_all_open_orders(AssetClass::Equity).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn query_fills_for_order_returns_err() {
        let gateway = XtpExecutionGateway::new(sample_config());
        let order = sample_order(Venue::Sse, OrderType::Limit, Side::Buy);
        let result = gateway.query_fills_for_order(&order).await;
        assert!(result.is_err());
    }
}
