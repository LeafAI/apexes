//! [`IbkrExecutionGateway`]: real TCP client + IBKR wire protocol
//! encoding, implementing `apexes_oms::ExecutionGateway`. Never
//! connected to a real TWS/IB Gateway process -- every method that
//! requires a response the gateway has never actually received
//! returns `GatewayOutcome::Uncertain`/`Err` with an explicit reason,
//! never a fabricated success.

use apexes_core::{AssetClass, Order, OrderId, OrderStatus};
use apexes_oms::{ExchangeFill, ExecutionGateway, GatewayOutcome};
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::config::IbkrGatewayConfig;
use crate::protocol::encode_message;

/// Real TCP client + IBKR wire protocol implementation, never
/// connected to a real TWS/IB Gateway process. `stream` is `None`
/// until `connect()` succeeds; every trading method that needs a
/// response the gateway has never actually received from a real TWS
/// process returns `GatewayOutcome::Uncertain`/`Err`, not a fabricated
/// success -- see this crate's `lib.rs` docs for the full rationale.
pub struct IbkrExecutionGateway {
    config: IbkrGatewayConfig,
    stream: Mutex<Option<TcpStream>>,
}

impl IbkrExecutionGateway {
    pub fn new(config: IbkrGatewayConfig) -> Self {
        Self {
            config,
            stream: Mutex::new(None),
        }
    }

    /// Establishes a real TCP connection to the configured host/port.
    /// This is genuinely real network I/O (unlike
    /// `apexes-gateway-xtp`'s pure request-shape mapping) -- it has
    /// simply never been exercised against a real TWS/IB Gateway
    /// process, only against this crate's own hand-written mock TCP
    /// server in integration tests (Task 6).
    pub async fn connect(&self) -> Result<(), String> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|error| format!("failed to connect to IBKR gateway at {addr}: {error}"))?;
        *self.stream.lock().await = Some(stream);
        Ok(())
    }

    pub(crate) fn order_to_ibkr_side(order: &Order) -> &'static str {
        match order.side {
            apexes_core::Side::Buy => "BUY",
            apexes_core::Side::Sell => "SELL",
        }
    }

    pub(crate) fn order_to_ibkr_order_type(order: &Order) -> Result<&'static str, String> {
        match order.order_type {
            apexes_core::OrderType::Limit => Ok("LMT"),
            apexes_core::OrderType::Market => Ok("MKT"),
            apexes_core::OrderType::Stop => Ok("STP"),
            apexes_core::OrderType::StopLimit => Ok("STP LMT"),
        }
    }

    async fn send_message(&self, fields: &[String]) -> Result<(), String> {
        let mut guard = self.stream.lock().await;
        let stream = guard.as_mut().ok_or_else(|| {
            "not connected to an IBKR gateway -- call connect() first".to_string()
        })?;
        let bytes = encode_message(fields);
        stream
            .write_all(&bytes)
            .await
            .map_err(|error| format!("failed to write to IBKR socket: {error}"))
    }

    fn no_response_reason() -> String {
        "sent to socket, but never connected to a real TWS/IB Gateway process -- no response \
         was ever received to confirm processing; see \
         docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md \
         §4 for scope"
            .to_string()
    }
}

#[async_trait]
impl ExecutionGateway for IbkrExecutionGateway {
    async fn submit_order(&self, order: &Order) -> GatewayOutcome {
        let side = Self::order_to_ibkr_side(order);
        let order_type = match Self::order_to_ibkr_order_type(order) {
            Ok(order_type) => order_type,
            Err(reason) => return GatewayOutcome::Rejected { reason },
        };
        let fields = vec![
            "PLACE_ORDER".to_string(),
            order.id.0.to_string(),
            order.symbol.clone(),
            side.to_string(),
            order_type.to_string(),
            order.quantity.to_string(),
        ];
        match self.send_message(&fields).await {
            Ok(()) => GatewayOutcome::Uncertain {
                reason: Self::no_response_reason(),
            },
            Err(reason) => GatewayOutcome::Uncertain { reason },
        }
    }

    async fn cancel_order(&self, order: &Order) -> GatewayOutcome {
        let fields = vec!["CANCEL_ORDER".to_string(), order.id.0.to_string()];
        match self.send_message(&fields).await {
            Ok(()) => GatewayOutcome::Uncertain {
                reason: Self::no_response_reason(),
            },
            Err(reason) => GatewayOutcome::Uncertain { reason },
        }
    }

    async fn query_order_status(&self, order: &Order) -> Result<Option<OrderStatus>, String> {
        let fields = vec!["QUERY_ORDER_STATUS".to_string(), order.id.0.to_string()];
        self.send_message(&fields).await?;
        Err(Self::no_response_reason())
    }

    async fn query_all_open_orders(
        &self,
        asset_class: AssetClass,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
        let fields = vec![
            "QUERY_ALL_OPEN_ORDERS".to_string(),
            format!("{asset_class:?}"),
        ];
        self.send_message(&fields).await?;
        Err(Self::no_response_reason())
    }

    async fn query_fills_for_order(&self, order: &Order) -> Result<Vec<ExchangeFill>, String> {
        let fields = vec!["QUERY_FILLS_FOR_ORDER".to_string(), order.id.0.to_string()];
        self.send_message(&fields).await?;
        Err(Self::no_response_reason())
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

    fn sample_config() -> IbkrGatewayConfig {
        IbkrGatewayConfig {
            host: "127.0.0.1".to_string(),
            port: 7497,
            client_id: 1,
        }
    }

    fn sample_order(order_type: OrderType, side: Side) -> Order {
        Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "AAPL".to_string(),
            venue: Venue::Nasdaq,
            asset_class: AssetClass::Equity,
            side,
            order_type,
            quantity: dec!(100),
            price: Some(dec!(150.25)),
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
    fn order_to_ibkr_side_maps_buy_and_sell() {
        let buy_order = sample_order(OrderType::Limit, Side::Buy);
        let sell_order = sample_order(OrderType::Limit, Side::Sell);
        assert_eq!(IbkrExecutionGateway::order_to_ibkr_side(&buy_order), "BUY");
        assert_eq!(
            IbkrExecutionGateway::order_to_ibkr_side(&sell_order),
            "SELL"
        );
    }

    #[test]
    fn order_to_ibkr_order_type_maps_all_four_types() {
        assert_eq!(
            IbkrExecutionGateway::order_to_ibkr_order_type(&sample_order(
                OrderType::Limit,
                Side::Buy
            ))
            .unwrap(),
            "LMT"
        );
        assert_eq!(
            IbkrExecutionGateway::order_to_ibkr_order_type(&sample_order(
                OrderType::Market,
                Side::Buy
            ))
            .unwrap(),
            "MKT"
        );
        assert_eq!(
            IbkrExecutionGateway::order_to_ibkr_order_type(&sample_order(
                OrderType::Stop,
                Side::Buy
            ))
            .unwrap(),
            "STP"
        );
        assert_eq!(
            IbkrExecutionGateway::order_to_ibkr_order_type(&sample_order(
                OrderType::StopLimit,
                Side::Buy
            ))
            .unwrap(),
            "STP LMT"
        );
    }

    #[tokio::test]
    async fn submit_order_returns_uncertain_when_not_connected() {
        let gateway = IbkrExecutionGateway::new(sample_config());
        let order = sample_order(OrderType::Limit, Side::Buy);
        let outcome = gateway.submit_order(&order).await;
        assert!(matches!(outcome, GatewayOutcome::Uncertain { .. }));
    }

    #[tokio::test]
    async fn cancel_order_returns_uncertain_when_not_connected() {
        let gateway = IbkrExecutionGateway::new(sample_config());
        let order = sample_order(OrderType::Limit, Side::Buy);
        let outcome = gateway.cancel_order(&order).await;
        assert!(matches!(outcome, GatewayOutcome::Uncertain { .. }));
    }

    #[tokio::test]
    async fn query_order_status_returns_err_when_not_connected() {
        let gateway = IbkrExecutionGateway::new(sample_config());
        let order = sample_order(OrderType::Limit, Side::Buy);
        let result = gateway.query_order_status(&order).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn query_all_open_orders_returns_err_when_not_connected() {
        let gateway = IbkrExecutionGateway::new(sample_config());
        let result = gateway.query_all_open_orders(AssetClass::Equity).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn query_fills_for_order_returns_err_when_not_connected() {
        let gateway = IbkrExecutionGateway::new(sample_config());
        let order = sample_order(OrderType::Limit, Side::Buy);
        let result = gateway.query_fills_for_order(&order).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn connect_fails_cleanly_against_an_unreachable_address() {
        let gateway = IbkrExecutionGateway::new(IbkrGatewayConfig {
            host: "127.0.0.1".to_string(),
            // Port 1 is a reserved/unroutable port on virtually every
            // system, so this connection attempt fails deterministically
            // without needing a real TWS process or a mock server.
            port: 1,
            client_id: 1,
        });
        let result = gateway.connect().await;
        assert!(result.is_err());
    }
}
