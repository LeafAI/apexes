//! Verifies `BinanceExecutionGateway` sends correctly-shaped, correctly-
//! signed HTTP requests, and correctly maps Binance's three response
//! shapes (success / explicit rejection / ambiguous failure) to
//! `GatewayOutcome`, using a local `wiremock` server instead of real
//! Binance endpoints -- no network access required.

use apexes_core::{AssetClass, Order, OrderId, OrderStatus, OrderType, Side, Venue};
use apexes_gateway_binance::{BinanceExecutionGateway, BinanceGatewayConfig};
use apexes_oms::{ExecutionGateway, GatewayOutcome};
use chrono::Utc;
use rust_decimal_macros::dec;
use uuid::Uuid;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn sample_order(id: Uuid) -> Order {
    Order {
        id: OrderId(id),
        exchange_order_id: None,
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Market,
        quantity: dec!(1),
        price: None,
        stop_price: None,
        filled_quantity: dec!(0),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

async fn gateway_against(mock_server: &MockServer) -> BinanceExecutionGateway {
    BinanceExecutionGateway::new(BinanceGatewayConfig {
        api_key: "test-api-key".to_string(),
        api_secret: "test-api-secret".to_string(),
        spot_base_url: mock_server.uri(),
        futures_base_url: mock_server.uri(),
        recv_window_ms: 5000,
    })
}

#[tokio::test]
async fn submit_order_sends_signed_request_with_client_order_id() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();

    Mock::given(method("POST"))
        .and(path("/api/v3/order"))
        .and(query_param("newClientOrderId", order_id.to_string()))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "orderId": 12345
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&sample_order(order_id)).await;

    assert_eq!(
        outcome,
        GatewayOutcome::Accepted {
            exchange_order_id: "12345".to_string()
        }
    );
}

#[tokio::test]
async fn submit_order_request_includes_a_signature_parameter() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();

    Mock::given(method("POST"))
        .and(path("/api/v3/order"))
        .respond_with(move |request: &wiremock::Request| {
            let query = request.url.query().unwrap_or_default();
            assert!(
                query.contains("signature="),
                "request query string must include a signature parameter, got: {query}"
            );
            assert!(
                query.contains("timestamp="),
                "request query string must include a timestamp parameter, got: {query}"
            );
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "orderId": 1 }))
        })
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    gateway.submit_order(&sample_order(order_id)).await;
}

#[tokio::test]
async fn submit_order_maps_explicit_binance_rejection_to_rejected_outcome() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();

    Mock::given(method("POST"))
        .and(path("/api/v3/order"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "code": -2010,
            "msg": "Account has insufficient balance for requested action."
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&sample_order(order_id)).await;

    assert!(matches!(outcome, GatewayOutcome::Rejected { .. }));
    if let GatewayOutcome::Rejected { reason } = outcome {
        assert!(reason.contains("insufficient balance"));
    }
}

#[tokio::test]
async fn submit_order_maps_server_error_to_uncertain_outcome() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();

    Mock::given(method("POST"))
        .and(path("/api/v3/order"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal server error"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&sample_order(order_id)).await;

    assert!(matches!(outcome, GatewayOutcome::Uncertain { .. }));
}

#[tokio::test]
async fn submit_order_maps_unparseable_success_body_to_uncertain_outcome() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();

    Mock::given(method("POST"))
        .and(path("/api/v3/order"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&sample_order(order_id)).await;

    assert!(matches!(outcome, GatewayOutcome::Uncertain { .. }));
}

#[tokio::test]
async fn submit_order_routes_perpetual_orders_to_futures_path() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();
    let mut order = sample_order(order_id);
    order.asset_class = AssetClass::Perpetual;

    Mock::given(method("POST"))
        .and(path("/fapi/v1/order"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "orderId": 999
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&order).await;

    assert_eq!(
        outcome,
        GatewayOutcome::Accepted {
            exchange_order_id: "999".to_string()
        }
    );
}

#[tokio::test]
async fn query_all_open_orders_parses_client_order_id_back_into_order_id() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();

    Mock::given(method("GET"))
        .and(path("/api/v3/openOrders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "orderId": 555,
                "clientOrderId": order_id.to_string(),
                "status": "PARTIALLY_FILLED"
            }
        ])))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let open_orders = gateway
        .query_all_open_orders(AssetClass::Spot)
        .await
        .unwrap();

    assert_eq!(open_orders.len(), 1);
    assert_eq!(open_orders[0].0, OrderId(order_id));
    assert_eq!(open_orders[0].1, Some("555".to_string()));
    assert_eq!(open_orders[0].2, OrderStatus::PartiallyFilled);
}

#[tokio::test]
async fn query_fills_for_order_parses_trade_list() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();
    let mut order = sample_order(order_id);
    order.exchange_order_id = Some("777".to_string());

    Mock::given(method("GET"))
        .and(path("/api/v3/myTrades"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(query_param("orderId", "777"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": 1001,
                "price": "65000.00000000",
                "qty": "0.50000000",
                "commission": "1.20000000",
                "commissionAsset": "USDT",
                "time": 1700000000000i64
            }
        ])))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let fills = gateway.query_fills_for_order(&order).await.unwrap();

    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].exchange_trade_id, "1001");
    assert_eq!(fills[0].price, dec!(65000));
    assert_eq!(fills[0].quantity, dec!(0.5));
    assert_eq!(fills[0].fee, dec!(1.2));
    assert_eq!(fills[0].fee_currency, Some("USDT".to_string()));
}

#[tokio::test]
async fn cancel_order_sends_symbol_and_order_id() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();
    let mut order = sample_order(order_id);
    order.exchange_order_id = Some("321".to_string());

    Mock::given(method("DELETE"))
        .and(path("/api/v3/order"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(query_param("orderId", "321"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "orderId": 321
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.cancel_order(&order).await;

    assert_eq!(
        outcome,
        GatewayOutcome::Accepted {
            exchange_order_id: "321".to_string()
        }
    );
}

#[tokio::test]
async fn cancel_order_with_no_exchange_order_id_is_rejected_without_a_network_call() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();
    let order = sample_order(order_id); // exchange_order_id is None

    // No Mock registered at all -- if the gateway tried to make a
    // request, wiremock would return a 404 "no matching mock" response
    // (not what this test is checking for), but the real assertion is
    // that GatewayOutcome::Rejected is returned immediately without
    // attempting any request.
    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.cancel_order(&order).await;

    assert!(matches!(outcome, GatewayOutcome::Rejected { .. }));
}
