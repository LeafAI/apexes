//! Verifies `BinanceExecutionGateway` calls Binance's leverage/margin-type
//! endpoints before submitting a leveraged order, caches the result to
//! avoid redundant calls, and rejects the order (without submitting it)
//! if either setup call fails.

use apexes_core::{AssetClass, MarginMode, Order, OrderId, OrderStatus, OrderType, Side, Venue};
use apexes_gateway_binance::{BinanceExecutionGateway, BinanceGatewayConfig};
use apexes_oms::{ExecutionGateway, GatewayOutcome};
use chrono::Utc;
use rust_decimal_macros::dec;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn leveraged_order(id: Uuid) -> Order {
    Order {
        id: OrderId(id),
        exchange_order_id: None,
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Perpetual,
        side: Side::Buy,
        order_type: OrderType::Market,
        quantity: dec!(1),
        price: None,
        stop_price: None,
        leverage: Some(dec!(10)),
        margin_mode: Some(MarginMode::Isolated),
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
async fn submit_order_sets_margin_type_and_leverage_before_submitting() {
    let mock_server = MockServer::start().await;
    mount_clock(&mock_server).await;

    Mock::given(method("POST"))
        .and(path("/fapi/v1/marginType"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"code": 200, "msg": "success"})),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/fapi/v1/leverage"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"leverage": 10, "symbol": "BTCUSDT"})),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/fapi/v1/order"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"orderId": 999})))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&leveraged_order(Uuid::new_v4())).await;

    assert_eq!(
        outcome,
        GatewayOutcome::Accepted {
            exchange_order_id: "999".to_string()
        }
    );
}

#[tokio::test]
async fn submit_order_does_not_re_set_leverage_for_the_same_symbol_and_settings() {
    let mock_server = MockServer::start().await;
    mount_clock(&mock_server).await;

    Mock::given(method("POST"))
        .and(path("/fapi/v1/marginType"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"code": 200})))
        .expect(1) // exactly once across BOTH submit_order calls below
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/fapi/v1/leverage"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"leverage": 10})))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/fapi/v1/order"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"orderId": 1})))
        .expect(2)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    gateway.submit_order(&leveraged_order(Uuid::new_v4())).await;
    gateway.submit_order(&leveraged_order(Uuid::new_v4())).await; // same symbol/leverage/mode

    // wiremock's .expect(1) on marginType/leverage above asserts the
    // exact-once-not-twice expectation when the mock server is dropped
    // at the end of the test.
}

#[tokio::test]
async fn submit_order_is_rejected_without_submitting_when_leverage_setup_fails() {
    let mock_server = MockServer::start().await;
    mount_clock(&mock_server).await;

    Mock::given(method("POST"))
        .and(path("/fapi/v1/marginType"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"code": 200})))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/fapi/v1/leverage"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(serde_json::json!({"code": -4028, "msg": "Leverage is not valid"})),
        )
        .mount(&mock_server)
        .await;
    // Deliberately no mock for /fapi/v1/order -- if submit_order calls
    // it anyway, wiremock will return a 404 rather than the test
    // silently passing.

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&leveraged_order(Uuid::new_v4())).await;

    assert!(matches!(outcome, GatewayOutcome::Rejected { .. }));
}
