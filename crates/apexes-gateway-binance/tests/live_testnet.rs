//! Manual, opt-in integration test against real Binance Testnet
//! (https://testnet.binance.vision). NOT run by default `cargo test` --
//! requires real testnet API credentials, which this codebase never
//! auto-loads (see `apexes-gateway-binance`'s design: `BinanceGatewayConfig`
//! has no env-var/config-file loading in production code). Run with:
//!
//!   export APEXES_BINANCE_TESTNET_API_KEY=...
//!   export APEXES_BINANCE_TESTNET_API_SECRET=...
//!   cargo test -p apexes-gateway-binance --test live_testnet -- --ignored

use apexes_core::{AssetClass, Order, OrderId, OrderStatus, OrderType, Side, Venue};
use apexes_gateway_binance::{BinanceExecutionGateway, BinanceGatewayConfig};
use apexes_oms::{ExecutionGateway, GatewayOutcome};
use chrono::Utc;
use rust_decimal_macros::dec;
use uuid::Uuid;

fn testnet_config() -> BinanceGatewayConfig {
    BinanceGatewayConfig {
        api_key: std::env::var("APEXES_BINANCE_TESTNET_API_KEY")
            .expect("set APEXES_BINANCE_TESTNET_API_KEY to run this ignored test"),
        api_secret: std::env::var("APEXES_BINANCE_TESTNET_API_SECRET")
            .expect("set APEXES_BINANCE_TESTNET_API_SECRET to run this ignored test"),
        spot_base_url: "https://testnet.binance.vision".to_string(),
        futures_base_url: "https://testnet.binancefuture.com".to_string(),
        recv_window_ms: 5000,
    }
}

#[tokio::test]
#[ignore]
async fn submit_and_cancel_a_limit_order_far_from_market_price_on_spot_testnet() {
    let gateway = BinanceExecutionGateway::new(testnet_config());

    // A limit price far below any realistic BTCUSDT market price, so
    // this order rests indefinitely on the book rather than filling
    // during the test (avoiding any real, even testnet-fake-money,
    // execution as a side effect of running this test).
    let order = Order {
        id: OrderId(Uuid::new_v4()),
        exchange_order_id: None,
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Limit,
        quantity: dec!(0.001),
        price: Some(dec!(1000)),
        stop_price: None,
        filled_quantity: dec!(0),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    let outcome = gateway.submit_order(&order).await;
    let GatewayOutcome::Accepted { exchange_order_id } = outcome else {
        panic!("expected Accepted, got {outcome:?}");
    };

    let mut order_with_id = order.clone();
    order_with_id.exchange_order_id = Some(exchange_order_id.clone());

    let status = gateway.query_order_status(&order_with_id).await.unwrap();
    assert_eq!(status, Some(OrderStatus::New));

    let cancel_outcome = gateway.cancel_order(&order_with_id).await;
    assert!(matches!(cancel_outcome, GatewayOutcome::Accepted { .. }));
}

#[tokio::test]
#[ignore]
async fn query_all_open_orders_returns_the_order_just_placed() {
    let gateway = BinanceExecutionGateway::new(testnet_config());

    let order = Order {
        id: OrderId(Uuid::new_v4()),
        exchange_order_id: None,
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Limit,
        quantity: dec!(0.001),
        price: Some(dec!(1000)),
        stop_price: None,
        filled_quantity: dec!(0),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    let outcome = gateway.submit_order(&order).await;
    assert!(matches!(outcome, GatewayOutcome::Accepted { .. }));

    let open_orders = gateway
        .query_all_open_orders(AssetClass::Spot)
        .await
        .unwrap();
    assert!(open_orders.iter().any(|(id, _, _)| *id == order.id));

    // Clean up: cancel it so it doesn't linger on the testnet account
    // across repeated runs of this test.
    let exchange_order_id = open_orders
        .iter()
        .find(|(id, _, _)| *id == order.id)
        .and_then(|(_, exchange_id, _)| exchange_id.clone());
    let mut order_with_id = order.clone();
    order_with_id.exchange_order_id = exchange_order_id;
    gateway.cancel_order(&order_with_id).await;
}

#[tokio::test]
#[ignore]
async fn user_data_stream_connects_and_receives_at_least_the_initial_handshake() {
    let gateway = BinanceExecutionGateway::new(testnet_config());
    let (_handle, stream) = gateway.start_user_data_stream().await.unwrap();
    // The merged user-data stream is `!Unpin` (it contains
    // `pin-project`-based websocket machinery internally), so pin it on
    // the stack to make `StreamExt::next()` callable.
    futures::pin_mut!(stream);

    // Submit an order to generate a User Data Stream event, then wait
    // briefly for it to arrive. This test only proves connectivity and
    // basic event delivery -- it does not assert on specific field
    // values (those are covered by user_data_stream.rs's unit tests
    // against synthetic JSON).
    let order = Order {
        id: OrderId(Uuid::new_v4()),
        exchange_order_id: None,
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Limit,
        quantity: dec!(0.001),
        price: Some(dec!(1000)),
        stop_price: None,
        filled_quantity: dec!(0),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    gateway.submit_order(&order).await;

    use futures::StreamExt;
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next()).await;
    assert!(
        result.is_ok(),
        "expected at least one ExchangeUpdate within 10 seconds of submitting an order"
    );
}
