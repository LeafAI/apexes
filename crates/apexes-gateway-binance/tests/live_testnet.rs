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
use rust_decimal::Decimal;
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

/// Fetches the current BTCUSDT price from Binance Testnet's public,
/// unauthenticated `ticker/price` endpoint and derives a limit price
/// far enough below it to rest indefinitely without filling, while
/// staying inside Binance's `PERCENT_PRICE_BY_SIDE` filter (a buy must
/// be >= 0.5x the 5-minute weighted average price) and above the
/// `NOTIONAL` filter's minimum (`price * quantity >= 5 USDT`, checked
/// against the `qty` this function is called with).
///
/// A fixed price literal (e.g. `1000`) would be rejected outright:
/// Binance Testnet's BTCUSDT `NOTIONAL` filter requires >= 5 USDT
/// notional, and `PERCENT_PRICE_BY_SIDE`'s buy-side floor is ~0.5x the
/// live average price (tens of thousands of dollars at any point since
/// 2017) -- both filters reject at submission time, which is why these
/// tests previously failed when actually run with credentials (see
/// Task 21's review). Using 0.6x of the live price stays safely above
/// the 0.5x floor (leaving margin for the average price moving between
/// this fetch and order submission) while remaining far enough below
/// market to avoid filling during a test run.
async fn safe_resting_limit_price(spot_base_url: &str, qty: rust_decimal::Decimal) -> Decimal {
    let url = format!("{spot_base_url}/api/v3/ticker/price?symbol=BTCUSDT");
    let body = reqwest::get(&url)
        .await
        .expect("failed to reach Binance Testnet's public ticker/price endpoint")
        .text()
        .await
        .expect("failed to read ticker/price response body");

    #[derive(serde::Deserialize)]
    struct TickerPrice {
        price: String,
    }
    let ticker: TickerPrice = serde_json::from_str(&body)
        .unwrap_or_else(|error| panic!("parse error: {error}; body={body}"));
    let last_price: Decimal = ticker
        .price
        .parse()
        .unwrap_or_else(|error| panic!("could not parse ticker price {:?}: {error}", ticker.price));

    let price = (last_price * dec!(0.6)).round_dp(2);
    let notional = price * qty;
    assert!(
        notional >= dec!(5),
        "derived price {price} * qty {qty} = {notional} notional, below Binance's 5 USDT NOTIONAL minimum -- increase qty"
    );
    price
}

#[tokio::test]
#[ignore]
async fn submit_and_cancel_a_limit_order_far_from_market_price_on_spot_testnet() {
    let gateway = BinanceExecutionGateway::new(testnet_config());

    // A limit price derived from the live testnet price (see
    // `safe_resting_limit_price`'s doc comment): far enough below
    // market to rest indefinitely without filling, but not so far that
    // Binance's own PERCENT_PRICE_BY_SIDE/NOTIONAL submission filters
    // reject it outright (a fixed literal like 1000 would be rejected
    // -- see Task 21's review).
    let quantity = dec!(0.001);
    let price = safe_resting_limit_price("https://testnet.binance.vision", quantity).await;
    let order = Order {
        id: OrderId(Uuid::new_v4()),
        exchange_order_id: None,
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Limit,
        quantity,
        price: Some(price),
        stop_price: None,
        leverage: None,
        margin_mode: None,
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

    // The testnet's order lookup can lag a moment behind placement (it
    // sometimes answers "Order does not exist" for an order it just
    // accepted), so poll briefly. Production reconcile treats that answer as
    // "log and retry", never as an order that is gone.
    let mut status = Ok(None);
    for _ in 0..10 {
        status = gateway.query_order_status(&order_with_id).await;
        if matches!(status, Ok(Some(_))) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    // Cancel on every path, including a failed assertion below: a resting
    // order left on the shared testnet account makes the next live engine
    // refuse to start ("exchange has open orders with no local record").
    let cancel_outcome = gateway.cancel_order(&order_with_id).await;
    println!("exchange_order_id={exchange_order_id} status={status:?} cancel={cancel_outcome:?}");
    assert_eq!(status.unwrap(), Some(OrderStatus::New));
    assert!(matches!(cancel_outcome, GatewayOutcome::Accepted { .. }));
}

#[tokio::test]
#[ignore]
async fn query_all_open_orders_returns_the_order_just_placed() {
    let gateway = BinanceExecutionGateway::new(testnet_config());

    let quantity = dec!(0.001);
    let price = safe_resting_limit_price("https://testnet.binance.vision", quantity).await;
    let order = Order {
        id: OrderId(Uuid::new_v4()),
        exchange_order_id: None,
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Limit,
        quantity,
        price: Some(price),
        stop_price: None,
        leverage: None,
        margin_mode: None,
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

/// The Spot user-data stream over the WebSocket API, against the real
/// testnet and through the real gateway: subscribing must succeed, and the
/// updates for an order this test places and cancels must arrive keyed by
/// OUR client order id (including the cancel, which Binance reports under a
/// different `c` and puts our id in `C`).
#[tokio::test]
#[ignore]
async fn spot_user_data_stream_delivers_updates_for_our_order_through_the_gateway() {
    use apexes_core::binance_network::BinanceNetwork;
    use futures::StreamExt;

    let gateway = BinanceExecutionGateway::for_network(
        BinanceNetwork::Testnet,
        std::env::var("APEXES_BINANCE_TESTNET_API_KEY")
            .expect("set APEXES_BINANCE_TESTNET_API_KEY to run this ignored test"),
        std::env::var("APEXES_BINANCE_TESTNET_API_SECRET")
            .expect("set APEXES_BINANCE_TESTNET_API_SECRET to run this ignored test"),
    );
    let (_handle, stream) = gateway
        .start_user_data_stream()
        .await
        .expect("the Spot user-data subscription must succeed on testnet");
    futures::pin_mut!(stream);

    let quantity = dec!(0.001);
    let price =
        safe_resting_limit_price(BinanceNetwork::Testnet.endpoints().spot_rest, quantity).await;
    let order = Order {
        id: OrderId(Uuid::new_v4()),
        exchange_order_id: None,
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Limit,
        quantity,
        price: Some(price),
        stop_price: None,
        leverage: None,
        margin_mode: None,
        filled_quantity: dec!(0),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let outcome = gateway.submit_order(&order).await;
    let GatewayOutcome::Accepted { exchange_order_id } = outcome else {
        panic!("the resting order must be accepted, got {outcome:?}");
    };

    let wait = std::time::Duration::from_secs(15);
    let new_update = tokio::time::timeout(wait, async {
        while let Some(update) = stream.next().await {
            if update.client_order_id == order.id && update.status == OrderStatus::New {
                return update;
            }
        }
        panic!("stream ended");
    })
    .await
    .expect("a NEW update for our order within 15s");
    assert_eq!(
        new_update.exchange_order_id.as_deref(),
        Some(exchange_order_id.as_str())
    );

    let mut with_id = order.clone();
    with_id.exchange_order_id = Some(exchange_order_id);
    gateway.cancel_order(&with_id).await;

    tokio::time::timeout(wait, async {
        while let Some(update) = stream.next().await {
            if update.client_order_id == order.id && update.status == OrderStatus::Cancelled {
                return;
            }
        }
        panic!("stream ended");
    })
    .await
    .expect("a CANCELED update keyed by OUR order id within 15s");
}
