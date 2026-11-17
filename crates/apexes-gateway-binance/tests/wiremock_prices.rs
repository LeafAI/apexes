//! The last-price lookup the engine uses to value assets at startup, against a
//! local `wiremock` server: a public (unsigned) endpoint, one call per symbol,
//! and one symbol failing must not lose the others.

use apexes_gateway_binance::{BinanceExecutionGateway, BinanceGatewayConfig};
use rust_decimal_macros::dec;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn gateway_against(server: &MockServer) -> BinanceExecutionGateway {
    BinanceExecutionGateway::new(BinanceGatewayConfig {
        api_key: "test-api-key".to_string(),
        api_secret: "test-api-secret".to_string(),
        spot_base_url: server.uri(),
        futures_base_url: server.uri(),
        recv_window_ms: 5000,
    })
    .spot_only()
}

async fn price_of(server: &MockServer, symbol: &str, price: &str) {
    Mock::given(method("GET"))
        .and(path("/api/v3/ticker/price"))
        .and(query_param("symbol", symbol))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "symbol": symbol,
            "price": price,
        })))
        .mount(server)
        .await;
}

#[tokio::test]
async fn last_prices_returns_every_requested_symbol_exactly() {
    let server = MockServer::start().await;
    price_of(&server, "BTCUSDT", "64901.99000000").await;
    price_of(&server, "ETHUSDT", "1821.49000000").await;
    price_of(&server, "BNBUSDT", "591.03000000").await;

    let prices = gateway_against(&server)
        .last_prices(&["BTCUSDT".into(), "ETHUSDT".into(), "BNBUSDT".into()])
        .await;
    assert_eq!(prices.len(), 3);
    assert_eq!(prices["BTCUSDT"], dec!(64901.99));
    assert_eq!(prices["BNBUSDT"], dec!(591.03));
}

/// A price needs no key and no signature: it is public data, and sending the
/// signing material to a data endpoint would be needless exposure.
#[tokio::test]
async fn the_price_request_is_unsigned_and_carries_no_api_key() {
    let server = MockServer::start().await;
    price_of(&server, "BTCUSDT", "1").await;
    let _ = gateway_against(&server)
        .last_prices(&["BTCUSDT".into()])
        .await;
    let requests: Vec<Request> = server.received_requests().await.unwrap_or_default();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    let query = request.url.query().unwrap_or_default();
    assert!(!query.contains("signature"), "{query}");
    assert!(!query.contains("timestamp"), "{query}");
    assert!(
        request.headers.get("x-mbx-apikey").is_none(),
        "no key header on a public call"
    );
}

/// One symbol the exchange refuses (or cannot answer) must not cost the session
/// the prices it did get: startup then values those and leaves the rest to the
/// first trade.
#[tokio::test]
async fn a_symbol_that_fails_is_left_out_and_the_others_are_kept() {
    let server = MockServer::start().await;
    price_of(&server, "BTCUSDT", "64901.99").await;
    Mock::given(method("GET"))
        .and(path("/api/v3/ticker/price"))
        .and(query_param("symbol", "NOPEUSDT"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "code": -1121, "msg": "Invalid symbol."
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/ticker/price"))
        .and(query_param("symbol", "GARBLEDUSDT"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;

    let prices = gateway_against(&server)
        .last_prices(&["BTCUSDT".into(), "NOPEUSDT".into(), "GARBLEDUSDT".into()])
        .await;
    assert_eq!(prices.len(), 1, "{prices:?}");
    assert_eq!(prices["BTCUSDT"], dec!(64901.99));
}

#[tokio::test]
async fn a_zero_or_negative_price_is_never_used() {
    let server = MockServer::start().await;
    price_of(&server, "ZEROUSDT", "0.00000000").await;
    price_of(&server, "BTCUSDT", "64901.99").await;
    let prices = gateway_against(&server)
        .last_prices(&["ZEROUSDT".into(), "BTCUSDT".into()])
        .await;
    assert!(
        !prices.contains_key("ZEROUSDT"),
        "a zero price would value an asset at nothing"
    );
    assert!(prices.contains_key("BTCUSDT"));
}

#[tokio::test]
async fn no_symbols_means_no_requests() {
    let server = MockServer::start().await;
    let prices = gateway_against(&server).last_prices(&[]).await;
    assert!(prices.is_empty());
    assert!(
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty()
    );
}
