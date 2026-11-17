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

// ---- trading rules (exchangeInfo) --------------------------------------

/// The shape the real Spot Test Network returned for BNBUSDT.
fn exchange_info(symbol: &str, step: &str, min_qty: &str) -> serde_json::Value {
    serde_json::json!({
        "symbols": [{
            "symbol": symbol,
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0.01000000",
                 "maxPrice": "100000.00000000", "tickSize": "0.01000000"},
                {"filterType": "LOT_SIZE", "minQty": min_qty,
                 "maxQty": "900000.00000000", "stepSize": step},
                {"filterType": "MARKET_LOT_SIZE", "minQty": "0.00000000",
                 "maxQty": "4907.60017991", "stepSize": "0.00000000"},
                {"filterType": "NOTIONAL", "minNotional": "5.00000000",
                 "applyMinToMarket": true, "maxNotional": "9000000.00000000",
                 "applyMaxToMarket": false, "avgPriceMins": 5}
            ]
        }]
    })
}

async fn rules_of(server: &MockServer, symbol: &str, step: &str, min_qty: &str) {
    Mock::given(method("GET"))
        .and(path("/api/v3/exchangeInfo"))
        .and(query_param("symbol", symbol))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(exchange_info(symbol, step, min_qty)),
        )
        .mount(server)
        .await;
}

#[tokio::test]
async fn each_symbols_own_step_minimum_tick_and_notional_are_read() {
    let server = MockServer::start().await;
    rules_of(&server, "BNBUSDT", "0.00100000", "0.00100000").await;
    rules_of(&server, "ETHUSDT", "0.00010000", "0.00010000").await;

    let rules = gateway_against(&server)
        .symbol_rules(&["BNBUSDT".into(), "ETHUSDT".into()])
        .await;
    assert_eq!(rules.len(), 2);
    let bnb = &rules["BNBUSDT"];
    assert_eq!(bnb.quantity_step, dec!(0.001));
    assert_eq!(bnb.min_quantity, dec!(0.001));
    assert_eq!(bnb.max_quantity, dec!(900000));
    assert_eq!(bnb.price_tick, dec!(0.01));
    assert_eq!(bnb.min_notional, dec!(5));
    assert_eq!(
        rules["ETHUSDT"].quantity_step,
        dec!(0.0001),
        "not BNB's step"
    );
}

#[tokio::test]
async fn the_rules_request_is_unsigned_and_carries_no_api_key() {
    let server = MockServer::start().await;
    rules_of(&server, "BNBUSDT", "0.001", "0.001").await;
    let _ = gateway_against(&server)
        .symbol_rules(&["BNBUSDT".into()])
        .await;
    let requests = server.received_requests().await.unwrap_or_default();
    assert_eq!(requests.len(), 1);
    let query = requests[0].url.query().unwrap_or_default();
    assert!(
        !query.contains("signature") && !query.contains("timestamp"),
        "{query}"
    );
    assert!(requests[0].headers.get("x-mbx-apikey").is_none());
}

/// A symbol whose rules cannot be had is left out (the exchange then decides
/// for it, as before); it must not cost the others theirs.
#[tokio::test]
async fn a_symbol_whose_rules_fail_is_left_out_and_the_rest_kept() {
    let server = MockServer::start().await;
    rules_of(&server, "BNBUSDT", "0.001", "0.001").await;
    Mock::given(method("GET"))
        .and(path("/api/v3/exchangeInfo"))
        .and(query_param("symbol", "NOPEUSDT"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "code": -1121, "msg": "Invalid symbol."
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/exchangeInfo"))
        .and(query_param("symbol", "EMPTYUSDT"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"symbols": []})))
        .mount(&server)
        .await;
    let rules = gateway_against(&server)
        .symbol_rules(&["BNBUSDT".into(), "NOPEUSDT".into(), "EMPTYUSDT".into()])
        .await;
    assert_eq!(rules.len(), 1, "{rules:?}");
    assert!(rules.contains_key("BNBUSDT"));
}

/// A symbol that lists only some filters keeps "no rule" (zero) for the rest.
#[tokio::test]
async fn a_symbol_with_only_some_filters_has_no_rule_for_the_others() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/exchangeInfo"))
        .and(query_param("symbol", "LONELYUSDT"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "symbols": [{"symbol": "LONELYUSDT", "filters": [
                {"filterType": "LOT_SIZE", "minQty": "0.5", "maxQty": "100", "stepSize": "0.5"}
            ]}]
        })))
        .mount(&server)
        .await;
    let rules = gateway_against(&server)
        .symbol_rules(&["LONELYUSDT".into()])
        .await;
    let r = &rules["LONELYUSDT"];
    assert_eq!(r.quantity_step, dec!(0.5));
    assert_eq!(r.price_tick, dec!(0));
    assert_eq!(r.min_notional, dec!(0));
}

/// Older symbols carry MIN_NOTIONAL instead of NOTIONAL.
#[tokio::test]
async fn the_older_min_notional_filter_is_understood_too() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v3/exchangeInfo"))
        .and(query_param("symbol", "OLDUSDT"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "symbols": [{"symbol": "OLDUSDT", "filters": [
                {"filterType": "MIN_NOTIONAL", "minNotional": "10.00000000",
                 "applyToMarket": true, "avgPriceMins": 5}
            ]}]
        })))
        .mount(&server)
        .await;
    let rules = gateway_against(&server)
        .symbol_rules(&["OLDUSDT".into()])
        .await;
    assert_eq!(rules["OLDUSDT"].min_notional, dec!(10));
}
