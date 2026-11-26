//! Verifies `BinanceExecutionGateway`'s new account/fund-management
//! methods (balances, positions, internal transfer, deposit/withdrawal
//! history) send correctly-shaped, correctly-signed requests and map
//! Binance's response shapes correctly -- using a local `wiremock`
//! server, no real network access.

use apexes_core::{FundMovementStatus, WalletId};
use apexes_gateway_binance::{BinanceExecutionGateway, BinanceGatewayConfig};
use rust_decimal_macros::dec;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

fn commission_response(symbol: &str) -> serde_json::Value {
    serde_json::json!({
        "symbol":symbol,
        "standardCommission":{"maker":"0.001","taker":"0.002","buyer":"0.0001","seller":"0.0002"},
        "specialCommission":{"maker":"0.003","taker":"0.001","buyer":"0.0003","seller":"0.0004"},
        "taxCommission":{"maker":"0.0001","taker":"0.0002","buyer":"0.00001","seller":"0.00002"},
        "discount":{"enabledForAccount":true,"enabledForSymbol":true,"discountAsset":"BNB","discount":"0.25"}
    })
}

async fn mount_commission(server: &MockServer) {
    use wiremock::matchers::{header, query_param};
    Mock::given(method("GET"))
        .and(path("/api/v3/account/commission"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(header("X-MBX-APIKEY", "test-api-key"))
        .respond_with(|request: &wiremock::Request| {
            assert!(
                request
                    .url
                    .query()
                    .unwrap_or_default()
                    .contains("signature=")
            );
            ResponseTemplate::new(200).set_body_json(commission_response("BTCUSDT"))
        })
        .mount(server)
        .await;
}

#[tokio::test]
async fn fee_conversion_uses_unsigned_verified_pair_and_closed_causal_candle() {
    use wiremock::matchers::{header_exists, query_param};
    let server = MockServer::start().await;
    let at = chrono::DateTime::from_timestamp_millis(1_800_000_030_000).unwrap();
    let minute = at.timestamp_millis().div_euclid(60_000) * 60_000;
    Mock::given(method("GET")).and(path("/api/v3/exchangeInfo"))
        .and(query_param("symbol","BNBUSDT"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "symbols":[{"symbol":"BNBUSDT","baseAsset":"BNB","quoteAsset":"USDT","isSpotTradingAllowed":true}]
        }))).expect(1).mount(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .and(query_param("symbol", "BNBUSDT"))
        .and(query_param("interval", "1m"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            [minute - 120000, "1", "1", "1", "299", "1", minute - 60001],
            [minute - 60000, "1", "1", "1", "300", "1", minute - 1],
            [minute, "1", "1", "1", "999", "1", minute + 59999]
        ])))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(header_exists("X-MBX-APIKEY"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .with_priority(1)
        .mount(&server)
        .await;
    let gateway = gateway_against(&server).await;
    let price = gateway.fee_conversion("BNB", "USDT", at).await.unwrap();
    assert_eq!(price.rate, dec!(300));
    assert_eq!(price.observed_at.timestamp_millis(), minute - 1);
    assert!(gateway.fee_conversion("BNB&bad", "USDT", at).await.is_err());
}

#[tokio::test]
async fn fee_conversion_rejects_mismatched_pair_metadata() {
    let server = MockServer::start().await;
    Mock::given(method("GET")).and(path("/api/v3/exchangeInfo"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "symbols":[{"symbol":"BNBUSDT","baseAsset":"OTHER","quoteAsset":"USDT","isSpotTradingAllowed":true}]
        }))).expect(1).mount(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    assert!(
        gateway_against(&server)
            .await
            .fee_conversion("BNB", "USDT", chrono::Utc::now())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn spot_preflight_conforms_to_fresh_rules_and_uses_only_free_balance() {
    use apexes_core::{AssetClass, OrderType, Side, Venue};
    use apexes_oms::ExecutionGateway;
    use apexes_strategy::{OrderIntent, OrderRouting};
    let server = MockServer::start().await;
    let gateway = gateway_against(&server).await;
    let mut intent = OrderIntent {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Limit,
        quantity: dec!(1.27),
        price: Some(dec!(101.27)),
        stop_price: None,
        execution_algo: None,
        routing: OrderRouting::Fixed,
        leverage: None,
        margin_mode: None,
    };
    for (step, expected, status) in [
        ("0.01", dec!(1.27), "TRADING"),
        ("0.1", dec!(1.2), "TRADING"),
        ("0.1", dec!(1.2), "BREAK"),
    ] {
        server.reset().await;
        mount_clock(&server).await;
        mount_commission(&server).await;
        Mock::given(method("GET")).and(path("/api/v3/exchangeInfo"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "symbols":[{"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT",
                    "status":status,"isSpotTradingAllowed":true,"orderTypes":["LIMIT","MARKET"],
                    "filters":[{"filterType":"LOT_SIZE","minQty":"0.01","maxQty":"100","stepSize":step},
                    {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                    {"filterType":"NOTIONAL","minNotional":"5","maxNotional":"200","applyMinToMarket":true,"applyMaxToMarket":true,"avgPriceMins":0}]}]
            }))).mount(&server).await;
        Mock::given(method("GET")).and(path("/api/v3/account"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "balances":[{"asset":"USDT","free":"50","locked":"900"},{"asset":"BTC","free":"2","locked":"3"}]
            }))).mount(&server).await;
        if status != "TRADING" {
            assert!(gateway.spot_preflight(&intent).await.is_err());
            continue;
        }
        let evidence = gateway.spot_preflight(&intent).await.unwrap();
        assert_eq!(evidence.quantity, expected);
        assert_eq!(evidence.price, Some(dec!(101.2)));
        assert_eq!(evidence.free_quote, dec!(50));
        assert_eq!(evidence.free_base, dec!(2));
        assert_eq!(evidence.commission.rate, dec!(0.00451));
        assert_eq!(evidence.commission.discount_asset.as_deref(), Some("BNB"));
        intent.side = Side::Sell;
        let sell = gateway.spot_preflight(&intent).await.unwrap();
        assert_eq!(sell.price, Some(dec!(101.3)));
        assert_eq!(sell.commission.rate, dec!(0.00472));
        intent.side = Side::Buy;
        intent.quantity = dec!(3);
        assert!(gateway.spot_preflight(&intent).await.is_err()); // max notional
        intent.quantity = dec!(1.27);
    }
}

#[tokio::test]
async fn market_preflight_requires_both_lot_filters_and_matching_average_window() {
    use apexes_core::{AssetClass, OrderType, Side, Venue};
    use apexes_oms::ExecutionGateway;
    use apexes_strategy::{OrderIntent, OrderRouting};
    let server = MockServer::start().await;
    mount_clock(&server).await;
    mount_commission(&server).await;
    Mock::given(method("GET")).and(path("/api/v3/exchangeInfo"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "symbols":[{"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT",
                "status":"TRADING","isSpotTradingAllowed":true,"orderTypes":["MARKET","LIMIT"],
                "filters":[{"filterType":"LOT_SIZE","minQty":"0.01","maxQty":"100","stepSize":"0.01"},
                {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                {"filterType":"MARKET_LOT_SIZE","minQty":"0.1","maxQty":"2","stepSize":"0.1"},
                {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true,"avgPriceMins":5}]}]
        }))).mount(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/v3/ticker/price"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"price":"100"})))
        .mount(&server)
        .await;
    Mock::given(method("GET")).and(path("/api/v3/avgPrice"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"mins":5,"price":"100","closeTime":chrono::Utc::now().timestamp_millis()}))).mount(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/v3/account"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"balances":[{"asset":"USDT","free":"1000","locked":"0"}]}),
        ))
        .mount(&server)
        .await;
    let gateway = gateway_against(&server).await;
    let mut intent = OrderIntent {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Market,
        quantity: dec!(1.27),
        price: None,
        stop_price: None,
        execution_algo: None,
        routing: OrderRouting::Fixed,
        leverage: None,
        margin_mode: None,
    };
    let evidence = gateway.spot_preflight(&intent).await.unwrap();
    assert_eq!(evidence.quantity, dec!(1.2));
    assert_eq!(evidence.market_buy_cap, Some(dec!(100)));
    assert_eq!(evidence.commission.rate, dec!(0.00451));
    let mut missing_tax = commission_response("BTCUSDT");
    missing_tax.as_object_mut().unwrap().remove("taxCommission");
    Mock::given(method("GET"))
        .and(path("/api/v3/account/commission"))
        .respond_with(ResponseTemplate::new(200).set_body_json(missing_tax))
        .with_priority(1)
        .mount(&server)
        .await;
    assert!(gateway.spot_preflight(&intent).await.is_err());
    intent.quantity = dec!(2.2);
    assert!(gateway.spot_preflight(&intent).await.is_err());
    intent.quantity = dec!(1);
    Mock::given(method("GET")).and(path("/api/v3/avgPrice"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"mins":1,"price":"100","closeTime":chrono::Utc::now().timestamp_millis()})))
        .with_priority(1).mount(&server).await;
    assert!(gateway.spot_preflight(&intent).await.is_err());
}

#[tokio::test]
async fn account_balances_merges_spot_and_futures_wallets() {
    let mock_server = MockServer::start().await;
    mount_clock(&mock_server).await;

    Mock::given(method("GET"))
        .and(path("/api/v3/account"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "balances": [
                {"asset": "USDT", "free": "1000.0", "locked": "0.0"},
                {"asset": "BTC", "free": "0.5", "locked": "0.1"}
            ]
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/fapi/v2/balance"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"asset": "USDT", "balance": "500.0"}
        ])))
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let spot = gateway.spot_balances().await.unwrap();
    assert_eq!(spot["BTC"].free, dec!(0.5));
    assert_eq!(spot["BTC"].locked, dec!(0.1));
    let balances = gateway.account_balances().await.unwrap();

    // Spot USDT (1000, free+locked) + Futures USDT (500) = 1500.
    assert_eq!(*balances.get("USDT").unwrap(), dec!(1500));
    // BTC: free 0.5 + locked 0.1 = 0.6, only present in Spot.
    assert_eq!(*balances.get("BTC").unwrap(), dec!(0.6));
}

#[tokio::test]
async fn account_positions_maps_futures_position_risk_response() {
    let mock_server = MockServer::start().await;
    mount_clock(&mock_server).await;

    Mock::given(method("GET"))
        .and(path("/fapi/v2/positionRisk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "symbol": "BTCUSDT",
                "positionAmt": "0.5",
                "entryPrice": "60000.0",
                "leverage": "10",
                "marginType": "isolated",
                "isolatedMargin": "3000.0"
            },
            {
                "symbol": "ETHUSDT",
                "positionAmt": "0.0",
                "entryPrice": "0.0",
                "leverage": "5",
                "marginType": "cross",
                "isolatedMargin": "0.0"
            }
        ])))
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let positions = gateway.account_positions().await.unwrap();

    // Zero-quantity entries (ETHUSDT here) are omitted -- no open position.
    assert_eq!(positions.len(), 1);
    let btc = positions.get("BTCUSDT").unwrap();
    assert_eq!(btc.quantity, dec!(0.5));
    assert_eq!(btc.average_entry_price, dec!(60000));
    assert_eq!(btc.leverage, Some(dec!(10)));
    assert_eq!(btc.allocated_margin, dec!(3000));
}

#[tokio::test]
async fn internal_transfer_sends_correct_transfer_type() {
    let mock_server = MockServer::start().await;
    mount_clock(&mock_server).await;

    Mock::given(method("POST"))
        .and(path("/sapi/v1/asset/transfer"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"tranId": 123456})),
        )
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let result = gateway
        .internal_transfer(WalletId::Spot, WalletId::UsdsFutures, "USDT", dec!(100))
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn internal_transfer_surfaces_explicit_rejection() {
    let mock_server = MockServer::start().await;
    mount_clock(&mock_server).await;

    Mock::given(method("POST"))
        .and(path("/sapi/v1/asset/transfer"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "code": -3020,
            "msg": "Transfer out amount exceeds max amount."
        })))
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let result = gateway
        .internal_transfer(WalletId::Spot, WalletId::UsdsFutures, "USDT", dec!(100))
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn deposit_history_maps_response_to_fund_movements() {
    let mock_server = MockServer::start().await;
    mount_clock(&mock_server).await;

    Mock::given(method("GET"))
        .and(path("/sapi/v1/capital/deposit/hisrec"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": "dep-1",
                "amount": "50.0",
                "coin": "USDT",
                "status": 1,
                "insertTime": 1700000000000i64
            }
        ])))
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let history = gateway.deposit_history().await.unwrap();

    assert_eq!(history.len(), 1);
    assert_eq!(history[0].asset, "USDT");
    assert_eq!(history[0].amount, dec!(50));
    assert_eq!(history[0].status, FundMovementStatus::Completed);
    assert_eq!(history[0].exchange_tx_id, "dep-1");
}

#[tokio::test]
async fn withdrawal_history_maps_response_to_fund_movements() {
    let mock_server = MockServer::start().await;
    mount_clock(&mock_server).await;

    Mock::given(method("GET"))
        .and(path("/sapi/v1/capital/withdraw/history"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": "wd-1",
                "amount": "20.0",
                "coin": "USDT",
                "status": 6,
                "applyTime": "2024-01-01 00:00:00"
            }
        ])))
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let history = gateway.withdrawal_history().await.unwrap();

    assert_eq!(history.len(), 1);
    assert_eq!(history[0].asset, "USDT");
    assert_eq!(history[0].amount, dec!(20));
    assert_eq!(history[0].status, FundMovementStatus::Completed);
    assert_eq!(history[0].exchange_tx_id, "wd-1");
}
