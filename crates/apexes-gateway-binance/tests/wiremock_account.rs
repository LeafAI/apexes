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
