//! A live session must refuse a market-data gateway that is on a different
//! Binance network than its own (or that does not say), at `subscribe` time.
//! Uses a real local WebSocket server standing in for the gateway, so the
//! real header-reading path of `GatewayMarketDataProvider` is exercised.

use apexes_core::binance_network::BinanceNetwork;
use apexes_core::{ApexesError, AssetClass, Instrument, MarketDataProvider, MarketType, Venue};
use apexes_engine::gateway_client::{GATEWAY_NETWORK_HEADER, GatewayMarketDataProvider};
use axum::Router;
use axum::extract::ws::WebSocketUpgrade;
use axum::response::IntoResponse;
use axum::routing::get;
use rust_decimal_macros::dec;
use tokio::net::TcpListener;

fn btc_usdt() -> Instrument {
    Instrument::new(
        "BTCUSDT",
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("BTC".to_string()),
        Some("USDT".to_string()),
        dec!(0.01),
        dec!(0.0001),
        dec!(1),
    )
    .unwrap()
}

async fn fake_gateway(announce: Option<&'static str>) -> String {
    let app = Router::new().route(
        "/market",
        get(move |ws: WebSocketUpgrade| async move {
            let mut response = ws.on_upgrade(|_socket| async {}).into_response();
            if let Some(value) = announce {
                response
                    .headers_mut()
                    .insert(GATEWAY_NETWORK_HEADER, value.parse().unwrap());
            }
            response
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("ws://{addr}/market")
}

async fn subscribe_result(
    announce: Option<&'static str>,
    expected: BinanceNetwork,
) -> Result<(), ApexesError> {
    let url = fake_gateway(announce).await;
    let provider = GatewayMarketDataProvider::new(url).with_expected_network(expected);
    provider.subscribe(&[btc_usdt()]).await.map(|_| ())
}

#[tokio::test]
async fn a_matching_gateway_is_accepted() {
    subscribe_result(Some("testnet"), BinanceNetwork::Testnet)
        .await
        .expect("matching network must subscribe");
}

#[tokio::test]
async fn a_testnet_session_refuses_a_mainnet_gateway() {
    let error = subscribe_result(Some("mainnet"), BinanceNetwork::Testnet)
        .await
        .expect_err("must refuse");
    let message = error.to_string();
    assert!(
        message.contains("mainnet") && message.contains("testnet"),
        "{message}"
    );
}

#[tokio::test]
async fn a_mainnet_session_refuses_a_testnet_gateway() {
    assert!(
        subscribe_result(Some("testnet"), BinanceNetwork::Mainnet)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_gateway_that_announces_nothing_is_refused_for_live() {
    let error = subscribe_result(None, BinanceNetwork::Testnet)
        .await
        .expect_err("must refuse");
    assert!(error.to_string().contains("did not announce"), "{error}");
}
