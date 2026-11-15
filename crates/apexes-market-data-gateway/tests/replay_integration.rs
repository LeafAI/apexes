//! Real end-to-end integration test: binds a real `127.0.0.1:0`
//! listener, serves the real `apexes_market_data_gateway::build_app`
//! router over it, connects a real WebSocket client (`tokio-tungstenite`)
//! against that address, and asserts an event pushed through a mock
//! upstream provider is received end-to-end as a JSON text frame.
//! Uses `apexes_message_bus::test_support::MockMarketDataProvider`,
//! not a real Binance connection -- matches this project's
//! established "unit/integration tests never depend on real external
//! services" convention. Mirrors `apexes-ui`'s own
//! `tests/server_client_roundtrip.rs` structural precedent.
//!
//! A `?replay_from=` test against a real ClickHouse instance lives in
//! `tests/live_clickhouse.rs` (already `#[ignore]`d there per Task 4's
//! established pattern) -- this file covers the live-forwarding path
//! only, with `storage_reader: None` (persistence/replay disabled),
//! since that is what a `127.0.0.1:0` + mock-provider setup can
//! exercise without a real ClickHouse dependency.

use apexes_core::{AssetClass, Instrument, MarketEvent, MarketType, TradeEvent, Venue};
use apexes_market_data_gateway::Gateway;
use apexes_message_bus::test_support::MockMarketDataProvider;
use futures::StreamExt;
use rust_decimal_macros::dec;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

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

#[tokio::test]
async fn a_trade_pushed_upstream_is_received_end_to_end_by_a_ws_client() {
    let mock = MockMarketDataProvider::new();
    let handle = mock.handle();
    let gateway = Arc::new(Gateway::new(Box::new(mock), 16));
    gateway
        .connect(&[btc_usdt()])
        .await
        .expect("connect should succeed against the mock provider");

    let app = apexes_market_data_gateway::build_app(Arc::clone(&gateway), None, vec![btc_usdt()]);

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind should succeed");
    let addr = listener.local_addr().expect("local_addr should succeed");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("axum::serve should not exit with an error in this test");
    });

    let url = format!("ws://{addr}/market");
    let (mut ws_stream, _response) = tokio_tungstenite::connect_async(url)
        .await
        .expect("client should connect to /market");

    // No `subscribe_call_count` polling needed here (unlike
    // `apexes-ui`'s equivalent test): `Gateway::subscribe()` (called by
    // this crate's `market_handler` on WS upgrade) is a plain broadcast
    // subscribe with no upstream side effect to wait for -- the
    // upstream mock's `subscribe()` was already called once, at
    // `gateway.connect()` above, before the listener was even bound.
    // A short sleep still lets the server's `on_upgrade` callback
    // actually run and call `Gateway::subscribe()` before the push
    // below, so the event isn't sent to a broadcast channel with zero
    // live receivers yet.
    tokio::time::sleep(Duration::from_millis(50)).await;

    handle.push_event(MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        price: dec!(65000),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: chrono::Utc::now(),
    }));

    let received = tokio::time::timeout(Duration::from_secs(5), ws_stream.next())
        .await
        .expect("should receive a frame within 5 seconds")
        .expect("stream should not end")
        .expect("frame should not be a WS-level error");

    let Message::Text(text) = received else {
        panic!("expected a text frame, got {received:?}");
    };
    let event: MarketEvent =
        serde_json::from_str(&text).expect("frame should deserialize as a MarketEvent");
    match event {
        MarketEvent::Trade(trade) => {
            assert_eq!(trade.symbol, "BTCUSDT");
            assert_eq!(trade.price, dec!(65000));
        }
        other => panic!("expected a Trade event, got {other:?}"),
    }
}

#[tokio::test]
async fn a_client_requesting_replay_without_a_storage_reader_gets_closed_gracefully() {
    // `storage_reader: None` (persistence/replay disabled) + a client
    // that supplies `?replay_from=` must close the socket gracefully
    // (see `server::handle_socket`'s doc comment), never panic the
    // server task.
    let mock = MockMarketDataProvider::new();
    let gateway = Arc::new(Gateway::new(Box::new(mock), 16));
    gateway
        .connect(&[btc_usdt()])
        .await
        .expect("connect should succeed against the mock provider");

    let app = apexes_market_data_gateway::build_app(Arc::clone(&gateway), None, vec![btc_usdt()]);

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind should succeed");
    let addr = listener.local_addr().expect("local_addr should succeed");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("axum::serve should not exit with an error in this test");
    });

    let url = format!("ws://{addr}/market?replay_from=2020-01-01T00:00:00Z");
    let (mut ws_stream, _response) = tokio_tungstenite::connect_async(url)
        .await
        .expect("client should connect to /market");

    let outcome = tokio::time::timeout(Duration::from_secs(5), ws_stream.next())
        .await
        .expect("server should close the socket within 5 seconds");
    // Either the stream ends (`None`) or yields a Close frame --
    // both are "closed gracefully", never a panic propagated to the
    // client as a connection reset.
    match outcome {
        None => {}
        Some(Ok(Message::Close(_))) => {}
        Some(other) => panic!("expected the socket to close gracefully, got {other:?}"),
    }
}

#[tokio::test]
async fn multiple_ws_clients_all_receive_the_same_broadcast_event() {
    let mock = MockMarketDataProvider::new();
    let handle = mock.handle();
    let gateway = Arc::new(Gateway::new(Box::new(mock), 16));
    gateway
        .connect(&[btc_usdt()])
        .await
        .expect("connect should succeed against the mock provider");

    let app = apexes_market_data_gateway::build_app(Arc::clone(&gateway), None, vec![btc_usdt()]);

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind should succeed");
    let addr = listener.local_addr().expect("local_addr should succeed");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("axum::serve should not exit with an error in this test");
    });

    let url = format!("ws://{addr}/market");
    let (mut client_a, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("client A should connect");
    let (mut client_b, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("client B should connect");

    tokio::time::sleep(Duration::from_millis(50)).await;

    handle.push_event(MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        price: dec!(70000),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: chrono::Utc::now(),
    }));

    for client in [&mut client_a, &mut client_b] {
        let received = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("should receive a frame within 5 seconds")
            .expect("stream should not end")
            .expect("frame should not be a WS-level error");
        let Message::Text(text) = received else {
            panic!("expected a text frame, got {received:?}");
        };
        let event: MarketEvent =
            serde_json::from_str(&text).expect("frame should deserialize as a MarketEvent");
        match event {
            MarketEvent::Trade(trade) => assert_eq!(trade.price, dec!(70000)),
            other => panic!("expected a Trade event, got {other:?}"),
        }
    }
}

/// The `/market` upgrade announces the gateway's Binance network in the
/// `x-apexes-network` header (engines refuse a gateway on the other
/// network), and announces nothing when no network was configured.
#[tokio::test]
async fn market_upgrade_announces_the_configured_network() {
    async fn upgrade_headers(
        network: Option<String>,
    ) -> tokio_tungstenite::tungstenite::http::HeaderMap {
        let mock = MockMarketDataProvider::new();
        let gateway = Arc::new(Gateway::new(Box::new(mock), 16));
        gateway.connect(&[btc_usdt()]).await.expect("connect");
        let app = apexes_market_data_gateway::build_app_for_network(
            Arc::clone(&gateway),
            None,
            vec![btc_usdt()],
            None,
            network,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        let (_ws, response) = tokio_tungstenite::connect_async(format!("ws://{addr}/market"))
            .await
            .expect("connect to /market");
        response.headers().clone()
    }

    let headers = upgrade_headers(Some("testnet".to_string())).await;
    assert_eq!(
        headers
            .get(apexes_market_data_gateway::NETWORK_HEADER)
            .and_then(|v| v.to_str().ok()),
        Some("testnet")
    );

    let headers = upgrade_headers(None).await;
    assert!(
        headers
            .get(apexes_market_data_gateway::NETWORK_HEADER)
            .is_none()
    );
}
