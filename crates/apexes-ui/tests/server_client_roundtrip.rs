//! Real end-to-end integration test: binds a real `127.0.0.1:0`
//! listener (mirroring the `TcpListener::bind("127.0.0.1:0")` pattern
//! already used in `crates/apexes-gateway-ibkr/tests/mock_tws.rs` and
//! `crates/apexes-market-data-chain/src/eth_rpc_provider.rs`), serves
//! the real `apexes_ui::server::build_app` router over it, runs the
//! real `apexes_ui::client::run` client task against that address,
//! and asserts a `MarketEvent` pushed through a mock upstream
//! provider is received end-to-end on the client side. Uses
//! `apexes_message_bus::test_support::MockMarketDataProvider`, not a
//! real Binance connection -- matches this project's established
//! "unit/integration tests never depend on real external services"
//! convention.

use apexes_core::{AssetClass, Instrument, MarketEvent, MarketType, TradeEvent, Venue};
use apexes_message_bus::MarketEventBus;
use apexes_message_bus::test_support::MockMarketDataProvider;
use apexes_ui::client::ConnectionStatus;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

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
async fn a_trade_pushed_upstream_is_received_end_to_end_by_the_client() {
    let mock = MockMarketDataProvider::new();
    let handle = mock.handle();
    let bus = Arc::new(MarketEventBus::new(Box::new(mock)));
    let app = apexes_ui::server::build_app(Arc::clone(&bus), vec![btc_usdt()]);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let (tx, mut rx) = mpsc::unbounded_channel::<MarketEvent>();
    let (status_tx, mut status_rx) = watch::channel(ConnectionStatus::default());
    let url = format!("ws://{addr}/ws");
    tokio::spawn(apexes_ui::client::run(url, tx, status_tx));

    // Wait for the client to report Connected, then poll the mock's
    // subscribe_call_count until the server's ws_handler has actually
    // called bus.subscribe(). Connected alone is NOT sufficient: it
    // fires the moment the WebSocket handshake completes, while axum
    // runs the on_upgrade callback (which performs the subscribe) on
    // a separately-scheduled task afterward -- pushing an event
    // before that subscribe lands would panic the mock.
    loop {
        status_rx.changed().await.unwrap();
        if *status_rx.borrow() == ConnectionStatus::Connected {
            break;
        }
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while handle.subscribe_call_count() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("server should have subscribed to the bus within 5 seconds of the client connecting");

    handle.push_event(MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        price: dec!(65000),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: chrono::Utc::now(),
    }));

    let received = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("should receive an event within 5 seconds")
        .expect("channel should not be closed");

    match received {
        MarketEvent::Trade(trade) => {
            assert_eq!(trade.symbol, "BTCUSDT");
            assert_eq!(trade.price, dec!(65000));
        }
        other => panic!("expected a Trade event, got {other:?}"),
    }
}
