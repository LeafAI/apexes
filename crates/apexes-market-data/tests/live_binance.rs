//! Live integration test against real Binance WebSocket endpoints.
//! Excluded from default `cargo test` -- run explicitly with:
//!   cargo test -p apexes-market-data -- --ignored

use apexes_core::{AssetClass, Instrument, MarketDataProvider, MarketEvent, MarketType, Venue};
use apexes_market_data::BinanceMarketDataProvider;
use futures::StreamExt;
use rust_decimal_macros::dec;
use std::time::Duration;

#[tokio::test]
#[ignore]
async fn subscribing_to_btcusdt_produces_all_four_market_event_variants() {
    let provider = BinanceMarketDataProvider::new();
    let instrument = Instrument::new(
        "BTCUSDT",
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("BTC".to_string()),
        Some("USDT".to_string()),
        dec!(0.01),
        dec!(0.00001),
        dec!(1),
    )
    .unwrap();

    let mut stream = provider
        .subscribe(&[instrument])
        .await
        .expect("subscribe should succeed for a valid Binance Spot instrument");

    let mut saw_trade = false;
    let mut saw_tick = false;
    let mut saw_depth = false;
    let mut saw_bar = false;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);

    while tokio::time::Instant::now() < deadline && !(saw_trade && saw_tick && saw_depth && saw_bar)
    {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(event)) => match event {
                MarketEvent::Trade(_) => saw_trade = true,
                MarketEvent::Tick(_) => saw_tick = true,
                MarketEvent::Depth(_) => saw_depth = true,
                MarketEvent::Bar(_) => saw_bar = true,
                MarketEvent::MarkPrice(_) => {} // Spot-only test; MarkPrice never emitted for Spot instruments.
            },
            Ok(None) => break,
            Err(_timeout) => break,
        }
    }

    assert!(
        saw_trade,
        "expected at least one MarketEvent::Trade within 90s"
    );
    assert!(
        saw_tick,
        "expected at least one MarketEvent::Tick within 90s"
    );
    assert!(
        saw_depth,
        "expected at least one MarketEvent::Depth within 90s"
    );
    assert!(
        saw_bar,
        "expected at least one MarketEvent::Bar within 90s (requires at least one OneSecond interval boundary crossed with a trade before it)"
    );
}

#[tokio::test]
#[ignore]
async fn subscribing_to_a_perpetual_instrument_produces_mark_price_events() {
    let provider = BinanceMarketDataProvider::new();
    let instrument = Instrument::new(
        "BTCUSDT",
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Perpetual,
        Some("BTC".to_string()),
        Some("USDT".to_string()),
        dec!(0.01),
        dec!(0.001),
        dec!(1),
    )
    .unwrap();

    let mut stream = provider
        .subscribe(&[instrument])
        .await
        .expect("subscribe should succeed for a valid Binance Perpetual instrument");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut saw_mark_price = false;

    while tokio::time::Instant::now() < deadline && !saw_mark_price {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(MarketEvent::MarkPrice(_))) => saw_mark_price = true,
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_timeout) => break,
        }
    }

    assert!(
        saw_mark_price,
        "expected at least one MarketEvent::MarkPrice within 30s"
    );
}
