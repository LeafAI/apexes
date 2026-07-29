//! Live integration tests against a real ClickHouse instance.
//! Excluded from default `cargo test` -- run explicitly with:
//!   docker compose up -d
//!   cargo test -p apexes-storage -- --ignored

use apexes_core::{
    Bar, BarInterval, DepthUpdate, MarketEvent, PriceLevel, Side, Tick, TradeEvent, Venue,
};
use apexes_storage::{EventType, StorageReader, StorageWriter};
use chrono::Utc;
use rust_decimal_macros::dec;

const CLICKHOUSE_URL: &str = "http://localhost:8123";

/// Builds a symbol unique to this test invocation so re-running the suite
/// against a persistent (non-wiped) ClickHouse instance doesn't accumulate
/// rows from prior runs under the same symbol -- each test's exact-row-count
/// assertions require this for repeatability.
fn unique_symbol(prefix: &str, now: chrono::DateTime<Utc>) -> String {
    format!(
        "{prefix}{}",
        now.timestamp_nanos_opt().unwrap_or(now.timestamp())
    )
}

#[tokio::test]
#[ignore]
async fn write_and_read_back_trades() {
    let writer = StorageWriter::new(CLICKHOUSE_URL)
        .await
        .expect("StorageWriter::new should succeed with ClickHouse running locally");
    let reader = StorageReader::new(CLICKHOUSE_URL)
        .await
        .expect("StorageReader::new should succeed");

    let now = Utc::now();
    let symbol = unique_symbol("TESTTRADE", now);
    let events = vec![MarketEvent::Trade(TradeEvent {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        price: dec!(65000.12345),
        quantity: dec!(0.5),
        side: Some(Side::Buy),
        trade_id: Some("T1".to_string()),
        timestamp: now,
    })];

    writer
        .write_batch(&events)
        .await
        .expect("write_batch should succeed");

    let date = now.date_naive();
    let read_back = reader
        .read_batch(
            &symbol,
            Venue::Binance,
            date..(date.succ_opt().unwrap()),
            EventType::Trade,
        )
        .await
        .expect("read_batch should succeed");

    assert_eq!(read_back.len(), 1);
    let MarketEvent::Trade(trade) = &read_back[0] else {
        panic!("expected a Trade event");
    };
    assert_eq!(trade.symbol, symbol);
    assert_eq!(trade.price, dec!(65000.12345));
    assert_eq!(trade.quantity, dec!(0.5));
    assert_eq!(trade.side, Some(Side::Buy));
}

#[tokio::test]
#[ignore]
async fn write_and_read_back_ticks() {
    let writer = StorageWriter::new(CLICKHOUSE_URL).await.unwrap();
    let reader = StorageReader::new(CLICKHOUSE_URL).await.unwrap();

    let now = Utc::now();
    let symbol = unique_symbol("TESTTICK", now);
    let events = vec![MarketEvent::Tick(Tick {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        bid_price: dec!(64999.5),
        bid_size: dec!(1.5),
        ask_price: dec!(65000.5),
        ask_size: dec!(2.0),
        timestamp: now,
    })];

    writer.write_batch(&events).await.unwrap();

    let date = now.date_naive();
    let read_back = reader
        .read_batch(
            &symbol,
            Venue::Binance,
            date..(date.succ_opt().unwrap()),
            EventType::Tick,
        )
        .await
        .unwrap();

    assert_eq!(read_back.len(), 1);
    let MarketEvent::Tick(tick) = &read_back[0] else {
        panic!("expected a Tick event");
    };
    assert_eq!(tick.bid_price, dec!(64999.5));
    assert_eq!(tick.ask_price, dec!(65000.5));
}

#[tokio::test]
#[ignore]
async fn write_and_read_back_depth_with_nested_levels() {
    let writer = StorageWriter::new(CLICKHOUSE_URL).await.unwrap();
    let reader = StorageReader::new(CLICKHOUSE_URL).await.unwrap();

    let now = Utc::now();
    let symbol = unique_symbol("TESTDEPTH", now);
    let events = vec![MarketEvent::Depth(DepthUpdate {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        bids: vec![
            PriceLevel {
                price: dec!(64999.5),
                quantity: dec!(1.5),
            },
            PriceLevel {
                price: dec!(64998.0),
                quantity: dec!(3.0),
            },
        ],
        asks: vec![PriceLevel {
            price: dec!(65000.5),
            quantity: dec!(2.0),
        }],
        sequence: Some(42),
        is_snapshot: true,
        timestamp: now,
    })];

    writer.write_batch(&events).await.unwrap();

    let date = now.date_naive();
    let read_back = reader
        .read_batch(
            &symbol,
            Venue::Binance,
            date..(date.succ_opt().unwrap()),
            EventType::Depth,
        )
        .await
        .unwrap();

    assert_eq!(read_back.len(), 1);
    let MarketEvent::Depth(depth) = &read_back[0] else {
        panic!("expected a Depth event");
    };
    assert_eq!(depth.bids.len(), 2);
    assert_eq!(depth.bids[0].price, dec!(64999.5));
    assert_eq!(depth.bids[1].price, dec!(64998.0));
    assert_eq!(depth.asks.len(), 1);
    assert_eq!(depth.asks[0].price, dec!(65000.5));
    assert_eq!(depth.sequence, Some(42));
    assert!(depth.is_snapshot);
}

#[tokio::test]
#[ignore]
async fn write_and_read_back_bars() {
    let writer = StorageWriter::new(CLICKHOUSE_URL).await.unwrap();
    let reader = StorageReader::new(CLICKHOUSE_URL).await.unwrap();

    let now = Utc::now();
    let symbol = unique_symbol("TESTBAR", now);
    let events = vec![MarketEvent::Bar(Bar {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        open: dec!(65000),
        high: dec!(65200),
        low: dec!(64900),
        close: dec!(65100),
        volume: dec!(500.25),
        interval: BarInterval::OneMinute,
        open_time: now,
        close_time: now,
    })];

    writer.write_batch(&events).await.unwrap();

    let date = now.date_naive();
    let read_back = reader
        .read_batch(
            &symbol,
            Venue::Binance,
            date..(date.succ_opt().unwrap()),
            EventType::Bar,
        )
        .await
        .unwrap();

    assert_eq!(read_back.len(), 1);
    let MarketEvent::Bar(bar) = &read_back[0] else {
        panic!("expected a Bar event");
    };
    assert_eq!(bar.open, dec!(65000));
    assert_eq!(bar.volume, dec!(500.25));
    assert_eq!(bar.interval, BarInterval::OneMinute);
}

#[tokio::test]
#[ignore]
async fn write_batch_rejects_mixed_event_types() {
    let writer = StorageWriter::new(CLICKHOUSE_URL).await.unwrap();

    let now = Utc::now();
    let events = vec![
        MarketEvent::Trade(TradeEvent {
            symbol: "MIXEDTEST".to_string(),
            venue: Venue::Binance,
            price: dec!(1),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: now,
        }),
        MarketEvent::Tick(Tick {
            symbol: "MIXEDTEST".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(1),
            bid_size: dec!(1),
            ask_price: dec!(1),
            ask_size: dec!(1),
            timestamp: now,
        }),
    ];

    let result = writer.write_batch(&events).await;
    assert!(
        result.is_err(),
        "write_batch should reject a mixed-variant batch"
    );
}
