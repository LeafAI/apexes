//! Live integration tests against a real ClickHouse instance.
//! Excluded from default `cargo test` -- run explicitly with:
//!   docker compose up -d
//!   cargo test -p apexes-storage -- --ignored

use apexes_core::{
    Bar, BarInterval, DepthUpdate, MarkPriceUpdate, MarketEvent, PriceLevel, Side, Tick,
    TradeEvent, Venue,
};
use apexes_storage::{EventType, StorageError, StorageReader, StorageWriter};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
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
    let mut events = vec![MarketEvent::Depth(DepthUpdate {
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
        source_range: None,
        is_snapshot: true,
        timestamp: now,
    })];

    let MarketEvent::Depth(mut delta) = events[0].clone() else {
        unreachable!()
    };
    delta.is_snapshot = false;
    delta.sequence = Some(47);
    delta.source_range = Some(apexes_core::DepthSourceRange {
        first: 43,
        last: 47,
        previous_final: Some(42),
    });
    // Keep both rows within the same UTC day, independent of wall clock.
    delta.timestamp = now;
    events.push(MarketEvent::Depth(delta.clone()));
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

    assert_eq!(read_back.len(), 2);
    let snapshot = read_back
        .iter()
        .find(|event| matches!(event,MarketEvent::Depth(d) if d.is_snapshot))
        .unwrap();
    let MarketEvent::Depth(depth) = snapshot else {
        panic!("expected a Depth event");
    };
    assert_eq!(depth.bids.len(), 2);
    assert_eq!(depth.bids[0].price, dec!(64999.5));
    assert_eq!(depth.bids[1].price, dec!(64998.0));
    assert_eq!(depth.asks.len(), 1);
    assert_eq!(depth.asks[0].price, dec!(65000.5));
    assert_eq!(depth.sequence, Some(42));
    assert!(depth.is_snapshot);
    assert_eq!(depth.source_range, None);
    let actual = read_back
        .iter()
        .find_map(|event| match event {
            MarketEvent::Depth(d) if !d.is_snapshot => Some(d),
            _ => None,
        })
        .unwrap();
    assert_eq!(actual.source_range, delta.source_range);
    assert_eq!(actual.sequence, delta.sequence);
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
async fn write_and_read_back_mark_prices() {
    let writer = StorageWriter::new(CLICKHOUSE_URL)
        .await
        .expect("StorageWriter::new should succeed with ClickHouse running locally");
    let reader = StorageReader::new(CLICKHOUSE_URL)
        .await
        .expect("StorageReader::new should succeed");

    let now = Utc::now();
    let symbol = unique_symbol("TESTMARKPRICE", now);
    let events = vec![MarketEvent::MarkPrice(MarkPriceUpdate {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        mark_price: dec!(65012.34567),
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
            EventType::MarkPrice,
        )
        .await
        .expect("read_batch should succeed");

    assert_eq!(read_back.len(), 1);
    let MarketEvent::MarkPrice(update) = &read_back[0] else {
        panic!("expected MarketEvent::MarkPrice");
    };
    assert_eq!(update.symbol, symbol);
    assert_eq!(update.mark_price, dec!(65012.34567));
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

#[tokio::test]
#[ignore]
async fn intraday_volume_curve_averages_across_lookback_days() {
    let writer = StorageWriter::new(CLICKHOUSE_URL).await.unwrap();
    let reader = StorageReader::new(CLICKHOUSE_URL).await.unwrap();

    // Seed 2 days of 1-hour bars for the same symbol/venue, each day
    // with the SAME hourly volume distribution (50% of daily volume in
    // the first bar, 50% in the second) so the averaged curve has an
    // exactly predictable shape regardless of which two days ClickHouse
    // picks up.
    let day1_bars = vec![
        test_bar(
            "EMSCURVE",
            Venue::Binance,
            "2026-01-01T00:00:00Z",
            dec!(100),
        ),
        test_bar(
            "EMSCURVE",
            Venue::Binance,
            "2026-01-01T01:00:00Z",
            dec!(100),
        ),
    ];
    let day2_bars = vec![
        test_bar(
            "EMSCURVE",
            Venue::Binance,
            "2026-01-02T00:00:00Z",
            dec!(200),
        ),
        test_bar(
            "EMSCURVE",
            Venue::Binance,
            "2026-01-02T01:00:00Z",
            dec!(200),
        ),
    ];
    let events: Vec<MarketEvent> = day1_bars
        .into_iter()
        .chain(day2_bars)
        .map(MarketEvent::Bar)
        .collect();
    writer.write_batch(&events).await.unwrap();

    let curve = reader
        .intraday_volume_curve("EMSCURVE", Venue::Binance, 2)
        .await
        .unwrap();

    // Both days split their volume 50/50 across the two bars, so the
    // averaged cumulative curve must be exactly [0.5, 1.0] regardless
    // of each day's absolute volume (100+100 vs 200+200) -- proving
    // this is a FRACTION-of-day curve, not an absolute-volume curve.
    assert_eq!(curve.cumulative_fractions.len(), 2);
    assert_eq!(curve.cumulative_fractions[0].1, dec!(0.5));
    assert_eq!(curve.cumulative_fractions[1].1, dec!(1.0));
}

#[tokio::test]
#[ignore]
async fn intraday_volume_curve_errors_on_insufficient_history() {
    let reader = StorageReader::new(CLICKHOUSE_URL).await.unwrap();

    // No bars seeded at all for this symbol -- 0 available days.
    let result = reader
        .intraday_volume_curve("EMSCURVE_NO_HISTORY", Venue::Binance, 5)
        .await;

    assert!(matches!(
        result,
        Err(StorageError::InsufficientHistory {
            available_days: 0,
            requested_days: 5,
        })
    ));
}

fn test_bar(symbol: &str, venue: Venue, open_time_rfc3339: &str, volume: Decimal) -> Bar {
    let open_time: DateTime<Utc> = open_time_rfc3339.parse().unwrap();
    Bar {
        symbol: symbol.to_string(),
        venue,
        open: dec!(100),
        high: dec!(100),
        low: dec!(100),
        close: dec!(100),
        volume,
        interval: BarInterval::OneHour,
        open_time,
        close_time: open_time + chrono::Duration::hours(1),
    }
}

#[tokio::test]
#[ignore]
async fn isolated_raw_depth_batch_preserves_payload_and_rejects_duplicates() {
    use apexes_storage::raw_depth_store::{RawDepthBatch, RawDepthStore};
    let store = RawDepthStore::new(CLICKHOUSE_URL).await.unwrap();
    let now = Utc::now();
    let run = unique_symbol("raw-run-", now);
    let event = MarketEvent::Depth(DepthUpdate {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        bids: vec![PriceLevel {
            price: dec!(0.123456789012),
            quantity: dec!(1),
        }],
        asks: vec![],
        sequence: Some(103),
        source_range: Some(apexes_core::DepthSourceRange {
            first: 101,
            last: 103,
            previous_final: Some(100),
        }),
        is_snapshot: false,
        timestamp: now,
    });
    let batch = RawDepthBatch::new(&run, 0, std::slice::from_ref(&event)).unwrap();
    assert!(store.read_batch(&run, 0).await.unwrap().is_none());
    store.write_batch(&batch).await.unwrap();
    let read = store.read_batch(&run, 0).await.unwrap().unwrap();
    assert_eq!(read.events_json, batch.events_json);
    assert_eq!(read.events().unwrap(), vec![event]);
    assert!(store.read_batch(&run, 1).await.unwrap().is_none());
    store.write_batch(&batch).await.unwrap();
    assert!(store.read_batch(&run, 0).await.is_err());
}
