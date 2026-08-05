//! Live integration tests against a real ClickHouse instance, exercising
//! `replay_since` and `PersistenceBuffer` against `apexes-storage`'s real
//! backend rather than a fake. Excluded from default `cargo test` --
//! run explicitly with:
//!   docker compose up -d
//!   cargo test -p apexes-market-data-gateway -- --ignored
//!
//! Mirrors `apexes-storage`'s own `tests/live_clickhouse.rs` structural
//! pattern: a `CLICKHOUSE_URL` const, `#[ignore]` on each test,
//! `StorageWriter::new`/`StorageReader::new(CLICKHOUSE_URL).await.unwrap()`
//! at the top of each test.

use apexes_core::{AssetClass, Instrument, MarketEvent, MarketType, Tick, TradeEvent, Venue};
use apexes_market_data_gateway::persistence::PersistenceBuffer;
use apexes_market_data_gateway::replay::replay_since;
use apexes_storage::{StorageReader, StorageWriter};
use chrono::{Duration as ChronoDuration, Utc};
use rust_decimal_macros::dec;
use std::time::Duration;
use tokio::time::sleep;

const CLICKHOUSE_URL: &str = "http://localhost:8123";

/// Builds a symbol unique to this test invocation so re-running the
/// suite against a persistent (non-wiped) ClickHouse instance doesn't
/// accumulate rows from prior runs under the same symbol -- mirrors
/// `apexes-storage`'s own helper of the identical name and purpose.
fn unique_symbol(prefix: &str, now: chrono::DateTime<Utc>) -> String {
    format!(
        "{prefix}{}",
        now.timestamp_nanos_opt().unwrap_or(now.timestamp())
    )
}

fn instrument_for(symbol: &str) -> Instrument {
    Instrument::new(
        symbol,
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("BTC".to_string()),
        Some("USDT".to_string()),
        dec!(0.01),
        dec!(0.0001),
        dec!(1),
    )
    .unwrap_or_else(|err| panic!("valid instrument: {err}"))
}

#[tokio::test]
#[ignore]
async fn replay_since_finds_a_trade_persisted_earlier_today() {
    let writer = StorageWriter::new(CLICKHOUSE_URL)
        .await
        .expect("StorageWriter::new should succeed with ClickHouse running locally");
    let reader = StorageReader::new(CLICKHOUSE_URL)
        .await
        .expect("StorageReader::new should succeed");

    let now = Utc::now();
    let symbol = unique_symbol("REPLAYTRADE", now);
    let events = vec![MarketEvent::Trade(TradeEvent {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        price: dec!(65000),
        quantity: dec!(0.5),
        side: None,
        trade_id: Some("R1".to_string()),
        timestamp: now,
    })];
    writer
        .write_batch(&events)
        .await
        .expect("write_batch should succeed");

    let replay_from = now - ChronoDuration::hours(1);
    let replayed = replay_since(&reader, &[instrument_for(&symbol)], replay_from)
        .await
        .expect("replay_since should succeed");

    assert_eq!(replayed.len(), 1);
    let MarketEvent::Trade(trade) = &replayed[0] else {
        panic!("expected a Trade event");
    };
    assert_eq!(trade.symbol, symbol);
    assert_eq!(trade.price, dec!(65000));
}

#[tokio::test]
#[ignore]
async fn replay_since_merges_multiple_event_types_by_timestamp() {
    let writer = StorageWriter::new(CLICKHOUSE_URL).await.unwrap();
    let reader = StorageReader::new(CLICKHOUSE_URL).await.unwrap();

    let now = Utc::now();
    let symbol = unique_symbol("REPLAYMERGE", now);

    let earlier_tick_bid_price = dec!(64999);
    let later_trade_price = dec!(65000);
    let earlier_tick = MarketEvent::Tick(Tick {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        bid_price: earlier_tick_bid_price,
        bid_size: dec!(1),
        ask_price: dec!(65001),
        ask_size: dec!(1),
        timestamp: now - ChronoDuration::seconds(10),
    });
    let later_trade = MarketEvent::Trade(TradeEvent {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        price: later_trade_price,
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: now,
    });

    // Written as two separate homogeneous batches (write_batch itself
    // requires this) but must come back merged by timestamp.
    writer.write_batch(&[earlier_tick]).await.unwrap();
    writer.write_batch(&[later_trade]).await.unwrap();

    let replay_from = now - ChronoDuration::hours(1);
    let replayed = replay_since(&reader, &[instrument_for(&symbol)], replay_from)
        .await
        .expect("replay_since should succeed");

    // Field-by-field comparison, not full struct `==`: ClickHouse's
    // DateTime64(3) column truncates timestamps to millisecond
    // precision on round-trip (matching `apexes-storage`'s own test
    // precedent of asserting on individual fields rather than whole-
    // struct equality against a `DateTime<Utc>` with sub-millisecond
    // precision).
    assert_eq!(replayed.len(), 2);
    let MarketEvent::Tick(tick) = &replayed[0] else {
        panic!(
            "expected the earlier Tick to come first, got {:?}",
            replayed[0]
        );
    };
    assert_eq!(tick.symbol, symbol);
    assert_eq!(tick.bid_price, earlier_tick_bid_price);

    let MarketEvent::Trade(trade) = &replayed[1] else {
        panic!(
            "expected the later Trade to come second, got {:?}",
            replayed[1]
        );
    };
    assert_eq!(trade.symbol, symbol);
    assert_eq!(trade.price, later_trade_price);
}

#[tokio::test]
#[ignore]
async fn persistence_buffer_flushes_interleaved_variants_without_mixed_batch_errors() {
    // The core correctness property `PersistenceBuffer` exists to
    // guarantee: even when fed a strictly interleaved Trade/Tick/
    // Trade/Tick sequence, every underlying `write_batch` call must
    // stay homogeneous (never trigger `StorageError::MixedBatch`,
    // which would surface as a `write_batch` failure logged via
    // `tracing::error!` inside the spawned flush task -- this test
    // instead asserts on the OBSERVABLE side effect: every written
    // event is later readable back, proving no batch was silently
    // dropped due to a mixing error).
    let writer = StorageWriter::new(CLICKHOUSE_URL).await.unwrap();
    let reader = StorageReader::new(CLICKHOUSE_URL).await.unwrap();

    let now = Utc::now();
    let symbol = unique_symbol("PBUFMIX", now);

    // max_batch_size = 2 so each variant's 2nd event triggers an
    // immediate size-based flush.
    let mut buffer = PersistenceBuffer::new(writer, 2, Duration::from_secs(3600));

    let trade_1 = MarketEvent::Trade(TradeEvent {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        price: dec!(1),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: now,
    });
    let tick_1 = MarketEvent::Tick(Tick {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        bid_price: dec!(1),
        bid_size: dec!(1),
        ask_price: dec!(2),
        ask_size: dec!(1),
        timestamp: now,
    });
    let trade_2 = MarketEvent::Trade(TradeEvent {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        price: dec!(2),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: now,
    });
    let tick_2 = MarketEvent::Tick(Tick {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        bid_price: dec!(2),
        bid_size: dec!(1),
        ask_price: dec!(3),
        ask_size: dec!(1),
        timestamp: now,
    });

    // Strictly interleaved: Trade, Tick, Trade, Tick.
    buffer.record(&trade_1).await;
    buffer.record(&tick_1).await;
    buffer.record(&trade_2).await; // 2nd Trade -> triggers size-flush
    buffer.record(&tick_2).await; // 2nd Tick -> triggers size-flush

    // Flushes are spawned tasks; give them a moment to complete
    // against the real ClickHouse instance.
    sleep(Duration::from_millis(500)).await;

    let date = now.date_naive();
    let trades = reader
        .read_batch(
            &symbol,
            Venue::Binance,
            date..(date.succ_opt().unwrap()),
            apexes_storage::reader::EventType::Trade,
        )
        .await
        .unwrap();
    let ticks = reader
        .read_batch(
            &symbol,
            Venue::Binance,
            date..(date.succ_opt().unwrap()),
            apexes_storage::reader::EventType::Tick,
        )
        .await
        .unwrap();

    assert_eq!(
        trades.len(),
        2,
        "both Trade events should have been flushed"
    );
    assert_eq!(ticks.len(), 2, "both Tick events should have been flushed");
}

#[tokio::test]
#[ignore]
async fn persistence_buffer_flush_aged_flushes_a_bucket_below_size_threshold() {
    let writer = StorageWriter::new(CLICKHOUSE_URL).await.unwrap();
    let reader = StorageReader::new(CLICKHOUSE_URL).await.unwrap();

    let now = Utc::now();
    let symbol = unique_symbol("PBUFAGE", now);

    // max_batch_size large enough that the single event below never
    // triggers a size-flush; max_batch_age short enough that
    // flush_aged picks it up quickly.
    let mut buffer = PersistenceBuffer::new(writer, 100, Duration::from_millis(50));

    let trade = MarketEvent::Trade(TradeEvent {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        price: dec!(1),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: now,
    });
    buffer.record(&trade).await;

    sleep(Duration::from_millis(100)).await;
    buffer.flush_aged().await;
    sleep(Duration::from_millis(500)).await;

    let date = now.date_naive();
    let trades = reader
        .read_batch(
            &symbol,
            Venue::Binance,
            date..(date.succ_opt().unwrap()),
            apexes_storage::reader::EventType::Trade,
        )
        .await
        .unwrap();

    assert_eq!(
        trades.len(),
        1,
        "flush_aged should have flushed the below-size-threshold bucket"
    );
}
