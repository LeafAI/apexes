//! Live integration test: subscribes to real Binance market data and
//! ingests it into a real local ClickHouse instance via
//! `run_ingestion_loop`. Excluded from default `cargo test` -- run
//! explicitly:
//!   docker compose up -d clickhouse
//!   cargo test -p apexes-market-data --test live_ingestion -- --ignored

use apexes_core::{AssetClass, Instrument, MarketType, Venue};
use apexes_market_data::BinanceMarketDataProvider;
use apexes_market_data::ingestion::run_ingestion_loop;
use apexes_storage::{EventType, StorageReader, StorageWriter};
use rust_decimal_macros::dec;
use std::time::Duration;

const CLICKHOUSE_URL: &str = "http://localhost:8123";

#[tokio::test]
#[ignore]
async fn ingests_real_binance_trades_into_clickhouse() {
    let provider = BinanceMarketDataProvider::new();
    let writer = StorageWriter::new(CLICKHOUSE_URL)
        .await
        .expect("StorageWriter::new should succeed with ClickHouse running locally");
    let reader = StorageReader::new(CLICKHOUSE_URL)
        .await
        .expect("StorageReader::new should succeed");

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

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let ingestion_handle = tokio::spawn(async move {
        run_ingestion_loop(&provider, &writer, &[instrument], stop_rx).await
    });

    // Let ingestion run long enough to accumulate at least one trade
    // and cross the 5-second flush interval.
    tokio::time::sleep(Duration::from_secs(20)).await;
    let _ = stop_tx.send(());
    ingestion_handle
        .await
        .expect("ingestion task should not panic")
        .expect("run_ingestion_loop should return Ok on graceful stop");

    let today = chrono::Utc::now().date_naive();
    let read_back = reader
        .read_batch(
            "BTCUSDT",
            Venue::Binance,
            today..today.succ_opt().unwrap(),
            EventType::Trade,
        )
        .await
        .expect("read_batch should succeed");

    assert!(
        !read_back.is_empty(),
        "expected at least one ingested trade to be readable back from ClickHouse"
    );
}
