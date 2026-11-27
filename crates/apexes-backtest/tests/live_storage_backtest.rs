//! Live integration test proving `StorageMarketDataSource` (backed by a
//! real ClickHouse instance via `apexes-storage`) feeds correctly into
//! `run_backtest`. Excluded from default `cargo test` -- run explicitly
//! with:
//!   docker compose up -d
//!   cargo test -p apexes-backtest -- --ignored

use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, OrderIntent, ReplaySpeed, StorageMarketDataSource,
    Strategy, StrategyContext, run_backtest,
};
use apexes_core::{AssetClass, Instrument, MarketEvent, MarketType, Side, TradeEvent, Venue};
use apexes_storage::{StorageReader, StorageWriter};
use chrono::Utc;
use rust_decimal_macros::dec;
use std::collections::HashMap;

const CLICKHOUSE_URL: &str = "http://localhost:8123";

struct NoOpStrategy;

impl Strategy for NoOpStrategy {
    fn on_market_event(
        &mut self,
        _event: &MarketEvent,
        _ctx: &StrategyContext,
    ) -> Vec<OrderIntent> {
        vec![]
    }
}

#[tokio::test]
#[ignore]
async fn backtest_reads_real_data_from_clickhouse_via_storage_market_data_source() {
    let writer = StorageWriter::new(CLICKHOUSE_URL)
        .await
        .expect("StorageWriter::new should succeed with ClickHouse running locally");
    let reader = StorageReader::new(CLICKHOUSE_URL)
        .await
        .expect("StorageReader::new should succeed");

    let now = Utc::now();
    let symbol = "BACKTESTLIVE";
    let events = vec![MarketEvent::Trade(TradeEvent {
        symbol: symbol.to_string(),
        venue: Venue::Binance,
        price: dec!(65000),
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
    let market_data = StorageMarketDataSource::new(
        reader,
        symbol,
        Venue::Binance,
        date..(date.succ_opt().unwrap()),
    );

    let instrument = Instrument::new(
        symbol,
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("BACKTEST".to_string()),
        Some("LIVE".to_string()),
        dec!(0.01),
        dec!(0.0001),
        dec!(1),
    )
    .unwrap();

    let mut initial_balances = HashMap::new();
    initial_balances.insert("LIVE".to_string(), dec!(10000));

    let config = BacktestConfig {
        instruments: vec![instrument],
        date_range: date..(date.succ_opt().unwrap()),
        initial_balances,
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
        margin_tiers: HashMap::new(),
        market_condition_config: None,
        control: None,
    };

    let registry = apexes_core::registry::Registry::new();
    let report = run_backtest(config, &registry, market_data, NoOpStrategy)
        .await
        .unwrap();

    // NoOpStrategy places no orders, so no trades -- but the run must
    // complete without error, and the equity curve must be non-empty,
    // proving the written Trade event was actually read from ClickHouse
    // and flowed through StorageMarketDataSource -> run_backtest (an
    // empty equity curve would mean zero events were processed).
    assert!(report.trades.is_empty());
    assert!(
        !report.equity_curve.is_empty(),
        "equity curve must be non-empty, proving at least one event flowed through the pipeline"
    );
    // NoOpStrategy never trades, so equity stays flat at the initial
    // 10,000 LIVE balance for every recorded point.
    assert!(
        report
            .equity_curve
            .iter()
            .all(|(_, equity)| *equity == dec!(10000)),
        "with no orders, equity must stay flat at the initial balance"
    );
}

/// CI uses a disposable ClickHouse service and a unique synthetic run ID.
/// No exchange access, no deletion or filtering of surplus rows.
#[tokio::test]
#[ignore]
async fn qualifies_complete_raw_capture_and_rejects_unreceipted_rows() {
    use apexes_backtest::collector_receipts::{QualificationMethod, qualify_recorded_run};
    use apexes_core::{DepthSourceRange, DepthUpdate, PriceLevel};
    use apexes_storage::raw_depth_store::{RawDepthBatch, RawDepthReader, RawDepthStore};
    use serde_json::json;
    use sha2::{Digest, Sha256};

    let writer = RawDepthStore::new(CLICKHOUSE_URL).await.unwrap();
    let reader = RawDepthReader::new(CLICKHOUSE_URL);
    let run = uuid::Uuid::new_v4().to_string();
    let now = Utc::now();
    let snap = DepthUpdate {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        bids: vec![PriceLevel {
            price: dec!(99),
            quantity: dec!(1),
        }],
        asks: vec![PriceLevel {
            price: dec!(101),
            quantity: dec!(1),
        }],
        sequence: Some(100),
        source_range: None,
        is_snapshot: true,
        timestamp: now,
    };
    let delta = DepthUpdate {
        sequence: Some(103),
        source_range: Some(DepthSourceRange {
            first: 101,
            last: 103,
            previous_final: None,
        }),
        is_snapshot: false,
        ..snap.clone()
    };
    let events = vec![MarketEvent::Depth(snap), MarketEvent::Depth(delta)];
    let first = RawDepthBatch::new(&run, 0, &events[..1]).unwrap();
    let second = RawDepthBatch::new(&run, 1, &events[1..]).unwrap();
    let mut lines = vec![
        json!({"kind":"header","schema_version":1,"run_id":run,"symbol":"BTCUSDT","market":"spot","started_at":now}),
    ];
    for (i, row) in [&first, &second].into_iter().enumerate() {
        lines.push(json!({"kind":"intent","batch_index":i,"expected_count":1,"payload_sha256":format!("{:x}",Sha256::digest(row.events_json.as_bytes())),"invalidations":0}));
        lines.push(json!({"kind":"commit","batch_index":i}));
    }
    lines.push(json!({"kind":"terminal","stopped":true,"reason":null,"committed_batches":2}));
    let journal = lines
        .iter()
        .map(|line| format!("{line}\n"))
        .collect::<String>();
    assert!(qualify_recorded_run(&journal, &reader).await.is_err());
    writer.write_batch(&second).await.unwrap();
    assert!(qualify_recorded_run(&journal, &reader).await.is_err());
    writer.write_batch(&first).await.unwrap();
    // An unrelated run cannot contaminate this query.
    let other = RawDepthBatch::new(&uuid::Uuid::new_v4().to_string(), 0, &events).unwrap();
    writer.write_batch(&other).await.unwrap();
    let qualified = qualify_recorded_run(&journal, &reader).await.unwrap();
    assert_eq!(
        qualified.qualification_method,
        QualificationMethod::DirectClickHouseRead
    );
    assert_eq!(qualified.events, events);
    assert!(!qualified.dataset.upstream_completeness_verified);
    let dir = std::env::temp_dir().join(format!("apexes-qualified-{run}"));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("journal.jsonl");
    std::fs::write(&path, &journal).unwrap();
    let run_cli = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_historical-manifest"))
            .arg("qualify-collector")
            .arg(&path)
            .env("APEXES_CAPTURE_CLICKHOUSE_URL", CLICKHOUSE_URL)
            .output()
            .unwrap()
    };
    let output = run_cli();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual, serde_json::to_value(&qualified).unwrap());
    // Exercise the same serialized output an operator will replay.
    let bundle_path = dir.join("qualified.json");
    let instrument_path = dir.join("instrument.json");
    std::fs::write(&bundle_path, &output.stdout).unwrap();
    let instrument = Instrument::new(
        "BTCUSDT",
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("BTC".into()),
        Some("USDT".into()),
        dec!(0.01),
        dec!(0.00001),
        dec!(1),
    )
    .unwrap();
    std::fs::write(&instrument_path, serde_json::to_vec(&instrument).unwrap()).unwrap();
    let smoke = std::process::Command::new(env!("CARGO_BIN_EXE_historical-manifest"))
        .arg("verify-collector-replay")
        .arg(&bundle_path)
        .arg(&instrument_path)
        .output()
        .unwrap();
    assert!(
        smoke.status.success(),
        "{}",
        String::from_utf8_lossy(&smoke.stderr)
    );
    let smoke: serde_json::Value = serde_json::from_slice(&smoke.stdout).unwrap();
    assert_eq!(smoke["events_observed"], 2);
    assert_eq!(smoke["fills"], 0);
    assert_eq!(smoke["dataset"], actual["dataset"]);
    // A high unexpected index must be visible even though it exceeds
    // the claimed committed count.
    writer
        .write_batch(&RawDepthBatch::new(&run, 999, &events).unwrap())
        .await
        .unwrap();
    assert!(qualify_recorded_run(&journal, &reader).await.is_err());
    let output = run_cli();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    std::fs::remove_dir_all(dir).unwrap();
    // A separate run proves that duplicate identities cannot be hidden.
    let duplicate_run = uuid::Uuid::new_v4().to_string();
    let duplicate = RawDepthBatch::new(&duplicate_run, 0, &events).unwrap();
    writer.write_batch(&duplicate).await.unwrap();
    writer.write_batch(&duplicate).await.unwrap();
    assert!(reader.read_run(&duplicate_run, 1).await.is_err());
}
