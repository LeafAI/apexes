use apexes_backtest::historical_dataset::{
    DatasetDefinition, DatasetManifest, DatasetStream, StreamKind, seal_dataset,
};
use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, InMemoryMarketDataSource, ReplaySpeed, run_backtest,
};
use apexes_core::{AssetClass, Instrument, MarketEvent, MarketType, Side, Tick, TradeEvent, Venue};
use apexes_strategy::{OrderIntent, Strategy, StrategyContext};
use chrono::{NaiveDate, TimeZone, Utc};
use rust_decimal_macros::dec;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

fn day(n: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 1, n).unwrap()
}

fn definition() -> DatasetDefinition {
    DatasetDefinition {
        dataset_id: "historical-fixture".into(),
        version: "v1".into(),
        source_id: "synthetic export, not exchange attestation".into(),
        start_day: day(1),
        end_day_exclusive: day(4),
        streams: [StreamKind::Trade, StreamKind::Tick]
            .into_iter()
            .map(|kind| DatasetStream {
                venue: Venue::Binance,
                symbol: "BTCUSDT".into(),
                kind,
            })
            .collect(),
    }
}

fn trade(n: u32, id: Option<&str>) -> MarketEvent {
    MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        price: dec!(100),
        quantity: dec!(1),
        side: Some(Side::Buy),
        trade_id: id.map(str::to_owned),
        timestamp: Utc.with_ymd_and_hms(2026, 1, n, 0, 0, 1).unwrap(),
    })
}

fn tick(n: u32) -> MarketEvent {
    MarketEvent::Tick(Tick {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        bid_price: dec!(99),
        bid_size: dec!(1),
        ask_price: dec!(101),
        ask_size: dec!(2),
        timestamp: Utc.with_ymd_and_hms(2026, 1, n, 0, 0, 1).unwrap(),
    })
}

fn events() -> Vec<MarketEvent> {
    vec![trade(1, Some("one")), tick(1), trade(3, Some("two"))]
}

#[test]
fn seals_all_stream_days_and_preserves_empty_partition_evidence() {
    let manifest = seal_dataset(definition(), &events()).unwrap();
    assert_eq!(manifest.partitions.len(), 6);
    assert_eq!(manifest.partitions[0].event_count, 1);
    assert_eq!(manifest.partitions[1].day, day(2));
    assert_eq!(manifest.partitions[1].event_count, 0);
    assert_eq!(
        manifest.partitions[1].sha256,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    let verified = manifest.verify(&events()).unwrap();
    assert_eq!(verified.event_count, 3);
    assert_eq!(verified.empty_partitions, 3);
    assert!(!verified.upstream_completeness_verified);
    assert_eq!(manifest, seal_dataset(definition(), &events()).unwrap());
    let roundtrip: DatasetManifest =
        serde_json::from_str(&serde_json::to_string(&manifest).unwrap()).unwrap();
    assert_eq!(roundtrip.verify(&events()).unwrap(), verified);
}

#[test]
fn rejects_missing_duplicate_or_substituted_partitions() {
    let original = seal_dataset(definition(), &events()).unwrap();
    let mut manifest = original.clone();
    manifest.partitions.remove(1);
    assert!(manifest.verify(&events()).is_err());
    let mut manifest = original.clone();
    manifest.partitions[1] = manifest.partitions[0].clone();
    assert!(manifest.verify(&events()).is_err());
    let mut manifest = original;
    manifest.partitions[1].day = day(4);
    assert!(manifest.verify(&events()).is_err());
}

#[test]
fn detects_truncation_insertions_and_payload_tampering() {
    let manifest = seal_dataset(definition(), &events()).unwrap();
    let mut changed = events();
    changed.pop();
    assert!(manifest.verify(&changed).unwrap_err().contains("partition"));
    let mut changed = events();
    changed.insert(2, trade(2, Some("unexpected")));
    assert!(manifest.verify(&changed).unwrap_err().contains("partition"));
    let mut changed = events();
    let MarketEvent::Trade(trade) = &mut changed[0] else {
        unreachable!()
    };
    trade.price = dec!(101);
    assert!(
        manifest
            .verify(&changed)
            .unwrap_err()
            .contains("digest mismatch")
    );
}

#[test]
fn detects_cross_stream_tie_reordering_without_sorting_away_evidence() {
    let manifest = seal_dataset(definition(), &events()).unwrap();
    let mut changed = events();
    changed.swap(0, 1);
    assert_eq!(
        seal_dataset(definition(), &changed).unwrap().partitions,
        manifest.partitions
    );
    assert!(
        manifest
            .verify(&changed)
            .unwrap_err()
            .contains("ordered replay digest")
    );
}

#[test]
fn refuses_out_of_scope_events_regressions_and_bad_definitions() {
    let mut changed = events();
    changed.push(trade(4, Some("outside")));
    assert!(seal_dataset(definition(), &changed).is_err());
    let mut changed = events();
    changed.swap(0, 2);
    assert!(seal_dataset(definition(), &changed).is_err());
    let mut changed = events();
    let MarketEvent::Trade(t) = &mut changed[0] else {
        unreachable!()
    };
    t.symbol = "ETHUSDT".into();
    assert!(seal_dataset(definition(), &changed).is_err());
    let mut def = definition();
    def.streams.push(def.streams[0].clone());
    assert!(seal_dataset(def, &events()).is_err());
    let mut def = definition();
    def.end_day_exclusive = def.start_day;
    assert!(seal_dataset(def, &events()).is_err());
}

#[test]
fn duplicate_identified_trades_fail_but_anonymous_records_are_not_deduplicated() {
    let mut changed = events();
    changed.push(trade(3, Some("one")));
    assert!(
        seal_dataset(definition(), &changed)
            .unwrap_err()
            .contains("duplicate identified trade")
    );
    let anonymous = vec![trade(1, None), trade(1, None)];
    let manifest = seal_dataset(definition(), &anonymous).unwrap();
    assert_eq!(manifest.verify(&anonymous).unwrap().event_count, 2);
}

#[test]
fn rejects_incompatible_schema_and_malformed_digest() {
    let mut manifest = seal_dataset(definition(), &events()).unwrap();
    manifest.schema_version = 2;
    assert!(manifest.verify(&events()).is_err());
    manifest.schema_version = 1;
    manifest.replay_sha256 = "0".repeat(63);
    assert!(manifest.verify(&events()).is_err());
}

struct CountEvents(Arc<AtomicUsize>);
impl Strategy for CountEvents {
    fn on_market_event(&mut self, _: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
        self.0.fetch_add(1, Ordering::Relaxed);
        vec![]
    }
}

fn config() -> BacktestConfig {
    BacktestConfig {
        instruments: vec![
            Instrument::new(
                "BTCUSDT",
                MarketType::Crypto,
                Venue::Binance,
                AssetClass::Spot,
                Some("BTC".into()),
                Some("USDT".into()),
                dec!(0.01),
                dec!(0.001),
                dec!(1),
            )
            .unwrap(),
        ],
        date_range: day(1)..day(4),
        initial_balances: HashMap::from([("USDT".into(), dec!(1000))]),
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: dec!(0),
        risk_free_return: dec!(0),
        margin_tiers: HashMap::new(),
        market_condition_config: None,
        control: None,
    }
}

fn registry(manifest: DatasetManifest) -> apexes_core::registry::Registry {
    let registry = apexes_core::registry::Registry::new();
    registry
        .register::<DatasetManifest, ()>((), manifest)
        .unwrap();
    registry
}

#[tokio::test]
async fn verified_replay_records_manifest_in_report_and_legacy_is_unverified() {
    let manifest = seal_dataset(definition(), &events()).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let report = run_backtest(
        config(),
        &registry(manifest.clone()),
        InMemoryMarketDataSource::new(events()),
        CountEvents(count.clone()),
    )
    .await
    .unwrap();
    assert_eq!(count.load(Ordering::Relaxed), 3);
    assert_eq!(report.historical_dataset.unwrap().manifest, manifest);
    let report = run_backtest(
        config(),
        &apexes_core::registry::Registry::new(),
        InMemoryMarketDataSource::new(events()),
        CountEvents(count),
    )
    .await
    .unwrap();
    assert!(report.historical_dataset.is_none());
    let mut legacy = serde_json::to_value(&report).unwrap();
    legacy.as_object_mut().unwrap().remove("historical_dataset");
    let legacy: apexes_backtest::BacktestReport = serde_json::from_value(legacy).unwrap();
    assert!(legacy.historical_dataset.is_none());
}

#[tokio::test]
async fn mismatched_manifest_or_scope_refuses_replay_before_strategy_runs() {
    let manifest = seal_dataset(definition(), &events()).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let mut altered = events();
    altered.pop();
    let result = run_backtest(
        config(),
        &registry(manifest.clone()),
        InMemoryMarketDataSource::new(altered),
        CountEvents(count.clone()),
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("historical dataset verification failed")
    );
    let mut bad_config = config();
    bad_config.date_range.end = day(3);
    let result = run_backtest(
        bad_config,
        &registry(manifest),
        InMemoryMarketDataSource::new(events()),
        CountEvents(count.clone()),
    )
    .await;
    assert!(result.unwrap_err().to_string().contains("dataset scope"));
    assert_eq!(count.load(Ordering::Relaxed), 0);
}

#[test]
fn cli_seals_and_verifies_local_export_without_modifying_it() {
    let dir = std::env::temp_dir().join(format!("apexes-historical-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let definition_path = dir.join("definition.json");
    let events_path = dir.join("events.json");
    let manifest_path = dir.join("manifest.json");
    let original = serde_json::to_vec(&events()).unwrap();
    std::fs::write(&definition_path, serde_json::to_vec(&definition()).unwrap()).unwrap();
    std::fs::write(&events_path, &original).unwrap();
    let sealed = std::process::Command::new(env!("CARGO_BIN_EXE_historical-manifest"))
        .arg("seal")
        .arg(&definition_path)
        .arg(&events_path)
        .output()
        .unwrap();
    assert!(
        sealed.status.success(),
        "{}",
        String::from_utf8_lossy(&sealed.stderr)
    );
    std::fs::write(&manifest_path, &sealed.stdout).unwrap();
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_historical-manifest"))
        .arg("verify")
        .arg(&manifest_path)
        .arg(&events_path)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let output: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(output["event_count"], 3);
    assert_eq!(output["upstream_completeness_verified"], false);
    assert_eq!(std::fs::read(events_path).unwrap(), original);
    std::fs::remove_dir_all(dir).unwrap();
}
