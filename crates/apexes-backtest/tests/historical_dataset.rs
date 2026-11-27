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

use apexes_backtest::source_coverage::{
    CollectionWindow, SequenceContract, SequenceRange, SourceCoverage, StreamCoverage,
};

fn receipt(stream: &str, n: u32, count: u64, range: Option<(u64, u64)>) -> CollectionWindow {
    CollectionWindow {
        receipt_id: format!("{stream}-{n}"),
        start: day(n).and_hms_opt(0, 0, 0).unwrap().and_utc(),
        end_exclusive: day(n + 1).and_hms_opt(0, 0, 0).unwrap().and_utc(),
        complete: true,
        expected_count: count,
        sequence_range: range.map(|(first, last)| SequenceRange { first, last }),
    }
}

fn coverage_fixture() -> (DatasetManifest, Vec<MarketEvent>, SourceCoverage) {
    let events = vec![trade(1, Some("100")), tick(1), trade(3, Some("101"))];
    let manifest = seal_dataset(definition(), &events).unwrap();
    let d = &manifest.definition;
    let coverage = SourceCoverage {
        schema_version: 1,
        dataset_id: d.dataset_id.clone(),
        dataset_version: d.version.clone(),
        source_id: d.source_id.clone(),
        replay_sha256: manifest.replay_sha256.clone(),
        streams: vec![
            StreamCoverage {
                stream: d.streams[0].clone(),
                sequence_contract: SequenceContract::ContiguousTradeId,
                windows: vec![
                    receipt("trade", 1, 1, Some((100, 100))),
                    receipt("trade", 2, 0, None),
                    receipt("trade", 3, 1, Some((101, 101))),
                ],
            },
            StreamCoverage {
                stream: d.streams[1].clone(),
                sequence_contract: SequenceContract::Unavailable,
                windows: vec![
                    receipt("tick", 1, 1, None),
                    receipt("tick", 2, 0, None),
                    receipt("tick", 3, 0, None),
                ],
            },
        ],
    };
    (manifest, events, coverage)
}

#[test]
fn explicit_empty_windows_and_contiguous_ids_across_days_are_reconciled() {
    let (manifest, events, coverage) = coverage_fixture();
    let verified = coverage.verify_dataset(&manifest, &events).unwrap();
    let report = verified.source_coverage.unwrap();
    assert_eq!(
        (
            report.windows_verified,
            report.empty_windows,
            report.sequence_checked_streams,
            report.sequence_unavailable_streams
        ),
        (6, 3, 1, 1)
    );
    assert!(!report.independently_attested);
    assert!(!verified.upstream_completeness_verified);
    assert_eq!(report.evidence, coverage);
}

#[test]
fn missing_failed_overlapping_and_duplicate_receipts_are_refused() {
    let (manifest, events, coverage) = coverage_fixture();
    let mut c = coverage.clone();
    c.streams[0].windows.remove(1);
    assert!(c.verify(&manifest, &events).is_err());
    let mut c = coverage.clone();
    c.streams[0].windows[1].complete = false;
    assert!(c.verify(&manifest, &events).is_err());
    let mut c = coverage.clone();
    c.streams[0].windows[1].start -= chrono::TimeDelta::seconds(1);
    assert!(c.verify(&manifest, &events).is_err());
    let mut c = coverage.clone();
    c.streams[1].windows[0].receipt_id = c.streams[0].windows[0].receipt_id.clone();
    assert!(c.verify(&manifest, &events).is_err());
    let mut c = coverage;
    c.streams[0].windows.pop();
    assert!(c.verify(&manifest, &events).is_err());
}

#[test]
fn collection_counts_detect_omission_even_when_resealed_manifest_matches() {
    let (_, mut events, mut coverage) = coverage_fixture();
    events.pop();
    let manifest = seal_dataset(definition(), &events).unwrap();
    coverage.replay_sha256 = manifest.replay_sha256.clone();
    assert!(
        coverage
            .verify(&manifest, &events)
            .unwrap_err()
            .contains("count mismatch")
    );
    coverage.streams[0].windows[2].expected_count = 0;
    coverage.streams[0].windows[2].sequence_range = None;
    assert!(coverage.verify(&manifest, &events).is_ok()); // claims cannot authenticate themselves
}

#[test]
fn rejects_sequence_gaps_between_windows_and_within_window() {
    let (manifest, events, coverage) = coverage_fixture();
    let mut c = coverage.clone();
    c.streams[0].windows[2].sequence_range = Some(SequenceRange {
        first: 102,
        last: 102,
    });
    assert!(
        c.verify(&manifest, &events)
            .unwrap_err()
            .contains("gap or overlap")
    );
    let (_, mut events, mut c) = coverage_fixture();
    events.insert(1, trade(1, Some("102")));
    let manifest = seal_dataset(definition(), &events).unwrap();
    c.replay_sha256 = manifest.replay_sha256.clone();
    c.streams[0].windows[0].expected_count = 2;
    c.streams[0].windows[0].sequence_range = Some(SequenceRange {
        first: 100,
        last: 101,
    });
    c.streams[0].windows[2].sequence_range = Some(SequenceRange {
        first: 102,
        last: 102,
    });
    // The source receipts expect 100,101,102; the sealed tape contains 100,102,101.
    assert!(
        c.verify(&manifest, &events)
            .unwrap_err()
            .contains("sequence mismatch")
    );
}

#[test]
fn refuses_wrong_identity_missing_stream_and_invalid_sequence_contract() {
    let (manifest, events, coverage) = coverage_fixture();
    let mut c = coverage.clone();
    c.dataset_version = "v2".into();
    assert!(c.verify(&manifest, &events).is_err());
    let mut c = coverage.clone();
    c.streams.pop();
    assert!(c.verify(&manifest, &events).is_err());
    let mut c = coverage.clone();
    c.streams[0].sequence_contract = SequenceContract::ContiguousDepthUpdate;
    assert!(c.verify(&manifest, &events).is_err());
    let mut c = coverage;
    c.streams[0].windows[0].sequence_range = Some(SequenceRange {
        first: 0,
        last: u64::MAX,
    });
    assert!(c.verify(&manifest, &events).is_err());
}

#[test]
fn depth_sequence_contract_refuses_invalidation_and_missing_sequence() {
    let mut d = definition();
    d.streams.truncate(1);
    d.streams[0].kind = StreamKind::Depth;
    let depth = |sequence| {
        MarketEvent::Depth(apexes_core::DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![apexes_core::PriceLevel {
                price: dec!(99),
                quantity: dec!(1),
            }],
            asks: vec![apexes_core::PriceLevel {
                price: dec!(101),
                quantity: dec!(1),
            }],
            sequence,
            is_snapshot: true,
            timestamp: day(1).and_hms_opt(0, 0, 1).unwrap().and_utc(),
        })
    };
    let mut events = vec![depth(Some(10))];
    let m = seal_dataset(d.clone(), &events).unwrap();
    let mut c = SourceCoverage {
        schema_version: 1,
        dataset_id: d.dataset_id.clone(),
        dataset_version: d.version.clone(),
        source_id: d.source_id.clone(),
        replay_sha256: m.replay_sha256.clone(),
        streams: vec![StreamCoverage {
            stream: d.streams[0].clone(),
            sequence_contract: SequenceContract::ContiguousDepthUpdate,
            windows: vec![
                receipt("depth", 1, 1, Some((10, 10))),
                receipt("depth", 2, 0, None),
                receipt("depth", 3, 0, None),
            ],
        }],
    };
    assert!(c.verify(&m, &events).is_ok());
    events[0] = depth(None);
    let m = seal_dataset(d.clone(), &events).unwrap();
    c.replay_sha256 = m.replay_sha256.clone();
    assert!(
        c.verify(&m, &events)
            .unwrap_err()
            .contains("missing depth sequence")
    );
    if let MarketEvent::Depth(update) = &mut events[0] {
        update.bids.clear();
        update.asks.clear();
    }
    let m = seal_dataset(d, &events).unwrap();
    c.replay_sha256 = m.replay_sha256.clone();
    assert!(c.verify(&m, &events).unwrap_err().contains("invalidation"));
}

#[tokio::test]
async fn coverage_is_enforced_before_strategy_and_recorded_on_success() {
    let (manifest, events, coverage) = coverage_fixture();
    let count = Arc::new(AtomicUsize::new(0));
    let reg = registry(manifest.clone());
    reg.register::<SourceCoverage, ()>((), coverage.clone())
        .unwrap();
    let report = run_backtest(
        config(),
        &reg,
        InMemoryMarketDataSource::new(events.clone()),
        CountEvents(count.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        report
            .historical_dataset
            .unwrap()
            .source_coverage
            .unwrap()
            .evidence,
        coverage
    );
    assert_eq!(count.load(Ordering::Relaxed), 3);
    count.store(0, Ordering::Relaxed);
    let mut c = coverage.clone();
    c.streams[0].windows[1].complete = false;
    let reg = registry(manifest);
    reg.register::<SourceCoverage, ()>((), c).unwrap();
    assert!(
        run_backtest(
            config(),
            &reg,
            InMemoryMarketDataSource::new(events.clone()),
            CountEvents(count.clone())
        )
        .await
        .is_err()
    );
    let reg = apexes_core::registry::Registry::new();
    reg.register::<SourceCoverage, ()>((), coverage).unwrap();
    assert!(
        run_backtest(
            config(),
            &reg,
            InMemoryMarketDataSource::new(events),
            CountEvents(count.clone())
        )
        .await
        .is_err()
    );
    assert_eq!(count.load(Ordering::Relaxed), 0);
}

#[test]
fn cli_verifies_coverage_and_rejects_failed_receipts() {
    let (manifest, events, mut coverage) = coverage_fixture();
    let dir = std::env::temp_dir().join(format!("apexes-coverage-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let manifest_path = dir.join("manifest.json");
    let events_path = dir.join("events.json");
    let coverage_path = dir.join("coverage.json");
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    std::fs::write(&events_path, serde_json::to_vec(&events).unwrap()).unwrap();
    std::fs::write(&coverage_path, serde_json::to_vec(&coverage).unwrap()).unwrap();
    let verify = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_historical-manifest"))
            .arg("verify-coverage")
            .arg(&manifest_path)
            .arg(&events_path)
            .arg(&coverage_path)
            .output()
            .unwrap()
    };
    let output = verify();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        actual,
        serde_json::to_value(coverage.verify_dataset(&manifest, &events).unwrap()).unwrap()
    );
    coverage.streams[0].windows[1].complete = false;
    std::fs::write(&coverage_path, serde_json::to_vec(&coverage).unwrap()).unwrap();
    let output = verify();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    std::fs::remove_dir_all(dir).unwrap();
}

use apexes_backtest::source_identity::{
    IdentityContract, IdentityPolicy, StreamIdentity, normalize_dataset,
};

fn identity_policy(d: &DatasetDefinition, contracts: &[IdentityContract]) -> IdentityPolicy {
    assert_eq!(d.streams.len(), contracts.len());
    IdentityPolicy {
        schema_version: 1,
        source_id: d.source_id.clone(),
        streams: d
            .streams
            .iter()
            .cloned()
            .zip(contracts.iter().copied())
            .map(|(stream, contract)| StreamIdentity { stream, contract })
            .collect(),
    }
}

#[test]
fn identified_retransmission_removed_but_anonymous_quote_repeats_preserved() {
    let d = definition();
    let p = identity_policy(
        &d,
        &[
            IdentityContract::OpaqueTradeId,
            IdentityContract::Unavailable,
        ],
    );
    let input = vec![
        trade(1, Some("a")),
        tick(1),
        trade(1, Some("a")),
        tick(1),
        trade(3, Some("b")),
    ];
    let result = normalize_dataset(d.clone(), p.clone(), &input).unwrap();
    assert_eq!(
        result.events,
        vec![
            input[0].clone(),
            input[1].clone(),
            input[3].clone(),
            input[4].clone()
        ]
    );
    assert_eq!(
        result.deduplication.streams[0].identical_duplicates_removed,
        1
    );
    assert_eq!(result.deduplication.streams[1].unidentified_retained, 2);
    assert_ne!(
        result.deduplication.input_sha256,
        result.deduplication.output_sha256
    );
    assert_eq!(result.manifest.identity_policy, Some(p.clone()));
    assert_eq!(
        result.manifest.verify(&result.events).unwrap().event_count,
        4
    );
    let twice = normalize_dataset(d, p, &result.events).unwrap();
    assert_eq!(twice.events, result.events);
    assert_eq!(twice.manifest, result.manifest);
    assert_eq!(
        twice.deduplication.input_sha256,
        twice.deduplication.output_sha256
    );
}

#[test]
fn conflicting_trade_payload_and_missing_explicit_id_fail_closed() {
    let d = definition();
    let p = identity_policy(
        &d,
        &[
            IdentityContract::OpaqueTradeId,
            IdentityContract::Unavailable,
        ],
    );
    let mut conflicting = trade(1, Some("a"));
    if let MarketEvent::Trade(t) = &mut conflicting {
        t.quantity = dec!(2);
    }
    assert!(
        normalize_dataset(d.clone(), p.clone(), &[trade(1, Some("a")), conflicting])
            .unwrap_err()
            .contains("conflicting payload")
    );
    assert!(
        normalize_dataset(d, p, &[trade(1, None)])
            .unwrap_err()
            .contains("missing opaque")
    );
}

#[test]
fn numeric_trade_alias_is_conflict_and_opaque_ids_stay_distinct() {
    let d = definition();
    let input = vec![trade(1, Some("01")), trade(1, Some("1"))];
    let p = identity_policy(
        &d,
        &[
            IdentityContract::NumericTradeId,
            IdentityContract::Unavailable,
        ],
    );
    assert!(
        normalize_dataset(d.clone(), p.clone(), &input)
            .unwrap_err()
            .contains("conflicting payload")
    );
    assert!(normalize_dataset(d.clone(), p, &[trade(1, Some("not numeric"))]).is_err());
    let p = identity_policy(
        &d,
        &[
            IdentityContract::OpaqueTradeId,
            IdentityContract::Unavailable,
        ],
    );
    assert_eq!(normalize_dataset(d, p, &input).unwrap().events.len(), 2);
}

#[test]
fn depth_increment_duplicates_removed_without_discarding_snapshots_or_invalidations() {
    let mut d = definition();
    d.streams.truncate(1);
    d.streams[0].kind = StreamKind::Depth;
    let p = identity_policy(&d, &[IdentityContract::DepthIncrementSequence]);
    let depth = |snapshot, sequence| {
        MarketEvent::Depth(apexes_core::DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![apexes_core::PriceLevel {
                price: dec!(99),
                quantity: dec!(1),
            }],
            asks: vec![apexes_core::PriceLevel {
                price: dec!(101),
                quantity: dec!(1),
            }],
            sequence,
            is_snapshot: snapshot,
            timestamp: day(1).and_hms_opt(0, 0, 1).unwrap().and_utc(),
        })
    };
    let mut invalidation = depth(true, None);
    if let MarketEvent::Depth(x) = &mut invalidation {
        x.bids.clear();
        x.asks.clear();
    }
    let increment = depth(false, Some(9));
    let snapshot = depth(true, Some(9));
    let input = vec![
        snapshot.clone(),
        increment.clone(),
        increment.clone(),
        snapshot,
        invalidation.clone(),
        invalidation,
    ];
    let result = normalize_dataset(d.clone(), p.clone(), &input).unwrap();
    assert_eq!(result.events.len(), 5);
    assert_eq!(result.deduplication.streams[0].unidentified_retained, 4);
    assert_eq!(
        result.deduplication.streams[0].identical_duplicates_removed,
        1
    );
    assert!(normalize_dataset(d, p, &[depth(false, None)]).is_err());
}

#[test]
fn final_bars_are_keyed_by_interval_and_open_time_and_conflicts_refused() {
    let mut d = definition();
    d.streams.truncate(1);
    d.streams[0].kind = StreamKind::Bar;
    let p = identity_policy(&d, &[IdentityContract::FinalBar]);
    let start = day(1).and_hms_opt(0, 0, 0).unwrap().and_utc();
    let bar = |interval, minutes| {
        MarketEvent::Bar(apexes_core::Bar {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            open: dec!(100),
            high: dec!(101),
            low: dec!(99),
            close: dec!(100),
            volume: dec!(5),
            interval,
            open_time: start,
            close_time: start + chrono::TimeDelta::minutes(minutes),
        })
    };
    let one = bar(apexes_core::BarInterval::OneMinute, 1);
    let five = bar(apexes_core::BarInterval::FiveMinutes, 5);
    let result = normalize_dataset(
        d.clone(),
        p.clone(),
        &[one.clone(), five.clone(), one.clone()],
    )
    .unwrap();
    assert_eq!(result.events, vec![one.clone(), five]);
    let mut changed = one.clone();
    if let MarketEvent::Bar(b) = &mut changed {
        b.volume = dec!(6);
    }
    assert!(normalize_dataset(d, p, &[one, changed]).is_err());
}

#[test]
fn timestamp_identity_requires_explicit_contract_and_refuses_conflicts() {
    let d = definition();
    let p = identity_policy(
        &d,
        &[
            IdentityContract::OpaqueTradeId,
            IdentityContract::UniqueTimestamp,
        ],
    );
    let input = vec![tick(1), tick(1)];
    let result = normalize_dataset(d.clone(), p.clone(), &input).unwrap();
    assert_eq!(result.events.len(), 1);
    let mut changed = tick(1);
    if let MarketEvent::Tick(t) = &mut changed {
        t.bid_size = dec!(2);
    }
    assert!(normalize_dataset(d, p, &[tick(1), changed]).is_err());
}

#[test]
fn source_identity_policy_rejects_bad_scope_and_contract() {
    let d = definition();
    let p = identity_policy(
        &d,
        &[
            IdentityContract::OpaqueTradeId,
            IdentityContract::Unavailable,
        ],
    );
    let mut bad = p.clone();
    bad.source_id = "other".into();
    assert!(normalize_dataset(d.clone(), bad, &[]).is_err());
    let mut bad = p.clone();
    bad.streams[0].contract = IdentityContract::FinalBar;
    assert!(normalize_dataset(d.clone(), bad, &[]).is_err());
    let mut bad = p.clone();
    bad.streams[1] = bad.streams[0].clone();
    assert!(normalize_dataset(d.clone(), bad, &[]).is_err());
    assert!(
        normalize_dataset(
            d.clone(),
            p.clone(),
            &[trade(3, Some("b")), trade(1, Some("a"))]
        )
        .is_err()
    );
    assert!(normalize_dataset(d, p, &[trade(4, Some("a"))]).is_err());
}

#[test]
fn manifest_policy_refuses_duplicate_identities_even_if_content_hash_matches() {
    let d = definition();
    let p = identity_policy(
        &d,
        &[
            IdentityContract::OpaqueTradeId,
            IdentityContract::UniqueTimestamp,
        ],
    );
    let events = vec![tick(1), tick(1)];
    let mut manifest = seal_dataset(d, &events).unwrap();
    manifest.identity_policy = Some(p);
    assert!(
        manifest
            .verify(&events)
            .unwrap_err()
            .contains("duplicate source identities")
    );
}

#[test]
fn cli_normalizes_without_mutating_input_and_refuses_conflicts() {
    let d = definition();
    let p = identity_policy(
        &d,
        &[
            IdentityContract::OpaqueTradeId,
            IdentityContract::Unavailable,
        ],
    );
    let dir = std::env::temp_dir().join(format!("apexes-dedup-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let def_path = dir.join("definition.json");
    let events_path = dir.join("events.json");
    let policy_path = dir.join("policy.json");
    std::fs::write(&def_path, serde_json::to_vec(&d).unwrap()).unwrap();
    std::fs::write(&policy_path, serde_json::to_vec(&p).unwrap()).unwrap();
    let input = vec![trade(1, Some("a")), trade(1, Some("a"))];
    let raw = serde_json::to_vec(&input).unwrap();
    std::fs::write(&events_path, &raw).unwrap();
    let run = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_historical-manifest"))
            .arg("normalize")
            .arg(&def_path)
            .arg(&events_path)
            .arg(&policy_path)
            .output()
            .unwrap()
    };
    let output = run();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::to_value(normalize_dataset(d, p, &input).unwrap()).unwrap()
    );
    assert_eq!(std::fs::read(&events_path).unwrap(), raw);
    std::fs::write(&events_path, serde_json::to_vec(&[trade(1, None)]).unwrap()).unwrap();
    let output = run();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    std::fs::remove_dir_all(dir).unwrap();
}

use apexes_backtest::depth_ranges::{
    DepthRange, DepthRangeEvidence, DepthStreamRanges, RangeContract,
};

fn range_fixture(
    contract: RangeContract,
) -> (DatasetManifest, Vec<MarketEvent>, DepthRangeEvidence) {
    let mut d = definition();
    d.streams[0].kind = StreamKind::Depth;
    let depth = |seq, snapshot, ms| {
        MarketEvent::Depth(apexes_core::DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![apexes_core::PriceLevel {
                price: dec!(99),
                quantity: dec!(2),
            }],
            asks: vec![apexes_core::PriceLevel {
                price: dec!(101),
                quantity: dec!(2),
            }],
            sequence: Some(seq),
            is_snapshot: snapshot,
            timestamp: day(1).and_hms_opt(0, 0, 0).unwrap().and_utc()
                + chrono::TimeDelta::milliseconds(ms),
        })
    };
    let events = vec![
        depth(100, true, 0),
        depth(103, false, 500),
        tick(1),
        depth(110, false, 1500),
    ];
    let manifest = seal_dataset(d, &events).unwrap();
    let (first, second) = match contract {
        RangeContract::InclusiveNext => ((99, None), (104, None)),
        RangeContract::PreviousFinal => ((99, Some(98)), (108, Some(103))),
    };
    let ranges = DepthRangeEvidence {
        schema_version: 1,
        dataset_id: manifest.definition.dataset_id.clone(),
        dataset_version: manifest.definition.version.clone(),
        source_id: manifest.definition.source_id.clone(),
        replay_sha256: manifest.replay_sha256.clone(),
        streams: vec![DepthStreamRanges {
            stream: manifest.definition.streams[0].clone(),
            contract,
            increments: vec![
                DepthRange {
                    event_index: 1,
                    first: first.0,
                    last: 103,
                    previous_final: first.1,
                },
                DepthRange {
                    event_index: 3,
                    first: second.0,
                    last: 110,
                    previous_final: second.1,
                },
            ],
        }],
    };
    (manifest, events, ranges)
}

#[test]
fn ranges_accept_overlapping_and_batched_updates_with_explicit_contracts() {
    for contract in [RangeContract::InclusiveNext, RangeContract::PreviousFinal] {
        let (manifest, events, ranges) = range_fixture(contract);
        let verified = ranges.verify_dataset(&manifest, &events).unwrap();
        let report = verified.depth_ranges.unwrap();
        assert_eq!(
            (
                report.streams_verified,
                report.snapshots_verified,
                report.increments_verified
            ),
            (1, 1, 2)
        );
        assert!(!report.independently_attested);
        assert!(!verified.upstream_completeness_verified);
        assert_eq!(report.evidence, ranges);
    }
}

#[test]
fn ranges_refuse_initial_bridge_gap_and_later_gap() {
    let (manifest, events, mut ranges) = range_fixture(RangeContract::InclusiveNext);
    ranges.streams[0].increments[0].first = 102;
    assert!(
        ranges
            .verify_dataset(&manifest, &events)
            .unwrap_err()
            .contains("range gap")
    );
    let (manifest, events, mut ranges) = range_fixture(RangeContract::InclusiveNext);
    ranges.streams[0].increments[1].first = 105;
    assert!(
        ranges
            .verify_dataset(&manifest, &events)
            .unwrap_err()
            .contains("range gap")
    );
}

#[test]
fn previous_final_requires_initial_bridge_and_exact_previous_link() {
    let (manifest, events, mut ranges) = range_fixture(RangeContract::PreviousFinal);
    ranges.streams[0].increments[0].first = 101;
    assert!(
        ranges
            .verify_dataset(&manifest, &events)
            .unwrap_err()
            .contains("bridge snapshot")
    );
    let (manifest, events, mut ranges) = range_fixture(RangeContract::PreviousFinal);
    ranges.streams[0].increments[1].previous_final = Some(102);
    assert!(
        ranges
            .verify_dataset(&manifest, &events)
            .unwrap_err()
            .contains("link mismatch")
    );
    ranges.streams[0].increments[1].previous_final = None;
    assert!(
        ranges
            .verify_dataset(&manifest, &events)
            .unwrap_err()
            .contains("missing previous-final")
    );
}

#[test]
fn ranges_require_exact_event_binding_and_complete_metadata() {
    let (manifest, events, ranges) = range_fixture(RangeContract::InclusiveNext);
    let mut bad = ranges.clone();
    bad.streams[0].increments[1].event_index = 2;
    assert!(bad.verify_dataset(&manifest, &events).is_err());
    let mut bad = ranges.clone();
    bad.streams[0].increments[0].last = 102;
    assert!(bad.verify_dataset(&manifest, &events).is_err());
    let mut bad = ranges.clone();
    bad.streams[0].increments.pop();
    assert!(bad.verify_dataset(&manifest, &events).is_err());
    let mut bad = ranges.clone();
    let extra = bad.streams[0].increments[0].clone();
    bad.streams[0].increments.push(extra);
    assert!(bad.verify_dataset(&manifest, &events).is_err());
    let mut bad = ranges.clone();
    bad.replay_sha256 = "0".repeat(64);
    assert!(bad.verify_dataset(&manifest, &events).is_err());
    let mut bad = ranges;
    bad.streams.clear();
    assert!(bad.verify_dataset(&manifest, &events).is_err());
}

#[test]
fn ranges_refuse_snapshot_jumps_and_invalidation_even_with_resealed_input() {
    let (manifest, mut events, mut ranges) = range_fixture(RangeContract::InclusiveNext);
    if let MarketEvent::Depth(d) = &mut events[3] {
        d.is_snapshot = true;
    }
    let m = seal_dataset(manifest.definition.clone(), &events).unwrap();
    ranges.replay_sha256 = m.replay_sha256.clone();
    ranges.streams[0].increments.pop();
    assert!(
        ranges
            .verify_dataset(&m, &events)
            .unwrap_err()
            .contains("snapshot jumps")
    );
    if let MarketEvent::Depth(d) = &mut events[3] {
        d.sequence = None;
        d.bids.clear();
        d.asks.clear();
    }
    let m = seal_dataset(manifest.definition, &events).unwrap();
    ranges.replay_sha256 = m.replay_sha256.clone();
    assert!(
        ranges
            .verify_dataset(&m, &events)
            .unwrap_err()
            .contains("invalidation")
    );
}

#[test]
fn repeated_same_id_snapshot_cannot_reset_previous_final_chain() {
    let (manifest, mut events, mut ranges) = range_fixture(RangeContract::PreviousFinal);
    let mut snap = events[1].clone();
    if let MarketEvent::Depth(d) = &mut snap {
        d.is_snapshot = true;
    }
    events.insert(2, snap);
    let m = seal_dataset(manifest.definition, &events).unwrap();
    ranges.replay_sha256 = m.replay_sha256.clone();
    ranges.streams[0].increments[1].event_index = 4;
    assert!(ranges.verify_dataset(&m, &events).is_ok());
    ranges.streams[0].increments[1].first = 102;
    ranges.streams[0].increments[1].previous_final = Some(102);
    assert!(
        ranges
            .verify_dataset(&m, &events)
            .unwrap_err()
            .contains("link mismatch")
    );
}

struct BuyOnTick;
impl Strategy for BuyOnTick {
    fn on_market_event(&mut self, event: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
        if !matches!(event, MarketEvent::Tick(_)) {
            return vec![];
        }
        vec![OrderIntent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: apexes_core::OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }]
    }
}

#[tokio::test]
async fn verified_ranges_enable_strict_replay_without_weakening_freshness() {
    use apexes_backtest::{DepthSequence, SimulatedDepthPolicy};
    let (manifest, events, ranges) = range_fixture(RangeContract::InclusiveNext);
    let reg = registry(manifest.clone());
    reg.register::<DepthRangeEvidence, ()>((), ranges.clone())
        .unwrap();
    reg.register::<SimulatedDepthPolicy, ()>(
        (),
        SimulatedDepthPolicy {
            max_age_ms: 1000,
            sequence: DepthSequence::Contiguous,
        },
    )
    .unwrap();
    let report = run_backtest(
        config(),
        &reg,
        InMemoryMarketDataSource::new(events.clone()),
        BuyOnTick,
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].price, dec!(101));
    assert_eq!(
        report.simulated_depth.unwrap().sequence,
        DepthSequence::VerifiedRanges
    );
    assert!(report.historical_dataset.unwrap().depth_ranges.is_some());
    let reg = registry(manifest.clone());
    reg.register::<SimulatedDepthPolicy, ()>(
        (),
        SimulatedDepthPolicy {
            max_age_ms: 1000,
            sequence: DepthSequence::Contiguous,
        },
    )
    .unwrap();
    let report = run_backtest(
        config(),
        &reg,
        InMemoryMarketDataSource::new(events.clone()),
        BuyOnTick,
    )
    .await
    .unwrap();
    assert!(report.trades.is_empty());
    let reg = registry(manifest);
    reg.register::<DepthRangeEvidence, ()>((), ranges).unwrap();
    reg.register::<SimulatedDepthPolicy, ()>(
        (),
        SimulatedDepthPolicy {
            max_age_ms: 100,
            sequence: DepthSequence::Contiguous,
        },
    )
    .unwrap();
    let report = run_backtest(
        config(),
        &reg,
        InMemoryMarketDataSource::new(events),
        BuyOnTick,
    )
    .await
    .unwrap();
    assert!(report.trades.is_empty());
}

#[tokio::test]
async fn range_verification_failure_or_direct_mode_selection_prevents_strategy_execution() {
    use apexes_backtest::{DepthSequence, SimulatedDepthPolicy};
    let (manifest, events, mut ranges) = range_fixture(RangeContract::InclusiveNext);
    ranges.streams[0].increments[1].first = 105;
    let count = Arc::new(AtomicUsize::new(0));
    let reg = registry(manifest.clone());
    reg.register::<DepthRangeEvidence, ()>((), ranges.clone())
        .unwrap();
    assert!(
        run_backtest(
            config(),
            &reg,
            InMemoryMarketDataSource::new(events.clone()),
            CountEvents(count.clone())
        )
        .await
        .is_err()
    );
    let reg = apexes_core::registry::Registry::new();
    reg.register::<DepthRangeEvidence, ()>((), ranges).unwrap();
    assert!(
        run_backtest(
            config(),
            &reg,
            InMemoryMarketDataSource::new(events.clone()),
            CountEvents(count.clone())
        )
        .await
        .is_err()
    );
    let reg = registry(manifest);
    reg.register::<SimulatedDepthPolicy, ()>(
        (),
        SimulatedDepthPolicy {
            max_age_ms: 1000,
            sequence: DepthSequence::VerifiedRanges,
        },
    )
    .unwrap();
    assert!(
        run_backtest(
            config(),
            &reg,
            InMemoryMarketDataSource::new(events),
            CountEvents(count.clone())
        )
        .await
        .is_err()
    );
    assert_eq!(count.load(Ordering::Relaxed), 0);
}

#[test]
fn cli_verifies_range_evidence_without_modifying_input() {
    let (manifest, events, mut ranges) = range_fixture(RangeContract::InclusiveNext);
    let dir = std::env::temp_dir().join(format!("apexes-ranges-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let mp = dir.join("manifest.json");
    let ep = dir.join("events.json");
    let rp = dir.join("ranges.json");
    let raw = serde_json::to_vec(&events).unwrap();
    std::fs::write(&mp, serde_json::to_vec(&manifest).unwrap()).unwrap();
    std::fs::write(&ep, &raw).unwrap();
    std::fs::write(&rp, serde_json::to_vec(&ranges).unwrap()).unwrap();
    let verify = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_historical-manifest"))
            .arg("verify-ranges")
            .arg(&mp)
            .arg(&ep)
            .arg(&rp)
            .output()
            .unwrap()
    };
    let output = verify();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::to_value(ranges.verify_dataset(&manifest, &events).unwrap()).unwrap()
    );
    ranges.streams[0].increments[1].first = 105;
    std::fs::write(&rp, serde_json::to_vec(&ranges).unwrap()).unwrap();
    let output = verify();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(std::fs::read(ep).unwrap(), raw);
    std::fs::remove_dir_all(dir).unwrap();
}
