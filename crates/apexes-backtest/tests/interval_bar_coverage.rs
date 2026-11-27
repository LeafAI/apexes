//! F03.03/F04.02: verify interval receipts against real sealed exports.
use apexes_backtest::{
    historical_dataset::{
        DatasetDefinition, DatasetManifest, DatasetStream, StreamKind, VerifiedDataset,
        seal_dataset, seal_observation_dataset,
    },
    source_coverage::{
        CollectionWindow, SequenceContract, SequenceRange, SourceCoverage, StreamCoverage,
    },
};
use apexes_core::{
    Bar, BarInterval, DepthSourceRange, DepthUpdate, MarketEvent, TradeEvent, Venue,
};
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal_macros::dec;

const DAY: i64 = 86_400;

fn at(seconds: i64) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .to_utc()
        + TimeDelta::seconds(seconds)
}

fn bar(open: i64, close: i64, interval: BarInterval) -> MarketEvent {
    MarketEvent::Bar(Bar {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        open: dec!(100),
        high: dec!(102),
        low: dec!(99),
        close: dec!(101),
        volume: dec!(2),
        interval,
        open_time: at(open),
        close_time: at(close),
    })
}

fn window(id: impl Into<String>, start: i64, end: i64, expected_count: u64) -> CollectionWindow {
    CollectionWindow {
        receipt_id: id.into(),
        start: at(start),
        end_exclusive: at(end),
        complete: true,
        expected_count,
        sequence_range: None,
    }
}

fn stream(venue: Venue, symbol: &str, kind: StreamKind) -> DatasetStream {
    DatasetStream {
        venue,
        symbol: symbol.into(),
        kind,
    }
}

fn definition(streams: Vec<DatasetStream>) -> DatasetDefinition {
    DatasetDefinition {
        dataset_id: "interval-fixture".into(),
        version: "v2".into(),
        source_id: "synthetic".into(),
        start_day: at(0).date_naive(),
        end_day_exclusive: at(2 * DAY).date_naive(),
        streams,
    }
}

fn receipts(
    manifest: &DatasetManifest,
    schema_version: u32,
    streams: Vec<StreamCoverage>,
) -> SourceCoverage {
    SourceCoverage {
        schema_version,
        dataset_id: manifest.definition.dataset_id.clone(),
        dataset_version: manifest.definition.version.clone(),
        source_id: manifest.definition.source_id.clone(),
        replay_sha256: manifest.replay_sha256.clone(),
        streams,
    }
}

fn plain(
    stream: &DatasetStream,
    bar_interval: Option<BarInterval>,
    windows: Vec<CollectionWindow>,
) -> StreamCoverage {
    StreamCoverage {
        stream: stream.clone(),
        bar_interval,
        sequence_contract: SequenceContract::Unavailable,
        windows,
    }
}

fn with_sequence(
    mut stream: StreamCoverage,
    contract: SequenceContract,
    first: u64,
    last: u64,
) -> StreamCoverage {
    stream.sequence_contract = contract;
    stream.windows[0].sequence_range = Some(SequenceRange { first, last });
    stream
}

fn fixture() -> (DatasetManifest, Vec<MarketEvent>, SourceCoverage) {
    use BarInterval::{FiveMinutes, OneMinute, OneSecond};
    use StreamKind::{Bar, Depth, Tick, Trade};
    use Venue::Binance;
    let mut eth = bar(0, 60, OneMinute);
    let MarketEvent::Bar(ref mut eth_bar) = eth else {
        unreachable!()
    };
    eth_bar.symbol = "ETHUSDT".into();
    let mut other_venue = bar(0, 60, OneMinute);
    let MarketEvent::Bar(ref mut other_bar) = other_venue else {
        unreachable!()
    };
    other_bar.venue = Venue::Okx;
    let events = vec![
        bar(0, 1, OneSecond),
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Binance,
            price: dec!(100),
            quantity: dec!(1),
            side: None,
            trade_id: Some("42".into()),
            timestamp: at(1),
        }),
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Binance,
            bids: vec![],
            asks: vec![],
            sequence: Some(7),
            source_range: Some(DepthSourceRange {
                first: 7,
                last: 7,
                previous_final: Some(6),
            }),
            is_snapshot: false,
            timestamp: at(2),
        }),
        bar(4, 5, OneSecond),
        MarketEvent::Tick(apexes_core::Tick {
            symbol: "BTCUSDT".into(),
            venue: Binance,
            bid_price: dec!(99),
            ask_price: dec!(101),
            bid_size: dec!(1),
            ask_size: dec!(1),
            timestamp: at(59),
        }),
        bar(59, 60, OneSecond),
        bar(0, 60, OneMinute),
        eth,
        other_venue,
        bar(0, 300, FiveMinutes),
        bar(DAY - 1, DAY, OneSecond),
        bar(DAY - 60, DAY, OneMinute),
        bar(DAY, DAY + 1, OneSecond),
    ];
    let btc = stream(Binance, "BTCUSDT", Bar);
    let eth = stream(Binance, "ETHUSDT", Bar);
    let other = stream(Venue::Okx, "BTCUSDT", Bar);
    let trade = stream(Binance, "BTCUSDT", Trade);
    let depth = stream(Binance, "BTCUSDT", Depth);
    let tick = stream(Binance, "BTCUSDT", Tick);
    let streams = vec![
        plain(
            &btc,
            Some(OneSecond),
            vec![
                window("seconds-early", 0, 10, 2),
                window("seconds-empty", 10, 50, 0),
                window("seconds-day-1", 50, DAY, 2),
                window("seconds-day-2", DAY, 2 * DAY, 1),
            ],
        ),
        plain(
            &btc,
            Some(OneMinute),
            vec![
                window("minutes-early", 0, 10, 1),
                window("minutes-day-1", 10, DAY, 1),
                window("minutes-day-2", DAY, 2 * DAY, 0),
            ],
        ),
        plain(
            &btc,
            Some(FiveMinutes),
            vec![window("five-minutes", 0, 2 * DAY, 1)],
        ),
        plain(
            &eth,
            Some(OneMinute),
            vec![window("eth-minute", 0, 2 * DAY, 1)],
        ),
        plain(
            &other,
            Some(OneMinute),
            vec![window("other-venue", 0, 2 * DAY, 1)],
        ),
        plain(
            &btc,
            Some(BarInterval::OneHour),
            vec![window("hours-empty", 0, 2 * DAY, 0)],
        ),
        with_sequence(
            plain(&trade, None, vec![window("trades", 0, 2 * DAY, 1)]),
            SequenceContract::ContiguousTradeId,
            42,
            42,
        ),
        with_sequence(
            plain(&depth, None, vec![window("depth", 0, 2 * DAY, 1)]),
            SequenceContract::ContiguousDepthUpdate,
            7,
            7,
        ),
        plain(&tick, None, vec![window("ticks", 0, 2 * DAY, 1)]),
    ];
    let manifest = seal_observation_dataset(
        definition(vec![btc, eth, other, trade, depth, tick]),
        &events,
    )
    .unwrap();
    let coverage = receipts(&manifest, 2, streams);
    (manifest, events, coverage)
}

#[test]
fn mixed_intervals_reconcile_source_windows_in_exact_observation_order() {
    let (manifest, events, coverage) = fixture();
    let verified = coverage.verify_dataset(&manifest, &events).unwrap();
    assert_eq!(verified.event_count, 13);
    assert_eq!(verified.manifest, manifest);
    assert!(!verified.upstream_completeness_verified);
    let report = verified.source_coverage.as_ref().unwrap();
    assert_eq!(report.windows_verified, 14);
    assert_eq!(report.empty_windows, 3);
    assert_eq!(report.sequence_checked_streams, 2);
    assert_eq!(report.sequence_unavailable_streams, 7);
    assert!(!report.independently_attested);
    assert_eq!(report.evidence, coverage);
    for _ in 0..10 {
        assert_eq!(
            coverage.verify_dataset(&manifest, &events).unwrap(),
            verified
        );
    }
}

#[test]
fn bars_are_attributed_to_open_day_even_when_they_close_in_the_next_day() {
    let stream = stream(Venue::Binance, "BTCUSDT", StreamKind::Bar);
    let events = [
        bar(DAY - 30, DAY + 30, BarInterval::OneMinute),
        bar(2 * DAY - 60, 2 * DAY, BarInterval::OneMinute),
    ];
    let manifest = seal_observation_dataset(definition(vec![stream.clone()]), &events).unwrap();
    assert_eq!(manifest.partitions[0].event_count, 1);
    assert_eq!(manifest.partitions[1].event_count, 1);
    let coverage = receipts(
        &manifest,
        2,
        vec![plain(
            &stream,
            Some(BarInterval::OneMinute),
            vec![
                window("first-day", 0, DAY, 1),
                window("last-day", DAY, 2 * DAY, 1),
            ],
        )],
    );
    assert_eq!(
        coverage
            .verify(&manifest, &events)
            .unwrap()
            .windows_verified,
        2
    );
}

#[test]
fn legacy_receipt_wire_format_and_verification_are_preserved() {
    let raw = r#"{"stream":{"venue":"Binance","symbol":"BTCUSDT","kind":"bar"},"sequence_contract":"unavailable","windows":[{"receipt_id":"legacy","start":"2026-01-01T00:00:00Z","end_exclusive":"2026-01-03T00:00:00Z","complete":true,"expected_count":2,"sequence_range":null}]}"#;
    let legacy: StreamCoverage = serde_json::from_str(raw).unwrap();
    assert_eq!(serde_json::to_string(&legacy).unwrap(), raw);
    let events = [
        bar(0, 60, BarInterval::OneMinute),
        bar(0, 1, BarInterval::OneSecond),
    ];
    let def = definition(vec![legacy.stream.clone()]);
    let v1 = seal_dataset(def.clone(), &events).unwrap();
    let mut coverage = receipts(&v1, 1, vec![legacy]);
    assert!(coverage.verify(&v1, &events).is_ok());
    coverage.streams[0].bar_interval = Some(BarInterval::OneMinute);
    assert!(coverage.verify(&v1, &events).is_err());
    coverage.schema_version = 2;
    assert!(coverage.verify(&v1, &events).is_err());

    let observed = [events[1].clone(), events[0].clone()];
    let v2 = seal_observation_dataset(def, &observed).unwrap();
    coverage = receipts(&v2, 1, vec![serde_json::from_str(raw).unwrap()]);
    assert!(
        coverage
            .verify(&v2, &observed)
            .unwrap_err()
            .contains("interval-specific")
    );
}

#[test]
fn legacy_coverage_still_verifies_schema_two_without_bars() {
    let (manifest, mut events, mut coverage) = fixture();
    let mut def = manifest.definition;
    def.streams.retain(|s| s.kind != StreamKind::Bar);
    events.retain(|e| !matches!(e, MarketEvent::Bar(_)));
    coverage.streams.retain(|s| s.bar_interval.is_none());
    let sealed = seal_observation_dataset(def, &events).unwrap();
    coverage = receipts(&sealed, 1, coverage.streams);
    assert_eq!(
        coverage
            .verify(&sealed, &events)
            .unwrap()
            .sequence_checked_streams,
        2
    );
}

#[test]
fn missing_duplicate_or_ambiguous_declarations_fail() {
    let (manifest, events, coverage) = fixture();
    let mut invalid = coverage.clone();
    invalid.streams.remove(0);
    assert!(
        invalid
            .verify(&manifest, &events)
            .unwrap_err()
            .contains("event outside coverage streams")
    );
    invalid = coverage.clone();
    invalid.streams[0].bar_interval = None;
    assert!(
        invalid
            .verify(&manifest, &events)
            .unwrap_err()
            .contains("bar interval")
    );
    invalid = coverage.clone();
    invalid.streams.push(invalid.streams[0].clone());
    assert!(
        invalid
            .verify(&manifest, &events)
            .unwrap_err()
            .contains("duplicate coverage stream")
    );
    invalid = coverage.clone();
    invalid.streams[8].bar_interval = Some(BarInterval::OneSecond);
    assert!(invalid.verify(&manifest, &events).is_err());
    invalid = coverage;
    invalid.streams.remove(3);
    assert!(
        invalid
            .verify(&manifest, &events)
            .unwrap_err()
            .contains("missing coverage stream")
    );
}

#[test]
fn malformed_interval_fields_are_rejected_during_deserialization() {
    let (_, _, coverage) = fixture();
    let mut invalid = serde_json::to_value(&coverage).unwrap();
    invalid["streams"][0]["bar_interval"] = serde_json::json!(null);
    assert!(serde_json::from_value::<SourceCoverage>(invalid.clone()).is_err());
    invalid["streams"][0]["bar_interval"] = serde_json::json!("TwoSeconds");
    assert!(serde_json::from_value::<SourceCoverage>(invalid).is_err());
    let json = serde_json::to_string(&coverage).unwrap();
    let duplicate = json.replacen(
        r#""bar_interval":"OneSecond""#,
        r#""bar_interval":"OneSecond","bar_interval":"OneMinute""#,
        1,
    );
    assert!(serde_json::from_str::<SourceCoverage>(&duplicate).is_err());
}

#[test]
fn swapped_interval_counts_and_nonempty_claim_for_empty_interval_fail() {
    let (manifest, events, mut coverage) = fixture();
    coverage.streams[0].windows[0].expected_count = 1;
    coverage.streams[1].windows[0].expected_count = 2;
    assert!(
        coverage
            .verify(&manifest, &events)
            .unwrap_err()
            .contains("seconds-early")
    );
    let (manifest, events, mut coverage) = fixture();
    coverage.streams[5].windows[0].expected_count = 1;
    assert!(
        coverage
            .verify(&manifest, &events)
            .unwrap_err()
            .contains("hours-empty")
    );
}

#[test]
fn omission_insertion_and_payload_tampering_fail() {
    let (manifest, events, coverage) = fixture();
    let mut removed = events.clone();
    removed.remove(3);
    let mut inserted = events.clone();
    inserted.insert(4, bar(5, 6, BarInterval::OneSecond));
    for changed in [removed, inserted] {
        assert!(
            coverage
                .verify(&manifest, &changed)
                .unwrap_err()
                .contains("partition")
        );
        let resealed = seal_observation_dataset(manifest.definition.clone(), &changed).unwrap();
        let rebound = receipts(&resealed, 2, coverage.streams.clone());
        assert!(
            rebound
                .verify(&resealed, &changed)
                .unwrap_err()
                .contains("collection count")
        );
    }
    let mut tampered = events.clone();
    let MarketEvent::Bar(ref mut changed) = tampered[0] else {
        unreachable!()
    };
    changed.volume = dec!(999);
    let mut invalid_receipt = coverage.clone();
    invalid_receipt.streams.clear();
    assert!(
        invalid_receipt
            .verify(&manifest, &tampered)
            .unwrap_err()
            .contains("partition")
    );
    let mut swapped = events;
    swapped.swap(5, 6);
    assert!(
        coverage
            .verify(&manifest, &swapped)
            .unwrap_err()
            .contains("partition")
    );
}

#[test]
fn invalid_windows_sequences_and_bindings_fail() {
    let (manifest, events, coverage) = fixture();
    for mutate in [
        |w: &mut CollectionWindow| w.start = at(1),
        |w: &mut CollectionWindow| w.end_exclusive = w.start,
        |w: &mut CollectionWindow| w.end_exclusive = at(11),
        |w: &mut CollectionWindow| w.end_exclusive = at(9),
        |w: &mut CollectionWindow| w.complete = false,
        |w: &mut CollectionWindow| w.receipt_id = "minutes-early".into(),
        |w: &mut CollectionWindow| w.sequence_range = Some(SequenceRange { first: 1, last: 2 }),
    ] {
        let mut invalid = coverage.clone();
        mutate(&mut invalid.streams[0].windows[0]);
        assert!(invalid.verify(&manifest, &events).is_err());
    }
    for mutate in [
        |c: &mut SourceCoverage| c.schema_version = 3,
        |c: &mut SourceCoverage| c.dataset_id.push('!'),
        |c: &mut SourceCoverage| c.dataset_version.push('!'),
        |c: &mut SourceCoverage| c.source_id.push('!'),
        |c: &mut SourceCoverage| c.replay_sha256 = "0".repeat(64),
        |c: &mut SourceCoverage| {
            c.streams[0].sequence_contract = SequenceContract::ContiguousTradeId
        },
        |c: &mut SourceCoverage| {
            c.streams[0].sequence_contract = SequenceContract::ContiguousDepthUpdate
        },
    ] {
        let mut invalid = coverage.clone();
        mutate(&mut invalid);
        assert!(invalid.verify(&manifest, &events).is_err());
    }
}

#[test]
fn invalid_bar_boundaries_and_duplicate_identities_fail_even_when_sealed() {
    let (_, _, coverage) = fixture();
    let mut stream = coverage.streams[0].clone();
    let def = definition(vec![stream.stream.clone()]);
    stream.windows = vec![window("seconds", 0, 2 * DAY, 1)];
    let second = BarInterval::OneSecond;
    for event in [
        bar(-1, 0, second),
        bar(1, 1, second),
        bar(0, 2 * DAY + 1, second),
    ] {
        assert!(seal_observation_dataset(def.clone(), &[event]).is_err());
    }
    let cases = [
        (vec![bar(0, 2, second)], "invalid scheduled bar boundary"),
        (
            vec![bar(0, 1, second), bar(0, 1, second)],
            "duplicate or overlapping bar",
        ),
    ];
    for (events, error) in cases {
        stream.windows[0].expected_count = u64::try_from(events.len()).unwrap();
        let manifest = seal_observation_dataset(def.clone(), &events).unwrap();
        let coverage = receipts(&manifest, 2, vec![stream.clone()]);
        assert!(
            coverage
                .verify(&manifest, &events)
                .unwrap_err()
                .contains(error)
        );
    }
}

#[test]
fn coverage_bounds_and_overflowing_sequence_ranges_fail() {
    let (manifest, events, mut coverage) = fixture();
    let window = coverage.streams[0].windows[0].clone();
    coverage.streams[0].windows = vec![window; 100_001];
    assert!(
        coverage
            .verify(&manifest, &events)
            .unwrap_err()
            .contains("window limit")
    );
    let (manifest, events, mut coverage) = fixture();
    let trade = &mut coverage.streams[6].windows[0];
    trade.expected_count = u64::MAX;
    trade.sequence_range = Some(SequenceRange {
        first: 0,
        last: u64::MAX,
    });
    assert!(
        coverage
            .verify(&manifest, &events)
            .unwrap_err()
            .contains("sequence range")
    );
}

#[test]
fn cli_seals_and_verifies_interval_receipts_then_refuses_tampering() {
    let (manifest, events, mut coverage) = fixture();
    let dir = std::env::temp_dir().join(format!("apexes-interval-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let write = |name: &str, bytes: &[u8]| std::fs::write(dir.join(name), bytes).unwrap();
    write(
        "definition.json",
        &serde_json::to_vec(&manifest.definition).unwrap(),
    );
    write("events.json", &serde_json::to_vec(&events).unwrap());
    let run = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_historical-manifest"))
            .current_dir(&dir)
            .args(args)
            .output()
            .unwrap()
    };
    let seal = run(&["seal-observation", "definition.json", "events.json"]);
    assert!(
        seal.status.success(),
        "{}",
        String::from_utf8_lossy(&seal.stderr)
    );
    write("manifest.json", &seal.stdout);
    write("coverage.json", &serde_json::to_vec(&coverage).unwrap());
    let args = [
        "verify-coverage",
        "manifest.json",
        "events.json",
        "coverage.json",
    ];
    let result = run(&args);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let verified: VerifiedDataset = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(
        verified,
        coverage.verify_dataset(&manifest, &events).unwrap()
    );

    coverage.streams[0].windows[0].expected_count = 1;
    write("coverage.json", &serde_json::to_vec(&coverage).unwrap());
    let failure = run(&args);
    assert!(!failure.status.success());
    assert!(failure.stdout.is_empty());
    assert!(String::from_utf8_lossy(&failure.stderr).contains("collection count"));
    coverage.streams[0].windows[0].expected_count = 2;
    write("coverage.json", &serde_json::to_vec(&coverage).unwrap());
    let mut tampered = events;
    tampered.remove(0);
    write("events.json", &serde_json::to_vec(&tampered).unwrap());
    let failure = run(&args);
    assert!(!failure.status.success());
    assert!(failure.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&failure.stderr).contains("partition count or digest mismatch")
    );
    std::fs::remove_dir_all(dir).unwrap();
}
