//! F04.02: explicit v1-to-v2 export migration and independent verification.
use apexes_backtest::{
    HistoricalMarketDataSource, InMemoryMarketDataSource,
    historical_dataset::{
        DatasetDefinition, DatasetManifest, DatasetStream, ObservationExport, StreamKind,
        migrate_observation_dataset, seal_dataset, seal_observation_dataset,
    },
    source_identity::{IdentityContract, IdentityPolicy, StreamIdentity, normalize_dataset},
};
use apexes_core::{Bar, BarInterval, MarketEvent, TradeEvent, Venue};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal_macros::dec;

fn at(second: i64) -> DateTime<Utc> {
    NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        + chrono::TimeDelta::seconds(second)
}

fn bar(open: i64, close: i64, interval: BarInterval) -> MarketEvent {
    MarketEvent::Bar(Bar {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        open: dec!(100),
        high: dec!(101),
        low: dec!(99),
        close: dec!(100),
        volume: dec!(2),
        interval,
        open_time: at(open),
        close_time: at(close),
    })
}

fn trade(second: i64, id: &str) -> MarketEvent {
    MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        price: dec!(100),
        quantity: dec!(1),
        side: None,
        trade_id: Some(id.into()),
        timestamp: at(second),
    })
}

fn definition() -> DatasetDefinition {
    DatasetDefinition {
        dataset_id: "observation-fixture".into(),
        version: "v1".into(),
        source_id: "synthetic".into(),
        start_day: at(0).date_naive(),
        end_day_exclusive: at(86400).date_naive(),
        streams: [StreamKind::Bar, StreamKind::Trade]
            .into_iter()
            .map(|kind| DatasetStream {
                symbol: "BTCUSDT".into(),
                venue: Venue::Binance,
                kind,
            })
            .collect(),
    }
}

fn events() -> Vec<MarketEvent> {
    vec![
        bar(0, 60, BarInterval::OneMinute),
        bar(0, 1, BarInterval::OneSecond),
        trade(1, "tie-a"),
        trade(1, "tie-b"),
        bar(1, 2, BarInterval::OneSecond),
        trade(60, "later"),
    ]
}

#[tokio::test]
async fn explicit_migration_preserves_events_ties_and_normalized_identity_contract() {
    let original = events();
    let definition = definition();
    let policy = IdentityPolicy {
        schema_version: 1,
        source_id: definition.source_id.clone(),
        streams: definition
            .streams
            .iter()
            .map(|s| StreamIdentity {
                stream: s.clone(),
                contract: match s.kind {
                    StreamKind::Bar => IdentityContract::FinalBar,
                    _ => IdentityContract::OpaqueTradeId,
                },
            })
            .collect(),
    };
    let normalized = normalize_dataset(definition, policy.clone(), &original).unwrap();
    let source = normalized.manifest;
    let migrated = migrate_observation_dataset(&source, &original, "observation-v2").unwrap();
    assert_eq!(migrated.manifest.identity_policy, Some(policy));
    assert_eq!(migrated.source_version, "v1");
    assert_eq!(migrated.source_replay_sha256, source.replay_sha256);
    assert_eq!(
        migrated.events,
        vec![
            original[1].clone(),
            original[2].clone(),
            original[3].clone(),
            original[4].clone(),
            original[0].clone(),
            original[5].clone()
        ]
    );
    assert_eq!(
        migrated
            .manifest
            .verify(&migrated.events)
            .unwrap()
            .event_count,
        6
    );
    assert_eq!(source.verify(&original).unwrap().event_count, 6);
    assert!(source.verify(&migrated.events).is_err());
    assert!(migrated.manifest.verify(&original).is_err());
    assert!(migrate_observation_dataset(&source, &original, "v1").is_err());
    assert!(migrate_observation_dataset(&migrated.manifest, &migrated.events, "v3").is_err());
    let mut tampered = original.clone();
    tampered.swap(2, 3);
    assert!(migrate_observation_dataset(&source, &tampered, "v2").is_err());
    assert!(InMemoryMarketDataSource::from_observation_ordered(original).is_err());
    let replay_source =
        InMemoryMarketDataSource::from_observation_ordered(migrated.events.clone()).unwrap();
    assert_eq!(replay_source.events().await.unwrap(), migrated.events);
    let mut changed = migrated.events.clone();
    changed.swap(1, 2);
    assert!(migrated.manifest.verify(&changed).is_err());
}

#[test]
fn schema_and_horizon_are_fail_closed_without_changing_legacy_seals() {
    let input = vec![trade(1, "a")];
    let v1 = seal_dataset(definition(), &input).unwrap();
    let v2 = seal_observation_dataset(definition(), &input).unwrap();
    assert_eq!(v1.partitions, v2.partitions);
    assert_ne!(v1.replay_sha256, v2.replay_sha256);
    let mut relabeled = v1.clone();
    relabeled.schema_version = 2;
    assert!(relabeled.verify(&input).is_err());
    assert!(v1.verify(&input).is_ok());
    let midnight = vec![bar(86399, 86400, BarInterval::OneSecond)];
    let sealed = seal_observation_dataset(definition(), &midnight).unwrap();
    assert_eq!(sealed.partitions[0].event_count, 1);
    assert!(sealed.verify(&midnight).is_ok());
    for invalid in [
        bar(86399, 86401, BarInterval::OneSecond),
        bar(1, 1, BarInterval::OneSecond),
        bar(2, 1, BarInterval::OneSecond),
    ] {
        assert!(seal_observation_dataset(definition(), &[invalid]).is_err());
    }
    let mut raw = definition();
    raw.source_id = "binance-raw:spot:fixture".into();
    assert!(seal_observation_dataset(raw, &input).is_err());
}

#[test]
fn cli_migration_and_seal_are_explicit_and_do_not_modify_inputs() {
    let dir = std::env::temp_dir().join(format!("apexes-observation-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let input = events();
    let manifest = seal_dataset(definition(), &input).unwrap();
    let source = dir.join("source.json");
    let source_events = dir.join("events.json");
    let source_bytes = serde_json::to_vec(&manifest).unwrap();
    let event_bytes = serde_json::to_vec(&input).unwrap();
    std::fs::write(&source, &source_bytes).unwrap();
    std::fs::write(&source_events, &event_bytes).unwrap();
    let run = |args: &[&std::path::Path]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_historical-manifest"))
            .args(args)
            .output()
            .unwrap()
    };
    let failed = run(&[
        std::path::Path::new("migrate-observation"),
        &source,
        &source_events,
        std::path::Path::new("v1"),
    ]);
    assert!(!failed.status.success());
    assert!(failed.stdout.is_empty());
    let output = run(&[
        std::path::Path::new("migrate-observation"),
        &source,
        &source_events,
        std::path::Path::new("v2"),
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let export: ObservationExport = serde_json::from_slice(&output.stdout).unwrap();
    let definition_path = dir.join("v2-definition.json");
    let ordered = dir.join("v2-events.json");
    let sealed = dir.join("v2-manifest.json");
    std::fs::write(
        &definition_path,
        serde_json::to_vec(&export.manifest.definition).unwrap(),
    )
    .unwrap();
    std::fs::write(&ordered, serde_json::to_vec(&export.events).unwrap()).unwrap();
    let result = run(&[
        std::path::Path::new("seal-observation"),
        &definition_path,
        &ordered,
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<DatasetManifest>(&result.stdout).unwrap(),
        export.manifest
    );
    std::fs::write(&sealed, &result.stdout).unwrap();
    let verified = run(&[std::path::Path::new("verify"), &sealed, &ordered]);
    assert!(verified.status.success());
    assert_eq!(std::fs::read(&source).unwrap(), source_bytes);
    assert_eq!(std::fs::read(&source_events).unwrap(), event_bytes);
    std::fs::remove_dir_all(dir).unwrap();
}
