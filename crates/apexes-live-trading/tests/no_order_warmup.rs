//! Actual Drift interpreter qualification with no database or order executor.
use apexes_backtest::observation_receipt::{
    CompletedObservation, ObservationClock, ObservationReceipt,
};
use apexes_core::{AssetClass, Instrument, MarketEvent, MarketType, TradeEvent, Venue};
use apexes_live_trading::{
    OpeningObservationEvidence, RecoverySelection, WarmupInspectionError, inspect_no_order_warmup,
};
use apexes_strategy::{
    Strategy, StrategyContext,
    checkpoint::{self, CheckpointReceipt, StateCursor, StateIdentity},
    drift_dsl::{DriftScriptStrategy, WarmupError},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

fn script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/no_order_warmup.drift")
}

fn cursor(ordinal: u64) -> StateCursor {
    StateCursor {
        ordinal,
        observed_at: DateTime::<Utc>::from_timestamp(1_800_000_000 + ordinal as i64, 0).unwrap(),
    }
}

fn instruments() -> Vec<Instrument> {
    ["BTC", "ETH"]
        .into_iter()
        .map(|base| {
            Instrument::new(
                format!("{base}USDT"),
                MarketType::Crypto,
                Venue::Binance,
                AssetClass::Spot,
                Some(base.into()),
                Some("USDT".into()),
                dec!(0.01),
                dec!(0.001),
                dec!(1),
            )
            .unwrap()
        })
        .collect()
}

fn record(ordinal: u64, price: Decimal, symbol: &str) -> CompletedObservation {
    CompletedObservation {
        cursor: cursor(ordinal),
        event: MarketEvent::Trade(TradeEvent {
            symbol: symbol.into(),
            venue: Venue::Binance,
            price,
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: cursor(ordinal).observed_at,
        }),
    }
}

fn receipt(
    records: &[CompletedObservation],
    instruments: &[Instrument],
    path: &Path,
) -> (RecoverySelection, OpeningObservationEvidence) {
    let params = HashMap::from([("factor".into(), dec!(3))]);
    let mut strategy =
        DriftScriptStrategy::from_file_with_scoped_checkpoints(path, params.clone(), instruments)
            .unwrap();
    for record in records {
        let ctx = StrategyContext {
            positions: &HashMap::new(),
            balances: &HashMap::new(),
            open_orders: &HashMap::new(),
            open_parent_orders: &HashMap::new(),
            margin_status: &HashMap::new(),
            now: record.cursor.observed_at,
            market: None,
            book: None,
            costs: None,
            capital: None,
            health: None,
        };
        assert!(strategy.on_market_event(&record.event, &ctx).is_empty());
    }
    let identity = StateIdentity {
        strategy: "warmup-test".into(),
        revision: "1".into(),
        config_sha256: "a".repeat(64),
        scope: "test".into(),
        payload_version: 1,
    };
    let through = records.last().unwrap().cursor;
    let bytes = checkpoint::capture(&strategy, identity.clone(), through).unwrap();
    let selected = checkpoint::verify_checkpoint(&bytes, &identity, through).unwrap();
    let opening = opening(&selected, records, instruments, "b");
    (
        RecoverySelection::new("b".repeat(64), selected, params).unwrap(),
        opening,
    )
}

fn opening(
    target: &CheckpointReceipt,
    records: &[CompletedObservation],
    instruments: &[Instrument],
    owner: &str,
) -> OpeningObservationEvidence {
    let scope = instruments
        .iter()
        .map(|i| (i.venue, i.symbol.clone()))
        .collect::<Vec<_>>();
    let receipt = ObservationReceipt::build(
        cursor(1),
        target.cursor,
        ObservationClock::SourceAvailability,
        &scope,
        records,
    )
    .unwrap();
    OpeningObservationEvidence::new(owner.repeat(64), target.clone(), receipt).unwrap()
}

#[test]
fn independently_selected_warmup_matches_reference_and_refuses_changed_history() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _enter = runtime.enter();
    let instruments = instruments();
    let records = vec![
        record(2, dec!(100), "BTCUSDT"),
        record(3, dec!(200), "ETHUSDT"),
        record(4, dec!(105), "BTCUSDT"),
    ];
    let (selection, evidence) = receipt(&records, &instruments, &script());
    for _ in 0..2 {
        assert_eq!(
            inspect_no_order_warmup(&selection, &script(), &instruments, &evidence, &records)
                .unwrap(),
            3
        );
    }
    let mut changed = records.clone();
    changed[1] = record(3, dec!(201), "ETHUSDT");
    assert!(matches!(
        inspect_no_order_warmup(
            &selection,
            Path::new("absent-script"),
            &instruments,
            &evidence,
            &changed
        ),
        Err(WarmupInspectionError::Evidence(_))
    ));
    let changed_receipt = opening(selection.receipt(), &changed, &instruments, "b");
    assert!(matches!(
        inspect_no_order_warmup(
            &selection,
            &script(),
            &instruments,
            &changed_receipt,
            &changed
        ),
        Err(WarmupInspectionError::Mismatch)
    ));
    let wrong_owner = opening(selection.receipt(), &records, &instruments, "c");
    assert!(matches!(
        inspect_no_order_warmup(&selection, &script(), &instruments, &wrong_owner, &records),
        Err(WarmupInspectionError::Evidence(_))
    ));
    assert!(matches!(
        inspect_no_order_warmup(
            &selection,
            &script(),
            &instruments[..1],
            &evidence,
            &records
        ),
        Err(WarmupInspectionError::Evidence(_))
    ));
    let mut future = records.clone();
    if let MarketEvent::Trade(t) = &mut future[1].event {
        t.timestamp = cursor(5).observed_at;
    }
    assert!(matches!(
        inspect_no_order_warmup(&selection, &script(), &instruments, &evidence, &future),
        Err(WarmupInspectionError::Evidence(_))
    ));
    let wrong_params =
        RecoverySelection::new("b".repeat(64), selection.receipt().clone(), HashMap::new())
            .unwrap();
    assert!(matches!(
        inspect_no_order_warmup(&wrong_params, &script(), &instruments, &evidence, &records),
        Err(WarmupInspectionError::Mismatch)
    ));
}

#[test]
fn no_order_hook_refuses_nonempty_malformed_state_and_account_context() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _enter = runtime.enter();
    let instruments = instruments();
    let records = [record(2, dec!(100), "BTCUSDT")];
    let (selection, _) = receipt(&records, &instruments, &script());
    for (price, expected) in [
        (dec!(998), WarmupError::NonemptyReturn),
        (dec!(999), WarmupError::NonemptyReturn),
        (dec!(997), WarmupError::State),
        (dec!(996), WarmupError::Callback),
    ] {
        let records = [record(2, price, "BTCUSDT")];
        let evidence = opening(selection.receipt(), &records, &instruments, "b");
        let result =
            inspect_no_order_warmup(&selection, &script(), &instruments, &evidence, &records);
        assert!(
            matches!(result, Err(WarmupInspectionError::Callback { ordinal: 2, error }) if error == expected),
            "{result:?}"
        );
    }
    let mut unscoped = DriftScriptStrategy::from_file(&script()).unwrap();
    assert_eq!(
        unscoped.warmup_observation(&records[0].event, records[0].cursor.observed_at),
        Err(WarmupError::Unsupported)
    );
}
