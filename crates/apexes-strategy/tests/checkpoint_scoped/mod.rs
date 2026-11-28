//! F04.05: actual Drift interpreter restoration through the versioned envelope.
use super::*;
use apexes_core::{AssetClass, Instrument, MarketType};
use apexes_strategy::checkpoint::{self, CheckpointError, StateCursor, StateIdentity};

fn instruments() -> Vec<Instrument> {
    [
        ("BTCUSDT", Venue::Binance),
        ("ETHUSDT", Venue::Binance),
        ("BTCUSDT", Venue::Okx),
    ]
    .into_iter()
    .map(|(symbol, venue)| {
        Instrument::new(
            symbol,
            MarketType::Crypto,
            venue,
            AssetClass::Spot,
            Some(symbol.trim_end_matches("USDT").into()),
            Some("USDT".into()),
            dec!(0.01),
            dec!(0.001),
            dec!(1),
        )
        .unwrap()
    })
    .collect()
}

fn initial_params() -> HashMap<String, Decimal> {
    HashMap::from([("initial_factor".into(), dec!(3)), ("hot".into(), dec!(2))])
}

fn candidate() -> DriftScriptStrategy {
    DriftScriptStrategy::from_file_with_scoped_checkpoints(
        &fixture_path("checkpoint_scoped.drift"),
        initial_params(),
        &instruments(),
    )
    .unwrap()
}

fn cursor(step: u64) -> StateCursor {
    StateCursor {
        ordinal: step,
        observed_at: chrono::DateTime::from_timestamp_millis(1_700_000_000_000 + step as i64 * 500)
            .unwrap(),
    }
}

fn identity() -> StateIdentity {
    StateIdentity {
        strategy: "scoped-drift".into(),
        revision: "v1".into(),
        config_sha256: "f".repeat(64),
        scope: "paper:owner-a:three-instruments".into(),
        payload_version: 1,
    }
}

fn run_step(
    strategy: &mut DriftScriptStrategy,
    step: u64,
    price: Decimal,
    instrument: &Instrument,
) -> Vec<apexes_strategy::OrderIntent> {
    let now = cursor(step).observed_at;
    let (positions, balances, open_orders, _) = empty_ctx(now);
    let ctx = StrategyContext {
        health: None,
        capital: None,
        costs: None,
        book: None,
        market: None,
        positions: &positions,
        balances: &balances,
        open_orders: &open_orders,
        open_parent_orders: &HashMap::new(),
        margin_status: &HashMap::new(),
        now,
    };
    let event = MarketEvent::Trade(TradeEvent {
        symbol: instrument.symbol.clone(),
        venue: instrument.venue,
        price,
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: now,
    });
    strategy.on_market_event(&event, &ctx)
}

#[test]
fn scoped_drift_restart_preserves_intents_hot_params_and_committed_payload() {
    with_runtime(|| {
        let instruments = instruments();
        let identity = identity();
        let mut reference = candidate();
        // Each instrument has its own indicator warm-up and elapsed gate.
        for step in 1..=8 {
            let instrument = &instruments[(step as usize - 1) % instruments.len()];
            run_step(
                &mut reference,
                step,
                dec!(10) + Decimal::from(step),
                instrument,
            );
        }
        reference.set_param("hot", dec!(11)).unwrap();
        // A hot change to the top-level parameter does not alter its
        // load-time value. The fresh interpreter must use original overrides.
        reference.set_param("initial_factor", dec!(77)).unwrap();
        let before_invalid = reference.checkpoint_payload().unwrap();
        assert!(run_step(&mut reference, 9, dec!(998), &instruments[2]).is_empty());
        assert_eq!(reference.checkpoint_payload().unwrap(), before_invalid);
        let bytes = checkpoint::capture(&reference, identity.clone(), cursor(9)).unwrap();
        let mut restored = candidate();
        checkpoint::restore(&mut restored, candidate(), &bytes, &identity, cursor(9)).unwrap();
        assert_eq!(restored.current_params(), reference.current_params());
        assert_eq!(
            checkpoint::capture(&restored, identity.clone(), cursor(9)).unwrap(),
            bytes
        );
        let mut emitted = 0;
        for step in 10..=35 {
            let instrument = &instruments[(step as usize - 1) % instruments.len()];
            let price = dec!(10) + Decimal::from(step);
            let a = run_step(&mut reference, step, price, instrument);
            let b = run_step(&mut restored, step, price, instrument);
            emitted += a.len();
            assert_eq!(format!("{a:?}"), format!("{b:?}"), "step {step}");
            assert_eq!(
                checkpoint::capture(&reference, identity.clone(), cursor(step)).unwrap(),
                checkpoint::capture(&restored, identity.clone(), cursor(step)).unwrap(),
                "step {step}"
            );
        }
        assert!(emitted > 0);
    });
}

#[test]
fn scoped_restore_refuses_wrong_script_configuration_or_instrument_universe() {
    with_runtime(|| {
        let mut original = candidate();
        run_step(&mut original, 1, dec!(10), &instruments()[0]);
        let identity = identity();
        let bytes = checkpoint::capture(&original, identity.clone(), cursor(1)).unwrap();
        let mut target = candidate();
        let unchanged = target.checkpoint_payload().unwrap();
        let other_script = DriftScriptStrategy::from_file_with_scoped_checkpoints(
            &fixture_path("symbol_state.drift"),
            initial_params(),
            &instruments(),
        )
        .unwrap();
        let other_params = DriftScriptStrategy::from_file_with_scoped_checkpoints(
            &fixture_path("checkpoint_scoped.drift"),
            HashMap::new(),
            &instruments(),
        )
        .unwrap();
        let other_scope = DriftScriptStrategy::from_file_with_scoped_checkpoints(
            &fixture_path("checkpoint_scoped.drift"),
            initial_params(),
            &instruments()[0..1],
        )
        .unwrap();
        for wrong in [other_script, other_params, other_scope] {
            assert_eq!(
                checkpoint::restore(&mut target, wrong, &bytes, &identity, cursor(1)),
                Err(CheckpointError::Identity)
            );
            assert_eq!(target.checkpoint_payload().unwrap(), unchanged);
        }
        let legacy =
            DriftScriptStrategy::from_file(&fixture_path("checkpoint_scoped.drift")).unwrap();
        assert_eq!(
            checkpoint::capture(&legacy, identity.clone(), cursor(1)),
            Err(CheckpointError::Unsupported)
        );
        assert!(
            DriftScriptStrategy::from_file_with_scoped_checkpoints(
                &fixture_path("checkpoint_scoped.drift"),
                initial_params(),
                &[]
            )
            .is_err()
        );
        let mut foreign = instruments()[0].clone();
        foreign.venue = Venue::Coinbase;
        assert!(run_step(&mut target, 2, dec!(10), &foreign).is_empty());
        assert_eq!(target.checkpoint_payload().unwrap(), unchanged);
    });
}
