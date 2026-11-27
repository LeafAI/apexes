use apexes_backtest::{
    CostModel, FlatFeeSchedule, MatchingEngine, OrderIntent, OrderRouting,
    queue_calibration::{EvidenceKind, QueueCalibrationInput, QueueObservation, calibrate_queue},
};
use apexes_core::{AssetClass, DepthUpdate, OrderType, PriceLevel, Side, TradeEvent, Venue};
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + TimeDelta::seconds(seconds)
}

fn observation(index: i64, side: Side, volume: Decimal, filled: Decimal) -> QueueObservation {
    let accepted_at = at(index * 10);
    let limit_price = match side {
        Side::Buy => dec!(100),
        Side::Sell => dec!(101),
    };
    QueueObservation {
        order_id: format!("order-{index}"),
        accepted_at,
        observed_until: accepted_at + TimeDelta::seconds(3),
        side,
        limit_price,
        quantity: dec!(4),
        observed_filled: filled,
        snapshot: DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(8),
            }],
            asks: vec![PriceLevel {
                price: dec!(101),
                quantity: dec!(8),
            }],
            sequence: Some(index as u64),
            is_snapshot: true,
            timestamp: accepted_at - TimeDelta::milliseconds(50),
        },
        trades: vec![TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: limit_price,
            quantity: volume,
            side: Some(match side {
                Side::Buy => Side::Sell,
                Side::Sell => Side::Buy,
            }),
            trade_id: Some(format!("trade-{index}")),
            timestamp: accepted_at + TimeDelta::seconds(1),
        }],
    }
}

fn fixture() -> QueueCalibrationInput {
    QueueCalibrationInput {
        dataset_id: "synthetic-isolated-maker".into(),
        dataset_version: "1".into(),
        market_source: "synthetic depth/trade fixtures".into(),
        order_fill_source: "synthetic maker labels".into(),
        evidence: EvidenceKind::Synthetic,
        venue: Venue::Binance,
        symbol: "BTCUSDT".into(),
        holdout_start: at(25),
        max_snapshot_age_ms: 100,
        factors: vec![dec!(1), dec!(0.5), dec!(0), dec!(0.25), dec!(0.75)],
        observations: vec![
            observation(1, Side::Buy, dec!(6), dec!(2)),
            observation(2, Side::Sell, dec!(5), dec!(1)),
            observation(3, Side::Sell, dec!(7), dec!(3)),
            observation(4, Side::Buy, dec!(3), dec!(0)),
        ],
    }
}

#[test]
fn recovers_known_factor_with_reproducible_holdout_report() {
    let input = fixture();
    let report = calibrate_queue(&input).unwrap();
    assert_eq!(report.selected_factor, dec!(0.5));
    assert_eq!(report.tied_training_candidates, 1);
    assert!(report.training_identifiable);
    assert_eq!(report.evidence, EvidenceKind::Synthetic);
    assert_eq!(report.holdout.samples, 2);
    assert_eq!(
        report.holdout.mean_absolute_fill_fraction_error,
        Decimal::ZERO
    );
    assert_eq!(report.holdout.mean_observed_fill_fraction, dec!(0.375));
    assert_eq!(report.holdout.mean_predicted_fill_fraction, dec!(0.375));
    assert_eq!(report.holdout_predictions[0].predicted_filled, dec!(3));
    assert_eq!(report, calibrate_queue(&input).unwrap());
    let mut reordered = input.clone();
    reordered.factors.reverse();
    assert_eq!(report, calibrate_queue(&reordered).unwrap());
}

#[test]
fn changing_holdout_labels_cannot_change_training_selection() {
    let mut input = fixture();
    let original = calibrate_queue(&input).unwrap();
    input.observations[2].observed_filled = dec!(0);
    input.observations[3].observed_filled = dec!(4);
    let altered = calibrate_queue(&input).unwrap();
    assert_eq!(original.selected_factor, altered.selected_factor);
    assert_eq!(original.training_candidates, altered.training_candidates);
    assert_eq!(
        altered.holdout.mean_absolute_fill_fraction_error,
        dec!(0.875)
    );
    assert_eq!(altered.holdout.full_fill_mismatches, 1);
}

#[test]
fn uninformative_training_ties_are_not_claimed_as_identification() {
    let mut input = fixture();
    for observation in &mut input.observations[..2] {
        observation.trades[0].quantity = dec!(100);
        observation.observed_filled = observation.quantity;
    }
    let report = calibrate_queue(&input).unwrap();
    assert_eq!(report.selected_factor, dec!(1));
    assert_eq!(report.tied_training_candidates, 5);
    assert!(!report.training_identifiable);
}

#[test]
fn rejects_split_leakage_overlap_and_missing_holdout() {
    let mut input = fixture();
    input.holdout_start = at(21);
    assert!(calibrate_queue(&input).unwrap_err().contains("boundary"));
    let mut input = fixture();
    input.observations[1].snapshot.timestamp = input.observations[0].observed_until;
    assert!(calibrate_queue(&input).unwrap_err().contains("overlapping"));
    let mut input = fixture();
    input.observations.truncate(2);
    assert!(calibrate_queue(&input).unwrap_err().contains("nonempty"));
}

#[test]
fn rejects_future_stale_crossed_and_nonpassive_snapshots() {
    let mut input = fixture();
    input.observations[0].snapshot.timestamp =
        input.observations[0].accepted_at + TimeDelta::milliseconds(1);
    assert!(calibrate_queue(&input).is_err());
    let mut input = fixture();
    input.observations[0].snapshot.timestamp =
        input.observations[0].accepted_at - TimeDelta::microseconds(100001);
    assert!(calibrate_queue(&input).is_err());
    let mut input = fixture();
    input.observations[0].snapshot.bids[0].price = dec!(101);
    assert!(calibrate_queue(&input).is_err());
    let mut input = fixture();
    input.observations[0].limit_price = dec!(101);
    assert!(calibrate_queue(&input).unwrap_err().contains("not passive"));
}

#[test]
fn rejects_duplicate_ambiguous_and_out_of_window_trades() {
    let mut input = fixture();
    let duplicate = input.observations[0].trades[0].clone();
    input.observations[0].trades.push(duplicate);
    assert!(calibrate_queue(&input).is_err());
    let mut input = fixture();
    input.observations[1].trades[0].trade_id = input.observations[0].trades[0].trade_id.clone();
    assert!(calibrate_queue(&input).is_err());
    let mut input = fixture();
    input.observations[0].trades[0].side = None;
    assert!(calibrate_queue(&input).is_err());
    let mut input = fixture();
    input.observations[0].trades[0].timestamp = input.observations[0].accepted_at;
    assert!(calibrate_queue(&input).is_err());
    let mut input = fixture();
    input.observations[0].trades[0].timestamp =
        input.observations[0].observed_until + TimeDelta::milliseconds(1);
    assert!(calibrate_queue(&input).is_err());
}

#[test]
fn rejects_invalid_factor_grid_labels_and_scope() {
    let mut input = fixture();
    input.factors.push(dec!(1.1));
    assert!(calibrate_queue(&input).is_err());
    let mut input = fixture();
    input.factors.push(dec!(0.5));
    assert!(calibrate_queue(&input).is_err());
    let mut input = fixture();
    input.observations[0].observed_filled = dec!(5);
    assert!(calibrate_queue(&input).is_err());
    let mut input = fixture();
    input.observations[0].snapshot.symbol = "ETHUSDT".into();
    assert!(calibrate_queue(&input).is_err());
}

#[test]
fn isolated_prediction_matches_actual_resting_engine_for_every_candidate() {
    let mut input = fixture();
    // Multiple prints and at-or-better depth. Noneligible aggressors must not
    // deplete the model's queue; include them in the chronological tape.
    for o in &mut input.observations {
        match o.side {
            Side::Buy => o.snapshot.bids.insert(
                0,
                PriceLevel {
                    price: dec!(100.5),
                    quantity: dec!(2),
                },
            ),
            Side::Sell => o.snapshot.asks.insert(
                0,
                PriceLevel {
                    price: dec!(100.5),
                    quantity: dec!(2),
                },
            ),
        }
        let mut other = o.trades[0].clone();
        other.quantity = dec!(10);
        other.side = Some(o.side);
        other.trade_id = Some(format!("noneligible-{}", o.order_id));
        other.timestamp = o.accepted_at + TimeDelta::milliseconds(500);
        o.trades.insert(0, other);
        let mut later = o.trades[1].clone();
        later.quantity = dec!(1.5);
        later.trade_id = Some(format!("later-{}", o.order_id));
        later.timestamp = o.accepted_at + TimeDelta::seconds(2);
        o.trades.push(later);
    }
    let report = calibrate_queue(&input).unwrap();
    for candidate in &report.training_candidates {
        let mut error = Decimal::ZERO;
        for o in &input.observations[..2] {
            let predicted = engine_fill(o, candidate.factor);
            error += ((predicted - o.observed_filled) / o.quantity).abs();
        }
        assert_eq!(
            candidate.training.mean_absolute_fill_fraction_error,
            error / dec!(2)
        );
    }
    for (o, prediction) in input.observations[2..]
        .iter()
        .zip(&report.holdout_predictions)
    {
        assert_eq!(
            engine_fill(o, report.selected_factor),
            prediction.predicted_filled
        );
    }
}

fn engine_fill(o: &QueueObservation, factor: Decimal) -> Decimal {
    let mut engine = MatchingEngine::new(
        CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: Decimal::ZERO,
                taker_bps: Decimal::ZERO,
            }),
            funding_rate_bps: None,
        },
        factor,
    );
    engine.on_depth_update(&o.snapshot);
    engine
        .submit_limit_order(
            &OrderIntent {
                symbol: o.snapshot.symbol.clone(),
                venue: o.snapshot.venue,
                asset_class: AssetClass::Spot,
                side: o.side,
                order_type: OrderType::Limit,
                quantity: o.quantity,
                price: Some(o.limit_price),
                stop_price: None,
                execution_algo: None,
                routing: OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            },
            o.accepted_at,
        )
        .unwrap();
    o.trades
        .iter()
        .flat_map(|t| engine.on_trade_event(t, t.timestamp))
        .flat_map(|(_, fills)| fills)
        .map(|f| f.quantity)
        .sum()
}

#[test]
fn cli_reads_versioned_synthetic_fixture_and_outputs_same_report() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/queue-calibration-synthetic.json"
    );
    let input: QueueCalibrationInput =
        serde_json::from_reader(std::fs::File::open(path).unwrap()).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_queue-calibrate"))
        .arg(path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let expected = serde_json::to_value(calibrate_queue(&input).unwrap()).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual["selected_factor"], "0.5");
    assert_eq!(actual["evidence"], "synthetic");
}
