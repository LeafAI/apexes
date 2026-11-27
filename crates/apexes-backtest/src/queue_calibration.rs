//! F03.02 — Offline calibration of the isolated passive-order queue model.
//! Historical market data alone cannot identify true queue position. Each
//! sample must include observed maker fills for a known passive order, not
//! simulated labels inferred from the same trade tape.

use apexes_core::{DepthUpdate, Side, TradeEvent, Venue};
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

const MODEL: &str = "isolated-visible-queue-v1";

/// Source classification is supplied by the caller, not independently attested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    Synthetic,
    Recorded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCalibrationInput {
    pub dataset_id: String,
    pub dataset_version: String,
    pub market_source: String,
    pub order_fill_source: String,
    pub evidence: EvidenceKind,
    pub venue: Venue,
    pub symbol: String,
    /// Both training and holdout must contain at least one complete window.
    pub holdout_start: DateTime<Utc>,
    pub max_snapshot_age_ms: u32,
    pub factors: Vec<Decimal>,
    pub observations: Vec<QueueObservation>,
}

/// One isolated, acknowledged passive order, observed continuously from
/// acceptance through the horizon. No other own orders may compete with it.
/// Quantity and fills must use the same exchange units.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueObservation {
    pub order_id: String,
    pub accepted_at: DateTime<Utc>,
    pub observed_until: DateTime<Utc>,
    pub side: Side,
    pub limit_price: Decimal,
    pub quantity: Decimal,
    pub observed_filled: Decimal,
    pub snapshot: DepthUpdate,
    pub trades: Vec<TradeEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CalibrationScore {
    pub samples: usize,
    pub mean_absolute_fill_fraction_error: Decimal,
    pub mean_predicted_fill_fraction: Decimal,
    pub mean_observed_fill_fraction: Decimal,
    pub predicted_full_fills: usize,
    pub observed_full_fills: usize,
    pub full_fill_mismatches: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CandidateScore {
    pub factor: Decimal,
    pub training: CalibrationScore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HoldoutPrediction {
    pub order_id: String,
    pub accepted_at: DateTime<Utc>,
    pub observed_until: DateTime<Utc>,
    pub visible_ahead: Decimal,
    pub eligible_volume: Decimal,
    pub quantity: Decimal,
    pub observed_filled: Decimal,
    pub predicted_filled: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueueCalibrationReport {
    pub model: &'static str,
    pub dataset_id: String,
    pub dataset_version: String,
    pub market_source: String,
    pub order_fill_source: String,
    pub evidence: EvidenceKind,
    pub venue: Venue,
    pub symbol: String,
    pub holdout_start: DateTime<Utc>,
    pub max_snapshot_age_ms: u32,
    pub selected_factor: Decimal,
    pub tied_training_candidates: usize,
    pub training_candidates: Vec<CandidateScore>,
    pub holdout: CalibrationScore,
    pub holdout_predictions: Vec<HoldoutPrediction>,
    /// True only for a unique minimum on the provided training grid.
    /// This is not statistical identification or a deployment decision.
    pub training_identifiable: bool,
}

#[derive(Debug)]
struct Sample<'a> {
    observation: &'a QueueObservation,
    visible: Decimal,
    traded: Decimal,
}

fn invalid(reason: &str) -> String {
    reason.to_owned()
}

fn positive(value: Decimal) -> bool {
    value > Decimal::ZERO
}

fn validated_levels(levels: &[apexes_core::PriceLevel], descending: bool) -> bool {
    !levels.is_empty()
        && levels
            .iter()
            .all(|l| positive(l.price) && positive(l.quantity))
        && levels.windows(2).all(|pair| {
            if descending {
                pair[0].price > pair[1].price
            } else {
                pair[0].price < pair[1].price
            }
        })
}

fn validate_snapshot(
    input: &QueueCalibrationInput,
    o: &QueueObservation,
) -> Result<Decimal, String> {
    let s = &o.snapshot;
    if !s.is_snapshot
        || s.symbol != input.symbol
        || s.venue != input.venue
        || s.timestamp > o.accepted_at
        || o.accepted_at.signed_duration_since(s.timestamp)
            > TimeDelta::milliseconds(i64::from(input.max_snapshot_age_ms))
        || !validated_levels(&s.bids, true)
        || !validated_levels(&s.asks, false)
        || s.bids[0].price >= s.asks[0].price
    {
        return Err(invalid(
            "missing, stale, crossed or invalid causal snapshot",
        ));
    }
    let (levels, crosses) = match o.side {
        Side::Buy => (&s.bids, o.limit_price >= s.asks[0].price),
        Side::Sell => (&s.asks, o.limit_price <= s.bids[0].price),
    };
    if crosses {
        return Err(invalid(
            "observation is not passive at the admission snapshot",
        ));
    }
    levels
        .iter()
        .filter(|l| match o.side {
            Side::Buy => l.price >= o.limit_price,
            Side::Sell => l.price <= o.limit_price,
        })
        .try_fold(Decimal::ZERO, |sum, l| sum.checked_add(l.quantity))
        .ok_or_else(|| invalid("visible depth overflow"))
}

fn eligible_volume<'a>(
    input: &QueueCalibrationInput,
    o: &'a QueueObservation,
    seen: &mut HashSet<&'a String>,
) -> Result<Decimal, String> {
    let mut previous = o.accepted_at;
    let mut volume = Decimal::ZERO;
    for t in &o.trades {
        if t.venue != input.venue
            || t.symbol != input.symbol
            || t.timestamp <= o.accepted_at
            || t.timestamp < previous
            || t.timestamp > o.observed_until
            || !positive(t.price)
            || !positive(t.quantity)
            || t.trade_id
                .as_ref()
                .is_none_or(|id| id.trim().is_empty() || !seen.insert(id))
            || t.side.is_none()
        {
            return Err(invalid("invalid, duplicate, ambiguous or noncausal trade"));
        }
        previous = t.timestamp;
        let eligible = match o.side {
            Side::Buy => t.side == Some(Side::Sell) && t.price <= o.limit_price,
            Side::Sell => t.side == Some(Side::Buy) && t.price >= o.limit_price,
        };
        if eligible {
            volume = volume
                .checked_add(t.quantity)
                .ok_or_else(|| invalid("eligible volume overflow"))?;
        }
    }
    Ok(volume)
}

struct CalibrationSplit<'a> {
    training: Vec<Sample<'a>>,
    holdout: Vec<Sample<'a>>,
    factors: Vec<Decimal>,
}

fn validate(input: &QueueCalibrationInput) -> Result<CalibrationSplit<'_>, String> {
    if [
        &input.dataset_id,
        &input.dataset_version,
        &input.market_source,
        &input.order_fill_source,
        &input.symbol,
    ]
    .iter()
    .any(|s| s.trim().is_empty())
        || input.max_snapshot_age_ms == 0
    {
        return Err(invalid(
            "dataset identity, sources, symbol and snapshot age required",
        ));
    }
    let mut factors = input.factors.clone();
    factors.sort();
    if factors.len() < 2
        || factors.len() > 1001
        || factors.windows(2).any(|p| p[0] == p[1])
        || factors
            .iter()
            .any(|f| !(Decimal::ZERO..=Decimal::ONE).contains(f))
    {
        return Err(invalid("provide 2–1001 distinct queue factors in [0,1]"));
    }
    let mut training = Vec::new();
    let mut holdout = Vec::new();
    let mut ids = HashSet::new();
    let mut trades_seen = HashSet::new();
    let mut previous_end = None;
    for o in &input.observations {
        if o.order_id.trim().is_empty()
            || !ids.insert(&o.order_id)
            || !positive(o.quantity)
            || !positive(o.limit_price)
            || o.observed_filled < Decimal::ZERO
            || o.observed_filled > o.quantity
            || o.observed_until <= o.accepted_at
            || previous_end.is_some_and(|end| o.snapshot.timestamp <= end)
        {
            return Err(invalid(
                "invalid order label or overlapping/unordered observation windows",
            ));
        }
        let sample = Sample {
            observation: o,
            visible: validate_snapshot(input, o)?,
            traded: eligible_volume(input, o, &mut trades_seen)?,
        };
        if o.observed_until < input.holdout_start {
            training.push(sample);
        } else if o.snapshot.timestamp >= input.holdout_start {
            holdout.push(sample);
        } else {
            return Err(invalid("observation crosses the training/holdout boundary"));
        }
        previous_end = Some(o.observed_until);
    }
    if training.is_empty() || holdout.is_empty() {
        return Err(invalid("training and holdout must both be nonempty"));
    }
    Ok(CalibrationSplit {
        training,
        holdout,
        factors,
    })
}

/// Exact isolated-order quantity model used by the resting matcher:
/// external queue consumes eligible prints first, then the own order.
/// Depth changes do not replenish or cancel the seeded queue estimate.
fn predict(sample: &Sample<'_>, factor: Decimal) -> Result<Decimal, String> {
    let ahead = sample
        .visible
        .checked_mul(factor)
        .ok_or_else(|| invalid("queue estimate overflow"))?;
    Ok((sample.traded - sample.traded.min(ahead)).min(sample.observation.quantity))
}

fn score(samples: &[Sample<'_>], factor: Decimal) -> Result<CalibrationScore, String> {
    let mut result = CalibrationScore {
        samples: samples.len(),
        mean_absolute_fill_fraction_error: Decimal::ZERO,
        mean_predicted_fill_fraction: Decimal::ZERO,
        mean_observed_fill_fraction: Decimal::ZERO,
        predicted_full_fills: 0,
        observed_full_fills: 0,
        full_fill_mismatches: 0,
    };
    for sample in samples {
        let o = sample.observation;
        let predicted = predict(sample, factor)?;
        let p = predicted / o.quantity;
        let a = o.observed_filled / o.quantity;
        result.mean_absolute_fill_fraction_error += (p - a).abs();
        result.mean_predicted_fill_fraction += p;
        result.mean_observed_fill_fraction += a;
        let full = predicted == o.quantity;
        let observed_full = o.observed_filled == o.quantity;
        result.predicted_full_fills += usize::from(full);
        result.observed_full_fills += usize::from(observed_full);
        result.full_fill_mismatches += usize::from(full != observed_full);
    }
    let count = Decimal::from(samples.len());
    result.mean_absolute_fill_fraction_error /= count;
    result.mean_predicted_fill_fraction /= count;
    result.mean_observed_fill_fraction /= count;
    Ok(result)
}

/// Select the factor using training mean absolute fill-fraction error only.
/// On equal scores choose the larger, more conservative factor. The holdout
/// is scored exactly once with that selected value and cannot affect it.
pub fn calibrate_queue(input: &QueueCalibrationInput) -> Result<QueueCalibrationReport, String> {
    let CalibrationSplit {
        training,
        holdout,
        factors,
    } = validate(input)?;
    let training_candidates = factors
        .iter()
        .map(|&factor| {
            Ok(CandidateScore {
                factor,
                training: score(&training, factor)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let winner = training_candidates
        .iter()
        .min_by(|a, b| {
            a.training
                .mean_absolute_fill_fraction_error
                .cmp(&b.training.mean_absolute_fill_fraction_error)
                .then_with(|| b.factor.cmp(&a.factor))
        })
        .ok_or_else(|| invalid("no calibration candidates"))?;
    let ties = training_candidates
        .iter()
        .filter(|c| {
            c.training.mean_absolute_fill_fraction_error
                == winner.training.mean_absolute_fill_fraction_error
        })
        .count();
    let factor = winner.factor;
    let holdout_predictions = holdout
        .iter()
        .map(|s| {
            let o = s.observation;
            Ok(HoldoutPrediction {
                order_id: o.order_id.clone(),
                accepted_at: o.accepted_at,
                observed_until: o.observed_until,
                visible_ahead: s.visible,
                eligible_volume: s.traded,
                quantity: o.quantity,
                observed_filled: o.observed_filled,
                predicted_filled: predict(s, factor)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(QueueCalibrationReport {
        model: MODEL,
        dataset_id: input.dataset_id.clone(),
        dataset_version: input.dataset_version.clone(),
        market_source: input.market_source.clone(),
        order_fill_source: input.order_fill_source.clone(),
        evidence: input.evidence,
        venue: input.venue,
        symbol: input.symbol.clone(),
        holdout_start: input.holdout_start,
        max_snapshot_age_ms: input.max_snapshot_age_ms,
        selected_factor: factor,
        tied_training_candidates: ties,
        training_identifiable: ties == 1,
        training_candidates,
        holdout: score(&holdout, factor)?,
        holdout_predictions,
    })
}
