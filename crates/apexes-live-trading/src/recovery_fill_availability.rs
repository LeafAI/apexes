//! Offline comparison against independently supplied observation prefixes.
//! A prefix witness is not a callback receipt or proof of actual interleaving.
use crate::{ConversionScopeEvidence, LedgerContinuityError as E};
use apexes_backtest::{
    fee_evidence::{FeeDecisionEvidence, FeeDecisionSource},
    ledger_evidence::LedgerEvidence,
    observation_receipt::CompletedObservation,
};
use apexes_core::{Fill, FillId, MarketEvent, Venue};
use apexes_oms::CoherentCheckpointReceipt;
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

pub use apexes_backtest::observation_receipt::FillObservationPrefix;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FillObservationAvailability {
    pub version: u32,
    pub boundary: CoherentCheckpointReceipt,
    pub owner_fingerprint: String,
    /// Exactly one entry per retained decision, in attribution order.
    pub prefixes: Vec<FillObservationPrefix>,
}

type Windows<'a> = BTreeMap<(&'a str, &'a str), BTreeMap<DateTime<Utc>, Decimal>>;

fn record_price<'a>(
    windows: &mut Windows<'a>,
    scope: &'a ConversionScopeEvidence,
    record: &CompletedObservation,
) -> Result<(), E> {
    let MarketEvent::Trade(trade) = &record.event else {
        return Ok(());
    };
    let role = scope
        .instruments
        .iter()
        .find(|r| r.symbol == trade.symbol)
        .ok_or(E::Execution)?;
    if role.venue != trade.venue {
        return Err(E::Execution);
    }
    if trade.price > Decimal::ZERO {
        let history = windows.entry((&role.base, &role.quote)).or_default();
        history.insert(trade.timestamp, trade.price);
        if history.len() > 2048 {
            history.pop_first();
        }
    }
    Ok(())
}

fn expected_decision(
    scope: &ConversionScopeEvidence,
    windows: &Windows<'_>,
    d: &FeeDecisionEvidence,
    f: &Fill,
) -> Result<(FeeDecisionSource, Option<Decimal>), E> {
    let role = scope
        .instruments
        .iter()
        .find(|r| r.symbol == f.symbol)
        .ok_or(E::Execution)?;
    let asset = f.fee_currency.as_deref().unwrap_or(&role.quote);
    if d.fill_id != f.id
        || d.symbol != f.symbol
        || d.venue != f.venue
        || role.venue != f.venue
        || f.venue != Venue::Binance
        || d.asset != asset
        || d.filled_at != f.filled_at
        || d.price != f.price
        || d.fee != f.fee
        || f.fee < Decimal::ZERO
        || f.price <= Decimal::ZERO
        || f.quantity <= Decimal::ZERO
        || f.leverage.is_some()
        || f.margin_mode.is_some()
        || (!f.fee.is_zero() && f.fee_currency.is_none())
    {
        return Err(E::FeeMismatch);
    }
    let (source, value) = if f.fee.is_zero() {
        (FeeDecisionSource::Zero, Some(Decimal::ZERO))
    } else if asset == role.base {
        (
            FeeDecisionSource::Base,
            Some(crate::recovery_ledger::exact_mul(f.fee, f.price)?),
        )
    } else if asset == role.quote {
        (FeeDecisionSource::Quote, Some(f.fee))
    } else if let Some((at, rate)) = windows
        .get(&(asset, role.quote.as_str()))
        .and_then(|h| h.range(..=f.filled_at).next_back())
        .filter(|(at, _)| f.filled_at.signed_duration_since(**at) <= TimeDelta::minutes(5))
    {
        (
            FeeDecisionSource::Observed {
                observed_at: *at,
                rate: *rate,
            },
            Some(crate::recovery_ledger::exact_mul(f.fee, *rate)?),
        )
    } else {
        (FeeDecisionSource::Missing, None)
    };
    Ok((source, value))
}

/// Differences under a supplied pre-start observation history. Neither
/// the counterfactual nor unchanged decisions are runtime restore evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpeningValuationDifference {
    pub fill_id: FillId,
    pub retained_source: FeeDecisionSource,
    pub retained_value: Option<Decimal>,
    pub warmup_source: FeeDecisionSource,
    pub warmup_value: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpeningValuationPreview {
    pub compared_fills: usize,
    pub missing_after_warmup: usize,
    pub differences: Vec<OpeningValuationDifference>,
}

impl FillObservationAvailability {
    /// Only after receipt-bound scope and final-window checks. Prefixes
    /// are independent caller assertions, not inferred from fill timestamps.
    pub(crate) fn compare(
        &self,
        boundary: &CoherentCheckpointReceipt,
        owner: &str,
        scope: &ConversionScopeEvidence,
        captured: &LedgerEvidence,
        records: &[CompletedObservation],
        fills: &[Fill],
    ) -> Result<(), E> {
        self.compare_with_opening(boundary, owner, scope, captured, records, fills, &[])
            .map(|_| ())
    }

    /// Verify the actual cold-start decision first; then independently
    /// evaluate the same fills with the supplied opening windows.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn compare_with_opening(
        &self,
        boundary: &CoherentCheckpointReceipt,
        owner: &str,
        scope: &ConversionScopeEvidence,
        captured: &LedgerEvidence,
        records: &[CompletedObservation],
        fills: &[Fill],
        opening: &[CompletedObservation],
    ) -> Result<OpeningValuationPreview, E> {
        if self.version != 1 || &self.boundary != boundary || self.owner_fingerprint != owner {
            return Err(E::Identity);
        }
        let decisions = captured
            .fee_valuation
            .as_ref()
            .and_then(|f| f.decisions.as_deref())
            .ok_or(E::FeeMismatch)?;
        if records.len() > 8192
            || opening.len() > 8192
            || fills.len() > 8192
            || self.prefixes.len() != fills.len()
            || decisions.len() != fills.len()
        {
            return Err(E::FeeMismatch);
        }
        let mut journal = fills.iter().map(|f| (f.id, f)).collect::<HashMap<_, _>>();
        if journal.len() != fills.len() {
            return Err(E::FeeMismatch);
        }
        let mut windows = Windows::new();
        let mut warmed = Windows::new();
        for record in opening {
            record_price(&mut warmed, scope, record)?;
        }
        let mut preview = OpeningValuationPreview {
            compared_fills: fills.len(),
            missing_after_warmup: 0,
            differences: Vec::new(),
        };
        let mut consumed = 0;
        for (prefix, decision) in self.prefixes.iter().zip(decisions) {
            if prefix.fill_id != decision.fill_id
                || prefix.observations < consumed
                || prefix.observations > records.len()
            {
                return Err(E::FeeMismatch);
            }
            for record in &records[consumed..prefix.observations] {
                record_price(&mut windows, scope, record)?;
                record_price(&mut warmed, scope, record)?;
            }
            consumed = prefix.observations;
            let fill = journal.remove(&prefix.fill_id).ok_or(E::FeeMismatch)?;
            let (source, value) = expected_decision(scope, &windows, decision, fill)?;
            if decision.source != source || decision.value != value {
                return Err(E::FeeMismatch);
            }
            let (warmup_source, warmup_value) = expected_decision(scope, &warmed, decision, fill)?;
            if warmup_value.is_none() {
                preview.missing_after_warmup += 1;
            }
            if warmup_source != source || warmup_value != value {
                preview.differences.push(OpeningValuationDifference {
                    fill_id: fill.id,
                    retained_source: source,
                    retained_value: value,
                    warmup_source,
                    warmup_value,
                });
            }
        }
        if !journal.is_empty() {
            return Err(E::FeeMismatch);
        }
        Ok(preview)
    }
}
