//! Independent read-only reconciliation of Spot fee valuations.
//! Third-asset rates must be independently supplied for exact fills.
use crate::recovery_ledger::{LedgerContinuityError as E, exact_add, exact_mul};
use apexes_backtest::ledger_evidence::LedgerEvidence;
use apexes_core::{AssetClass, Fill, FillId, Instrument, Venue};
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Independently supplied historical conversion observation for one exact
/// third-asset fee. This does not authenticate the observation's source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThirdAssetFeeRate {
    pub fill_id: FillId,
    pub venue: Venue,
    pub symbol: String,
    pub asset: String,
    pub quote: String,
    pub observed_at: DateTime<Utc>,
    pub rate: Decimal,
}

/// Bound to the exact selected boundary and owner; no unbound rate lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThirdAssetFeeEvidence {
    pub version: u32,
    pub boundary: apexes_oms::CoherentCheckpointReceipt,
    pub owner_fingerprint: String,
    /// Exactly one row per nonzero third-asset fee, sorted by fill ID.
    pub rates: Vec<ThirdAssetFeeRate>,
}

impl ThirdAssetFeeEvidence {
    pub(crate) fn compare(
        &self,
        boundary: &apexes_oms::CoherentCheckpointReceipt,
        owner: &str,
        captured: &LedgerEvidence,
        instruments: &[Instrument],
        fills: &[Fill],
    ) -> Result<(), E> {
        if self.version != 1 || &self.boundary != boundary || self.owner_fingerprint != owner {
            return Err(E::Identity);
        }
        if self.rates.len() > 8192
            || !self
                .rates
                .windows(2)
                .all(|w| w[0].fill_id.0 < w[1].fill_id.0)
        {
            return Err(E::FeeMismatch);
        }
        let rates = self.rates.iter().map(|r| (r.fill_id, r)).collect();
        compare_fees(captured, instruments, fills, rates)
    }
}

#[derive(Default)]
struct DirectFees {
    quantities: BTreeMap<String, Decimal>,
    historical: Decimal,
}

fn accumulate(
    state: &mut DirectFees,
    fill: &Fill,
    instrument: &Instrument,
    rates: &mut HashMap<FillId, &ThirdAssetFeeRate>,
) -> Result<(), E> {
    let base = instrument.base_currency.as_deref().ok_or(E::Execution)?;
    let quote = instrument.quote_currency.as_deref().ok_or(E::Execution)?;
    if base.is_empty()
        || quote.is_empty()
        || base == quote
        || fill.venue != instrument.venue
        || fill.leverage.is_some()
        || fill.margin_mode.is_some()
        || fill.quantity <= Decimal::ZERO
        || fill.price <= Decimal::ZERO
        || fill.fee < Decimal::ZERO
    {
        return Err(E::Execution);
    }
    if fill.fee.is_zero() {
        return Ok(());
    }
    let asset = fill.fee_currency.as_deref().ok_or(E::Execution)?;
    let value = if asset == base {
        exact_mul(fill.fee, fill.price)?
    } else if asset == quote {
        fill.fee
    } else {
        let evidence = rates.remove(&fill.id).ok_or(E::Execution)?;
        if evidence.venue != fill.venue
            || evidence.symbol != fill.symbol
            || evidence.asset != asset
            || evidence.quote != quote
            || evidence.rate <= Decimal::ZERO
            || evidence.observed_at > fill.filled_at
            || fill.filled_at.signed_duration_since(evidence.observed_at) > TimeDelta::minutes(5)
        {
            return Err(E::FeeMismatch);
        }
        exact_mul(fill.fee, evidence.rate)?
    };
    let total = state.quantities.entry(asset.to_string()).or_default();
    *total = exact_add(*total, fill.fee)?;
    state.historical = exact_add(state.historical, value)?;
    Ok(())
}

/// Compare direct fee quantities, known historical subtotal and absence
/// of missing/external valuations for every actually traded symbol.
/// No valuation window is assumed to prove a past conversion decision.
pub(crate) fn compare_direct_fees(
    captured: &LedgerEvidence,
    instruments: &[Instrument],
    fills: &[Fill],
) -> Result<(), E> {
    compare_fees(captured, instruments, fills, HashMap::new())
}

fn compare_fees(
    captured: &LedgerEvidence,
    instruments: &[Instrument],
    fills: &[Fill],
    mut rates: HashMap<FillId, &ThirdAssetFeeRate>,
) -> Result<(), E> {
    let scope = instruments
        .iter()
        .map(|i| (i.venue, i.symbol.clone()))
        .collect::<Vec<_>>();
    captured
        .validate(&scope)
        .map_err(|_| E::CapturedLedgerMismatch)?;
    let evidence = captured.fee_valuation.as_ref().ok_or(E::FeeMismatch)?;
    if fills.len() > 8192
        || instruments
            .iter()
            .any(|i| i.venue != Venue::Binance || i.asset_class != AssetClass::Spot)
    {
        return Err(E::Execution);
    }
    let instruments = instruments
        .iter()
        .map(|i| (i.symbol.as_str(), i))
        .collect::<HashMap<_, _>>();
    let mut expected = HashMap::<&str, DirectFees>::new();
    for f in fills {
        let instrument = instruments.get(f.symbol.as_str()).ok_or(E::Execution)?;
        accumulate(
            expected.entry(&f.symbol).or_default(),
            f,
            instrument,
            &mut rates,
        )?;
    }
    if !rates.is_empty() || expected.len() != evidence.accounts.len() {
        return Err(E::FeeMismatch);
    }
    for account in &evidence.accounts {
        let i = instruments
            .get(account.symbol.as_str())
            .ok_or(E::FeeMismatch)?;
        let fees = expected
            .get(account.symbol.as_str())
            .ok_or(E::FeeMismatch)?;
        if account.venue != i.venue
            || Some(account.base.as_str()) != i.base_currency.as_deref()
            || Some(account.quote.as_str()) != i.quote_currency.as_deref()
            || account
                .fees
                .iter()
                .map(|(a, v)| (a, v))
                .ne(fees.quantities.iter())
            || account.historical_fee != fees.historical
            || account.missing_execution_valuations != 0
            || !account.external_fees.is_empty()
        {
            return Err(E::FeeMismatch);
        }
    }
    Ok(())
}
