//! Independent read-only reconciliation of direct Spot fee valuations.
//! Third-asset conversion decisions require separate per-fill evidence.
use crate::recovery_ledger::{LedgerContinuityError as E, exact_add, exact_mul};
use apexes_backtest::ledger_evidence::LedgerEvidence;
use apexes_core::{AssetClass, Fill, Instrument, Venue};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap};

#[derive(Default)]
struct DirectFees {
    quantities: BTreeMap<String, Decimal>,
    historical: Decimal,
}

fn accumulate(state: &mut DirectFees, fill: &Fill, instrument: &Instrument) -> Result<(), E> {
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
        return Err(E::Execution);
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
        accumulate(expected.entry(&f.symbol).or_default(), f, instrument)?;
    }
    if expected.len() != evidence.accounts.len() {
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
