//! F04.05 — reconstruct an isolated cold-start Spot ledger from exact,
//! independently qualified opening allocation and retained application order.
//! Never expose the ledger or advance a live recovery claim.
use crate::{LedgerContinuityError as E, OpeningTrackedAssetEvidence, OpeningWalletEvidence};
use apexes_backtest::{
    ledger::PortfolioLedger,
    observation_receipt::{CapturedObservations, CompletedObservation},
};
use apexes_core::{AssetClass, Fill, Instrument, MarketEvent, Venue};
use rust_decimal::Decimal;
use std::collections::HashMap;

fn observe(ledger: &mut PortfolioLedger, records: &[CompletedObservation]) {
    for record in records {
        if let MarketEvent::Trade(trade) = &record.event {
            ledger.observe_spot_price(&trade.symbol, trade.timestamp, trade.price);
        }
    }
}

/// This deliberately conservative numeric envelope bounds the unchecked
/// Decimal intermediates in the ordinary ledger implementation. The
/// independent cash flow and P&L checks must also have succeeded first.
/// Reject rather than approximate or panic for unqualified magnitudes.
fn bounded(value: Decimal) -> bool {
    value.abs() <= Decimal::from(1_000_000_000_u64) && value.normalize().scale() <= 12
}

pub(crate) fn compare_applied_ledger(
    capture: &CapturedObservations,
    instruments: &[Instrument],
    opening: &OpeningWalletEvidence,
    allocation: &OpeningTrackedAssetEvidence,
    fills: &[Fill],
) -> Result<(), E> {
    let retained = capture.ledger.as_ref().ok_or(E::CapturedLedgerMismatch)?;
    let prefixes = capture
        .fill_observation_prefixes
        .as_ref()
        .ok_or(E::FeeMismatch)?;
    let initial = allocation.opening_map(opening)?;
    if instruments.is_empty()
        || instruments.len() > 128
        || fills.len() > 8192
        || capture.records.len() > 8192
        || prefixes.len() != fills.len()
        || initial.values().any(|v| !bounded(*v))
        || fills.iter().any(|f| {
            !bounded(f.price)
                || !bounded(f.quantity)
                || !bounded(f.fee)
                || f.price <= Decimal::ZERO
                || f.quantity <= Decimal::ZERO
                || f.fee < Decimal::ZERO
                || f.leverage.is_some()
                || f.margin_mode.is_some()
                || f.venue != Venue::Binance
        })
        || capture
            .records
            .iter()
            .any(|r| matches!(&r.event, MarketEvent::Trade(t) if !bounded(t.price)))
        || instruments
            .iter()
            .any(|i| i.venue != Venue::Binance || i.asset_class != AssetClass::Spot)
    {
        return Err(E::Execution);
    }
    let by_symbol: HashMap<_, _> = instruments
        .iter()
        .map(|i| (i.symbol.clone(), i.clone()))
        .collect();
    if by_symbol.len() != instruments.len() {
        return Err(E::Execution);
    }
    let mut journal: HashMap<_, _> = fills.iter().map(|f| (f.id, f)).collect();
    if journal.len() != fills.len() {
        return Err(E::FeeMismatch);
    }
    let mut ledger = PortfolioLedger::new(by_symbol, initial.into_iter().collect());
    let mut consumed = 0;
    for prefix in prefixes {
        if prefix.observations < consumed || prefix.observations > capture.records.len() {
            return Err(E::FeeMismatch);
        }
        observe(&mut ledger, &capture.records[consumed..prefix.observations]);
        consumed = prefix.observations;
        let fill = journal.remove(&prefix.fill_id).ok_or(E::FeeMismatch)?;
        // All independent exact arithmetic, journal, scope and fee checks
        // precede this call; discard on any failed application.
        ledger
            .apply_fill(fill)
            .map_err(|_| E::ApplicationMismatch)?;
    }
    observe(&mut ledger, &capture.records[consumed..]);
    if !journal.is_empty()
        || ledger
            .inspect_evidence()
            .map_err(|_| E::ApplicationMismatch)?
            != *retained
    {
        return Err(E::ApplicationMismatch);
    }
    Ok(())
}
