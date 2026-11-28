//! Offline comparison of local fee classification and tracked asset coverage.
//! This is not proof of fee funding provenance or authorization to restore.
use crate::WalletEvidence;
use crate::recovery_ledger::{
    LedgerContinuityError as E, OpeningWalletEvidence, exact_add, exact_mul,
};
use apexes_backtest::ledger_evidence::LedgerEvidence;
use apexes_core::{AssetClass, Fill, Instrument, Side, Venue};
use apexes_strategy::checkpoint::CheckpointReceipt;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

/// Independently retained opening local asset allocation. Explicit zero
/// entries matter: a zero tracked asset is not an external fee asset.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpeningTrackedAssetEvidence {
    pub version: u32,
    pub predecessor: CheckpointReceipt,
    pub owner_fingerprint: String,
    pub captured_at: DateTime<Utc>,
    pub balances: Vec<(String, Decimal)>,
}

impl OpeningTrackedAssetEvidence {
    pub(crate) fn opening_map(
        &self,
        opening: &OpeningWalletEvidence,
    ) -> Result<BTreeMap<String, Decimal>, E> {
        if self.version != 1
            || self.predecessor != opening.predecessor
            || self.owner_fingerprint != opening.wallet.owner_fingerprint
        {
            return Err(E::Identity);
        }
        if self.captured_at < opening.wallet.window.started_at
            || self.captured_at > opening.wallet.window.finished_at
        {
            return Err(E::Interval);
        }
        if self.balances.len() > 1024
            || !self.balances.windows(2).all(|w| w[0].0 < w[1].0)
            || self.balances.iter().any(|(asset, amount)| {
                amount < &Decimal::ZERO
                    || opening
                        .wallet
                        .balances
                        .get(asset)
                        .is_none_or(|b| b.free != *amount || !b.locked.is_zero())
            })
        {
            return Err(E::Balance);
        }
        Ok(self.balances.iter().cloned().collect())
    }
}

fn credit(balances: &mut BTreeMap<String, Decimal>, asset: &str, delta: Decimal) -> Result<(), E> {
    let amount = balances.entry(asset.to_string()).or_default();
    *amount = exact_add(*amount, delta)?;
    Ok(())
}

fn apply_fill(
    balances: &mut BTreeMap<String, Decimal>,
    fill: &Fill,
    instrument: &Instrument,
    decision: &apexes_backtest::fee_evidence::FeeDecisionEvidence,
) -> Result<(), E> {
    let (base, quote) = instrument
        .base_currency
        .as_deref()
        .zip(instrument.quote_currency.as_deref())
        .ok_or(E::Execution)?;
    if instrument.venue != Venue::Binance
        || instrument.asset_class != AssetClass::Spot
        || fill.venue != instrument.venue
        || base == quote
        || fill.leverage.is_some()
        || fill.margin_mode.is_some()
        || fill.quantity <= Decimal::ZERO
        || fill.price <= Decimal::ZERO
        || fill.fee < Decimal::ZERO
        || (!fill.fee.is_zero() && fill.fee_currency.is_none())
    {
        return Err(E::Execution);
    }
    let asset = fill.fee_currency.as_deref().unwrap_or(quote);
    let external = asset != base && asset != quote && !balances.contains_key(asset);
    if decision.fill_id != fill.id
        || decision.venue != fill.venue
        || decision.symbol != fill.symbol
        || decision.filled_at != fill.filled_at
        || decision.price != fill.price
        || decision.fee != fill.fee
        || decision.asset != asset
        || decision.external_fee != external
    {
        return Err(E::FeeMismatch);
    }
    let notional = exact_mul(fill.price, fill.quantity)?;
    let (base_delta, quote_delta) = match fill.side {
        Side::Buy => (fill.quantity, -notional),
        Side::Sell => (-fill.quantity, notional),
    };
    credit(balances, base, base_delta)?;
    credit(balances, quote, quote_delta)?;
    if !external && !fill.fee.is_zero() {
        credit(balances, asset, -fill.fee)?;
    }
    if balances.values().any(|v| *v < Decimal::ZERO) {
        return Err(E::Negative);
    }
    Ok(())
}

/// Only for a separately qualified exact account journal and wallet
/// continuity. A tracked balance must equal the complete account amount;
/// partial allocations and unexplained funding are outside this contract.
pub(crate) fn compare_external_fee_ownership(
    allocation: &OpeningTrackedAssetEvidence,
    opening: &OpeningWalletEvidence,
    closing: &WalletEvidence,
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
    let decisions = captured
        .fee_valuation
        .as_ref()
        .and_then(|f| f.decisions.as_deref())
        .ok_or(E::FeeMismatch)?;
    if fills.len() > 8192
        || fills.len() != decisions.len()
        || !fills.windows(2).all(|w| w[0].filled_at < w[1].filled_at)
    {
        return Err(E::Execution);
    }
    let mut balances = allocation.opening_map(opening)?;
    let by_symbol = instruments
        .iter()
        .map(|i| (i.symbol.as_str(), i))
        .collect::<HashMap<_, _>>();
    let mut ids = HashSet::new();
    for (fill, decision) in fills.iter().zip(decisions) {
        if !ids.insert(fill.id) {
            return Err(E::FeeMismatch);
        }
        let instrument = by_symbol.get(fill.symbol.as_str()).ok_or(E::Execution)?;
        apply_fill(&mut balances, fill, instrument, decision)?;
    }
    if captured
        .balances
        .iter()
        .map(|(a, v)| (a, v))
        .ne(balances.iter())
        || balances.iter().any(|(asset, amount)| {
            closing
                .balances
                .get(asset)
                .is_none_or(|b| b.free != *amount || !b.locked.is_zero())
        })
    {
        return Err(E::CapturedLedgerMismatch);
    }
    Ok(())
}
