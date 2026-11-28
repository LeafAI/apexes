//! Independent read-only reconciliation of Spot fee valuations.
//! Third-asset rates must be independently supplied for exact fills.
use crate::recovery_ledger::{LedgerContinuityError as E, exact_add, exact_mul};
use apexes_backtest::fee_evidence::{FeeDecisionEvidence, FeeDecisionSource};
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
        self.compare_aggregates(boundary, owner, captured, instruments, fills, false)
    }

    fn compare_aggregates(
        &self,
        boundary: &apexes_oms::CoherentCheckpointReceipt,
        owner: &str,
        captured: &LedgerEvidence,
        instruments: &[Instrument],
        fills: &[Fill],
        classified_external: bool,
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
        compare_fees(captured, instruments, fills, rates, classified_external)
    }

    /// Stronger comparison: require every actual retained valuation decision
    /// to match the independently supplied evidence for its exact fill ID.
    pub(crate) fn compare_decisions(
        &self,
        boundary: &apexes_oms::CoherentCheckpointReceipt,
        owner: &str,
        captured: &LedgerEvidence,
        instruments: &[Instrument],
        fills: &[Fill],
    ) -> Result<(), E> {
        self.compare_exact_decisions(boundary, owner, captured, instruments, fills, false)
    }

    /// Compare classification and exact valuations together. External
    /// classification is independently checked before using the wider
    /// private comparison; legacy public inspection remains strict.
    pub(crate) fn compare_with_ownership(
        &self,
        boundary: &apexes_oms::CoherentCheckpointReceipt,
        owner: &str,
        captured: &LedgerEvidence,
        instruments: &[Instrument],
        fills: &[Fill],
        ownership: (
            &crate::OpeningTrackedAssetEvidence,
            &crate::OpeningWalletEvidence,
            &crate::WalletEvidence,
        ),
    ) -> Result<(), E> {
        crate::recovery_fee_ownership::compare_external_fee_ownership(
            ownership.0,
            ownership.1,
            ownership.2,
            captured,
            instruments,
            fills,
        )?;
        self.compare_exact_decisions(boundary, owner, captured, instruments, fills, true)
    }

    fn compare_exact_decisions(
        &self,
        boundary: &apexes_oms::CoherentCheckpointReceipt,
        owner: &str,
        captured: &LedgerEvidence,
        instruments: &[Instrument],
        fills: &[Fill],
        classified_external: bool,
    ) -> Result<(), E> {
        self.compare_aggregates(
            boundary,
            owner,
            captured,
            instruments,
            fills,
            classified_external,
        )?;
        let decisions = captured
            .fee_valuation
            .as_ref()
            .and_then(|e| e.decisions.as_deref())
            .ok_or(E::FeeMismatch)?;
        if decisions.len() != fills.len() {
            return Err(E::FeeMismatch);
        }
        let mut journal = fills.iter().map(|f| (f.id, f)).collect::<HashMap<_, _>>();
        if journal.len() != fills.len() {
            return Err(E::FeeMismatch);
        }
        let instruments = instruments
            .iter()
            .map(|i| (i.symbol.as_str(), i))
            .collect::<HashMap<_, _>>();
        let rates = self
            .rates
            .iter()
            .map(|r| (r.fill_id, r))
            .collect::<HashMap<_, _>>();
        for decision in decisions {
            let fill = journal.remove(&decision.fill_id).ok_or(E::FeeMismatch)?;
            let instrument = instruments
                .get(fill.symbol.as_str())
                .ok_or(E::FeeMismatch)?;
            compare_decision(
                decision,
                fill,
                instrument,
                rates.get(&fill.id).copied(),
                classified_external,
            )?;
        }
        if !journal.is_empty() {
            return Err(E::FeeMismatch);
        }
        Ok(())
    }
}

fn compare_decision(
    decision: &FeeDecisionEvidence,
    fill: &Fill,
    instrument: &Instrument,
    rate: Option<&ThirdAssetFeeRate>,
    classified_external: bool,
) -> Result<(), E> {
    let base = instrument.base_currency.as_deref().ok_or(E::Execution)?;
    let quote = instrument.quote_currency.as_deref().ok_or(E::Execution)?;
    let asset = fill.fee_currency.as_deref().unwrap_or(quote);
    if decision.venue != fill.venue
        || decision.symbol != fill.symbol
        || decision.asset != asset
        || decision.filled_at != fill.filled_at
        || decision.price != fill.price
        || decision.fee != fill.fee
        || (!classified_external && decision.external_fee)
    {
        return Err(E::FeeMismatch);
    }
    let (source, value) = if fill.fee.is_zero() {
        (FeeDecisionSource::Zero, Decimal::ZERO)
    } else if asset == base {
        (FeeDecisionSource::Base, exact_mul(fill.fee, fill.price)?)
    } else if asset == quote {
        (FeeDecisionSource::Quote, fill.fee)
    } else {
        let rate = rate.ok_or(E::FeeMismatch)?;
        (
            FeeDecisionSource::Observed {
                observed_at: rate.observed_at,
                rate: rate.rate,
            },
            exact_mul(fill.fee, rate.rate)?,
        )
    };
    if decision.source != source || decision.value != Some(value) {
        return Err(E::FeeMismatch);
    }
    Ok(())
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
    compare_fees(captured, instruments, fills, HashMap::new(), false)
}

fn compare_fees(
    captured: &LedgerEvidence,
    instruments: &[Instrument],
    fills: &[Fill],
    mut rates: HashMap<FillId, &ThirdAssetFeeRate>,
    classified_external: bool,
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
            || (!classified_external && !account.external_fees.is_empty())
        {
            return Err(E::FeeMismatch);
        }
    }
    Ok(())
}

#[cfg(test)]
mod combined_tests {
    use super::*;
    use crate::{
        ClosingFeeMarkEvidence, ClosingFeeMarkRate, ClosingFeeRevaluation, EvidenceWindow,
        OpeningTrackedAssetEvidence, OpeningWalletEvidence, SpotWalletBalance, WalletEvidence,
    };
    use apexes_backtest::ledger::PortfolioLedger;
    use apexes_core::{MarketType, OrderId, Side};
    use apexes_strategy::checkpoint::{CheckpointReceipt, StateCursor, StateIdentity};
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn at(t: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + t, 0).unwrap()
    }
    fn wallet(t: i64, rows: &[(&str, Decimal)]) -> WalletEvidence {
        WalletEvidence {
            window: EvidenceWindow {
                started_at: at(t),
                finished_at: at(t),
            },
            owner_fingerprint: "owner".into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            account_wide_complete: true,
            balances: rows
                .iter()
                .map(|(a, v)| {
                    (
                        a.to_string(),
                        SpotWalletBalance {
                            free: *v,
                            locked: Decimal::ZERO,
                        },
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn combined_external_fee_components_reconcile_without_weakening_legacy_scope() {
        let instruments = ["BNB", "BTC"]
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
            .collect::<Vec<_>>();
        let mut ledger = PortfolioLedger::new(
            instruments
                .iter()
                .map(|i| (i.symbol.clone(), i.clone()))
                .collect(),
            [("USDT".into(), dec!(1000))].into_iter().collect(),
        );
        ledger.observe_spot_price("BNBUSDT", at(1), dec!(300));
        let fills = [
            (Side::Buy, dec!(1), dec!(100), Decimal::ZERO, "USDT", 2),
            (Side::Sell, dec!(0.5), dec!(120), dec!(1), "USDT", 3),
            (Side::Buy, dec!(0.5), dec!(100), dec!(0.01), "BNB", 4),
        ]
        .into_iter()
        .map(|(side, quantity, price, fee, asset, t)| Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            side,
            price,
            quantity,
            fee,
            fee_currency: Some(asset.into()),
            filled_at: at(t),
            leverage: None,
            margin_mode: None,
        })
        .collect::<Vec<_>>();
        for f in &fills {
            ledger.apply_fill(f).unwrap();
        }
        ledger.observe_spot_price("BNBUSDT", at(10), dec!(400));
        let captured = ledger.inspect_evidence().unwrap();
        let report = ledger.spot_pnl();
        assert_eq!(report["BTCUSDT"].net_realized, Some(dec!(6)));
        assert_eq!(report["BTCUSDT"].fee_revaluation, Some(dec!(1)));
        let predecessor = CheckpointReceipt {
            identity: StateIdentity {
                strategy: "test".into(),
                revision: "1".into(),
                config_sha256: "a".repeat(64),
                scope: "scope".into(),
                payload_version: 1,
            },
            cursor: StateCursor {
                ordinal: 1,
                observed_at: at(0),
            },
            envelope_sha256: "b".repeat(64),
        };
        let opening = OpeningWalletEvidence {
            predecessor: predecessor.clone(),
            wallet: wallet(
                1,
                &[("BTC", dec!(0)), ("USDT", dec!(1000)), ("BNB", dec!(1))],
            ),
        };
        let closing = wallet(
            15,
            &[("BTC", dec!(1)), ("USDT", dec!(909)), ("BNB", dec!(0.99))],
        );
        let allocation = OpeningTrackedAssetEvidence {
            version: 1,
            predecessor: predecessor.clone(),
            owner_fingerprint: "owner".into(),
            captured_at: at(1),
            balances: vec![("USDT".into(), dec!(1000))],
        };
        let mut checkpoint = predecessor;
        checkpoint.cursor = StateCursor {
            ordinal: 5,
            observed_at: at(12),
        };
        let boundary = apexes_oms::CoherentCheckpointReceipt {
            version: 1,
            checkpoint,
            run_scope: Uuid::new_v4(),
            last_intent: 3,
            fill_sequence: 3,
            terminal_sequence: 3,
            order_count: 3,
            fill_count: 3,
            journal_sha256: "c".repeat(64),
            capture_sha256: Some("d".repeat(64)),
        };
        let rates = ThirdAssetFeeEvidence {
            version: 1,
            boundary: boundary.clone(),
            owner_fingerprint: "owner".into(),
            rates: vec![ThirdAssetFeeRate {
                fill_id: fills[2].id,
                venue: Venue::Binance,
                symbol: "BTCUSDT".into(),
                asset: "BNB".into(),
                quote: "USDT".into(),
                observed_at: at(1),
                rate: dec!(300),
            }],
        };
        let marks = ClosingFeeMarkEvidence {
            version: 1,
            boundary: boundary.clone(),
            owner_fingerprint: "owner".into(),
            rates: vec![ClosingFeeMarkRate {
                venue: Venue::Binance,
                asset: "BNB".into(),
                quote: "USDT".into(),
                observed_at: at(10),
                rate: dec!(400),
            }],
            symbols: vec![ClosingFeeRevaluation {
                symbol: "BTCUSDT".into(),
                fee_at_execution: dec!(4),
                fee_at_mark: dec!(5),
                fee_revaluation: dec!(1),
            }],
        };
        let check =
            |r: &ThirdAssetFeeEvidence, c: &LedgerEvidence, a: &OpeningTrackedAssetEvidence| {
                r.compare_with_ownership(
                    &boundary,
                    "owner",
                    c,
                    &instruments,
                    &fills,
                    (a, &opening, &closing),
                )?;
                crate::recovery_pnl::compare_fee_bearing_pnl(c, &instruments, &fills)?;
                marks.compare(&boundary, "owner", c, &instruments)
            };
        check(&rates, &captured, &allocation).unwrap();
        assert_eq!(
            rates.compare_decisions(&boundary, "owner", &captured, &instruments, &fills),
            Err(E::FeeMismatch)
        );
        let mut changed = rates.clone();
        changed.rates[0].rate = dec!(301);
        assert_eq!(check(&changed, &captured, &allocation), Err(E::FeeMismatch));
        let mut changed = allocation.clone();
        changed.balances.insert(0, ("BNB".into(), dec!(1)));
        assert_eq!(check(&rates, &captured, &changed), Err(E::FeeMismatch));
        let mut changed = captured.clone();
        changed.realized_pnl[0].1 = dec!(11);
        assert_eq!(
            check(&rates, &changed, &allocation),
            Err(E::RealizedPnlMismatch)
        );
        let mut changed = captured.clone();
        changed.fee_valuation.as_mut().unwrap().prices[0]
            .observations
            .last_mut()
            .unwrap()
            .1 = dec!(401);
        assert_eq!(check(&rates, &changed, &allocation), Err(E::FeeMismatch));
        // The price-only conversion instrument is still considered an owned
        // base by the current public recovery gate. Do not relax that gate.
        assert!(matches!(
            crate::spot_recovery::recover_spot_positions(
                &instruments,
                &fills,
                &closing.balances,
                false
            ),
            Err(crate::spot_recovery::SpotRecoveryError::InvalidFill(_))
        ));
    }
}
