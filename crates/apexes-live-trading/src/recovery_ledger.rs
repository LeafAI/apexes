//! Offline closed-account cash-flow continuity, not a restart authorization.
use crate::spot_recovery::SpotWalletBalance;
use crate::{RecoveryEvidenceWindow, WalletEvidence};
use apexes_core::{AssetClass, Fill, Instrument, Side, Venue};
use apexes_oms::CoherentRecoverySnapshot;
use apexes_strategy::checkpoint::CheckpointReceipt;
use rust_decimal::Decimal;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct OpeningWalletEvidence {
    pub predecessor: CheckpointReceipt,
    pub wallet: WalletEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LedgerContinuityError {
    #[error("opening evidence does not bind to the selected predecessor and owner")]
    Identity,
    #[error("opening evidence interval is not before execution history")]
    Interval,
    #[error("opening and closing wallet asset coverage or amounts are invalid")]
    Balance,
    #[error("invalid or unsupported historical execution scope or fee")]
    Execution,
    #[error("cash-flow arithmetic cannot be represented exactly")]
    Arithmetic,
    #[error("cash-flow history implies negative holdings")]
    Negative,
    #[error("closing balance differs from opening balance plus durable cash flows")]
    Mismatch,
    #[error("independently retained position evidence differs from the journal preview")]
    PositionMismatch,
    #[error("actual captured ledger is missing or differs from qualified independent evidence")]
    CapturedLedgerMismatch,
    #[error("captured realized P&L differs from the independently reconstructed journal")]
    RealizedPnlMismatch,
    #[error("captured fee attribution differs from independently reconstructed evidence")]
    FeeMismatch,
    #[error("isolated ledger fill application failed or exact closing state differed")]
    ApplicationMismatch,
}

/// Independently retained closing position evidence. Only a read-only
/// comparison is supported; this cannot seed a ledger or authorize startup.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosingSpotPosition {
    pub symbol: String,
    pub quantity: Decimal,
    pub average_entry_price: Decimal,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosingPositionEvidence {
    pub version: u32,
    pub owner_fingerprint: String,
    pub boundary: apexes_oms::CoherentCheckpointReceipt,
    pub captured_at: chrono::DateTime<chrono::Utc>,
    /// Explicit complete scope, including instruments with no position.
    pub symbols: Vec<String>,
    /// Sorted nonzero strategy-owned positions, not account inventory.
    pub positions: Vec<ClosingSpotPosition>,
}

impl ClosingPositionEvidence {
    pub(crate) fn compare(
        &self,
        boundary: &apexes_oms::CoherentCheckpointReceipt,
        owner: &str,
        instruments: &[Instrument],
        wallet: &WalletEvidence,
        preview: &crate::CoherentRecoveryPreview,
    ) -> Result<(), LedgerContinuityError> {
        use LedgerContinuityError as E;
        if self.version != 1
            || &self.boundary != boundary
            || self.owner_fingerprint != owner
            || preview.cursor != boundary.checkpoint.cursor
        {
            return Err(E::Identity);
        }
        if self.captured_at < wallet.window.started_at
            || self.captured_at > wallet.window.finished_at
            || self.captured_at < boundary.checkpoint.cursor.observed_at
        {
            return Err(E::Interval);
        }
        if instruments.is_empty()
            || instruments.len() > 128
            || self.symbols.len() != instruments.len()
            || self.positions.len() > self.symbols.len()
        {
            return Err(E::Execution);
        }
        let mut symbols = instruments
            .iter()
            .map(|i| i.symbol.as_str())
            .collect::<Vec<_>>();
        symbols.sort_unstable();
        if self
            .symbols
            .iter()
            .map(String::as_str)
            .ne(symbols.iter().copied())
            || !self.symbols.windows(2).all(|w| w[0] < w[1])
            || self
                .symbols
                .iter()
                .any(|s| s.is_empty() || s.len() > 128 || s.chars().any(char::is_control))
            || instruments
                .iter()
                .any(|i| i.venue != Venue::Binance || i.asset_class != AssetClass::Spot)
            || !self.positions.windows(2).all(|w| w[0].symbol < w[1].symbol)
            || self.positions.iter().any(|p| {
                self.symbols.binary_search(&p.symbol).is_err()
                    || p.quantity <= Decimal::ZERO
                    || p.average_entry_price <= Decimal::ZERO
            })
        {
            return Err(E::Execution);
        }
        if preview.owned_positions.len() != self.positions.len()
            || self.positions.iter().any(|p| {
                preview.owned_positions.get(&p.symbol).is_none_or(|v| {
                    v.symbol != p.symbol
                        || v.quantity != p.quantity
                        || v.average_entry_price != p.average_entry_price
                        || v.leverage.is_some()
                        || v.margin_mode.is_some()
                        || !v.allocated_margin.is_zero()
                })
            })
        {
            return Err(E::PositionMismatch);
        }
        Ok(())
    }
}

/// Compare only the supported basic maps. Exact asset coverage is
/// required; unrelated account holdings must not be inferred as local
/// ledger state. Realized P&L and fee history remain unqualified.
pub(crate) fn compare_captured_ledger(
    captured: &apexes_backtest::ledger_evidence::LedgerEvidence,
    instruments: &[Instrument],
    preview: &crate::CoherentRecoveryPreview,
) -> Result<(), LedgerContinuityError> {
    use LedgerContinuityError as E;
    let scope = instruments
        .iter()
        .map(|i| (i.venue, i.symbol.clone()))
        .collect::<Vec<_>>();
    captured
        .validate(&scope)
        .map_err(|_| E::CapturedLedgerMismatch)?;
    if captured.balances.len() != preview.wallet_balances.len()
        || captured.balances.iter().any(|(asset, amount)| {
            preview
                .wallet_balances
                .get(asset)
                .is_none_or(|balance| balance.free != *amount || !balance.locked.is_zero())
        })
    {
        return Err(E::CapturedLedgerMismatch);
    }
    let nonzero = captured.positions.iter().filter(|p| !p.quantity.is_zero());
    if nonzero.clone().count() != preview.owned_positions.len()
        || nonzero
            .into_iter()
            .any(|p| preview.owned_positions.get(&p.symbol) != Some(p))
    {
        return Err(E::CapturedLedgerMismatch);
    }
    Ok(())
}

/// Independent offline gross realized P&L check for an explicitly
/// restricted fee-free Spot journal. Refuse ambiguous chronology or
/// unrepresentable arithmetic; never approximate missing evidence.
pub(crate) fn compare_realized_pnl(
    captured: &apexes_backtest::ledger_evidence::LedgerEvidence,
    instruments: &[Instrument],
    fills: &[Fill],
) -> Result<(), LedgerContinuityError> {
    use LedgerContinuityError as E;
    let scope = instruments
        .iter()
        .map(|i| (i.venue, i.symbol.clone()))
        .collect::<Vec<_>>();
    captured
        .validate(&scope)
        .map_err(|_| E::CapturedLedgerMismatch)?;
    if fills.len() > 8192
        || instruments
            .iter()
            .any(|i| i.venue != Venue::Binance || i.asset_class != AssetClass::Spot)
    {
        return Err(E::Execution);
    }
    let mut positions: HashMap<&str, (Decimal, Decimal, chrono::DateTime<chrono::Utc>)> =
        HashMap::new();
    let mut realized: HashMap<String, Decimal> = HashMap::new();
    for f in fills {
        if f.venue != Venue::Binance
            || f.leverage.is_some()
            || f.margin_mode.is_some()
            || !f.fee.is_zero()
            || f.price <= Decimal::ZERO
            || f.quantity <= Decimal::ZERO
            || !scope.iter().any(|(v, s)| *v == f.venue && s == &f.symbol)
        {
            return Err(E::Execution);
        }
        let entry = positions.entry(&f.symbol);
        let (quantity, average, last) = match entry {
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert((Decimal::ZERO, Decimal::ZERO, f.filled_at))
            }
            std::collections::hash_map::Entry::Occupied(v) => {
                if v.get().2 >= f.filled_at {
                    return Err(E::Execution);
                }
                v.into_mut()
            }
        };
        match f.side {
            Side::Buy => {
                let cost = exact_add(
                    exact_mul(*quantity, *average)?,
                    exact_mul(f.quantity, f.price)?,
                )?;
                *quantity = exact_add(*quantity, f.quantity)?;
                let price = cost.checked_div(*quantity).ok_or(E::Arithmetic)?;
                if exact_mul(price, *quantity)? != cost {
                    return Err(E::Arithmetic);
                }
                *average = price;
            }
            Side::Sell => {
                if f.quantity > *quantity {
                    return Err(E::Execution);
                }
                let pnl = exact_mul(exact_add(f.price, -*average)?, f.quantity)?;
                let total = realized.entry(f.symbol.clone()).or_default();
                *total = exact_add(*total, pnl)?;
                *quantity = exact_add(*quantity, -f.quantity)?;
            }
        }
        *last = f.filled_at;
    }
    let mut expected = realized.into_iter().collect::<Vec<_>>();
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    if captured.realized_pnl != expected {
        return Err(E::RealizedPnlMismatch);
    }
    Ok(())
}

fn exact_decimal(mut n: i128, mut scale: u32) -> Result<Decimal, LedgerContinuityError> {
    while scale > 0 && n % 10 == 0 {
        n /= 10;
        scale -= 1;
    }
    Decimal::try_from_i128_with_scale(n, scale).map_err(|_| LedgerContinuityError::Arithmetic)
}

pub(crate) fn exact_add(a: Decimal, b: Decimal) -> Result<Decimal, LedgerContinuityError> {
    let a = a.normalize();
    let b = b.normalize();
    let scale = a.scale().max(b.scale());
    let scaled = |v: Decimal| v.mantissa().checked_mul(10_i128.pow(scale - v.scale()));
    let n = scaled(a)
        .and_then(|a| scaled(b).and_then(|b| a.checked_add(b)))
        .ok_or(LedgerContinuityError::Arithmetic)?;
    exact_decimal(n, scale)
}

pub(crate) fn exact_mul(a: Decimal, b: Decimal) -> Result<Decimal, LedgerContinuityError> {
    let a = a.normalize();
    let b = b.normalize();
    let n = a
        .mantissa()
        .checked_mul(b.mantissa())
        .ok_or(LedgerContinuityError::Arithmetic)?;
    exact_decimal(n, a.scale() + b.scale())
}

fn apply(
    balances: &mut HashMap<String, Decimal>,
    asset: &str,
    delta: Decimal,
) -> Result<(), LedgerContinuityError> {
    let amount = balances
        .get_mut(asset)
        .ok_or(LedgerContinuityError::Balance)?;
    *amount = exact_add(*amount, delta)?;
    Ok(())
}

/// Derive closing account balances from the independent opening evidence
/// and exact journal. No deposit, withdrawal, transfer, rebate, or other
/// unexplained movement is inferred. This is not proof of provenance.
pub(crate) fn validate_ledger_continuity(
    opening: &OpeningWalletEvidence,
    closing: &WalletEvidence,
    selection: (&CheckpointReceipt, &str),
    evidence: &RecoveryEvidenceWindow,
    snapshot: &CoherentRecoverySnapshot,
    instruments: &[Instrument],
    fills: &[Fill],
) -> Result<(), LedgerContinuityError> {
    use LedgerContinuityError as E;
    let (predecessor, owner) = selection;
    let initial = &opening.wallet;
    if &opening.predecessor != predecessor
        || initial.owner_fingerprint != owner
        || initial.venue != Venue::Binance
        || initial.asset_class != AssetClass::Spot
        || !initial.account_wide_complete
    {
        return Err(E::Identity);
    }
    let earliest = snapshot
        .orders
        .iter()
        .map(|o| o.created_at)
        .chain(fills.iter().map(|f| f.filled_at))
        .chain(std::iter::once(evidence.terminal_window.started_at))
        .min()
        .ok_or(E::Interval)?;
    if initial.window.started_at < predecessor.cursor.observed_at
        || initial.window.finished_at < initial.window.started_at
        || initial.window.finished_at > earliest
    {
        return Err(E::Interval);
    }
    let valid = |b: &SpotWalletBalance| b.free >= Decimal::ZERO && b.locked.is_zero();
    if initial.balances.len() > 1024
        || initial.balances.len() != closing.balances.len()
        || initial
            .balances
            .iter()
            .any(|(asset, b)| !valid(b) || !closing.balances.contains_key(asset))
    {
        return Err(E::Balance);
    }
    let mut balances: HashMap<_, _> = initial
        .balances
        .iter()
        .map(|(a, b)| (a.clone(), b.free))
        .collect();
    let scope: HashMap<_, _> = instruments.iter().map(|i| (i.symbol.as_str(), i)).collect();
    if scope.len() != instruments.len() || scope.is_empty() {
        return Err(E::Execution);
    }
    for f in fills {
        let i = scope.get(f.symbol.as_str()).ok_or(E::Execution)?;
        let (base, quote) = i
            .base_currency
            .as_deref()
            .zip(i.quote_currency.as_deref())
            .ok_or(E::Execution)?;
        if i.venue != Venue::Binance
            || i.asset_class != AssetClass::Spot
            || f.venue != i.venue
            || base == quote
            || f.leverage.is_some()
            || f.margin_mode.is_some()
            || f.price <= Decimal::ZERO
            || f.quantity <= Decimal::ZERO
            || f.fee < Decimal::ZERO
        {
            return Err(E::Execution);
        }
        let notional = exact_mul(f.quantity, f.price)?;
        match f.side {
            Side::Buy => {
                apply(&mut balances, base, f.quantity)?;
                apply(&mut balances, quote, -notional)?;
            }
            Side::Sell => {
                apply(&mut balances, base, -f.quantity)?;
                apply(&mut balances, quote, notional)?;
            }
        }
        if !f.fee.is_zero() {
            apply(
                &mut balances,
                f.fee_currency.as_deref().ok_or(E::Execution)?,
                -f.fee,
            )?;
        }
        // Apply all legs of a fill before checking the resulting holdings.
        if balances.values().any(|v| *v < Decimal::ZERO) {
            return Err(E::Negative);
        }
    }
    if balances
        .iter()
        .any(|(asset, value)| closing.balances.get(asset).is_none_or(|b| b.free != *value))
    {
        return Err(E::Mismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EvidenceWindow, OpenOrderCoverage};
    use apexes_core::{FillId, MarketType, OrderId};
    use apexes_strategy::checkpoint::{StateCursor, StateIdentity};
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn at(s: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + s, 0).unwrap()
    }

    fn wallet(at: DateTime<Utc>, amounts: &[(&str, Decimal)]) -> WalletEvidence {
        WalletEvidence {
            window: EvidenceWindow {
                started_at: at,
                finished_at: at,
            },
            owner_fingerprint: "owner".into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            account_wide_complete: true,
            balances: amounts
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

    fn fixture() -> (
        OpeningWalletEvidence,
        WalletEvidence,
        RecoveryEvidenceWindow,
        CoherentRecoverySnapshot,
        Vec<Instrument>,
        Vec<Fill>,
    ) {
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
            predecessor,
            wallet: wallet(
                at(1),
                &[("BTC", dec!(4)), ("USDT", dec!(1000)), ("BNB", dec!(1))],
            ),
        };
        let close = wallet(
            at(15),
            &[
                ("BTC", dec!(4.49)),
                ("USDT", dec!(959)),
                ("BNB", dec!(0.99)),
            ],
        );
        let evidence = RecoveryEvidenceWindow {
            terminal_window: EvidenceWindow {
                started_at: at(10),
                finished_at: at(11),
            },
            terminal_owner_fingerprint: "owner".into(),
            terminal_evidence_complete: true,
            terminal_orders: vec![],
            open_orders: OpenOrderCoverage {
                window: EvidenceWindow {
                    started_at: at(11),
                    finished_at: at(12),
                },
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                owner_fingerprint: "owner".into(),
                account_wide_complete: true,
                open_order_count: 0,
                foreign_order_count: 0,
            },
        };
        let instrument = Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            Some("BTC".into()),
            Some("USDT".into()),
            dec!(0.01),
            dec!(0.001),
            dec!(1),
        )
        .unwrap();
        let fill = |side, qty, price, fee, asset: &str, t| Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            side,
            price,
            quantity: qty,
            fee,
            fee_currency: Some(asset.into()),
            filled_at: at(t),
            leverage: None,
            margin_mode: None,
        };
        let fills = vec![
            fill(Side::Buy, dec!(1), dec!(100), dec!(0.01), "BTC", 2),
            fill(Side::Sell, dec!(0.5), dec!(120), dec!(1), "USDT", 3),
            fill(Side::Buy, dec!(0.01), dec!(100), dec!(0.01), "BNB", 4),
        ];
        // Third fill contributes +0.01 BTC and -1 USDT.
        let mut close = close;
        close.balances.get_mut("BTC").unwrap().free = dec!(4.50);
        close.balances.get_mut("USDT").unwrap().free = dec!(958);
        (
            opening,
            close,
            evidence,
            CoherentRecoverySnapshot {
                capture: None,
                envelope: vec![],
                orders: vec![],
                fills: vec![],
            },
            vec![instrument],
            fills,
        )
    }

    #[test]
    fn isolated_application_reproduces_full_ledger_and_refuses_changed_application_order() {
        use apexes_backtest::{
            ledger::PortfolioLedger,
            observation_receipt::{
                CapturedObservations, CompletedObservation, FillObservationPrefix,
                ObservationClock, ObservationReceipt,
            },
        };
        use apexes_core::{MarketEvent, TradeEvent};
        let (opening, _, _, _, instruments, fills) = fixture();
        let allocation = crate::OpeningTrackedAssetEvidence {
            version: 1,
            predecessor: opening.predecessor.clone(),
            owner_fingerprint: "owner".into(),
            captured_at: opening.wallet.window.finished_at,
            balances: vec![("BTC".into(), dec!(4)), ("USDT".into(), dec!(1000))],
        };
        let scope = vec![(Venue::Binance, "BTCUSDT".into())];
        let mut records = Vec::new();
        let mut ledger = PortfolioLedger::new(
            instruments
                .iter()
                .map(|i| (i.symbol.clone(), i.clone()))
                .collect(),
            allocation.balances.iter().cloned().collect(),
        );
        let mut prefixes = Vec::new();
        for (i, fill) in fills.iter().enumerate() {
            let event = MarketEvent::Trade(TradeEvent {
                symbol: fill.symbol.clone(),
                venue: fill.venue,
                price: fill.price,
                quantity: fill.quantity,
                side: None,
                trade_id: None,
                timestamp: fill.filled_at,
            });
            let cursor = StateCursor {
                ordinal: 2 + i as u64,
                observed_at: fill.filled_at,
            };
            records.push(CompletedObservation { cursor, event });
            ledger.observe_spot_price(&fill.symbol, fill.filled_at, fill.price);
            ledger.apply_fill(fill).unwrap();
            prefixes.push(FillObservationPrefix {
                fill_id: fill.id,
                observations: i + 1,
            });
        }
        let receipt = ObservationReceipt::build(
            opening.predecessor.cursor,
            StateCursor {
                ordinal: 4,
                observed_at: fills[2].filled_at,
            },
            ObservationClock::SourceAvailability,
            &scope,
            &records,
        )
        .unwrap();
        let capture = CapturedObservations {
            receipt,
            scope,
            records,
            registered_risk: None,
            market_monitor_unqualified: false,
            margin_monitor_unqualified: false,
            market_monitor: None,
            margin_tiers: None,
            ledger: Some(ledger.inspect_evidence().unwrap()),
            fill_observation_prefixes: Some(prefixes),
            opening_risk: None,
        };
        let compare = |capture: &apexes_backtest::observation_receipt::CapturedObservations| {
            crate::recovery_ledger_application::compare_applied_ledger(
                capture,
                &instruments,
                &opening,
                &allocation,
                &fills,
            )
        };
        compare(&capture).unwrap();
        let mut altered = capture.clone();
        altered.ledger.as_mut().unwrap().balances[0].1 += dec!(1);
        assert_eq!(
            compare(&altered),
            Err(LedgerContinuityError::ApplicationMismatch)
        );
        let mut altered = capture.clone();
        altered.fill_observation_prefixes.as_mut().unwrap()[1].fill_id = fills[0].id;
        assert_eq!(compare(&altered), Err(LedgerContinuityError::FeeMismatch));
        let mut altered = capture;
        altered.fill_observation_prefixes = None;
        assert_eq!(compare(&altered), Err(LedgerContinuityError::FeeMismatch));
    }

    #[test]
    fn external_fee_classification_matches_independent_opening_asset_coverage() {
        use crate::OpeningTrackedAssetEvidence;
        use crate::recovery_fee_ownership::compare_external_fee_ownership;
        use apexes_backtest::ledger::PortfolioLedger;
        let (opening, closing, _, _, instruments, fills) = fixture();
        let mut allocation = OpeningTrackedAssetEvidence {
            version: 1,
            predecessor: opening.predecessor.clone(),
            owner_fingerprint: "owner".into(),
            captured_at: opening.wallet.window.finished_at,
            balances: vec![("BTC".into(), dec!(4)), ("USDT".into(), dec!(1000))],
        };
        let capture = |a: &OpeningTrackedAssetEvidence| {
            let mut ledger = PortfolioLedger::new(
                instruments
                    .iter()
                    .map(|i| (i.symbol.clone(), i.clone()))
                    .collect(),
                a.balances.iter().cloned().collect(),
            );
            for fill in &fills {
                ledger.apply_fill(fill).unwrap();
            }
            ledger.inspect_evidence().unwrap()
        };
        let external = capture(&allocation);
        assert_eq!(
            external.balances,
            vec![("BTC".into(), dec!(4.50)), ("USDT".into(), dec!(958))]
        );
        let fees = external.fee_valuation.as_ref().unwrap();
        assert!(fees.decisions.as_ref().unwrap()[2].external_fee);
        assert_eq!(
            fees.accounts[0].external_fees,
            vec![("BNB".into(), dec!(0.01))]
        );
        compare_external_fee_ownership(
            &allocation,
            &opening,
            &closing,
            &external,
            &instruments,
            &fills,
        )
        .unwrap();
        // A syntactically self-consistent alteration still fails independent
        // classification even when the underlying fee quantity is unchanged.
        let mut altered = external.clone();
        let fees = altered.fee_valuation.as_mut().unwrap();
        fees.accounts[0].external_fees.clear();
        fees.decisions.as_mut().unwrap()[2].external_fee = false;
        assert_eq!(
            compare_external_fee_ownership(
                &allocation,
                &opening,
                &closing,
                &altered,
                &instruments,
                &fills
            ),
            Err(LedgerContinuityError::FeeMismatch)
        );
        let mut altered = external.clone();
        altered.fee_valuation.as_mut().unwrap().decisions = None;
        assert_eq!(
            compare_external_fee_ownership(
                &allocation,
                &opening,
                &closing,
                &altered,
                &instruments,
                &fills
            ),
            Err(LedgerContinuityError::FeeMismatch)
        );
        let mut wrong = allocation.clone();
        wrong.owner_fingerprint = "wrong".into();
        assert_eq!(
            compare_external_fee_ownership(
                &wrong,
                &opening,
                &closing,
                &external,
                &instruments,
                &fills
            ),
            Err(LedgerContinuityError::Identity)
        );
        wrong = allocation.clone();
        wrong.captured_at = at(2);
        assert_eq!(
            compare_external_fee_ownership(
                &wrong,
                &opening,
                &closing,
                &external,
                &instruments,
                &fills
            ),
            Err(LedgerContinuityError::Interval)
        );
        wrong = allocation.clone();
        wrong.balances[1].1 = dec!(999);
        assert_eq!(
            compare_external_fee_ownership(
                &wrong,
                &opening,
                &closing,
                &external,
                &instruments,
                &fills
            ),
            Err(LedgerContinuityError::Balance)
        );
        let mut tied = fills.clone();
        tied[2].filled_at = tied[1].filled_at;
        assert_eq!(
            compare_external_fee_ownership(
                &allocation,
                &opening,
                &closing,
                &external,
                &instruments,
                &tied
            ),
            Err(LedgerContinuityError::Execution)
        );
        allocation.balances.insert(0, ("BNB".into(), dec!(1)));
        assert_eq!(
            compare_external_fee_ownership(
                &allocation,
                &opening,
                &closing,
                &external,
                &instruments,
                &fills
            ),
            Err(LedgerContinuityError::FeeMismatch)
        );
        let internal = capture(&allocation);
        assert_eq!(internal.balances[0], ("BNB".into(), dec!(0.99)));
        assert!(
            !internal
                .fee_valuation
                .as_ref()
                .unwrap()
                .decisions
                .as_ref()
                .unwrap()[2]
                .external_fee
        );
        compare_external_fee_ownership(
            &allocation,
            &opening,
            &closing,
            &internal,
            &instruments,
            &fills,
        )
        .unwrap();
    }

    #[test]
    fn third_asset_fee_evidence_requires_exact_causal_complete_rates() {
        use crate::{ThirdAssetFeeEvidence, ThirdAssetFeeRate};
        use apexes_backtest::fee_evidence::{FeeAccountEvidence, FeeValuationEvidence};
        let (opening, _, _, _, instruments, fills) = fixture();
        let boundary = apexes_oms::CoherentCheckpointReceipt {
            version: 1,
            checkpoint: opening.predecessor,
            run_scope: Uuid::new_v4(),
            last_intent: 3,
            fill_sequence: 3,
            terminal_sequence: 3,
            order_count: 3,
            fill_count: 3,
            journal_sha256: "c".repeat(64),
            capture_sha256: Some("d".repeat(64)),
        };
        let captured = apexes_backtest::ledger_evidence::LedgerEvidence {
            version: 1,
            scope: vec![(Venue::Binance, "BTCUSDT".into())],
            balances: vec![],
            positions: vec![],
            realized_pnl: vec![],
            fee_valuation: Some(FeeValuationEvidence {
                version: 1,
                accounts: vec![FeeAccountEvidence {
                    symbol: "BTCUSDT".into(),
                    venue: Venue::Binance,
                    base: "BTC".into(),
                    quote: "USDT".into(),
                    gross_quantity: dec!(0.51),
                    entry: dec!(100),
                    gross_realized: dec!(10),
                    fees: vec![
                        ("BNB".into(), dec!(0.01)),
                        ("BTC".into(), dec!(0.01)),
                        ("USDT".into(), dec!(1)),
                    ],
                    external_fees: vec![],
                    historical_fee: dec!(5),
                    missing_execution_valuations: 0,
                }],
                prices: vec![],
                decisions: None,
            }),
        };
        let evidence = ThirdAssetFeeEvidence {
            version: 1,
            boundary: boundary.clone(),
            owner_fingerprint: "owner".into(),
            rates: vec![ThirdAssetFeeRate {
                fill_id: fills[2].id,
                venue: Venue::Binance,
                symbol: "BTCUSDT".into(),
                asset: "BNB".into(),
                quote: "USDT".into(),
                observed_at: at(3),
                rate: dec!(300),
            }],
        };
        let check = |e: &ThirdAssetFeeEvidence| {
            e.compare(&boundary, "owner", &captured, &instruments, &fills)
        };
        check(&evidence).unwrap();
        let mut changed = evidence.clone();
        changed.rates[0].rate = dec!(301);
        assert_eq!(check(&changed), Err(LedgerContinuityError::FeeMismatch));
        changed = evidence.clone();
        changed.rates[0].observed_at = at(5);
        assert_eq!(check(&changed), Err(LedgerContinuityError::FeeMismatch));
        changed.rates[0].observed_at = at(-297);
        assert_eq!(check(&changed), Err(LedgerContinuityError::FeeMismatch));
        changed = evidence.clone();
        changed.rates[0].quote = "BTC".into();
        assert_eq!(check(&changed), Err(LedgerContinuityError::FeeMismatch));
        changed = evidence.clone();
        changed.rates.push(changed.rates[0].clone());
        assert_eq!(check(&changed), Err(LedgerContinuityError::FeeMismatch));
        changed = evidence.clone();
        let mut extra = changed.rates[0].clone();
        extra.fill_id = fills[0].id;
        changed.rates.push(extra);
        changed.rates.sort_by_key(|r| r.fill_id.0);
        assert_eq!(check(&changed), Err(LedgerContinuityError::FeeMismatch));
        changed = evidence.clone();
        changed.rates.clear();
        assert_eq!(check(&changed), Err(LedgerContinuityError::Execution));
        changed = evidence.clone();
        changed.boundary.journal_sha256 = "e".repeat(64);
        assert_eq!(check(&changed), Err(LedgerContinuityError::Identity));
        changed = evidence.clone();
        changed.owner_fingerprint = "different".into();
        assert_eq!(check(&changed), Err(LedgerContinuityError::Identity));
        let mut changed_capture = captured.clone();
        changed_capture.fee_valuation.as_mut().unwrap().accounts[0].external_fees =
            vec![("BNB".into(), dec!(0.01))];
        assert_eq!(
            evidence.compare(&boundary, "owner", &changed_capture, &instruments, &fills),
            Err(LedgerContinuityError::FeeMismatch)
        );
    }

    #[test]
    fn exact_fee_decisions_reject_offsetting_rate_errors_and_missing_zero_fill() {
        use crate::{ThirdAssetFeeEvidence, ThirdAssetFeeRate};
        use apexes_backtest::fee_evidence::{
            FeeAccountEvidence, FeeDecisionEvidence, FeeDecisionSource as Source,
            FeeValuationEvidence,
        };
        let (opening, _, _, _, instruments, mut fills) = fixture();
        let mut extra = fills[2].clone();
        extra.id = FillId(Uuid::new_v4());
        extra.filled_at = at(5);
        fills.push(extra);
        let mut zero = fills[0].clone();
        zero.id = FillId(Uuid::new_v4());
        zero.filled_at = at(6);
        zero.fee = Decimal::ZERO;
        zero.fee_currency = None;
        fills.push(zero);
        let boundary = apexes_oms::CoherentCheckpointReceipt {
            version: 1,
            checkpoint: opening.predecessor,
            run_scope: Uuid::new_v4(),
            last_intent: 5,
            fill_sequence: 5,
            terminal_sequence: 5,
            order_count: 5,
            fill_count: 5,
            journal_sha256: "c".repeat(64),
            capture_sha256: Some("d".repeat(64)),
        };
        let row = |f: &Fill, source: Source, value: Decimal| FeeDecisionEvidence {
            fill_id: f.id,
            venue: f.venue,
            symbol: f.symbol.clone(),
            asset: f.fee_currency.clone().unwrap_or("USDT".into()),
            filled_at: f.filled_at,
            price: f.price,
            fee: f.fee,
            external_fee: false,
            value: Some(value),
            source,
        };
        let decisions = vec![
            row(&fills[0], Source::Base, dec!(1)),
            row(&fills[1], Source::Quote, dec!(1)),
            row(
                &fills[2],
                Source::Observed {
                    observed_at: at(3),
                    rate: dec!(300),
                },
                dec!(3),
            ),
            row(
                &fills[3],
                Source::Observed {
                    observed_at: at(4),
                    rate: dec!(400),
                },
                dec!(4),
            ),
            row(&fills[4], Source::Zero, Decimal::ZERO),
        ];
        let mut rates = vec![
            ThirdAssetFeeRate {
                fill_id: fills[2].id,
                venue: Venue::Binance,
                symbol: "BTCUSDT".into(),
                asset: "BNB".into(),
                quote: "USDT".into(),
                observed_at: at(3),
                rate: dec!(300),
            },
            ThirdAssetFeeRate {
                fill_id: fills[3].id,
                venue: Venue::Binance,
                symbol: "BTCUSDT".into(),
                asset: "BNB".into(),
                quote: "USDT".into(),
                observed_at: at(4),
                rate: dec!(400),
            },
        ];
        rates.sort_by_key(|r| r.fill_id.0);
        let evidence = ThirdAssetFeeEvidence {
            version: 1,
            boundary: boundary.clone(),
            owner_fingerprint: "owner".into(),
            rates,
        };
        let captured = apexes_backtest::ledger_evidence::LedgerEvidence {
            version: 1,
            scope: vec![(Venue::Binance, "BTCUSDT".into())],
            balances: vec![],
            positions: vec![],
            realized_pnl: vec![],
            fee_valuation: Some(FeeValuationEvidence {
                version: 1,
                accounts: vec![FeeAccountEvidence {
                    symbol: "BTCUSDT".into(),
                    venue: Venue::Binance,
                    base: "BTC".into(),
                    quote: "USDT".into(),
                    gross_quantity: dec!(1.52),
                    entry: dec!(100),
                    gross_realized: dec!(10),
                    fees: vec![
                        ("BNB".into(), dec!(0.02)),
                        ("BTC".into(), dec!(0.01)),
                        ("USDT".into(), dec!(1)),
                    ],
                    external_fees: vec![],
                    historical_fee: dec!(9),
                    missing_execution_valuations: 0,
                }],
                prices: vec![],
                decisions: Some(decisions),
            }),
        };
        let check = |c: &apexes_backtest::ledger_evidence::LedgerEvidence| {
            evidence.compare_decisions(&boundary, "owner", c, &instruments, &fills)
        };
        check(&captured).unwrap();
        let mut altered = captured.clone();
        let rows = altered
            .fee_valuation
            .as_mut()
            .unwrap()
            .decisions
            .as_mut()
            .unwrap();
        rows[2].source = Source::Observed {
            observed_at: at(3),
            rate: dec!(400),
        };
        rows[2].value = Some(dec!(4));
        rows[3].source = Source::Observed {
            observed_at: at(4),
            rate: dec!(300),
        };
        rows[3].value = Some(dec!(3));
        evidence
            .compare(&boundary, "owner", &altered, &instruments, &fills)
            .unwrap();
        assert_eq!(check(&altered), Err(LedgerContinuityError::FeeMismatch));
        let mut altered = captured.clone();
        altered
            .fee_valuation
            .as_mut()
            .unwrap()
            .decisions
            .as_mut()
            .unwrap()[2]
            .source = Source::Observed {
            observed_at: at(2),
            rate: dec!(300),
        };
        assert_eq!(check(&altered), Err(LedgerContinuityError::FeeMismatch));
        let mut altered = captured.clone();
        altered
            .fee_valuation
            .as_mut()
            .unwrap()
            .decisions
            .as_mut()
            .unwrap()[2]
            .source = Source::Provided;
        assert_eq!(check(&altered), Err(LedgerContinuityError::FeeMismatch));
        let mut altered = captured.clone();
        altered
            .fee_valuation
            .as_mut()
            .unwrap()
            .decisions
            .as_mut()
            .unwrap()[4]
            .fill_id = FillId(Uuid::new_v4());
        assert_eq!(check(&altered), Err(LedgerContinuityError::FeeMismatch));
        let mut altered = captured.clone();
        altered
            .fee_valuation
            .as_mut()
            .unwrap()
            .decisions
            .as_mut()
            .unwrap()
            .pop();
        assert_eq!(check(&altered), Err(LedgerContinuityError::FeeMismatch));
        let mut altered = captured.clone();
        altered.fee_valuation.as_mut().unwrap().decisions = None;
        assert_eq!(check(&altered), Err(LedgerContinuityError::FeeMismatch));
    }

    #[test]
    fn direct_fee_evidence_checks_exact_amounts_and_subtotal() {
        use crate::recovery_fee::compare_direct_fees;
        use apexes_backtest::fee_evidence::{FeeAccountEvidence, FeeValuationEvidence};
        let (_, _, _, _, instruments, fills) = fixture();
        let mut captured = apexes_backtest::ledger_evidence::LedgerEvidence {
            version: 1,
            scope: vec![(Venue::Binance, "BTCUSDT".into())],
            balances: vec![],
            positions: vec![],
            realized_pnl: vec![],
            fee_valuation: Some(FeeValuationEvidence {
                version: 1,
                accounts: vec![FeeAccountEvidence {
                    symbol: "BTCUSDT".into(),
                    venue: Venue::Binance,
                    base: "BTC".into(),
                    quote: "USDT".into(),
                    gross_quantity: dec!(0.5),
                    entry: dec!(100),
                    gross_realized: dec!(10),
                    fees: vec![("BTC".into(), dec!(0.01)), ("USDT".into(), dec!(1))],
                    external_fees: vec![],
                    historical_fee: dec!(2),
                    missing_execution_valuations: 0,
                }],
                prices: vec![],
                decisions: None,
            }),
        };
        compare_direct_fees(&captured, &instruments, &fills[..2]).unwrap();
        assert_eq!(
            compare_direct_fees(&captured, &instruments, &fills),
            Err(LedgerContinuityError::Execution)
        );
        let mut changed = captured.clone();
        changed.fee_valuation.as_mut().unwrap().accounts[0].historical_fee = dec!(3);
        assert_eq!(
            compare_direct_fees(&changed, &instruments, &fills[..2]),
            Err(LedgerContinuityError::FeeMismatch)
        );
        let mut changed = captured.clone();
        changed.fee_valuation.as_mut().unwrap().accounts[0].fees[0].1 = dec!(0.02);
        assert_eq!(
            compare_direct_fees(&changed, &instruments, &fills[..2]),
            Err(LedgerContinuityError::FeeMismatch)
        );
        let mut changed = captured.clone();
        changed.fee_valuation.as_mut().unwrap().accounts[0].missing_execution_valuations = 1;
        assert_eq!(
            compare_direct_fees(&changed, &instruments, &fills[..2]),
            Err(LedgerContinuityError::FeeMismatch)
        );
        let mut changed = captured.clone();
        changed.fee_valuation.as_mut().unwrap().accounts[0].external_fees =
            vec![("BTC".into(), dec!(0.01))];
        assert_eq!(
            compare_direct_fees(&changed, &instruments, &fills[..2]),
            Err(LedgerContinuityError::FeeMismatch)
        );
        let mut changed = captured.clone();
        changed.fee_valuation.as_mut().unwrap().accounts[0].quote = "USD".into();
        assert_eq!(
            compare_direct_fees(&changed, &instruments, &fills[..2]),
            Err(LedgerContinuityError::FeeMismatch)
        );
        let mut changed_fills = fills[..2].to_vec();
        changed_fills[0].fee_currency = None;
        assert_eq!(
            compare_direct_fees(&captured, &instruments, &changed_fills),
            Err(LedgerContinuityError::Execution)
        );
        captured.fee_valuation = None;
        assert_eq!(
            compare_direct_fees(&captured, &instruments, &fills[..2]),
            Err(LedgerContinuityError::FeeMismatch)
        );
    }

    #[test]
    fn realized_pnl_reconciles_weighted_cost_and_multiple_closes() {
        let (_, _, _, _, instruments, mut fills) = fixture();
        for fill in &mut fills {
            fill.fee = Decimal::ZERO;
            fill.fee_currency = None;
        }
        fills[0].quantity = dec!(1);
        fills[0].price = dec!(100);
        fills[1].side = Side::Buy;
        fills[1].quantity = dec!(1);
        fills[1].price = dec!(120);
        fills[2].side = Side::Sell;
        fills[2].quantity = dec!(1);
        fills[2].price = dec!(130);
        let mut close = fills[2].clone();
        close.id = FillId(Uuid::new_v4());
        close.filled_at = at(5);
        close.price = dec!(100);
        fills.push(close);
        let mut captured = apexes_backtest::ledger_evidence::LedgerEvidence {
            version: 1,
            scope: vec![(Venue::Binance, "BTCUSDT".into())],
            balances: vec![],
            positions: vec![],
            realized_pnl: vec![("BTCUSDT".into(), dec!(10))],
            fee_valuation: None,
        };
        compare_realized_pnl(&captured, &instruments, &fills).unwrap();
        captured.realized_pnl[0].1 = dec!(11);
        assert_eq!(
            compare_realized_pnl(&captured, &instruments, &fills),
            Err(LedgerContinuityError::RealizedPnlMismatch)
        );
        captured.realized_pnl.clear();
        assert_eq!(
            compare_realized_pnl(&captured, &instruments, &fills),
            Err(LedgerContinuityError::RealizedPnlMismatch)
        );
        compare_realized_pnl(&captured, &instruments, &fills[..2]).unwrap();
    }

    #[test]
    fn realized_pnl_refuses_fee_ambiguous_time_and_inexact_average() {
        let (_, _, _, _, instruments, mut fills) = fixture();
        let captured = apexes_backtest::ledger_evidence::LedgerEvidence {
            version: 1,
            scope: vec![(Venue::Binance, "BTCUSDT".into())],
            balances: vec![],
            positions: vec![],
            realized_pnl: vec![],
            fee_valuation: None,
        };
        assert_eq!(
            compare_realized_pnl(&captured, &instruments, &fills),
            Err(LedgerContinuityError::Execution)
        );
        for f in &mut fills {
            f.fee = Decimal::ZERO;
        }
        let mut pair = fills[..2].to_vec();
        pair[1].filled_at = pair[0].filled_at;
        assert_eq!(
            compare_realized_pnl(&captured, &instruments, &pair),
            Err(LedgerContinuityError::Execution)
        );
        pair[1].filled_at = at(3);
        pair[1].side = Side::Buy;
        pair[0].quantity = dec!(2);
        pair[0].price = dec!(1);
        pair[1].quantity = dec!(1);
        pair[1].price = dec!(2);
        assert_eq!(
            compare_realized_pnl(&captured, &instruments, &pair),
            Err(LedgerContinuityError::Arithmetic)
        );
        pair[1].side = Side::Sell;
        pair[1].quantity = dec!(3);
        assert_eq!(
            compare_realized_pnl(&captured, &instruments, &pair),
            Err(LedgerContinuityError::Execution)
        );
    }

    #[test]
    fn all_three_fee_assets_and_both_trade_sides_reconcile() {
        let (o, c, e, s, i, f) = fixture();
        validate_ledger_continuity(&o, &c, (&o.predecessor, "owner"), &e, &s, &i, &f).unwrap();
    }

    #[test]
    fn unexplained_balance_change_and_missing_asset_refused() {
        let (o, mut c, e, s, i, f) = fixture();
        c.balances.get_mut("USDT").unwrap().free += dec!(1);
        assert_eq!(
            validate_ledger_continuity(&o, &c, (&o.predecessor, "owner"), &e, &s, &i, &f),
            Err(LedgerContinuityError::Mismatch)
        );
        c.balances.remove("BNB");
        assert_eq!(
            validate_ledger_continuity(&o, &c, (&o.predecessor, "owner"), &e, &s, &i, &f),
            Err(LedgerContinuityError::Balance)
        );
    }

    #[test]
    fn opening_must_bind_to_original_receipt_and_precede_executions() {
        let (mut o, c, e, s, i, f) = fixture();
        let p = o.predecessor.clone();
        o.predecessor.envelope_sha256 = "c".repeat(64);
        assert_eq!(
            validate_ledger_continuity(&o, &c, (&p, "owner"), &e, &s, &i, &f),
            Err(LedgerContinuityError::Identity)
        );
        o.predecessor = p.clone();
        o.wallet.window.finished_at = at(3);
        assert_eq!(
            validate_ledger_continuity(&o, &c, (&p, "owner"), &e, &s, &i, &f),
            Err(LedgerContinuityError::Interval)
        );
    }

    #[test]
    fn negative_intermediate_account_holdings_refused() {
        let (mut o, c, e, s, i, f) = fixture();
        o.wallet.balances.get_mut("USDT").unwrap().free = dec!(50);
        assert_eq!(
            validate_ledger_continuity(&o, &c, (&o.predecessor, "owner"), &e, &s, &i, &f),
            Err(LedgerContinuityError::Negative)
        );
    }

    #[test]
    fn exact_arithmetic_refuses_unrepresentable_precision_and_overflow() {
        assert_eq!(
            exact_mul(dec!(0.000000000000001), dec!(0.000000000000001)),
            Err(LedgerContinuityError::Arithmetic)
        );
        assert_eq!(
            exact_add(Decimal::MAX, Decimal::ONE),
            Err(LedgerContinuityError::Arithmetic)
        );
        assert_eq!(exact_mul(dec!(1.20), dec!(0.25)).unwrap(), dec!(0.30));
        assert_eq!(exact_add(dec!(1.01), dec!(-0.001)).unwrap(), dec!(1.009));
    }
}
