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

fn exact_decimal(mut n: i128, mut scale: u32) -> Result<Decimal, LedgerContinuityError> {
    while scale > 0 && n % 10 == 0 {
        n /= 10;
        scale -= 1;
    }
    Decimal::try_from_i128_with_scale(n, scale).map_err(|_| LedgerContinuityError::Arithmetic)
}

fn exact_add(a: Decimal, b: Decimal) -> Result<Decimal, LedgerContinuityError> {
    let a = a.normalize();
    let b = b.normalize();
    let scale = a.scale().max(b.scale());
    let scaled = |v: Decimal| v.mantissa().checked_mul(10_i128.pow(scale - v.scale()));
    let n = scaled(a)
        .and_then(|a| scaled(b).and_then(|b| a.checked_add(b)))
        .ok_or(LedgerContinuityError::Arithmetic)?;
    exact_decimal(n, scale)
}

fn exact_mul(a: Decimal, b: Decimal) -> Result<Decimal, LedgerContinuityError> {
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
