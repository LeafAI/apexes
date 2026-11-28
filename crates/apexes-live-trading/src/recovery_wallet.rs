//! Offline validation of supplied wallet evidence for recovery inspection.
//! No balance acquisition, account mutation, or execution authorization.
use crate::recovery_evidence_window::{EvidenceWindow, MAX_EVIDENCE_AGE, RecoveryEvidenceWindow};
use crate::spot_recovery::SpotWalletBalance;
use apexes_core::{AssetClass, Fill, Instrument, Venue};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};

const MAX_ASSETS: usize = 1024;

#[derive(Debug, Clone)]
pub struct WalletEvidence {
    pub window: EvidenceWindow,
    pub owner_fingerprint: String,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub account_wide_complete: bool,
    /// Explicit amounts, including zero balances for relevant assets.
    pub balances: HashMap<String, SpotWalletBalance>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WalletEvidenceError {
    #[error("wallet evidence has incomplete account coverage")]
    Incomplete,
    #[error("wallet evidence owner or market scope differs")]
    Scope,
    #[error("wallet evidence interval is reversed, future-dated or out of sequence")]
    Interval,
    #[error("wallet evidence exceeds the supplied recovery observation window")]
    Stale,
    #[error("wallet evidence has invalid asset identities or amounts")]
    Balance,
    #[error("wallet evidence omits an explicit relevant asset balance")]
    MissingAsset,
    #[error("wallet evidence reports locked funds")]
    Locked,
    #[error("historical fee has no asset attribution")]
    FeeAsset,
}

fn valid_asset(asset: &str) -> bool {
    !asset.is_empty()
        && asset.len() <= 24
        && asset
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

/// Assumes the existing terminal/open-order window was independently
/// validated. The entire sequence must fit the same freshness ceiling.
pub(crate) fn validate_wallet_evidence(
    wallet: &WalletEvidence,
    evidence: &RecoveryEvidenceWindow,
    owner: &str,
    inspected_at: DateTime<Utc>,
    instruments: &[Instrument],
    fills: &[Fill],
) -> Result<(), WalletEvidenceError> {
    use WalletEvidenceError as E;
    if !wallet.account_wide_complete {
        return Err(E::Incomplete);
    }
    if wallet.owner_fingerprint != owner
        || wallet.venue != Venue::Binance
        || wallet.asset_class != AssetClass::Spot
    {
        return Err(E::Scope);
    }
    if wallet.window.started_at < evidence.open_orders.window.finished_at
        || wallet.window.finished_at < wallet.window.started_at
        || wallet.window.finished_at > inspected_at
    {
        return Err(E::Interval);
    }
    if inspected_at.signed_duration_since(evidence.terminal_window.started_at) > MAX_EVIDENCE_AGE {
        return Err(E::Stale);
    }
    if wallet.balances.len() > MAX_ASSETS
        || wallet.balances.iter().any(|(asset, b)| {
            !valid_asset(asset) || b.free.is_sign_negative() || b.locked.is_sign_negative()
        })
    {
        return Err(E::Balance);
    }
    // Account-wide quiescence: unexplained locks anywhere are incompatible
    // with the supplied complete zero-open-order observation.
    if wallet.balances.values().any(|b| !b.locked.is_zero()) {
        return Err(E::Locked);
    }
    let mut required = HashSet::new();
    for instrument in instruments {
        for asset in [&instrument.base_currency, &instrument.quote_currency] {
            let asset = asset
                .as_deref()
                .filter(|a| valid_asset(a))
                .ok_or(E::Balance)?;
            required.insert(asset);
        }
    }
    for fill in fills {
        if !fill.fee.is_zero() {
            let asset = fill
                .fee_currency
                .as_deref()
                .filter(|a| valid_asset(a))
                .ok_or(E::FeeAsset)?;
            required.insert(asset);
        }
    }
    if required
        .iter()
        .any(|asset| !wallet.balances.contains_key(*asset))
    {
        return Err(E::MissingAsset);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OpenOrderCoverage;
    use apexes_core::{FillId, MarketType, OrderId, Side};
    use chrono::TimeDelta;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn at(s: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + s, 0).unwrap()
    }

    fn fixture() -> (
        RecoveryEvidenceWindow,
        WalletEvidence,
        Vec<Instrument>,
        Vec<Fill>,
    ) {
        let evidence = RecoveryEvidenceWindow {
            terminal_window: EvidenceWindow {
                started_at: at(0),
                finished_at: at(5),
            },
            terminal_owner_fingerprint: "owner".into(),
            terminal_evidence_complete: true,
            terminal_orders: vec![],
            open_orders: OpenOrderCoverage {
                window: EvidenceWindow {
                    started_at: at(5),
                    finished_at: at(10),
                },
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                owner_fingerprint: "owner".into(),
                account_wide_complete: true,
                open_order_count: 0,
                foreign_order_count: 0,
            },
        };
        let wallet = WalletEvidence {
            window: EvidenceWindow {
                started_at: at(10),
                finished_at: at(15),
            },
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            owner_fingerprint: "owner".into(),
            account_wide_complete: true,
            balances: HashMap::from([
                (
                    "BTC".into(),
                    SpotWalletBalance {
                        free: dec!(5),
                        locked: Decimal::ZERO,
                    },
                ),
                (
                    "USDT".into(),
                    SpotWalletBalance {
                        free: dec!(100),
                        locked: Decimal::ZERO,
                    },
                ),
                ("BNB".into(), SpotWalletBalance::default()),
            ]),
        };
        let instruments = vec![
            Instrument::new(
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
            .unwrap(),
        ];
        let fills = vec![Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            side: Side::Buy,
            price: dec!(100),
            quantity: dec!(1),
            fee: dec!(0.01),
            fee_currency: Some("BNB".into()),
            filled_at: at(-1),
            leverage: None,
            margin_mode: None,
        }];
        (evidence, wallet, instruments, fills)
    }

    #[test]
    fn ordered_wallet_evidence_requires_explicit_balances_including_zero_fee_asset() {
        let (e, mut w, i, f) = fixture();
        validate_wallet_evidence(&w, &e, "owner", at(60), &i, &f).unwrap();
        w.balances.remove("BNB");
        assert_eq!(
            validate_wallet_evidence(&w, &e, "owner", at(15), &i, &f),
            Err(WalletEvidenceError::MissingAsset)
        );
        w.balances
            .insert("BNB".into(), SpotWalletBalance::default());
        w.balances.remove("USDT");
        assert_eq!(
            validate_wallet_evidence(&w, &e, "owner", at(15), &i, &f),
            Err(WalletEvidenceError::MissingAsset)
        );
    }

    #[test]
    fn wallet_window_must_follow_open_order_checks_and_share_freshness_budget() {
        let (e, w, i, f) = fixture();
        assert_eq!(
            validate_wallet_evidence(&w, &e, "owner", at(61), &i, &f),
            Err(WalletEvidenceError::Stale)
        );
        for case in 0..3 {
            let mut bad = w.clone();
            match case {
                0 => bad.window.started_at = at(9),
                1 => bad.window.finished_at = at(9),
                _ => bad.window.finished_at = at(15) + TimeDelta::nanoseconds(1),
            }
            assert_eq!(
                validate_wallet_evidence(&bad, &e, "owner", at(15), &i, &f),
                Err(WalletEvidenceError::Interval)
            );
        }
    }

    #[test]
    fn wallet_scope_and_coverage_must_match() {
        let (e, w, i, f) = fixture();
        let mut bad = w.clone();
        bad.account_wide_complete = false;
        assert_eq!(
            validate_wallet_evidence(&bad, &e, "owner", at(15), &i, &f),
            Err(WalletEvidenceError::Incomplete)
        );
        let mut bad = w.clone();
        bad.owner_fingerprint = "other".into();
        assert_eq!(
            validate_wallet_evidence(&bad, &e, "owner", at(15), &i, &f),
            Err(WalletEvidenceError::Scope)
        );
        let mut bad = w;
        bad.venue = Venue::Okx;
        assert_eq!(
            validate_wallet_evidence(&bad, &e, "owner", at(15), &i, &f),
            Err(WalletEvidenceError::Scope)
        );
    }

    #[test]
    fn all_supplied_assets_must_be_valid_nonnegative_and_unlocked() {
        let (e, w, i, f) = fixture();
        let mut bad = w.clone();
        bad.balances
            .insert("bad asset".into(), SpotWalletBalance::default());
        assert_eq!(
            validate_wallet_evidence(&bad, &e, "owner", at(15), &i, &f),
            Err(WalletEvidenceError::Balance)
        );
        let mut bad = w.clone();
        bad.balances.get_mut("BNB").unwrap().free = dec!(-1);
        assert_eq!(
            validate_wallet_evidence(&bad, &e, "owner", at(15), &i, &f),
            Err(WalletEvidenceError::Balance)
        );
        let mut bad = w;
        bad.balances.insert(
            "ETH".into(),
            SpotWalletBalance {
                free: Decimal::ZERO,
                locked: dec!(1),
            },
        );
        assert_eq!(
            validate_wallet_evidence(&bad, &e, "owner", at(15), &i, &f),
            Err(WalletEvidenceError::Locked)
        );
    }

    #[test]
    fn nonzero_fees_require_explicit_valid_asset_attribution() {
        let (e, w, i, mut f) = fixture();
        f[0].fee_currency = None;
        assert_eq!(
            validate_wallet_evidence(&w, &e, "owner", at(15), &i, &f),
            Err(WalletEvidenceError::FeeAsset)
        );
        f[0].fee_currency = Some("bnb".into());
        assert_eq!(
            validate_wallet_evidence(&w, &e, "owner", at(15), &i, &f),
            Err(WalletEvidenceError::FeeAsset)
        );
        f[0].fee = Decimal::ZERO;
        validate_wallet_evidence(&w, &e, "owner", at(15), &i, &f).unwrap();
    }
}
