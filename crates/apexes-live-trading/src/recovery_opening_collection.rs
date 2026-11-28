//! Read-only opening collection. Account holdings do not establish
//! strategy funding; independently selected allocation amounts are mandatory.
use crate::{
    EvidenceWindow, LedgerContinuityError, OpeningTrackedAssetEvidence, OpeningWalletEvidence,
    RecoverySelection, SpotWalletBalance, StrategyRecoveryError, WalletEvidence,
};
use apexes_core::{AssetClass, Instrument, Venue};
use apexes_oms::{CheckpointStoreError, OrderStore, SpotEvidenceSource};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

#[derive(Debug, thiserror::Error)]
pub enum OpeningCollectionError {
    #[error("independent opening selection is invalid: {0}")]
    Selection(#[from] StrategyRecoveryError),
    #[error("opening store qualification failed: {0}")]
    Store(#[from] CheckpointStoreError),
    #[error("authenticated owner or instrument scope differs")]
    Owner,
    #[error("opening collection query failed")]
    Source,
    #[error("opening collection exceeded its deadline or clock regressed")]
    Deadline,
    #[error("account is not quiescent")]
    OpenOrders,
    #[error("opening wallet or independently selected allocation is invalid: {0}")]
    Allocation(#[from] LedgerContinuityError),
}

/// Historical account observation and explicitly selected local allocation.
/// The allocation timestamp records comparison, not a funding event.
/// No current freshness, ownership provenance, or startup authority.
pub struct CollectedOpeningEvidence {
    pub opening: OpeningWalletEvidence,
    pub allocation: OpeningTrackedAssetEvidence,
    pub open_before: EvidenceWindow,
    pub open_after: EvidenceWindow,
    pub inspected_at: DateTime<Utc>,
}

fn now_after(previous: DateTime<Utc>) -> Result<DateTime<Utc>, OpeningCollectionError> {
    let now = Utc::now();
    if now < previous {
        return Err(OpeningCollectionError::Deadline);
    }
    Ok(now)
}

fn valid_asset(asset: &str) -> bool {
    !asset.is_empty()
        && asset.len() <= 24
        && asset
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

fn validate_allocation(
    opening: &OpeningWalletEvidence,
    allocation: &OpeningTrackedAssetEvidence,
    instruments: &[Instrument],
) -> Result<(), LedgerContinuityError> {
    use LedgerContinuityError as E;
    let balances = &opening.wallet.balances;
    if balances.len() > 1024
        || balances.is_empty()
        || balances.iter().any(|(asset, b)| {
            !valid_asset(asset) || b.free.is_sign_negative() || !b.locked.is_zero()
        })
    {
        return Err(E::Balance);
    }
    let map = allocation.opening_map(opening)?;
    if map.is_empty() || map.keys().any(|a| !valid_asset(a)) {
        return Err(E::Balance);
    }
    // Explicitly retain all scoped base/quote wallet rows, even zero
    // amounts. Do not infer the tracked subset from those rows.
    for i in instruments {
        for asset in [&i.base_currency, &i.quote_currency] {
            if asset
                .as_deref()
                .is_none_or(|a| !valid_asset(a) || !balances.contains_key(a))
            {
                return Err(E::Balance);
            }
        }
    }
    Ok(())
}

impl RecoverySelection {
    /// Collect before the one-time claim and before any local execution.
    /// `tracked_balances` must be independently selected exact amounts
    /// (including explicit zero rows), not derived from this wallet query.
    /// Existing full-account equality is required for each tracked asset.
    /// The caller must independently establish funding provenance.
    pub async fn collect_opening_spot_evidence(
        &self,
        store: &mut OrderStore,
        ems_postgres_url: &str,
        source: &dyn SpotEvidenceSource,
        strategy_ref: &serde_json::Value,
        instruments: &[Instrument],
        tracked_balances: &[(String, Decimal)],
    ) -> Result<CollectedOpeningEvidence, OpeningCollectionError> {
        use OpeningCollectionError as E;
        self.validate()?;
        let owner = source
            .owner_fingerprint(strategy_ref, instruments)
            .map_err(|_| E::Owner)?;
        let mut symbols = HashSet::new();
        if owner != self.owner_fingerprint()
            || instruments.is_empty()
            || instruments.len() > 128
            || instruments.iter().any(|i| {
                i.venue != Venue::Binance
                    || i.asset_class != AssetClass::Spot
                    || !symbols.insert(&i.symbol)
                    || i.base_currency == i.quote_currency
            })
        {
            return Err(E::Owner);
        }
        if tracked_balances.is_empty()
            || tracked_balances.len() > 1024
            || !tracked_balances.windows(2).all(|w| w[0].0 < w[1].0)
            || tracked_balances
                .iter()
                .any(|(a, b)| !valid_asset(a) || b.is_sign_negative())
        {
            return Err(LedgerContinuityError::Balance.into());
        }
        store
            .inspect_opening_checkpoint(self.receipt(), &owner, ems_postgres_url)
            .await?;
        let started_at = Utc::now();
        if started_at < self.receipt().cursor.observed_at {
            return Err(E::Deadline);
        }
        let monotonic = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(60), async {
            if source
                .account_open_order_count()
                .await
                .map_err(|_| E::Source)?
                != 0
            {
                return Err(E::OpenOrders);
            }
            let before_finished = now_after(started_at)?;
            let wallet_started = now_after(before_finished)?;
            let balances = source.account_wallet().await.map_err(|_| E::Source)?;
            let wallet_finished = now_after(wallet_started)?;
            let opening = OpeningWalletEvidence {
                predecessor: self.receipt().clone(),
                wallet: WalletEvidence {
                    window: EvidenceWindow {
                        started_at: wallet_started,
                        finished_at: wallet_finished,
                    },
                    owner_fingerprint: owner.clone(),
                    venue: Venue::Binance,
                    asset_class: AssetClass::Spot,
                    account_wide_complete: true,
                    balances: balances
                        .into_iter()
                        .map(|(a, b)| {
                            (
                                a,
                                SpotWalletBalance {
                                    free: b.free,
                                    locked: b.locked,
                                },
                            )
                        })
                        .collect(),
                },
            };
            let allocation = OpeningTrackedAssetEvidence {
                version: 1,
                predecessor: self.receipt().clone(),
                owner_fingerprint: owner.clone(),
                captured_at: wallet_finished,
                balances: tracked_balances.to_vec(),
            };
            validate_allocation(&opening, &allocation, instruments)?;
            let after_started = now_after(wallet_finished)?;
            if source
                .account_open_order_count()
                .await
                .map_err(|_| E::Source)?
                != 0
            {
                return Err(E::OpenOrders);
            }
            let after_finished = now_after(after_started)?;
            Ok::<_, E>(CollectedOpeningEvidence {
                opening,
                allocation,
                open_before: EvidenceWindow {
                    started_at,
                    finished_at: before_finished,
                },
                open_after: EvidenceWindow {
                    started_at: after_started,
                    finished_at: after_finished,
                },
                inspected_at: after_finished,
            })
        })
        .await
        .map_err(|_| E::Deadline)??;
        store
            .inspect_opening_checkpoint(self.receipt(), &owner, ems_postgres_url)
            .await?;
        if monotonic.elapsed() > Duration::from_secs(60)
            || now_after(result.inspected_at)?.signed_duration_since(started_at)
                > crate::recovery_evidence_window::MAX_EVIDENCE_AGE
        {
            return Err(E::Deadline);
        }
        Ok(result)
    }
}
