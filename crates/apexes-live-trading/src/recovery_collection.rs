//! Read-only collection against an independently selected coherent journal.
//! Never reconciles, cancels, consumes a claim or authorizes startup.
use crate::{
    CoherentInspectionError, CoherentRecoverySelection, EvidenceWindow, OpenOrderCoverage,
    RecoveryEvidenceWindow, SpotWalletBalance, WalletEvidence,
};
use apexes_core::{AssetClass, Fill, Instrument, Venue};
use apexes_oms::{OrderStore, SpotEvidenceSource};
use chrono::{DateTime, Utc};
use std::time::{Duration, Instant};

#[derive(Debug, thiserror::Error)]
pub enum SpotCollectionError {
    #[error("selected journal inspection failed: {0}")]
    Inspection(#[from] CoherentInspectionError),
    #[error("authenticated source does not match selected owner or instrument scope")]
    Owner,
    #[error("read-only evidence query failed")]
    Source,
    #[error("collection exceeded the evidence deadline or clock regressed")]
    Deadline,
}

/// Successful collection is a historical sequence of authenticated queries,
/// not an atomic exchange snapshot or proof of absence of unseen activity.
pub struct CollectedSpotEvidence {
    pub window: RecoveryEvidenceWindow,
    pub wallet: WalletEvidence,
    pub inspected_at: DateTime<Utc>,
}

fn now_after(previous: DateTime<Utc>) -> Result<DateTime<Utc>, SpotCollectionError> {
    let now = Utc::now();
    if now < previous {
        return Err(SpotCollectionError::Deadline);
    }
    Ok(now)
}

impl CoherentRecoverySelection {
    /// Fetch only through the strict read-only source. The strategy ref
    /// and instrument metadata must be independently selected. Match the
    /// authenticated credential-derived owner before any exchange query.
    pub async fn collect_spot_evidence(
        &self,
        store: &mut OrderStore,
        ems_postgres_url: &str,
        source: &dyn SpotEvidenceSource,
        strategy_ref: &serde_json::Value,
        instruments: &[Instrument],
    ) -> Result<CollectedSpotEvidence, SpotCollectionError> {
        use SpotCollectionError as E;
        let owner = source
            .owner_fingerprint(strategy_ref, instruments)
            .map_err(|_| E::Owner)?;
        if owner != self.predecessor.owner_fingerprint()
            || instruments.is_empty()
            || instruments.len() > 128
            || instruments
                .iter()
                .any(|i| i.venue != Venue::Binance || i.asset_class != AssetClass::Spot)
        {
            return Err(E::Owner);
        }
        let snapshot = store
            .inspect_coherent_recovery(
                self.predecessor.receipt(),
                self.receipt(),
                &owner,
                ems_postgres_url,
            )
            .await
            .map_err(CoherentInspectionError::from)?;
        if snapshot.orders.iter().any(|o| {
            !instruments.iter().any(|i| {
                i.symbol == o.symbol && i.venue == o.venue && i.asset_class == o.asset_class
            })
        }) {
            return Err(E::Owner);
        }
        let started_at = Utc::now();
        let monotonic = Instant::now();
        let collected = tokio::time::timeout(Duration::from_secs(60), async {
            let mut terminal_orders = Vec::with_capacity(snapshot.orders.len());
            let mut last = started_at;
            for order in &snapshot.orders {
                terminal_orders.push(source.terminal_order(order).await.map_err(|_| E::Source)?);
                last = now_after(last)?;
            }
            let terminal_finished = now_after(last)?;
            let open_started = now_after(terminal_finished)?;
            let open_count = source
                .account_open_order_count()
                .await
                .map_err(|_| E::Source)?;
            let open_finished = now_after(open_started)?;
            // Never label unfamiliar rows as absent. Any row fails the
            // existing quiescence check; no wallet request is necessary.
            if open_count != 0 {
                return Err(
                    CoherentInspectionError::from(crate::EvidenceWindowError::OpenOrders).into(),
                );
            }
            let wallet_started = now_after(open_finished)?;
            let balances = source.account_wallet().await.map_err(|_| E::Source)?;
            let wallet_finished = now_after(wallet_started)?;
            let inspected_at = now_after(wallet_finished)?;
            let window = RecoveryEvidenceWindow {
                terminal_window: EvidenceWindow {
                    started_at,
                    finished_at: terminal_finished,
                },
                terminal_owner_fingerprint: owner.clone(),
                terminal_evidence_complete: true,
                terminal_orders,
                open_orders: OpenOrderCoverage {
                    window: EvidenceWindow {
                        started_at: open_started,
                        finished_at: open_finished,
                    },
                    venue: Venue::Binance,
                    asset_class: AssetClass::Spot,
                    owner_fingerprint: owner.clone(),
                    account_wide_complete: true,
                    open_order_count: 0,
                    foreign_order_count: 0,
                },
            };
            let wallet = WalletEvidence {
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
                    .map(|(asset, b)| {
                        (
                            asset,
                            SpotWalletBalance {
                                free: b.free,
                                locked: b.locked,
                            },
                        )
                    })
                    .collect(),
            };
            Ok::<_, E>(CollectedSpotEvidence {
                window,
                wallet,
                inspected_at,
            })
        })
        .await
        .map_err(|_| E::Deadline)??;
        crate::recovery_evidence_window::validate_evidence_window(
            &collected.window,
            &snapshot,
            self.receipt().checkpoint.cursor.observed_at,
            &owner,
            collected.inspected_at,
        )
        .map_err(CoherentInspectionError::from)?;
        apexes_oms::compare_terminal_evidence(&snapshot, &collected.window.terminal_orders)
            .map_err(CoherentInspectionError::from)?;
        let fills = snapshot
            .fills
            .into_iter()
            .map(Fill::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| CoherentInspectionError::Fill)?;
        crate::recovery_wallet::validate_wallet_evidence(
            &collected.wallet,
            &collected.window,
            &owner,
            collected.inspected_at,
            instruments,
            &fills,
        )
        .map_err(CoherentInspectionError::from)?;
        // Reverify exact journal after the external sequence. Never return
        // evidence if the durable boundary changed during collection.
        store
            .inspect_coherent_recovery(
                self.predecessor.receipt(),
                self.receipt(),
                &owner,
                ems_postgres_url,
            )
            .await
            .map_err(CoherentInspectionError::from)?;
        if monotonic.elapsed() > Duration::from_secs(60)
            || now_after(collected.inspected_at)?.signed_duration_since(started_at)
                > crate::recovery_evidence_window::MAX_EVIDENCE_AGE
        {
            return Err(E::Deadline);
        }
        Ok(collected)
    }
}
