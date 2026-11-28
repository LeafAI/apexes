//! Durable historical collection evidence. A checksum is not authentication;
//! independently retain the digest returned by successful collection.
use crate::{
    CoherentInspectionError, CoherentRecoverySelection, CollectedSpotEvidence, EvidenceWindow,
    RecoveryEvidenceWindow, SpotCollectionError, SpotWalletBalance, WalletEvidence,
    strategy_recovery::{
        StrategyRecoveryError, digest_valid, read_bounded_receipt_file, write_receipt_file,
    },
};
use apexes_core::{AssetClass, Fill, Instrument, Venue};
use apexes_oms::{CoherentCheckpointReceipt, OrderStore, SpotEvidenceSource};
use apexes_strategy::checkpoint::CheckpointReceipt;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path};

const MAX_BYTES: usize = 16 * 1024 * 1024;

fn digest(domain: &[u8], value: &impl Serialize) -> Result<String, StrategyRecoveryError> {
    let bytes = serde_json::to_vec(value).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
    if bytes.len() > MAX_BYTES {
        return Err(StrategyRecoveryError::InvalidReceipt);
    }
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(bytes);
    Ok(format!("{:x}", hash.finalize()))
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedWallet {
    window: EvidenceWindow,
    owner_fingerprint: String,
    venue: Venue,
    asset_class: AssetClass,
    account_wide_complete: bool,
    // Sequence avoids silent duplicate JSON-map keys and preserves zero rows.
    balances: Vec<(String, SpotWalletBalance)>,
}

impl RetainedWallet {
    fn retain(wallet: WalletEvidence) -> Self {
        let balances = wallet.balances.into_iter().collect::<BTreeMap<_, _>>();
        Self {
            window: wallet.window,
            owner_fingerprint: wallet.owner_fingerprint,
            venue: wallet.venue,
            asset_class: wallet.asset_class,
            account_wide_complete: wallet.account_wide_complete,
            balances: balances.into_iter().collect(),
        }
    }

    fn evidence(&self) -> WalletEvidence {
        WalletEvidence {
            window: self.window,
            owner_fingerprint: self.owner_fingerprint.clone(),
            venue: self.venue,
            asset_class: self.asset_class,
            account_wide_complete: self.account_wide_complete,
            balances: self.balances.iter().cloned().collect(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    version: u32,
    predecessor: CheckpointReceipt,
    boundary: CoherentCheckpointReceipt,
    owner_fingerprint: String,
    selection_sha256: String,
    instruments_sha256: String,
    window: RecoveryEvidenceWindow,
    wallet: RetainedWallet,
    inspected_at: DateTime<Utc>,
}

/// Explicitly selected historical evidence; never a startup permit.
/// Read requires an independently retained digest, not a checksum obtained
/// from the same file. A trusted collector and protected selection remain
/// prerequisites; no authenticity or current freshness is inferred.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpotCollectionReceipt {
    body: Body,
    sha256: String,
}

impl SpotCollectionReceipt {
    fn issue(
        selection: &CoherentRecoverySelection,
        instruments: &[Instrument],
        collected: CollectedSpotEvidence,
    ) -> Result<Self, StrategyRecoveryError> {
        let body = Body {
            version: 1,
            predecessor: selection.predecessor.receipt().clone(),
            boundary: selection.receipt().clone(),
            owner_fingerprint: selection.predecessor.owner_fingerprint().into(),
            selection_sha256: digest(b"apexes-spot-collection-selection-v1:", selection)?,
            instruments_sha256: digest(b"apexes-spot-collection-instruments-v1:", &instruments)?,
            window: collected.window,
            wallet: RetainedWallet::retain(collected.wallet),
            inspected_at: collected.inspected_at,
        };
        let sha256 = digest(b"apexes-spot-collection-receipt-v1:", &body)?;
        let receipt = Self { body, sha256 };
        receipt.encode()?;
        Ok(receipt)
    }

    fn encode(&self) -> Result<Vec<u8>, StrategyRecoveryError> {
        let b = &self.body;
        let w = &b.window;
        let wallet = &b.wallet;
        if b.version != 1
            || b.boundary.version != 1
            || b.boundary.run_scope.is_nil()
            || b.predecessor.identity != b.boundary.checkpoint.identity
            || b.predecessor.cursor.ordinal == 0
            || b.boundary.checkpoint.cursor.ordinal <= b.predecessor.cursor.ordinal
            || b.boundary.checkpoint.cursor.observed_at < b.predecessor.cursor.observed_at
            || !digest_valid(&b.predecessor.envelope_sha256)
            || !digest_valid(&b.boundary.checkpoint.envelope_sha256)
            || !digest_valid(&b.boundary.journal_sha256)
            || b.boundary
                .capture_sha256
                .as_ref()
                .is_some_and(|d| !digest_valid(d))
            || !digest_valid(&b.owner_fingerprint)
            || !digest_valid(&b.selection_sha256)
            || !digest_valid(&b.instruments_sha256)
            || w.terminal_owner_fingerprint != b.owner_fingerprint
            || w.open_orders.owner_fingerprint != b.owner_fingerprint
            || wallet.owner_fingerprint != b.owner_fingerprint
            || !w.terminal_evidence_complete
            || !w.open_orders.account_wide_complete
            || !wallet.account_wide_complete
            || w.open_orders.venue != Venue::Binance
            || wallet.venue != Venue::Binance
            || w.open_orders.asset_class != AssetClass::Spot
            || wallet.asset_class != AssetClass::Spot
            || w.open_orders.open_order_count != 0
            || w.open_orders.foreign_order_count != 0
            || w.terminal_window.started_at < b.boundary.checkpoint.cursor.observed_at
            || w.terminal_window.finished_at < w.terminal_window.started_at
            || w.open_orders.window.started_at < w.terminal_window.finished_at
            || w.open_orders.window.finished_at < w.open_orders.window.started_at
            || wallet.window.started_at < w.open_orders.window.finished_at
            || wallet.window.finished_at < wallet.window.started_at
            || b.inspected_at < wallet.window.finished_at
            || b.inspected_at
                .signed_duration_since(w.terminal_window.started_at)
                > crate::recovery_evidence_window::MAX_EVIDENCE_AGE
            || w.terminal_orders.len() > 2048
            || w.terminal_orders.len() as i64 != b.boundary.order_count
            || wallet.balances.len() > 1024
            || !wallet.balances.windows(2).all(|w| w[0].0 < w[1].0)
            || self.sha256 != digest(b"apexes-spot-collection-receipt-v1:", b)?
        {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        let bytes = serde_json::to_vec(self).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        if bytes.len() > MAX_BYTES {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(bytes)
    }

    /// Retain this digest independently at collection time, outside the
    /// evidence file and checkpoint store.
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn collected_at(&self) -> DateTime<Utc> {
        self.body.inspected_at
    }

    pub fn write_new(&self, path: &Path) -> Result<(), StrategyRecoveryError> {
        write_receipt_file(path, &self.encode()?)
    }

    pub fn read(
        path: &Path,
        independently_selected_sha256: &str,
    ) -> Result<Self, StrategyRecoveryError> {
        let bytes = read_bounded_receipt_file(path, MAX_BYTES)?;
        let receipt: Self =
            serde_json::from_slice(&bytes).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        if receipt.sha256 != independently_selected_sha256 || receipt.encode()? != bytes {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(receipt)
    }
}

impl CoherentRecoverySelection {
    /// Collect and seal only a successfully qualified historical sequence.
    pub async fn collect_spot_evidence_receipt(
        &self,
        store: &mut OrderStore,
        ems_postgres_url: &str,
        source: &dyn SpotEvidenceSource,
        strategy_ref: &serde_json::Value,
        instruments: &[Instrument],
    ) -> Result<SpotCollectionReceipt, SpotCollectionError> {
        let collected = self
            .collect_spot_evidence(store, ems_postgres_url, source, strategy_ref, instruments)
            .await?;
        SpotCollectionReceipt::issue(self, instruments, collected)
            .map_err(CoherentInspectionError::from)
            .map_err(SpotCollectionError::from)
    }

    /// Compare retained evidence to the exact durable selection at its
    /// original inspection time. Never re-label it as a fresh observation.
    /// No exchange request, strategy restore, or claim mutation occurs.
    pub async fn inspect_spot_collection_receipt(
        &self,
        store: &mut OrderStore,
        ems_postgres_url: &str,
        instruments: &[Instrument],
        receipt: &SpotCollectionReceipt,
        independently_selected_sha256: &str,
    ) -> Result<CollectedSpotEvidence, CoherentInspectionError> {
        receipt.encode()?;
        let b = &receipt.body;
        if receipt.sha256 != independently_selected_sha256
            || b.owner_fingerprint != self.predecessor.owner_fingerprint()
            || b.predecessor != *self.predecessor.receipt()
            || b.boundary != *self.receipt()
            || b.selection_sha256 != digest(b"apexes-spot-collection-selection-v1:", self)?
            || b.instruments_sha256
                != digest(b"apexes-spot-collection-instruments-v1:", &instruments)?
        {
            return Err(StrategyRecoveryError::InvalidReceipt.into());
        }
        let snapshot = store
            .inspect_coherent_recovery(
                self.predecessor.receipt(),
                self.receipt(),
                &b.owner_fingerprint,
                ems_postgres_url,
            )
            .await?;
        crate::recovery_evidence_window::validate_evidence_window(
            &b.window,
            &snapshot,
            b.boundary.checkpoint.cursor.observed_at,
            &b.owner_fingerprint,
            b.inspected_at,
        )?;
        apexes_oms::compare_terminal_evidence(&snapshot, &b.window.terminal_orders)?;
        let fills = snapshot
            .fills
            .into_iter()
            .map(Fill::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| CoherentInspectionError::Fill)?;
        let wallet = b.wallet.evidence();
        crate::recovery_wallet::validate_wallet_evidence(
            &wallet,
            &b.window,
            &b.owner_fingerprint,
            b.inspected_at,
            instruments,
            &fills,
        )?;
        store
            .inspect_coherent_recovery(
                self.predecessor.receipt(),
                self.receipt(),
                &b.owner_fingerprint,
                ems_postgres_url,
            )
            .await?;
        Ok(CollectedSpotEvidence {
            window: b.window.clone(),
            wallet,
            inspected_at: b.inspected_at,
        })
    }
}
