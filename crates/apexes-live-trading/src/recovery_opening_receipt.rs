//! Historical opening collection receipts. No funding ownership or
//! startup authority is inferred from an independently selected digest.
use crate::{
    CoherentInspectionError, CoherentRecoverySelection, CollectedOpeningEvidence, EvidenceWindow,
    LedgerContinuityError, OpeningCollectionError, OpeningTrackedAssetEvidence,
    OpeningWalletEvidence, RecoverySelection, SpotCollectionReceipt, StrategyRecoveryError,
    recovery_collection_receipt::{RetainedWallet, digest},
    recovery_opening_collection::validate_allocation,
    strategy_recovery::{digest_valid, read_bounded_receipt_file, write_receipt_file},
};
use apexes_core::{AssetClass, Fill, Instrument, Venue};
use apexes_oms::{OrderStore, SpotEvidenceSource};
use apexes_strategy::checkpoint::CheckpointReceipt;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::path::Path;

const MAX_BYTES: usize = 1024 * 1024;
const DOMAIN: &[u8] = b"apexes-opening-collection-receipt-v1:";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    version: u32,
    predecessor: CheckpointReceipt,
    owner_fingerprint: String,
    selection_sha256: String,
    instruments_sha256: String,
    wallet: RetainedWallet,
    allocation: OpeningTrackedAssetEvidence,
    open_before: EvidenceWindow,
    open_after: EvidenceWindow,
    inspected_at: DateTime<Utc>,
}

/// A historical observation, sealed only after successful opening
/// collection. Retain the digest separately in a trusted location.
/// Reopening is neither authentication nor evidence of current freshness.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpeningCollectionReceipt {
    body: Body,
    sha256: String,
}

impl OpeningCollectionReceipt {
    fn issue(
        selection: &RecoverySelection,
        instruments: &[Instrument],
        collected: CollectedOpeningEvidence,
    ) -> Result<Self, StrategyRecoveryError> {
        let body = Body {
            version: 1,
            predecessor: collected.opening.predecessor,
            owner_fingerprint: selection.owner_fingerprint().into(),
            selection_sha256: digest(b"apexes-opening-selection-v1:", selection)?,
            instruments_sha256: digest(b"apexes-opening-instruments-v1:", &instruments)?,
            wallet: RetainedWallet::retain(collected.opening.wallet),
            allocation: collected.allocation,
            open_before: collected.open_before,
            open_after: collected.open_after,
            inspected_at: collected.inspected_at,
        };
        let sha256 = digest(DOMAIN, &body)?;
        let receipt = Self { body, sha256 };
        receipt.encode()?;
        Ok(receipt)
    }

    fn evidence(&self) -> CollectedOpeningEvidence {
        let b = &self.body;
        CollectedOpeningEvidence {
            opening: OpeningWalletEvidence {
                predecessor: b.predecessor.clone(),
                wallet: b.wallet.evidence(),
            },
            allocation: b.allocation.clone(),
            open_before: b.open_before,
            open_after: b.open_after,
            inspected_at: b.inspected_at,
        }
    }

    fn encode(&self) -> Result<Vec<u8>, StrategyRecoveryError> {
        let b = &self.body;
        if !b.wallet.canonical_rows() {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        let e = self.evidence();
        let w = &e.opening.wallet;
        if b.version != 1
            || b.predecessor.cursor.ordinal == 0
            || b.predecessor.identity.payload_version == 0
            || !digest_valid(&b.predecessor.envelope_sha256)
            || !digest_valid(&b.predecessor.identity.config_sha256)
            || !digest_valid(&b.owner_fingerprint)
            || !digest_valid(&b.selection_sha256)
            || !digest_valid(&b.instruments_sha256)
            || w.owner_fingerprint != b.owner_fingerprint
            || w.venue != Venue::Binance
            || w.asset_class != AssetClass::Spot
            || !w.account_wide_complete
            || b.open_before.started_at < b.predecessor.cursor.observed_at
            || b.open_before.finished_at < b.open_before.started_at
            || w.window.started_at < b.open_before.finished_at
            || w.window.finished_at < w.window.started_at
            || b.open_after.started_at < w.window.finished_at
            || b.open_after.finished_at < b.open_after.started_at
            || b.inspected_at != b.open_after.finished_at
            || b.allocation.captured_at != w.window.finished_at
            || b.inspected_at
                .signed_duration_since(b.open_before.started_at)
                > crate::recovery_evidence_window::MAX_EVIDENCE_AGE
            || validate_allocation(&e.opening, &e.allocation, &[]).is_err()
            || self.sha256 != digest(DOMAIN, b)?
        {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        let bytes = serde_json::to_vec(self).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        if bytes.len() > MAX_BYTES {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(bytes)
    }

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

    fn select(
        &self,
        selection: &RecoverySelection,
        instruments: &[Instrument],
        tracked: &[(String, Decimal)],
        sha256: &str,
    ) -> Result<CollectedOpeningEvidence, StrategyRecoveryError> {
        self.encode()?;
        selection.validate()?;
        let b = &self.body;
        if self.sha256 != sha256
            || b.predecessor != *selection.receipt()
            || b.owner_fingerprint != selection.owner_fingerprint()
            || b.selection_sha256 != digest(b"apexes-opening-selection-v1:", selection)?
            || b.instruments_sha256 != digest(b"apexes-opening-instruments-v1:", &instruments)?
            || b.allocation.balances != tracked
        {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        let e = self.evidence();
        validate_allocation(&e.opening, &e.allocation, instruments)
            .map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        Ok(e)
    }
}

impl RecoverySelection {
    /// Collect and seal the opening evidence without reserving the claim.
    pub async fn collect_opening_spot_receipt(
        &self,
        store: &mut OrderStore,
        ems_postgres_url: &str,
        source: &dyn SpotEvidenceSource,
        strategy_ref: &serde_json::Value,
        instruments: &[Instrument],
        tracked: &[(String, Decimal)],
    ) -> Result<OpeningCollectionReceipt, OpeningCollectionError> {
        let evidence = self
            .collect_opening_spot_evidence(
                store,
                ems_postgres_url,
                source,
                strategy_ref,
                instruments,
                tracked,
            )
            .await?;
        Ok(OpeningCollectionReceipt::issue(
            self,
            instruments,
            evidence,
        )?)
    }
}

/// Both receipts and allocation amounts must be independently selected.
/// The opening receipt is collected before any claim; the closing
/// receipt is collected only after the coherent boundary is durable.
pub struct OpeningClosingEvidenceSelection<'a> {
    pub opening: &'a OpeningCollectionReceipt,
    pub opening_sha256: &'a str,
    pub tracked_balances: &'a [(String, Decimal)],
    pub closing: &'a SpotCollectionReceipt,
    pub closing_sha256: &'a str,
}

impl CoherentRecoverySelection {
    /// Read-only historical reinspection of the exact opening/closing
    /// pair. Verify the complete opening sequence precedes retained
    /// activity and exact cash-flow continuity. No live state returned.
    pub async fn inspect_opening_collection_receipt(
        &self,
        store: &mut OrderStore,
        ems_postgres_url: &str,
        instruments: &[Instrument],
        evidence: OpeningClosingEvidenceSelection<'_>,
    ) -> Result<CollectedOpeningEvidence, CoherentInspectionError> {
        let opening = evidence.opening.select(
            &self.predecessor,
            instruments,
            evidence.tracked_balances,
            evidence.opening_sha256,
        )?;
        let closing = self
            .inspect_spot_collection_receipt(
                store,
                ems_postgres_url,
                instruments,
                evidence.closing,
                evidence.closing_sha256,
            )
            .await?;
        let snapshot = store
            .inspect_coherent_recovery(
                self.predecessor.receipt(),
                self.receipt(),
                self.predecessor.owner_fingerprint(),
                ems_postgres_url,
            )
            .await?;
        let fills = snapshot
            .fills
            .iter()
            .cloned()
            .map(Fill::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| CoherentInspectionError::Fill)?;
        let capture = self
            .inspect_durable_capture(store, ems_postgres_url)
            .await?;
        // The observation prefix provides the first completed market
        // iteration, not a proof of the first callback entry time.
        let first = capture
            .records
            .first()
            .ok_or(LedgerContinuityError::Interval)?;
        if opening.inspected_at >= first.cursor.observed_at
            || opening.inspected_at > closing.window.terminal_window.started_at
            || snapshot
                .orders
                .iter()
                .any(|o| opening.inspected_at >= o.created_at)
            || fills.iter().any(|f| opening.inspected_at >= f.filled_at)
        {
            return Err(LedgerContinuityError::Interval.into());
        }
        crate::recovery_ledger::validate_ledger_continuity(
            &opening.opening,
            &closing.wallet,
            (
                self.predecessor.receipt(),
                self.predecessor.owner_fingerprint(),
            ),
            &closing.window,
            &snapshot,
            instruments,
            &fills,
        )?;
        store
            .inspect_coherent_recovery(
                self.predecessor.receipt(),
                self.receipt(),
                self.predecessor.owner_fingerprint(),
                ems_postgres_url,
            )
            .await?;
        Ok(opening)
    }
}
