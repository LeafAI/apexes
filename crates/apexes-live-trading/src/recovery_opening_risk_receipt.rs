//! Capture-time handoff of the actual pre-event risk snapshot.
use crate::{
    CoherentInspectionError, CoherentRecoverySelection, RecoverySelection, StrategyRecoveryError,
    recovery_collection_receipt::digest,
    strategy_recovery::{digest_valid, read_bounded_receipt_file, write_receipt_file},
};
use apexes_backtest::observation_receipt::OpeningRiskSnapshot;
use apexes_oms::OrderStore;
use apexes_strategy::checkpoint::CheckpointReceipt;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::sync::oneshot;

const MAX_BYTES: usize = 16 * 1024 * 1024;
const DOMAIN: &[u8] = b"apexes-opening-risk-handoff-v1:";

/// Opt-in destination and separate capture-time digest delivery.
/// The caller owns protected retention of the file and digest.
pub struct OpeningRiskHandoff {
    pub path: PathBuf,
    pub digest: oneshot::Sender<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    version: u32,
    predecessor: CheckpointReceipt,
    owner_fingerprint: String,
    run_scope: uuid::Uuid,
    snapshot: OpeningRiskSnapshot,
}

/// Historical evidence only; not a startup permit or live risk state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpeningRiskReceipt {
    body: Body,
    sha256: String,
}

impl OpeningRiskReceipt {
    pub(crate) fn retain(
        selection: &RecoverySelection,
        run_scope: uuid::Uuid,
        snapshot_bytes: &[u8],
        handoff: OpeningRiskHandoff,
    ) -> Result<(), StrategyRecoveryError> {
        if snapshot_bytes.len() > MAX_BYTES {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        let snapshot = serde_json::from_slice(snapshot_bytes)
            .map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        let body = Body {
            version: 1,
            predecessor: selection.receipt().clone(),
            owner_fingerprint: selection.owner_fingerprint().into(),
            run_scope,
            snapshot,
        };
        let sha256 = digest(DOMAIN, &body)?;
        let receipt = Self { body, sha256 };
        write_receipt_file(&handoff.path, &receipt.encode()?)?;
        handoff
            .digest
            .send(receipt.sha256)
            .map_err(|_| StrategyRecoveryError::ReceiptIo)
    }

    fn encode(&self) -> Result<Vec<u8>, StrategyRecoveryError> {
        let b = &self.body;
        if b.version != 1
            || b.run_scope.is_nil()
            || b.predecessor.cursor.ordinal == 0
            || b.snapshot.captured_at < b.predecessor.cursor.observed_at
            || !digest_valid(&b.predecessor.envelope_sha256)
            || !digest_valid(&b.owner_fingerprint)
            || b.snapshot
                .registered_risk
                .as_ref()
                .is_some_and(|s| s.validate().is_err())
            || b.snapshot
                .market_monitor
                .as_ref()
                .is_some_and(|s| s.validate().is_err())
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
    /// Verify the separately retained capture-time handoff against the
    /// exact acknowledged durable run and its actual opening snapshot.
    pub async fn inspect_opening_risk_receipt(
        &self,
        store: &mut OrderStore,
        ems_postgres_url: &str,
        receipt: &OpeningRiskReceipt,
        independently_selected_sha256: &str,
    ) -> Result<OpeningRiskSnapshot, CoherentInspectionError> {
        receipt.encode()?;
        let b = &receipt.body;
        if receipt.sha256 != independently_selected_sha256
            || b.predecessor != *self.predecessor.receipt()
            || b.owner_fingerprint != self.predecessor.owner_fingerprint()
            || b.run_scope != self.receipt().run_scope
        {
            return Err(StrategyRecoveryError::InvalidReceipt.into());
        }
        let capture = self
            .inspect_durable_capture(store, ems_postgres_url)
            .await?;
        if capture.opening_risk.as_ref() != Some(&b.snapshot) {
            return Err(StrategyRecoveryError::InvalidReceipt.into());
        }
        Ok(b.snapshot.clone())
    }
}
