//! Separate retention of risk state from an acknowledged closing capture.
use crate::{
    CoherentInspectionError, CoherentRecoverySelection, RecoverySelection, StrategyRecoveryError,
    recovery_collection_receipt::digest,
    strategy_recovery::{digest_valid, read_bounded_receipt_file, write_receipt_file},
};
use apexes_backtest::observation_receipt::CapturedObservations;
use apexes_oms::{CoherentCheckpointReceipt, OrderStore};
use apexes_strategy::{MarginTierTable, checkpoint::CheckpointReceipt};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::sync::oneshot;

const MAX_BYTES: usize = 16 * 1024 * 1024;
const DOMAIN: &[u8] = b"apexes-closing-risk-handoff-v1:";

/// Caller-selected create-new path and separate digest delivery.
pub struct ClosingRiskHandoff {
    pub path: PathBuf,
    pub digest: oneshot::Sender<String>,
}

/// Both optional evidence channels are required on the explicit paired path.
pub struct RiskBoundaryHandoffs {
    pub opening: crate::OpeningRiskHandoff,
    pub closing: ClosingRiskHandoff,
}

/// Actual closing state, including configuration and coverage flags.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosingRiskSnapshot {
    pub captured_at: DateTime<Utc>,
    pub registered_risk: Option<apexes_risk::RegisteredRiskSnapshot>,
    pub market_monitor: Option<apexes_risk::MarketConditionSnapshot>,
    pub margin_tiers: Option<Vec<(String, MarginTierTable)>>,
    pub market_monitor_unqualified: bool,
    pub margin_monitor_unqualified: bool,
}

impl ClosingRiskSnapshot {
    fn from_capture(capture: &CapturedObservations) -> Result<Self, StrategyRecoveryError> {
        Ok(Self {
            captured_at: capture
                .closing_risk_captured_at
                .ok_or(StrategyRecoveryError::InvalidReceipt)?,
            registered_risk: capture.registered_risk.clone(),
            market_monitor: capture.market_monitor.clone(),
            margin_tiers: capture.margin_tiers.clone(),
            market_monitor_unqualified: capture.market_monitor_unqualified,
            margin_monitor_unqualified: capture.margin_monitor_unqualified,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    version: u32,
    predecessor: CheckpointReceipt,
    owner_fingerprint: String,
    boundary: CoherentCheckpointReceipt,
    snapshot: ClosingRiskSnapshot,
    retained_at: DateTime<Utc>,
}

/// Historical closing receipt; reading alone does not qualify the run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosingRiskReceipt {
    body: Body,
    sha256: String,
}

impl ClosingRiskReceipt {
    pub(crate) fn retain(
        selection: &RecoverySelection,
        boundary: &CoherentCheckpointReceipt,
        capture: &CapturedObservations,
        handoff: ClosingRiskHandoff,
    ) -> Result<(), StrategyRecoveryError> {
        let bytes = capture
            .encode_durable()
            .map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        if boundary.capture_sha256.as_deref() != Some(&format!("{:x}", Sha256::digest(&bytes)))
            || capture.receipt.predecessor != selection.receipt().cursor
            || capture.receipt.through != boundary.checkpoint.cursor
        {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        let body = Body {
            version: 1,
            predecessor: selection.receipt().clone(),
            owner_fingerprint: selection.owner_fingerprint().into(),
            boundary: boundary.clone(),
            snapshot: ClosingRiskSnapshot::from_capture(capture)?,
            retained_at: Utc::now(),
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
        let boundary = &b.boundary;
        if b.version != 1
            || boundary.version != 1
            || boundary.run_scope.is_nil()
            || b.predecessor.identity != boundary.checkpoint.identity
            || b.predecessor.cursor.ordinal == 0
            || b.predecessor.cursor.ordinal >= boundary.checkpoint.cursor.ordinal
            || b.predecessor.cursor.observed_at > boundary.checkpoint.cursor.observed_at
            || b.snapshot.captured_at < b.predecessor.cursor.observed_at
            || b.retained_at < b.snapshot.captured_at
            || !digest_valid(&b.owner_fingerprint)
            || !digest_valid(&b.predecessor.envelope_sha256)
            || !digest_valid(&boundary.checkpoint.envelope_sha256)
            || !digest_valid(&boundary.journal_sha256)
            || !boundary
                .capture_sha256
                .as_ref()
                .is_some_and(|d| digest_valid(d))
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

    /// Original post-acknowledgement retention time, not exchange time.
    pub fn retained_at(&self) -> DateTime<Utc> {
        self.body.retained_at
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
    /// Reinspect the exact acknowledged journal and durable capture,
    /// refusing detached, legacy or different-run closing evidence.
    pub async fn inspect_closing_risk_receipt(
        &self,
        store: &mut OrderStore,
        ems_postgres_url: &str,
        receipt: &ClosingRiskReceipt,
        independently_selected_sha256: &str,
    ) -> Result<ClosingRiskSnapshot, CoherentInspectionError> {
        receipt.encode()?;
        let b = &receipt.body;
        if receipt.sha256 != independently_selected_sha256
            || b.predecessor != *self.predecessor.receipt()
            || b.owner_fingerprint != self.predecessor.owner_fingerprint()
            || b.boundary != *self.receipt()
        {
            return Err(StrategyRecoveryError::InvalidReceipt.into());
        }
        let capture = self
            .inspect_durable_capture(store, ems_postgres_url)
            .await?;
        if ClosingRiskSnapshot::from_capture(&capture)? != b.snapshot {
            return Err(StrategyRecoveryError::InvalidReceipt.into());
        }
        Ok(b.snapshot.clone())
    }
}
