//! Durable historical qualification of two receipt-bound runs. Neither
//! a checksum nor a successful comparison grants startup authority.
use crate::{
    ReceiptBoundSuccessorRun, StrategyRecoveryError, SuccessorInspectionError,
    inspect_receipt_bound_risk_successor,
    recovery_collection_receipt::digest,
    strategy_recovery::{
        MAX_RECEIPT_FILE_BYTES, digest_valid, read_receipt_file, write_receipt_file,
    },
};
use apexes_backtest::observation_receipt::OpeningRiskSnapshot;
use apexes_oms::CoherentCheckpointReceipt;
use apexes_strategy::Strategy;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;

const DOMAIN: &[u8] = b"apexes-risk-bound-successor-v1:";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    version: u32,
    owner_fingerprint: String,
    prior_boundary: CoherentCheckpointReceipt,
    next_boundary: CoherentCheckpointReceipt,
    prior_witness_sha256: String,
    next_witness_sha256: String,
    opening_risk_sha256: String,
    opening_risk_captured_at: DateTime<Utc>,
}

/// A bounded canonical receipt for an exact historical successor pair.
/// Retain the receipt digest, both transition witness digests and the
/// opening risk evidence independently. Reading is integrity checking,
/// not qualification; rerun the historical inspection before comparing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskBoundSuccessorReceipt {
    body: Body,
    sha256: String,
}

impl RiskBoundSuccessorReceipt {
    fn encode(&self) -> Result<Vec<u8>, StrategyRecoveryError> {
        let b = &self.body;
        let a = &b.prior_boundary;
        let n = &b.next_boundary;
        if b.version != 1
            || a.version != 1
            || n.version != 1
            || a.run_scope.is_nil()
            || n.run_scope.is_nil()
            || a.run_scope == n.run_scope
            || a.checkpoint.identity != n.checkpoint.identity
            || a.checkpoint.cursor.ordinal == 0
            || a.checkpoint.cursor.ordinal >= n.checkpoint.cursor.ordinal
            || a.checkpoint.cursor.observed_at > n.checkpoint.cursor.observed_at
            || b.opening_risk_captured_at < a.checkpoint.cursor.observed_at
            || !digest_valid(&b.owner_fingerprint)
            || !digest_valid(&b.prior_witness_sha256)
            || !digest_valid(&b.next_witness_sha256)
            || b.prior_witness_sha256 == b.next_witness_sha256
            || !digest_valid(&b.opening_risk_sha256)
            || [a, n].iter().any(|r| {
                !digest_valid(&r.checkpoint.envelope_sha256)
                    || !digest_valid(&r.journal_sha256)
                    || !r.capture_sha256.as_ref().is_some_and(|d| digest_valid(d))
            })
            || self.sha256 != digest(DOMAIN, b)?
        {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        let bytes = serde_json::to_vec(self).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        if bytes.len() > MAX_RECEIPT_FILE_BYTES {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(bytes)
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn write_new(&self, path: &Path) -> Result<(), StrategyRecoveryError> {
        write_receipt_file(path, &self.encode()?)
    }

    pub fn read(
        path: &Path,
        independently_selected_sha256: &str,
    ) -> Result<Self, StrategyRecoveryError> {
        let bytes = read_receipt_file(path)?;
        let receipt: Self =
            serde_json::from_slice(&bytes).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        if receipt.sha256 != independently_selected_sha256 || receipt.encode()? != bytes {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(receipt)
    }

    /// Compare to a newly independently qualified receipt.
    pub fn compare(&self, qualified: &Self) -> Result<(), StrategyRecoveryError> {
        self.encode()?;
        qualified.encode()?;
        if self != qualified {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(())
    }
}

/// Issue only after complete requalification of both selected transition
/// witnesses and exact opening risk continuity. No new observation time
/// is assigned, and no live state or startup claim is modified.
pub async fn inspect_risk_bound_successor_receipt<S: Strategy, T: Strategy>(
    prior: ReceiptBoundSuccessorRun<'_, S>,
    next: ReceiptBoundSuccessorRun<'_, T>,
    independently_selected_opening: &OpeningRiskSnapshot,
) -> Result<RiskBoundSuccessorReceipt, SuccessorInspectionError> {
    let body = Body {
        version: 1,
        owner_fingerprint: prior.selection.predecessor.owner_fingerprint().into(),
        prior_boundary: prior.selection.receipt().clone(),
        next_boundary: next.selection.receipt().clone(),
        prior_witness_sha256: prior.witness_sha256.into(),
        next_witness_sha256: next.witness_sha256.into(),
        opening_risk_sha256: digest(
            b"apexes-risk-bound-successor-opening-v1:",
            independently_selected_opening,
        )?,
        opening_risk_captured_at: independently_selected_opening.captured_at,
    };
    inspect_receipt_bound_risk_successor(prior, next, independently_selected_opening).await?;
    let sha256 = digest(DOMAIN, &body)?;
    let receipt = RiskBoundSuccessorReceipt { body, sha256 };
    receipt.encode()?;
    Ok(receipt)
}
