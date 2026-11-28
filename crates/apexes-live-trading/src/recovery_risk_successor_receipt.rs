//! Durable historical qualification of two receipt-bound runs. Neither
//! a checksum nor a successful comparison grants startup authority.
use crate::{
    OpeningRiskReceipt, ReceiptBoundSuccessorRun, StrategyRecoveryError, SuccessorInspectionError,
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

/// Independently selected capture-time opening risk handoff, rather than
/// a detached snapshot reconstructed from the closing database.
pub struct OpeningRiskReceiptSelection<'a> {
    pub receipt: &'a OpeningRiskReceipt,
    pub sha256: &'a str,
}

const HANDOFF_DOMAIN: &[u8] = b"apexes-risk-handoff-successor-v1:";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HandoffBody {
    version: u32,
    qualification: RiskBoundSuccessorReceipt,
    opening_handoff_sha256: String,
}

/// Stronger historical receipt binding the independently retained
/// capture-time handoff to the complete risk-bound qualification.
/// Retain all original inputs independently and requalify before
/// comparison. This does not grant live startup authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskHandoffSuccessorReceipt {
    body: HandoffBody,
    sha256: String,
}

impl RiskHandoffSuccessorReceipt {
    fn encode(&self) -> Result<Vec<u8>, StrategyRecoveryError> {
        let b = &self.body;
        b.qualification.encode()?;
        if b.version != 1
            || !digest_valid(&b.opening_handoff_sha256)
            || self.sha256 != digest(HANDOFF_DOMAIN, b)?
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

    /// Compare against a newly independently qualified handoff receipt.
    pub fn compare(&self, qualified: &Self) -> Result<(), StrategyRecoveryError> {
        self.encode()?;
        qualified.encode()?;
        if self != qualified {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(())
    }
}

/// Reinspect the exact independently selected handoff against the
/// acknowledged successor capture, then run all existing successor
/// checks with that snapshot. No detached snapshot input is accepted.
pub async fn inspect_risk_handoff_successor_receipt<S: Strategy, T: Strategy>(
    prior: ReceiptBoundSuccessorRun<'_, S>,
    next: ReceiptBoundSuccessorRun<'_, T>,
    opening: OpeningRiskReceiptSelection<'_>,
) -> Result<RiskHandoffSuccessorReceipt, SuccessorInspectionError> {
    let snapshot = next
        .selection
        .inspect_opening_risk_receipt(
            next.store,
            next.ems_postgres_url,
            opening.receipt,
            opening.sha256,
        )
        .await?;
    qualify_opening_handoff(prior, next, &snapshot, opening.sha256).await
}

async fn qualify_opening_handoff<S: Strategy, T: Strategy>(
    prior: ReceiptBoundSuccessorRun<'_, S>,
    next: ReceiptBoundSuccessorRun<'_, T>,
    snapshot: &OpeningRiskSnapshot,
    opening_sha256: &str,
) -> Result<RiskHandoffSuccessorReceipt, SuccessorInspectionError> {
    let qualification = inspect_risk_bound_successor_receipt(prior, next, snapshot).await?;
    let body = HandoffBody {
        version: 1,
        qualification,
        opening_handoff_sha256: opening_sha256.into(),
    };
    let sha256 = digest(HANDOFF_DOMAIN, &body)?;
    let receipt = RiskHandoffSuccessorReceipt { body, sha256 };
    receipt.encode()?;
    Ok(receipt)
}

/// Independently selected actual prior closing handoff.
pub struct ClosingRiskReceiptSelection<'a> {
    pub receipt: &'a crate::ClosingRiskReceipt,
    pub sha256: &'a str,
}

/// Both independently retained boundary handoffs are required.
pub struct DualRiskHandoffSelection<'a> {
    pub prior_closing: ClosingRiskReceiptSelection<'a>,
    pub next_opening: OpeningRiskReceiptSelection<'a>,
}

const DUAL_DOMAIN: &[u8] = b"apexes-dual-risk-handoff-successor-v1:";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DualBody {
    version: u32,
    qualification: RiskHandoffSuccessorReceipt,
    closing_handoff_sha256: String,
    closing_risk_captured_at: DateTime<Utc>,
    closing_handoff_retained_at: DateTime<Utc>,
}

/// Durable qualification of two independently selected risk handoffs.
/// Reopening checks integrity; independently rerun the complete
/// qualification before comparison. No live authority is returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DualRiskHandoffSuccessorReceipt {
    body: DualBody,
    sha256: String,
}

impl DualRiskHandoffSuccessorReceipt {
    fn encode(&self) -> Result<Vec<u8>, StrategyRecoveryError> {
        let b = &self.body;
        b.qualification.encode()?;
        let inner = &b.qualification.body.qualification.body;
        if b.version != 1
            || !digest_valid(&b.closing_handoff_sha256)
            || b.closing_handoff_sha256 == b.qualification.body.opening_handoff_sha256
            || b.closing_risk_captured_at > b.closing_handoff_retained_at
            || b.closing_handoff_retained_at > inner.opening_risk_captured_at
            || self.sha256 != digest(DUAL_DOMAIN, b)?
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

    pub fn compare(&self, qualified: &Self) -> Result<(), StrategyRecoveryError> {
        self.encode()?;
        qualified.encode()?;
        if self != qualified {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(())
    }
}

fn compare_handoff_state(
    closing: &crate::ClosingRiskSnapshot,
    retained_at: DateTime<Utc>,
    opening: &OpeningRiskSnapshot,
) -> Result<(), crate::SuccessorContinuityError> {
    use crate::SuccessorContinuityError as E;
    if closing.market_monitor_unqualified
        || closing.margin_monitor_unqualified
        || closing.registered_risk != opening.registered_risk
        || closing.market_monitor != opening.market_monitor
    {
        return Err(E::RiskState);
    }
    if closing.captured_at > retained_at || retained_at > opening.captured_at {
        return Err(E::Interval);
    }
    Ok(())
}

/// Require independently retained prior closing and successor opening
/// evidence, exact acknowledged captures and full successor qualification.
pub async fn inspect_dual_risk_handoff_successor_receipt<S: Strategy, T: Strategy>(
    prior: ReceiptBoundSuccessorRun<'_, S>,
    next: ReceiptBoundSuccessorRun<'_, T>,
    handoffs: DualRiskHandoffSelection<'_>,
) -> Result<DualRiskHandoffSuccessorReceipt, SuccessorInspectionError> {
    let closing = prior
        .selection
        .inspect_closing_risk_receipt(
            prior.store,
            prior.ems_postgres_url,
            handoffs.prior_closing.receipt,
            handoffs.prior_closing.sha256,
        )
        .await?;
    let opening = next
        .selection
        .inspect_opening_risk_receipt(
            next.store,
            next.ems_postgres_url,
            handoffs.next_opening.receipt,
            handoffs.next_opening.sha256,
        )
        .await?;
    let retained_at = handoffs.prior_closing.receipt.retained_at();
    compare_handoff_state(&closing, retained_at, &opening)?;
    // Both selected collection receipts will be requalified below.
    // Refuse closing retention after the successor's opening collection starts.
    if retained_at > next.evidence.collections.opening.collection_started_at() {
        return Err(crate::SuccessorContinuityError::Interval.into());
    }
    let qualification =
        qualify_opening_handoff(prior, next, &opening, handoffs.next_opening.sha256).await?;
    let body = DualBody {
        version: 1,
        qualification,
        closing_handoff_sha256: handoffs.prior_closing.sha256.into(),
        closing_risk_captured_at: closing.captured_at,
        closing_handoff_retained_at: retained_at,
    };
    let sha256 = digest(DUAL_DOMAIN, &body)?;
    let receipt = DualRiskHandoffSuccessorReceipt { body, sha256 };
    receipt.encode()?;
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handoff_state_refuses_coverage_loss_and_time_regression() {
        let now = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let mut closing = crate::ClosingRiskSnapshot {
            captured_at: now,
            registered_risk: None,
            market_monitor: None,
            margin_tiers: None,
            market_monitor_unqualified: false,
            margin_monitor_unqualified: false,
        };
        let opening = OpeningRiskSnapshot {
            captured_at: now + chrono::TimeDelta::seconds(2),
            registered_risk: None,
            market_monitor: None,
        };
        let retained = now + chrono::TimeDelta::seconds(1);
        compare_handoff_state(&closing, retained, &opening).unwrap();
        assert_eq!(
            compare_handoff_state(&closing, now - chrono::TimeDelta::seconds(1), &opening),
            Err(crate::SuccessorContinuityError::Interval)
        );
        assert_eq!(
            compare_handoff_state(&closing, now + chrono::TimeDelta::seconds(3), &opening),
            Err(crate::SuccessorContinuityError::Interval)
        );
        closing.market_monitor_unqualified = true;
        assert_eq!(
            compare_handoff_state(&closing, retained, &opening),
            Err(crate::SuccessorContinuityError::RiskState)
        );
    }
}
