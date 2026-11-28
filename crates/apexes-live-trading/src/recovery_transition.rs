//! Historical offline qualification witness; never a startup permit.
use crate::{
    ClosingFeeMarkEvidence, ClosingPositionEvidence, ConversionScopeEvidence,
    ObservationBoundaryEvidence, OpeningTrackedAssetEvidence, OpeningWalletEvidence,
    RecoveryEvidenceWindow, ThirdAssetFeeEvidence, WalletEvidence,
    strategy_recovery::{
        MAX_RECEIPT_FILE_BYTES, StrategyRecoveryError, digest_valid, read_receipt_file,
        write_receipt_file,
    },
};
use apexes_core::Instrument;
use apexes_oms::CoherentCheckpointReceipt;
use apexes_risk::{MarketConditionSnapshot, RegisteredRiskSnapshot};
use apexes_strategy::{MarginTierTable, checkpoint::CheckpointReceipt};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path};

/// Independent inputs to offline qualification. No gateway or live checker.
pub struct TransitionEvidence<'a> {
    pub instruments: &'a [Instrument],
    pub opening: &'a OpeningWalletEvidence,
    pub window: &'a RecoveryEvidenceWindow,
    pub closing: &'a WalletEvidence,
    pub inspected_at: DateTime<Utc>,
    pub positions: &'a ClosingPositionEvidence,
    pub allocation: &'a OpeningTrackedAssetEvidence,
    pub fees: &'a ThirdAssetFeeEvidence,
    pub marks: &'a ClosingFeeMarkEvidence,
    pub conversion: &'a ConversionScopeEvidence,
    pub observations: &'a ObservationBoundaryEvidence,
    pub registered_risk: Option<&'a RegisteredRiskSnapshot>,
    pub market_monitor: Option<&'a MarketConditionSnapshot>,
    pub margin_tiers: Option<&'a [(String, MarginTierTable)]>,
}

fn digest(domain: &[u8], value: &impl Serialize) -> Result<String, StrategyRecoveryError> {
    let bytes = serde_json::to_vec(value).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
    if bytes.len() > 68 * 1024 * 1024 {
        return Err(StrategyRecoveryError::InvalidReceipt);
    }
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(bytes);
    Ok(format!("{:x}", hash.finalize()))
}

fn wallet(w: &WalletEvidence) -> impl Serialize + '_ {
    (
        &w.window,
        &w.owner_fingerprint,
        w.venue,
        w.asset_class,
        w.account_wide_complete,
        w.balances.iter().collect::<BTreeMap<_, _>>(),
    )
}

impl TransitionEvidence<'_> {
    // Preserve supplied sequence order, but canonicalize wallet maps.
    fn digest(&self) -> Result<String, StrategyRecoveryError> {
        digest(
            b"apexes-offline-transition-evidence-v1:",
            &(
                self.instruments,
                (&self.opening.predecessor, wallet(&self.opening.wallet)),
                self.window,
                wallet(self.closing),
                self.inspected_at,
                self.positions,
                self.allocation,
                self.fees,
                self.marks,
                self.conversion,
                self.observations,
                self.registered_risk,
                self.market_monitor,
                self.margin_tiers,
            ),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    version: u32,
    owner_fingerprint: String,
    predecessor: CheckpointReceipt,
    boundary: CoherentCheckpointReceipt,
    selection_sha256: String,
    evidence_sha256: String,
    inspected_at: DateTime<Utc>,
}

/// A historical result, not authorization. Reading verifies encoding and
/// checksum only. Rerun independent qualification and compare the result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfflineTransitionWitness {
    body: Body,
    sha256: String,
}

impl OfflineTransitionWitness {
    pub(crate) fn issue(
        owner: &str,
        predecessor: &CheckpointReceipt,
        boundary: &CoherentCheckpointReceipt,
        selection: &impl Serialize,
        evidence: &TransitionEvidence<'_>,
    ) -> Result<Self, StrategyRecoveryError> {
        let body = Body {
            version: 1,
            owner_fingerprint: owner.into(),
            predecessor: predecessor.clone(),
            boundary: boundary.clone(),
            selection_sha256: digest(b"apexes-offline-transition-selection-v1:", selection)?,
            evidence_sha256: evidence.digest()?,
            inspected_at: evidence.inspected_at,
        };
        let sha256 = digest(b"apexes-offline-transition-witness-v1:", &body)?;
        let witness = Self { body, sha256 };
        witness.encode()?;
        Ok(witness)
    }

    fn encode(&self) -> Result<Vec<u8>, StrategyRecoveryError> {
        let b = &self.body;
        if b.version != 1
            || b.boundary.version != 1
            || b.boundary.run_scope.is_nil()
            || b.boundary.checkpoint.identity != b.predecessor.identity
            || b.predecessor.cursor.ordinal == 0
            || b.boundary.checkpoint.cursor.ordinal <= b.predecessor.cursor.ordinal
            || b.boundary.checkpoint.cursor.observed_at < b.predecessor.cursor.observed_at
            || b.inspected_at < b.boundary.checkpoint.cursor.observed_at
            || !digest_valid(&b.owner_fingerprint)
            || !digest_valid(&b.predecessor.envelope_sha256)
            || !digest_valid(&b.boundary.checkpoint.envelope_sha256)
            || !digest_valid(&b.boundary.journal_sha256)
            || !b
                .boundary
                .capture_sha256
                .as_ref()
                .is_some_and(|d| digest_valid(d))
            || !digest_valid(&b.selection_sha256)
            || !digest_valid(&b.evidence_sha256)
            || self.sha256 != digest(b"apexes-offline-transition-witness-v1:", b)?
        {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        let bytes = serde_json::to_vec(self).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        if bytes.len() > MAX_RECEIPT_FILE_BYTES {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(bytes)
    }

    pub fn write_new(&self, path: &Path) -> Result<(), StrategyRecoveryError> {
        write_receipt_file(path, &self.encode()?)
    }

    pub fn read(path: &Path) -> Result<Self, StrategyRecoveryError> {
        let bytes = read_receipt_file(path)?;
        let witness: Self =
            serde_json::from_slice(&bytes).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        if witness.encode()? != bytes {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(witness)
    }

    /// Compare against a newly independently qualified result.
    pub fn compare(&self, qualified: &Self) -> Result<(), StrategyRecoveryError> {
        self.encode()?;
        qualified.encode()?;
        if self != qualified {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(())
    }
}

/// Independent non-wallet inputs. Wallets, allocation and historical
/// inspection time are taken exclusively from selected collection receipts.
pub struct ReceiptTransitionEvidence<'a> {
    pub instruments: &'a [Instrument],
    pub collections: crate::OpeningClosingEvidenceSelection<'a>,
    pub positions: &'a ClosingPositionEvidence,
    pub fees: &'a ThirdAssetFeeEvidence,
    pub marks: &'a ClosingFeeMarkEvidence,
    pub conversion: &'a ConversionScopeEvidence,
    pub observations: &'a ObservationBoundaryEvidence,
    pub registered_risk: Option<&'a RegisteredRiskSnapshot>,
    pub market_monitor: Option<&'a MarketConditionSnapshot>,
    pub margin_tiers: Option<&'a [(String, MarginTierTable)]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptBoundBody {
    version: u32,
    opening_sha256: String,
    closing_sha256: String,
    transition: OfflineTransitionWitness,
}

/// Historical receipt-bound result, distinct from the supplied evidence
/// witness. Reopen and independently rerun qualification before comparing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptBoundTransitionWitness {
    body: ReceiptBoundBody,
    sha256: String,
}

impl ReceiptBoundTransitionWitness {
    fn issue(
        transition: OfflineTransitionWitness,
        opening_sha256: &str,
        closing_sha256: &str,
    ) -> Result<Self, StrategyRecoveryError> {
        let body = ReceiptBoundBody {
            version: 1,
            opening_sha256: opening_sha256.into(),
            closing_sha256: closing_sha256.into(),
            transition,
        };
        let sha256 = digest(b"apexes-receipt-bound-transition-v1:", &body)?;
        let witness = Self { body, sha256 };
        witness.encode()?;
        Ok(witness)
    }

    fn encode(&self) -> Result<Vec<u8>, StrategyRecoveryError> {
        self.body.transition.encode()?;
        if self.body.version != 1
            || !digest_valid(&self.body.opening_sha256)
            || !digest_valid(&self.body.closing_sha256)
            || self.body.opening_sha256 == self.body.closing_sha256
            || self.sha256 != digest(b"apexes-receipt-bound-transition-v1:", &self.body)?
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
        let witness: Self =
            serde_json::from_slice(&bytes).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        if witness.sha256 != independently_selected_sha256 || witness.encode()? != bytes {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(witness)
    }

    /// Compare only to a newly independently requalified result.
    pub fn compare(&self, qualified: &Self) -> Result<(), StrategyRecoveryError> {
        self.encode()?;
        qualified.encode()?;
        if self != qualified {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(())
    }
}

impl crate::CoherentRecoverySelection {
    /// Reinspect both exact collection receipts and derive all wallet
    /// inputs from them. Run the unchanged complete isolated accounting,
    /// observation and risk qualification. Never authorize startup.
    pub async fn inspect_receipt_bound_transition<S: apexes_strategy::Strategy>(
        &self,
        store: &mut apexes_oms::OrderStore,
        ems_postgres_url: &str,
        strategies: (S, S),
        evidence: &ReceiptTransitionEvidence<'_>,
    ) -> Result<ReceiptBoundTransitionWitness, crate::CoherentInspectionError> {
        let (witness, _, _) = self
            .qualify_receipt_bound_transition(store, ems_postgres_url, strategies, evidence)
            .await?;
        Ok(witness)
    }

    pub(crate) async fn qualify_receipt_bound_transition<S: apexes_strategy::Strategy>(
        &self,
        store: &mut apexes_oms::OrderStore,
        ems_postgres_url: &str,
        strategies: (S, S),
        evidence: &ReceiptTransitionEvidence<'_>,
    ) -> Result<
        (
            ReceiptBoundTransitionWitness,
            crate::CollectedOpeningEvidence,
            crate::CollectedSpotEvidence,
        ),
        crate::CoherentInspectionError,
    > {
        let (opening, closing) = self
            .inspect_opening_closing_pair(
                store,
                ems_postgres_url,
                evidence.instruments,
                evidence.collections,
            )
            .await?;
        let inputs = TransitionEvidence {
            instruments: evidence.instruments,
            opening: &opening.opening,
            window: &closing.window,
            closing: &closing.wallet,
            inspected_at: closing.inspected_at,
            positions: evidence.positions,
            allocation: &opening.allocation,
            fees: evidence.fees,
            marks: evidence.marks,
            conversion: evidence.conversion,
            observations: evidence.observations,
            registered_risk: evidence.registered_risk,
            market_monitor: evidence.market_monitor,
            margin_tiers: evidence.margin_tiers,
        };
        let transition = self
            .inspect_offline_transition(store, ems_postgres_url, strategies, &inputs)
            .await?;
        let witness = ReceiptBoundTransitionWitness::issue(
            transition,
            evidence.collections.opening_sha256,
            evidence.collections.closing_sha256,
        )?;
        Ok((witness, opening, closing))
    }
}
