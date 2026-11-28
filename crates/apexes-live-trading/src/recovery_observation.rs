//! Independent offline observation evidence; no live capture or replay.
use crate::strategy_recovery::{
    MAX_RECEIPT_FILE_BYTES, StrategyRecoveryError, digest_valid, read_receipt_file,
    write_receipt_file,
};
use apexes_backtest::observation_receipt::{
    CompletedObservation, ObservationError, ObservationReceipt,
};
use apexes_core::Venue;
use apexes_oms::CoherentCheckpointReceipt;
use apexes_strategy::checkpoint::CheckpointReceipt;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationBoundaryEvidence {
    version: u32,
    owner_fingerprint: String,
    predecessor: CheckpointReceipt,
    boundary: CoherentCheckpointReceipt,
    history: ObservationReceipt,
}

#[derive(Debug, thiserror::Error)]
pub enum ObservationEvidenceError {
    #[error("invalid observation evidence encoding")]
    Encoding,
    #[error("observation evidence belongs to a different boundary or owner")]
    Identity,
    #[error("observation evidence comparison refused: {0}")]
    History(#[from] ObservationError),
}

impl ObservationBoundaryEvidence {
    /// Independently retain the digest of supplied completed observations.
    /// The constructor cannot establish actual capture or completeness.
    pub fn new(
        owner_fingerprint: String,
        predecessor: CheckpointReceipt,
        boundary: CoherentCheckpointReceipt,
        history: ObservationReceipt,
    ) -> Result<Self, StrategyRecoveryError> {
        let evidence = Self {
            version: 1,
            owner_fingerprint,
            predecessor,
            boundary,
            history,
        };
        evidence.encode()?;
        Ok(evidence)
    }

    fn encode(&self) -> Result<Vec<u8>, StrategyRecoveryError> {
        if self.version != 1
            || self.boundary.version != 1
            || self.boundary.run_scope.is_nil()
            || !digest_valid(&self.owner_fingerprint)
            || !digest_valid(&self.predecessor.envelope_sha256)
            || !digest_valid(&self.boundary.checkpoint.envelope_sha256)
            || !digest_valid(&self.boundary.journal_sha256)
            || self.predecessor.identity != self.boundary.checkpoint.identity
            || self.history.predecessor != self.predecessor.cursor
            || self.history.through != self.boundary.checkpoint.cursor
            || self.history.validate().is_err()
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
        let evidence: Self =
            serde_json::from_slice(&bytes).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        if evidence.encode()? != bytes {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(evidence)
    }

    pub(crate) fn compare(
        &self,
        owner: &str,
        predecessor: &CheckpointReceipt,
        boundary: &CoherentCheckpointReceipt,
        scope: &[(Venue, String)],
        records: &[CompletedObservation],
    ) -> Result<(), ObservationEvidenceError> {
        self.encode()
            .map_err(|_| ObservationEvidenceError::Encoding)?;
        if self.owner_fingerprint != owner
            || &self.predecessor != predecessor
            || &self.boundary != boundary
        {
            return Err(ObservationEvidenceError::Identity);
        }
        self.history.verify(scope, records)?;
        Ok(())
    }
}
