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
            || self
                .boundary
                .capture_sha256
                .as_ref()
                .is_some_and(|d| !digest_valid(d))
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

/// Independently selected completed history ending at the original startup
/// checkpoint. This supplies a counterfactual opening price window, not
/// evidence that the running ledger consumed it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpeningObservationEvidence {
    version: u32,
    owner_fingerprint: String,
    checkpoint: CheckpointReceipt,
    history: ObservationReceipt,
}

impl OpeningObservationEvidence {
    pub fn new(
        owner_fingerprint: String,
        checkpoint: CheckpointReceipt,
        history: ObservationReceipt,
    ) -> Result<Self, StrategyRecoveryError> {
        let evidence = Self {
            version: 1,
            owner_fingerprint,
            checkpoint,
            history,
        };
        evidence.encode()?;
        Ok(evidence)
    }

    fn encode(&self) -> Result<Vec<u8>, StrategyRecoveryError> {
        if self.version != 1
            || !digest_valid(&self.owner_fingerprint)
            || !digest_valid(&self.checkpoint.envelope_sha256)
            || self.history.through != self.checkpoint.cursor
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
        checkpoint: &CheckpointReceipt,
        scope: &[(Venue, String)],
        records: &[CompletedObservation],
        earliest_fill: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), ObservationEvidenceError> {
        self.verify_history(owner, checkpoint, scope, records)?;
        // The receipt excludes observations at its opening timestamp.
        // Start strictly before every potentially relevant five-minute horizon.
        let earliest = earliest_fill.map_or(checkpoint.cursor.observed_at, |at| {
            at.min(checkpoint.cursor.observed_at)
        });
        let horizon = earliest
            .checked_sub_signed(chrono::TimeDelta::minutes(5))
            .ok_or(ObservationError::Time)?;
        if self.history.predecessor.observed_at >= horizon {
            return Err(ObservationError::Time.into());
        }
        Ok(())
    }

    /// Warm-up has a strategy-specific horizon, not a fee lookback.
    pub(crate) fn verify_history(
        &self,
        owner: &str,
        checkpoint: &CheckpointReceipt,
        scope: &[(Venue, String)],
        records: &[CompletedObservation],
    ) -> Result<(), ObservationEvidenceError> {
        self.encode()
            .map_err(|_| ObservationEvidenceError::Encoding)?;
        if self.owner_fingerprint != owner || &self.checkpoint != checkpoint {
            return Err(ObservationEvidenceError::Identity);
        }
        self.history.verify(scope, records)?;
        Ok(())
    }
}

#[cfg(test)]
mod opening_tests {
    use super::*;
    use apexes_backtest::observation_receipt::ObservationClock;
    use apexes_core::{MarketEvent, TradeEvent};
    use apexes_strategy::checkpoint::{StateCursor, StateIdentity};
    use chrono::{TimeDelta, Utc};
    use rust_decimal_macros::dec;

    #[test]
    fn opening_receipt_requires_exact_checkpoint_owner_scope_and_lookback() {
        let end = chrono::DateTime::<Utc>::from_timestamp(1_800_000_000, 0).unwrap();
        let checkpoint = CheckpointReceipt {
            identity: StateIdentity {
                strategy: "test".into(),
                revision: "1".into(),
                config_sha256: "a".repeat(64),
                scope: "scope".into(),
                payload_version: 1,
            },
            cursor: StateCursor {
                ordinal: 2,
                observed_at: end,
            },
            envelope_sha256: "b".repeat(64),
        };
        let owner = "c".repeat(64);
        let scope = [(Venue::Binance, "BNBUSDT".into())];
        let records = [CompletedObservation {
            cursor: checkpoint.cursor,
            event: MarketEvent::Trade(TradeEvent {
                symbol: "BNBUSDT".into(),
                venue: Venue::Binance,
                price: dec!(300),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: end,
            }),
        }];
        let make = |seconds| {
            let start = StateCursor {
                ordinal: 1,
                observed_at: end - TimeDelta::seconds(seconds),
            };
            let receipt = ObservationReceipt::build(
                start,
                checkpoint.cursor,
                ObservationClock::SourceAvailability,
                &scope,
                &records,
            )
            .unwrap();
            OpeningObservationEvidence::new(owner.clone(), checkpoint.clone(), receipt).unwrap()
        };
        let evidence = make(301);
        evidence
            .compare(&owner, &checkpoint, &scope, &records, None)
            .unwrap();
        assert!(
            evidence
                .compare(&"d".repeat(64), &checkpoint, &scope, &records, None)
                .is_err()
        );
        let mut other = checkpoint.clone();
        other.envelope_sha256 = "e".repeat(64);
        assert!(
            evidence
                .compare(&owner, &other, &scope, &records, None)
                .is_err()
        );
        assert!(
            evidence
                .compare(
                    &owner,
                    &checkpoint,
                    &[(Venue::Binance, "BTCUSDT".into())],
                    &records,
                    None
                )
                .is_err()
        );
        assert!(
            evidence
                .compare(&owner, &checkpoint, &scope, &[], None)
                .is_err()
        );
        assert!(
            make(300)
                .compare(&owner, &checkpoint, &scope, &records, None)
                .is_err()
        );
        assert!(
            evidence
                .compare(
                    &owner,
                    &checkpoint,
                    &scope,
                    &records,
                    Some(end - TimeDelta::seconds(2))
                )
                .is_err()
        );
    }
}
