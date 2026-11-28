//! Independent risk evidence for offline checkpoint inspection.
//! No claim transition or runnable risk state restoration.
use crate::strategy_recovery::{
    MAX_RECEIPT_FILE_BYTES, StrategyRecoveryError, digest_valid, read_receipt_file,
    write_receipt_file,
};
use apexes_oms::{CoherentCheckpointReceipt, CoherentRecoverySnapshot};
use apexes_risk::{CircuitStateSnapshot, LimitStateSnapshot, RiskStateError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskBoundaryEvidence {
    version: u32,
    owner_fingerprint: String,
    boundary: CoherentCheckpointReceipt,
    captured_at: DateTime<Utc>,
    limit_state: LimitStateSnapshot,
    circuit_state: Option<CircuitStateSnapshot>,
}

#[derive(Debug, thiserror::Error)]
pub enum RiskEvidenceError {
    #[error("invalid risk evidence encoding")]
    Encoding,
    #[error("risk evidence does not match the selected boundary or owner")]
    Identity,
    #[error("risk evidence timestamp is outside the selected inspection interval")]
    Interval,
    #[error("risk evidence has invalid configuration or state: {0}")]
    State(#[from] RiskStateError),
    #[error("risk evidence differs in circuit breaker presence")]
    Monitor,
}

impl RiskBoundaryEvidence {
    /// The caller must separately capture these actual monitor states at
    /// the acknowledged boundary; this constructor cannot prove atomicity.
    pub fn new(
        owner_fingerprint: String,
        boundary: CoherentCheckpointReceipt,
        captured_at: DateTime<Utc>,
        limit_state: LimitStateSnapshot,
        circuit_state: Option<CircuitStateSnapshot>,
    ) -> Result<Self, StrategyRecoveryError> {
        let evidence = Self {
            version: 1,
            owner_fingerprint,
            boundary,
            captured_at,
            limit_state,
            circuit_state,
        };
        evidence.encode()?;
        Ok(evidence)
    }

    fn encode(&self) -> Result<Vec<u8>, StrategyRecoveryError> {
        if self.version != 1
            || self.boundary.version != 1
            || self.boundary.run_scope.is_nil()
            || self.boundary.checkpoint.cursor.ordinal == 0
            || !digest_valid(&self.owner_fingerprint)
            || !digest_valid(&self.boundary.checkpoint.envelope_sha256)
            || !digest_valid(&self.boundary.journal_sha256)
            || self
                .boundary
                .capture_sha256
                .as_ref()
                .is_some_and(|d| !digest_valid(d))
            || self.captured_at < self.boundary.checkpoint.cursor.observed_at
            || self
                .limit_state
                .day
                .is_some_and(|day| day > self.captured_at.date_naive())
            || self.limit_state.validate().is_err()
            || self
                .circuit_state
                .as_ref()
                .is_some_and(|s| s.validate().is_err())
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

    /// Compare observed states without resetting or instantiating a
    /// checker. This is exact equality, not a complete risk proof.
    pub(crate) fn compare(
        &self,
        snapshot: &CoherentRecoverySnapshot,
        expected_owner: &str,
        expected: &CoherentCheckpointReceipt,
        inspected_at: DateTime<Utc>,
        observed: (&LimitStateSnapshot, Option<&CircuitStateSnapshot>),
    ) -> Result<(), RiskEvidenceError> {
        self.encode().map_err(|_| RiskEvidenceError::Encoding)?;
        if &self.boundary != expected || self.owner_fingerprint != expected_owner {
            return Err(RiskEvidenceError::Identity);
        }
        let latest = snapshot
            .orders
            .iter()
            .fold(expected.checkpoint.cursor.observed_at, |at, o| {
                at.max(o.created_at).max(o.updated_at)
            });
        let latest = snapshot
            .fills
            .iter()
            .fold(latest, |at, f| at.max(f.filled_at));
        if self.captured_at < latest || self.captured_at > inspected_at {
            return Err(RiskEvidenceError::Interval);
        }
        self.limit_state.compare(observed.0)?;
        match (&self.circuit_state, observed.1) {
            (Some(a), Some(b)) => a.compare(b)?,
            (None, None) => {}
            _ => return Err(RiskEvidenceError::Monitor),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_risk::RiskLimits;
    use apexes_strategy::checkpoint::{CheckpointReceipt, StateCursor, StateIdentity};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn fixture() -> (
        CoherentCheckpointReceipt,
        LimitStateSnapshot,
        CircuitStateSnapshot,
        DateTime<Utc>,
    ) {
        let at = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let boundary = CoherentCheckpointReceipt {
            version: 1,
            checkpoint: CheckpointReceipt {
                identity: StateIdentity {
                    strategy: "probe".into(),
                    revision: "1".into(),
                    config_sha256: "a".repeat(64),
                    scope: "spot".into(),
                    payload_version: 1,
                },
                cursor: StateCursor {
                    ordinal: 2,
                    observed_at: at,
                },
                envelope_sha256: "b".repeat(64),
            },
            run_scope: Uuid::new_v4(),
            last_intent: 1,
            fill_sequence: 0,
            terminal_sequence: 0,
            order_count: 0,
            fill_count: 0,
            journal_sha256: "c".repeat(64),
            capture_sha256: None,
        };
        let limits = RiskLimits {
            max_daily_trade_count: Some(1),
            max_position_per_symbol: Some(Decimal::ONE),
            ..Default::default()
        };
        let limit = LimitStateSnapshot {
            version: 1,
            limits: limits.clone(),
            day: Some(at.date_naive()),
            trade_count: 1,
            traded_volume: Decimal::ONE,
        };
        let circuit = CircuitStateSnapshot {
            version: 1,
            limits,
            tripped_symbols: vec!["BTCUSDT".into()],
            account_tripped: false,
        };
        (boundary, limit, circuit, at)
    }

    #[test]
    fn risk_file_round_trip_refuses_overwrite_and_noncanonical_bytes() {
        let (boundary, limit, circuit, at) = fixture();
        let retained =
            RiskBoundaryEvidence::new("d".repeat(64), boundary, at, limit, Some(circuit)).unwrap();
        let dir = std::env::temp_dir().join(format!("apexes-risk-evidence-{}", Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("risk.json");
        retained.write_new(&path).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(RiskBoundaryEvidence::read(&path).unwrap(), retained);
        assert!(retained.write_new(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        for bad in [
            bytes[..bytes.len() - 1].to_vec(),
            [bytes, b" ".to_vec()].concat(),
            vec![b'x'; MAX_RECEIPT_FILE_BYTES + 1],
        ] {
            std::fs::write(&path, bad).unwrap();
            assert!(RiskBoundaryEvidence::read(&path).is_err());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn exact_boundary_and_risk_state_equality_required() {
        let (boundary, limit, circuit, at) = fixture();
        let owner = "d".repeat(64);
        let retained = RiskBoundaryEvidence::new(
            owner.clone(),
            boundary.clone(),
            at,
            limit.clone(),
            Some(circuit.clone()),
        )
        .unwrap();
        let snapshot = CoherentRecoverySnapshot {
            envelope: vec![],
            orders: vec![],
            fills: vec![],
            capture: None,
        };
        retained
            .compare(&snapshot, &owner, &boundary, at, (&limit, Some(&circuit)))
            .unwrap();
        assert!(matches!(
            retained.compare(&snapshot, "wrong", &boundary, at, (&limit, Some(&circuit))),
            Err(RiskEvidenceError::Identity)
        ));
        let mut other = boundary.clone();
        other.last_intent += 1;
        assert!(matches!(
            retained.compare(&snapshot, &owner, &other, at, (&limit, Some(&circuit))),
            Err(RiskEvidenceError::Identity)
        ));
        assert!(matches!(
            retained.compare(&snapshot, &owner, &boundary, at, (&limit, None)),
            Err(RiskEvidenceError::Monitor)
        ));
        let mut reset = limit.clone();
        reset.day = None;
        reset.trade_count = 0;
        reset.traded_volume = Decimal::ZERO;
        assert!(matches!(
            retained.compare(&snapshot, &owner, &boundary, at, (&reset, Some(&circuit))),
            Err(RiskEvidenceError::State(RiskStateError::Mismatch))
        ));
        let mut cleared = circuit.clone();
        cleared.tripped_symbols.clear();
        assert!(matches!(
            retained.compare(&snapshot, &owner, &boundary, at, (&limit, Some(&cleared))),
            Err(RiskEvidenceError::State(RiskStateError::Mismatch))
        ));
    }

    #[test]
    fn invalid_versions_future_days_and_capture_intervals_refused() {
        let (boundary, limit, circuit, at) = fixture();
        let owner = "d".repeat(64);
        assert!(
            RiskBoundaryEvidence::new(
                owner.clone(),
                boundary.clone(),
                at - chrono::TimeDelta::seconds(1),
                limit.clone(),
                Some(circuit.clone())
            )
            .is_err()
        );
        let mut future = limit.clone();
        future.day = Some(at.date_naive().succ_opt().unwrap());
        assert!(
            RiskBoundaryEvidence::new(
                owner.clone(),
                boundary.clone(),
                at,
                future,
                Some(circuit.clone())
            )
            .is_err()
        );
        let mut retained = RiskBoundaryEvidence::new(
            owner.clone(),
            boundary.clone(),
            at,
            limit.clone(),
            Some(circuit.clone()),
        )
        .unwrap();
        let snapshot = CoherentRecoverySnapshot {
            envelope: vec![],
            orders: vec![],
            fills: vec![],
            capture: None,
        };
        assert!(matches!(
            retained.compare(
                &snapshot,
                &owner,
                &boundary,
                at - chrono::TimeDelta::seconds(1),
                (&limit, Some(&circuit))
            ),
            Err(RiskEvidenceError::Interval)
        ));
        retained.version = 2;
        assert!(matches!(
            retained.compare(&snapshot, &owner, &boundary, at, (&limit, Some(&circuit))),
            Err(RiskEvidenceError::Encoding)
        ));
    }
}
