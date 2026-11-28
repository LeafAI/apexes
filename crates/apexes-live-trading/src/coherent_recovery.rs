//! F04.05: independent receipt selection and read-only recovery inspection.
//! Does not authorize trading or consume/reset the durable startup claim.
use crate::spot_recovery::{SpotRecoveryError, SpotWalletBalance, recover_spot_positions};
use crate::strategy_recovery::{
    MAX_RECEIPT_FILE_BYTES, RecoverySelection, StrategyRecoveryError, digest_valid,
    read_receipt_file, write_receipt_file,
};
use apexes_core::{Fill, Instrument};
use apexes_oms::{CoherentCheckpointReceipt, OrderStore};
use apexes_strategy::checkpoint::{StateCursor, restore};
use apexes_strategy::{Position, Strategy};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoherentRecoverySelection {
    version: u32,
    predecessor: RecoverySelection,
    boundary: CoherentCheckpointReceipt,
}

impl CoherentRecoverySelection {
    /// Derive only from the original independent selection and the
    /// acknowledged coherent publication, never by discovering a DB head.
    pub fn new(
        predecessor: RecoverySelection,
        boundary: CoherentCheckpointReceipt,
    ) -> Result<Self, StrategyRecoveryError> {
        let selection = Self {
            version: 1,
            predecessor,
            boundary,
        };
        selection.encode()?;
        Ok(selection)
    }

    fn encode(&self) -> Result<Vec<u8>, StrategyRecoveryError> {
        self.predecessor.validate()?;
        let previous = self.predecessor.receipt();
        let b = &self.boundary;
        if self.version != 1
            || b.version != 1
            || b.run_scope.is_nil()
            || b.checkpoint.identity != previous.identity
            || b.checkpoint.cursor.ordinal <= previous.cursor.ordinal
            || b.checkpoint.cursor.observed_at < previous.cursor.observed_at
            || !digest_valid(&b.checkpoint.envelope_sha256)
            || !digest_valid(&b.journal_sha256)
            || !(0..=2048).contains(&b.order_count)
            || !(0..=8192).contains(&b.fill_count)
            || b.fill_sequence < 0
            || b.terminal_sequence < 0
            || (b.fill_count == 0) != (b.fill_sequence == 0)
            || (b.order_count == 0 && (b.terminal_sequence != 0 || b.fill_count != 0))
            || u64::try_from(b.order_count).map_or(true, |count| count > b.last_intent)
        {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        let bytes = serde_json::to_vec(self).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        if bytes.len() > MAX_RECEIPT_FILE_BYTES {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(bytes)
    }

    /// Write to a new independently selected file and sync it and its
    /// directory. The caller is responsible for a trusted directory.
    pub fn write_new(&self, path: &Path) -> Result<(), StrategyRecoveryError> {
        write_receipt_file(path, &self.encode()?)
    }

    pub fn read(path: &Path) -> Result<Self, StrategyRecoveryError> {
        let bytes = read_receipt_file(path)?;
        let selected: Self =
            serde_json::from_slice(&bytes).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        if selected.encode()? != bytes {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(selected)
    }

    pub fn receipt(&self) -> &CoherentCheckpointReceipt {
        &self.boundary
    }

    pub fn initial_params(&self) -> HashMap<String, Decimal> {
        self.predecessor.initial_params()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CoherentInspectionError {
    #[error("coherent receipt selection refused: {0}")]
    Selection(#[from] StrategyRecoveryError),
    #[error("coherent journal inspection refused: {0}")]
    Store(#[from] apexes_oms::CheckpointStoreError),
    #[error("strategy state inspection refused: {0}")]
    Restore(#[from] apexes_strategy::checkpoint::CheckpointError),
    #[error("stored order does not match the selected Spot instrument scope")]
    InstrumentScope,
    #[error("invalid persisted fill row")]
    Fill,
    #[error("supplied recovery evidence window refused: {0}")]
    EvidenceWindow(#[from] crate::EvidenceWindowError),
    #[error("terminal execution evidence comparison refused: {0}")]
    Terminal(#[from] apexes_oms::TerminalAuditError),
    #[error("Spot inventory preview refused: {0}")]
    Inventory(#[from] SpotRecoveryError),
}

/// An offline preview, not a startup token. The restored strategy is
/// discarded, no historical callbacks are replayed, and neither OMS
/// delivery watermarks nor the startup claim are changed.
pub struct CoherentRecoveryPreview {
    pub cursor: StateCursor,
    pub owned_positions: HashMap<String, Position>,
    pub wallet_balances: HashMap<String, SpotWalletBalance>,
    pub historical_fill_count: usize,
    pub historical_order_count: usize,
}

impl CoherentRecoverySelection {
    /// Compare caller-supplied, independently collected terminal evidence
    /// against this exact selected journal. This is an offline inspection,
    /// not proof of freshness or an authoritative exchange query. No
    /// callback receipts, startup claims, or order state are changed.
    pub async fn inspect_terminal_evidence(
        &self,
        store: &mut OrderStore,
        ems_postgres_url: &str,
        evidence: &[apexes_oms::TerminalOrderEvidence],
    ) -> Result<(), CoherentInspectionError> {
        self.encode()?;
        let snapshot = store
            .inspect_coherent_recovery(
                self.predecessor.receipt(),
                &self.boundary,
                self.predecessor.owner_fingerprint(),
                ems_postgres_url,
            )
            .await?;
        apexes_oms::compare_terminal_evidence(&snapshot, evidence)?;
        store
            .inspect_coherent_recovery(
                self.predecessor.receipt(),
                &self.boundary,
                self.predecessor.owner_fingerprint(),
                ems_postgres_url,
            )
            .await?;
        Ok(())
    }

    /// Offline comparison of complete supplied terminal/account evidence
    /// within a single bounded observation window. The independently
    /// supplied inspection time is not derived from the evidence itself.
    /// This does not fetch exchange evidence or authorize startup.
    pub async fn inspect_evidence_window(
        &self,
        store: &mut OrderStore,
        ems_postgres_url: &str,
        evidence: &crate::RecoveryEvidenceWindow,
        inspected_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), CoherentInspectionError> {
        self.encode()?;
        let snapshot = store
            .inspect_coherent_recovery(
                self.predecessor.receipt(),
                &self.boundary,
                self.predecessor.owner_fingerprint(),
                ems_postgres_url,
            )
            .await?;
        crate::recovery_evidence_window::validate_evidence_window(
            evidence,
            &snapshot,
            self.boundary.checkpoint.cursor.observed_at,
            self.predecessor.owner_fingerprint(),
            inspected_at,
        )?;
        apexes_oms::compare_terminal_evidence(&snapshot, &evidence.terminal_orders)?;
        store
            .inspect_coherent_recovery(
                self.predecessor.receipt(),
                &self.boundary,
                self.predecessor.owner_fingerprint(),
                ems_postgres_url,
            )
            .await?;
        Ok(())
    }

    /// Reconstruct owned inventory from the exact locked journal using
    /// caller-supplied wallet evidence and verify isolated strategy
    /// restoration. The supplied wallet is not fetched or authenticated
    /// here; authoritative exchange verification is a separate stage.
    pub async fn inspect<S: Strategy>(
        &self,
        store: &mut OrderStore,
        ems_postgres_url: &str,
        mut active: S,
        candidate: S,
        instruments: &[Instrument],
        wallet: &HashMap<String, SpotWalletBalance>,
    ) -> Result<CoherentRecoveryPreview, CoherentInspectionError> {
        self.encode()?;
        let snapshot = store
            .inspect_coherent_recovery(
                self.predecessor.receipt(),
                &self.boundary,
                self.predecessor.owner_fingerprint(),
                ems_postgres_url,
            )
            .await?;
        let scope: HashSet<_> = instruments
            .iter()
            .map(|i| (i.venue, i.symbol.as_str(), i.asset_class))
            .collect();
        if scope.len() != instruments.len()
            || scope.is_empty()
            || instruments.iter().any(|i| {
                i.venue != apexes_core::Venue::Binance
                    || i.asset_class != apexes_core::AssetClass::Spot
            })
            || snapshot
                .orders
                .iter()
                .any(|o| !scope.contains(&(o.venue, o.symbol.as_str(), o.asset_class)))
        {
            return Err(CoherentInspectionError::InstrumentScope);
        }
        let count = snapshot.orders.len();
        let fills = snapshot
            .fills
            .into_iter()
            .map(Fill::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| CoherentInspectionError::Fill)?;
        let owned_positions = recover_spot_positions(instruments, &fills, wallet, false)?;
        let receipt = &self.boundary.checkpoint;
        restore(
            &mut active,
            candidate,
            &snapshot.envelope,
            &receipt.identity,
            receipt.cursor,
        )?;
        // Reverify the independently selected evidence after semantic
        // inspection. The result is point-in-time, not an execution lease.
        store
            .inspect_coherent_recovery(
                self.predecessor.receipt(),
                &self.boundary,
                self.predecessor.owner_fingerprint(),
                ems_postgres_url,
            )
            .await?;
        Ok(CoherentRecoveryPreview {
            cursor: receipt.cursor,
            owned_positions,
            wallet_balances: wallet.clone(),
            historical_fill_count: fills.len(),
            historical_order_count: count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_strategy::checkpoint::{CheckpointReceipt, StateIdentity};
    use chrono::DateTime;
    use uuid::Uuid;

    fn pair() -> (RecoverySelection, CoherentCheckpointReceipt) {
        let previous = CheckpointReceipt {
            identity: StateIdentity {
                strategy: "counter".into(),
                revision: "1".into(),
                config_sha256: "a".repeat(64),
                scope: "live:owner:BTCUSDT".into(),
                payload_version: 1,
            },
            cursor: StateCursor {
                ordinal: 100,
                observed_at: DateTime::from_timestamp(1_800_000_000, 123).unwrap(),
            },
            envelope_sha256: "b".repeat(64),
        };
        let mut next = previous.clone();
        next.cursor.ordinal += 1;
        next.envelope_sha256 = "c".repeat(64);
        let selection = RecoverySelection::new(
            "d".repeat(64),
            previous,
            HashMap::from([("period".into(), Decimal::from(7))]),
        )
        .unwrap();
        let boundary = CoherentCheckpointReceipt {
            version: 1,
            checkpoint: next,
            run_scope: Uuid::new_v4(),
            last_intent: 1,
            fill_sequence: 2,
            terminal_sequence: 3,
            order_count: 1,
            fill_count: 1,
            journal_sha256: "e".repeat(64),
        };
        (selection, boundary)
    }

    #[test]
    fn coherent_receipt_file_preserves_exact_selection_and_never_overwrites() {
        let (previous, boundary) = pair();
        let selection = CoherentRecoverySelection::new(previous, boundary.clone()).unwrap();
        let dir = std::env::temp_dir().join(format!("apexes-coherent-receipt-{}", Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("selected.json");
        selection.write_new(&path).unwrap();
        let loaded = CoherentRecoverySelection::read(&path).unwrap();
        assert_eq!(loaded.receipt(), &boundary);
        assert_eq!(loaded.initial_params()["period"], Decimal::from(7));
        let bytes = std::fs::read(&path).unwrap();
        assert!(selection.write_new(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert!(RecoverySelection::read(&path).is_err());
        for bad in [
            bytes[..bytes.len() - 1].to_vec(),
            [bytes, b" ".to_vec()].concat(),
            vec![b'x'; MAX_RECEIPT_FILE_BYTES + 1],
        ] {
            std::fs::write(&path, bad).unwrap();
            assert!(CoherentRecoverySelection::read(&path).is_err());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn incoherent_selection_rejects_invalid_identity_cursor_and_prefix() {
        let (previous, boundary) = pair();
        for invalid in [
            CoherentCheckpointReceipt {
                version: 2,
                ..boundary.clone()
            },
            CoherentCheckpointReceipt {
                run_scope: Uuid::nil(),
                ..boundary.clone()
            },
            CoherentCheckpointReceipt {
                last_intent: 0,
                ..boundary.clone()
            },
            CoherentCheckpointReceipt {
                order_count: 2049,
                ..boundary.clone()
            },
            CoherentCheckpointReceipt {
                fill_count: 8193,
                ..boundary.clone()
            },
            CoherentCheckpointReceipt {
                fill_sequence: 0,
                ..boundary.clone()
            },
            CoherentCheckpointReceipt {
                journal_sha256: "broken".into(),
                ..boundary.clone()
            },
        ] {
            assert!(CoherentRecoverySelection::new(previous.clone(), invalid).is_err());
        }
        let mut invalid = boundary.clone();
        invalid.checkpoint.cursor = previous.receipt().cursor;
        assert!(CoherentRecoverySelection::new(previous.clone(), invalid).is_err());
        let mut invalid = boundary;
        invalid.checkpoint.identity.revision = "other".into();
        assert!(CoherentRecoverySelection::new(previous, invalid).is_err());
    }
}
