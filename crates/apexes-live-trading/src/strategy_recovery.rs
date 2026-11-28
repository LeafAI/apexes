//! F04.05: independently supplied recovery receipts and one-time verified
//! live strategy startup. Not an atomic strategy/OMS recovery protocol.
use apexes_core::MarketEvent;
use apexes_oms::{CheckpointStoreError, OrderManager};
use apexes_strategy::Strategy;
use apexes_strategy::checkpoint::{CheckpointError, CheckpointReceipt, restore};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

pub(super) const MAX_RECEIPT_FILE_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum StrategyRecoveryError {
    #[error("invalid recovery receipt file")]
    InvalidReceipt,
    #[error("recovery receipt file I/O failed")]
    ReceiptIo,
    #[error(
        "strategy restoration requires an empty initial position ledger and the same managed kill switch"
    )]
    StartupConfig,
    #[error("strategy checkpoint startup refused: {0}")]
    Store(#[from] CheckpointStoreError),
    #[error("strategy checkpoint restoration refused: {0}")]
    Restore(#[from] CheckpointError),
}

type Result<T> = std::result::Result<T, StrategyRecoveryError>;

/// Operator-controlled receipt, stored outside the checkpoint database.
/// The caller must obtain the receipt from an acknowledged publication,
/// independently establish the owner and original load-time overrides,
/// and protect this file against unauthorized modification/rollback.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverySelection {
    version: u32,
    inventory_owner_sha256: String,
    receipt: CheckpointReceipt,
    initial_params: Vec<(String, Decimal)>,
}

pub(super) fn digest_valid(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl RecoverySelection {
    pub fn new(
        inventory_owner_sha256: String,
        receipt: CheckpointReceipt,
        initial_params: HashMap<String, Decimal>,
    ) -> Result<Self> {
        let mut initial_params: Vec<_> = initial_params.into_iter().collect();
        initial_params.sort_by(|a, b| a.0.cmp(&b.0));
        let selection = Self {
            version: 1,
            inventory_owner_sha256,
            receipt,
            initial_params,
        };
        selection.encode()?;
        Ok(selection)
    }

    pub(super) fn validate(&self) -> Result<()> {
        if self.version != 1
            || !digest_valid(&self.inventory_owner_sha256)
            || !digest_valid(&self.receipt.envelope_sha256)
            || !digest_valid(&self.receipt.identity.config_sha256)
            || self.receipt.identity.payload_version == 0
            || self.receipt.cursor.ordinal == 0
            || !self.initial_params.windows(2).all(|w| w[0].0 < w[1].0)
            || self
                .initial_params
                .iter()
                .any(|(k, _)| k.is_empty() || k.len() > 256)
        {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        if bytes.len() > MAX_RECEIPT_FILE_BYTES {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(bytes)
    }

    /// Write a new independent receipt file, never overwriting a prior one.
    /// A crash/error may leave an incomplete file; loading it fails closed.
    /// The caller chooses a trusted existing directory and separately
    /// selects this exact file for recovery; no latest-file discovery.
    pub fn write_new(&self, path: &Path) -> Result<()> {
        let bytes = self.encode()?;
        write_receipt_file(path, &bytes)
    }

    /// Read only the explicitly selected, bounded regular file.
    pub fn read(path: &Path) -> Result<Self> {
        let bytes = read_receipt_file(path)?;
        let selection: Self =
            serde_json::from_slice(&bytes).map_err(|_| StrategyRecoveryError::InvalidReceipt)?;
        // Refuse altered or ambiguous encodings (including duplicate fields).
        if selection.encode()? != bytes {
            return Err(StrategyRecoveryError::InvalidReceipt);
        }
        Ok(selection)
    }

    pub fn initial_params(&self) -> HashMap<String, Decimal> {
        self.initial_params.iter().cloned().collect()
    }

    pub(super) fn owner_fingerprint(&self) -> &str {
        &self.inventory_owner_sha256
    }

    pub fn receipt(&self) -> &CheckpointReceipt {
        &self.receipt
    }
}

pub(super) fn write_receipt_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|_| StrategyRecoveryError::ReceiptIo)?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| StrategyRecoveryError::ReceiptIo)?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| StrategyRecoveryError::ReceiptIo)
}

pub(super) fn read_receipt_file(path: &Path) -> Result<Vec<u8>> {
    read_bounded_receipt_file(path, MAX_RECEIPT_FILE_BYTES)
}

pub(super) fn read_bounded_receipt_file(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let meta = std::fs::metadata(path).map_err(|_| StrategyRecoveryError::ReceiptIo)?;
    if !meta.is_file() || meta.len() > limit as u64 {
        return Err(StrategyRecoveryError::InvalidReceipt);
    }
    let file = File::open(path).map_err(|_| StrategyRecoveryError::ReceiptIo)?;
    if !file
        .metadata()
        .map_err(|_| StrategyRecoveryError::ReceiptIo)?
        .is_file()
    {
        return Err(StrategyRecoveryError::InvalidReceipt);
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| StrategyRecoveryError::ReceiptIo)?;
    if bytes.len() > limit {
        return Err(StrategyRecoveryError::InvalidReceipt);
    }
    Ok(bytes)
}

/// A caller constructs the candidate independently using the original
/// qualified scoped strategy configuration, before starting the runner.
/// No mutable state or external side effects may be shared with the active
/// strategy.
pub enum StrategyStartup<S> {
    Legacy(S),
    Restore {
        active: S,
        candidate: S,
        selection: Box<RecoverySelection>,
    },
}

impl<S: Strategy> StrategyStartup<S> {
    pub(crate) async fn prepare(
        self,
        manager: &mut OrderManager,
        config: &crate::LiveTradingConfig,
    ) -> Result<(S, Option<DateTime<Utc>>)> {
        let result = self.prepare_inner(manager, config).await;
        if result.is_err() {
            manager.report_health_fault("live strategy checkpoint restoration refused");
            if let Some(gate) = &config.kill_switch {
                gate.fault("live strategy checkpoint restoration refused");
            }
        }
        result
    }

    async fn prepare_inner(
        self,
        manager: &mut OrderManager,
        config: &crate::LiveTradingConfig,
    ) -> Result<(S, Option<DateTime<Utc>>)> {
        match self {
            Self::Legacy(strategy) => {
                manager.check_legacy_strategy_startup().await?;
                Ok((strategy, None))
            }
            Self::Restore {
                mut active,
                candidate,
                selection,
            } => {
                let same_gate = manager
                    .submission_gate()
                    .zip(config.kill_switch.as_ref())
                    .is_some_and(|(a, b)| std::sync::Arc::ptr_eq(&a, b));
                if !same_gate || !config.initial_positions.is_empty() {
                    return Err(StrategyRecoveryError::StartupConfig);
                }
                selection.validate()?;
                let receipt = selection.receipt();
                let bytes = manager
                    .claim_strategy_startup(
                        receipt,
                        &selection.inventory_owner_sha256,
                        &config.ems_postgres_url,
                    )
                    .await?;
                restore(
                    &mut active,
                    candidate,
                    &bytes,
                    &receipt.identity,
                    receipt.cursor,
                )?;
                Ok((active, Some(receipt.cursor.observed_at)))
            }
        }
    }
}

/// Exclude source events at or before the restored observation boundary;
/// a closed bar is available at its close, not at its open. This is only
/// a conservative lower-bound filter, not proof of gap-free warm-up.
pub(crate) fn event_after_checkpoint(event: &MarketEvent, through: DateTime<Utc>) -> bool {
    let at = match event {
        MarketEvent::Tick(e) => e.timestamp,
        MarketEvent::Depth(e) => e.timestamp,
        MarketEvent::Trade(e) => e.timestamp,
        MarketEvent::MarkPrice(e) => e.timestamp,
        MarketEvent::Bar(e) => e.close_time,
    };
    at > through
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{Bar, BarInterval, Venue};
    use apexes_strategy::checkpoint::{StateCursor, StateIdentity};

    fn selection() -> RecoverySelection {
        RecoverySelection::new(
            "a".repeat(64),
            CheckpointReceipt {
                identity: StateIdentity {
                    strategy: "counter".into(),
                    revision: "1".into(),
                    config_sha256: "b".repeat(64),
                    scope: "live:owner:BTCUSDT".into(),
                    payload_version: 1,
                },
                cursor: StateCursor {
                    ordinal: 1,
                    observed_at: DateTime::from_timestamp(1_800_000_000, 123).unwrap(),
                },
                envelope_sha256: "c".repeat(64),
            },
            HashMap::from([("size".into(), Decimal::new(123, 2))]),
        )
        .unwrap()
    }

    #[test]
    fn independent_receipt_is_durable_bounded_canonical_and_never_overwritten() {
        let directory =
            std::env::temp_dir().join(format!("apexes-recovery-receipt-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("selected.json");
        let selection = selection();
        selection.write_new(&path).unwrap();
        let before = std::fs::read(&path).unwrap();
        assert_eq!(
            RecoverySelection::read(&path).unwrap().receipt(),
            selection.receipt()
        );
        assert_eq!(
            RecoverySelection::read(&path).unwrap().initial_params(),
            selection.initial_params()
        );
        assert!(selection.write_new(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        for invalid in [
            before[..before.len() - 1].to_vec(),
            [before, b" ".to_vec()].concat(),
            vec![b'x'; MAX_RECEIPT_FILE_BYTES + 1],
        ] {
            std::fs::write(&path, invalid).unwrap();
            assert!(RecoverySelection::read(&path).is_err());
        }
        assert!(RecoverySelection::read(&directory).is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn restored_boundary_is_strict_and_uses_bar_close() {
        let through = selection().receipt().cursor.observed_at;
        let mut bar = Bar {
            venue: Venue::Binance,
            symbol: "BTCUSDT".into(),
            interval: BarInterval::OneSecond,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: Decimal::ONE,
            open_time: through - chrono::TimeDelta::seconds(1),
            close_time: through,
        };
        assert!(!event_after_checkpoint(
            &MarketEvent::Bar(bar.clone()),
            through
        ));
        bar.close_time += chrono::TimeDelta::nanoseconds(1);
        assert!(event_after_checkpoint(&MarketEvent::Bar(bar), through));
    }
}
