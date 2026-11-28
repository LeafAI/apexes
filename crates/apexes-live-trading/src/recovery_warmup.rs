//! F04.05 — offline exact scoped Drift warm-up qualification.
//! Never yields a runnable strategy, a replacement checkpoint or a startup claim.
use crate::{
    ObservationEvidenceError, OpeningObservationEvidence,
    strategy_recovery::{RecoverySelection, StrategyRecoveryError},
};
use apexes_backtest::observation_receipt::CompletedObservation;
use apexes_core::Instrument;
use apexes_strategy::{
    StrategyError,
    checkpoint::{self, CheckpointError},
    drift_dsl::{DriftScriptStrategy, WarmupError},
};
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum WarmupInspectionError {
    #[error("warm-up selection refused: {0}")]
    Selection(#[from] StrategyRecoveryError),
    #[error("warm-up observation receipt refused: {0}")]
    Evidence(#[from] ObservationEvidenceError),
    #[error("isolated scoped strategy construction failed: {0}")]
    Strategy(#[from] StrategyError),
    #[error("warm-up observation ordinal {ordinal} refused: {error}")]
    Callback { ordinal: u64, error: WarmupError },
    #[error("warm-up checkpoint encoding refused: {0}")]
    Checkpoint(#[from] CheckpointError),
    #[error("warm-up checkpoint differs from the independently selected receipt")]
    Mismatch,
}

/// Compare a freshly constructed trusted scoped Drift script against the
/// exact independently selected checkpoint. The caller qualifies script
/// scope, deterministic initialization and required history length; matching
/// bytes do not prove source completeness or arbitrary interpreter globals.
/// Call with an entered Tokio runtime, outside an async runtime block_on
/// (same requirement as the existing synchronous Drift strategy callbacks).
/// All records are verified before script construction. No executor, live
/// account state or database handle is accepted; candidate is always dropped.
pub fn inspect_no_order_warmup(
    selection: &RecoverySelection,
    script: &Path,
    instruments: &[Instrument],
    opening: &OpeningObservationEvidence,
    records: &[CompletedObservation],
) -> Result<usize, WarmupInspectionError> {
    selection.validate()?;
    let target = selection.receipt();
    let scope = instruments
        .iter()
        .map(|i| (i.venue, i.symbol.clone()))
        .collect::<Vec<_>>();
    opening.verify_history(selection.owner_fingerprint(), target, &scope, records)?;
    let mut candidate = DriftScriptStrategy::from_file_with_scoped_checkpoints(
        script,
        selection.initial_params(),
        instruments,
    )?;
    for record in records {
        candidate
            .warmup_observation(&record.event, record.cursor.observed_at)
            .map_err(|error| WarmupInspectionError::Callback {
                ordinal: record.cursor.ordinal,
                error,
            })?;
    }
    let bytes = checkpoint::capture(&candidate, target.identity.clone(), target.cursor)?;
    let actual = checkpoint::verify_checkpoint(&bytes, &target.identity, target.cursor)?;
    if &actual != target {
        return Err(WarmupInspectionError::Mismatch);
    }
    Ok(records.len())
}
