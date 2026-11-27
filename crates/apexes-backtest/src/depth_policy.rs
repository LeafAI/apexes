//! F03.02: explicit replay depth-integrity assumptions.
use apexes_core::{ApexesError, registry::Registry};
use chrono::TimeDelta;
use serde::{Deserialize, Serialize};

/// Sequence semantics are a property of the recorded source, not
/// inferred from the venue. Binance final update IDs may span ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DepthSequence {
    SnapshotOnly,
    Contiguous,
    /// Historical runner only, after verifying the entire sealed range evidence.
    VerifiedRanges,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulatedDepthPolicy {
    pub max_age_ms: u32,
    pub sequence: DepthSequence,
}

impl SimulatedDepthPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_age_ms == 0 {
            return Err("depth maximum age must be positive".into());
        }
        Ok(())
    }
    pub fn from_registry(registry: &Registry) -> Result<Option<Self>, ApexesError> {
        let policy =
            registry
                .try_one::<Self>()
                .map_err(|e| ApexesError::InvalidBacktestConfig {
                    reason: e.to_string(),
                })?;
        if let Some(p) = policy {
            if p.sequence == DepthSequence::VerifiedRanges {
                return Err(ApexesError::InvalidBacktestConfig {
                    reason: "verified_ranges is selected by historical replay after range verification, not by registry policy".into(),
                });
            }
            p.validate()
                .map_err(|reason| ApexesError::InvalidBacktestConfig { reason })?;
        }
        Ok(policy)
    }
    pub(crate) fn max_age(&self) -> TimeDelta {
        TimeDelta::milliseconds(i64::from(self.max_age_ms))
    }
}
