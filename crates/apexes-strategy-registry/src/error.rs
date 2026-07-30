//! Error type for `apexes-strategy-registry`.

use thiserror::Error;

/// Errors returned by `VersionStore`/`CanaryComparator`.
#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("failed to connect to ClickHouse: {reason}")]
    ConnectionFailed { reason: String },

    #[error("ClickHouse query failed: {reason}")]
    QueryFailed { reason: String },

    #[error("strategy '{strategy_name}' has no version {version}")]
    VersionNotFound { strategy_name: String, version: u32 },

    #[error("failed to serialize/deserialize params snapshot: {reason}")]
    ParamsSerializationFailed { reason: String },

    #[error("failed to convert Decimal to ClickHouse FixedPoint: {reason}")]
    DecimalConversionFailed { reason: String },

    #[error("no version among {versions:?} for strategy '{strategy_name}' evaluated successfully")]
    NoSuccessfulVersion {
        strategy_name: String,
        versions: Vec<u32>,
    },
}

impl From<clickhouse::error::Error> for RegistryError {
    fn from(error: clickhouse::error::Error) -> Self {
        RegistryError::QueryFailed {
            reason: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_not_found_display() {
        let err = RegistryError::VersionNotFound {
            strategy_name: "threshold_strategy".to_string(),
            version: 5,
        };
        assert_eq!(
            err.to_string(),
            "strategy 'threshold_strategy' has no version 5"
        );
    }

    #[test]
    fn no_successful_version_display() {
        let err = RegistryError::NoSuccessfulVersion {
            strategy_name: "threshold_strategy".to_string(),
            versions: vec![1, 2, 3],
        };
        assert_eq!(
            err.to_string(),
            "no version among [1, 2, 3] for strategy 'threshold_strategy' evaluated successfully"
        );
    }
}
