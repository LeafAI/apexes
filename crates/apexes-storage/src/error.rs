use thiserror::Error;

/// Errors returned by `apexes-storage`'s `StorageWriter`/`StorageReader`.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("failed to connect to ClickHouse: {reason}")]
    ConnectionFailed { reason: String },

    #[error(
        "write_batch requires a homogeneous batch of the same MarketEvent variant, but found mixed types"
    )]
    MixedBatch,

    #[error("write_batch was called with an empty batch")]
    EmptyBatch,

    #[error("failed to convert Decimal to ClickHouse FixedPoint: {reason}")]
    DecimalConversionFailed { reason: String },

    #[error("invalid depth range: {reason}")]
    InvalidDepthRange { reason: String },

    #[error("ClickHouse query failed: {reason}")]
    QueryFailed { reason: String },

    #[error(
        "insufficient history for intraday volume curve: requested {requested_days} days, only {available_days} available"
    )]
    InsufficientHistory {
        available_days: u32,
        requested_days: u32,
    },
}

impl From<clickhouse::error::Error> for StorageError {
    fn from(error: clickhouse::error::Error) -> Self {
        StorageError::QueryFailed {
            reason: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_batch_display() {
        let err = StorageError::MixedBatch;
        assert_eq!(
            err.to_string(),
            "write_batch requires a homogeneous batch of the same MarketEvent variant, but found mixed types"
        );
    }

    #[test]
    fn empty_batch_display() {
        let err = StorageError::EmptyBatch;
        assert_eq!(
            err.to_string(),
            "write_batch was called with an empty batch"
        );
    }

    #[test]
    fn decimal_conversion_failed_display() {
        let err = StorageError::DecimalConversionFailed {
            reason: "overflow".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "failed to convert Decimal to ClickHouse FixedPoint: overflow"
        );
    }

    #[test]
    fn insufficient_history_display() {
        let err = StorageError::InsufficientHistory {
            available_days: 3,
            requested_days: 10,
        };
        assert_eq!(
            err.to_string(),
            "insufficient history for intraday volume curve: requested 10 days, only 3 available"
        );
    }
}
