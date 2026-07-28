use thiserror::Error;

/// Unified error type for apexes-core. Cloneable — variants that carry
/// underlying-error context store it as a formatted `String`, not
/// `Box<dyn Error>`, so `ApexesError` can be cloned freely.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ApexesError {
    #[error("invalid instrument: {reason}")]
    InvalidInstrument { reason: String },

    #[error("market data connection failed: {reason}")]
    ConnectionFailed {
        reason: String,
        source_detail: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_instrument_display() {
        let err = ApexesError::InvalidInstrument {
            reason: "venue does not belong to market_type".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "invalid instrument: venue does not belong to market_type"
        );
    }

    #[test]
    fn connection_failed_is_cloneable() {
        let err = ApexesError::ConnectionFailed {
            reason: "timeout".to_string(),
            source_detail: Some("io error: connection reset".to_string()),
        };
        let cloned = err.clone();
        assert_eq!(err, cloned);
    }
}
