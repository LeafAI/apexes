use std::error::Error;
use std::fmt;

/// Unified error type for apexes-core. Cloneable — variants that carry
/// underlying-error context store it as a formatted `String`, not
/// `Box<dyn Error>`, so `ApexesError` can be cloned freely.
///
/// NOTE: `Display` and `Error` are implemented manually rather than via
/// `thiserror::Error` because thiserror treats any field named `source`
/// as the error source (requiring `T: Error`), which conflicts with the
/// `source: Option<String>` context field on `ConnectionFailed`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApexesError {
    InvalidInstrument {
        reason: String,
    },

    ConnectionFailed {
        reason: String,
        source: Option<String>,
    },
}

impl fmt::Display for ApexesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInstrument { reason } => {
                write!(f, "invalid instrument: {reason}")
            }
            Self::ConnectionFailed { reason, .. } => {
                write!(f, "market data connection failed: {reason}")
            }
        }
    }
}

impl Error for ApexesError {}

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
            source: Some("io error: connection reset".to_string()),
        };
        let cloned = err.clone();
        assert_eq!(err, cloned);
    }
}
