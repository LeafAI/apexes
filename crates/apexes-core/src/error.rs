use crate::instrument::Venue;
use crate::order::OrderStatus;
use thiserror::Error;

/// Unified error type for apexes-core. Cloneable — variants that carry
/// underlying-error context store it as a formatted `String`, not
/// `Box<dyn Error>`, so `ApexesError` can be cloned freely.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ApexesError {
    #[error("invalid instrument: {reason}")]
    InvalidInstrument { reason: String },

    #[error("invalid order state transition: {from:?} -> {to:?}")]
    InvalidStateTransition { from: OrderStatus, to: OrderStatus },

    #[error("market data connection failed: {reason}")]
    ConnectionFailed {
        reason: String,
        source_detail: Option<String>,
    },

    #[error("subscription failed for {symbol} on {venue:?}: {reason}")]
    SubscriptionFailed {
        symbol: String,
        venue: Venue,
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

    #[test]
    fn subscription_failed_display() {
        let err = ApexesError::SubscriptionFailed {
            symbol: "BTCUSDT".to_string(),
            venue: crate::instrument::Venue::Binance,
            reason: "rate limited".to_string(),
            source_detail: None,
        };
        assert_eq!(
            err.to_string(),
            "subscription failed for BTCUSDT on Binance: rate limited"
        );
    }
}
