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

    #[error("insufficient balance: required {required} {asset}, available {available}")]
    InsufficientBalance {
        asset: String,
        required: rust_decimal::Decimal,
        available: rust_decimal::Decimal,
    },

    #[error("invalid backtest config: {reason}")]
    InvalidBacktestConfig { reason: String },

    #[error(
        "inconsistent leverage for isolated position on {symbol}: existing {existing}, requested {requested}"
    )]
    InconsistentLeverageForIsolatedPosition {
        symbol: String,
        existing: rust_decimal::Decimal,
        requested: rust_decimal::Decimal,
    },

    #[error("order rejected: {reason}")]
    OrderRejected { reason: String },
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

    #[test]
    fn insufficient_balance_display() {
        let err = ApexesError::InsufficientBalance {
            asset: "USDT".to_string(),
            required: rust_decimal_macros::dec!(1000),
            available: rust_decimal_macros::dec!(500),
        };
        assert_eq!(
            err.to_string(),
            "insufficient balance: required 1000 USDT, available 500"
        );
    }

    #[test]
    fn invalid_backtest_config_display() {
        let err = ApexesError::InvalidBacktestConfig {
            reason: "missing initial balance for traded asset BTC".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "invalid backtest config: missing initial balance for traded asset BTC"
        );
    }

    #[test]
    fn inconsistent_leverage_for_isolated_position_display() {
        let err = ApexesError::InconsistentLeverageForIsolatedPosition {
            symbol: "BTCUSDT".to_string(),
            existing: rust_decimal_macros::dec!(10),
            requested: rust_decimal_macros::dec!(5),
        };
        assert_eq!(
            err.to_string(),
            "inconsistent leverage for isolated position on BTCUSDT: existing 10, requested 5"
        );
    }

    #[test]
    fn order_rejected_display() {
        let err = ApexesError::OrderRejected {
            reason: "unsupported order kind".to_string(),
        };
        assert_eq!(err.to_string(), "order rejected: unsupported order kind");
    }

    #[test]
    fn insufficient_balance_is_cloneable() {
        let err = ApexesError::InsufficientBalance {
            asset: "BTC".to_string(),
            required: rust_decimal_macros::dec!(1),
            available: rust_decimal_macros::dec!(0.5),
        };
        let cloned = err.clone();
        assert_eq!(err, cloned);
    }
}
