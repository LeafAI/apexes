//! [`RiskViolation`]: describes exactly which risk rule was violated and
//! why, so callers can log/debug precisely which limit/list/self-trade/
//! circuit-breaker condition rejected an order or fill.

use apexes_core::Venue;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Error)]
pub enum RiskViolation {
    #[error("order quantity {actual} exceeds per-order limit {limit}")]
    OrderQuantityExceeded { actual: Decimal, limit: Decimal },

    #[error("order notional {actual} exceeds per-order notional limit {limit}")]
    OrderNotionalExceeded { actual: Decimal, limit: Decimal },

    #[error("position for {symbol} would reach {projected}, exceeding limit {limit}")]
    PositionLimitExceeded {
        symbol: String,
        projected: Decimal,
        limit: Decimal,
    },

    #[error("account equity {actual} is below the configured floor {floor}")]
    EquityFloorBreached { actual: Decimal, floor: Decimal },

    #[error("daily trade count {actual} would exceed limit {limit} (day: {day})")]
    DailyTradeCountExceeded {
        actual: u32,
        limit: u32,
        day: DateTime<Utc>,
    },

    #[error("daily traded volume {actual} would exceed limit {limit} (day: {day})")]
    DailyVolumeExceeded {
        actual: Decimal,
        limit: Decimal,
        day: DateTime<Utc>,
    },

    #[error("symbol {symbol} is blacklisted")]
    SymbolBlacklisted { symbol: String },

    #[error("symbol {symbol} is not in the whitelist")]
    SymbolNotWhitelisted { symbol: String },

    #[error("venue {venue:?} is blacklisted")]
    VenueBlacklisted { venue: Venue },

    #[error("venue {venue:?} is not in the whitelist")]
    VenueNotWhitelisted { venue: Venue },

    #[error("order for {symbol} would self-trade against an existing opposite-side open order")]
    SelfTradeRisk { symbol: String },

    #[error("circuit breaker is open for {scope:?}; new orders are rejected until it resets")]
    CircuitBreakerOpen { scope: CircuitBreakerScope },

    #[error("margin ratio for {symbol} is {ratio} (at or below the warning threshold {threshold})")]
    MarginRatioBreached {
        symbol: String,
        ratio: Decimal,
        threshold: Decimal,
    },
}

/// Which granularity a [`crate::circuit_breaker::CircuitBreaker`] trip
/// applies to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CircuitBreakerScope {
    Symbol(String),
    Account,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn order_quantity_exceeded_display() {
        let err = RiskViolation::OrderQuantityExceeded {
            actual: dec!(10),
            limit: dec!(5),
        };
        assert_eq!(
            err.to_string(),
            "order quantity 10 exceeds per-order limit 5"
        );
    }

    #[test]
    fn symbol_blacklisted_display() {
        let err = RiskViolation::SymbolBlacklisted {
            symbol: "BTCUSDT".to_string(),
        };
        assert_eq!(err.to_string(), "symbol BTCUSDT is blacklisted");
    }

    #[test]
    fn circuit_breaker_open_display_for_symbol_scope() {
        let err = RiskViolation::CircuitBreakerOpen {
            scope: CircuitBreakerScope::Symbol("BTCUSDT".to_string()),
        };
        assert_eq!(
            err.to_string(),
            "circuit breaker is open for Symbol(\"BTCUSDT\"); new orders are rejected until it resets"
        );
    }

    #[test]
    fn circuit_breaker_open_display_for_account_scope() {
        let err = RiskViolation::CircuitBreakerOpen {
            scope: CircuitBreakerScope::Account,
        };
        assert_eq!(
            err.to_string(),
            "circuit breaker is open for Account; new orders are rejected until it resets"
        );
    }

    #[test]
    fn circuit_breaker_scope_equality() {
        assert_eq!(
            CircuitBreakerScope::Symbol("BTCUSDT".to_string()),
            CircuitBreakerScope::Symbol("BTCUSDT".to_string())
        );
        assert_ne!(
            CircuitBreakerScope::Account,
            CircuitBreakerScope::Symbol("BTCUSDT".to_string())
        );
    }
}
