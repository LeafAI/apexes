//! Versioned read-only risk state evidence. These DTOs cannot instantiate
//! a runnable checker, clear a trip, or grant submission authority.
use crate::RiskLimits;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RiskStateError {
    #[error("risk state lock is poisoned")]
    Poisoned,
    #[error("invalid or unsupported risk state evidence")]
    Invalid,
    #[error("risk configuration or retained state differs from independent evidence")]
    Mismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitStateSnapshot {
    pub version: u32,
    pub limits: RiskLimits,
    pub day: Option<NaiveDate>,
    pub trade_count: u32,
    pub traded_volume: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircuitStateSnapshot {
    pub version: u32,
    pub limits: RiskLimits,
    pub tripped_symbols: Vec<String>,
    pub account_tripped: bool,
}

/// Evidence for one registered slot. Unsupported monitors are explicit,
/// not treated as stateless or as a successfully captured empty state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RegisteredRiskState {
    Limits(LimitStateSnapshot),
    Circuit(CircuitStateSnapshot),
    Unsupported,
}

/// Registration order and role are retained. Two split circuit-breaker
/// slots can refer to one shared monitor; equality does not prove aliasing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisteredRiskSnapshot {
    pub version: u32,
    pub pre_trade: Vec<RegisteredRiskState>,
    pub in_trade: Vec<RegisteredRiskState>,
    pub position_monitor_count: usize,
    pub market_condition_monitor_count: usize,
}

impl RegisteredRiskSnapshot {
    /// Validate bounded partial evidence without constructing a monitor.
    pub fn validate(&self) -> Result<(), RiskStateError> {
        if self.version != 1
            || [
                self.pre_trade.len(),
                self.in_trade.len(),
                self.position_monitor_count,
                self.market_condition_monitor_count,
            ]
            .into_iter()
            .any(|n| n > 128)
        {
            return Err(RiskStateError::Invalid);
        }
        for state in &self.pre_trade {
            match state {
                RegisteredRiskState::Limits(s) => s.validate()?,
                RegisteredRiskState::Circuit(s) => s.validate()?,
                RegisteredRiskState::Unsupported => {}
            }
        }
        for state in &self.in_trade {
            match state {
                RegisteredRiskState::Circuit(s) => s.validate()?,
                RegisteredRiskState::Unsupported => {}
                RegisteredRiskState::Limits(_) => return Err(RiskStateError::Invalid),
            }
        }
        Ok(())
    }

    /// Only the supported slot kinds have captured state. This is
    /// coverage information, not a recovery or trading authorization.
    pub fn has_unsupported_slots(&self) -> bool {
        self.position_monitor_count != 0
            || self.market_condition_monitor_count != 0
            || self
                .pre_trade
                .iter()
                .chain(&self.in_trade)
                .any(|s| matches!(s, RegisteredRiskState::Unsupported))
    }
}

fn limits_valid(limits: &RiskLimits) -> bool {
    [
        limits.max_order_quantity,
        limits.max_order_notional,
        limits.max_position_per_symbol,
        limits.min_account_equity,
        limits.max_daily_volume,
    ]
    .into_iter()
    .flatten()
    .all(|n| n >= Decimal::ZERO)
}

impl LimitStateSnapshot {
    pub fn validate(&self) -> Result<(), RiskStateError> {
        if self.version != 1
            || !limits_valid(&self.limits)
            || self.traded_volume < Decimal::ZERO
            || self.trade_count == u32::MAX
            || (self.day.is_none() && (self.trade_count != 0 || !self.traded_volume.is_zero()))
            || (self.trade_count == 0) != self.traded_volume.is_zero()
            || (self.day.is_some() && self.trade_count == 0)
        {
            return Err(RiskStateError::Invalid);
        }
        // Counters can exceed a tightened limit or protective exit budget;
        // never clamp them or infer acceptance eligibility from this DTO.
        Ok(())
    }

    pub fn compare(&self, independently_retained: &Self) -> Result<(), RiskStateError> {
        self.validate()?;
        independently_retained.validate()?;
        if self != independently_retained {
            return Err(RiskStateError::Mismatch);
        }
        Ok(())
    }
}

impl CircuitStateSnapshot {
    pub fn validate(&self) -> Result<(), RiskStateError> {
        if self.version != 1
            || !limits_valid(&self.limits)
            || self.tripped_symbols.len() > 128
            || self
                .tripped_symbols
                .iter()
                .any(|s| s.is_empty() || s.len() > 128 || s.chars().any(char::is_control))
            || !self.tripped_symbols.windows(2).all(|w| w[0] < w[1])
            || (!self.tripped_symbols.is_empty() && self.limits.max_position_per_symbol.is_none())
            || (self.account_tripped && self.limits.min_account_equity.is_none())
        {
            return Err(RiskStateError::Invalid);
        }
        Ok(())
    }

    pub fn compare(&self, independently_retained: &Self) -> Result<(), RiskStateError> {
        self.validate()?;
        independently_retained.validate()?;
        if self != independently_retained {
            return Err(RiskStateError::Mismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn invalid_daily_state_refuses_saturation_and_inconsistent_day_or_volume() {
        let state = LimitStateSnapshot {
            version: 1,
            limits: RiskLimits::default(),
            day: Some(NaiveDate::from_ymd_opt(2026, 11, 28).unwrap()),
            trade_count: 2,
            traded_volume: dec!(3),
        };
        state.validate().unwrap();
        for bad in [
            LimitStateSnapshot {
                version: 2,
                ..state.clone()
            },
            LimitStateSnapshot {
                day: None,
                ..state.clone()
            },
            LimitStateSnapshot {
                trade_count: u32::MAX,
                ..state.clone()
            },
            LimitStateSnapshot {
                trade_count: 0,
                ..state.clone()
            },
            LimitStateSnapshot {
                traded_volume: dec!(-1),
                ..state.clone()
            },
            LimitStateSnapshot {
                traded_volume: Decimal::ZERO,
                ..state.clone()
            },
        ] {
            assert_eq!(bad.validate(), Err(RiskStateError::Invalid));
        }
    }

    #[test]
    fn invalid_trip_snapshot_rejects_noncanonical_symbols_and_unconfigured_scope() {
        let state = CircuitStateSnapshot {
            version: 1,
            limits: RiskLimits {
                max_position_per_symbol: Some(dec!(1)),
                ..Default::default()
            },
            tripped_symbols: vec!["BTCUSDT".into(), "ETHUSDT".into()],
            account_tripped: false,
        };
        state.validate().unwrap();
        for bad in [
            CircuitStateSnapshot {
                version: 2,
                ..state.clone()
            },
            CircuitStateSnapshot {
                tripped_symbols: vec!["BTCUSDT".into(), "BTCUSDT".into()],
                ..state.clone()
            },
            CircuitStateSnapshot {
                tripped_symbols: vec!["ETHUSDT".into(), "BTCUSDT".into()],
                ..state.clone()
            },
            CircuitStateSnapshot {
                account_tripped: true,
                ..state.clone()
            },
            CircuitStateSnapshot {
                limits: RiskLimits::default(),
                ..state.clone()
            },
        ] {
            assert_eq!(bad.validate(), Err(RiskStateError::Invalid));
        }
    }
}
