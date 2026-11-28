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
    #[error("shared risk monitor is busy; coherent inspection refused")]
    Busy,
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

/// Canonical evidence of the configured list, including disabled and
/// empty whitelist/blacklist distinctions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ListState<T> {
    Disabled,
    Blacklist(Vec<T>),
    Whitelist(Vec<T>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SymbolVenueListSnapshot {
    pub version: u32,
    pub symbols: ListState<String>,
    pub venues: ListState<apexes_core::Venue>,
}

fn canonical_symbols(values: &[String]) -> bool {
    values.len() <= 128
        && values
            .iter()
            .all(|s| !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control))
        && values.windows(2).all(|w| w[0] < w[1])
}

pub(crate) fn venue_key(venue: &apexes_core::Venue) -> String {
    format!("{venue:?}")
}

impl SymbolVenueListSnapshot {
    pub fn validate(&self) -> Result<(), RiskStateError> {
        let symbols_valid = match &self.symbols {
            ListState::Disabled => true,
            ListState::Blacklist(v) | ListState::Whitelist(v) => canonical_symbols(v),
        };
        let venues_valid = match &self.venues {
            ListState::Disabled => true,
            ListState::Blacklist(v) | ListState::Whitelist(v) => {
                v.len() <= 128 && v.windows(2).all(|w| venue_key(&w[0]) < venue_key(&w[1]))
            }
        };
        if self.version != 1 || !symbols_valid || !venues_valid {
            return Err(RiskStateError::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarginStateSnapshot {
    pub version: u32,
    pub warning_threshold: Decimal,
    pub tripped_symbols: Vec<String>,
}

impl MarginStateSnapshot {
    pub fn validate(&self) -> Result<(), RiskStateError> {
        if self.version != 1
            || self.warning_threshold <= Decimal::ZERO
            || !canonical_symbols(&self.tripped_symbols)
        {
            return Err(RiskStateError::Invalid);
        }
        Ok(())
    }
}

/// Actual price and volatility evidence keyed by the monitor's symbol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketSymbolSnapshot {
    pub symbol: String,
    pub last_price: Decimal,
    pub volatility: apexes_indicators::checkpoint::StddevState,
    pub reduce_fraction: Option<Decimal>,
}

/// Read-only evidence, not an instantiated monitor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketConditionSnapshot {
    pub version: u32,
    pub config: crate::MarketConditionConfig,
    pub symbols: Vec<MarketSymbolSnapshot>,
    pub equity_volatility: Option<apexes_indicators::checkpoint::StddevState>,
    pub account_wide_reduce_fraction: Option<Decimal>,
}

impl MarketConditionSnapshot {
    pub fn validate(&self) -> Result<(), RiskStateError> {
        let c = &self.config;
        let valid_fraction =
            |f: Option<Decimal>| f.is_none_or(|v| v > Decimal::ZERO && v <= Decimal::ONE);
        if self.version != 1
            || !(1..=apexes_indicators::checkpoint::MAX_PERIOD).contains(&c.volatility_window)
            || c.volatility_threshold <= Decimal::ZERO
            || c.jump_threshold_pct <= Decimal::ZERO
            || c.sensitivity < Decimal::ZERO
            || c.account_equity_volatility_threshold
                .is_some_and(|v| v <= Decimal::ZERO)
            || self.symbols.len() > 128
            || !canonical_symbols(
                &self
                    .symbols
                    .iter()
                    .map(|s| s.symbol.clone())
                    .collect::<Vec<_>>(),
            )
            || self.equity_volatility.is_some() != c.account_equity_volatility_threshold.is_some()
            || !valid_fraction(self.account_wide_reduce_fraction)
            || (self.account_wide_reduce_fraction.is_some()
                && self
                    .equity_volatility
                    .as_ref()
                    .is_none_or(|v| v.sample_count() != c.volatility_window))
        {
            return Err(RiskStateError::Invalid);
        }
        for symbol in &self.symbols {
            if symbol.last_price <= Decimal::ZERO
                || symbol.volatility.last() != Some(symbol.last_price)
                || !valid_fraction(symbol.reduce_fraction)
            {
                return Err(RiskStateError::Invalid);
            }
            // Existing bounded indicator validator; discard the isolated value.
            symbol
                .volatility
                .clone()
                .restore(c.volatility_window)
                .map_err(|_| RiskStateError::Invalid)?;
        }
        if let Some(equity) = &self.equity_volatility {
            equity
                .clone()
                .restore(c.volatility_window)
                .map_err(|_| RiskStateError::Invalid)?;
        }
        Ok(())
    }
}

/// Evidence for one registered slot. Unsupported monitors are explicit,
/// not treated as stateless or as a successfully captured empty state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RegisteredRiskState {
    Limits(LimitStateSnapshot),
    Circuit(CircuitStateSnapshot),
    SymbolVenueList(SymbolVenueListSnapshot),
    SelfTradePreventer { version: u32 },
    Margin(MarginStateSnapshot),
    MarketCondition(MarketConditionSnapshot),
    Unsupported,
}

/// Canonical local alias identifiers, numbered by first encounter in
/// pre-trade, in-trade, position and market-condition registration order.
/// None means that slot did not expose an actual built-in shared handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisteredAliases {
    pub pre_trade: Vec<Option<u16>>,
    pub in_trade: Vec<Option<u16>>,
    pub position: Vec<Option<u16>>,
    pub market_condition: Vec<Option<u16>>,
}

impl RegisteredAliases {
    fn validate(&self, snapshot: &RegisteredRiskSnapshot) -> Result<(), RiskStateError> {
        let roles = [
            (&self.pre_trade, snapshot.pre_trade.as_slice()),
            (&self.in_trade, snapshot.in_trade.as_slice()),
            (
                &self.position,
                snapshot.position.as_deref().unwrap_or_default(),
            ),
            (
                &self.market_condition,
                snapshot.market_condition.as_deref().unwrap_or_default(),
            ),
        ];
        if self.position.len() != snapshot.position_monitor_count
            || self.market_condition.len() != snapshot.market_condition_monitor_count
        {
            return Err(RiskStateError::Invalid);
        }
        let mut groups: Vec<&RegisteredRiskState> = Vec::new();
        for (ids, states) in roles {
            if ids.len() != states.len() {
                return Err(RiskStateError::Invalid);
            }
            for (id, state) in ids.iter().zip(states) {
                if id.is_none()
                    && matches!(
                        state,
                        RegisteredRiskState::Margin(_) | RegisteredRiskState::MarketCondition(_)
                    )
                {
                    return Err(RiskStateError::Invalid);
                }
                if let Some(id) = id {
                    if !matches!(
                        state,
                        RegisteredRiskState::Circuit(_)
                            | RegisteredRiskState::Margin(_)
                            | RegisteredRiskState::MarketCondition(_)
                    ) {
                        return Err(RiskStateError::Invalid);
                    }
                    let index = usize::from(*id);
                    if index == groups.len() {
                        groups.push(state);
                    } else if groups.get(index).copied() != Some(state) {
                        return Err(RiskStateError::Invalid);
                    }
                }
            }
        }
        Ok(())
    }
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
    /// Absent on legacy receipts: those slots were not inspected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<Vec<RegisteredRiskState>>,
    pub market_condition_monitor_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_condition: Option<Vec<RegisteredRiskState>>,
    /// Absent for legacy receipts: alias topology was not qualified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aliases: Option<RegisteredAliases>,
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
                RegisteredRiskState::SymbolVenueList(s) => s.validate()?,
                RegisteredRiskState::SelfTradePreventer { version: 1 } => {}
                RegisteredRiskState::SelfTradePreventer { .. } => {
                    return Err(RiskStateError::Invalid);
                }
                RegisteredRiskState::Margin(s) => s.validate()?,
                RegisteredRiskState::MarketCondition(s) => s.validate()?,
                RegisteredRiskState::Unsupported => {}
            }
        }
        for state in &self.in_trade {
            match state {
                RegisteredRiskState::Circuit(s) => s.validate()?,
                RegisteredRiskState::Unsupported => {}
                _ => return Err(RiskStateError::Invalid),
            }
        }
        if let Some(position) = &self.position {
            if position.len() != self.position_monitor_count {
                return Err(RiskStateError::Invalid);
            }
            for state in position {
                match state {
                    RegisteredRiskState::Margin(s) => s.validate()?,
                    RegisteredRiskState::Unsupported => {}
                    _ => return Err(RiskStateError::Invalid),
                }
            }
        }
        if let Some(market) = &self.market_condition {
            if market.len() != self.market_condition_monitor_count {
                return Err(RiskStateError::Invalid);
            }
            for state in market {
                match state {
                    RegisteredRiskState::MarketCondition(s) => s.validate()?,
                    RegisteredRiskState::Unsupported => {}
                    _ => return Err(RiskStateError::Invalid),
                }
            }
        }
        if let Some(aliases) = &self.aliases {
            aliases.validate(self)?;
        }
        Ok(())
    }

    /// Only the supported slot kinds have captured state. This is
    /// coverage information, not a recovery or trading authorization.
    pub fn has_unsupported_slots(&self) -> bool {
        self.position
            .as_ref()
            .map_or(self.position_monitor_count != 0, |slots| {
                slots.len() != self.position_monitor_count
                    || slots
                        .iter()
                        .any(|s| matches!(s, RegisteredRiskState::Unsupported))
            })
            || self.market_condition.as_ref().map_or(
                self.market_condition_monitor_count != 0,
                |slots| {
                    slots.len() != self.market_condition_monitor_count
                        || slots
                            .iter()
                            .any(|s| matches!(s, RegisteredRiskState::Unsupported))
                },
            )
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
    fn list_and_margin_evidence_refuse_noncanonical_state() {
        use apexes_core::Venue;
        let mut list = SymbolVenueListSnapshot {
            version: 1,
            symbols: ListState::Whitelist(vec![]),
            venues: ListState::Disabled,
        };
        list.validate().unwrap();
        let mut other = list.clone();
        other.symbols = ListState::Disabled;
        assert_ne!(list, other);
        list.symbols = ListState::Blacklist(vec!["ETH".into(), "BTC".into()]);
        assert!(list.validate().is_err());
        list.symbols = ListState::Disabled;
        list.venues = ListState::Whitelist(vec![Venue::Binance, Venue::Binance]);
        assert!(list.validate().is_err());
        let mut margin = MarginStateSnapshot {
            version: 1,
            warning_threshold: dec!(1.5),
            tripped_symbols: vec!["BTC".into()],
        };
        margin.validate().unwrap();
        margin.tripped_symbols.push("BTC".into());
        assert!(margin.validate().is_err());
        margin.tripped_symbols.clear();
        margin.warning_threshold = Decimal::ZERO;
        assert!(margin.validate().is_err());
    }

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
