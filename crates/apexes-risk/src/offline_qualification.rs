//! Read-only compatibility contract for independently configured offline
//! candidates. No state is applied, no runnable monitors are returned.
use crate::{MarketConditionSnapshot, RegisteredRiskSnapshot, RegisteredRiskState, RiskStateError};

impl MarketConditionSnapshot {
    /// Require the same configuration and an unused independent candidate.
    pub fn qualify_offline_candidate(&self, candidate: &Self) -> Result<(), RiskStateError> {
        self.validate()?;
        candidate.validate()?;
        if self.config != candidate.config
            || !candidate.symbols.is_empty()
            || candidate.account_wide_reduce_fraction.is_some()
            || candidate
                .equity_volatility
                .as_ref()
                .is_some_and(|s| s.sample_count() != 0)
        {
            return Err(RiskStateError::Mismatch);
        }
        Ok(())
    }
}

fn qualify_slot(
    retained: &RegisteredRiskState,
    candidate: &RegisteredRiskState,
) -> Result<(), RiskStateError> {
    use RegisteredRiskState::*;
    let matches = match (retained, candidate) {
        (Limits(a), Limits(b)) => {
            a.limits == b.limits
                && b.day.is_none()
                && b.trade_count == 0
                && b.traded_volume.is_zero()
        }
        (Circuit(a), Circuit(b)) => {
            a.limits == b.limits && b.tripped_symbols.is_empty() && !b.account_tripped
        }
        (Margin(a), Margin(b)) => {
            a.warning_threshold == b.warning_threshold && b.tripped_symbols.is_empty()
        }
        (SymbolVenueList(a), SymbolVenueList(b)) => a == b,
        (SelfTradePreventer { version: a }, SelfTradePreventer { version: b }) => a == b,
        (MarketCondition(a), MarketCondition(b)) => return a.qualify_offline_candidate(b),
        _ => false,
    };
    if !matches {
        return Err(RiskStateError::Mismatch);
    }
    Ok(())
}

impl RegisteredRiskSnapshot {
    /// Qualify a pristine candidate's exact registration, configuration
    /// and actual alias topology against retained evidence. A positive
    /// result proves only compatibility, never state restoration,
    /// source completeness, freshness, or execution authority.
    pub fn qualify_offline_candidate(&self, candidate: &Self) -> Result<(), RiskStateError> {
        self.validate()?;
        candidate.validate()?;
        if self.has_unsupported_slots()
            || candidate.has_unsupported_slots()
            || self.aliases.is_none()
            || candidate.aliases.is_none()
        {
            return Err(RiskStateError::Invalid);
        }
        if self.aliases != candidate.aliases
            || self.position_monitor_count != candidate.position_monitor_count
            || self.market_condition_monitor_count != candidate.market_condition_monitor_count
        {
            return Err(RiskStateError::Mismatch);
        }
        let roles = [
            (self.pre_trade.as_slice(), candidate.pre_trade.as_slice()),
            (self.in_trade.as_slice(), candidate.in_trade.as_slice()),
            (
                self.position.as_deref().unwrap_or_default(),
                candidate.position.as_deref().unwrap_or_default(),
            ),
            (
                self.market_condition.as_deref().unwrap_or_default(),
                candidate.market_condition.as_deref().unwrap_or_default(),
            ),
        ];
        for (retained, fresh) in roles {
            if retained.len() != fresh.len() {
                return Err(RiskStateError::Mismatch);
            }
            for (a, b) in retained.iter().zip(fresh) {
                qualify_slot(a, b)?;
            }
        }
        Ok(())
    }
}
