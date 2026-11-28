//! F04.05 — isolated built-in risk reconstruction. Never expose the
//! reconstructed checker or its handles, and never mutate a live candidate.
use crate::{
    CircuitBreaker, CircuitBreakerInTradeAdapter, CircuitBreakerPreTradeAdapter,
    CompositeRiskChecker, LimitChecker, ListMode, ListState, MarginMonitor,
    MarginMonitorPositionAdapter, MarginMonitorPreTradeAdapter, MarketConditionEventAdapter,
    MarketConditionMonitor, MarketConditionPreTradeAdapter, RegisteredRiskSnapshot,
    RegisteredRiskState, RiskStateError as E,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

enum Shared {
    Circuit(Arc<Mutex<CircuitBreaker>>),
    Margin(Arc<Mutex<MarginMonitor>>),
    Market(Arc<Mutex<MarketConditionMonitor>>),
}

fn list<T: Clone + Eq + std::hash::Hash>(state: &ListState<T>) -> ListMode<T> {
    match state {
        ListState::Disabled => ListMode::Disabled,
        ListState::Blacklist(v) => ListMode::Blacklist(v.iter().cloned().collect()),
        ListState::Whitelist(v) => ListMode::Whitelist(v.iter().cloned().collect()),
    }
}

fn shared<'a>(
    groups: &'a mut HashMap<u16, Shared>,
    id: u16,
    state: &RegisteredRiskState,
) -> Result<&'a Shared, E> {
    use std::collections::hash_map::Entry;
    Ok(match groups.entry(id) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => entry.insert(match state {
            RegisteredRiskState::Circuit(s) => Shared::Circuit(Arc::new(Mutex::new(
                CircuitBreaker::reconstruct_offline(s)?,
            ))),
            RegisteredRiskState::Margin(s) => {
                Shared::Margin(Arc::new(Mutex::new(MarginMonitor::reconstruct_offline(s)?)))
            }
            RegisteredRiskState::MarketCondition(s) => Shared::Market(Arc::new(Mutex::new(
                MarketConditionMonitor::reconstruct_offline(s)?,
            ))),
            _ => return Err(E::Invalid),
        }),
    })
}

fn reconstruct(snapshot: &RegisteredRiskSnapshot) -> Result<CompositeRiskChecker, E> {
    snapshot.validate()?;
    if snapshot.has_unsupported_slots() {
        return Err(E::Invalid);
    }
    let aliases = snapshot.aliases.as_ref().ok_or(E::Invalid)?;
    let mut groups = HashMap::new();
    let mut result = CompositeRiskChecker::new();
    for (state, id) in snapshot.pre_trade.iter().zip(&aliases.pre_trade) {
        use RegisteredRiskState::*;
        let check: Box<dyn crate::PreTradeCheck> = match (state, id) {
            (Limits(s), None) => Box::new(LimitChecker::reconstruct_offline(s)?),
            (Circuit(s), None) => Box::new(CircuitBreaker::reconstruct_offline(s)?),
            (SymbolVenueList(s), None) => Box::new(crate::SymbolVenueList::new(
                list(&s.symbols),
                list(&s.venues),
            )),
            (SelfTradePreventer { version: 1 }, None) => Box::new(crate::SelfTradePreventer),
            (_, Some(id)) => match shared(&mut groups, *id, state)? {
                Shared::Circuit(m) => Box::new(CircuitBreakerPreTradeAdapter(Arc::clone(m))),
                Shared::Margin(m) => Box::new(MarginMonitorPreTradeAdapter(Arc::clone(m))),
                Shared::Market(m) => Box::new(MarketConditionPreTradeAdapter(Arc::clone(m))),
            },
            _ => return Err(E::Invalid),
        };
        result = result.with_pre_trade_check(check);
    }
    for (state, id) in snapshot.in_trade.iter().zip(&aliases.in_trade) {
        let monitor: Box<dyn crate::InTradeMonitor> = match (state, id) {
            (RegisteredRiskState::Circuit(s), None) => {
                Box::new(CircuitBreaker::reconstruct_offline(s)?)
            }
            (_, Some(id)) => match shared(&mut groups, *id, state)? {
                Shared::Circuit(m) => Box::new(CircuitBreakerInTradeAdapter(Arc::clone(m))),
                _ => return Err(E::Invalid),
            },
            _ => return Err(E::Invalid),
        };
        result = result.with_in_trade_monitor(monitor);
    }
    for (state, id) in snapshot
        .position
        .as_deref()
        .unwrap_or_default()
        .iter()
        .zip(&aliases.position)
    {
        match shared(&mut groups, id.ok_or(E::Invalid)?, state)? {
            Shared::Margin(m) => {
                result = result
                    .with_position_monitor(Box::new(MarginMonitorPositionAdapter(Arc::clone(m))))
            }
            _ => return Err(E::Invalid),
        }
    }
    for (state, id) in snapshot
        .market_condition
        .as_deref()
        .unwrap_or_default()
        .iter()
        .zip(&aliases.market_condition)
    {
        match shared(&mut groups, id.ok_or(E::Invalid)?, state)? {
            Shared::Market(m) => {
                result = result.with_market_condition_monitor(Box::new(
                    MarketConditionEventAdapter(Arc::clone(m)),
                ))
            }
            _ => return Err(E::Invalid),
        }
    }
    Ok(result)
}

impl RegisteredRiskSnapshot {
    /// Require independently configured pristine registration evidence,
    /// then reconstruct private built-ins and compare their actual
    /// re-inspected state and shared topology exactly. Never return a
    /// runnable checker or imply that the retained state is authoritative.
    pub fn qualify_isolated_restoration(&self, pristine: &Self) -> Result<(), E> {
        self.qualify_offline_candidate(pristine)?;
        let reconstructed = reconstruct(self)?;
        if reconstructed.inspect_registered_state()? != *self {
            return Err(E::Mismatch);
        }
        Ok(())
    }
}

impl crate::MarketConditionSnapshot {
    /// The separately registered market monitor is reconstructed privately
    /// and inspected; no runnable monitor or de-risking handle is returned.
    pub fn qualify_isolated_restoration(&self, pristine: &Self) -> Result<(), E> {
        self.qualify_offline_candidate(pristine)?;
        if MarketConditionMonitor::reconstruct_offline(self)?.inspect_checkpoint()? != *self {
            return Err(E::Mismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MarketConditionConfig, PriceSource, RiskLimits};
    use apexes_core::{MarketEvent, TradeEvent, Venue};
    use apexes_indicators::{Indicator, RollingStdDev};
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;

    #[test]
    fn reconstructs_all_roles_and_actual_shared_aliases_with_exact_indicator_state() {
        let config = MarketConditionConfig {
            volatility_window: 3,
            volatility_threshold: dec!(1),
            jump_threshold_pct: dec!(0.05),
            sensitivity: dec!(1),
            price_source: PriceSource::Trade,
            account_equity_volatility_threshold: Some(dec!(1)),
            include_spot: false,
        };
        let limits = RiskLimits {
            max_daily_trade_count: Some(1),
            max_position_per_symbol: Some(dec!(1)),
            min_account_equity: Some(dec!(10)),
            ..Default::default()
        };
        let (cpre, cin) = CircuitBreaker::new(limits.clone()).split();
        let (mpre, mpos) = MarginMonitor::new(dec!(1.5)).split();
        let (vpre, vevent, _) = MarketConditionMonitor::new(config.clone()).split();
        let fresh = CompositeRiskChecker::new()
            .with_pre_trade_check(Box::new(LimitChecker::new(limits.clone())))
            .with_pre_trade_check(Box::new(cpre))
            .with_pre_trade_check(Box::new(mpre))
            .with_pre_trade_check(Box::new(vpre))
            .with_pre_trade_check(Box::new(crate::SelfTradePreventer))
            .with_pre_trade_check(Box::new(crate::SymbolVenueList::new(
                ListMode::Whitelist(["BTCUSDT".into()].into_iter().collect()),
                ListMode::Disabled,
            )))
            .with_in_trade_monitor(Box::new(cin))
            .with_in_trade_monitor(Box::new(CircuitBreaker::new(limits)))
            .with_position_monitor(Box::new(mpos))
            .with_market_condition_monitor(Box::new(vevent));
        let pristine = fresh.inspect_registered_state().unwrap();
        let mut retained = pristine.clone();
        if let RegisteredRiskState::Limits(s) = &mut retained.pre_trade[0] {
            s.day = Some(
                DateTime::<Utc>::from_timestamp(1_800_000_000, 0)
                    .unwrap()
                    .date_naive(),
            );
            s.trade_count = 2;
            s.traded_volume = dec!(100);
        }
        if let RegisteredRiskState::Circuit(s) = &mut retained.pre_trade[1] {
            s.tripped_symbols = vec!["BTCUSDT".into()];
            s.account_tripped = true;
        }
        retained.in_trade[0] = retained.pre_trade[1].clone();
        if let RegisteredRiskState::Margin(s) = &mut retained.pre_trade[2] {
            s.tripped_symbols = vec!["BTCUSDT".into()];
        }
        retained.position.as_mut().unwrap()[0] = retained.pre_trade[2].clone();
        let mut volatility = RollingStdDev::new(3);
        for value in [dec!(100), dec!(105), dec!(120)] {
            volatility.update(value);
        }
        let state = apexes_indicators::checkpoint::StddevState::capture(&volatility);
        if let RegisteredRiskState::MarketCondition(s) = &mut retained.pre_trade[3] {
            s.symbols = vec![crate::MarketSymbolSnapshot {
                symbol: "BTCUSDT".into(),
                last_price: dec!(120),
                volatility: state.clone(),
                reduce_fraction: Some(dec!(0.5)),
            }];
            s.equity_volatility = Some(state);
            s.account_wide_reduce_fraction = Some(dec!(0.25));
        }
        retained.market_condition.as_mut().unwrap()[0] = retained.pre_trade[3].clone();
        retained.qualify_isolated_restoration(&pristine).unwrap();
        assert_eq!(fresh.inspect_registered_state().unwrap(), pristine);
        let restored = reconstruct(&retained).unwrap();
        assert_eq!(restored.inspect_registered_state().unwrap(), retained);
        // A new event mutates the actual shared market state in both roles.
        let event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: dec!(121),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: DateTime::<Utc>::from_timestamp(1_800_000_001, 0).unwrap(),
        });
        let _ = restored.on_market_event(&event);
        let next = restored.inspect_registered_state().unwrap();
        assert_ne!(next.pre_trade[3], retained.pre_trade[3]);
        assert_eq!(next.pre_trade[3], next.market_condition.unwrap()[0]);
        assert_eq!(fresh.inspect_registered_state().unwrap(), pristine);
        let mut broken = retained.clone();
        broken.aliases.as_mut().unwrap().in_trade[0] = None;
        assert!(broken.qualify_isolated_restoration(&pristine).is_err());
        let mut broken = retained.clone();
        broken.pre_trade[1] = pristine.pre_trade[1].clone();
        assert!(broken.qualify_isolated_restoration(&pristine).is_err());
        if let (RegisteredRiskState::MarketCondition(s), RegisteredRiskState::MarketCondition(p)) =
            (&retained.pre_trade[3], &pristine.pre_trade[3])
        {
            s.qualify_isolated_restoration(p).unwrap();
            assert!(s.qualify_isolated_restoration(s).is_err());
        } else {
            panic!("market role missing");
        }
    }
}
