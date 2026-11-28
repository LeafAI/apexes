//! F04.02: causal elapsed-time decision gates for one instrument's strategy state.
use apexes_core::{BarInterval, MarketEvent};
use chrono::{DateTime, TimeDelta, Utc};
use std::collections::HashMap;

const MAX_COOLDOWNS: usize = 32;
pub const MAX_COOLDOWN_MS: u64 = 365 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TimeGateError {
    #[error("observation precedes event availability or bar interval is invalid")]
    InvalidObservation,
    #[error("strategy observation time regressed")]
    RegressingObservation,
    #[error("invalid cooldown name or duration")]
    InvalidCooldown,
    #[error("cooldown name capacity exceeded")]
    Capacity,
}

/// Engine-supplied clock with the event's earliest causal availability.
#[derive(Clone, Copy)]
pub struct TimeObservation {
    at: DateTime<Utc>,
    bar: Option<(BarInterval, DateTime<Utc>, DateTime<Utc>)>,
}

impl TimeObservation {
    pub fn new(event: &MarketEvent, now: DateTime<Utc>) -> Result<Self, TimeGateError> {
        let (available_at, bar) = match event {
            MarketEvent::Bar(b) if b.close_time > b.open_time => {
                (b.close_time, Some((b.interval, b.open_time, b.close_time)))
            }
            MarketEvent::Bar(_) => return Err(TimeGateError::InvalidObservation),
            MarketEvent::Tick(t) => (t.timestamp, None),
            MarketEvent::Depth(d) => (d.timestamp, None),
            MarketEvent::Trade(t) => (t.timestamp, None),
            MarketEvent::MarkPrice(m) => (m.timestamp, None),
        };
        if now < available_at {
            return Err(TimeGateError::InvalidObservation);
        }
        Ok(Self { at: now, bar })
    }

    pub fn is_bar(self) -> bool {
        self.bar.is_some()
    }

    pub fn at(self) -> DateTime<Utc> {
        self.at
    }
}

/// Own one instance per venue/symbol, e.g. using SymbolState<TimeState>.
/// Grants are not exchange acknowledgments. All times come from the caller.
#[derive(Clone, Default)]
pub struct TimeState {
    last_observed: Option<DateTime<Utc>>,
    last_grants: HashMap<String, DateTime<Utc>>,
    bar_ends: HashMap<BarInterval, DateTime<Utc>>,
}

/// Ordered wire representation; duplicate names/intervals are rejected.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TimeCheckpoint {
    last_observed: Option<DateTime<Utc>>,
    last_grants: Vec<(String, DateTime<Utc>)>,
    bar_ends: Vec<(BarInterval, DateTime<Utc>)>,
}

impl TimeCheckpoint {
    pub(crate) fn restore(
        self,
        through: DateTime<Utc>,
    ) -> Result<TimeState, crate::checkpoint::CheckpointError> {
        use crate::checkpoint::CheckpointError::Payload;
        if self.last_grants.len() > MAX_COOLDOWNS
            || self.bar_ends.len() > 6
            || !self.last_grants.windows(2).all(|w| w[0].0 < w[1].0)
            || !self
                .bar_ends
                .windows(2)
                .all(|w| format!("{:?}", w[0].0) < format!("{:?}", w[1].0))
            || self.last_observed.is_some_and(|at| at > through)
        {
            return Err(Payload);
        }
        let valid_at = |at| self.last_observed.is_some_and(|last| at <= last);
        let mut last_grants = HashMap::new();
        let mut bar_ends = HashMap::new();
        for (name, at) in self.last_grants {
            if name.is_empty()
                || name.len() > 64
                || !valid_at(at)
                || last_grants.insert(name, at).is_some()
            {
                return Err(Payload);
            }
        }
        for (interval, at) in self.bar_ends {
            if !valid_at(at) || bar_ends.insert(interval, at).is_some() {
                return Err(Payload);
            }
        }
        if self.last_observed.is_some() && last_grants.is_empty() && bar_ends.is_empty() {
            return Err(Payload);
        }
        Ok(TimeState {
            last_observed: self.last_observed,
            last_grants,
            bar_ends,
        })
    }
}

impl TimeState {
    pub(crate) fn checkpoint(&self) -> TimeCheckpoint {
        let mut last_grants: Vec<_> = self
            .last_grants
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        last_grants.sort_by(|a, b| a.0.cmp(&b.0));
        let mut bar_ends: Vec<_> = self.bar_ends.iter().map(|(k, v)| (*k, *v)).collect();
        bar_ends.sort_by_key(|(interval, _)| format!("{interval:?}"));
        TimeCheckpoint {
            last_observed: self.last_observed,
            last_grants,
            bar_ends,
        }
    }

    fn check(&self, observation: TimeObservation) -> Result<(), TimeGateError> {
        if self.last_observed.is_some_and(|last| observation.at < last) {
            return Err(TimeGateError::RegressingObservation);
        }
        Ok(())
    }

    /// First request grants immediately; subsequent grants require elapsed
    /// time >= duration. Refused/errored grants never move last_grants.
    pub fn try_cooldown(
        &mut self,
        observation: TimeObservation,
        name: &str,
        duration_ms: u64,
    ) -> Result<bool, TimeGateError> {
        self.check(observation)?;
        if name.is_empty() || name.len() > 64 || !(1..=MAX_COOLDOWN_MS).contains(&duration_ms) {
            return Err(TimeGateError::InvalidCooldown);
        }
        let duration = TimeDelta::milliseconds(duration_ms as i64);
        if let Some(last) = self.last_grants.get(name) {
            if observation.at.signed_duration_since(*last) < duration {
                self.last_observed = Some(observation.at);
                return Ok(false);
            }
        } else if self.last_grants.len() >= MAX_COOLDOWNS {
            return Err(TimeGateError::Capacity);
        }
        self.last_grants.insert(name.to_owned(), observation.at);
        self.last_observed = Some(observation.at);
        Ok(true)
    }

    /// Grant one non-overlapping closed bar per interval. Old/duplicate
    /// bars are ignored; other event types do not consume a bar.
    pub fn accept_closed_bar(
        &mut self,
        observation: TimeObservation,
    ) -> Result<bool, TimeGateError> {
        self.check(observation)?;
        let Some((interval, open, close)) = observation.bar else {
            return Ok(false);
        };
        self.last_observed = Some(observation.at);
        if self.bar_ends.get(&interval).is_some_and(|end| open < *end) {
            return Ok(false);
        }
        self.bar_ends.insert(interval, close);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{Bar, TradeEvent, Venue};
    use rust_decimal::Decimal;

    fn at(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ms).unwrap()
    }
    fn trade(at: DateTime<Utc>) -> MarketEvent {
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: Decimal::ONE,
            quantity: Decimal::ONE,
            side: None,
            trade_id: None,
            timestamp: at,
        })
    }
    fn bar(open: i64, close: i64, interval: BarInterval) -> MarketEvent {
        MarketEvent::Bar(Bar {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: Decimal::ONE,
            interval,
            open_time: at(open),
            close_time: at(close),
        })
    }
    fn observation(ms: i64) -> TimeObservation {
        let now = at(ms);
        TimeObservation::new(&trade(now), now).unwrap()
    }

    #[test]
    fn cooldown_depends_on_exact_elapsed_time_and_not_event_count() {
        for dense in [false, true] {
            let mut state = TimeState::default();
            assert!(state.try_cooldown(observation(0), "entry", 1000).unwrap());
            if dense {
                for ms in 1..999 {
                    assert!(!state.try_cooldown(observation(ms), "entry", 1000).unwrap());
                }
            }
            let before = at(1000) - TimeDelta::nanoseconds(1);
            assert!(
                !state
                    .try_cooldown(
                        TimeObservation::new(&trade(before), before).unwrap(),
                        "entry",
                        1000
                    )
                    .unwrap()
            );
            assert!(
                state
                    .try_cooldown(observation(1000), "entry", 1000)
                    .unwrap()
            );
            assert!(
                !state
                    .try_cooldown(observation(1000), "entry", 1000)
                    .unwrap()
            );
            assert_eq!(
                state.try_cooldown(observation(999), "entry", 1000),
                Err(TimeGateError::RegressingObservation)
            );
            assert!(
                state
                    .try_cooldown(observation(2000), "entry", 1000)
                    .unwrap()
            );
        }
    }

    #[test]
    fn closed_bar_gate_rejects_future_and_overlapping_bars_and_separates_intervals() {
        let mut state = TimeState::default();
        let b = bar(0, 1000, BarInterval::OneSecond);
        assert!(matches!(
            TimeObservation::new(&b, at(999)),
            Err(TimeGateError::InvalidObservation)
        ));
        let o = TimeObservation::new(&b, at(1000)).unwrap();
        assert!(state.accept_closed_bar(o).unwrap());
        assert!(!state.accept_closed_bar(o).unwrap());
        let overlap = bar(500, 1500, BarInterval::OneSecond);
        assert!(
            !state
                .accept_closed_bar(TimeObservation::new(&overlap, at(1500)).unwrap())
                .unwrap()
        );
        let next = bar(1000, 2000, BarInterval::OneSecond);
        assert!(
            state
                .accept_closed_bar(TimeObservation::new(&next, at(2000)).unwrap())
                .unwrap()
        );
        let minute = bar(0, 60000, BarInterval::OneMinute);
        assert!(
            state
                .accept_closed_bar(TimeObservation::new(&minute, at(60000)).unwrap())
                .unwrap()
        );
        assert!(matches!(
            TimeObservation::new(&bar(1, 1, BarInterval::OneSecond), at(1)),
            Err(TimeGateError::InvalidObservation)
        ));
        assert_eq!(
            state.accept_closed_bar(o),
            Err(TimeGateError::RegressingObservation)
        );
    }

    #[test]
    fn bounded_named_cooldowns_never_evict_existing_grants() {
        let mut state = TimeState::default();
        for i in 0..MAX_COOLDOWNS {
            assert!(
                state
                    .try_cooldown(observation(0), &format!("gate{i}"), 1000)
                    .unwrap()
            );
        }
        assert_eq!(
            state.try_cooldown(observation(0), "extra", 1000),
            Err(TimeGateError::Capacity)
        );
        assert_eq!(
            state.try_cooldown(observation(0), "gate0", 0),
            Err(TimeGateError::InvalidCooldown)
        );
        assert!(!state.try_cooldown(observation(999), "gate0", 1000).unwrap());
        assert!(
            state
                .try_cooldown(observation(1000), "gate0", 1000)
                .unwrap()
        );
    }
}
