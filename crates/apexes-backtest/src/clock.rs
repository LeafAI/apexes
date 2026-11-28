use apexes_core::{ApexesError, MarketEvent};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Controls the pacing of backtest replay. Logical time for all
/// historical strategy/risk calculations derives from event
/// timestamps regardless of `ReplaySpeed` (design spec §8) -- this clock
/// only controls whether/how long to artificially wait between
/// dispatching events, purely for demo/visual-inspection purposes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReplaySpeed {
    /// Process events back-to-back with zero artificial delay (default).
    AsFastAsPossible,
    /// Insert real delays scaled by this multiplier (e.g. `10.0` = 10x
    /// real-time; `0.5` = half real-time/slower).
    Multiplier(f64),
    /// 1:1 wall-clock pacing (equivalent to `Multiplier(1.0)`).
    RealTime,
}

/// Optional live-only decision clock. Historical and simulated paper
/// clocks must not implicitly inherit local wall-clock arrival times.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveObservationPolicy {
    ConsumerUtc,
}

/// Paces backtest replay according to a `ReplaySpeed`. Tracks the
/// timestamp of the previously-dispatched event to compute how long to
/// wait before the next one, for `Multiplier`/`RealTime` modes.
pub struct BacktestClock {
    speed: ReplaySpeed,
    last_event_timestamp: Option<DateTime<Utc>>,
    live_source: Option<Box<dyn FnMut() -> DateTime<Utc> + Send>>,
    last_observation: Option<DateTime<Utc>>,
}

impl BacktestClock {
    pub fn new(speed: ReplaySpeed) -> Self {
        Self {
            speed,
            last_event_timestamp: None,
            live_source: None,
            last_observation: None,
        }
    }

    /// Live-only clock; uses consumer dequeue UTC, not exchange time.
    pub fn live_observations(policy: LiveObservationPolicy) -> Self {
        match policy {
            LiveObservationPolicy::ConsumerUtc => Self::with_observation_source(Utc::now),
        }
    }

    pub(crate) fn with_observation_source(
        source: impl FnMut() -> DateTime<Utc> + Send + 'static,
    ) -> Self {
        let mut clock = Self::new(ReplaySpeed::AsFastAsPossible);
        clock.live_source = Some(Box::new(source));
        clock
    }

    pub(crate) fn observation_receipt_clock(&self) -> crate::observation_receipt::ObservationClock {
        use crate::observation_receipt::ObservationClock;
        if self.live_source.is_some() {
            ObservationClock::ConsumerUtc
        } else {
            ObservationClock::SourceAvailability
        }
    }

    pub(crate) fn logical_timestamp(
        &mut self,
        event: &MarketEvent,
        causal: bool,
    ) -> Result<DateTime<Utc>, ApexesError> {
        if let Some(source) = &mut self.live_source {
            let now = source();
            let available = crate::market_data_source::observation_timestamp(event);
            if matches!(event,MarketEvent::Bar(b) if b.close_time <= b.open_time)
                || now < available
                || self.last_observation.is_some_and(|last| now < last)
            {
                return Err(ApexesError::InvalidBacktestConfig {
                    reason: format!(
                        "invalid live observation clock for {}: consumer {now}, source availability {available}, previous {:?}",
                        event.symbol(),
                        self.last_observation
                    ),
                });
            }
            self.last_observation = Some(now);
            return Ok(now);
        }
        Ok(if causal {
            crate::market_data_source::observation_timestamp(event)
        } else {
            crate::market_data_source::event_timestamp(event)
        })
    }

    /// Waits (if `speed` requires it) before the caller dispatches the
    /// event with `event_timestamp`. For `AsFastAsPossible`, returns
    /// immediately. For `Multiplier`/`RealTime`, sleeps for the
    /// wall-clock-scaled gap since the previously-dispatched event's
    /// timestamp (zero on the very first call, or if timestamps go
    /// backward/are identical -- never sleeps a negative duration).
    pub async fn wait_for_next_event(&mut self, event_timestamp: DateTime<Utc>) {
        if let Some(last) = self.last_event_timestamp {
            let gap = event_timestamp.signed_duration_since(last);
            if gap.num_milliseconds() > 0 {
                let multiplier = match self.speed {
                    ReplaySpeed::AsFastAsPossible => None,
                    ReplaySpeed::Multiplier(m) if m > 0.0 => Some(m),
                    ReplaySpeed::Multiplier(_) => None, // non-positive multiplier: treat as AsFastAsPossible
                    ReplaySpeed::RealTime => Some(1.0),
                };
                if let Some(multiplier) = multiplier {
                    let scaled_millis = (gap.num_milliseconds() as f64) / multiplier;
                    if scaled_millis > 0.0 {
                        tokio::time::sleep(std::time::Duration::from_millis(scaled_millis as u64))
                            .await;
                    }
                }
            }
        }
        self.last_event_timestamp = Some(event_timestamp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::time::Instant;

    fn time_at(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, hour, minute, second)
            .unwrap()
    }

    #[tokio::test]
    async fn as_fast_as_possible_never_sleeps() {
        let mut clock = BacktestClock::new(ReplaySpeed::AsFastAsPossible);
        let start = Instant::now();

        clock.wait_for_next_event(time_at(10, 0, 0)).await;
        clock.wait_for_next_event(time_at(10, 5, 0)).await; // 5 minutes later in event-time

        // No real sleep should have occurred despite a 5-minute event-time gap.
        assert!(start.elapsed().as_millis() < 100);
    }

    #[tokio::test]
    async fn first_call_never_sleeps_regardless_of_speed() {
        let mut clock = BacktestClock::new(ReplaySpeed::RealTime);
        let start = Instant::now();

        clock.wait_for_next_event(time_at(10, 0, 0)).await;

        assert!(start.elapsed().as_millis() < 100);
    }

    #[tokio::test]
    async fn multiplier_scales_the_wait_duration() {
        // 100x speed: a 1-second event-time gap becomes a 10ms real wait.
        let mut clock = BacktestClock::new(ReplaySpeed::Multiplier(100.0));

        clock.wait_for_next_event(time_at(10, 0, 0)).await;
        let start = Instant::now();
        clock.wait_for_next_event(time_at(10, 0, 1)).await; // 1 second later in event-time
        let elapsed = start.elapsed();

        // Should be roughly 10ms (1000ms / 100), allow generous scheduling slack.
        assert!(
            elapsed.as_millis() >= 5,
            "expected some sleep to occur, got {elapsed:?}"
        );
        assert!(
            elapsed.as_millis() < 200,
            "expected a short sleep, got {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn backward_or_identical_timestamps_never_sleep() {
        let mut clock = BacktestClock::new(ReplaySpeed::RealTime);
        clock.wait_for_next_event(time_at(10, 0, 5)).await;

        let start = Instant::now();
        clock.wait_for_next_event(time_at(10, 0, 0)).await; // earlier than the previous event
        clock.wait_for_next_event(time_at(10, 0, 0)).await; // identical timestamp
        let elapsed = start.elapsed();

        assert!(elapsed.as_millis() < 100);
    }

    #[tokio::test]
    async fn non_positive_multiplier_is_treated_as_as_fast_as_possible() {
        let mut clock = BacktestClock::new(ReplaySpeed::Multiplier(0.0));
        let start = Instant::now();

        clock.wait_for_next_event(time_at(10, 0, 0)).await;
        clock.wait_for_next_event(time_at(10, 0, 10)).await; // 10 second event-time gap

        assert!(start.elapsed().as_millis() < 100);
    }
    #[test]
    fn consumer_clock_rejects_future_and_regressing_samples_without_rewriting_source() {
        use apexes_core::{Bar, BarInterval, Venue};
        use rust_decimal::Decimal;
        let start = time_at(10, 0, 0);
        let close = start + chrono::TimeDelta::minutes(1);
        let bar = MarketEvent::Bar(Bar {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: Decimal::ONE,
            interval: BarInterval::OneMinute,
            open_time: start,
            close_time: close,
        });
        let samples = [
            close - chrono::TimeDelta::nanoseconds(1),
            close + chrono::TimeDelta::seconds(5),
            close + chrono::TimeDelta::seconds(4),
            close + chrono::TimeDelta::seconds(5),
        ];
        let mut samples = samples.into_iter();
        let mut clock = BacktestClock::with_observation_source(move || samples.next().unwrap());
        assert!(clock.logical_timestamp(&bar, false).is_err());
        assert_eq!(
            clock.logical_timestamp(&bar, false).unwrap(),
            close + chrono::TimeDelta::seconds(5)
        );
        assert!(clock.logical_timestamp(&bar, false).is_err());
        assert_eq!(
            clock.logical_timestamp(&bar, false).unwrap(),
            close + chrono::TimeDelta::seconds(5)
        );
        assert_eq!(
            BacktestClock::new(ReplaySpeed::AsFastAsPossible)
                .logical_timestamp(&bar, false)
                .unwrap(),
            start
        );
        assert_eq!(
            BacktestClock::new(ReplaySpeed::AsFastAsPossible)
                .logical_timestamp(&bar, true)
                .unwrap(),
            close
        );
    }
}
