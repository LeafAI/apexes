//! [`LatencyModel`]: simulates network round-trip latency between a
//! strategy deciding to submit an order and the (simulated) exchange
//! receiving it. Defined in `apexes-backtest` (not `apexes-paper`) to
//! avoid a circular crate dependency, since `run_unified_event_loop` (this
//! crate) needs to accept an `Option<LatencyModel>` parameter and
//! `apexes-paper` depends on `apexes-backtest`, not the reverse.
//! `apexes-paper` re-exports this type via `pub use
//! apexes_backtest::LatencyModel;` so it still reads naturally as a
//! paper-trading concept to that crate's users.
//!
//! Historical replay (`run_backtest`) has no use for this -- recorded
//! event timestamps are already fixed, so `run_backtest` always passes
//! `None` to `run_unified_event_loop`'s `latency_model` parameter.

use rand::RngExt;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub enum LatencyModel {
    /// No artificial delay -- the order reaches the matching engine as
    /// soon as it passes any configured `PreTradeCheck`.
    None,
    Fixed(Duration),
    /// Deterministic event-time arrival, scheduled by the unified loop.
    EventTime(crate::EventLatencyPolicy),
    /// Uniformly distributed random delay in `[min, max]` (inclusive).
    Uniform {
        min: Duration,
        max: Duration,
    },
}

impl LatencyModel {
    /// Returns the delay to apply for one order submission. `Uniform`
    /// draws a new random value on every call (not cached), so
    /// consecutive orders in the same run get independent delays,
    /// simulating real network jitter.
    pub fn sample(&self) -> Duration {
        match self {
            LatencyModel::None | LatencyModel::EventTime(_) => Duration::ZERO,
            LatencyModel::Fixed(duration) => *duration,
            LatencyModel::Uniform { min, max } => {
                if min >= max {
                    return *min;
                }
                let min_nanos = min.as_nanos() as u64;
                let max_nanos = max.as_nanos() as u64;
                let sampled_nanos = rand::rng().random_range(min_nanos..=max_nanos);
                Duration::from_nanos(sampled_nanos)
            }
        }
    }

    /// Actually delays execution by `self.sample()`'s result via
    /// `tokio::time::sleep` -- a real wall-clock delay, not just a
    /// timestamp adjustment, to most faithfully simulate real network
    /// latency's effect on timing.
    pub async fn apply(&self) {
        let delay = self.sample();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_samples_zero_duration() {
        let model = LatencyModel::None;
        assert_eq!(model.sample(), Duration::ZERO);
    }

    #[test]
    fn fixed_always_samples_the_configured_value() {
        let model = LatencyModel::Fixed(Duration::from_millis(50));
        for _ in 0..5 {
            assert_eq!(model.sample(), Duration::from_millis(50));
        }
    }

    #[test]
    fn uniform_samples_fall_within_the_configured_range() {
        let model = LatencyModel::Uniform {
            min: Duration::from_millis(10),
            max: Duration::from_millis(20),
        };
        for _ in 0..100 {
            let sampled = model.sample();
            assert!(sampled >= Duration::from_millis(10));
            assert!(sampled <= Duration::from_millis(20));
        }
    }

    #[test]
    fn uniform_with_equal_min_and_max_always_returns_that_value() {
        let model = LatencyModel::Uniform {
            min: Duration::from_millis(5),
            max: Duration::from_millis(5),
        };
        assert_eq!(model.sample(), Duration::from_millis(5));
    }

    #[tokio::test]
    async fn apply_with_none_returns_almost_immediately() {
        let model = LatencyModel::None;
        let start = tokio::time::Instant::now();
        model.apply().await;
        assert!(start.elapsed() < Duration::from_millis(20));
    }

    #[tokio::test]
    async fn apply_with_fixed_delay_actually_sleeps() {
        let model = LatencyModel::Fixed(Duration::from_millis(30));
        let start = tokio::time::Instant::now();
        model.apply().await;
        assert!(start.elapsed() >= Duration::from_millis(30));
    }
}
