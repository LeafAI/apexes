//! F03.05: observed equity high-water episodes and recovery durations.
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrawdownEpisode {
    pub peak_at: DateTime<Utc>,
    pub trough_at: DateTime<Utc>,
    pub peak_equity: Decimal,
    pub trough_equity: Decimal,
    pub depth: Decimal,
    pub recovered_at: Option<DateTime<Utc>>,
    pub observed_duration_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrawdownDuration {
    pub completed_episodes: u64,
    pub longest_completed_ms: Option<i64>,
    pub longest_observed_ms: i64,
    pub worst: Option<DrawdownEpisode>,
    pub current: Option<DrawdownEpisode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DrawdownEvidence {
    Available(Box<DrawdownDuration>),
    Unavailable { reason: String },
}

struct Tracker {
    peak: (DateTime<Utc>, Decimal),
    current: Option<DrawdownEpisode>,
    summary: DrawdownDuration,
}

impl Tracker {
    fn finish_episode(&mut self, episode: DrawdownEpisode) {
        self.summary.longest_observed_ms = self
            .summary
            .longest_observed_ms
            .max(episode.observed_duration_ms);
        if episode.recovered_at.is_some() {
            self.summary.completed_episodes += 1;
            self.summary.longest_completed_ms = Some(
                self.summary
                    .longest_completed_ms
                    .unwrap_or(0)
                    .max(episode.observed_duration_ms),
            );
        }
        if self
            .summary
            .worst
            .as_ref()
            .is_none_or(|worst| episode.depth > worst.depth)
        {
            self.summary.worst = Some(episode);
        }
    }

    fn observe(&mut self, at: DateTime<Utc>, equity: Decimal) -> Option<()> {
        if equity >= self.peak.1 {
            if let Some(mut episode) = self.current.take() {
                episode.recovered_at = Some(at);
                episode.observed_duration_ms = (at - episode.peak_at).num_milliseconds();
                self.finish_episode(episode);
            }
            self.peak = (at, equity);
            return Some(());
        }
        let depth = self.peak.1.checked_sub(equity)?.checked_div(self.peak.1)?;
        let episode = self.current.get_or_insert(DrawdownEpisode {
            peak_at: self.peak.0,
            trough_at: at,
            peak_equity: self.peak.1,
            trough_equity: equity,
            depth,
            recovered_at: None,
            observed_duration_ms: 0,
        });
        episode.observed_duration_ms = (at - episode.peak_at).num_milliseconds();
        if equity < episode.trough_equity {
            episode.trough_at = at;
            episode.trough_equity = equity;
            episode.depth = depth;
        }
        Some(())
    }
}

fn summarize(curve: &[(DateTime<Utc>, Decimal)]) -> Result<DrawdownDuration, &'static str> {
    let &first = curve.first().ok_or("empty equity curve")?;
    if first.1 <= Decimal::ZERO {
        return Err("initial equity must be positive");
    }
    let mut tracker = Tracker {
        peak: first,
        current: None,
        summary: DrawdownDuration {
            completed_episodes: 0,
            longest_completed_ms: None,
            longest_observed_ms: 0,
            worst: None,
            current: None,
        },
    };
    let mut previous = first.0;
    for &(at, equity) in &curve[1..] {
        if at < previous {
            return Err("equity observation time regressed");
        }
        tracker
            .observe(at, equity)
            .ok_or("drawdown arithmetic overflow")?;
        previous = at;
    }
    if let Some(episode) = tracker.current.take() {
        tracker.summary.current = Some(episode.clone());
        tracker.finish_episode(episode);
    }
    Ok(tracker.summary)
}

pub(crate) fn evaluate(curve: &[(DateTime<Utc>, Decimal)]) -> DrawdownEvidence {
    match summarize(curve) {
        Ok(summary) => DrawdownEvidence::Available(Box::new(summary)),
        Err(reason) => DrawdownEvidence::Unavailable {
            reason: reason.into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;
    use rust_decimal_macros::dec;

    fn at(ms: i64) -> DateTime<Utc> {
        DateTime::UNIX_EPOCH + TimeDelta::milliseconds(ms)
    }

    #[test]
    fn completed_and_censored_episodes_preserve_equal_timestamp_order() {
        let curve = [
            (at(0), dec!(100)),
            (at(1000), dec!(100)),
            (at(1000), dec!(90)),
            (at(2000), dec!(80)),
            (at(3000), dec!(100)),
            (at(4000), dec!(110)),
            (at(5000), dec!(99)),
            (at(8000), dec!(100)),
        ];
        let d = summarize(&curve).unwrap();
        assert_eq!(d.completed_episodes, 1);
        assert_eq!(d.longest_completed_ms, Some(2000));
        assert_eq!(d.longest_observed_ms, 4000);
        let worst = d.worst.unwrap();
        assert_eq!(worst.depth, dec!(0.2));
        assert_eq!(worst.peak_at, at(1000));
        assert_eq!(worst.trough_at, at(2000));
        assert_eq!(worst.recovered_at, Some(at(3000)));
        let current = d.current.unwrap();
        assert_eq!(current.depth, dec!(0.1));
        assert_eq!(current.observed_duration_ms, 4000);
        assert_eq!(current.recovered_at, None);
    }

    #[test]
    fn empty_nonpositive_regressing_and_flat_curves() {
        assert!(summarize(&[]).is_err());
        assert!(summarize(&[(at(0), Decimal::ZERO)]).is_err());
        assert!(summarize(&[(at(1), dec!(10)), (at(0), dec!(9))]).is_err());
        let flat = summarize(&[(at(0), dec!(10)), (at(100), dec!(10))]).unwrap();
        assert_eq!(flat.completed_episodes, 0);
        assert_eq!(flat.longest_completed_ms, None);
        assert_eq!(flat.longest_observed_ms, 0);
        assert!(flat.worst.is_none() && flat.current.is_none());
        let negative = summarize(&[(at(0), dec!(10)), (at(5), dec!(-5))]).unwrap();
        assert_eq!(negative.current.unwrap().depth, dec!(1.5));
    }
}
