//! Bounded time series for what a snapshot charts: the equity curve and each
//! symbol's price. A series never grows past [`HISTORY_CAP`] raw points however
//! long the session runs, and never sends more than a requested number.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

/// Raw points kept before a series is halved.
pub(crate) const HISTORY_CAP: usize = 4096;

pub type Point = (DateTime<Utc>, Decimal);

/// A series that stays bounded by halving: when [`HISTORY_CAP`] is reached
/// every other point is dropped and the sampling stride doubles, so early and
/// recent history stay evenly represented.
#[derive(Debug, Clone)]
pub(crate) struct BoundedSeries {
    pub(crate) points: Vec<Point>,
    stride: u64,
    seen: u64,
    latest: Option<Point>,
}

impl BoundedSeries {
    pub(crate) fn new() -> Self {
        Self {
            points: Vec::new(),
            stride: 1,
            seen: 0,
            latest: None,
        }
    }

    pub(crate) fn record(&mut self, at: DateTime<Utc>, value: Decimal) {
        self.latest = Some((at, value));
        if self.seen.is_multiple_of(self.stride) {
            self.points.push((at, value));
            if self.points.len() >= HISTORY_CAP {
                self.points = self.points.iter().step_by(2).copied().collect();
                self.stride *= 2;
            }
        }
        self.seen += 1;
    }

    /// At most `max` points, evenly thinned, always ending on the latest.
    pub(crate) fn view(&self, max: usize) -> Vec<Point> {
        let mut out: Vec<_> = if self.points.len() <= max || max < 2 {
            self.points.clone()
        } else {
            let step = self.points.len().div_ceil(max);
            self.points.iter().step_by(step).copied().collect()
        };
        if let Some(latest) = self.latest
            && out.last() != Some(&latest)
        {
            out.push(latest);
        }
        out
    }
}

/// The last-trade price history of each symbol of a session.
#[derive(Debug, Clone, Default)]
pub struct PriceHistory {
    series: BTreeMap<String, BoundedSeries>,
}

impl PriceHistory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, symbol: &str, at: DateTime<Utc>, price: Decimal) {
        self.series
            .entry(symbol.to_string())
            .or_insert_with(BoundedSeries::new)
            .record(at, price);
    }

    /// Each symbol's history, at most `max_points` per symbol.
    pub fn view(&self, max_points: usize) -> std::collections::HashMap<String, Vec<Point>> {
        self.series
            .iter()
            .map(|(symbol, series)| (symbol.clone(), series.view(max_points)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    #[test]
    fn each_symbol_keeps_its_own_series() {
        let mut history = PriceHistory::new();
        history.record("BTCUSDT", at(0), dec!(65000));
        history.record("ETHUSDT", at(1), dec!(1800));
        history.record("BTCUSDT", at(2), dec!(65010));
        let view = history.view(100);
        assert_eq!(view["BTCUSDT"].len(), 2);
        assert_eq!(view["ETHUSDT"], vec![(at(1), dec!(1800))]);
    }

    #[test]
    fn a_long_session_stays_bounded_and_keeps_both_ends() {
        let mut history = PriceHistory::new();
        let n = (HISTORY_CAP * 6) as i64;
        for i in 0..n {
            history.record("BTCUSDT", at(i), Decimal::from(1000 + i));
        }
        assert!(history.series["BTCUSDT"].points.len() < HISTORY_CAP);
        let view = history.view(300);
        let btc = &view["BTCUSDT"];
        assert!(btc.len() <= 301, "{}", btc.len());
        assert_eq!(btc.first().map(|p| p.0), Some(at(0)));
        assert_eq!(btc.last(), Some(&(at(n - 1), Decimal::from(1000 + n - 1))));
        assert!(btc.windows(2).all(|w| w[0].0 < w[1].0));
    }

    #[test]
    fn no_trades_means_an_empty_view() {
        assert!(PriceHistory::new().view(100).is_empty());
    }
}
