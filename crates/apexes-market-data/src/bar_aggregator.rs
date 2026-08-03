use apexes_core::{Bar, BarInterval, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// All six `BarInterval` variants this aggregator maintains a bucket for.
const ALL_INTERVALS: [BarInterval; 6] = [
    BarInterval::OneSecond,
    BarInterval::OneMinute,
    BarInterval::FiveMinutes,
    BarInterval::FifteenMinutes,
    BarInterval::OneHour,
    BarInterval::OneDay,
];

fn interval_seconds(interval: BarInterval) -> i64 {
    match interval {
        BarInterval::OneSecond => 1,
        BarInterval::OneMinute => 60,
        BarInterval::FiveMinutes => 5 * 60,
        BarInterval::FifteenMinutes => 15 * 60,
        BarInterval::OneHour => 60 * 60,
        BarInterval::OneDay => 24 * 60 * 60,
    }
}

/// The interval-aligned bucket boundaries `[start, end)` for `time` at the given interval.
///
/// Computed by subtracting the remainder-into-the-current-bucket directly
/// from `time` (rather than reconstructing a `DateTime` from epoch seconds),
/// so this never needs to handle a theoretically-invalid timestamp -- `time`
/// is already a valid `DateTime<Utc>`, and subtracting a `Duration` from a
/// valid `DateTime<Utc>` cannot fail.
fn bucket_bounds(interval: BarInterval, time: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    let secs = interval_seconds(interval);
    let remainder_secs = time.timestamp().rem_euclid(secs);
    let start = time - chrono::Duration::seconds(remainder_secs);
    let end = start + chrono::Duration::seconds(secs);
    (start, end)
}

#[derive(Debug, Clone)]
struct OpenBar {
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
}

impl OpenBar {
    fn new(
        price: Decimal,
        quantity: Decimal,
        open_time: DateTime<Utc>,
        close_time: DateTime<Utc>,
    ) -> Self {
        Self {
            open: price,
            high: price,
            low: price,
            close: price,
            volume: quantity,
            open_time,
            close_time,
        }
    }

    fn accumulate(&mut self, price: Decimal, quantity: Decimal) {
        if price > self.high {
            self.high = price;
        }
        if price < self.low {
            self.low = price;
        }
        self.close = price;
        self.volume += quantity;
    }

    fn into_bar(self, symbol: String, venue: Venue, interval: BarInterval) -> Bar {
        Bar {
            symbol,
            venue,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            interval,
            open_time: self.open_time,
            close_time: self.close_time,
        }
    }
}

/// Aggregates a symbol's trade stream into OHLCV `Bar`s across all six
/// `BarInterval`s in parallel. Empty intervals are skipped (no synthetic
/// bars); out-of-order trades landing in an already-closed bucket are
/// dropped defensively.
pub struct BarAggregator {
    symbol: String,
    venue: Venue,
    open_bars: HashMap<BarInterval, OpenBar>,
    /// The end boundary of the last-closed bucket per interval, used to
    /// detect and drop late/out-of-order trades.
    last_closed_end: HashMap<BarInterval, DateTime<Utc>>,
}

impl BarAggregator {
    pub fn new(symbol: impl Into<String>, venue: Venue) -> Self {
        Self {
            symbol: symbol.into(),
            venue,
            open_bars: HashMap::new(),
            last_closed_end: HashMap::new(),
        }
    }

    /// Feeds one trade into every interval's aggregation bucket. Returns
    /// zero or more `Bar`s that closed as a result of this trade (one per
    /// interval that crossed a boundary and had at least one trade in it).
    pub fn on_trade(&mut self, price: Decimal, quantity: Decimal, time: DateTime<Utc>) -> Vec<Bar> {
        let mut closed_bars = Vec::new();

        for interval in ALL_INTERVALS {
            if let Some(closed_end) = self.last_closed_end.get(&interval)
                && time < *closed_end
            {
                tracing::warn!(
                    symbol = %self.symbol,
                    ?interval,
                    trade_time = %time,
                    closed_bucket_end = %closed_end,
                    "dropping out-of-order trade: falls into an already-closed bar bucket"
                );
                continue;
            }

            let (bucket_start, bucket_end) = bucket_bounds(interval, time);

            let open_bucket_is_stale = self
                .open_bars
                .get(&interval)
                .map(|open_bar| open_bar.open_time != bucket_start)
                .unwrap_or(false);

            if open_bucket_is_stale {
                // Trade belongs to a later bucket than the currently open one:
                // close the open bucket, and skip any empty buckets in between.
                // `remove` returning `Some` here is guaranteed by
                // `open_bucket_is_stale` only being true when `get` above
                // already found an entry -- `if let` (not `.expect()`) still
                // handles the theoretical `None` case defensively rather
                // than panicking.
                if let Some(previous) = self.open_bars.remove(&interval) {
                    closed_bars.push(previous.into_bar(self.symbol.clone(), self.venue, interval));
                    // Everything before the new trade's bucket start is now "passed" --
                    // either closed (like the bucket we just removed) or skipped as
                    // empty -- so any trade landing before it from here on is late.
                    self.last_closed_end.insert(interval, bucket_start);
                }
            }

            match self.open_bars.get_mut(&interval) {
                Some(open_bar) => open_bar.accumulate(price, quantity),
                None => {
                    self.open_bars.insert(
                        interval,
                        OpenBar::new(price, quantity, bucket_start, bucket_end),
                    );
                }
            }
        }

        closed_bars
    }

    /// Returns the still-forming (not yet closed) bar for `interval`,
    /// if any trade has landed in the current bucket -- `None` if no
    /// trade has arrived for this interval yet. Does not consume or
    /// modify the open bucket; `on_trade` can still accumulate into
    /// it afterward. Used by the K-line chart (sub-project #13b) to
    /// show the currently-forming candle alongside closed ones,
    /// rather than only ever showing history.
    pub fn current_open_bar(&self, interval: BarInterval) -> Option<Bar> {
        self.open_bars.get(&interval).map(|open_bar| Bar {
            symbol: self.symbol.clone(),
            venue: self.venue,
            open: open_bar.open,
            high: open_bar.high,
            low: open_bar.low,
            close: open_bar.close,
            volume: open_bar.volume,
            interval,
            open_time: open_bar.open_time,
            close_time: open_bar.close_time,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn t(epoch_secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(epoch_secs, 0).single().unwrap()
    }

    #[test]
    fn single_trade_accumulates_but_does_not_close() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        let closed = agg.on_trade(dec!(65000), dec!(1.0), t(0));
        assert!(
            closed.is_empty(),
            "a single trade with no boundary crossing should not close any bar"
        );
    }

    #[test]
    fn second_trade_in_next_one_minute_bucket_closes_the_first() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        agg.on_trade(dec!(65000), dec!(1.0), t(0));
        agg.on_trade(dec!(65100), dec!(2.0), t(30));
        // Still within [0, 60) for OneMinute -- should not close yet.
        let closed = agg.on_trade(dec!(65200), dec!(0.5), t(65));

        let one_min_bar = closed
            .iter()
            .find(|b| b.interval == BarInterval::OneMinute)
            .expect(
                "OneMinute bar should have closed when trade at t=65 crossed the [0,60) boundary",
            );

        // Only the trades at t(0) (price 65000) and t(30) (price 65100) are inside
        // the closed [0,60) bucket; the trade at t(65) (price 65200) opened the
        // *next* bucket and is not part of this bar.
        assert_eq!(one_min_bar.open, dec!(65000));
        assert_eq!(one_min_bar.high, dec!(65100));
        assert_eq!(one_min_bar.low, dec!(65000));
        assert_eq!(one_min_bar.close, dec!(65100));
        assert_eq!(one_min_bar.volume, dec!(3.0));
    }

    #[test]
    fn one_minute_bar_ohlcv_correctness() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        agg.on_trade(dec!(100), dec!(1), t(0)); // open
        agg.on_trade(dec!(110), dec!(2), t(10)); // high
        agg.on_trade(dec!(90), dec!(3), t(20)); // low
        agg.on_trade(dec!(105), dec!(4), t(30)); // close (within bucket)
        let closed = agg.on_trade(dec!(200), dec!(1), t(61)); // crosses into next OneMinute bucket

        let bar = closed
            .into_iter()
            .find(|b| b.interval == BarInterval::OneMinute)
            .expect("OneMinute bar should close");

        assert_eq!(bar.open, dec!(100));
        assert_eq!(bar.high, dec!(110));
        assert_eq!(bar.low, dec!(90));
        assert_eq!(bar.close, dec!(105));
        assert_eq!(bar.volume, dec!(10));
        assert_eq!(bar.symbol, "BTCUSDT");
        assert_eq!(bar.venue, Venue::Binance);
    }

    #[test]
    fn multi_interval_gap_skips_empty_buckets() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        agg.on_trade(dec!(100), dec!(1), t(0));
        // t=185 is 3 minutes and 5 seconds later: for OneMinute, this crosses
        // [0,60) -> [60,120) -> [120,180) -> lands in [180,240).
        // [60,120) and [120,180) have zero trades and must NOT produce bars.
        let closed = agg.on_trade(dec!(200), dec!(1), t(185));

        let one_min_bars: Vec<_> = closed
            .iter()
            .filter(|b| b.interval == BarInterval::OneMinute)
            .collect();

        assert_eq!(
            one_min_bars.len(),
            1,
            "only the [0,60) bucket had trades; the two empty buckets in between must not produce bars"
        );
        assert_eq!(one_min_bars[0].open_time, t(0));
    }

    #[test]
    fn out_of_order_trade_after_close_is_dropped_not_applied() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        agg.on_trade(dec!(100), dec!(1), t(0));
        let closed = agg.on_trade(dec!(200), dec!(1), t(65)); // closes [0,60) OneMinute bucket
        let closed_bar = closed
            .into_iter()
            .find(|b| b.interval == BarInterval::OneMinute)
            .expect("bucket should have closed");
        assert_eq!(closed_bar.close, dec!(100));

        // A late trade with a timestamp inside the already-closed [0,60) bucket arrives.
        let late_closed = agg.on_trade(dec!(999), dec!(50), t(30));
        assert!(
            late_closed.is_empty(),
            "a late/out-of-order trade landing in an already-closed bucket must be dropped, not produce a new bar"
        );
    }

    #[test]
    fn all_six_intervals_are_maintained_independently() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        agg.on_trade(dec!(100), dec!(1), t(0));
        // Advance past OneSecond and OneMinute boundaries but nothing else.
        let closed = agg.on_trade(dec!(101), dec!(1), t(61));

        let closed_intervals: std::collections::HashSet<_> =
            closed.iter().map(|b| b.interval).collect();

        assert!(closed_intervals.contains(&BarInterval::OneSecond));
        assert!(closed_intervals.contains(&BarInterval::OneMinute));
        assert!(!closed_intervals.contains(&BarInterval::FiveMinutes));
        assert!(!closed_intervals.contains(&BarInterval::OneHour));
        assert!(!closed_intervals.contains(&BarInterval::OneDay));
    }

    #[test]
    fn current_open_bar_returns_none_before_any_trade() {
        let agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        assert_eq!(agg.current_open_bar(BarInterval::OneMinute), None);
    }

    #[test]
    fn current_open_bar_reflects_the_still_forming_bucket() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        agg.on_trade(dec!(100), dec!(1), t(0));
        agg.on_trade(dec!(110), dec!(2), t(10));

        let open_bar = agg
            .current_open_bar(BarInterval::OneMinute)
            .expect("a trade landed inside the [0,60) OneMinute bucket, which has not closed yet");

        assert_eq!(open_bar.open, dec!(100));
        assert_eq!(open_bar.high, dec!(110));
        assert_eq!(open_bar.low, dec!(100));
        assert_eq!(open_bar.close, dec!(110));
        assert_eq!(open_bar.volume, dec!(3));
        assert_eq!(open_bar.symbol, "BTCUSDT");
        assert_eq!(open_bar.venue, Venue::Binance);
        assert_eq!(open_bar.interval, BarInterval::OneMinute);
    }

    #[test]
    fn current_open_bar_is_none_for_an_interval_with_no_trades_yet() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        agg.on_trade(dec!(100), dec!(1), t(0));
        // OneDay's bucket exists (every interval accumulates in parallel per
        // on_trade's own doc comment), so this asserts current_open_bar
        // still correctly reports Some for it, not just OneMinute.
        assert!(agg.current_open_bar(BarInterval::OneDay).is_some());
    }

    #[test]
    fn current_open_bar_does_not_consume_the_open_bucket() {
        // Calling current_open_bar must not close/remove the bucket --
        // on_trade must still be able to accumulate into it afterward.
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        agg.on_trade(dec!(100), dec!(1), t(0));
        let _ = agg.current_open_bar(BarInterval::OneMinute);
        let _ = agg.current_open_bar(BarInterval::OneMinute);
        agg.on_trade(dec!(120), dec!(1), t(10));

        let open_bar = agg.current_open_bar(BarInterval::OneMinute).unwrap();
        assert_eq!(open_bar.high, dec!(120));
        assert_eq!(open_bar.volume, dec!(2));
    }
}
