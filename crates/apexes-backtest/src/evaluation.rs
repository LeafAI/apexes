//! [`EvaluationTracker`]: a bounded, incremental view of how a session is
//! doing -- PnL, drawdown, exposure, fees, turnover, time in market, and
//! how close it is to its risk limits -- for the operator console.
//!
//! It is deliberately separate from [`crate::PerformanceTracker`], which
//! keeps every equity point and fill for an end-of-run report and so grows
//! without bound. This tracker is for a session that runs for days: O(1)
//! state per update, and an equity history thinned by halving so the whole
//! session stays visible in a fixed amount of memory.

use std::collections::HashMap;

use apexes_core::Fill;
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Raw points kept before the history is halved.
const HISTORY_CAP: usize = 4096;
/// Most points ever sent in one snapshot.
pub const SNAPSHOT_HISTORY_POINTS: usize = 400;

/// One configured limit and how much of it is currently used.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitUsage {
    pub name: String,
    pub used: Decimal,
    pub limit: Decimal,
}

impl LimitUsage {
    /// `used / limit`, or zero for a non-positive limit.
    pub fn fraction(&self) -> Decimal {
        if self.limit > Decimal::ZERO {
            self.used / self.limit
        } else {
            Decimal::ZERO
        }
    }
}

/// A limit the tracker reports utilisation for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitSpec {
    /// Largest net position per symbol, in base units.
    pub max_position_per_symbol: Option<Decimal>,
    /// Fills per UTC day.
    pub max_daily_fills: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluationSummary {
    pub start_equity: Decimal,
    /// `equity - start_equity`.
    pub total_pnl: Decimal,
    pub realized_pnl: Decimal,
    /// `total_pnl - realized_pnl`: includes fees and any balance changes
    /// the ledger does not classify as realised.
    pub unrealized_pnl: Decimal,
    /// Fraction of the running peak currently lost (`0.05` = 5 %).
    pub current_drawdown: Decimal,
    pub max_drawdown: Decimal,
    /// Sum of |position value| over equity.
    pub gross_exposure_fraction: Decimal,
    /// Fees by fee currency (`""` when a fill reported none). Not summed
    /// across currencies: they are different units.
    pub fees_paid: HashMap<String, Decimal>,
    pub trade_count: u64,
    pub turnover: Decimal,
    /// Fraction of observed time holding any position.
    pub time_in_market_fraction: Decimal,
    pub limit_usage: Vec<LimitUsage>,
}

/// Equity history that stays bounded by halving: when `HISTORY_CAP` is
/// reached every other point is dropped and the sampling stride doubles, so
/// early and recent history stay evenly represented.
#[derive(Debug, Clone)]
struct EquityHistory {
    points: Vec<(DateTime<Utc>, Decimal)>,
    stride: u64,
    seen: u64,
    latest: Option<(DateTime<Utc>, Decimal)>,
}

impl EquityHistory {
    fn new() -> Self {
        Self {
            points: Vec::new(),
            stride: 1,
            seen: 0,
            latest: None,
        }
    }

    fn record(&mut self, at: DateTime<Utc>, equity: Decimal) {
        self.latest = Some((at, equity));
        if self.seen.is_multiple_of(self.stride) {
            self.points.push((at, equity));
            if self.points.len() >= HISTORY_CAP {
                self.points = self.points.iter().step_by(2).copied().collect();
                self.stride *= 2;
            }
        }
        self.seen += 1;
    }

    /// At most `max` points, evenly thinned, always ending on the latest.
    fn view(&self, max: usize) -> Vec<(DateTime<Utc>, Decimal)> {
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

#[derive(Debug, Clone)]
pub struct EvaluationTracker {
    start_equity: Option<Decimal>,
    peak_equity: Decimal,
    max_drawdown: Decimal,
    history: EquityHistory,
    fees_paid: HashMap<String, Decimal>,
    trade_count: u64,
    turnover: Decimal,
    fills_seen: usize,
    fills_today: (Option<NaiveDate>, u32),
    in_market: Duration2,
    last_point: Option<(DateTime<Utc>, bool)>,
    limits: LimitSpec,
}

/// Milliseconds observed holding a position vs. in total.
#[derive(Debug, Clone, Copy, Default)]
struct Duration2 {
    in_market_ms: i64,
    total_ms: i64,
}

impl EvaluationTracker {
    pub fn new(limits: LimitSpec) -> Self {
        Self {
            start_equity: None,
            peak_equity: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
            history: EquityHistory::new(),
            fees_paid: HashMap::new(),
            trade_count: 0,
            turnover: Decimal::ZERO,
            fills_seen: 0,
            fills_today: (None, 0),
            in_market: Duration2::default(),
            last_point: None,
            limits,
        }
    }

    /// Records one mark-to-market point. `in_market` is whether any
    /// position is held *after* this point; the interval since the previous
    /// point is attributed to the previous point's state.
    ///
    /// `priced` says every non-quote asset held has a market price. Until it
    /// does, the ledger values such assets at 1.0, so `equity` is not real:
    /// recording it would make the session's start equity wrong and its PnL
    /// jump by the whole holding the moment the first price arrives. Points
    /// before that are skipped entirely.
    pub fn record_equity(
        &mut self,
        at: DateTime<Utc>,
        equity: Decimal,
        in_market: bool,
        priced: bool,
    ) {
        if !priced {
            return;
        }
        // Points must go forward in time. A back-dated one would stretch the
        // timeline over the whole gap and fold the line back on itself.
        if self.last_point.is_some_and(|(previous, _)| at < previous) {
            return;
        }
        if self.start_equity.is_none() {
            self.start_equity = Some(equity);
        }
        if equity > self.peak_equity {
            self.peak_equity = equity;
        }
        let drawdown = self.drawdown_at(equity);
        if drawdown > self.max_drawdown {
            self.max_drawdown = drawdown;
        }
        if let Some((previous_at, previous_in_market)) = self.last_point {
            let elapsed = (at - previous_at).num_milliseconds().max(0);
            self.in_market.total_ms += elapsed;
            if previous_in_market {
                self.in_market.in_market_ms += elapsed;
            }
        }
        self.last_point = Some((at, in_market));
        self.history.record(at, equity);
    }

    fn drawdown_at(&self, equity: Decimal) -> Decimal {
        if self.peak_equity > Decimal::ZERO && equity < self.peak_equity {
            (self.peak_equity - equity) / self.peak_equity
        } else {
            Decimal::ZERO
        }
    }

    /// Folds in fills not seen before. `all_fills` is the session's
    /// append-only fill list; the tracker remembers how far it has read.
    pub fn observe_fills(&mut self, all_fills: &[Fill]) {
        if self.fills_seen > all_fills.len() {
            self.fills_seen = 0;
        }
        for fill in &all_fills[self.fills_seen..] {
            self.trade_count += 1;
            self.turnover += fill.price * fill.quantity;
            let currency = fill.fee_currency.clone().unwrap_or_default();
            *self.fees_paid.entry(currency).or_insert(Decimal::ZERO) += fill.fee;
            let day = fill.filled_at.date_naive();
            if self.fills_today.0 != Some(day) {
                self.fills_today = (Some(day), 0);
            }
            self.fills_today.1 += 1;
        }
        self.fills_seen = all_fills.len();
    }

    /// Builds the summary. `gross_exposure` is the sum of |position value|
    /// in quote currency; `max_abs_position` the largest |net position| in
    /// base units.
    pub fn summary(
        &self,
        equity: Decimal,
        realized_pnl: Decimal,
        gross_exposure: Decimal,
        max_abs_position: Decimal,
    ) -> EvaluationSummary {
        let start = self.start_equity.unwrap_or(equity);
        let total_pnl = equity - start;
        let time_fraction = if self.in_market.total_ms > 0 {
            Decimal::from(self.in_market.in_market_ms) / Decimal::from(self.in_market.total_ms)
        } else {
            Decimal::ZERO
        };
        let exposure_fraction = if equity > Decimal::ZERO {
            gross_exposure / equity
        } else {
            Decimal::ZERO
        };
        EvaluationSummary {
            start_equity: start,
            total_pnl,
            realized_pnl,
            unrealized_pnl: total_pnl - realized_pnl,
            current_drawdown: self.drawdown_at(equity),
            max_drawdown: self.max_drawdown,
            gross_exposure_fraction: exposure_fraction,
            fees_paid: self.fees_paid.clone(),
            trade_count: self.trade_count,
            turnover: self.turnover,
            time_in_market_fraction: time_fraction,
            limit_usage: self.limit_usage(max_abs_position),
        }
    }

    fn limit_usage(&self, max_abs_position: Decimal) -> Vec<LimitUsage> {
        let mut usage = Vec::new();
        if let Some(limit) = self.limits.max_position_per_symbol {
            usage.push(LimitUsage {
                name: "position per symbol".to_string(),
                used: max_abs_position,
                limit,
            });
        }
        if let Some(limit) = self.limits.max_daily_fills {
            usage.push(LimitUsage {
                name: "fills today".to_string(),
                used: Decimal::from(self.fills_today.1),
                limit: Decimal::from(limit),
            });
        }
        usage
    }

    /// The thinned equity history to send with a snapshot.
    pub fn equity_history(&self) -> Vec<(DateTime<Utc>, Decimal)> {
        self.history.view(SNAPSHOT_HISTORY_POINTS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{FillId, OrderId, Side, Venue};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    fn tracker() -> EvaluationTracker {
        EvaluationTracker::new(LimitSpec {
            max_position_per_symbol: Some(dec!(0.05)),
            max_daily_fills: Some(10),
        })
    }

    fn fill(price: Decimal, qty: Decimal, fee: Decimal, currency: Option<&str>, secs: i64) -> Fill {
        Fill {
            id: FillId(uuid::Uuid::new_v4()),
            order_id: OrderId(uuid::Uuid::new_v4()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            price,
            quantity: qty,
            fee,
            fee_currency: currency.map(str::to_string),
            filled_at: at(secs),
            leverage: None,
            margin_mode: None,
        }
    }

    #[test]
    fn pnl_and_drawdown_follow_the_equity_curve() {
        let mut t = tracker();
        for (i, e) in [dec!(1000), dec!(1100), dec!(990), dec!(1050)]
            .iter()
            .enumerate()
        {
            t.record_equity(at(i as i64), *e, false, true);
        }
        let s = t.summary(dec!(1050), dec!(20), dec!(0), dec!(0));
        assert_eq!(s.start_equity, dec!(1000));
        assert_eq!(s.total_pnl, dec!(50));
        assert_eq!(s.realized_pnl, dec!(20));
        assert_eq!(s.unrealized_pnl, dec!(30));
        assert_eq!(s.max_drawdown, dec!(0.1)); // 1100 -> 990
        assert_eq!(s.current_drawdown, (dec!(1100) - dec!(1050)) / dec!(1100));
    }

    #[test]
    fn drawdown_is_zero_at_a_new_peak() {
        let mut t = tracker();
        t.record_equity(at(0), dec!(100), false, true);
        t.record_equity(at(1), dec!(120), false, true);
        assert_eq!(
            t.summary(dec!(120), dec!(0), dec!(0), dec!(0))
                .current_drawdown,
            dec!(0)
        );
    }

    #[test]
    fn time_in_market_attributes_each_interval_to_the_previous_state() {
        let mut t = tracker();
        t.record_equity(at(0), dec!(100), true, true); // enters the market
        t.record_equity(at(10), dec!(100), false, true); // 10s in market, then flat
        t.record_equity(at(20), dec!(100), false, true); // 10s flat
        let s = t.summary(dec!(100), dec!(0), dec!(0), dec!(0));
        assert_eq!(s.time_in_market_fraction, dec!(0.5));
    }

    #[test]
    fn fills_accumulate_fees_per_currency_turnover_and_count_once() {
        let mut t = tracker();
        let fills = vec![
            fill(dec!(100), dec!(2), dec!(0.1), Some("USDT"), 0),
            fill(dec!(200), dec!(1), dec!(0.001), Some("BNB"), 1),
            fill(dec!(50), dec!(1), dec!(0.2), Some("USDT"), 2),
        ];
        t.observe_fills(&fills[..2]);
        t.observe_fills(&fills); // re-presenting the same list must not double count
        let s = t.summary(dec!(0), dec!(0), dec!(0), dec!(0));
        assert_eq!(s.trade_count, 3);
        assert_eq!(s.turnover, dec!(450));
        assert_eq!(s.fees_paid["USDT"], dec!(0.3));
        assert_eq!(s.fees_paid["BNB"], dec!(0.001));
    }

    #[test]
    fn fills_without_a_fee_currency_are_grouped_under_empty_string() {
        let mut t = tracker();
        t.observe_fills(&[fill(dec!(1), dec!(1), dec!(0.5), None, 0)]);
        assert_eq!(
            t.summary(dec!(0), dec!(0), dec!(0), dec!(0)).fees_paid[""],
            dec!(0.5)
        );
    }

    #[test]
    fn limit_usage_reports_position_and_daily_fills() {
        let mut t = tracker();
        t.observe_fills(&[
            fill(dec!(1), dec!(1), dec!(0), None, 0),
            fill(dec!(1), dec!(1), dec!(0), None, 1),
        ]);
        let usage = t.summary(dec!(0), dec!(0), dec!(0), dec!(0.04)).limit_usage;
        let pos = usage
            .iter()
            .find(|u| u.name == "position per symbol")
            .unwrap();
        assert_eq!(pos.fraction(), dec!(0.8));
        let fills = usage.iter().find(|u| u.name == "fills today").unwrap();
        assert_eq!(fills.used, dec!(2));
        assert_eq!(fills.fraction(), dec!(0.2));
    }

    #[test]
    fn fills_today_resets_on_a_new_utc_day() {
        let mut t = tracker();
        t.observe_fills(&[fill(dec!(1), dec!(1), dec!(0), None, 0)]);
        t.observe_fills(&[
            fill(dec!(1), dec!(1), dec!(0), None, 0),
            fill(dec!(1), dec!(1), dec!(0), None, 86_400 * 2),
        ]);
        let used = t.summary(dec!(0), dec!(0), dec!(0), dec!(0)).limit_usage[1].used;
        assert_eq!(used, dec!(1));
    }

    #[test]
    fn exposure_fraction_is_zero_without_equity() {
        let t = tracker();
        assert_eq!(
            t.summary(dec!(0), dec!(0), dec!(500), dec!(0))
                .gross_exposure_fraction,
            dec!(0)
        );
        assert_eq!(
            t.summary(dec!(1000), dec!(0), dec!(250), dec!(0))
                .gross_exposure_fraction,
            dec!(0.25)
        );
    }

    #[test]
    fn history_stays_bounded_and_keeps_the_first_and_latest_points() {
        let mut t = tracker();
        let n = (HISTORY_CAP * 6) as i64;
        for i in 0..n {
            t.record_equity(at(i), Decimal::from(1000 + i), false, true);
        }
        assert!(
            t.history.points.len() < HISTORY_CAP,
            "raw history must be halved"
        );
        let view = t.equity_history();
        assert!(view.len() <= SNAPSHOT_HISTORY_POINTS + 1);
        assert_eq!(
            view.first().unwrap().0,
            at(0),
            "start of the session stays visible"
        );
        assert_eq!(
            view.last().unwrap(),
            &(at(n - 1), Decimal::from(1000 + n - 1))
        );
        assert!(view.windows(2).all(|w| w[0].0 < w[1].0), "chronological");
    }

    #[test]
    fn unpriced_points_are_ignored_so_start_equity_is_the_first_real_value() {
        let mut t = tracker();
        // The ledger values an unpriced BTC holding at 1.0: not a real equity.
        t.record_equity(at(0), dec!(10001), false, false);
        t.record_equity(at(1), dec!(10001), false, false);
        assert!(t.equity_history().is_empty(), "nothing recorded yet");
        // The first price arrives: equity jumps to its real value.
        t.record_equity(at(2), dec!(75000), false, true);
        t.record_equity(at(3), dec!(75100), false, true);
        let s = t.summary(dec!(75100), dec!(0), dec!(0), dec!(0));
        assert_eq!(
            s.start_equity,
            dec!(75000),
            "start is the first PRICED equity"
        );
        assert_eq!(s.total_pnl, dec!(100), "no phantom gain from the repricing");
        assert_eq!(s.max_drawdown, dec!(0));
        assert_eq!(t.equity_history().len(), 2);
    }

    #[test]
    fn a_point_older_than_the_last_is_ignored_so_the_timeline_never_folds_back() {
        let mut t = tracker();
        t.record_equity(at(100), dec!(1000), false, true);
        t.record_equity(at(110), dec!(1010), false, true);
        // A back-dated point (e.g. from a daily bar stamped 00:00 UTC).
        t.record_equity(at(-50_000), dec!(5000), false, true);
        t.record_equity(at(120), dec!(1020), false, true);
        let view = t.equity_history();
        assert_eq!(view.len(), 3);
        assert!(view.windows(2).all(|w| w[0].0 < w[1].0), "strictly forward");
        assert_eq!(
            t.summary(dec!(1020), dec!(0), dec!(0), dec!(0))
                .max_drawdown,
            dec!(0)
        );
    }

    #[test]
    fn small_histories_are_returned_whole() {
        let mut t = tracker();
        for i in 0..5 {
            t.record_equity(at(i), Decimal::from(i), false, true);
        }
        assert_eq!(t.equity_history().len(), 5);
    }
}
