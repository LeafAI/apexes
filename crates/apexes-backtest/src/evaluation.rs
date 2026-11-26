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

use crate::history::BoundedSeries as EquityHistory;
use apexes_core::Fill;
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Decimal places the PnL split is rounded to. Prices arrive as `f64` and
/// become 28-digit decimals, so `total - holdings` of an idle strategy is
/// about `3e-25` rather than zero (seen against the real testnet). 8 places is
/// a hundred-millionth of a USDT: finer than any price or fee moves a number.
const PNL_DP: u32 = 8;

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
    /// The strategy's own result: `total_pnl - holdings_pnl`. Includes fees.
    /// What the session's trades did, as opposed to what the market did to
    /// assets the account held before the session started.
    #[serde(default)]
    pub strategy_pnl: Decimal,
    /// What the market did to the assets already held when the session
    /// started: `sum(starting quantity x (price now - price at start))`.
    /// Zero for an account that started flat.
    #[serde(default)]
    pub holdings_pnl: Decimal,
    /// Value of those pre-existing holdings at the start (zero when flat).
    #[serde(default)]
    pub holdings_value_at_start: Decimal,
    /// Drawdown of the strategy's own equity (account equity minus the
    /// market's effect on the starting holdings). Equal to `current_drawdown`
    /// and `max_drawdown` when the account started flat.
    #[serde(default)]
    pub strategy_current_drawdown: Decimal,
    #[serde(default)]
    pub strategy_max_drawdown: Decimal,
    /// `strategy_pnl - realized_pnl`: includes fees and any balance changes
    /// the ledger does not classify as realised.
    pub unrealized_pnl: Decimal,
    /// Fraction of the running peak of ACCOUNT equity currently lost
    /// (`0.05` = 5 %), including the market's effect on held assets.
    pub current_drawdown: Decimal,
    pub max_drawdown: Decimal,
    /// Value held in assets other than cash, over equity: the larger of the
    /// session's positions and the account's non-cash holdings, so a
    /// pre-existing holding counts even while the strategy is flat.
    pub gross_exposure_fraction: Decimal,
    /// Fees by fee currency (`""` when a fill reported none). Not summed
    /// across currencies: they are different units.
    pub fees_paid: HashMap<String, Decimal>,
    /// Explicit spot fee and P&L attribution; empty for old snapshots.
    #[serde(default)]
    pub spot_pnl: HashMap<String, crate::spot_accounting::SpotPnlSummary>,
    /// Bound journal lifetime attribution; absent for non-live or unavailable pricing.
    #[serde(default)]
    pub lifetime_spot_pnl: Option<crate::spot_accounting::LifetimeSpotPnl>,
    pub trade_count: u64,
    pub turnover: Decimal,
    /// Fraction of observed time holding any position.
    pub time_in_market_fraction: Decimal,
    pub limit_usage: Vec<LimitUsage>,
}

#[derive(Debug, Clone)]
pub struct EvaluationTracker {
    start_equity: Option<Decimal>,
    peak_equity: Decimal,
    max_drawdown: Decimal,
    /// Peak and worst drawdown of strategy equity (see
    /// [`EvaluationSummary::strategy_max_drawdown`]).
    strategy_peak: Decimal,
    strategy_max_drawdown: Decimal,
    history: EquityHistory,
    fees_paid: HashMap<String, Decimal>,
    trade_count: u64,
    turnover: Decimal,
    fills_seen: usize,
    fills_today: (Option<NaiveDate>, u32),
    in_market: Duration2,
    last_point: Option<(DateTime<Utc>, bool)>,
    limits: LimitSpec,
    /// Non-cash holdings at the moment the session's start equity was fixed.
    baseline: Vec<BaselineHolding>,
    baseline_set: bool,
}

/// An asset already held when the session started, and its price then.
#[derive(Debug, Clone, PartialEq)]
pub struct BaselineHolding {
    pub asset: String,
    pub quantity: Decimal,
    pub price: Decimal,
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
            strategy_peak: Decimal::ZERO,
            strategy_max_drawdown: Decimal::ZERO,
            history: EquityHistory::new(),
            fees_paid: HashMap::new(),
            trade_count: 0,
            turnover: Decimal::ZERO,
            fills_seen: 0,
            fills_today: (None, 0),
            in_market: Duration2::default(),
            last_point: None,
            limits,
            baseline: Vec::new(),
            baseline_set: false,
        }
    }

    /// Records what the account held before the session traded, once. Call it
    /// when [`Self::record_equity`] reports that it fixed the start equity.
    /// Later calls are ignored: the baseline is the starting state, not a
    /// running one.
    pub fn set_baseline(&mut self, holdings: Vec<BaselineHolding>) {
        if self.baseline_set {
            return;
        }
        self.baseline = holdings
            .into_iter()
            .filter(|h| !h.quantity.is_zero())
            .collect();
        self.baseline_set = true;
    }

    /// What the market did to the starting holdings, at `prices` (keyed by
    /// base asset). An asset with no current price counts as unchanged.
    fn holdings_pnl(&self, prices: &HashMap<String, Decimal>) -> Decimal {
        self.baseline
            .iter()
            .map(|h| {
                let now = prices.get(&h.asset).copied().unwrap_or(h.price);
                h.quantity * (now - h.price)
            })
            .sum()
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
    ///
    /// Returns true when this call fixed the session's start equity, which is
    /// the moment to hand over the starting holdings ([`Self::set_baseline`]).
    pub fn record_equity(
        &mut self,
        at: DateTime<Utc>,
        equity: Decimal,
        in_market: bool,
        priced: bool,
    ) -> bool {
        self.record_equity_at(at, equity, in_market, priced, &HashMap::new())
    }

    /// [`Self::record_equity`] with current prices (by base asset), which the
    /// strategy-only drawdown needs: strategy equity is account equity less
    /// the market's effect on the starting holdings.
    pub fn record_equity_at(
        &mut self,
        at: DateTime<Utc>,
        equity: Decimal,
        in_market: bool,
        priced: bool,
        prices: &HashMap<String, Decimal>,
    ) -> bool {
        if !priced {
            return false;
        }
        // Points must go forward in time. A back-dated one would stretch the
        // timeline over the whole gap and fold the line back on itself.
        if self.last_point.is_some_and(|(previous, _)| at < previous) {
            return false;
        }
        let started = self.start_equity.is_none();
        if started {
            self.start_equity = Some(equity);
        }
        if equity > self.peak_equity {
            self.peak_equity = equity;
        }
        let drawdown = self.drawdown_at(equity);
        if drawdown > self.max_drawdown {
            self.max_drawdown = drawdown;
        }
        // Rounded like the summary's, so float noise in the prices cannot
        // register as a (tiny) drawdown for a strategy that did nothing.
        let strategy_equity = (equity - self.holdings_pnl(prices)).round_dp(PNL_DP);
        if strategy_equity > self.strategy_peak {
            self.strategy_peak = strategy_equity;
        }
        let strategy_drawdown = Self::drawdown_from(self.strategy_peak, strategy_equity);
        if strategy_drawdown > self.strategy_max_drawdown {
            self.strategy_max_drawdown = strategy_drawdown;
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
        started
    }

    fn drawdown_at(&self, equity: Decimal) -> Decimal {
        Self::drawdown_from(self.peak_equity, equity)
    }

    fn drawdown_from(peak: Decimal, equity: Decimal) -> Decimal {
        if peak > Decimal::ZERO && equity < peak {
            (peak - equity) / peak
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
        self.summary_at(
            equity,
            realized_pnl,
            gross_exposure,
            max_abs_position,
            &HashMap::new(),
        )
    }

    /// [`Self::summary`] with current prices (by base asset), which the
    /// split between the strategy's PnL and the starting holdings needs.
    pub fn summary_at(
        &self,
        equity: Decimal,
        realized_pnl: Decimal,
        gross_exposure: Decimal,
        max_abs_position: Decimal,
        prices: &HashMap<String, Decimal>,
    ) -> EvaluationSummary {
        let start = self.start_equity.unwrap_or(equity);
        let total_pnl = equity - start;
        let holdings_pnl = self.holdings_pnl(prices).round_dp(PNL_DP);
        let strategy_pnl = (total_pnl - holdings_pnl).round_dp(PNL_DP);
        let strategy_equity = equity - holdings_pnl;
        let holdings_value_at_start: Decimal =
            self.baseline.iter().map(|h| h.quantity * h.price).sum();
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
            strategy_pnl,
            holdings_pnl,
            holdings_value_at_start,
            unrealized_pnl: strategy_pnl - realized_pnl,
            current_drawdown: self.drawdown_at(equity),
            max_drawdown: self.max_drawdown,
            strategy_current_drawdown: Self::drawdown_from(self.strategy_peak, strategy_equity),
            strategy_max_drawdown: self.strategy_max_drawdown,
            gross_exposure_fraction: exposure_fraction,
            fees_paid: self.fees_paid.clone(),
            spot_pnl: HashMap::new(),
            lifetime_spot_pnl: None,
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
    use crate::history::HISTORY_CAP;
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

    fn btc_baseline(quantity: Decimal, price: Decimal) -> Vec<BaselineHolding> {
        vec![BaselineHolding {
            asset: "BTC".to_string(),
            quantity,
            price,
        }]
    }

    fn prices(btc: Decimal) -> HashMap<String, Decimal> {
        HashMap::from([("BTC".to_string(), btc)])
    }

    /// The case from the screenshot: the account held 1 BTC before the session
    /// and the strategy did nothing, yet "Total PnL" showed the BTC price move
    /// as if the strategy had lost it.
    #[test]
    fn a_price_move_on_pre_existing_holdings_is_not_the_strategys_pnl() {
        let mut t = tracker();
        assert!(t.record_equity(at(0), dec!(10100), false, true));
        t.set_baseline(btc_baseline(dec!(1), dec!(100)));
        t.record_equity(at(60), dec!(10090), false, true);
        let s = t.summary_at(dec!(10090), dec!(0), dec!(0), dec!(0), &prices(dec!(90)));
        assert_eq!(s.total_pnl, dec!(-10));
        assert_eq!(s.holdings_pnl, dec!(-10), "the market moved 1 BTC by -10");
        assert_eq!(s.strategy_pnl, dec!(0), "the strategy did nothing");
        assert_eq!(s.unrealized_pnl, dec!(0), "no phantom unrealised loss");
        assert_eq!(s.holdings_value_at_start, dec!(100));
    }

    /// Equity = cash + sum(quantity x price), so total PnL splits exactly into
    /// the starting holdings' revaluation and what the trades did.
    #[test]
    fn the_two_parts_always_add_up_to_the_total() {
        // Start: 10000 USDT + 1 BTC at 100 = 10100. The strategy buys 0.1 BTC at
        // 100 (cash -10, BTC +0.1), then BTC rises to 110.
        let mut t = tracker();
        t.record_equity(at(0), dec!(10100), false, true);
        t.set_baseline(btc_baseline(dec!(1), dec!(100)));
        let equity_now = dec!(9990) + dec!(1.1) * dec!(110);
        let s = t.summary_at(equity_now, dec!(0), dec!(0), dec!(0), &prices(dec!(110)));
        assert_eq!(s.total_pnl, dec!(11));
        assert_eq!(s.holdings_pnl, dec!(10), "1 BTC x +10");
        assert_eq!(s.strategy_pnl, dec!(1), "0.1 BTC bought x +10");
        assert_eq!(s.strategy_pnl + s.holdings_pnl, s.total_pnl);
    }

    /// Selling into the starting holding is still a trade: it gave up upside.
    #[test]
    fn selling_part_of_the_starting_holding_counts_against_the_strategy() {
        // Sold 0.5 BTC at 100 (cash +50); BTC then rose to 110.
        let mut t = tracker();
        t.record_equity(at(0), dec!(10100), false, true);
        t.set_baseline(btc_baseline(dec!(1), dec!(100)));
        let equity_now = dec!(10050) + dec!(0.5) * dec!(110);
        let s = t.summary_at(equity_now, dec!(0), dec!(0), dec!(0), &prices(dec!(110)));
        assert_eq!(s.total_pnl, dec!(5));
        assert_eq!(s.holdings_pnl, dec!(10));
        assert_eq!(s.strategy_pnl, dec!(-5), "the half it sold missed the rise");
        assert_eq!(s.strategy_pnl + s.holdings_pnl, s.total_pnl);
    }

    #[test]
    fn an_account_that_started_flat_has_no_holdings_pnl() {
        let mut t = tracker();
        t.record_equity(at(0), dec!(10000), false, true);
        t.set_baseline(Vec::new());
        let s = t.summary_at(dec!(10030), dec!(20), dec!(0), dec!(0), &prices(dec!(150)));
        assert_eq!(s.holdings_pnl, dec!(0));
        assert_eq!(s.holdings_value_at_start, dec!(0));
        assert_eq!(s.strategy_pnl, dec!(30));
        assert_eq!(s.unrealized_pnl, dec!(10), "30 total - 20 realised");
    }

    #[test]
    fn the_start_is_reported_once_and_the_baseline_is_not_replaced() {
        let mut t = tracker();
        assert!(
            !t.record_equity(at(0), dec!(10001), false, false),
            "an unpriced point does not start the session"
        );
        assert!(t.record_equity(at(1), dec!(10100), false, true));
        assert!(!t.record_equity(at(2), dec!(10101), false, true));
        t.set_baseline(btc_baseline(dec!(1), dec!(100)));
        t.set_baseline(btc_baseline(dec!(5), dec!(999)));
        let s = t.summary_at(dec!(10100), dec!(0), dec!(0), dec!(0), &prices(dec!(100)));
        assert_eq!(s.holdings_value_at_start, dec!(100), "first baseline kept");
        assert!(
            !t.record_equity(at(-5), dec!(1), false, true),
            "a back-dated point is ignored and starts nothing"
        );
    }

    #[test]
    fn without_prices_the_holdings_count_as_unchanged_not_as_a_loss() {
        let mut t = tracker();
        t.record_equity(at(0), dec!(10100), false, true);
        t.set_baseline(btc_baseline(dec!(1), dec!(100)));
        let s = t.summary(dec!(10100), dec!(0), dec!(0), dec!(0));
        assert_eq!(s.holdings_pnl, dec!(0));
    }

    /// A snapshot from an engine that predates the split must still parse.
    #[test]
    fn an_older_summary_without_the_new_fields_still_deserialises() {
        let mut t = tracker();
        t.record_equity(at(0), dec!(1000), false, true);
        let mut json: serde_json::Value =
            serde_json::to_value(t.summary(dec!(1000), dec!(0), dec!(0), dec!(0))).unwrap();
        for key in ["strategy_pnl", "holdings_pnl", "holdings_value_at_start"] {
            json.as_object_mut().unwrap().remove(key);
        }
        let parsed: EvaluationSummary = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.holdings_pnl, dec!(0));
        assert_eq!(parsed.strategy_pnl, dec!(0));
    }

    /// The 1.16% "max drawdown" on the screenshot was BTC's price swing on a
    /// holding the strategy never traded. The strategy's own drawdown, which
    /// is what judges it, must not move when only the market does.
    #[test]
    fn the_strategys_drawdown_ignores_the_market_moving_pre_existing_holdings() {
        let mut t = tracker();
        // 1 BTC at 100 + 9900 USDT = 10000.
        assert!(t.record_equity_at(at(0), dec!(10000), false, true, &prices(dec!(100))));
        t.set_baseline(btc_baseline(dec!(1), dec!(100)));
        t.record_equity_at(at(10), dec!(10050), false, true, &prices(dec!(150)));
        t.record_equity_at(at(20), dec!(9950), false, true, &prices(dec!(50)));
        let s = t.summary_at(dec!(9950), dec!(0), dec!(0), dec!(0), &prices(dec!(50)));
        // The account fell 50 -> 9950 from a 10050 peak: about 1%.
        assert!(s.max_drawdown > dec!(0.009), "{}", s.max_drawdown);
        // The strategy did nothing, so its own drawdown is exactly zero.
        assert_eq!(s.strategy_max_drawdown, dec!(0));
        assert_eq!(s.strategy_current_drawdown, dec!(0));
    }

    #[test]
    fn the_strategys_drawdown_sees_its_own_losses_even_while_the_market_rises() {
        let mut t = tracker();
        t.record_equity_at(at(0), dec!(10000), false, true, &prices(dec!(100)));
        t.set_baseline(btc_baseline(dec!(1), dec!(100)));
        // BTC +50 (holding +50) but the strategy lost 30 in trades: equity +20.
        t.record_equity_at(at(10), dec!(10020), false, true, &prices(dec!(150)));
        let s = t.summary_at(dec!(10020), dec!(0), dec!(0), dec!(0), &prices(dec!(150)));
        assert_eq!(s.total_pnl, dec!(20));
        assert_eq!(s.strategy_pnl, dec!(-30));
        // 10000 -> 9970 strategy equity: 0.3% drawdown the account view hides.
        assert_eq!(s.strategy_max_drawdown, dec!(0.003));
        assert_eq!(
            s.max_drawdown,
            dec!(0),
            "the account is up, so no account drawdown"
        );
    }

    #[test]
    fn without_holdings_the_two_drawdowns_are_identical() {
        let mut t = tracker();
        for (i, e) in [dec!(1000), dec!(1100), dec!(990)].iter().enumerate() {
            t.record_equity_at(at(i as i64), *e, false, true, &HashMap::new());
        }
        t.set_baseline(Vec::new());
        let s = t.summary_at(dec!(990), dec!(0), dec!(0), dec!(0), &HashMap::new());
        assert_eq!(s.strategy_max_drawdown, s.max_drawdown);
        assert_eq!(s.strategy_current_drawdown, s.current_drawdown);
    }

    /// Seen against the real testnet: prices come in as f64, so an idle
    /// strategy's `total - holdings` was 3.3e-25 instead of 0, and a flat
    /// session "had" a strategy PnL and drawdown.
    #[test]
    fn float_noise_in_the_prices_is_not_a_strategy_pnl_or_drawdown() {
        use std::str::FromStr;
        let price = |text: &str| Decimal::from_str(text).unwrap();
        let (p0, p1, p2) = (
            price("64370.6600000000031"),
            price("64530.7660059999975"),
            price("64498.1200000000012"),
        );
        let mut t = tracker();
        let qty = price("1.00060000");
        let cash = price("9940.905868");
        let equity = |p: Decimal| cash + qty * p;
        t.record_equity_at(at(0), equity(p0), false, true, &prices(p0));
        t.set_baseline(btc_baseline(qty, p0));
        t.record_equity_at(at(10), equity(p1), false, true, &prices(p1));
        t.record_equity_at(at(20), equity(p2), false, true, &prices(p2));
        let s = t.summary_at(equity(p2), dec!(0), dec!(0), dec!(0), &prices(p2));
        assert_eq!(s.strategy_pnl, dec!(0), "{}", s.strategy_pnl);
        assert_eq!(s.strategy_max_drawdown, dec!(0));
        assert_eq!(s.strategy_current_drawdown, dec!(0));
        assert!(s.max_drawdown > dec!(0), "the account itself did draw down");
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
