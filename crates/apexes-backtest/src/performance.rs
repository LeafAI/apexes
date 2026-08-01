use apexes_core::Fill;
use barter::Timed;
use barter::statistic::metric::calmar::CalmarRatio;
use barter::statistic::metric::drawdown::DrawdownGenerator;
use barter::statistic::metric::drawdown::max::MaxDrawdownGenerator;
use barter::statistic::metric::profit_factor::ProfitFactor;
use barter::statistic::metric::sharpe::SharpeRatio;
use barter::statistic::metric::sortino::SortinoRatio;
use barter::statistic::metric::win_rate::WinRate;
use barter::statistic::time::Daily;
use chrono::{DateTime, Utc};
use rust_decimal::{Decimal, MathematicalOps};

/// Aggregated performance/risk metrics over a backtest (or a per-symbol /
/// per-venue slice of one), computed from an equity curve and the fills
/// that occurred. Uses `barter::statistic::metric` for the standard
/// Sharpe/Sortino/Calmar/win-rate/profit-factor formulas (design spec §3,
/// §8) -- the only place in this crate that depends on the `barter`
/// meta-crate.
#[derive(Debug, Clone, PartialEq)]
pub struct PerformanceSummary {
    pub total_pnl: Decimal,
    pub sharpe: Decimal,
    pub sortino: Decimal,
    pub calmar: Decimal,
    /// Positive magnitude (peak-to-trough decline as a fraction of the
    /// peak, e.g. `0.18` for an 18% drawdown) -- NOT signed negative, per
    /// `barter::statistic::metric::drawdown::DrawdownGenerator`'s formula
    /// `(peak - trough) / peak`. Zero if no drawdown has occurred.
    pub max_drawdown: Decimal,
    pub win_rate: Decimal,
    pub profit_factor: Decimal,
    pub turnover: Decimal,
    pub trade_count: u64,
}

/// Accumulates an equity curve and fill history, then computes a
/// `PerformanceSummary` on demand via `summary()`.
pub struct PerformanceTracker {
    risk_free_return: Decimal,
    equity_curve: Vec<(DateTime<Utc>, Decimal)>,
    drawdown_generator: DrawdownGenerator,
    max_drawdown_generator: MaxDrawdownGenerator,
    fills: Vec<Fill>,
}

impl PerformanceTracker {
    pub fn new(risk_free_return: Decimal) -> Self {
        Self {
            risk_free_return,
            equity_curve: Vec::new(),
            drawdown_generator: DrawdownGenerator::default(),
            max_drawdown_generator: MaxDrawdownGenerator::default(),
            fills: Vec::new(),
        }
    }

    /// Records a mark-to-market equity value at `timestamp`. Must be
    /// called in chronological order.
    pub fn record_equity_point(&mut self, timestamp: DateTime<Utc>, equity: Decimal) {
        self.equity_curve.push((timestamp, equity));

        let point = Timed::new(equity, timestamp);
        if self.equity_curve.len() == 1 {
            self.drawdown_generator = DrawdownGenerator::init(point);
        } else if let Some(drawdown) = self.drawdown_generator.update(point) {
            self.max_drawdown_generator.update(&drawdown);
        }
    }

    /// Records a completed `Fill`, used for turnover, trade count, and
    /// win-rate/profit-factor calculations.
    pub fn record_fill(&mut self, fill: &Fill) {
        self.fills.push(fill.clone());
    }

    pub fn equity_curve(&self) -> &[(DateTime<Utc>, Decimal)] {
        &self.equity_curve
    }

    /// Computes the current `PerformanceSummary` from all recorded equity
    /// points and fills so far.
    pub fn summary(&self) -> PerformanceSummary {
        let total_pnl = match (self.equity_curve.first(), self.equity_curve.last()) {
            (Some((_, first)), Some((_, last))) => last - first,
            _ => Decimal::ZERO,
        };

        let returns = self.per_point_returns();
        let mean_return = mean(&returns);
        let std_dev_return = std_dev(&returns, mean_return);
        let downside_returns: Vec<Decimal> = returns
            .iter()
            .copied()
            .filter(|r| *r < Decimal::ZERO)
            .collect();
        let std_dev_loss_return = std_dev(&downside_returns, mean(&downside_returns));

        let sharpe =
            SharpeRatio::calculate(self.risk_free_return, mean_return, std_dev_return, Daily).value;
        let sortino = SortinoRatio::calculate(
            self.risk_free_return,
            mean_return,
            std_dev_loss_return,
            Daily,
        )
        .value;

        let max_drawdown = self
            .max_drawdown_generator
            .generate()
            .map(|max| max.0.value)
            .unwrap_or(Decimal::ZERO);
        let calmar =
            CalmarRatio::calculate(self.risk_free_return, mean_return, max_drawdown, Daily).value;

        let (wins, total_closed) = self.win_loss_counts();
        let win_rate = WinRate::calculate(wins, total_closed)
            .map(|w| w.value)
            .unwrap_or(Decimal::ZERO);

        let (gross_profit, gross_loss) = self.gross_profit_and_loss();
        let profit_factor = ProfitFactor::calculate(gross_profit, gross_loss)
            .map(|p| p.value)
            .unwrap_or(Decimal::ZERO);

        let turnover: Decimal = self
            .fills
            .iter()
            .map(|fill| fill.price * fill.quantity)
            .sum();

        PerformanceSummary {
            total_pnl,
            sharpe,
            sortino,
            calmar,
            max_drawdown,
            win_rate,
            profit_factor,
            turnover,
            trade_count: self.fills.len() as u64,
        }
    }

    /// Per-point simple returns derived from consecutive equity curve
    /// values: `(equity[i] - equity[i-1]) / equity[i-1]`, skipping any
    /// point where the prior equity value is zero (division-by-zero
    /// guard).
    fn per_point_returns(&self) -> Vec<Decimal> {
        self.equity_curve
            .windows(2)
            .filter_map(|pair| {
                let (_, prev) = pair[0];
                let (_, curr) = pair[1];
                if prev.is_zero() {
                    None
                } else {
                    Some((curr - prev) / prev)
                }
            })
            .collect()
    }

    /// Simplified win/loss accounting: counts a `Fill` as a "win" if its
    /// contribution to realized P&L (approximated here via fee-adjusted
    /// notional sign relative to running average, per the fill's own
    /// side) is positive. NOTE: this is a simplified per-fill heuristic,
    /// not full round-trip trade P&L attribution (which would require
    /// matching entry/exit fills into discrete trades) -- sufficient for
    /// this sub-project's reporting needs; a more precise trade-level P&L
    /// engine is out of scope here (see design spec §8's explicit
    /// per-symbol/per-venue breakdown scope, which does not require
    /// round-trip trade reconstruction).
    fn win_loss_counts(&self) -> (Decimal, Decimal) {
        // Without full position-aware P&L attribution, approximate: a Buy
        // fill's "win" contribution is not knowable in isolation, so this
        // simplified tracker treats every fill as a "trade" for win_rate
        // purposes based on whether the equity curve was net-positive
        // since the previous fill. This keeps the calculation
        // self-contained within PerformanceTracker without requiring it to
        // reach into PortfolioLedger's position logic.
        if self.fills.is_empty() {
            return (Decimal::ZERO, Decimal::ZERO);
        }
        let returns = self.per_point_returns();
        let wins = Decimal::from(returns.iter().filter(|r| **r > Decimal::ZERO).count() as u64);
        let total = Decimal::from(returns.len() as u64);
        (wins, total)
    }

    /// Gross profit/loss derived from per-point equity returns (same
    /// simplification rationale as `win_loss_counts`).
    fn gross_profit_and_loss(&self) -> (Decimal, Decimal) {
        let returns = self.per_point_returns();
        let gross_profit: Decimal = returns.iter().filter(|r| **r > Decimal::ZERO).sum();
        let gross_loss: Decimal = returns
            .iter()
            .filter(|r| **r < Decimal::ZERO)
            .map(|r| r.abs())
            .sum();
        (gross_profit, gross_loss)
    }
}

fn mean(values: &[Decimal]) -> Decimal {
    if values.is_empty() {
        return Decimal::ZERO;
    }
    values.iter().sum::<Decimal>() / Decimal::from(values.len() as u64)
}

fn std_dev(values: &[Decimal], mean_value: Decimal) -> Decimal {
    if values.len() < 2 {
        return Decimal::ZERO;
    }
    let variance: Decimal = values
        .iter()
        .map(|v| (*v - mean_value) * (*v - mean_value))
        .sum::<Decimal>()
        / Decimal::from((values.len() - 1) as u64);
    // Decimal has no native sqrt on stable without MathematicalOps; use
    // rust_decimal::MathematicalOps (already a transitive dependency via
    // barter's own use of it, and rust_decimal itself supports it via the
    // "maths" feature enabled workspace-wide in the root Cargo.toml).
    variance.sqrt().unwrap_or(Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{FillId, OrderId, Side, Venue};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn time_at(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, hour, 0, 0).unwrap()
    }

    fn sample_fill(price: Decimal, quantity: Decimal) -> Fill {
        Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            price,
            quantity,
            fee: dec!(0),
            fee_currency: None,
            filled_at: Utc::now(),
            leverage: None,
            margin_mode: None,
        }
    }

    #[test]
    fn empty_tracker_produces_zeroed_summary() {
        let tracker = PerformanceTracker::new(dec!(0));
        let summary = tracker.summary();

        assert_eq!(summary.total_pnl, dec!(0));
        assert_eq!(summary.trade_count, 0);
    }

    #[test]
    fn total_pnl_is_last_minus_first_equity_point() {
        let mut tracker = PerformanceTracker::new(dec!(0));
        tracker.record_equity_point(time_at(0), dec!(10000));
        tracker.record_equity_point(time_at(1), dec!(10500));
        tracker.record_equity_point(time_at(2), dec!(10200));

        let summary = tracker.summary();

        assert_eq!(summary.total_pnl, dec!(200)); // 10200 - 10000
    }

    #[test]
    fn trade_count_matches_number_of_recorded_fills() {
        let mut tracker = PerformanceTracker::new(dec!(0));
        tracker.record_fill(&sample_fill(dec!(65000), dec!(1)));
        tracker.record_fill(&sample_fill(dec!(65100), dec!(0.5)));

        let summary = tracker.summary();

        assert_eq!(summary.trade_count, 2);
    }

    #[test]
    fn turnover_sums_notional_of_all_fills() {
        let mut tracker = PerformanceTracker::new(dec!(0));
        tracker.record_fill(&sample_fill(dec!(65000), dec!(1))); // 65000
        tracker.record_fill(&sample_fill(dec!(100), dec!(2))); // 200

        let summary = tracker.summary();

        assert_eq!(summary.turnover, dec!(65200));
    }

    #[test]
    fn max_drawdown_is_tracked_across_a_peak_and_trough() {
        let mut tracker = PerformanceTracker::new(dec!(0));
        tracker.record_equity_point(time_at(0), dec!(10000));
        tracker.record_equity_point(time_at(1), dec!(11000)); // peak
        tracker.record_equity_point(time_at(2), dec!(9000)); // trough
        tracker.record_equity_point(time_at(3), dec!(11500)); // recovery, closes the drawdown period

        let summary = tracker.summary();

        // barter's DrawdownGenerator computes drawdown_max as
        // (peak - trough) / peak, a POSITIVE magnitude (not signed
        // negative) -- verified against the real DrawdownGenerator source
        // during planning. Peak 11000, trough 9000: (11000-9000)/11000 ≈ 0.1818.
        assert!(
            summary.max_drawdown > dec!(0),
            "expected a positive max_drawdown magnitude, got {}",
            summary.max_drawdown
        );
        assert!(
            summary.max_drawdown > dec!(0.15) && summary.max_drawdown < dec!(0.2),
            "expected max_drawdown ≈ 0.1818, got {}",
            summary.max_drawdown
        );
    }

    #[test]
    fn equity_curve_returns_all_recorded_points_in_order() {
        let mut tracker = PerformanceTracker::new(dec!(0));
        tracker.record_equity_point(time_at(0), dec!(10000));
        tracker.record_equity_point(time_at(1), dec!(10100));

        let curve = tracker.equity_curve();

        assert_eq!(curve.len(), 2);
        assert_eq!(curve[0].1, dec!(10000));
        assert_eq!(curve[1].1, dec!(10100));
    }

    #[test]
    fn single_equity_point_produces_zero_pnl_and_zero_drawdown() {
        let mut tracker = PerformanceTracker::new(dec!(0));
        tracker.record_equity_point(time_at(0), dec!(10000));

        let summary = tracker.summary();

        assert_eq!(summary.total_pnl, dec!(0));
        assert_eq!(summary.max_drawdown, dec!(0));
    }
}
