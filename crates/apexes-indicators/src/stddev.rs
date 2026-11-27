//! Rolling (population) standard deviation over a fixed-size trailing window.

use std::collections::VecDeque;

use rust_decimal::{Decimal, MathematicalOps};

use crate::indicator::Indicator;

/// Rolling population standard deviation: `sqrt(mean(x^2) - mean(x)^2)`
/// over the current trailing window, updated in O(1) via running sums.
#[derive(Clone)]
pub struct RollingStdDev {
    period: usize,
    window: VecDeque<Decimal>,
    sum: Decimal,
    sum_sq: Decimal,
}

impl RollingStdDev {
    /// # Panics
    /// Panics if `period == 0`.
    pub fn new(period: usize) -> Self {
        assert!(
            period > 0,
            "RollingStdDev::new: period must be > 0, got {period}"
        );
        Self {
            period,
            window: VecDeque::with_capacity(period),
            sum: Decimal::ZERO,
            sum_sq: Decimal::ZERO,
        }
    }
}

impl Indicator for RollingStdDev {
    type Input = Decimal;
    type Output = Option<Decimal>;

    fn update(&mut self, price: Decimal) -> Option<Decimal> {
        self.window.push_back(price);
        self.sum += price;
        self.sum_sq += price * price;
        if self.window.len() > self.period
            && let Some(oldest) = self.window.pop_front()
        {
            self.sum -= oldest;
            self.sum_sq -= oldest * oldest;
        }
        if self.window.len() == self.period {
            let n = Decimal::from(self.period as u64);
            let mean = self.sum / n;
            let variance = self.sum_sq / n - mean * mean;
            Some(variance.sqrt().unwrap_or(Decimal::ZERO))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn returns_none_during_warmup() {
        let mut std = RollingStdDev::new(2);
        assert_eq!(std.update(dec!(10)), None);
    }

    #[test]
    fn returns_constant_stddev_for_alternating_series() {
        // prices [10, 14, 10, 14], period=2 -> mean=12, stddev=2.0 at every full window.
        let mut std = RollingStdDev::new(2);
        std.update(dec!(10));
        assert_eq!(std.update(dec!(14)), Some(dec!(2)));
        assert_eq!(std.update(dec!(10)), Some(dec!(2)));
        assert_eq!(std.update(dec!(14)), Some(dec!(2)));
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_period_panics() {
        RollingStdDev::new(0);
    }
}
