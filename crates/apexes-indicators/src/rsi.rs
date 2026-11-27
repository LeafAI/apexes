//! Relative Strength Index, using Wilder's smoothing method.

use rust_decimal::Decimal;

use crate::indicator::Indicator;

/// Relative Strength Index (Wilder's smoothing): the first `period`
/// price changes are averaged with a simple mean to seed `avg_gain`/
/// `avg_loss`; every change after that uses Wilder's recursive smoothing
/// `avg = (avg * (period - 1) + current) / period`. When `avg_loss` is
/// zero (the price has only risen over the tracked history), RSI is
/// defined as `100` to avoid division by zero.
#[derive(Clone)]
pub struct Rsi {
    period: usize,
    prev_price: Option<Decimal>,
    avg_gain: Decimal,
    avg_loss: Decimal,
    /// Number of price *changes* folded into `avg_gain`/`avg_loss` so far
    /// (not the number of prices seen -- the first price has no prior
    /// price to diff against).
    change_count: usize,
}

impl Rsi {
    /// # Panics
    /// Panics if `period == 0`.
    pub fn new(period: usize) -> Self {
        assert!(period > 0, "Rsi::new: period must be > 0, got {period}");
        Self {
            period,
            prev_price: None,
            avg_gain: Decimal::ZERO,
            avg_loss: Decimal::ZERO,
            change_count: 0,
        }
    }

    fn rsi_from_averages(&self) -> Decimal {
        if self.avg_loss.is_zero() {
            return Decimal::from(100);
        }
        // Preserve the ordinary calculation; avoid overflow for extreme gain/loss
        // ratios by using the algebraically equivalent bounded fraction.
        if let Some(denominator) = self
            .avg_gain
            .checked_div(self.avg_loss)
            .and_then(|rs| Decimal::ONE.checked_add(rs))
        {
            Decimal::from(100) - Decimal::from(100) / denominator
        } else {
            let inverse = self.avg_loss / self.avg_gain;
            Decimal::from(100) - Decimal::from(100) * inverse / (Decimal::ONE + inverse)
        }
    }
}

impl Indicator for Rsi {
    type Input = Decimal;
    type Output = Option<Decimal>;

    fn update(&mut self, price: Decimal) -> Option<Decimal> {
        let Some(prev) = self.prev_price else {
            self.prev_price = Some(price);
            return None;
        };
        self.prev_price = Some(price);

        let change = price - prev;
        let gain = change.max(Decimal::ZERO);
        let loss = (-change).max(Decimal::ZERO);

        if self.change_count < self.period {
            let n = Decimal::from(self.change_count as u64);
            self.avg_gain = (self.avg_gain * n + gain) / (n + Decimal::ONE);
            self.avg_loss = (self.avg_loss * n + loss) / (n + Decimal::ONE);
            self.change_count += 1;
            if self.change_count < self.period {
                return None;
            }
        } else {
            let period_minus_one = Decimal::from((self.period - 1) as u64);
            let period = Decimal::from(self.period as u64);
            self.avg_gain = (self.avg_gain * period_minus_one + gain) / period;
            self.avg_loss = (self.avg_loss * period_minus_one + loss) / period;
        }

        Some(self.rsi_from_averages())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn returns_none_until_period_changes_seen() {
        // period=2 needs 2 price changes (3 prices) before a value.
        let mut rsi = Rsi::new(2);
        assert_eq!(rsi.update(dec!(10)), None); // seeds prev_price, no change yet
        assert_eq!(rsi.update(dec!(12)), None); // 1st change, still warming up
    }

    #[test]
    fn matches_hand_derived_golden_values() {
        // prices [10, 12, 11, 13], period=2.
        // change1: +2 (gain=2,loss=0); change2: -1 (gain=0,loss=1) -> seeds
        //   avg_gain=(2+0)/2=1, avg_loss=(0+1)/2=0.5 -> RS=2 -> RSI=100-100/3=66.666...
        // change3: +2 (gain=2,loss=0) -> avg_gain=(1*1+2)/2=1.5, avg_loss=(0.5*1+0)/2=0.25
        //   -> RS=6 -> RSI=100-100/7=85.714285...
        let mut rsi = Rsi::new(2);
        rsi.update(dec!(10));
        rsi.update(dec!(12));
        let third = rsi.update(dec!(11)).expect("warm after 2 changes");
        assert!((third - dec!(66.666666666666667)).abs() < dec!(0.0001));
        let fourth = rsi.update(dec!(13)).expect("still warm");
        assert!((fourth - dec!(85.714285714285714)).abs() < dec!(0.0001));
    }

    #[test]
    fn all_gains_yields_100() {
        let mut rsi = Rsi::new(2);
        rsi.update(dec!(10));
        rsi.update(dec!(11));
        assert_eq!(rsi.update(dec!(12)), Some(dec!(100)));
        assert_eq!(rsi.update(dec!(13)), Some(dec!(100)));
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_period_panics() {
        Rsi::new(0);
    }
    #[test]
    fn extreme_gain_loss_ratio_does_not_overflow() {
        let mut rsi = Rsi::new(2);
        rsi.avg_gain = dec!(1000000000000);
        rsi.avg_loss = dec!(0.0000000000000000000000000001);
        assert_eq!(rsi.rsi_from_averages(), dec!(100));
    }
}
