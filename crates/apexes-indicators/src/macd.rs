//! Moving Average Convergence Divergence.

use rust_decimal::Decimal;

use crate::ema::Ema;
use crate::indicator::Indicator;

/// The three values produced by one `Macd::update` call once warm.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MacdOutput {
    /// `fast_ema - slow_ema`.
    pub macd: Decimal,
    /// EMA of `macd` over `signal_period`.
    pub signal: Decimal,
    /// `macd - signal`.
    pub histogram: Decimal,
}

/// MACD: the difference between a fast and slow EMA, plus an EMA of that
/// difference (the signal line). `update` only starts returning
/// `Some(MacdOutput)` once both underlying EMAs AND the signal EMA have
/// all warmed up.
#[derive(Clone)]
pub struct Macd {
    pub(crate) fast_ema: Ema,
    pub(crate) slow_ema: Ema,
    pub(crate) signal_ema: Ema,
}

impl Macd {
    /// # Panics
    /// Panics if `fast_period == 0`, `slow_period == 0`, or `signal_period == 0`
    /// (via the underlying `Ema::new` calls).
    pub fn new(fast_period: usize, slow_period: usize, signal_period: usize) -> Self {
        Self {
            fast_ema: Ema::new(fast_period),
            slow_ema: Ema::new(slow_period),
            signal_ema: Ema::new(signal_period),
        }
    }
}

impl Indicator for Macd {
    type Input = Decimal;
    type Output = Option<MacdOutput>;

    fn update(&mut self, price: Decimal) -> Option<MacdOutput> {
        let fast = self.fast_ema.update(price);
        let slow = self.slow_ema.update(price);
        let (fast, slow) = (fast?, slow?);
        let macd = fast - slow;
        let signal = self.signal_ema.update(macd)?;
        Some(MacdOutput {
            macd,
            signal,
            histogram: macd - signal,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn returns_none_during_warmup() {
        let mut macd = Macd::new(2, 3, 2);
        assert_eq!(macd.update(dec!(10)), None);
        assert_eq!(macd.update(dec!(12)), None);
    }

    #[test]
    fn matches_hand_derived_golden_values() {
        // prices [10, 12, 11, 15, 13, 20], fast=2 slow=3 signal=2.
        // Cross-checked against an independent Python re-implementation of
        // the same recursive EMA formulas.
        let mut macd = Macd::new(2, 3, 2);
        macd.update(dec!(10));
        macd.update(dec!(12));
        macd.update(dec!(11));
        let fourth = macd.update(dec!(15)).expect("warm at 4th price");
        assert!((fourth.macd - dec!(0.7037037037037042)).abs() < dec!(0.0001));
        assert!((fourth.signal - dec!(0.506172839506173)).abs() < dec!(0.0001));
        assert!((fourth.histogram - dec!(0.1975308641975312)).abs() < dec!(0.0001));

        let fifth = macd.update(dec!(13)).expect("still warm");
        assert!((fifth.macd - dec!(0.23456790123456805)).abs() < dec!(0.0001));
        assert!((fifth.signal - dec!(0.3251028806584364)).abs() < dec!(0.0001));
        assert!((fifth.histogram - dec!(-0.09053497942386834)).abs() < dec!(0.0001));
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_fast_period_panics() {
        Macd::new(0, 3, 2);
    }
}
