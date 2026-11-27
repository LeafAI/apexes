//! Bollinger Bands.

use rust_decimal::Decimal;

use crate::indicator::Indicator;
use crate::sma::Sma;
use crate::stddev::RollingStdDev;

/// The three band values produced by one `Bollinger::update` call once warm.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BollingerOutput {
    pub upper: Decimal,
    /// Equal to the underlying SMA.
    pub middle: Decimal,
    pub lower: Decimal,
}

/// Bollinger Bands: an SMA (`middle`) plus upper/lower bands offset by
/// `num_std` rolling standard deviations, over the same trailing window.
#[derive(Clone)]
pub struct Bollinger {
    sma: Sma,
    stddev: RollingStdDev,
    num_std: Decimal,
}

impl Bollinger {
    /// # Panics
    /// Panics if `period == 0` (via the underlying `Sma::new`/`RollingStdDev::new` calls).
    pub fn new(period: usize, num_std: Decimal) -> Self {
        Self {
            sma: Sma::new(period),
            stddev: RollingStdDev::new(period),
            num_std,
        }
    }
}

impl Indicator for Bollinger {
    type Input = Decimal;
    type Output = Option<BollingerOutput>;

    fn update(&mut self, price: Decimal) -> Option<BollingerOutput> {
        let middle = self.sma.update(price);
        let std = self.stddev.update(price);
        let (middle, std) = (middle?, std?);
        let offset = self.num_std * std;
        Some(BollingerOutput {
            upper: middle + offset,
            middle,
            lower: middle - offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn returns_none_during_warmup() {
        let mut boll = Bollinger::new(2, dec!(2));
        assert_eq!(boll.update(dec!(10)), None);
    }

    #[test]
    fn matches_hand_derived_golden_values() {
        // prices [10, 14, 10, 14], period=2, num_std=2 -> constant
        // mean=12, stddev=2.0, so bands are constant (16, 12, 8) at every
        // full window (alternating series has the same pairwise stats
        // each step).
        let mut boll = Bollinger::new(2, dec!(2));
        boll.update(dec!(10));
        let expected = BollingerOutput {
            upper: dec!(16),
            middle: dec!(12),
            lower: dec!(8),
        };
        assert_eq!(boll.update(dec!(14)), Some(expected));
        assert_eq!(boll.update(dec!(10)), Some(expected));
        assert_eq!(boll.update(dec!(14)), Some(expected));
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_period_panics() {
        Bollinger::new(0, dec!(2));
    }
}
