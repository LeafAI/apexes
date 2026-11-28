//! Exponential Moving Average.

use rust_decimal::Decimal;

use crate::indicator::Indicator;

/// Exponential Moving Average with smoothing factor `alpha = 2 / (period + 1)`.
///
/// The first data point seeds the internal value (so a meaningful average
/// requires seeing at least `period` data points, matching `Sma`'s
/// warm-up length) but does not itself count toward the returned
/// warm-up-complete average -- see `returns_none_until_period_points_seen`
/// below for the exact boundary.
#[derive(Clone)]
pub struct Ema {
    pub(crate) period: usize,
    alpha: Decimal,
    pub(crate) value: Option<Decimal>,
    pub(crate) count: usize,
}

impl Ema {
    /// # Panics
    /// Panics if `period == 0`.
    pub fn new(period: usize) -> Self {
        assert!(period > 0, "Ema::new: period must be > 0, got {period}");
        let alpha = Decimal::from(2) / Decimal::from((period + 1) as u64);
        Self {
            period,
            alpha,
            value: None,
            count: 0,
        }
    }
}

impl Indicator for Ema {
    type Input = Decimal;
    type Output = Option<Decimal>;

    fn update(&mut self, price: Decimal) -> Option<Decimal> {
        self.value = Some(match self.value {
            None => price,
            Some(prev) => self.alpha * price + (Decimal::ONE - self.alpha) * prev,
        });
        self.count += 1;
        if self.count >= self.period {
            self.value
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
    fn returns_none_until_period_points_seen() {
        let mut ema = Ema::new(3);
        assert_eq!(ema.update(dec!(10)), None);
        assert_eq!(ema.update(dec!(11)), None);
    }

    #[test]
    fn returns_seeded_ema_once_warm() {
        // Golden values hand-derived: alpha = 2/4 = 0.5, seed = 10,
        // then 0.5*11 + 0.5*10 = 10.5, then 0.5*12 + 0.5*10.5 = 11.25.
        let mut ema = Ema::new(3);
        ema.update(dec!(10));
        ema.update(dec!(11));
        assert_eq!(ema.update(dec!(12)), Some(dec!(11.25)));
        assert_eq!(ema.update(dec!(13)), Some(dec!(12.125)));
        assert_eq!(ema.update(dec!(14)), Some(dec!(13.0625)));
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_period_panics() {
        Ema::new(0);
    }
}
