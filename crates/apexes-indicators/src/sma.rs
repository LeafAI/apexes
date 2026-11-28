//! Simple Moving Average.

use std::collections::VecDeque;

use rust_decimal::Decimal;

use crate::indicator::Indicator;

/// Simple Moving Average over a fixed-size trailing window.
#[derive(Clone)]
pub struct Sma {
    pub(crate) period: usize,
    pub(crate) window: VecDeque<Decimal>,
    pub(crate) sum: Decimal,
}

impl Sma {
    /// Creates a new `Sma` with the given window size.
    ///
    /// # Panics
    /// Panics if `period == 0` -- a zero-length window is a construction-time
    /// contract violation, not a runtime data error (design spec §六 decision 3).
    pub fn new(period: usize) -> Self {
        assert!(period > 0, "Sma::new: period must be > 0, got {period}");
        Self {
            period,
            window: VecDeque::with_capacity(period),
            sum: Decimal::ZERO,
        }
    }
}

impl Indicator for Sma {
    type Input = Decimal;
    type Output = Option<Decimal>;

    fn update(&mut self, price: Decimal) -> Option<Decimal> {
        self.window.push_back(price);
        self.sum += price;
        if self.window.len() > self.period
            && let Some(oldest) = self.window.pop_front()
        {
            self.sum -= oldest;
        }
        if self.window.len() == self.period {
            Some(self.sum / Decimal::from(self.period as u64))
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
        let mut sma = Sma::new(3);
        assert_eq!(sma.update(dec!(10)), None);
        assert_eq!(sma.update(dec!(11)), None);
    }

    #[test]
    fn returns_average_once_window_is_full() {
        let mut sma = Sma::new(3);
        sma.update(dec!(10));
        sma.update(dec!(11));
        assert_eq!(sma.update(dec!(12)), Some(dec!(11)));
    }

    #[test]
    fn slides_window_after_full() {
        let mut sma = Sma::new(3);
        sma.update(dec!(10));
        sma.update(dec!(11));
        sma.update(dec!(12));
        assert_eq!(sma.update(dec!(13)), Some(dec!(12)));
        assert_eq!(sma.update(dec!(14)), Some(dec!(13)));
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_period_panics() {
        Sma::new(0);
    }
}
