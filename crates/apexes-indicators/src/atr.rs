//! Average True Range.

use rust_decimal::Decimal;

use crate::indicator::Indicator;

/// The subset of OHLC fields `Atr` actually needs (high/low/close --
/// `Atr` never uses `open` or `volume`). `apexes_core::Bar` implements
/// `From<&Bar> for AtrInput` (defined here, not in `apexes-core`, to
/// avoid a reverse dependency) so Rust callers holding a real `Bar` can
/// pass it directly via `.into()`; the Drift Script binding layer
/// constructs an `AtrInput` straight from a script's `high`/`low`/`close`
/// fields without fabricating an entire fake `Bar`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AtrInput {
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
}

impl From<&apexes_core::Bar> for AtrInput {
    fn from(bar: &apexes_core::Bar) -> Self {
        Self {
            high: bar.high,
            low: bar.low,
            close: bar.close,
        }
    }
}

/// Average True Range (Wilder's smoothing), the only indicator in this
/// crate that needs more than a single price per update.
pub struct Atr {
    period: usize,
    prev_close: Option<Decimal>,
    avg_tr: Option<Decimal>,
    /// Number of true-range values folded into `avg_tr` so far.
    count: usize,
}

impl Atr {
    /// # Panics
    /// Panics if `period == 0`.
    pub fn new(period: usize) -> Self {
        assert!(period > 0, "Atr::new: period must be > 0, got {period}");
        Self {
            period,
            prev_close: None,
            avg_tr: None,
            count: 0,
        }
    }

    fn true_range(&self, input: AtrInput) -> Decimal {
        let base = input.high - input.low;
        match self.prev_close {
            None => base,
            Some(prev_close) => {
                let hc = (input.high - prev_close).abs();
                let lc = (input.low - prev_close).abs();
                base.max(hc).max(lc)
            }
        }
    }
}

impl Indicator for Atr {
    type Input = AtrInput;
    type Output = Option<Decimal>;

    fn update(&mut self, input: AtrInput) -> Option<Decimal> {
        let tr = self.true_range(input);
        self.prev_close = Some(input.close);

        if self.count < self.period {
            self.avg_tr = Some(match self.avg_tr {
                None => tr,
                Some(prev) => {
                    let n = Decimal::from(self.count as u64);
                    (prev * n + tr) / (n + Decimal::ONE)
                }
            });
            self.count += 1;
            if self.count < self.period {
                return None;
            }
        } else {
            let period_minus_one = Decimal::from((self.period - 1) as u64);
            let period = Decimal::from(self.period as u64);
            self.avg_tr = self
                .avg_tr
                .map(|prev| (prev * period_minus_one + tr) / period);
        }

        self.avg_tr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn bar(high: Decimal, low: Decimal, close: Decimal) -> AtrInput {
        AtrInput { high, low, close }
    }

    #[test]
    fn returns_none_during_warmup() {
        let mut atr = Atr::new(2);
        assert_eq!(atr.update(bar(dec!(10), dec!(8), dec!(9))), None);
    }

    #[test]
    fn matches_hand_derived_golden_values() {
        // bars: (10,8,9), (12,9,11), (11,9,10); period=2.
        // tr1 = 10-8 = 2 (no prev_close)
        // tr2 = max(12-9, |12-9|, |9-9|) = 3 -> avg_tr seeded=(2+3)/2=2.5
        // tr3 = max(11-9, |11-11|, |9-11|) = 2 -> avg_tr=(2.5*1+2)/2=2.25
        let mut atr = Atr::new(2);
        assert_eq!(atr.update(bar(dec!(10), dec!(8), dec!(9))), None);
        assert_eq!(
            atr.update(bar(dec!(12), dec!(9), dec!(11))),
            Some(dec!(2.5))
        );
        assert_eq!(
            atr.update(bar(dec!(11), dec!(9), dec!(10))),
            Some(dec!(2.25))
        );
    }

    #[test]
    fn from_bar_extracts_high_low_close_only() {
        let bar = apexes_core::Bar {
            symbol: "BTCUSDT".to_string(),
            venue: apexes_core::Venue::Binance,
            open: dec!(100),
            high: dec!(110),
            low: dec!(90),
            close: dec!(105),
            volume: dec!(1000),
            interval: apexes_core::BarInterval::OneMinute,
            open_time: chrono::Utc::now(),
            close_time: chrono::Utc::now(),
        };
        let input: AtrInput = (&bar).into();
        assert_eq!(
            input,
            AtrInput {
                high: dec!(110),
                low: dec!(90),
                close: dec!(105)
            }
        );
    }

    #[test]
    #[should_panic(expected = "period must be > 0")]
    fn zero_period_panics() {
        Atr::new(0);
    }
}
