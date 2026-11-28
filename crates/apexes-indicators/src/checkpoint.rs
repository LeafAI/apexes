//! F04.05: exact native accumulators for bounded Drift scoped indicators.
//! The wire DTOs never deserialize directly into runnable indicators.
use crate::{Atr, Bollinger, Ema, Macd, RollingStdDev, Rsi, Sma};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub const MAX_PERIOD: usize = 512;
const MAX_PRICE: Decimal = Decimal::from_parts(3_567_587_328, 232, 0, false, 0); // 1e12

#[derive(Debug, thiserror::Error)]
#[error("invalid scoped indicator checkpoint")]
pub struct InvalidCheckpoint;

type Result<T> = std::result::Result<T, InvalidCheckpoint>;

fn require(valid: bool) -> Result<()> {
    if valid {
        Ok(())
    } else {
        Err(InvalidCheckpoint)
    }
}

fn period(p: usize) -> Result<()> {
    require((1..=MAX_PERIOD).contains(&p))
}

fn price(p: Decimal) -> bool {
    p > Decimal::ZERO && p <= MAX_PRICE
}

fn nonnegative(p: Decimal) -> bool {
    p >= Decimal::ZERO && p <= MAX_PRICE
}

fn plausible_sum(actual: Decimal, recomputed: Decimal, full: bool) -> bool {
    // A partially populated window has never evicted an observation and
    // therefore must match exactly. A full window may have accumulated
    // decimal rounding from evictions. Preserve the original accumulator,
    // but refuse gross discrepancies instead of publishing unsafe state.
    if !full {
        return actual == recomputed;
    }
    let tolerance = Decimal::new(1, 12).max(recomputed.abs() * Decimal::new(1, 12));
    (actual - recomputed).abs() <= tolerance
}

fn window(p: usize, w: &VecDeque<Decimal>, sum: Decimal) -> Result<()> {
    period(p)?;
    require(
        w.len() <= p
            && w.iter().copied().all(price)
            && sum >= -Decimal::ONE
            && sum <= MAX_PRICE * Decimal::from(p as u64) + Decimal::ONE,
    )?;
    require(plausible_sum(sum, w.iter().copied().sum(), w.len() == p))
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SmaState {
    period: usize,
    window: VecDeque<Decimal>,
    sum: Decimal,
}

impl SmaState {
    pub fn capture(i: &Sma) -> Self {
        Self {
            period: i.period,
            window: i.window.clone(),
            sum: i.sum,
        }
    }
    pub fn restore(self, expected: usize) -> Result<Sma> {
        require(self.period == expected)?;
        window(self.period, &self.window, self.sum)?;
        Ok(Sma {
            period: self.period,
            window: self.window,
            sum: self.sum,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StddevState {
    period: usize,
    window: VecDeque<Decimal>,
    sum: Decimal,
    sum_sq: Decimal,
}

impl StddevState {
    /// Inspect the retained tail without changing any accumulator.
    pub fn last(&self) -> Option<Decimal> {
        self.window.back().copied()
    }

    pub fn sample_count(&self) -> usize {
        self.window.len()
    }

    pub fn capture(i: &RollingStdDev) -> Self {
        Self {
            period: i.period,
            window: i.window.clone(),
            sum: i.sum,
            sum_sq: i.sum_sq,
        }
    }
    pub fn restore(self, expected: usize) -> Result<RollingStdDev> {
        require(self.period == expected)?;
        window(self.period, &self.window, self.sum)?;
        require(
            self.sum_sq >= -MAX_PRICE
                && self.sum_sq
                    <= MAX_PRICE * MAX_PRICE * Decimal::from(self.period as u64) + MAX_PRICE
                && (!self.window.is_empty() || self.sum_sq.is_zero()),
        )?;
        require(plausible_sum(
            self.sum_sq,
            self.window.iter().map(|p| *p * *p).sum(),
            self.window.len() == self.period,
        ))?;
        Ok(RollingStdDev {
            period: self.period,
            window: self.window,
            sum: self.sum,
            sum_sq: self.sum_sq,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmaState {
    period: usize,
    value: Option<Decimal>,
    count: usize,
}

impl EmaState {
    pub fn capture(i: &Ema) -> Self {
        Self {
            period: i.period,
            value: i.value,
            count: i.count,
        }
    }
    fn restore_inner(self, expected: usize, signed: bool) -> Result<Ema> {
        require(self.period == expected)?;
        period(self.period)?;
        require(
            self.count <= usize::MAX / 2
                && (self.count == 0) == self.value.is_none()
                && self.value.is_none_or(|v| {
                    if signed {
                        v >= -MAX_PRICE && v <= MAX_PRICE
                    } else {
                        price(v)
                    }
                }),
        )?;
        let mut i = Ema::new(self.period);
        i.value = self.value;
        i.count = self.count;
        Ok(i)
    }
    pub fn restore(self, expected: usize) -> Result<Ema> {
        self.restore_inner(expected, false)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RsiState {
    period: usize,
    prev_price: Option<Decimal>,
    avg_gain: Decimal,
    avg_loss: Decimal,
    change_count: usize,
}

impl RsiState {
    pub fn capture(i: &Rsi) -> Self {
        Self {
            period: i.period,
            prev_price: i.prev_price,
            avg_gain: i.avg_gain,
            avg_loss: i.avg_loss,
            change_count: i.change_count,
        }
    }
    pub fn restore(self, expected: usize) -> Result<Rsi> {
        require(self.period == expected)?;
        period(self.period)?;
        require(
            self.change_count <= self.period
                && self.prev_price.is_none_or(price)
                && nonnegative(self.avg_gain)
                && nonnegative(self.avg_loss)
                && (self.change_count == 0 || self.prev_price.is_some())
                && (self.change_count != 0 || (self.avg_gain.is_zero() && self.avg_loss.is_zero())),
        )?;
        Ok(Rsi {
            period: self.period,
            prev_price: self.prev_price,
            avg_gain: self.avg_gain,
            avg_loss: self.avg_loss,
            change_count: self.change_count,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AtrState {
    period: usize,
    prev_close: Option<Decimal>,
    avg_tr: Option<Decimal>,
    count: usize,
}

impl AtrState {
    pub fn capture(i: &Atr) -> Self {
        Self {
            period: i.period,
            prev_close: i.prev_close,
            avg_tr: i.avg_tr,
            count: i.count,
        }
    }
    pub fn restore(self, expected: usize) -> Result<Atr> {
        require(self.period == expected)?;
        period(self.period)?;
        require(
            self.count <= self.period
                && (self.count == 0) == self.prev_close.is_none()
                && (self.count == 0) == self.avg_tr.is_none()
                && self.prev_close.is_none_or(price)
                && self.avg_tr.is_none_or(nonnegative),
        )?;
        Ok(Atr {
            period: self.period,
            prev_close: self.prev_close,
            avg_tr: self.avg_tr,
            count: self.count,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MacdState {
    fast: EmaState,
    slow: EmaState,
    signal: EmaState,
}

impl MacdState {
    pub fn capture(i: &Macd) -> Self {
        Self {
            fast: EmaState::capture(&i.fast_ema),
            slow: EmaState::capture(&i.slow_ema),
            signal: EmaState::capture(&i.signal_ema),
        }
    }
    pub fn restore(self, fast: usize, slow: usize, signal: usize) -> Result<Macd> {
        period(fast)?;
        period(slow)?;
        period(signal)?;
        require(
            self.fast.count == self.slow.count
                && self.signal.count == self.fast.count.saturating_sub(fast.max(slow) - 1),
        )?;
        Ok(Macd {
            fast_ema: self.fast.restore(fast)?,
            slow_ema: self.slow.restore(slow)?,
            signal_ema: self.signal.restore_inner(signal, true)?,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BollingerState {
    sma: SmaState,
    stddev: StddevState,
    width: Decimal,
}

impl BollingerState {
    pub fn capture(i: &Bollinger) -> Self {
        Self {
            sma: SmaState::capture(&i.sma),
            stddev: StddevState::capture(&i.stddev),
            width: i.num_std,
        }
    }
    pub fn restore(self, expected: usize, width: Decimal) -> Result<Bollinger> {
        require(
            self.width == width
                && width >= Decimal::ZERO
                && width <= Decimal::from(100)
                && self.sma.window == self.stddev.window
                && self.sma.sum == self.stddev.sum,
        )?;
        Ok(Bollinger {
            sma: self.sma.restore(expected)?,
            stddev: self.stddev.restore(expected)?,
            num_std: width,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Indicator;

    #[test]
    fn window_checkpoints_refuse_inconsistent_accumulators() {
        let mut sma = Sma::new(3);
        sma.update(Decimal::from(5));
        let mut checkpoint = SmaState::capture(&sma);
        checkpoint.sum = Decimal::ZERO;
        assert!(checkpoint.restore(3).is_err());
        let mut stddev = RollingStdDev::new(2);
        for p in [4, 5, 6] {
            stddev.update(Decimal::from(p));
        }
        let mut checkpoint = StddevState::capture(&stddev);
        checkpoint.sum_sq = Decimal::ZERO;
        assert!(checkpoint.restore(2).is_err());
        assert!(StddevState::capture(&stddev).restore(0).is_err());
        assert!(StddevState::capture(&stddev).restore(2).is_ok());
    }
}
