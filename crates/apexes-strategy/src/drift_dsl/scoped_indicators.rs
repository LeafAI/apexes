//! F04.01: named native indicator instances in a callback's instrument scope.
use super::indicators::{
    bollinger_output_to_drift_value, error_object, macd_output_to_drift_value,
    optional_decimal_field, require_number, require_positive_usize,
};
use apexes_indicators::{Atr, AtrInput, Bollinger, Ema, Indicator, Macd, RollingStdDev, Rsi, Sma};
use drift_script::DriftValue;
use rust_decimal::Decimal;

const MAX_PERIOD: usize = 512;
pub(super) const MAX_INDICATORS: usize = 16;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Sma,
    Ema,
    Rsi,
    Stddev,
    Macd,
    Bollinger,
    Atr,
}

impl Kind {
    pub(super) const ALL: [Self; 7] = [
        Self::Sma,
        Self::Ema,
        Self::Rsi,
        Self::Stddev,
        Self::Macd,
        Self::Bollinger,
        Self::Atr,
    ];

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Sma => "sma",
            Self::Ema => "ema",
            Self::Rsi => "rsi",
            Self::Stddev => "stddev",
            Self::Macd => "macd",
            Self::Bollinger => "bollinger",
            Self::Atr => "atr",
        }
    }

    pub(super) fn parse(self, args: &[DriftValue]) -> Result<(Spec, Input), DriftValue> {
        let name = self.name();
        let period = |index| {
            let value = require_positive_usize(args, index, name)?;
            if value > MAX_PERIOD {
                return Err(error_object("scoped indicator period exceeds 512".into()));
            }
            Ok(value)
        };
        let p = period(1)?;
        let expected = match self {
            Self::Macd => 5,
            Self::Bollinger => 4,
            _ => 3,
        };
        if args.len() != expected {
            return Err(error_object(format!(
                "{name}: expected {expected} arguments"
            )));
        }
        let spec = match self {
            Self::Macd => Spec::Macd(p, period(2)?, period(3)?),
            Self::Bollinger => {
                let width = require_number(args, 2, name)?;
                if width < Decimal::ZERO || width > Decimal::from(100) {
                    return Err(error_object(
                        "bollinger deviations must be in 0..=100".into(),
                    ));
                }
                Spec::Bollinger(p, width)
            }
            _ => Spec::Single(self, p),
        };
        let input = if self == Self::Atr {
            let bar = &args[2];
            let field = |key| optional_decimal_field(bar, key, name).map_err(error_object);
            let high = field("high")?;
            let low = field("low")?;
            let close = field("close")?;
            if low <= Decimal::ZERO
                || high < low
                || close < low
                || close > high
                || high > Decimal::from(1_000_000_000_000u64)
            {
                return Err(error_object("atr: invalid or out-of-range OHLC".into()));
            }
            Input::Bar(AtrInput { high, low, close })
        } else {
            let price = require_number(args, expected - 1, name)?;
            if price <= Decimal::ZERO || price > Decimal::from(1_000_000_000_000u64) {
                return Err(error_object(
                    "scoped indicator price must be positive and at most 1e12".into(),
                ));
            }
            Input::Price(price)
        };
        Ok((spec, input))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Spec {
    Single(Kind, usize),
    Macd(usize, usize, usize),
    Bollinger(usize, Decimal),
}
pub(super) enum Input {
    Price(Decimal),
    Bar(AtrInput),
}

#[derive(Clone)]
enum NativeIndicator {
    Sma(Sma),
    Ema(Ema),
    Rsi(Rsi),
    Stddev(RollingStdDev),
    Macd(Macd),
    Bollinger(Bollinger),
    Atr(Atr),
}

#[derive(Clone)]
pub(super) struct Instance {
    spec: Spec,
    native: NativeIndicator,
}

fn number(value: Option<Decimal>) -> DriftValue {
    value.map_or(DriftValue::Null, |n| {
        DriftValue::Number(n.try_into().unwrap_or(f64::NAN))
    })
}

impl Instance {
    pub(super) fn new(spec: Spec) -> Result<Self, DriftValue> {
        let native = match spec {
            Spec::Single(Kind::Sma, p) => NativeIndicator::Sma(Sma::new(p)),
            Spec::Single(Kind::Ema, p) => NativeIndicator::Ema(Ema::new(p)),
            Spec::Single(Kind::Rsi, p) => NativeIndicator::Rsi(Rsi::new(p)),
            Spec::Single(Kind::Stddev, p) => NativeIndicator::Stddev(RollingStdDev::new(p)),
            Spec::Single(Kind::Atr, p) => NativeIndicator::Atr(Atr::new(p)),
            Spec::Macd(f, s, g) => NativeIndicator::Macd(Macd::new(f, s, g)),
            Spec::Bollinger(p, d) => NativeIndicator::Bollinger(Bollinger::new(p, d)),
            _ => {
                return Err(error_object(
                    "invalid scoped indicator specification".into(),
                ));
            }
        };
        Ok(Self { spec, native })
    }

    pub(super) fn update(&mut self, spec: Spec, input: Input) -> Result<DriftValue, DriftValue> {
        if self.spec != spec {
            return Err(error_object(
                "scoped indicator name already has a different configuration".into(),
            ));
        }
        Ok(match (&mut self.native, input) {
            (NativeIndicator::Sma(i), Input::Price(p)) => number(i.update(p)),
            (NativeIndicator::Ema(i), Input::Price(p)) => number(i.update(p)),
            (NativeIndicator::Rsi(i), Input::Price(p)) => number(i.update(p)),
            (NativeIndicator::Stddev(i), Input::Price(p)) => number(i.update(p)),
            (NativeIndicator::Macd(i), Input::Price(p)) => i
                .update(p)
                .map_or(DriftValue::Null, macd_output_to_drift_value),
            (NativeIndicator::Bollinger(i), Input::Price(p)) => i
                .update(p)
                .map_or(DriftValue::Null, bollinger_output_to_drift_value),
            (NativeIndicator::Atr(i), Input::Bar(b)) => number(i.update(b)),
            _ => return Err(error_object("scoped indicator input mismatch".into())),
        })
    }
}
