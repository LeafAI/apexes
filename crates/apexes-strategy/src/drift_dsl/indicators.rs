//! `indicators.*` bindings exposed to Drift Script strategy scripts:
//! factory functions (`sma`/`ema`/`rsi`/`macd`/`bollinger`/`atr`/
//! `stddev`) that each construct an `apexes_indicators::*` instance and
//! return a script-facing object with a single `update` method. Unlike
//! `ctx.*` (rebuilt fresh on every `on_market_event` call, see
//! `bindings.rs`), these factories are meant to be called ONCE at a
//! script's top level (`let sma = indicators.sma(20);`) so the returned
//! object's `Arc<Mutex<_>>`-held state persists across ticks -- calling a
//! factory function itself is what constructs the indicator; the
//! returned `update` closure only ever mutates that one shared instance.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use apexes_indicators::{
    Atr, AtrInput, Bollinger, BollingerOutput, Ema, Indicator, Macd, MacdOutput, RollingStdDev,
    Rsi, Sma,
};
use drift_script::{DriftValue, Interpreter, NativeFn};
use rust_decimal::Decimal;

/// Installs the `indicators` global object into `interp`'s environment.
pub fn register_to(interp: &Interpreter) {
    let mut indicators_map: HashMap<String, DriftValue> = HashMap::new();
    indicators_map.insert("sma".to_string(), DriftValue::NativeFunction(sma_factory()));
    indicators_map.insert("ema".to_string(), DriftValue::NativeFunction(ema_factory()));
    indicators_map.insert("rsi".to_string(), DriftValue::NativeFunction(rsi_factory()));
    indicators_map.insert(
        "macd".to_string(),
        DriftValue::NativeFunction(macd_factory()),
    );
    indicators_map.insert(
        "bollinger".to_string(),
        DriftValue::NativeFunction(bollinger_factory()),
    );
    indicators_map.insert("atr".to_string(), DriftValue::NativeFunction(atr_factory()));
    indicators_map.insert(
        "stddev".to_string(),
        DriftValue::NativeFunction(stddev_factory()),
    );
    interp
        .env()
        .define_const("indicators".to_string(), DriftValue::Object(indicators_map));
}

fn error_object(message: String) -> DriftValue {
    let mut map = HashMap::new();
    map.insert("error".to_string(), DriftValue::String(Arc::new(message)));
    DriftValue::Object(map)
}

/// Extracts a positive `usize` period from `args[index]`, or an
/// `error_object` describing what went wrong (missing arg, non-number,
/// non-positive, non-integer).
fn require_positive_usize(
    args: &[DriftValue],
    index: usize,
    fn_name: &str,
) -> Result<usize, DriftValue> {
    match args.get(index) {
        Some(DriftValue::Number(n)) if *n > 0.0 && n.fract() == 0.0 => Ok(*n as usize),
        Some(DriftValue::Number(n)) => Err(error_object(format!(
            "{fn_name}: argument at position {index} must be a positive integer, got {n}"
        ))),
        Some(other) => Err(error_object(format!(
            "{fn_name}: argument at position {index} must be a number, got {}",
            other.type_name()
        ))),
        None => Err(error_object(format!(
            "{fn_name}: missing argument at position {index}"
        ))),
    }
}

fn require_number(args: &[DriftValue], index: usize, fn_name: &str) -> Result<Decimal, DriftValue> {
    match args.get(index) {
        Some(DriftValue::Number(n)) => Decimal::try_from(*n).map_err(|_| {
            error_object(format!(
                "{fn_name}: argument at position {index} is not a finite number ({n})"
            ))
        }),
        Some(other) => Err(error_object(format!(
            "{fn_name}: argument at position {index} must be a number, got {}",
            other.type_name()
        ))),
        None => Err(error_object(format!(
            "{fn_name}: missing argument at position {index}"
        ))),
    }
}

fn optional_decimal_field(obj: &DriftValue, key: &str, fn_name: &str) -> Result<Decimal, String> {
    let DriftValue::Object(map) = obj else {
        return Err(format!(
            "{fn_name}: expected an object argument, got {}",
            obj.type_name()
        ));
    };
    match map.get(key) {
        Some(DriftValue::Number(n)) => Decimal::try_from(*n)
            .map_err(|_| format!("{fn_name}: field {key:?} is not a finite number ({n})")),
        Some(other) => Err(format!(
            "{fn_name}: field {key:?} must be a number, got {}",
            other.type_name()
        )),
        None => Err(format!("{fn_name}: missing required field {key:?}")),
    }
}

/// Wraps a single-value indicator's `update` in a `DriftValue::Object`
/// with one `update` method: `Option<Decimal>` becomes `Number`/`Null`.
fn make_update_object<I>(indicator: I) -> DriftValue
where
    I: Indicator<Input = Decimal, Output = Option<Decimal>> + Send + 'static,
{
    let state = Arc::new(Mutex::new(indicator));
    let update: NativeFn = Arc::new(move |args| {
        let state = Arc::clone(&state);
        Box::pin(async move {
            let price = match require_number(&args, 0, "indicator.update") {
                Ok(p) => p,
                Err(e) => return e,
            };
            let Ok(mut guard) = state.lock() else {
                return error_object("indicator.update: internal lock poisoned".to_string());
            };
            match guard.update(price) {
                Some(v) => DriftValue::Number(v.try_into().unwrap_or(f64::NAN)),
                None => DriftValue::Null,
            }
        })
    });
    let mut obj = HashMap::new();
    obj.insert("update".to_string(), DriftValue::NativeFunction(update));
    DriftValue::Object(obj)
}

fn sma_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_positive_usize(&args, 0, "indicators.sma") {
                Ok(period) => make_update_object(Sma::new(period)),
                Err(e) => e,
            }
        })
    })
}

fn ema_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_positive_usize(&args, 0, "indicators.ema") {
                Ok(period) => make_update_object(Ema::new(period)),
                Err(e) => e,
            }
        })
    })
}

fn rsi_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_positive_usize(&args, 0, "indicators.rsi") {
                Ok(period) => make_update_object(Rsi::new(period)),
                Err(e) => e,
            }
        })
    })
}

fn stddev_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_positive_usize(&args, 0, "indicators.stddev") {
                Ok(period) => make_update_object(RollingStdDev::new(period)),
                Err(e) => e,
            }
        })
    })
}

fn macd_output_to_drift_value(output: MacdOutput) -> DriftValue {
    let mut obj = HashMap::new();
    obj.insert(
        "macd".to_string(),
        DriftValue::Number(output.macd.try_into().unwrap_or(f64::NAN)),
    );
    obj.insert(
        "signal".to_string(),
        DriftValue::Number(output.signal.try_into().unwrap_or(f64::NAN)),
    );
    obj.insert(
        "histogram".to_string(),
        DriftValue::Number(output.histogram.try_into().unwrap_or(f64::NAN)),
    );
    DriftValue::Object(obj)
}

fn macd_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            let fast = match require_positive_usize(&args, 0, "indicators.macd") {
                Ok(p) => p,
                Err(e) => return e,
            };
            let slow = match require_positive_usize(&args, 1, "indicators.macd") {
                Ok(p) => p,
                Err(e) => return e,
            };
            let signal = match require_positive_usize(&args, 2, "indicators.macd") {
                Ok(p) => p,
                Err(e) => return e,
            };
            let state = Arc::new(Mutex::new(Macd::new(fast, slow, signal)));
            let update: NativeFn = Arc::new(move |args| {
                let state = Arc::clone(&state);
                Box::pin(async move {
                    let price = match require_number(&args, 0, "macd.update") {
                        Ok(p) => p,
                        Err(e) => return e,
                    };
                    let Ok(mut guard) = state.lock() else {
                        return error_object("macd.update: internal lock poisoned".to_string());
                    };
                    match guard.update(price) {
                        Some(output) => macd_output_to_drift_value(output),
                        None => DriftValue::Null,
                    }
                })
            });
            let mut obj = HashMap::new();
            obj.insert("update".to_string(), DriftValue::NativeFunction(update));
            DriftValue::Object(obj)
        })
    })
}

fn bollinger_output_to_drift_value(output: BollingerOutput) -> DriftValue {
    let mut obj = HashMap::new();
    obj.insert(
        "upper".to_string(),
        DriftValue::Number(output.upper.try_into().unwrap_or(f64::NAN)),
    );
    obj.insert(
        "middle".to_string(),
        DriftValue::Number(output.middle.try_into().unwrap_or(f64::NAN)),
    );
    obj.insert(
        "lower".to_string(),
        DriftValue::Number(output.lower.try_into().unwrap_or(f64::NAN)),
    );
    DriftValue::Object(obj)
}

fn bollinger_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            let period = match require_positive_usize(&args, 0, "indicators.bollinger") {
                Ok(p) => p,
                Err(e) => return e,
            };
            let num_std = match require_number(&args, 1, "indicators.bollinger") {
                Ok(n) => n,
                Err(e) => return e,
            };
            let state = Arc::new(Mutex::new(Bollinger::new(period, num_std)));
            let update: NativeFn = Arc::new(move |args| {
                let state = Arc::clone(&state);
                Box::pin(async move {
                    let price = match require_number(&args, 0, "bollinger.update") {
                        Ok(p) => p,
                        Err(e) => return e,
                    };
                    let Ok(mut guard) = state.lock() else {
                        return error_object(
                            "bollinger.update: internal lock poisoned".to_string(),
                        );
                    };
                    match guard.update(price) {
                        Some(output) => bollinger_output_to_drift_value(output),
                        None => DriftValue::Null,
                    }
                })
            });
            let mut obj = HashMap::new();
            obj.insert("update".to_string(), DriftValue::NativeFunction(update));
            DriftValue::Object(obj)
        })
    })
}

fn atr_factory() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            let period = match require_positive_usize(&args, 0, "indicators.atr") {
                Ok(p) => p,
                Err(e) => return e,
            };
            let state = Arc::new(Mutex::new(Atr::new(period)));
            let update: NativeFn = Arc::new(move |args| {
                let state = Arc::clone(&state);
                Box::pin(async move {
                    let Some(bar_obj) = args.first() else {
                        return error_object("atr.update: missing bar argument".to_string());
                    };
                    let high = match optional_decimal_field(bar_obj, "high", "atr.update") {
                        Ok(v) => v,
                        Err(msg) => return error_object(msg),
                    };
                    let low = match optional_decimal_field(bar_obj, "low", "atr.update") {
                        Ok(v) => v,
                        Err(msg) => return error_object(msg),
                    };
                    let close = match optional_decimal_field(bar_obj, "close", "atr.update") {
                        Ok(v) => v,
                        Err(msg) => return error_object(msg),
                    };
                    let Ok(mut guard) = state.lock() else {
                        return error_object("atr.update: internal lock poisoned".to_string());
                    };
                    match guard.update(AtrInput { high, low, close }) {
                        Some(v) => DriftValue::Number(v.try_into().unwrap_or(f64::NAN)),
                        None => DriftValue::Null,
                    }
                })
            });
            let mut obj = HashMap::new();
            obj.insert("update".to_string(), DriftValue::NativeFunction(update));
            DriftValue::Object(obj)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(f: &NativeFn, args: Vec<DriftValue>) -> DriftValue {
        futures::executor::block_on(f(args))
    }

    #[test]
    fn sma_factory_rejects_zero_period() {
        let factory = sma_factory();
        let result = call(&factory, vec![DriftValue::Number(0.0)]);
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn sma_update_returns_null_during_warmup_then_number() {
        let factory = sma_factory();
        let sma_obj = call(&factory, vec![DriftValue::Number(3.0)]);
        let DriftValue::Object(obj) = sma_obj else {
            panic!("expected Object")
        };
        let DriftValue::NativeFunction(update) = obj.get("update").unwrap().clone() else {
            panic!("expected update NativeFunction")
        };
        assert!(matches!(
            call(&update, vec![DriftValue::Number(10.0)]),
            DriftValue::Null
        ));
        assert!(matches!(
            call(&update, vec![DriftValue::Number(11.0)]),
            DriftValue::Null
        ));
        let third = call(&update, vec![DriftValue::Number(12.0)]);
        assert!(matches!(third, DriftValue::Number(n) if (n - 11.0).abs() < 1e-9));
    }

    #[test]
    fn macd_update_returns_object_with_three_fields_once_warm() {
        let factory = macd_factory();
        let macd_obj = call(
            &factory,
            vec![
                DriftValue::Number(2.0),
                DriftValue::Number(3.0),
                DriftValue::Number(2.0),
            ],
        );
        let DriftValue::Object(obj) = macd_obj else {
            panic!("expected Object")
        };
        let DriftValue::NativeFunction(update) = obj.get("update").unwrap().clone() else {
            panic!("expected update NativeFunction")
        };
        for price in [10.0, 12.0, 11.0] {
            call(&update, vec![DriftValue::Number(price)]);
        }
        let fourth = call(&update, vec![DriftValue::Number(15.0)]);
        let DriftValue::Object(output) = fourth else {
            panic!("expected Object output")
        };
        assert!(output.contains_key("macd"));
        assert!(output.contains_key("signal"));
        assert!(output.contains_key("histogram"));
    }

    #[test]
    fn atr_update_reads_high_low_close_from_object_argument() {
        let factory = atr_factory();
        let atr_obj = call(&factory, vec![DriftValue::Number(2.0)]);
        let DriftValue::Object(obj) = atr_obj else {
            panic!("expected Object")
        };
        let DriftValue::NativeFunction(update) = obj.get("update").unwrap().clone() else {
            panic!("expected update NativeFunction")
        };
        let bar_arg = |h: f64, l: f64, c: f64| {
            let mut m = HashMap::new();
            m.insert("high".to_string(), DriftValue::Number(h));
            m.insert("low".to_string(), DriftValue::Number(l));
            m.insert("close".to_string(), DriftValue::Number(c));
            DriftValue::Object(m)
        };
        assert!(matches!(
            call(&update, vec![bar_arg(10.0, 8.0, 9.0)]),
            DriftValue::Null
        ));
        let second = call(&update, vec![bar_arg(12.0, 9.0, 11.0)]);
        assert!(matches!(second, DriftValue::Number(n) if (n - 2.5).abs() < 1e-9));
    }

    #[test]
    fn atr_update_rejects_non_object_argument() {
        let factory = atr_factory();
        let atr_obj = call(&factory, vec![DriftValue::Number(2.0)]);
        let DriftValue::Object(obj) = atr_obj else {
            panic!("expected Object")
        };
        let DriftValue::NativeFunction(update) = obj.get("update").unwrap().clone() else {
            panic!("expected update NativeFunction")
        };
        let result = call(&update, vec![DriftValue::Number(9.0)]);
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }
}
