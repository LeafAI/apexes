//! `signals.*` bindings exposed to Drift Script strategy scripts:
//! `combine`, `target_position`, `rebalance_order` -- pure, stateless

// TODO(Task 4): remove this once `strategy.rs` calls `signals::register_to`.
// The whole module is dead code until that wiring lands.
#![allow(dead_code)]
//! functions that let a `.drift` script turn multiple signal scores into
//! one composite score, map that score to a target position size, and
//! diff the target against the current position into a ready-to-return
//! order array. Unlike `indicators.*` (stateful factories requiring
//! `Arc<Mutex<_>>`, see `indicators.rs`), every function here is callable
//! directly inline inside `on_market_event` -- no top-level
//! `let x = signals.foo();` construction step is needed, since none of
//! these functions carry state across calls.

use std::collections::HashMap;
use std::sync::Arc;

use drift_script::{DriftValue, Interpreter, NativeFn};

/// Installs the `signals` global object into `interp`'s environment.
pub fn register_to(interp: &Interpreter) {
    let mut signals_map: HashMap<String, DriftValue> = HashMap::new();
    signals_map.insert(
        "combine".to_string(),
        DriftValue::NativeFunction(combine_fn()),
    );
    signals_map.insert(
        "target_position".to_string(),
        DriftValue::NativeFunction(target_position_fn()),
    );
    interp
        .env()
        .define_const("signals".to_string(), DriftValue::Object(signals_map));
}

fn error_object(message: String) -> DriftValue {
    let mut map = HashMap::new();
    map.insert("error".to_string(), DriftValue::String(Arc::new(message)));
    DriftValue::Object(map)
}

/// Clamps a signal score to `[-1.0, 1.0]`. Scores drifting slightly
/// outside this range are expected (e.g. a transient indicator spike)
/// and are never treated as an error -- see design spec §三/§四.
fn clamp_score(n: f64) -> f64 {
    n.clamp(-1.0, 1.0)
}

/// Reads a required `f64` field from a `DriftValue::Object`, returning a
/// plain `String` error message (not a `DriftValue` error object --
/// callers wrap it with context via `error_object` at the call site).
fn require_object_field_number(obj: &DriftValue, key: &str, fn_name: &str) -> Result<f64, String> {
    let DriftValue::Object(map) = obj else {
        return Err(format!(
            "{fn_name}: expected an object, got {}",
            obj.type_name()
        ));
    };
    match map.get(key) {
        Some(DriftValue::Number(n)) => Ok(*n),
        Some(other) => Err(format!(
            "{fn_name}: field {key:?} must be a number, got {}",
            other.type_name()
        )),
        None => Err(format!("{fn_name}: missing required field {key:?}")),
    }
}

/// Reads a required `f64` positional argument, returning a `DriftValue`
/// error object (unlike `require_object_field_number`, which returns a
/// plain `String` for the caller to wrap) since `target_position` and
/// `rebalance_order` read top-level positional args directly, with no
/// enclosing object to describe in the error message.
fn require_arg_number(args: &[DriftValue], index: usize, fn_name: &str) -> Result<f64, DriftValue> {
    match args.get(index) {
        Some(DriftValue::Number(n)) => Ok(*n),
        Some(other) => Err(error_object(format!(
            "{fn_name}: argument at position {index} must be a number, got {}",
            other.type_name()
        ))),
        None => Err(error_object(format!(
            "{fn_name}: missing argument at position {index}"
        ))),
    }
}

/// `signals.combine([{score, weight}, ...]) -> number`. Weighted average
/// of clamped scores; empty array or all-zero weights return `0.0`
/// (design spec §三).
fn combine_fn() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            const FN: &str = "signals.combine";
            let Some(DriftValue::Array(items)) = args.first() else {
                return error_object(format!(
                    "{FN}: expected an array argument, got {}",
                    args.first().map(|v| v.type_name()).unwrap_or("nothing")
                ));
            };

            let mut weighted_sum = 0.0_f64;
            let mut weight_sum = 0.0_f64;
            for (i, item) in items.iter().enumerate() {
                let score = match require_object_field_number(item, "score", FN) {
                    Ok(v) => clamp_score(v),
                    Err(msg) => return error_object(format!("{FN}: element at index {i}: {msg}")),
                };
                let weight = match require_object_field_number(item, "weight", FN) {
                    Ok(v) => v,
                    Err(msg) => return error_object(format!("{FN}: element at index {i}: {msg}")),
                };
                if weight < 0.0 {
                    return error_object(format!(
                        "{FN}: element at index {i}: weight must be non-negative, got {weight}"
                    ));
                }
                weighted_sum += score * weight;
                weight_sum += weight;
            }

            if weight_sum == 0.0 {
                return DriftValue::Number(0.0);
            }
            DriftValue::Number(weighted_sum / weight_sum)
        })
    })
}

/// `signals.target_position(score, max_position_size, threshold) -> number`.
/// Dead-zone + linear scaling: `|score| <= threshold` maps to `0`;
/// outside the dead zone, scales linearly from `0` at `threshold` to
/// `max_position_size` at `score == 1.0` (design spec §四).
fn target_position_fn() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            const FN: &str = "signals.target_position";
            let score = match require_arg_number(&args, 0, FN) {
                Ok(v) => clamp_score(v),
                Err(e) => return e,
            };
            let max_position_size = match require_arg_number(&args, 1, FN) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let threshold = match require_arg_number(&args, 2, FN) {
                Ok(v) => v,
                Err(e) => return e,
            };

            if max_position_size < 0.0 {
                return error_object(format!(
                    "{FN}: max_position_size must be non-negative, got {max_position_size}"
                ));
            }
            if threshold < 0.0 {
                return error_object(format!(
                    "{FN}: threshold must be in [0, 1], got {threshold}"
                ));
            }

            if score.abs() <= threshold {
                return DriftValue::Number(0.0);
            }
            let target = if score > threshold {
                max_position_size * (score - threshold) / (1.0 - threshold)
            } else {
                -max_position_size * (-score - threshold) / (1.0 - threshold)
            };
            DriftValue::Number(target)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(f: &NativeFn, args: Vec<DriftValue>) -> DriftValue {
        futures::executor::block_on(f(args))
    }

    fn score_weight(score: f64, weight: f64) -> DriftValue {
        let mut m = HashMap::new();
        m.insert("score".to_string(), DriftValue::Number(score));
        m.insert("weight".to_string(), DriftValue::Number(weight));
        DriftValue::Object(m)
    }

    #[test]
    fn combine_computes_weighted_average() {
        let f = combine_fn();
        let result = call(
            &f,
            vec![DriftValue::Array(vec![
                score_weight(0.8, 2.0),
                score_weight(-0.4, 1.0),
            ])],
        );
        // (0.8*2 + -0.4*1) / (2+1) = 1.2/3 = 0.4
        assert!(matches!(result, DriftValue::Number(n) if (n - 0.4).abs() < 1e-9));
    }

    #[test]
    fn combine_returns_zero_for_empty_array() {
        let f = combine_fn();
        let result = call(&f, vec![DriftValue::Array(vec![])]);
        assert!(matches!(result, DriftValue::Number(n) if n == 0.0));
    }

    #[test]
    fn combine_returns_zero_when_all_weights_zero() {
        let f = combine_fn();
        let result = call(
            &f,
            vec![DriftValue::Array(vec![
                score_weight(0.9, 0.0),
                score_weight(-0.9, 0.0),
            ])],
        );
        assert!(matches!(result, DriftValue::Number(n) if n == 0.0));
    }

    #[test]
    fn combine_clamps_out_of_range_scores() {
        let f = combine_fn();
        let result = call(&f, vec![DriftValue::Array(vec![score_weight(1.5, 1.0)])]);
        // 1.5 clamped to 1.0, weighted average of a single item is just that item's clamped score.
        assert!(matches!(result, DriftValue::Number(n) if (n - 1.0).abs() < 1e-9));
    }

    #[test]
    fn combine_rejects_non_array_argument() {
        let f = combine_fn();
        let result = call(&f, vec![DriftValue::Number(1.0)]);
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn combine_rejects_non_object_element() {
        let f = combine_fn();
        let result = call(&f, vec![DriftValue::Array(vec![DriftValue::Number(1.0)])]);
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn combine_rejects_missing_field() {
        let f = combine_fn();
        let mut incomplete = HashMap::new();
        incomplete.insert("score".to_string(), DriftValue::Number(0.5));
        let result = call(
            &f,
            vec![DriftValue::Array(vec![DriftValue::Object(incomplete)])],
        );
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn combine_rejects_negative_weight() {
        let f = combine_fn();
        let result = call(&f, vec![DriftValue::Array(vec![score_weight(0.5, -1.0)])]);
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn target_position_returns_zero_within_dead_zone() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![
                DriftValue::Number(0.15),
                DriftValue::Number(10.0),
                DriftValue::Number(0.2),
            ],
        );
        assert!(matches!(result, DriftValue::Number(n) if n == 0.0));
    }

    #[test]
    fn target_position_returns_zero_at_dead_zone_boundary() {
        let f = target_position_fn();
        // |score| == threshold exactly -> still zero (spec: "<=").
        let result = call(
            &f,
            vec![
                DriftValue::Number(0.2),
                DriftValue::Number(10.0),
                DriftValue::Number(0.2),
            ],
        );
        assert!(matches!(result, DriftValue::Number(n) if n == 0.0));
    }

    #[test]
    fn target_position_scales_linearly_above_threshold() {
        let f = target_position_fn();
        // max=10, threshold=0.2, score=0.6 -> 10*(0.6-0.2)/(1-0.2) = 5.0
        let result = call(
            &f,
            vec![
                DriftValue::Number(0.6),
                DriftValue::Number(10.0),
                DriftValue::Number(0.2),
            ],
        );
        assert!(matches!(result, DriftValue::Number(n) if (n - 5.0).abs() < 1e-9));
    }

    #[test]
    fn target_position_scales_linearly_below_negative_threshold() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![
                DriftValue::Number(-0.6),
                DriftValue::Number(10.0),
                DriftValue::Number(0.2),
            ],
        );
        assert!(matches!(result, DriftValue::Number(n) if (n - (-5.0)).abs() < 1e-9));
    }

    #[test]
    fn target_position_at_score_one_equals_max_position_size() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![
                DriftValue::Number(1.0),
                DriftValue::Number(10.0),
                DriftValue::Number(0.2),
            ],
        );
        assert!(matches!(result, DriftValue::Number(n) if (n - 10.0).abs() < 1e-9));
    }

    #[test]
    fn target_position_threshold_at_or_above_one_always_zero() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![
                DriftValue::Number(1.0),
                DriftValue::Number(10.0),
                DriftValue::Number(1.0),
            ],
        );
        assert!(matches!(result, DriftValue::Number(n) if n == 0.0));
    }

    #[test]
    fn target_position_clamps_out_of_range_score() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![
                DriftValue::Number(1.5),
                DriftValue::Number(10.0),
                DriftValue::Number(0.2),
            ],
        );
        // 1.5 clamped to 1.0 -> same as target_position_at_score_one_equals_max_position_size.
        assert!(matches!(result, DriftValue::Number(n) if (n - 10.0).abs() < 1e-9));
    }

    #[test]
    fn target_position_rejects_negative_max_position_size() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![
                DriftValue::Number(0.5),
                DriftValue::Number(-1.0),
                DriftValue::Number(0.2),
            ],
        );
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn target_position_rejects_negative_threshold() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![
                DriftValue::Number(0.5),
                DriftValue::Number(10.0),
                DriftValue::Number(-0.1),
            ],
        );
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }
}
