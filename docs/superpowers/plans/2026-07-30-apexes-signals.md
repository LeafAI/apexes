# Apexes-Signals: Multi-Signal Combination & Position Generation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `signals.*` Drift Script binding module to `apexes-strategy` — three pure,
stateless functions (`combine`, `target_position`, `rebalance_order`) that let `.drift` scripts
combine multiple signal scores into one composite score, map that score to a target position
size, and diff the target against the current position into a ready-to-return order array.

**Architecture:** Pure Drift Script binding extension — no new Rust trait, struct, or crate.
`crates/apexes-strategy/src/drift_dsl/signals.rs` registers a `signals` global object
(parallel to the existing `indicators` and `ctx` globals), following the exact same
`NativeFunction`/`error_object` conventions already used by `drift_dsl/indicators.rs`. Unlike
`indicators.*` (stateful factories requiring `Arc<Mutex<_>>`), all three `signals.*` functions
are stateless and callable directly inline inside `on_market_event` — no top-level
`let x = signals.foo();` construction step is needed.

**Tech Stack:** Rust edition 2024, `drift-script` (already a pinned git dependency of
`apexes-strategy`), `rust_decimal` (already a dependency). No new dependencies.

## Global Constraints

- Design spec of record: `docs/superpowers/specs/2026-07-30-apexes-signals-design.md`. If you
  find a mismatch between this plan and that spec while implementing, the spec wins — stop and
  flag it rather than improvising.
- Rust edition `2024`, matching `apexes`' workspace (`Cargo.toml` `[workspace.package] edition =
  "2024"`).
- No new Rust trait, struct, or crate for "signals" — this is exclusively a Drift Script binding
  layer, per the user's explicit scope decision (design spec §一).
- All three `signals.*` functions are pure/stateless: no `Arc<Mutex<_>>`, no factory pattern, no
  top-level script-side construction step required (design spec §六).
- Score inputs are silently clamped to `[-1.0, 1.0]` before use (never treated as an error);
  structural/type errors (wrong argument types, missing fields, negative weights/sizes) return a
  `{error: "..."}` script-level object, never panic, never propagate as a Rust `Result` past the
  binding boundary (design spec §三/§四/§五).
- Every commit in the `apexes` repo uses `git commit -S -s` (signed + signed-off), matching the
  project's established convention. Direct commits to `master` (no branches/PRs).
- `cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets
  -- -D warnings` / `cargo fmt --check` must all pass clean after every task that touches
  compiled code.

---

### Task 1: Implement `signals.combine(items)`

**Files:**
- Create: `crates/apexes-strategy/src/drift_dsl/signals.rs`
- Modify: `crates/apexes-strategy/src/drift_dsl/mod.rs`

**Interfaces:**
- Produces: `pub fn register_to(interp: &Interpreter)` in the new `drift_dsl::signals` module,
  registering a `signals` global object with (initially, this task) one function: `combine`.
  Consumed by Task 4's `strategy.rs` wiring.

- [ ] **Step 1: Create `crates/apexes-strategy/src/drift_dsl/signals.rs`**

```rust
//! `signals.*` bindings exposed to Drift Script strategy scripts:
//! `combine`, `target_position`, `rebalance_order` -- pure, stateless
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
        let result = call(&f, vec![DriftValue::Array(vec![DriftValue::Object(incomplete)])]);
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn combine_rejects_negative_weight() {
        let f = combine_fn();
        let result = call(&f, vec![DriftValue::Array(vec![score_weight(0.5, -1.0)])]);
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }
}
```

- [ ] **Step 2: Wire the new module into `mod.rs`**

Read `crates/apexes-strategy/src/drift_dsl/mod.rs` (current content):

```rust
mod bindings;
mod convert;
mod indicators;
mod strategy;

pub use strategy::DriftScriptStrategy;
```

Edit to add `mod signals;` (no `pub use` needed — `signals::register_to` is only called
internally by `strategy.rs`, matching the existing `indicators` module's pattern):

```rust
mod bindings;
mod convert;
mod indicators;
mod signals;
mod strategy;

pub use strategy::DriftScriptStrategy;
```

- [ ] **Step 3: Run the new module's tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy drift_dsl::signals:: -- --nocapture
```
Expected: `8 passed` (`combine_computes_weighted_average`, `combine_returns_zero_for_empty_array`,
`combine_returns_zero_when_all_weights_zero`, `combine_clamps_out_of_range_scores`,
`combine_rejects_non_array_argument`, `combine_rejects_non_object_element`,
`combine_rejects_missing_field`, `combine_rejects_negative_weight`).

- [ ] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-strategy --all-targets -- -D warnings
cargo fmt --check
```
Expected: no warnings, no formatting diffs. If `cargo fmt --check` reports diffs, run `cargo
fmt` (auto-fix) and re-run both commands to confirm clean.

- [ ] **Step 5: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-strategy/src/drift_dsl/signals.rs crates/apexes-strategy/src/drift_dsl/mod.rs
git commit -S -s -m "feat: add signals.combine Drift Script binding"
git push origin master
```

---

### Task 2: Implement `signals.target_position(score, max_position_size, threshold)`

**Files:**
- Modify: `crates/apexes-strategy/src/drift_dsl/signals.rs`

**Interfaces:**
- Consumes: `clamp_score`, `error_object` (Task 1, same file).
- Produces: `target_position` registered in the `signals` object, consumed by Task 4's
  end-to-end fixture test.

- [ ] **Step 1: Add a `require_number` argument-position helper**

Add this helper function to `crates/apexes-strategy/src/drift_dsl/signals.rs`, right after
`require_object_field_number`:

```rust
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
```

- [ ] **Step 2: Add `target_position_fn`**

Add this function after `combine_fn`:

```rust
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
```

- [ ] **Step 3: Register it in `register_to`**

```rust
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
```

- [ ] **Step 4: Add tests**

Append these tests inside the existing `#[cfg(test)] mod tests` block in `signals.rs`:

```rust
    #[test]
    fn target_position_returns_zero_within_dead_zone() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![DriftValue::Number(0.15), DriftValue::Number(10.0), DriftValue::Number(0.2)],
        );
        assert!(matches!(result, DriftValue::Number(n) if n == 0.0));
    }

    #[test]
    fn target_position_returns_zero_at_dead_zone_boundary() {
        let f = target_position_fn();
        // |score| == threshold exactly -> still zero (spec: "<=").
        let result = call(
            &f,
            vec![DriftValue::Number(0.2), DriftValue::Number(10.0), DriftValue::Number(0.2)],
        );
        assert!(matches!(result, DriftValue::Number(n) if n == 0.0));
    }

    #[test]
    fn target_position_scales_linearly_above_threshold() {
        let f = target_position_fn();
        // max=10, threshold=0.2, score=0.6 -> 10*(0.6-0.2)/(1-0.2) = 5.0
        let result = call(
            &f,
            vec![DriftValue::Number(0.6), DriftValue::Number(10.0), DriftValue::Number(0.2)],
        );
        assert!(matches!(result, DriftValue::Number(n) if (n - 5.0).abs() < 1e-9));
    }

    #[test]
    fn target_position_scales_linearly_below_negative_threshold() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![DriftValue::Number(-0.6), DriftValue::Number(10.0), DriftValue::Number(0.2)],
        );
        assert!(matches!(result, DriftValue::Number(n) if (n - (-5.0)).abs() < 1e-9));
    }

    #[test]
    fn target_position_at_score_one_equals_max_position_size() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![DriftValue::Number(1.0), DriftValue::Number(10.0), DriftValue::Number(0.2)],
        );
        assert!(matches!(result, DriftValue::Number(n) if (n - 10.0).abs() < 1e-9));
    }

    #[test]
    fn target_position_threshold_at_or_above_one_always_zero() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![DriftValue::Number(1.0), DriftValue::Number(10.0), DriftValue::Number(1.0)],
        );
        assert!(matches!(result, DriftValue::Number(n) if n == 0.0));
    }

    #[test]
    fn target_position_clamps_out_of_range_score() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![DriftValue::Number(1.5), DriftValue::Number(10.0), DriftValue::Number(0.2)],
        );
        // 1.5 clamped to 1.0 -> same as target_position_at_score_one_equals_max_position_size.
        assert!(matches!(result, DriftValue::Number(n) if (n - 10.0).abs() < 1e-9));
    }

    #[test]
    fn target_position_rejects_negative_max_position_size() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![DriftValue::Number(0.5), DriftValue::Number(-1.0), DriftValue::Number(0.2)],
        );
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn target_position_rejects_negative_threshold() {
        let f = target_position_fn();
        let result = call(
            &f,
            vec![DriftValue::Number(0.5), DriftValue::Number(10.0), DriftValue::Number(-0.1)],
        );
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }
```

- [ ] **Step 5: Run tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy drift_dsl::signals:: -- --nocapture
```
Expected: `17 passed` (8 from Task 1 + 9 new `target_position` tests).

- [ ] **Step 6: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-strategy --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-verify both commands).

- [ ] **Step 7: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-strategy/src/drift_dsl/signals.rs
git commit -S -s -m "feat: add signals.target_position Drift Script binding"
git push origin master
```

---

### Task 3: Implement `signals.rebalance_order(target_qty, current_qty, symbol, venue, min_order_size)`

**Files:**
- Modify: `crates/apexes-strategy/src/drift_dsl/signals.rs`

**Interfaces:**
- Consumes: `require_arg_number`, `error_object` (Tasks 1-2, same file).
- Produces: `rebalance_order` registered in the `signals` object, consumed by Task 4's
  end-to-end fixture test.

- [ ] **Step 1: Add a `require_arg_string` helper**

Add this helper function right after `require_arg_number`:

```rust
/// Reads a required non-empty string positional argument, returning a
/// `DriftValue` error object on any failure (wrong type, or an empty
/// string, which is never a valid symbol/venue).
fn require_arg_string(args: &[DriftValue], index: usize, fn_name: &str) -> Result<String, DriftValue> {
    match args.get(index) {
        Some(DriftValue::String(s)) if !s.is_empty() => Ok(s.as_str().to_string()),
        Some(DriftValue::String(_)) => Err(error_object(format!(
            "{fn_name}: argument at position {index} must be a non-empty string"
        ))),
        Some(other) => Err(error_object(format!(
            "{fn_name}: argument at position {index} must be a string, got {}",
            other.type_name()
        ))),
        None => Err(error_object(format!(
            "{fn_name}: missing argument at position {index}"
        ))),
    }
}
```

- [ ] **Step 2: Add `rebalance_order_fn`**

Add this function after `target_position_fn`:

```rust
/// `signals.rebalance_order(target_qty, current_qty, symbol, venue,
/// min_order_size) -> array`. Diffs `target_qty` against `current_qty`;
/// if the absolute difference is below `min_order_size`, returns an empty
/// array (nothing to trade); otherwise returns a single-element array
/// containing a Market `OrderIntent`-shaped object, directly usable as
/// `on_market_event`'s return value (design spec §五). `venue`'s
/// legality is NOT checked here -- that is `order_intents_from_drift_value`'s
/// job downstream in `convert.rs`.
fn rebalance_order_fn() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            const FN: &str = "signals.rebalance_order";
            let target_qty = match require_arg_number(&args, 0, FN) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let current_qty = match require_arg_number(&args, 1, FN) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let symbol = match require_arg_string(&args, 2, FN) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let venue = match require_arg_string(&args, 3, FN) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let min_order_size = match require_arg_number(&args, 4, FN) {
                Ok(v) => v,
                Err(e) => return e,
            };

            if min_order_size < 0.0 {
                return error_object(format!(
                    "{FN}: min_order_size must be non-negative, got {min_order_size}"
                ));
            }

            let diff = target_qty - current_qty;
            if diff.abs() < min_order_size {
                return DriftValue::Array(vec![]);
            }

            let mut order = HashMap::new();
            order.insert("symbol".to_string(), DriftValue::String(Arc::new(symbol)));
            order.insert("venue".to_string(), DriftValue::String(Arc::new(venue)));
            order.insert(
                "side".to_string(),
                DriftValue::String(Arc::new(
                    if diff > 0.0 { "buy" } else { "sell" }.to_string(),
                )),
            );
            order.insert(
                "order_type".to_string(),
                DriftValue::String(Arc::new("market".to_string())),
            );
            order.insert("quantity".to_string(), DriftValue::Number(diff.abs()));

            DriftValue::Array(vec![DriftValue::Object(order)])
        })
    })
}
```

- [ ] **Step 3: Register it in `register_to`**

```rust
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
    signals_map.insert(
        "rebalance_order".to_string(),
        DriftValue::NativeFunction(rebalance_order_fn()),
    );
    interp
        .env()
        .define_const("signals".to_string(), DriftValue::Object(signals_map));
}
```

- [ ] **Step 4: Add tests**

Append these tests inside the existing `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn rebalance_order_produces_buy_when_target_above_current() {
        let f = rebalance_order_fn();
        let result = call(
            &f,
            vec![
                DriftValue::Number(5.0),
                DriftValue::Number(2.0),
                DriftValue::String(Arc::new("BTCUSDT".to_string())),
                DriftValue::String(Arc::new("binance".to_string())),
                DriftValue::Number(0.1),
            ],
        );
        let DriftValue::Array(items) = result else { panic!("expected Array") };
        assert_eq!(items.len(), 1);
        let DriftValue::Object(obj) = &items[0] else { panic!("expected Object") };
        assert!(matches!(obj.get("side"), Some(DriftValue::String(s)) if s.as_str() == "buy"));
        assert!(matches!(obj.get("quantity"), Some(DriftValue::Number(n)) if (*n - 3.0).abs() < 1e-9));
        assert!(matches!(obj.get("order_type"), Some(DriftValue::String(s)) if s.as_str() == "market"));
        assert!(matches!(obj.get("symbol"), Some(DriftValue::String(s)) if s.as_str() == "BTCUSDT"));
        assert!(matches!(obj.get("venue"), Some(DriftValue::String(s)) if s.as_str() == "binance"));
    }

    #[test]
    fn rebalance_order_produces_sell_when_target_below_current() {
        let f = rebalance_order_fn();
        let result = call(
            &f,
            vec![
                DriftValue::Number(1.0),
                DriftValue::Number(4.0),
                DriftValue::String(Arc::new("BTCUSDT".to_string())),
                DriftValue::String(Arc::new("binance".to_string())),
                DriftValue::Number(0.1),
            ],
        );
        let DriftValue::Array(items) = result else { panic!("expected Array") };
        assert_eq!(items.len(), 1);
        let DriftValue::Object(obj) = &items[0] else { panic!("expected Object") };
        assert!(matches!(obj.get("side"), Some(DriftValue::String(s)) if s.as_str() == "sell"));
        assert!(matches!(obj.get("quantity"), Some(DriftValue::Number(n)) if (*n - 3.0).abs() < 1e-9));
    }

    #[test]
    fn rebalance_order_returns_empty_array_when_diff_below_min_order_size() {
        let f = rebalance_order_fn();
        let result = call(
            &f,
            vec![
                DriftValue::Number(5.001),
                DriftValue::Number(5.0),
                DriftValue::String(Arc::new("BTCUSDT".to_string())),
                DriftValue::String(Arc::new("binance".to_string())),
                DriftValue::Number(0.01),
            ],
        );
        assert!(matches!(result, DriftValue::Array(items) if items.is_empty()));
    }

    #[test]
    fn rebalance_order_returns_empty_array_when_diff_exactly_equals_min_order_size() {
        let f = rebalance_order_fn();
        // |diff| == min_order_size exactly -> still not traded (strict "<").
        let result = call(
            &f,
            vec![
                DriftValue::Number(5.1),
                DriftValue::Number(5.0),
                DriftValue::String(Arc::new("BTCUSDT".to_string())),
                DriftValue::String(Arc::new("binance".to_string())),
                DriftValue::Number(0.1),
            ],
        );
        assert!(matches!(result, DriftValue::Array(items) if items.is_empty()));
    }

    #[test]
    fn rebalance_order_rejects_negative_min_order_size() {
        let f = rebalance_order_fn();
        let result = call(
            &f,
            vec![
                DriftValue::Number(5.0),
                DriftValue::Number(2.0),
                DriftValue::String(Arc::new("BTCUSDT".to_string())),
                DriftValue::String(Arc::new("binance".to_string())),
                DriftValue::Number(-0.1),
            ],
        );
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn rebalance_order_rejects_non_string_symbol() {
        let f = rebalance_order_fn();
        let result = call(
            &f,
            vec![
                DriftValue::Number(5.0),
                DriftValue::Number(2.0),
                DriftValue::Number(1.0),
                DriftValue::String(Arc::new("binance".to_string())),
                DriftValue::Number(0.1),
            ],
        );
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn rebalance_order_rejects_missing_argument() {
        let f = rebalance_order_fn();
        let result = call(&f, vec![DriftValue::Number(5.0)]);
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }
```

- [ ] **Step 5: Run tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy drift_dsl::signals:: -- --nocapture
```
Expected: `24 passed` (17 from Tasks 1-2 + 7 new `rebalance_order` tests).

- [ ] **Step 6: Run the full `apexes-strategy` unit test suite**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy --lib
```
Expected: all prior unit tests (30 from before this plan) plus the 24 new `signals` tests = `54
passed`, 0 failed.

- [ ] **Step 7: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-strategy --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-verify both commands).

- [ ] **Step 8: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-strategy/src/drift_dsl/signals.rs
git commit -S -s -m "feat: add signals.rebalance_order Drift Script binding"
git push origin master
```

---

### Task 4: Wire `signals::register_to` into `DriftScriptStrategy` + end-to-end fixture test

**Files:**
- Modify: `crates/apexes-strategy/src/drift_dsl/strategy.rs`
- Create: `crates/apexes-strategy/tests/fixtures/uses_signals.drift`
- Modify: `crates/apexes-strategy/tests/drift_script_strategy.rs`

**Interfaces:**
- Consumes: `signals::register_to` (Tasks 1-3), `apexes_strategy::drift_dsl::DriftScriptStrategy`,
  `apexes_strategy::{Strategy, StrategyContext}` (pre-existing).
- Produces: nothing new consumed by later tasks — this is the plan's final behavioral proof
  point.

- [ ] **Step 1: Register `signals::register_to` in `DriftScriptStrategy::from_file`**

In `crates/apexes-strategy/src/drift_dsl/strategy.rs`, find this existing line block:

```rust
        let mut interpreter = Interpreter::new();
        drift_script_math::register_to(&interpreter);
        crate::drift_dsl::indicators::register_to(&interpreter);
```

Replace it with:

```rust
        let mut interpreter = Interpreter::new();
        drift_script_math::register_to(&interpreter);
        crate::drift_dsl::indicators::register_to(&interpreter);
        crate::drift_dsl::signals::register_to(&interpreter);
```

- [ ] **Step 2: Verify it compiles**

```bash
cd ~/src/oss/apexes
cargo build -p apexes-strategy
```
Expected: succeeds, 0 errors.

- [ ] **Step 3: Create the fixture script**

```bash
mkdir -p ~/src/oss/apexes/crates/apexes-strategy/tests/fixtures
cat > ~/src/oss/apexes/crates/apexes-strategy/tests/fixtures/uses_signals.drift << 'EOF'
let sma = indicators.sma(3);

function on_market_event(event, ctx) {
  let price = event.type == "bar" ? event.close : event.price;
  let sma_value = sma.update(price);

  if (sma_value == null) {
    // Still warming up -- treat as neutral, produce no orders this tick.
    return [];
  }

  let raw_score = price > sma_value ? 0.8 : -0.8;
  let composite = signals.combine([{ score: raw_score, weight: 1 }]);
  let target_qty = signals.target_position(composite, 10, 0.2);
  let position = ctx.get_position(event.symbol);

  return signals.rebalance_order(target_qty, position.quantity, event.symbol, event.venue, 0.01);
}
EOF
```

**Note:** this fixture reuses the `event.type == "bar" ? event.close : event.price` ternary
pattern already established in `uses_indicators.drift` (drift-script does not support `??`
nullish coalescing — confirmed unsupported in a prior plan, see
`docs/superpowers/plans/2026-07-30-apexes-indicators.md` Task 7's fixture for the identical
pattern and rationale).

- [ ] **Step 4: Add the driver tests**

Read `crates/apexes-strategy/tests/drift_script_strategy.rs` to confirm its existing
`with_runtime`/`fixture_path`/`empty_ctx`/`sample_trade_event` helpers are still present (they
should be, from the prior `apexes-indicators` plan), then append these two new `#[test]`
functions at the end of the file (same sync-`#[test]` + `with_runtime` pattern as all existing
tests in this file, for the same `Handle::current().block_on()`-inside-`#[tokio::test]` panic
reason already documented on `with_runtime`'s doc comment):

```rust
#[test]
fn uses_signals_produces_no_order_during_indicator_warmup() {
    with_runtime(|| {
        let mut strategy =
            DriftScriptStrategy::from_file(&fixture_path("uses_signals.drift")).unwrap();
        let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };
        let event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(10),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });

        // SMA(3) warm-up: first call must produce no orders.
        let intents = strategy.on_market_event(&event, &ctx);
        assert_eq!(intents.len(), 0, "should produce no orders while the SMA is still warming up");
    });
}

#[test]
fn uses_signals_produces_rebalance_order_once_warm() {
    with_runtime(|| {
        let mut strategy =
            DriftScriptStrategy::from_file(&fixture_path("uses_signals.drift")).unwrap();
        let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };

        // Prices [10, 11, 12]: SMA(3) becomes warm on the 3rd call
        // (average of [10,11,12] = 11), and price (12) > sma (11), so
        // raw_score = 0.8 -> composite = 0.8 -> target_position(0.8, 10,
        // 0.2) = 10*(0.8-0.2)/(1-0.2) = 7.5 -> rebalance_order(7.5, 0,
        // ..., 0.01) -> a single Buy order for quantity 7.5.
        for price in [dec!(10), dec!(11)] {
            let event = MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price,
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: Utc::now(),
            });
            let intents = strategy.on_market_event(&event, &ctx);
            assert_eq!(intents.len(), 0, "still warming up at price {price}");
        }

        let third_event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(12),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        let intents = strategy.on_market_event(&third_event, &ctx);
        assert_eq!(intents.len(), 1, "SMA should be warm by the 3rd tick and trigger a rebalance");
        assert_eq!(intents[0].side, Side::Buy);
        assert!(
            (intents[0].quantity - dec!(7.5)).abs() < dec!(0.0001),
            "expected quantity ~7.5, got {}",
            intents[0].quantity
        );
    });
}
```

- [ ] **Step 5: Run the new tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy --test drift_script_strategy -- --nocapture
```
Expected: `11 passed` (9 from the prior `apexes-indicators` plan + 2 new). If
`uses_signals_produces_rebalance_order_once_warm`'s quantity assertion fails, print the actual
`intents[0].quantity` value (temporarily) and re-derive the expected math by hand from the
fixture script's exact formula (`target_position` then `rebalance_order` against a starting
position of `0`) before changing the test's expected value -- the golden value `7.5` was
computed independently in this plan (`10*(0.8-0.2)/(1-0.2) = 7.5`) and cross-checked against
Task 2's `target_position_scales_linearly_above_threshold` unit test's identical formula
pattern.

- [ ] **Step 6: Run the full `apexes-strategy` test suite**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy
```
Expected: `54 passed` (unit tests: 30 pre-existing + 24 new `signals` tests) + `11 passed`
(integration tests), 0 failed.

- [ ] **Step 7: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-strategy --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run `cargo test -p apexes-strategy`
+ both clippy/fmt commands once more to confirm nothing regressed).

- [ ] **Step 8: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-strategy/src/drift_dsl/strategy.rs crates/apexes-strategy/tests
git commit -S -s -m "feat: wire signals.* into DriftScriptStrategy and add end-to-end fixture"
git push origin master
```

---

### Task 5: Final whole-effort verification

**Files:** None created/modified — this task only runs verification commands, updates
`docs/FEATURES.md`, and confirms the end state.

- [ ] **Step 1: Full workspace build**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [ ] **Step 2: Full workspace test suite**

```bash
cd ~/src/oss/apexes
cargo test --workspace
```
Expected: all tests across all crates pass. Report the exact total broken down by crate,
explicitly confirming `apexes-core`/`apexes-market-data`/`apexes-storage`/`apexes-backtest`/
`apexes-indicators` are completely unaffected by this plan (this plan only touches
`apexes-strategy`).

- [ ] **Step 3: Full workspace clippy**

```bash
cd ~/src/oss/apexes
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: 0 warnings.

- [ ] **Step 4: Full workspace fmt check**

```bash
cd ~/src/oss/apexes
cargo fmt --check
```
Expected: 0 diffs.

- [ ] **Step 5: Update `docs/FEATURES.md`**

Read `docs/FEATURES.md`, find row `2.3` (`Multi-signal combination & position generation
engine`, currently `🔲 Not Started`). Update its Status to `✅ Done` and its Notes column to:
```
Pure Drift Script binding (no new Rust trait/crate, per explicit scope decision): signals.combine (weighted-average signal scores), signals.target_position (dead-zone + linear scaling to a target position size), signals.rebalance_order (diffs target vs current position into a ready-to-return order array) -- all stateless, callable inline in on_market_event.
```

Update the Summary counts table (increment `✅ Done` by 1 to `21`, decrement `🔲 Not Started` by
1 to `48`, keep `**Total**` at `70`) and the "Current active work" paragraph to note sub-project
`#4`'s third phase (`2.3`) is now also complete alongside `2.1`/`2.2`; leave "what's next"
unstated since `2.4`/`2.5`/PyO3/`#5` are all still open options — don't guess which one the user
picks next.

- [ ] **Step 6: Commit the FEATURES.md update**

```bash
cd ~/src/oss/apexes
git add docs/FEATURES.md
git commit -S -s -m "docs: mark multi-signal combination & position generation engine complete in FEATURES.md"
git push origin master
```

- [ ] **Step 7: Report final summary**

Summarize: `signals.rs`'s final line count and function list, total test count (24 unit tests
in `signals.rs` + 2 new integration tests in `drift_script_strategy.rs`), confirmation that
`apexes-strategy`'s pre-existing 30 unit + 9 integration tests still pass unchanged, and the
`apexes` commit SHAs for Tasks 1-5. No further commit needed for this verification-only task.

---

## Plan Self-Review Notes

**Spec coverage:** Design spec §一 (scope: pure Drift Script binding, no new Rust trait/crate) →
every task avoids introducing any new Rust abstraction beyond plain functions in `signals.rs`;
confirmed no `struct Signal`/`trait Signal`/new crate anywhere in this plan. §二 (file layout:
`signals.rs` new, `mod.rs`/`strategy.rs` modified, new fixture + driver test) → Tasks 1 (create
+ wire `mod.rs`), 4 (wire `strategy.rs` + fixture + driver test). §三 (`combine`: weighted
average, clamp scores, empty/all-zero-weight → 0, validation errors) → Task 1, all 8 test cases
map 1:1 to the spec's enumerated behaviors. §四 (`target_position`: dead zone, linear scaling,
`threshold >= 1.0` edge case, clamp, validation errors) → Task 2, all 9 test cases including the
explicit `threshold >= 1.0` boundary test. §五 (`rebalance_order`: diff, min_order_size
threshold with strict `<`, Market-only order shape, venue legality deferred downstream,
validation errors) → Task 3, all 7 test cases including the exact-equality boundary test
(`rebalance_order_returns_empty_array_when_diff_exactly_equals_min_order_size`) that pins down
the spec's "strict less-than" clarification. §六 (stateless, no `Arc<Mutex<_>>`, no factory
pattern, reuse `indicators.rs`'s `error_object`/argument-helper conventions) → every function in
Tasks 1-3 is a plain `Arc<dyn Fn...>` with no captured mutable state, confirmed by inspecting the
actual `indicators.rs` source before writing this plan. §七 (unit tests for all three functions
covering every enumerated case, one end-to-end fixture combining an indicator score with
`ctx.get_position()`) → Tasks 1-3 (unit), Task 4 (end-to-end fixture + 2 driver tests: warm-up
no-op, and a hand-derived golden-value rebalance order). §八 (non-goals: no Rust trait/crate, no
`#4.4`/`#4.5`, no PyO3, no non-Market order types in `rebalance_order`, no cross-symbol
optimization) — confirmed none of Tasks 1-5 do any of these.

**Placeholder scan:** No "TBD"/"TODO" strings in any task. Every code step shows complete,
copy-pasteable code. Test counts in each task's "Run tests" step were recounted against the
actual test functions listed in that task's code block before finalizing (e.g. Task 1's 8
`combine` tests, Task 2's 9 `target_position` tests, Task 3's 7 `rebalance_order` tests).

**Type consistency:** `signals.rs`'s helper function names (`error_object`, `clamp_score`,
`require_object_field_number`, `require_arg_number`, `require_arg_string`) are introduced once
each (Tasks 1-3) and referenced identically in every subsequent task that needs them — no
renaming drift across tasks. `combine_fn`/`target_position_fn`/`rebalance_order_fn`'s exact
names match their registration keys (`"combine"`/`"target_position"`/`"rebalance_order"`) in
every version of `register_to` shown across Tasks 1-3 (each task's `register_to` snippet is the
complete, cumulative function body, not a diff fragment, so there is no ambiguity about the
final state). The Task 4 fixture script's field names (`event.type`, `event.close`,
`event.price`, `event.symbol`, `event.venue`) match `convert.rs`'s `market_event_to_drift_value`
output exactly (verified against the existing `uses_indicators.drift` fixture's identical
pattern from the prior plan, and against `convert.rs`'s `MarketEvent::Trade` branch which sets
`"type": "trade"` and a `"price"` field, not `"close"`).
