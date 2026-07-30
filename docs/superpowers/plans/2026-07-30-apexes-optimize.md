# Apexes-Optimize: Parameter Optimization Interface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `param(name, default)` Drift Script binding + `DriftScriptStrategy::from_file_with_params`
to `apexes-strategy`, then build a new `apexes-optimize` crate providing a pluggable
`ParameterOptimizer` interface with two implementations: `GridSearchOptimizer` (in-process
exhaustive search) and `ExternalProcessOptimizer` (delegates to an external subprocess, any
language, over a newline-delimited-JSON protocol -- zero `unsafe`, no native dylib loading).

**Architecture:** Scripts declare tunable parameters inline via `param(name, default)`; a Rust
caller overrides them per backtest run via `from_file_with_params`. `apexes-optimize` (new crate,
depends on `apexes-strategy` + `apexes-backtest`) defines `SearchSpace` (parameter name -> candidate
values), an `Evaluator` trait (caller-supplied "run one backtest, return a score" callback), and a
`ParameterOptimizer` trait implemented by both `GridSearchOptimizer` and `ExternalProcessOptimizer`
-- both share the same `Evaluator`, so callers can swap optimizers without changing their
evaluation logic.

**Tech Stack:** Rust edition 2024, `drift-script` (already pinned in `apexes-strategy`), `tokio`
(`process`/`io-util` features for subprocess I/O), `serde`/`serde_json` (JSON protocol messages),
`async-trait` (already a workspace dependency), `rust_decimal`.

## Global Constraints

- Design spec of record: `docs/superpowers/specs/2026-07-30-apexes-optimize-design.md`. If you
  find a mismatch between this plan and that spec while implementing, the spec wins -- stop and
  flag it rather than improvising.
- Rust edition `2024`, matching `apexes`' workspace (`Cargo.toml` `[workspace.package] edition =
  "2024"`).
- **No native dynamic-library (`.so`/`.dylib`) plugin loading, ever, in this plan** -- explicitly
  excluded per the design spec (no stable Rust ABI, would require `unsafe` code, violates the
  project's "unsafe requires explicit justification and approval" rule). The only pluggability
  mechanism in this plan is `ExternalProcessOptimizer`'s subprocess + JSON protocol.
- `param(name, default)` and `from_file_with_params` are scoped to `DriftScriptStrategy` only --
  no changes to the Rust-native `Strategy` trait's parameterization.
- `GridSearchOptimizer` and `ExternalProcessOptimizer` both execute sequentially (no
  parallel/concurrent backtest execution in this plan).
- Every commit in the `apexes` repo uses `git commit -S -s` (signed + signed-off), matching the
  project's established convention. Direct commits to `master` (no branches/PRs).
- `cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets
  -- -D warnings` / `cargo fmt --check` must all pass clean after every task that touches
  compiled code.

---

### Task 1: `param(name, default)` Drift Script binding + `DriftScriptStrategy::from_file_with_params`

**Files:**
- Create: `crates/apexes-strategy/src/drift_dsl/params.rs`
- Modify: `crates/apexes-strategy/src/drift_dsl/mod.rs`
- Modify: `crates/apexes-strategy/src/drift_dsl/strategy.rs`

**Interfaces:**
- Produces: `pub fn register_to(interp: &Interpreter, overrides: HashMap<String, Decimal>)` in
  the new `drift_dsl::params` module, and `pub fn
  DriftScriptStrategy::from_file_with_params(path: &Path, overrides: HashMap<String, Decimal>) ->
  Result<Self, StrategyError>` in `drift_dsl::strategy`. Consumed by Task 6's end-to-end test and
  by `apexes-optimize`'s `Evaluator` implementations (Task 5).

- [x] **Step 1: Write `crates/apexes-strategy/src/drift_dsl/params.rs`**

```rust
//! `param(name, default)` binding exposed to Drift Script strategy
//! scripts, letting a script declare a tunable parameter and read either
//! a caller-supplied override or its own literal default in one call:
//! `let sma_period = param("sma_period", 20);`. Unlike `indicators.*`/
//! `signals.*` (which are the same for every construction of a
//! `DriftScriptStrategy`), the `overrides` map this function consults is
//! supplied per-construction via
//! [`crate::drift_dsl::DriftScriptStrategy::from_file_with_params`], so
//! the exact same script can be re-run with different parameter values
//! across many backtests (the `apexes-optimize` crate's primary use
//! case).

use std::collections::HashMap;
use std::sync::Arc;

use drift_script::{DriftValue, Interpreter, NativeFn};
use rust_decimal::Decimal;

/// Installs the `param` global function into `interp`'s environment.
/// `overrides` is consulted once per `param(name, default)` call (not
/// cached) -- in practice a script calls `param(...)` once per
/// parameter, at the top level, and stores the result in a `let`
/// binding, so re-consulting `overrides` on every call has no observable
/// cost.
pub fn register_to(interp: &Interpreter, overrides: HashMap<String, Decimal>) {
    let overrides = Arc::new(overrides);
    let param_fn: NativeFn = Arc::new(move |args| {
        let overrides = Arc::clone(&overrides);
        Box::pin(async move {
            const FN: &str = "param";
            let name = match args.first() {
                Some(DriftValue::String(s)) => s.as_str().to_string(),
                Some(other) => {
                    return error_object(format!(
                        "{FN}: first argument (name) must be a string, got {}",
                        other.type_name()
                    ));
                }
                None => return error_object(format!("{FN}: missing argument \"name\"")),
            };
            let default = match args.get(1) {
                Some(DriftValue::Number(n)) => *n,
                Some(other) => {
                    return error_object(format!(
                        "{FN}: second argument (default) must be a number, got {}",
                        other.type_name()
                    ));
                }
                None => return error_object(format!("{FN}: missing argument \"default\"")),
            };

            match overrides.get(&name) {
                Some(value) => DriftValue::Number((*value).try_into().unwrap_or(default)),
                None => DriftValue::Number(default),
            }
        })
    });
    interp.env().define_const("param".to_string(), DriftValue::NativeFunction(param_fn));
}

fn error_object(message: String) -> DriftValue {
    let mut map = HashMap::new();
    map.insert("error".to_string(), DriftValue::String(Arc::new(message)));
    DriftValue::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use drift_script::ScriptParser;

    fn call(f: &NativeFn, args: Vec<DriftValue>) -> DriftValue {
        futures::executor::block_on(f(args))
    }

    /// Extracts the registered `param` `NativeFn` back out of the
    /// interpreter's environment, for direct unit testing without
    /// parsing a whole script.
    fn get_param_fn(interp: &Interpreter) -> NativeFn {
        match interp.env().get("param") {
            Some(DriftValue::NativeFunction(f)) => f,
            other => panic!("expected param to be registered as a NativeFunction, got {other:?}"),
        }
    }

    #[test]
    fn returns_override_value_when_present() {
        let interp = Interpreter::new();
        let mut overrides = HashMap::new();
        overrides.insert("sma_period".to_string(), rust_decimal_macros::dec!(30));
        register_to(&interp, overrides);

        let f = get_param_fn(&interp);
        let result = call(
            &f,
            vec![DriftValue::String(Arc::new("sma_period".to_string())), DriftValue::Number(20.0)],
        );
        assert!(matches!(result, DriftValue::Number(n) if n == 30.0));
    }

    #[test]
    fn returns_default_when_override_absent() {
        let interp = Interpreter::new();
        register_to(&interp, HashMap::new());

        let f = get_param_fn(&interp);
        let result = call(
            &f,
            vec![DriftValue::String(Arc::new("sma_period".to_string())), DriftValue::Number(20.0)],
        );
        assert!(matches!(result, DriftValue::Number(n) if n == 20.0));
    }

    #[test]
    fn rejects_non_string_name() {
        let interp = Interpreter::new();
        register_to(&interp, HashMap::new());

        let f = get_param_fn(&interp);
        let result = call(&f, vec![DriftValue::Number(1.0), DriftValue::Number(20.0)]);
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[test]
    fn rejects_non_number_default() {
        let interp = Interpreter::new();
        register_to(&interp, HashMap::new());

        let f = get_param_fn(&interp);
        let result = call(
            &f,
            vec![
                DriftValue::String(Arc::new("sma_period".to_string())),
                DriftValue::String(Arc::new("20".to_string())),
            ],
        );
        assert!(matches!(result, DriftValue::Object(obj) if obj.contains_key("error")));
    }

    #[tokio::test]
    async fn works_end_to_end_via_a_real_script() {
        let interp_cell = std::cell::RefCell::new(Interpreter::new());
        let mut overrides = HashMap::new();
        overrides.insert("threshold".to_string(), rust_decimal_macros::dec!(0.5));
        register_to(&interp_cell.borrow(), overrides);

        let stmts = ScriptParser::new()
            .parse("let threshold = param(\"threshold\", 0.2);")
            .unwrap();
        interp_cell.borrow_mut().evaluate(&stmts).await.unwrap();

        let value = interp_cell.borrow().env().get("threshold").unwrap();
        assert!(matches!(value, DriftValue::Number(n) if n == 0.5));
    }
}
```

**Note:** the `get_param_fn` test helper assumes `Interpreter::env()` returns something whose
`.get("param")` yields a `DriftValue` you can pattern-match on `DriftValue::NativeFunction(f)` to
extract the callable `NativeFn` -- this mirrors the exact same `interp.env().get(name)` pattern
already used in `crates/apexes-strategy/src/drift_dsl/strategy.rs`'s `from_file` (to resolve
`on_market_event`). If `Interpreter::new()` requires being wrapped differently to call
`.env()` before any `evaluate()` call, adjust the test setup to match `Interpreter`'s actual API
(check `crates/apexes-strategy/src/drift_dsl/strategy.rs` and
`crates/apexes-strategy/src/drift_dsl/indicators.rs` for the established call patterns) --the
core assertions (override present -> override value; override absent -> default value; wrong
argument types -> error object) must hold regardless of exact setup mechanics.

- [x] **Step 2: Wire the new module into `mod.rs`**

Read `crates/apexes-strategy/src/drift_dsl/mod.rs` (current content):

```rust
mod bindings;
mod convert;
mod indicators;
mod signals;
mod strategy;
```

Edit to add `mod params;` (alphabetical, matching the existing ordering convention):

```rust
mod bindings;
mod convert;
mod indicators;
mod params;
mod signals;
mod strategy;
```

- [x] **Step 3: Add `DriftScriptStrategy::from_file_with_params`**

In `crates/apexes-strategy/src/drift_dsl/strategy.rs`, the existing `from_file` method reads:

```rust
    pub fn from_file(path: &Path) -> Result<Self, StrategyError> {
        let source = std::fs::read_to_string(path).map_err(|source| StrategyError::Io {
            path: PathBuf::from(path),
            source,
        })?;

        let stmts = ScriptParser::new()
            .parse(&source)
            .map_err(|e| StrategyError::Parse(e.to_string()))?;

        let mut interpreter = Interpreter::new();
        drift_script_math::register_to(&interpreter);
        crate::drift_dsl::indicators::register_to(&interpreter);
        crate::drift_dsl::signals::register_to(&interpreter);

        let handle = tokio::runtime::Handle::current();
        handle
            .block_on(interpreter.evaluate(&stmts))
            .map_err(|e| StrategyError::Parse(e.to_string()))?;

        let on_market_event_fn = interpreter
            .env()
            .get("on_market_event")
            .ok_or(StrategyError::MissingEntryPoint)?;

        if !matches!(on_market_event_fn, DriftValue::Function(_)) {
            return Err(StrategyError::EntryPointNotCallable);
        }

        Ok(Self {
            interpreter,
            on_market_event_fn,
        })
    }
```

Replace this single method with two methods: `from_file` becomes a thin wrapper around a new
private `from_file_inner` that both public constructors share, and `from_file_with_params` is the
new public entry point:

```rust
    /// Reads and parses `path`, executes its top-level statements (so a
    /// `function on_market_event(event, ctx) { ... }` declaration enters
    /// the interpreter's environment), then resolves that function's
    /// value. Returns `Err` if the file cannot be read, fails to parse, or
    /// does not define a callable `on_market_event` function at the top
    /// level -- construction fails closed, never producing a
    /// partially-usable instance.
    pub fn from_file(path: &Path) -> Result<Self, StrategyError> {
        Self::from_file_inner(path, std::collections::HashMap::new())
    }

    /// Like `from_file`, but registers a `param(name, default)` native
    /// function (see `crate::drift_dsl::params::register_to`) that
    /// returns `overrides.get(name)` when present, otherwise `default`.
    /// Intended for parameter-optimization callers (`apexes-optimize`)
    /// that need to re-construct the same script with different
    /// parameter values across many backtest runs.
    pub fn from_file_with_params(
        path: &Path,
        overrides: std::collections::HashMap<String, rust_decimal::Decimal>,
    ) -> Result<Self, StrategyError> {
        Self::from_file_inner(path, overrides)
    }

    fn from_file_inner(
        path: &Path,
        overrides: std::collections::HashMap<String, rust_decimal::Decimal>,
    ) -> Result<Self, StrategyError> {
        let source = std::fs::read_to_string(path).map_err(|source| StrategyError::Io {
            path: PathBuf::from(path),
            source,
        })?;

        let stmts = ScriptParser::new()
            .parse(&source)
            .map_err(|e| StrategyError::Parse(e.to_string()))?;

        let mut interpreter = Interpreter::new();
        drift_script_math::register_to(&interpreter);
        crate::drift_dsl::indicators::register_to(&interpreter);
        crate::drift_dsl::signals::register_to(&interpreter);
        crate::drift_dsl::params::register_to(&interpreter, overrides);

        let handle = tokio::runtime::Handle::current();
        handle
            .block_on(interpreter.evaluate(&stmts))
            .map_err(|e| StrategyError::Parse(e.to_string()))?;

        let on_market_event_fn = interpreter
            .env()
            .get("on_market_event")
            .ok_or(StrategyError::MissingEntryPoint)?;

        if !matches!(on_market_event_fn, DriftValue::Function(_)) {
            return Err(StrategyError::EntryPointNotCallable);
        }

        Ok(Self {
            interpreter,
            on_market_event_fn,
        })
    }
```

- [x] **Step 4: Verify it compiles**

```bash
cd ~/src/oss/apexes
cargo build -p apexes-strategy
```
Expected: succeeds, 0 errors.

- [x] **Step 5: Run the new module's tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy drift_dsl::params:: -- --nocapture
```
Expected: `5 passed` (`returns_override_value_when_present`, `returns_default_when_override_absent`,
`rejects_non_string_name`, `rejects_non_number_default`, `works_end_to_end_via_a_real_script`).

- [x] **Step 6: Run the full `apexes-strategy` test suite**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy
```
Expected: all prior tests (54 unit + 11 integration, per the prior `apexes-signals` plan) plus the
5 new `params` tests = `59 passed` (unit) + `11 passed` (integration), 0 failed.

- [x] **Step 7: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-strategy --all-targets -- -D warnings
cargo fmt --check
```
Expected: no warnings, no formatting diffs. If `cargo fmt --check` reports diffs, run `cargo fmt`
(auto-fix) and re-run both commands to confirm clean.

- [x] **Step 8: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-strategy/src/drift_dsl/params.rs crates/apexes-strategy/src/drift_dsl/mod.rs crates/apexes-strategy/src/drift_dsl/strategy.rs
git commit -S -s -m "feat: add param() Drift Script binding and DriftScriptStrategy::from_file_with_params"
git push origin master
```

---

### Task 2: `apexes-backtest` blanket `&T` impl for `HistoricalMarketDataSource`

**Files:**
- Modify: `crates/apexes-backtest/src/market_data_source.rs`

**Interfaces:**
- Produces: `impl<T: HistoricalMarketDataSource + Sync> HistoricalMarketDataSource for &T`,
  consumed by Task 5's `GridSearchOptimizer`/`ExternalProcessOptimizer` (both pass
  `&InMemoryMarketDataSource` directly to `apexes_backtest::run_backtest`).

- [x] **Step 1: Read the existing `HistoricalMarketDataSource` trait definition**

Read `crates/apexes-backtest/src/market_data_source.rs`'s current top of file to confirm the
exact trait signature (as of the prior `apexes-indicators`/`apexes-signals` plans, unchanged by
this task):

```rust
pub trait HistoricalMarketDataSource {
    fn events(&self) -> impl Future<Output = Result<Vec<MarketEvent>, ApexesError>> + Send;
}
```

- [x] **Step 2: Add the blanket `&T` impl**

Add this immediately after the trait definition (before `InMemoryMarketDataSource`'s own impl
block):

```rust
impl<T: HistoricalMarketDataSource + Sync> HistoricalMarketDataSource for &T {
    async fn events(&self) -> Result<Vec<MarketEvent>, ApexesError> {
        (**self).events().await
    }
}
```

- [x] **Step 3: Write a test proving a `&InMemoryMarketDataSource` can be passed to `run_backtest`**

Append this test to the existing `#[cfg(test)] mod tests` block at the bottom of
`crates/apexes-backtest/src/market_data_source.rs`:

```rust
    #[tokio::test]
    async fn reference_to_in_memory_source_implements_the_trait() {
        let events = vec![trade_at("A", 1)];
        let source = InMemoryMarketDataSource::new(events);

        // This only compiles if `&InMemoryMarketDataSource` implements
        // `HistoricalMarketDataSource` -- the blanket `&T` impl this
        // task adds.
        let result = (&source).events().await.unwrap();

        assert_eq!(result.len(), 1);
    }
```

- [x] **Step 4: Run the tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-backtest market_data_source:: -- --nocapture
```
Expected: `4 passed` (`in_memory_source_sorts_events_chronologically`,
`in_memory_source_handles_empty_input`, `event_timestamp_extracts_bar_open_time_not_close_time`,
`reference_to_in_memory_source_implements_the_trait`).

- [x] **Step 5: Run the full `apexes-backtest` test suite**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-backtest
```
Expected: all prior tests (69 unit + 3 integration) plus the 1 new test = `70 passed` (unit) + `3
passed` (integration), 0 failed.

- [x] **Step 6: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-backtest --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-verify both commands).

- [x] **Step 7: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-backtest/src/market_data_source.rs
git commit -S -s -m "feat: add blanket &T HistoricalMarketDataSource impl for apexes-backtest"
git push origin master
```

---

### Task 3: Scaffold `apexes-optimize` crate + `SearchSpace`

**Files:**
- Modify: `Cargo.toml` (workspace root) -- add `"crates/apexes-optimize"` to `members`
- Create: `crates/apexes-optimize/Cargo.toml`
- Create: `crates/apexes-optimize/src/lib.rs`
- Create: `crates/apexes-optimize/src/search_space.rs`

**Interfaces:**
- Produces: `pub struct SearchSpace` with `pub fn new() -> Self`, `pub fn with_parameter(self,
  name: impl Into<String>, values: Vec<Decimal>) -> Self`, `pub fn combinations(&self) ->
  Vec<HashMap<String, Decimal>>`. Re-exported from `apexes_optimize::SearchSpace`. Consumed by
  Task 4's `ParameterOptimizer` trait and Task 5's optimizer implementations.

- [x] **Step 1: Add the new crate to the workspace**

```bash
cd ~/src/oss/apexes
cat Cargo.toml
```

Edit the `members` list to add `"crates/apexes-optimize"`:

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage", "crates/apexes-backtest", "crates/apexes-strategy", "crates/apexes-indicators", "crates/apexes-optimize"]
```

- [x] **Step 2: Create `crates/apexes-optimize/Cargo.toml`**

```toml
[package]
name = "apexes-optimize"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-strategy = { path = "../apexes-strategy" }
apexes-backtest = { path = "../apexes-backtest" }
async-trait = { workspace = true }
rust_decimal = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tokio = { workspace = true, features = ["process", "io-util", "macros", "rt-multi-thread"] }
tracing = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
```

- [x] **Step 3: Write `SearchSpace`'s failing tests**

Create `crates/apexes-optimize/src/search_space.rs`:

```rust
//! `SearchSpace`: maps each tunable parameter's name to its list of
//! candidate values, and expands the Cartesian product of all
//! combinations for `GridSearchOptimizer` (or is sent as-is, without
//! expansion, to an external process via `ExternalProcessOptimizer`).

use rust_decimal::Decimal;
use std::collections::HashMap;

/// Maps each tunable parameter's name to its list of candidate values.
#[derive(Debug, Clone, Default)]
pub struct SearchSpace {
    parameters: HashMap<String, Vec<Decimal>>,
}

impl SearchSpace {
    pub fn new() -> Self {
        Self { parameters: HashMap::new() }
    }

    /// Adds a parameter and its candidate values. Calling this again
    /// with the same `name` replaces the previous candidate list
    /// (last-write-wins, matching `HashMap::insert`'s own semantics).
    pub fn with_parameter(mut self, name: impl Into<String>, values: Vec<Decimal>) -> Self {
        self.parameters.insert(name.into(), values);
        self
    }

    /// Returns a reference to the underlying parameter-name ->
    /// candidate-values map, e.g. for `ExternalProcessOptimizer` to
    /// serialize the raw search space (not the expanded combinations)
    /// to an external process.
    pub fn parameters(&self) -> &HashMap<String, Vec<Decimal>> {
        &self.parameters
    }

    /// Returns every combination as a `HashMap<String, Decimal>`, one
    /// per grid point. Returns an empty `Vec` if there are no
    /// parameters, or if any single parameter's candidate list is empty
    /// (standard Cartesian-product semantics: any empty dimension makes
    /// the whole product empty) -- "no parameters to search" and "an
    /// empty search space" are both treated as "zero combinations to
    /// evaluate", never as "one combination with fewer keys".
    pub fn combinations(&self) -> Vec<HashMap<String, Decimal>> {
        if self.parameters.is_empty() || self.parameters.values().any(|values| values.is_empty()) {
            return Vec::new();
        }

        let mut names: Vec<&String> = self.parameters.keys().collect();
        names.sort();

        let mut combinations: Vec<HashMap<String, Decimal>> = vec![HashMap::new()];
        for name in names {
            let values = &self.parameters[name];
            let mut next = Vec::with_capacity(combinations.len() * values.len());
            for existing in &combinations {
                for value in values {
                    let mut combo = existing.clone();
                    combo.insert(name.clone(), *value);
                    next.push(combo);
                }
            }
            combinations = next;
        }
        combinations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn single_parameter_produces_one_combination_per_value() {
        let space = SearchSpace::new().with_parameter("x", vec![dec!(1), dec!(2), dec!(3)]);
        let combos = space.combinations();
        assert_eq!(combos.len(), 3);
        let mut values: Vec<Decimal> = combos.iter().map(|c| c["x"]).collect();
        values.sort();
        assert_eq!(values, vec![dec!(1), dec!(2), dec!(3)]);
    }

    #[test]
    fn two_parameters_produce_cartesian_product() {
        let space = SearchSpace::new()
            .with_parameter("x", vec![dec!(1), dec!(2)])
            .with_parameter("y", vec![dec!(10), dec!(20), dec!(30)]);
        let combos = space.combinations();
        // 2 * 3 = 6 combinations.
        assert_eq!(combos.len(), 6);
        for combo in &combos {
            assert!(combo.contains_key("x"));
            assert!(combo.contains_key("y"));
        }
        // Every (x, y) pair should be present exactly once.
        let mut pairs: Vec<(Decimal, Decimal)> = combos.iter().map(|c| (c["x"], c["y"])).collect();
        pairs.sort();
        let mut expected = vec![
            (dec!(1), dec!(10)),
            (dec!(1), dec!(20)),
            (dec!(1), dec!(30)),
            (dec!(2), dec!(10)),
            (dec!(2), dec!(20)),
            (dec!(2), dec!(30)),
        ];
        expected.sort();
        assert_eq!(pairs, expected);
    }

    #[test]
    fn empty_search_space_produces_no_combinations() {
        let space = SearchSpace::new();
        assert!(space.combinations().is_empty());
    }

    #[test]
    fn parameter_with_empty_candidate_list_produces_no_combinations() {
        let space = SearchSpace::new()
            .with_parameter("x", vec![dec!(1), dec!(2)])
            .with_parameter("y", vec![]);
        assert!(space.combinations().is_empty());
    }

    #[test]
    fn with_parameter_called_twice_for_same_name_replaces_candidates() {
        let space = SearchSpace::new()
            .with_parameter("x", vec![dec!(1), dec!(2)])
            .with_parameter("x", vec![dec!(99)]);
        let combos = space.combinations();
        assert_eq!(combos.len(), 1);
        assert_eq!(combos[0]["x"], dec!(99));
    }
}
```

- [x] **Step 4: Create `crates/apexes-optimize/src/lib.rs`**

```rust
//! Apexes parameter optimization interface: `SearchSpace` (parameter
//! name -> candidate values), a pluggable `ParameterOptimizer` trait,
//! and two implementations -- `GridSearchOptimizer` (in-process
//! exhaustive search) and `ExternalProcessOptimizer` (delegates to an
//! external subprocess over a newline-delimited-JSON protocol, per the
//! project roadmap's "pluggable external ML service" requirement --
//! deliberately NOT a native dynamic-library plugin, since Rust has no
//! stable ABI and that approach would require `unsafe` code).

pub mod search_space;

pub use search_space::SearchSpace;
```

- [x] **Step 5: Run the tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-optimize
```
Expected: `5 passed` (`single_parameter_produces_one_combination_per_value`,
`two_parameters_produce_cartesian_product`, `empty_search_space_produces_no_combinations`,
`parameter_with_empty_candidate_list_produces_no_combinations`,
`with_parameter_called_twice_for_same_name_replaces_candidates`).

- [x] **Step 6: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-optimize --all-targets -- -D warnings
cargo fmt --check
```
Expected: no warnings, no formatting diffs (auto-fix with `cargo fmt` if needed, then re-verify).

- [x] **Step 7: Commit**

```bash
cd ~/src/oss/apexes
git add Cargo.toml crates/apexes-optimize
git commit -S -s -m "feat: scaffold apexes-optimize crate with SearchSpace"
git push origin master
```

---

### Task 4: `ParameterOptimizer` + `Evaluator` traits

**Files:**
- Create: `crates/apexes-optimize/src/evaluator.rs`
- Create: `crates/apexes-optimize/src/optimizer.rs`
- Modify: `crates/apexes-optimize/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_optimize::SearchSpace` (Task 3), `apexes_backtest::InMemoryMarketDataSource`
  (already exists).
- Produces: `pub trait Evaluator` (with `async fn evaluate(&mut self, params:
  &HashMap<String, Decimal>, market_data: &InMemoryMarketDataSource) -> Result<Decimal,
  String>`), `pub struct EvaluatedCombination { pub params: HashMap<String, Decimal>, pub score:
  Option<Decimal> }`, `pub struct OptimizationResult { pub best: Option<EvaluatedCombination>,
  pub all_results: Vec<EvaluatedCombination>, pub error: Option<String> }`, `pub trait
  ParameterOptimizer` (with `async fn optimize(&self, search_space: &SearchSpace, evaluator:
  &mut dyn Evaluator) -> OptimizationResult`). Consumed by Task 5's `GridSearchOptimizer` and
  Task 6's `ExternalProcessOptimizer`.

- [x] **Step 1: Write `crates/apexes-optimize/src/evaluator.rs`**

```rust
//! [`Evaluator`]: the caller-supplied "run one backtest with these
//! parameters, return a score" callback that every `ParameterOptimizer`
//! implementation drives.

use apexes_backtest::InMemoryMarketDataSource;
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Evaluates a single parameter combination and returns its objective
/// score. Implementations decide how to construct the strategy (e.g.
/// `apexes_strategy::drift_dsl::DriftScriptStrategy::from_file_with_params`),
/// run the backtest (`apexes_backtest::run_backtest`), and compute the
/// score from the resulting `BacktestReport` (e.g. via a caller-supplied
/// `Fn(&apexes_backtest::BacktestReport) -> Decimal` objective closure)
/// -- this trait only fixes the input (`params`, `market_data`) and
/// output (`Decimal` score) shape, not any of that internal wiring.
#[async_trait]
pub trait Evaluator {
    /// Returns `Err(message)` if constructing the strategy or running
    /// the backtest failed for this parameter combination (e.g. a
    /// malformed `.drift` script, or a backtest configuration error).
    /// Every `ParameterOptimizer` implementation treats this as the
    /// worst possible score for ranking purposes, logs the error via
    /// `tracing::error!`, and continues with the remaining combinations
    /// -- one bad combination must not abort the whole search.
    async fn evaluate(
        &mut self,
        params: &HashMap<String, Decimal>,
        market_data: &InMemoryMarketDataSource,
    ) -> Result<Decimal, String>;
}
```

- [x] **Step 2: Write `crates/apexes-optimize/src/optimizer.rs`**

```rust
//! [`ParameterOptimizer`]: the pluggable parameter-search algorithm
//! interface. Two implementations exist in this crate:
//! [`crate::grid_search::GridSearchOptimizer`] (in-process exhaustive
//! search, added in a later task) and
//! [`crate::external_process::ExternalProcessOptimizer`] (delegates to
//! an external subprocess, added in a later task).

use crate::evaluator::Evaluator;
use crate::search_space::SearchSpace;
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// One parameter combination's evaluated result, whether it succeeded
/// or failed.
#[derive(Debug, Clone, PartialEq)]
pub struct EvaluatedCombination {
    pub params: HashMap<String, Decimal>,
    /// `None` if this combination's `Evaluator::evaluate` call returned
    /// `Err` -- excluded from "best" selection, but still recorded here
    /// so callers can inspect which combinations failed. The error
    /// message itself is logged via `tracing::error!` at the time of
    /// failure, not stored on this struct.
    pub score: Option<Decimal>,
}

/// The full result of an optimization run: the best-scoring combination
/// (if at least one combination succeeded) plus every combination's
/// result, in the order they were evaluated.
#[derive(Debug, Clone, PartialEq)]
pub struct OptimizationResult {
    /// `None` only if every single combination failed (or the search
    /// space was empty) -- callers must handle this case explicitly
    /// rather than assuming a best combination always exists.
    pub best: Option<EvaluatedCombination>,
    pub all_results: Vec<EvaluatedCombination>,
    /// `Some(message)` only for a run-level (not per-combination)
    /// failure -- e.g. `ExternalProcessOptimizer`'s subprocess crashed,
    /// exited unexpectedly, or sent malformed/unrecognized protocol
    /// messages. `GridSearchOptimizer` never sets this field (it has no
    /// such failure mode). Distinct from per-combination failures: this
    /// field describes something that invalidates the ENTIRE run, not
    /// just one parameter combination's result.
    pub error: Option<String>,
}

/// A pluggable parameter-search algorithm. This trait boundary is what
/// lets `GridSearchOptimizer` and `ExternalProcessOptimizer` share the
/// same `Evaluator` and caller code; a future third implementation only
/// needs to implement this trait, no other code changes.
#[async_trait]
pub trait ParameterOptimizer {
    async fn optimize(
        &self,
        search_space: &SearchSpace,
        evaluator: &mut dyn Evaluator,
    ) -> OptimizationResult;
}
```

- [x] **Step 3: Update `lib.rs` re-exports**

```rust
pub mod evaluator;
pub mod optimizer;
pub mod search_space;

pub use evaluator::Evaluator;
pub use optimizer::{EvaluatedCombination, OptimizationResult, ParameterOptimizer};
pub use search_space::SearchSpace;
```

- [x] **Step 4: Verify it compiles**

```bash
cd ~/src/oss/apexes
cargo build -p apexes-optimize
```
Expected: succeeds, 0 errors. (No new tests in this task -- `Evaluator`/`ParameterOptimizer` are
pure trait definitions with no implementation to unit test yet; Tasks 5-6 exercise them.)

- [x] **Step 5: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-optimize --all-targets -- -D warnings
cargo fmt --check
```
Expected: no warnings, no formatting diffs.

- [x] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-optimize/src/evaluator.rs crates/apexes-optimize/src/optimizer.rs crates/apexes-optimize/src/lib.rs
git commit -S -s -m "feat: add ParameterOptimizer and Evaluator traits to apexes-optimize"
git push origin master
```

---

### Task 5: `GridSearchOptimizer`

**Files:**
- Create: `crates/apexes-optimize/src/grid_search.rs`
- Modify: `crates/apexes-optimize/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_optimize::{Evaluator, EvaluatedCombination, OptimizationResult,
  ParameterOptimizer, SearchSpace}` (Tasks 3-4), `apexes_backtest::InMemoryMarketDataSource`,
  `apexes_core::MarketEvent`.
- Produces: `pub struct GridSearchOptimizer` with `pub fn new(events: Vec<MarketEvent>) ->
  Self`, implementing `ParameterOptimizer`. Consumed by Task 7's end-to-end test.

- [x] **Step 1: Write `crates/apexes-optimize/src/grid_search.rs`**

```rust
//! [`GridSearchOptimizer`]: exhaustively evaluates every combination in
//! a [`SearchSpace`]'s Cartesian product, sequentially, against a single
//! pre-loaded set of market events shared across all combinations.

use apexes_backtest::InMemoryMarketDataSource;
use apexes_core::MarketEvent;
use async_trait::async_trait;

use crate::evaluator::Evaluator;
use crate::optimizer::{EvaluatedCombination, OptimizationResult, ParameterOptimizer};
use crate::search_space::SearchSpace;

/// Exhaustively evaluates every combination in a `SearchSpace`'s
/// Cartesian product, sequentially, against a single pre-loaded set of
/// market events (loaded once by the caller, not re-fetched per
/// combination -- avoids repeated storage reads / event-vec clones
/// across the many backtests a grid search runs).
pub struct GridSearchOptimizer {
    market_data: InMemoryMarketDataSource,
}

impl GridSearchOptimizer {
    pub fn new(events: Vec<MarketEvent>) -> Self {
        Self { market_data: InMemoryMarketDataSource::new(events) }
    }
}

#[async_trait]
impl ParameterOptimizer for GridSearchOptimizer {
    async fn optimize(
        &self,
        search_space: &SearchSpace,
        evaluator: &mut dyn Evaluator,
    ) -> OptimizationResult {
        let mut all_results = Vec::new();
        let mut best: Option<EvaluatedCombination> = None;

        for params in search_space.combinations() {
            let score = match evaluator.evaluate(&params, &self.market_data).await {
                Ok(score) => Some(score),
                Err(message) => {
                    tracing::error!("grid search combination {params:?} failed: {message}");
                    None
                }
            };

            let is_new_best = match (score, best.as_ref().and_then(|b| b.score)) {
                (Some(candidate), Some(current_best)) => candidate > current_best,
                (Some(_), None) => true,
                (None, _) => false,
            };

            let combination = EvaluatedCombination { params, score };
            if is_new_best {
                best = Some(combination.clone());
            }
            all_results.push(combination);
        }

        OptimizationResult { best, all_results, error: None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evaluator::Evaluator;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    /// A mock `Evaluator` that returns a deterministic score derived
    /// directly from the `"x"` parameter (or fails for a specific
    /// sentinel value), so `GridSearchOptimizer`'s selection logic can
    /// be tested without running any real backtest.
    struct MockEvaluator {
        fail_on_x: Option<Decimal>,
    }

    #[async_trait]
    impl Evaluator for MockEvaluator {
        async fn evaluate(
            &mut self,
            params: &HashMap<String, Decimal>,
            _market_data: &InMemoryMarketDataSource,
        ) -> Result<Decimal, String> {
            let x = params["x"];
            if Some(x) == self.fail_on_x {
                return Err(format!("simulated failure for x={x}"));
            }
            Ok(x)
        }
    }

    #[tokio::test]
    async fn finds_the_highest_scoring_combination() {
        let optimizer = GridSearchOptimizer::new(vec![]);
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1), dec!(5), dec!(3)]);
        let mut evaluator = MockEvaluator { fail_on_x: None };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.error, None);
        let best = result.best.expect("expected a best combination");
        assert_eq!(best.params["x"], dec!(5));
        assert_eq!(best.score, Some(dec!(5)));
    }

    #[tokio::test]
    async fn all_results_contains_every_combination() {
        let optimizer = GridSearchOptimizer::new(vec![]);
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1), dec!(2), dec!(3)]);
        let mut evaluator = MockEvaluator { fail_on_x: None };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.all_results.len(), 3);
        let mut scores: Vec<Decimal> =
            result.all_results.iter().map(|c| c.score.unwrap()).collect();
        scores.sort();
        assert_eq!(scores, vec![dec!(1), dec!(2), dec!(3)]);
    }

    #[tokio::test]
    async fn a_failed_combination_is_excluded_from_best_but_recorded() {
        let optimizer = GridSearchOptimizer::new(vec![]);
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1), dec!(5), dec!(3)]);
        // "5" would otherwise be the best score, but evaluation fails for it.
        let mut evaluator = MockEvaluator { fail_on_x: Some(dec!(5)) };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        let best = result.best.expect("expected a best combination among the non-failed ones");
        assert_eq!(best.params["x"], dec!(3));
        assert_eq!(result.all_results.len(), 3);
        let failed = result.all_results.iter().find(|c| c.params["x"] == dec!(5)).unwrap();
        assert_eq!(failed.score, None);
    }

    #[tokio::test]
    async fn best_is_none_when_every_combination_fails() {
        let optimizer = GridSearchOptimizer::new(vec![]);
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1)]);
        let mut evaluator = MockEvaluator { fail_on_x: Some(dec!(1)) };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.best, None);
        assert_eq!(result.all_results.len(), 1);
        assert_eq!(result.error, None);
    }

    #[tokio::test]
    async fn empty_search_space_produces_empty_result() {
        let optimizer = GridSearchOptimizer::new(vec![]);
        let search_space = SearchSpace::new();
        let mut evaluator = MockEvaluator { fail_on_x: None };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.best, None);
        assert!(result.all_results.is_empty());
        assert_eq!(result.error, None);
    }
}
```

- [x] **Step 2: Update `lib.rs`**

```rust
pub mod evaluator;
pub mod grid_search;
pub mod optimizer;
pub mod search_space;

pub use evaluator::Evaluator;
pub use grid_search::GridSearchOptimizer;
pub use optimizer::{EvaluatedCombination, OptimizationResult, ParameterOptimizer};
pub use search_space::SearchSpace;
```

- [x] **Step 3: Run the tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-optimize
```
Expected: `10 passed` (5 from Task 3's `search_space` tests + 5 new `grid_search` tests:
`finds_the_highest_scoring_combination`, `all_results_contains_every_combination`,
`a_failed_combination_is_excluded_from_best_but_recorded`,
`best_is_none_when_every_combination_fails`, `empty_search_space_produces_empty_result`).

- [x] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-optimize --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-verify both commands).

- [x] **Step 5: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-optimize/src/grid_search.rs crates/apexes-optimize/src/lib.rs
git commit -S -s -m "feat: add GridSearchOptimizer to apexes-optimize"
git push origin master
```

---

### Task 6: `ExternalProcessOptimizer`

**Files:**
- Create: `crates/apexes-optimize/src/external_process.rs`
- Create: `crates/apexes-optimize/tests/fixtures/mock_optimizer.py`
- Modify: `crates/apexes-optimize/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_optimize::{Evaluator, EvaluatedCombination, OptimizationResult,
  ParameterOptimizer, SearchSpace}` (Tasks 3-4).
- Produces: `pub struct ExternalProcessOptimizer` with `pub fn new(command: impl Into<String>,
  args: Vec<String>, events: Vec<MarketEvent>) -> Self`, implementing `ParameterOptimizer`.
  Consumed by Task 7's end-to-end test (indirectly -- Task 7 exercises `GridSearchOptimizer`;
  this task's own tests exercise `ExternalProcessOptimizer` directly against the mock Python
  script).

- [x] **Step 1: Check that `python3` is available for the mock external optimizer**

```bash
which python3
```
Expected: a path is printed (e.g. `/usr/bin/python3`). This plan's mock "external optimizer" is
written in Python specifically to prove the protocol works across a language boundary (matching
the design spec's "any language, e.g. Python + scikit-optimize" framing) -- it does not run any
real optimization algorithm, just deterministic test logic. If `python3` is unavailable in the
execution environment, substitute a small shell script or a tiny separate Rust binary that
speaks the identical protocol (same JSON message shapes, same newline-delimited framing) and
adjust Step 2's fixture file and Step 6's spawn command accordingly -- the test assertions in
Step 7 do not depend on which language the mock is written in.

- [x] **Step 2: Write the mock external optimizer fixture**

```bash
mkdir -p ~/src/oss/apexes/crates/apexes-optimize/tests/fixtures
cat > ~/src/oss/apexes/crates/apexes-optimize/tests/fixtures/mock_optimizer.py << 'EOF'
#!/usr/bin/env python3
"""Deterministic mock "external optimizer" for testing
ExternalProcessOptimizer's protocol handling. Not a real optimization
algorithm -- reads the search space, evaluates every combination of the
"x" parameter in ascending order, and reports whichever had the highest
score as the winner. Speaks newline-delimited JSON over stdin/stdout,
per docs/superpowers/specs/2026-07-30-apexes-optimize-design.md §七.
"""
import json
import sys


def read_line():
    line = sys.stdin.readline()
    if not line:
        return None
    return json.loads(line)


def write_line(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def main():
    initial = read_line()
    search_space = initial["search_space"]
    x_values = search_space["x"]

    best_params = None
    best_score = None
    for x in sorted(x_values):
        write_line({"evaluate": {"x": x}})
        response = read_line()
        if "error" in response:
            continue
        score = response["score"]
        if best_score is None or score > best_score:
            best_score = score
            best_params = {"x": x}

    if best_params is None:
        write_line({"done": None})
    else:
        write_line({"done": {"best_params": best_params, "best_score": best_score}})


if __name__ == "__main__":
    main()
EOF
chmod +x ~/src/oss/apexes/crates/apexes-optimize/tests/fixtures/mock_optimizer.py
```

- [x] **Step 3: Write `crates/apexes-optimize/src/external_process.rs`**

```rust
//! [`ExternalProcessOptimizer`]: delegates parameter search to an
//! external subprocess (any language) speaking a newline-delimited-JSON
//! protocol, per the project roadmap's "pluggable external ML service"
//! requirement. Deliberately NOT a native dynamic-library plugin -- see
//! docs/superpowers/specs/2026-07-30-apexes-optimize-design.md §一/§七
//! for why (no stable Rust ABI, would require `unsafe` code).
//!
//! Protocol (see design spec §七 for full details): the host sends one
//! `{"search_space": {...}}` line at startup; the subprocess then sends
//! zero or more `{"evaluate": {...}}` lines (in whatever order/quantity
//! it chooses), each answered by the host with one `{"score": ...}` or
//! `{"error": "..."}` line; the subprocess finally sends one
//! `{"done": {"best_params": ..., "best_score": ...}}` or
//! `{"done": null}` line and the exchange ends.

use std::collections::HashMap;
use std::process::Stdio;

use apexes_core::MarketEvent;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

use crate::evaluator::Evaluator;
use crate::optimizer::{EvaluatedCombination, OptimizationResult, ParameterOptimizer};
use crate::search_space::SearchSpace;
use apexes_backtest::InMemoryMarketDataSource;

#[derive(Serialize)]
struct SearchSpaceMessage {
    search_space: HashMap<String, Vec<Decimal>>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PluginMessage {
    Evaluate { evaluate: HashMap<String, Decimal> },
    Done { done: Option<DoneResult> },
}

#[derive(Deserialize)]
struct DoneResult {
    best_params: HashMap<String, Decimal>,
    best_score: Decimal,
}

#[derive(Serialize)]
#[serde(untagged)]
enum HostResponse {
    Score { score: Decimal },
    Error { error: String },
}

/// Delegates parameter search to an external subprocess speaking the
/// newline-delimited-JSON protocol documented above.
pub struct ExternalProcessOptimizer {
    command: String,
    args: Vec<String>,
    market_data: InMemoryMarketDataSource,
}

impl ExternalProcessOptimizer {
    pub fn new(command: impl Into<String>, args: Vec<String>, events: Vec<MarketEvent>) -> Self {
        Self {
            command: command.into(),
            args,
            market_data: InMemoryMarketDataSource::new(events),
        }
    }
}

#[async_trait]
impl ParameterOptimizer for ExternalProcessOptimizer {
    async fn optimize(
        &self,
        search_space: &SearchSpace,
        evaluator: &mut dyn Evaluator,
    ) -> OptimizationResult {
        let mut child = match Command::new(&self.command)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                return OptimizationResult {
                    best: None,
                    all_results: Vec::new(),
                    error: Some(format!("failed to spawn external optimizer process: {e}")),
                };
            }
        };

        let mut stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let mut lines = BufReader::new(stdout).lines();

        let initial_message = SearchSpaceMessage {
            search_space: search_space.parameters().clone(),
        };
        if let Err(e) = write_json_line(&mut stdin, &initial_message).await {
            return OptimizationResult {
                best: None,
                all_results: Vec::new(),
                error: Some(format!("failed to write search space to external process: {e}")),
            };
        }

        let mut all_results = Vec::new();

        loop {
            let line = match lines.next_line().await {
                Ok(Some(line)) => line,
                Ok(None) => {
                    return OptimizationResult {
                        best: None,
                        all_results,
                        error: Some(
                            "external optimizer process exited before sending a done message"
                                .to_string(),
                        ),
                    };
                }
                Err(e) => {
                    return OptimizationResult {
                        best: None,
                        all_results,
                        error: Some(format!("failed to read from external process: {e}")),
                    };
                }
            };

            let message: PluginMessage = match serde_json::from_str(&line) {
                Ok(message) => message,
                Err(e) => {
                    return OptimizationResult {
                        best: None,
                        all_results,
                        error: Some(format!(
                            "external optimizer sent malformed JSON ({line:?}): {e}"
                        )),
                    };
                }
            };

            match message {
                PluginMessage::Evaluate { evaluate: params } => {
                    let score = match evaluator.evaluate(&params, &self.market_data).await {
                        Ok(score) => Some(score),
                        Err(message) => {
                            tracing::error!(
                                "external optimizer combination {params:?} failed: {message}"
                            );
                            None
                        }
                    };

                    let response = match score {
                        Some(score) => HostResponse::Score { score },
                        None => HostResponse::Error {
                            error: "evaluation failed, see host logs".to_string(),
                        },
                    };
                    if let Err(e) = write_json_line(&mut stdin, &response).await {
                        return OptimizationResult {
                            best: None,
                            all_results,
                            error: Some(format!("failed to write response to external process: {e}")),
                        };
                    }

                    all_results.push(EvaluatedCombination { params, score });
                }
                PluginMessage::Done { done: None } => {
                    return OptimizationResult { best: None, all_results, error: None };
                }
                PluginMessage::Done { done: Some(result) } => {
                    let best = EvaluatedCombination {
                        params: result.best_params,
                        score: Some(result.best_score),
                    };
                    return OptimizationResult { best: Some(best), all_results, error: None };
                }
            }
        }
    }
}

async fn write_json_line<T: Serialize>(
    stdin: &mut tokio::process::ChildStdin,
    value: &T,
) -> std::io::Result<()> {
    let mut line = serde_json::to_string(value).expect("serialization of a fixed message shape cannot fail");
    line.push('\n');
    stdin.write_all(line.as_bytes()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn fixture_path(name: &str) -> String {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name)
            .to_string_lossy()
            .to_string()
    }

    struct MockEvaluator {
        fail_on_x: Option<Decimal>,
    }

    #[async_trait]
    impl Evaluator for MockEvaluator {
        async fn evaluate(
            &mut self,
            params: &HashMap<String, Decimal>,
            _market_data: &InMemoryMarketDataSource,
        ) -> Result<Decimal, String> {
            let x = params["x"];
            if Some(x) == self.fail_on_x {
                return Err(format!("simulated failure for x={x}"));
            }
            Ok(x)
        }
    }

    #[tokio::test]
    async fn finds_the_highest_scoring_combination_via_external_process() {
        let optimizer = ExternalProcessOptimizer::new(
            "python3",
            vec![fixture_path("mock_optimizer.py")],
            vec![],
        );
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1), dec!(5), dec!(3)]);
        let mut evaluator = MockEvaluator { fail_on_x: None };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.error, None);
        let best = result.best.expect("expected a best combination");
        assert_eq!(best.params["x"], dec!(5));
        assert_eq!(best.score, Some(dec!(5)));
        assert_eq!(result.all_results.len(), 3);
    }

    #[tokio::test]
    async fn a_failed_combination_is_recorded_but_excluded_from_best() {
        let optimizer = ExternalProcessOptimizer::new(
            "python3",
            vec![fixture_path("mock_optimizer.py")],
            vec![],
        );
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1), dec!(5), dec!(3)]);
        let mut evaluator = MockEvaluator { fail_on_x: Some(dec!(5)) };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.error, None);
        let best = result.best.expect("expected a best combination among the non-failed ones");
        assert_eq!(best.params["x"], dec!(3));
        let failed = result.all_results.iter().find(|c| c.params["x"] == dec!(5)).unwrap();
        assert_eq!(failed.score, None);
    }

    #[tokio::test]
    async fn spawn_failure_is_reported_as_a_run_level_error() {
        let optimizer = ExternalProcessOptimizer::new(
            "this-command-definitely-does-not-exist-anywhere",
            vec![],
            vec![],
        );
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1)]);
        let mut evaluator = MockEvaluator { fail_on_x: None };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.best, None);
        assert!(result.error.is_some());
    }
}
```

**Note:** the `write_json_line` helper's `.expect("serialization of a fixed message shape cannot
fail")` is safe because `SearchSpaceMessage`/`HostResponse` are both plain structs/enums of
`Decimal`/`String` fields with no cyclic references or non-serializable types -- `serde_json`
can only fail to serialize these if a `Decimal` somehow produced a `NaN`-like non-finite value,
which `rust_decimal::Decimal` cannot represent in the first place (unlike `f64`). If a future
change adds a field type that COULD fail to serialize, replace this `.expect` with proper error
propagation into the `OptimizationResult.error` field, matching this task's other failure-path
patterns.

- [x] **Step 4: Update `lib.rs`**

```rust
pub mod evaluator;
pub mod external_process;
pub mod grid_search;
pub mod optimizer;
pub mod search_space;

pub use evaluator::Evaluator;
pub use external_process::ExternalProcessOptimizer;
pub use grid_search::GridSearchOptimizer;
pub use optimizer::{EvaluatedCombination, OptimizationResult, ParameterOptimizer};
pub use search_space::SearchSpace;
```

- [x] **Step 5: Run the tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-optimize external_process:: -- --nocapture
```
Expected: `3 passed` (`finds_the_highest_scoring_combination_via_external_process`,
`a_failed_combination_is_recorded_but_excluded_from_best`,
`spawn_failure_is_reported_as_a_run_level_error`). If
`finds_the_highest_scoring_combination_via_external_process` hangs instead of completing, check
that `mock_optimizer.py` was created with executable permissions (Step 2's `chmod +x`) and that
`python3` is actually on `PATH` inside the test execution environment (Step 1's check) --a hang
here means the subprocess never wrote its first line, most likely because it isn't running at
all.

- [x] **Step 6: Run the full `apexes-optimize` test suite**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-optimize
```
Expected: all prior tests (5 `search_space` + 5 `grid_search`) plus the 3 new `external_process`
tests = `13 passed`, 0 failed.

- [x] **Step 7: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-optimize --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run `cargo test -p apexes-optimize` +
both clippy/fmt commands once more to confirm nothing regressed).

- [x] **Step 8: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-optimize/src/external_process.rs crates/apexes-optimize/src/lib.rs crates/apexes-optimize/tests
git commit -S -s -m "feat: add ExternalProcessOptimizer to apexes-optimize"
git push origin master
```

---

### Task 7: End-to-end integration test (real `.drift` script + `run_backtest` + `GridSearchOptimizer`)

**Files:**
- Create: `crates/apexes-optimize/tests/fixtures/threshold_strategy.drift`
- Create: `crates/apexes-optimize/tests/grid_search_end_to_end.rs`

**Interfaces:**
- Consumes: `apexes_strategy::drift_dsl::DriftScriptStrategy::from_file_with_params`,
  `apexes_backtest::{run_backtest, BacktestConfig, CostModel, FlatFeeSchedule, ReplaySpeed}`,
  `apexes_optimize::{Evaluator, GridSearchOptimizer, ParameterOptimizer, SearchSpace}`.
- Produces: nothing new consumed by later tasks -- this is the plan's final behavioral proof
  point before Task 8's whole-effort verification.

- [x] **Step 1: Create the fixture script**

```bash
mkdir -p ~/src/oss/apexes/crates/apexes-optimize/tests/fixtures
cat > ~/src/oss/apexes/crates/apexes-optimize/tests/fixtures/threshold_strategy.drift << 'EOF'
let threshold = param("threshold", 100000);

function on_market_event(event, ctx) {
  let price = event.type == "bar" ? event.close : event.price;
  if (price > threshold) {
    return [{
      symbol: event.symbol,
      venue: event.venue,
      side: "buy",
      order_type: "market",
      quantity: 0.01,
    }];
  }
  return [];
}
EOF
```

This script buys whenever the trade price exceeds `threshold`. With a fixed sequence of trade
prices, a lower `threshold` produces more trades (and thus a higher `report.overall.trade_count`)
than a higher `threshold` -- the grid search's job is to find the `threshold` value that
maximizes `trade_count` given a known, hand-constructed price sequence.

- [x] **Step 2: Write the end-to-end test**

Create `crates/apexes-optimize/tests/grid_search_end_to_end.rs`:

```rust
//! End-to-end test wiring a real `.drift` script through
//! `DriftScriptStrategy::from_file_with_params`, `run_backtest`, and
//! `GridSearchOptimizer`, proving the full parameter-optimization
//! pipeline works together (not just each piece in isolation).

use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, InMemoryMarketDataSource, ReplaySpeed,
    run_backtest,
};
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, PriceLevel, TradeEvent, Venue,
};
use apexes_optimize::{Evaluator, GridSearchOptimizer, ParameterOptimizer, SearchSpace};
use apexes_strategy::drift_dsl::DriftScriptStrategy;
use async_trait::async_trait;
use chrono::{NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::path::PathBuf;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

fn btc_usdt_instrument() -> Instrument {
    Instrument::new(
        "BTCUSDT",
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("BTC".to_string()),
        Some("USDT".to_string()),
        dec!(0.01),
        dec!(0.0001),
        dec!(1),
    )
    .unwrap()
}

/// A depth snapshot with abundant ask-side liquidity (so every Market
/// buy order this test's strategy submits fills completely, regardless
/// of how many orders are submitted -- `MatchingEngine::submit_market_order`
/// walks the order book's ask levels and produces zero fills if the book
/// is empty, so a Depth snapshot MUST precede any Trade event a strategy
/// is expected to react to with a Market order; see
/// `crates/apexes-backtest/src/matching_engine.rs`'s `submit_market_order`),
/// followed by six trade prices (ascending then descending) so different
/// `threshold` values produce a predictably different number of trades
/// that exceed them.
fn sample_events() -> Vec<MarketEvent> {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();

    let depth_snapshot = MarketEvent::Depth(DepthUpdate {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        bids: vec![],
        asks: vec![PriceLevel { price: dec!(1), quantity: dec!(1000) }],
        sequence: Some(1),
        is_snapshot: true,
        timestamp: t0,
    });

    let trades = [
        dec!(90000),
        dec!(95000),
        dec!(100000),
        dec!(105000),
        dec!(110000),
        dec!(100000),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, price)| {
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price,
            quantity: dec!(1),
            side: None,
            trade_id: None,
            // +1 so every trade timestamp is strictly after the depth
            // snapshot (chronological ordering matters for the replay).
            timestamp: t0 + chrono::Duration::seconds(i as i64 + 1),
        })
    });

    std::iter::once(depth_snapshot).chain(trades).collect()
}

struct DriftScriptEvaluator {
    script_path: PathBuf,
}

#[async_trait]
impl Evaluator for DriftScriptEvaluator {
    async fn evaluate(
        &mut self,
        params: &HashMap<String, Decimal>,
        market_data: &InMemoryMarketDataSource,
    ) -> Result<Decimal, String> {
        let strategy = DriftScriptStrategy::from_file_with_params(&self.script_path, params.clone())
            .map_err(|e| e.to_string())?;

        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(100000));

        let config = BacktestConfig {
            instruments: vec![btc_usdt_instrument()],
            date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
                ..NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
            initial_balances,
            cost_model: CostModel {
                fee_schedule: Box::new(FlatFeeSchedule { maker_bps: dec!(0), taker_bps: dec!(0) }),
                funding_rate_bps: None,
            },
            speed: ReplaySpeed::AsFastAsPossible,
            queue_position_factor: dec!(0.0),
            risk_free_return: dec!(0),
        };

        let report = run_backtest(config, market_data, strategy).await.map_err(|e| e.to_string())?;

        Ok(Decimal::from(report.overall.trade_count))
    }
}

#[tokio::test]
async fn grid_search_finds_the_threshold_that_maximizes_trade_count() {
    let events = sample_events();
    let optimizer = GridSearchOptimizer::new(events);

    // threshold=80000 -> all 6 prices exceed it -> 6 trades.
    // threshold=97000 -> only 100000/105000/110000/100000 exceed it -> 4 trades.
    // threshold=120000 -> no price exceeds it -> 0 trades.
    let search_space =
        SearchSpace::new().with_parameter("threshold", vec![dec!(80000), dec!(97000), dec!(120000)]);

    let mut evaluator = DriftScriptEvaluator { script_path: fixture_path("threshold_strategy.drift") };

    let result = optimizer.optimize(&search_space, &mut evaluator).await;

    assert_eq!(result.error, None);
    let best = result.best.expect("expected a best combination");
    assert_eq!(best.params["threshold"], dec!(80000));
    assert_eq!(best.score, Some(dec!(6)));
    assert_eq!(result.all_results.len(), 3);
}
```

- [x] **Step 3: Run the test**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-optimize --test grid_search_end_to_end -- --nocapture
```
Expected: `1 passed`. This assertion was verified by hand against
`crates/apexes-backtest/src/matching_engine.rs`'s actual `submit_market_order` implementation
before this plan was finalized: it walks the order book's ask levels (`book.asks()`) and produces
**zero fills if the book has no depth**, which is why `sample_events()` in Step 2 prepends a
`Depth` snapshot with abundant liquidity (1000 units) before the six Trade events -- without it,
`trade_count` would be `0` for every `threshold` value and the test would fail regardless of which
threshold is "best". `submit_market_order` never mutates the book itself (only a subsequent
`on_depth_update` call would), so the same 1000-unit ask level backs all 6 potential fills. If the
assertion on `best.score` still fails, print `result.all_results` (temporarily) to see every
combination's actual trade count before changing the expected values -- do not assume the
fixture's math is wrong without first confirming the Depth snapshot is present and precedes every
Trade event chronologically (`sample_events()`'s `+1` second offset on trade timestamps exists
specifically to guarantee this ordering).

- [x] **Step 4: Run the full `apexes-optimize` test suite**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-optimize
```
Expected: `13 passed` (unit tests from Tasks 3, 5, 6) + `1 passed` (this task's integration test),
0 failed.

- [x] **Step 5: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-optimize --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run the full test suite + both
clippy/fmt commands once more to confirm nothing regressed).

- [x] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-optimize/tests
git commit -S -s -m "test: add end-to-end grid search integration test with a real .drift strategy"
git push origin master
```

---

### Task 8: Final whole-effort verification

**Files:** None created/modified -- this task only runs verification commands, updates
`docs/FEATURES.md`, and confirms the end state.

- [x] **Step 1: Full workspace build**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [x] **Step 2: Full workspace test suite**

```bash
cd ~/src/oss/apexes
cargo test --workspace
```
Expected: all tests across all crates pass. Report the exact total broken down by crate,
explicitly confirming `apexes-core`/`apexes-market-data`/`apexes-storage`/`apexes-indicators` are
completely unaffected by this plan (they were never touched); `apexes-backtest` gains exactly 1
new test (Task 2); `apexes-strategy` gains exactly 5 new tests (Task 1); `apexes-optimize` is
brand new with 14 tests (13 unit/integration from Tasks 3/5/6 + 1 end-to-end from Task 7).

- [x] **Step 3: Full workspace clippy**

```bash
cd ~/src/oss/apexes
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: 0 warnings.

- [x] **Step 4: Full workspace fmt check**

```bash
cd ~/src/oss/apexes
cargo fmt --check
```
Expected: 0 diffs.

- [x] **Step 5: Update `docs/FEATURES.md`**

Read `docs/FEATURES.md`, find row `2.4` (`Parameter optimization interface (grid/Bayesian,
pluggable external ML service)`, currently `🔲 Not Started`). Update its Status to `✅ Done` and
its Notes column to:
```
apexes-optimize crate: SearchSpace (Cartesian-product parameter grid), pluggable ParameterOptimizer trait (GridSearchOptimizer for in-process exhaustive search, ExternalProcessOptimizer delegating to an external subprocess over newline-delimited JSON -- no native dylib loading, zero unsafe, matching the roadmap's "external ML service" wording). apexes-strategy gained param(name, default) + DriftScriptStrategy::from_file_with_params so scripts can declare tunable parameters. Bayesian optimization itself (the algorithm) is deferred -- this pass only ships the protocol/interface plus grid search as the reference implementation.
```

**Important:** before finalizing, do not just increment the prior counts by 1/1 -- actually count
the current `✅ Done` / `🔲 Not Started` / `⏸️ Deferred` rows in the file (e.g. `grep -c` against
the row-level status markers, excluding the legend rows and the Summary table's own rows) and
confirm your new numbers match Done+1, Not Started-1, Total unchanged, against the real counts.
Update the Summary counts table accordingly and the "Current active work" paragraph to note
sub-project `#4`'s fourth phase (`2.4`) is now also complete alongside `2.1`/`2.2`/`2.3`; leave
"what's next" unstated since `2.5`/PyO3/`#5` are all still open options -- don't guess which one
the user picks next.

- [x] **Step 6: Commit the FEATURES.md update**

```bash
cd ~/src/oss/apexes
git add docs/FEATURES.md
git commit -S -s -m "docs: mark parameter optimization interface complete in FEATURES.md"
git push origin master
```

- [x] **Step 7: Mark this plan's checkboxes complete**

```bash
cd ~/src/oss/apexes
sed -i '' 's/^- \[ \]/- [x]/' docs/superpowers/plans/2026-07-30-apexes-optimize.md
```
Check `grep -c "^\- \[ \]" docs/superpowers/plans/2026-07-30-apexes-optimize.md` reports `0`
remaining unmarked checkboxes (there is no header line describing the checkbox syntax convention
itself in this plan's exact wording that would be affected by this substitution -- verify by
reading the top of the file after running `sed`; if the header's own descriptive sentence
happens to also get flipped to `- [x]` by this broad substitution, as happened in the prior
`apexes-signals` plan, manually revert just that one line back to `- [ ]` since it is describing
the markdown convention itself, not reporting a completed task).

```bash
git add docs/superpowers/plans/2026-07-30-apexes-optimize.md
git commit -S -s -m "docs: mark apexes-optimize plan complete"
git push origin master
```

- [x] **Step 8: Report final summary**

Summarize: `apexes-optimize` crate's file list and line counts, total test count (broken down:
unit tests per module, the end-to-end integration test), confirmation that
`apexes-strategy`'s/`apexes-backtest`'s pre-existing tests still pass unchanged after their small
additions, and the `apexes` commit SHAs for Tasks 1-8. No further commit needed for this
verification-only task.

---

## Plan Self-Review Notes

**Spec coverage:** Design spec §一 (scope: `param()`/`from_file_with_params`, new
`apexes-optimize` crate with `SearchSpace`/`ParameterOptimizer`/`Evaluator`/
`GridSearchOptimizer`/`ExternalProcessOptimizer`, blanket `&T` impl in `apexes-backtest`,
explicit non-goals around no dylib loading/no real Bayesian algorithm/no parallelism) → Task 1
(`param`/`from_file_with_params`), Task 2 (blanket `&T`), Task 3 (crate scaffold +
`SearchSpace`), Task 4 (`ParameterOptimizer`/`Evaluator` traits), Task 5
(`GridSearchOptimizer`), Task 6 (`ExternalProcessOptimizer`), Task 8 confirms nothing extra was
added (no dylib loading anywhere in this plan, no real optimization algorithm implemented, no
parallel execution). §二 (`param(name, default)` semantics, clamping/validation, construction-time
registration) → Task 1. §三 (crate file layout, `Cargo.toml` dependencies) → Task 3 Step 1-2,
extended incrementally by Tasks 4-6's `lib.rs` updates. §四 (`SearchSpace` Cartesian product,
empty-space/empty-candidate-list edge cases) → Task 3 Step 3, all 5 enumerated test cases present
verbatim. §五 (`ParameterOptimizer`/`Evaluator` trait shapes, `EvaluatedCombination`/
`OptimizationResult` including the `error` field) → Task 4. §六 (`GridSearchOptimizer`
implementation, why it owns `InMemoryMarketDataSource`) → Task 5, all 5 enumerated test scenarios
present. §七 (`ExternalProcessOptimizer` protocol: plugin-in-control state machine, message
shapes, malformed-JSON/process-crash/empty-done failure modes, "external process's own reported
best is authoritative" design decision) → Task 6, all failure modes from the design's inline
comments translated into either explicit code paths or explicit tests. §八 (blanket `&T` impl) →
Task 2. §九 (testing strategy: `param()` unit tests, `SearchSpace` unit tests,
`GridSearchOptimizer` mock-evaluator tests, `ExternalProcessOptimizer` mock-subprocess tests, one
true end-to-end test) → Tasks 1, 3, 5, 6, 7 respectively. §十 (non-goals: no dylib loading, no
real Bayesian algorithm, no search-space automation/UI, no parallelism, no Rust-native `Strategy`
parameterization, no non-stdio IPC) — confirmed none of Tasks 1-8 do any of these.

**Placeholder scan:** No "TBD"/"TODO" strings in any task (the one `TODO(Task 4)` comment
referenced while exploring `signals.rs` belongs to a DIFFERENT, already-completed plan and is not
part of this plan's own code). Every code step shows complete, copy-pasteable code, including the
mock Python "external optimizer" fixture (Task 6 Step 2) and the hand-derived trade-count
expectations in Task 7's end-to-end test.

**Bug caught during self-review (fixed before finalizing):** Task 7's initial draft of
`sample_events()` contained only `Trade` events with no preceding `Depth` snapshot. Reading
`crates/apexes-backtest/src/matching_engine.rs`'s actual `submit_market_order` implementation
directly (not assumed) revealed it walks the order book's ask levels via `book.asks()` and
produces **zero fills when the book is empty** -- with no `Depth` event ever sent, every Market
order the fixture's `.drift` script submits would silently fail to fill (`OrderStatus::New`,
`filled_quantity: 0`, no `Fill` ever added to `all_fills`), making `report.overall.trade_count`
`0` for every `threshold` value regardless of which one is "best" -- the test's core assertion
would have been unverifiable/meaningless as originally drafted. Fixed by prepending a `Depth`
snapshot (1000-unit ask liquidity, timestamped strictly before the first Trade event) to
`sample_events()`; confirmed by reading `order_book.rs`/`matching_engine.rs` that
`submit_market_order` never mutates the book itself, so this single snapshot correctly backs all
6 potential fills without depleting.

**Type consistency:** `SearchSpace::combinations()`'s return type (`Vec<HashMap<String,
Decimal>>`, defined in Task 3) is consumed identically by `GridSearchOptimizer::optimize` (Task
5) and by the manual Cartesian-product-vs-raw-parameters distinction in
`ExternalProcessOptimizer::optimize` (Task 6, which instead uses `SearchSpace::parameters()` --
also defined in Task 3 Step 3 -- to get the pre-expansion candidate-value map for the initial
protocol message, correctly NOT calling `combinations()` since the external process decides its
own evaluation order). `Evaluator::evaluate`'s exact signature (`&mut self, params:
&HashMap<String, Decimal>, market_data: &InMemoryMarketDataSource) -> Result<Decimal, String>`,
defined once in Task 4, is implemented identically by the `MockEvaluator` structs in Tasks 5/6
and by `DriftScriptEvaluator` in Task 7 -- no signature drift across tasks.
`EvaluatedCombination`/`OptimizationResult`'s field names (`params`/`score`,
`best`/`all_results`/`error`), defined in Task 4, are constructed identically in
`GridSearchOptimizer` (Task 5) and `ExternalProcessOptimizer` (Task 6) -- both leave `error: None`
on their success/normal-failure paths and only `ExternalProcessOptimizer` ever sets `error:
Some(...)`, matching the design spec's explicit division of responsibility.
`DriftScriptStrategy::from_file_with_params`'s exact signature (`path: &Path, overrides:
HashMap<String, Decimal>`), defined in Task 1, matches its call site in Task 7's
`DriftScriptEvaluator::evaluate` exactly (`params.clone()` passed directly, since `Evaluator`
already receives an owned-enough `&HashMap` that needs cloning to hand off ownership to
`from_file_with_params`).
