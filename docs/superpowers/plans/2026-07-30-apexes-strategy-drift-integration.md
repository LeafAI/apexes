# Apexes-Strategy: Strategy Trait Migration + Drift Script Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a new `apexes-strategy` crate that becomes the authoritative home for the
`Strategy`/`StrategyContext`/`OrderIntent`/`Position` types (migrated from `apexes-backtest`),
and add `DriftScriptStrategy`, a `Strategy` implementation that runs a user's `.drift` script
as the strategy's decision logic.

**Architecture:** `apexes-strategy` depends on `apexes-core` (shared types) and the external
`drift-script`/`drift-script-math` crates (from `github.com/LeafAI/drift-dsl`). A small,
independent commit to the `drift-dsl` repo first exposes a public `Interpreter::call()` method.
`apexes-backtest` is then updated to depend on `apexes-strategy` and re-export its types instead
of defining them locally.

**Tech Stack:** Rust edition 2024, Cargo workspace, `drift-script`/`drift-script-math` (git
dependency), `tokio` (for `block_on` bridging sync `Strategy` trait to async `Interpreter`),
`thiserror`, `rust_decimal`, `chrono`, `tracing`.

## Global Constraints

- Design spec of record: `docs/superpowers/specs/2026-07-30-apexes-strategy-drift-integration-design.md`.
  If you find a mismatch between this plan and that spec while implementing, the spec wins —
  stop and flag it rather than improvising.
- Rust edition `2024`, matching `apexes`' workspace (`Cargo.toml` `[workspace.package] edition = "2024"`).
- `apexes-strategy`'s `drift-script`/`drift-script-math` dependencies MUST use the exact git rev
  already pinned by `apexes`' sibling work in the `drift` monorepo:
  `rev = "679d149130ca0f014b0386aff00863baec6141c8"` (the `drift-dsl` commit that added
  `drift-script-math`). Do not bump this without explicit instruction.
- `Strategy`/`StrategyContext`/`OrderIntent`/`Position` field names, types, and doc comments
  must be preserved EXACTLY as they exist today in
  `crates/apexes-backtest/src/strategy.rs` when migrated — this is a relocation, not a redesign
  (design spec §3). Do not rename fields, change types, or "improve" anything during the move.
- Every commit in the `apexes` repo uses `git commit -S -s` (signed + signed-off), matching the
  project's established convention (see recent commit history). The one `drift-dsl` commit in
  Task 1 also uses `git commit -S -s`.
- `cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets
  -- -D warnings` / `cargo fmt --check` must all pass clean in `apexes` after every task that
  touches compiled code — this project's established quality bar (see `apexes`' `FEATURES.md`
  "workspace builds/tests/clippy/fmt clean" language for prior sub-projects).
- Runtime errors inside `DriftScriptStrategy::on_market_event` (script throws, script returns a
  malformed value) MUST NOT panic and MUST NOT propagate as a `Result` — the trait signature is
  `fn on_market_event(...) -> Vec<OrderIntent>` with no `Result`. On any runtime failure: log via
  `tracing::error!` and return `vec![]`. Construction-time failures (`DriftScriptStrategy::from_file`)
  DO return `Result<Self, StrategyError>` and must fail closed (no partially-constructed instance).

---

### Task 1: Expose `Interpreter::call()` in the `drift-dsl` repository

**Files (in `~/src/oss/drift-dsl`):**
- Modify: `crates/drift-script/src/interpreter.rs`

**Interfaces:**
- Produces: `pub async fn Interpreter::call(&mut self, func: DriftValue, args: Vec<DriftValue>) -> InterpretResult`,
  consumed by `apexes-strategy`'s `DriftScriptStrategy` in Task 5.

- [ ] **Step 1: Locate the existing private `call_function` method**

Read `~/src/oss/drift-dsl/crates/drift-script/src/interpreter.rs` around line 992-993 to confirm
the exact current signature:

```rust
    fn call_function<'a>(
        &'a mut self,
        func: DriftValue,
        args: Vec<DriftValue>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = InterpretResult> + Send + 'a>> {
```

- [ ] **Step 2: Add the public `call()` wrapper immediately before `call_function`**

Insert this method into the `impl Interpreter` block, directly above the existing
`fn call_function<'a>(` line:

```rust
    /// Calls a previously-resolved `DriftValue::Function`/`NativeFunction` with the
    /// given positional arguments, returning its result (or the error it raised).
    /// Intended for host code that needs to invoke a specific script-defined function
    /// by value (e.g. a resolved export or a plain top-level function looked up via
    /// [`Environment::get`]), as opposed to [`Interpreter::evaluate`] which runs a
    /// whole statement list.
    pub async fn call(&mut self, func: DriftValue, args: Vec<DriftValue>) -> InterpretResult {
        self.call_function(func, args).await
    }

```

- [ ] **Step 3: Verify it compiles**

Run:
```bash
cd ~/src/oss/drift-dsl
cargo build -p drift-script
```
Expected: succeeds with no warnings.

- [ ] **Step 4: Run drift-script's existing test suite to confirm no regression**

Run:
```bash
cd ~/src/oss/drift-dsl
cargo test -p drift-script
```
Expected: all existing tests still pass (this is a pure additive change, no existing behavior
touched).

- [ ] **Step 5: Run clippy and fmt across the whole drift-dsl workspace**

```bash
cd ~/src/oss/drift-dsl
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```
Expected: no warnings, no formatting diffs.

- [ ] **Step 6: Commit and push**

```bash
cd ~/src/oss/drift-dsl
git add crates/drift-script/src/interpreter.rs
git commit -S -s -m "feat: expose Interpreter::call for invoking resolved functions by value"
git push origin master
```

Record the commit SHA (`git rev-parse HEAD`) — Task 2 will pin `apexes-strategy`'s
`drift-script`/`drift-script-math` git dependencies to this exact SHA (both crates live in the
same repo at the same commit, so one SHA covers both).

---

### Task 2: Scaffold the `apexes-strategy` crate and migrate `Strategy`/`StrategyContext`/`OrderIntent`/`Position`

**Files (in `~/src/oss/apexes`):**
- Create: `crates/apexes-strategy/Cargo.toml`
- Create: `crates/apexes-strategy/src/lib.rs`
- Create: `crates/apexes-strategy/src/strategy.rs`
- Modify: `Cargo.toml` (add `crates/apexes-strategy` to workspace `members`)
- Modify: `crates/apexes-backtest/Cargo.toml` (add `apexes-strategy` dependency)
- Modify: `crates/apexes-backtest/src/lib.rs` (replace `pub mod strategy;` with re-export)
- Delete: `crates/apexes-backtest/src/strategy.rs`

**Interfaces:**
- Consumes: `apexes_core::{Order, OrderId, OrderType, Side, Venue, MarketEvent}` (unchanged).
- Produces: `apexes_strategy::{Strategy, StrategyContext, OrderIntent, Position}`, consumed by
  `apexes-backtest` (this task) and by `DriftScriptStrategy` (Task 5).

- [ ] **Step 1: Create `crates/apexes-strategy/Cargo.toml`**

Use the exact SHA recorded at the end of Task 1's Step 6 in place of `<SHA>` below:

```toml
[package]
name = "apexes-strategy"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
drift-script = { git = "https://github.com/LeafAI/drift-dsl.git", rev = "<SHA>" }
drift-script-math = { git = "https://github.com/LeafAI/drift-dsl.git", rev = "<SHA>" }
tokio = { workspace = true }
thiserror = { workspace = true }
rust_decimal = { workspace = true }
chrono = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
uuid = { workspace = true }
futures = { workspace = true }
```

- [ ] **Step 2: Add `apexes-strategy` to the workspace members list**

Read `~/src/oss/apexes/Cargo.toml`. It currently reads:

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage", "crates/apexes-backtest"]
```

Edit the `members` line to add `"crates/apexes-strategy"`:

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage", "crates/apexes-backtest", "crates/apexes-strategy"]
```

- [ ] **Step 3: Read the current `apexes-backtest/src/strategy.rs` in full**

Read `~/src/oss/apexes/crates/apexes-backtest/src/strategy.rs` (166 lines) to have its exact
content in front of you for the next step — every field, doc comment, and existing test must
be preserved verbatim.

- [ ] **Step 4: Create `crates/apexes-strategy/src/strategy.rs` with the migrated content**

Copy the full content of `apexes-backtest/src/strategy.rs` verbatim into this new file, with
ONLY the module-level doc comment's crate-name references updated (the trait/struct
definitions, field names, doc comments on fields, and the two existing `#[cfg(test)]` tests
are otherwise unchanged):

```rust
use apexes_core::{Order, OrderId, OrderType, Side, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// The shared strategy interface implemented by both the backtest engine
/// (`apexes-backtest`) and all future live-trading sub-projects (#6 Paper
/// Trading, #7 OMS, #8 live crypto gateway), per the project's
/// non-negotiable backtest/live logic-parity requirement. A `Strategy`
/// implementation never needs to know whether it is running in backtest
/// or live mode.
pub trait Strategy: Send {
    /// Called once per incoming `MarketEvent`, in timestamp order.
    fn on_market_event(
        &mut self,
        event: &apexes_core::MarketEvent,
        ctx: &StrategyContext,
    ) -> Vec<OrderIntent>;
}

/// Read-only view of current backtest/live state, passed to
/// `Strategy::on_market_event` so it can make informed order decisions.
pub struct StrategyContext<'a> {
    /// Current net positions, keyed by symbol.
    pub positions: &'a HashMap<String, Position>,
    /// Current balances, keyed by asset symbol (e.g. "USDT", "BTC").
    pub balances: &'a HashMap<String, Decimal>,
    /// Currently-open (unfilled or partially-filled) orders, keyed by
    /// `OrderId`.
    ///
    /// Triggered Stop orders: a Stop order that triggers converts
    /// internally to a Market order, and its fills are reported under the
    /// ORIGINAL `OrderId` this map already holds -- the runner's
    /// `open_orders.get_mut(&order_id)` lookup finds the same entry, so
    /// the strategy observes the fill on the order it placed.
    ///
    /// Known limitation (StopLimit only): a triggered StopLimit converts
    /// to a NEW resting Limit order tracked under a NEW `OrderId`; the
    /// original OrderId's entry in this map will not reflect fills of
    /// the converted order. See
    /// docs/superpowers/plans/2026-07-28-apexes-backtest.md Task 14 for
    /// full rationale.
    pub open_orders: &'a HashMap<OrderId, Order>,
    /// Current simulated (backtest) or real (live) time.
    pub now: DateTime<Utc>,
}

/// An order a `Strategy` wants to place, returned from
/// `on_market_event`. Distinct from `apexes_core::Order` (which represents
/// an order already accepted into the system with an assigned `OrderId`
/// and lifecycle state) -- `OrderIntent` is the strategy's *request*,
/// before the matching engine (backtest) or OMS (live) assigns it an
/// identity and begins tracking its status.
#[derive(Debug, Clone)]
pub struct OrderIntent {
    pub symbol: String,
    pub venue: Venue,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    /// Limit price (`Limit`, `StopLimit` only).
    pub price: Option<Decimal>,
    /// Trigger price (`Stop`, `StopLimit` only).
    pub stop_price: Option<Decimal>,
}

/// A net position in a single symbol.
#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    pub symbol: String,
    /// Signed: positive = long, negative = short.
    pub quantity: Decimal,
    pub average_entry_price: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::MarketEvent;
    use rust_decimal_macros::dec;

    /// A trivial test `Strategy` that emits a fixed `OrderIntent` on every
    /// event, used to verify the trait's call shape compiles and works as
    /// expected. Real strategy logic will be added by future sub-projects
    /// consuming this trait, not by this plan.
    struct AlwaysBuyOneUnit {
        symbol: String,
        venue: Venue,
    }

    impl Strategy for AlwaysBuyOneUnit {
        fn on_market_event(
            &mut self,
            _event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            vec![OrderIntent {
                symbol: self.symbol.clone(),
                venue: self.venue,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
            }]
        }
    }

    #[test]
    fn strategy_trait_is_callable_and_returns_intents() {
        let mut strategy = AlwaysBuyOneUnit {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
        };
        let positions = HashMap::new();
        let balances = HashMap::new();
        let open_orders = HashMap::new();
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now: Utc::now(),
        };
        let event = MarketEvent::Trade(apexes_core::TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });

        let intents = strategy.on_market_event(&event, &ctx);

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].symbol, "BTCUSDT");
        assert_eq!(intents[0].side, Side::Buy);
        assert_eq!(intents[0].quantity, dec!(1));
    }

    #[test]
    fn strategy_context_exposes_provided_state() {
        let mut positions = HashMap::new();
        positions.insert(
            "BTCUSDT".to_string(),
            Position {
                symbol: "BTCUSDT".to_string(),
                quantity: dec!(2),
                average_entry_price: dec!(64000),
            },
        );
        let balances = HashMap::new();
        let open_orders = HashMap::new();
        let now = Utc::now();

        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };

        assert_eq!(ctx.positions.get("BTCUSDT").unwrap().quantity, dec!(2));
        assert_eq!(ctx.now, now);
    }
}
```

- [ ] **Step 5: Create `crates/apexes-strategy/src/lib.rs`**

```rust
//! Apexes strategy SDK: the shared `Strategy` trait interface (implemented
//! identically by the backtest engine and all future live-trading
//! sub-projects, per the project's non-negotiable backtest/live logic-parity
//! requirement), plus `DriftScriptStrategy`, a `Strategy` implementation
//! that runs a user-authored Drift Script (`.drift`) file as the strategy's
//! decision logic.

pub mod strategy;

pub use strategy::{OrderIntent, Position, Strategy, StrategyContext};
```

- [ ] **Step 6: Delete the old `apexes-backtest/src/strategy.rs`**

```bash
cd ~/src/oss/apexes
rm crates/apexes-backtest/src/strategy.rs
```

- [ ] **Step 7: Update `crates/apexes-backtest/Cargo.toml`**

Read the current file, then add `apexes-strategy` as a dependency (alongside the existing
`apexes-core = { path = "../apexes-core" }` line):

```toml
apexes-strategy = { path = "../apexes-strategy" }
```

- [ ] **Step 8: Update `crates/apexes-backtest/src/lib.rs`**

Find:
```rust
// Populated in Task 6.
pub mod strategy;
```
Replace with:
```rust
pub use apexes_strategy::{OrderIntent, Position, Strategy, StrategyContext};
```

Find the existing re-export line near the bottom of the file:
```rust
pub use strategy::{OrderIntent, Position, Strategy, StrategyContext};
```
Delete this line (the new `pub use apexes_strategy::{...}` above replaces it — having both
would be a duplicate re-export of the same names and fail to compile).

- [ ] **Step 9: Verify the whole workspace still builds**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds. This is the first real network access to the `drift-dsl` git dependency —
if it fails to resolve the git dependency, verify the SHA from Task 1 Step 6 is correct and the
`drift-dsl` push actually succeeded.

- [ ] **Step 10: Run the full test suite**

```bash
cd ~/src/oss/apexes
cargo test --workspace
```
Expected: all existing tests pass, including the two migrated tests
(`strategy_trait_is_callable_and_returns_intents`, `strategy_context_exposes_provided_state`)
now running from `apexes-strategy` instead of `apexes-backtest`, and all of `apexes-backtest`'s
own existing tests (which reference `Strategy`/`OrderIntent`/etc. via the crate's public API,
now backed by the re-export) continuing to pass unchanged.

- [ ] **Step 11: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```
Expected: no warnings, no formatting diffs.

- [ ] **Step 12: Commit**

```bash
cd ~/src/oss/apexes
git add Cargo.toml crates/apexes-strategy crates/apexes-backtest/Cargo.toml crates/apexes-backtest/src/lib.rs
git add -u crates/apexes-backtest/src/strategy.rs
git commit -S -s -m "feat: add apexes-strategy crate, migrate Strategy trait from apexes-backtest"
git push origin master
```

---

### Task 3: Add `StrategyError` and the `drift_dsl` submodule skeleton

**Files:**
- Create: `crates/apexes-strategy/src/error.rs`
- Create: `crates/apexes-strategy/src/drift_dsl/mod.rs`
- Modify: `crates/apexes-strategy/src/lib.rs`

**Interfaces:**
- Produces: `apexes_strategy::StrategyError` (consumed by `DriftScriptStrategy::from_file` in
  Task 5); `apexes_strategy::drift_dsl` module namespace (populated by Tasks 4-5).

- [ ] **Step 1: Create `crates/apexes-strategy/src/error.rs`**

```rust
//! Error type for `apexes-strategy`'s Drift Script integration.

use std::path::PathBuf;
use thiserror::Error;

/// Errors that can occur constructing a [`crate::drift_dsl::DriftScriptStrategy`].
///
/// Runtime errors during `Strategy::on_market_event` (script throws, script
/// returns a malformed value) are deliberately NOT represented here -- the
/// `Strategy` trait's `on_market_event` returns `Vec<OrderIntent>` with no
/// `Result`, so runtime failures are logged via `tracing::error!` and the
/// call returns an empty `Vec` instead of propagating an error (a single
/// tick's strategy failure must never halt the whole backtest/live loop).
#[derive(Debug, Error)]
pub enum StrategyError {
    #[error("failed to read script file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse script: {0}")]
    Parse(String),

    #[error("script has no top-level `on_market_event` function")]
    MissingEntryPoint,

    #[error("`on_market_event` is defined but is not a function")]
    EntryPointNotCallable,
}
```

- [ ] **Step 2: Create `crates/apexes-strategy/src/drift_dsl/mod.rs`**

```rust
//! Drift Script integration: [`DriftScriptStrategy`] runs a user-authored
//! `.drift` script as a [`crate::Strategy`] implementation, so researchers
//! can author and iterate on strategy logic without a Rust compile cycle.

mod bindings;
mod convert;
mod strategy;

pub use strategy::DriftScriptStrategy;
```

- [ ] **Step 3: Add placeholder `bindings.rs` and `convert.rs` (populated in Task 4)**

```bash
cd ~/src/oss/apexes
cat > crates/apexes-strategy/src/drift_dsl/convert.rs << 'EOF'
//! Conversions between `apexes_core`/`apexes_strategy` types and
//! `drift_script::DriftValue`. Populated in a later task.
EOF
cat > crates/apexes-strategy/src/drift_dsl/bindings.rs << 'EOF'
//! `ctx.*` native-function bindings exposed to Drift Script strategy
//! scripts. Populated in a later task.
EOF
```

- [ ] **Step 4: Add the `drift_dsl` and `error` modules to `lib.rs`**

Update `crates/apexes-strategy/src/lib.rs` to:

```rust
//! Apexes strategy SDK: the shared `Strategy` trait interface (implemented
//! identically by the backtest engine and all future live-trading
//! sub-projects, per the project's non-negotiable backtest/live logic-parity
//! requirement), plus `DriftScriptStrategy`, a `Strategy` implementation
//! that runs a user-authored Drift Script (`.drift`) file as the strategy's
//! decision logic.

pub mod drift_dsl;
pub mod error;
pub mod strategy;

pub use error::StrategyError;
pub use strategy::{OrderIntent, Position, Strategy, StrategyContext};
```

- [ ] **Step 5: Verify it compiles**

```bash
cd ~/src/oss/apexes
cargo build -p apexes-strategy
```
Expected: succeeds (the empty `drift_dsl` submodule with no `pub` items other than the not-yet-
defined `DriftScriptStrategy` will fail — if so, temporarily stub `strategy.rs`'s content with
just a comment placeholder to get a clean compile at this checkpoint):

```bash
cat > crates/apexes-strategy/src/drift_dsl/strategy.rs << 'EOF'
//! `DriftScriptStrategy`. Populated in a later task.
EOF
```

Re-run `cargo build -p apexes-strategy` after adding this stub file; expected: succeeds
(with an unused-import warning for the empty `bindings`/`convert` modules being fine at this
checkpoint since they only contain doc comments, not unused imports).

- [ ] **Step 6: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-strategy --all-targets -- -D warnings
cargo fmt --check
```
Expected: no warnings, no formatting diffs.

- [ ] **Step 7: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-strategy/src/error.rs crates/apexes-strategy/src/drift_dsl crates/apexes-strategy/src/lib.rs
git commit -S -s -m "feat: add StrategyError and drift_dsl module skeleton to apexes-strategy"
git push origin master
```

---

### Task 4: Implement the type-conversion layer (`convert.rs`)

**Files:**
- Modify: `crates/apexes-strategy/src/drift_dsl/convert.rs`

**Interfaces:**
- Consumes: `apexes_core::{MarketEvent, Tick, DepthUpdate, PriceLevel, Bar, BarInterval, TradeEvent, Venue, Side, OrderType}`,
  `apexes_strategy::{Position, OrderIntent}`, `drift_script::DriftValue`.
- Produces:
  - `pub fn market_event_to_drift_value(event: &MarketEvent) -> DriftValue`
  - `pub fn position_to_drift_value(position: &Position) -> DriftValue`
  - `pub fn default_position_drift_value(symbol: &str) -> DriftValue`
  - `pub fn order_intents_from_drift_value(value: DriftValue) -> Result<Vec<OrderIntent>, String>`

  All four consumed by `bindings.rs` (Task 5, for the first three) and `strategy.rs` (Task 5,
  for `market_event_to_drift_value` and `order_intents_from_drift_value`).

- [ ] **Step 1: Write the failing tests for `market_event_to_drift_value`**

Replace the content of `crates/apexes-strategy/src/drift_dsl/convert.rs` with:

```rust
//! Conversions between `apexes_core`/`apexes_strategy` types and
//! `drift_script::DriftValue`, hand-written (not serde-based) so field
//! names and script-facing shapes are precisely controlled.

use apexes_core::{Instrument as _, MarketEvent, OrderType, Side, Venue};
use apexes_core::{Bar, BarInterval, DepthUpdate, PriceLevel, TradeEvent, Tick};
use drift_script::DriftValue;
use std::collections::HashMap;
use std::sync::Arc;

use crate::strategy::{OrderIntent, Position};

fn decimal_to_number(value: rust_decimal::Decimal) -> DriftValue {
    DriftValue::Number(value.try_into().unwrap_or(f64::NAN))
}

fn string_value(s: impl Into<String>) -> DriftValue {
    DriftValue::String(Arc::new(s.into()))
}

fn venue_to_str(venue: Venue) -> &'static str {
    match venue {
        Venue::Sse => "sse",
        Venue::Szse => "szse",
        Venue::Hkex => "hkex",
        Venue::Nasdaq => "nasdaq",
        Venue::Nyse => "nyse",
        Venue::Binance => "binance",
        Venue::Okx => "okx",
        Venue::Coinbase => "coinbase",
    }
}

fn side_to_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

fn bar_interval_to_str(interval: BarInterval) -> &'static str {
    match interval {
        BarInterval::OneSecond => "1s",
        BarInterval::OneMinute => "1m",
        BarInterval::FiveMinutes => "5m",
        BarInterval::FifteenMinutes => "15m",
        BarInterval::OneHour => "1h",
        BarInterval::OneDay => "1d",
    }
}

fn price_level_to_drift_value(level: &PriceLevel) -> DriftValue {
    let mut obj = HashMap::new();
    obj.insert("price".to_string(), decimal_to_number(level.price));
    obj.insert("quantity".to_string(), decimal_to_number(level.quantity));
    DriftValue::Object(obj)
}

/// Converts an `apexes_core::MarketEvent` to the `DriftValue::Object` shape
/// passed as `event` into a Drift Script strategy's `on_market_event(event,
/// ctx)` function. Every variant's object carries a `"type"` tag field
/// (`"tick"` / `"depth"` / `"bar"` / `"trade"`) plus variant-specific
/// fields. `Decimal` fields become `Number`; `DateTime<Utc>` fields become
/// ISO 8601 strings.
pub fn market_event_to_drift_value(event: &MarketEvent) -> DriftValue {
    let mut obj = HashMap::new();
    match event {
        MarketEvent::Tick(Tick { symbol, venue, bid_price, bid_size, ask_price, ask_size, timestamp }) => {
            obj.insert("type".to_string(), string_value("tick"));
            obj.insert("symbol".to_string(), string_value(symbol.clone()));
            obj.insert("venue".to_string(), string_value(venue_to_str(*venue)));
            obj.insert("bid_price".to_string(), decimal_to_number(*bid_price));
            obj.insert("bid_size".to_string(), decimal_to_number(*bid_size));
            obj.insert("ask_price".to_string(), decimal_to_number(*ask_price));
            obj.insert("ask_size".to_string(), decimal_to_number(*ask_size));
            obj.insert("timestamp".to_string(), string_value(timestamp.to_rfc3339()));
        }
        MarketEvent::Depth(DepthUpdate { symbol, venue, bids, asks, sequence, is_snapshot, timestamp }) => {
            obj.insert("type".to_string(), string_value("depth"));
            obj.insert("symbol".to_string(), string_value(symbol.clone()));
            obj.insert("venue".to_string(), string_value(venue_to_str(*venue)));
            obj.insert(
                "bids".to_string(),
                DriftValue::Array(bids.iter().map(price_level_to_drift_value).collect()),
            );
            obj.insert(
                "asks".to_string(),
                DriftValue::Array(asks.iter().map(price_level_to_drift_value).collect()),
            );
            obj.insert(
                "sequence".to_string(),
                sequence.map(|s| DriftValue::Number(s as f64)).unwrap_or(DriftValue::Null),
            );
            obj.insert("is_snapshot".to_string(), DriftValue::Bool(*is_snapshot));
            obj.insert("timestamp".to_string(), string_value(timestamp.to_rfc3339()));
        }
        MarketEvent::Bar(Bar { symbol, venue, open, high, low, close, volume, interval, open_time, close_time }) => {
            obj.insert("type".to_string(), string_value("bar"));
            obj.insert("symbol".to_string(), string_value(symbol.clone()));
            obj.insert("venue".to_string(), string_value(venue_to_str(*venue)));
            obj.insert("open".to_string(), decimal_to_number(*open));
            obj.insert("high".to_string(), decimal_to_number(*high));
            obj.insert("low".to_string(), decimal_to_number(*low));
            obj.insert("close".to_string(), decimal_to_number(*close));
            obj.insert("volume".to_string(), decimal_to_number(*volume));
            obj.insert("interval".to_string(), string_value(bar_interval_to_str(*interval)));
            obj.insert("open_time".to_string(), string_value(open_time.to_rfc3339()));
            obj.insert("close_time".to_string(), string_value(close_time.to_rfc3339()));
        }
        MarketEvent::Trade(TradeEvent { symbol, venue, price, quantity, side, trade_id, timestamp }) => {
            obj.insert("type".to_string(), string_value("trade"));
            obj.insert("symbol".to_string(), string_value(symbol.clone()));
            obj.insert("venue".to_string(), string_value(venue_to_str(*venue)));
            obj.insert("price".to_string(), decimal_to_number(*price));
            obj.insert("quantity".to_string(), decimal_to_number(*quantity));
            obj.insert(
                "side".to_string(),
                side.map(|s| string_value(side_to_str(s))).unwrap_or(DriftValue::Null),
            );
            obj.insert(
                "trade_id".to_string(),
                trade_id.clone().map(string_value).unwrap_or(DriftValue::Null),
            );
            obj.insert("timestamp".to_string(), string_value(timestamp.to_rfc3339()));
        }
    }
    DriftValue::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::TradeEvent;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn get_str<'a>(obj: &'a HashMap<String, DriftValue>, key: &str) -> &'a str {
        match obj.get(key) {
            Some(DriftValue::String(s)) => s.as_str(),
            other => panic!("expected string field {key:?}, got {other:?}"),
        }
    }

    fn get_num(obj: &HashMap<String, DriftValue>, key: &str) -> f64 {
        match obj.get(key) {
            Some(DriftValue::Number(n)) => *n,
            other => panic!("expected number field {key:?}, got {other:?}"),
        }
    }

    #[test]
    fn tick_converts_with_type_tag_and_all_fields() {
        let event = MarketEvent::Tick(Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(64999),
            bid_size: dec!(1.5),
            ask_price: dec!(65001),
            ask_size: dec!(2.0),
            timestamp: Utc::now(),
        });
        let DriftValue::Object(obj) = market_event_to_drift_value(&event) else {
            panic!("expected Object");
        };
        assert_eq!(get_str(&obj, "type"), "tick");
        assert_eq!(get_str(&obj, "symbol"), "BTCUSDT");
        assert_eq!(get_str(&obj, "venue"), "binance");
        assert_eq!(get_num(&obj, "bid_price"), 64999.0);
        assert_eq!(get_num(&obj, "ask_size"), 2.0);
    }

    #[test]
    fn trade_with_none_side_and_trade_id_uses_null() {
        let event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        let DriftValue::Object(obj) = market_event_to_drift_value(&event) else {
            panic!("expected Object");
        };
        assert_eq!(get_str(&obj, "type"), "trade");
        assert!(matches!(obj.get("side"), Some(DriftValue::Null)));
        assert!(matches!(obj.get("trade_id"), Some(DriftValue::Null)));
    }

    #[test]
    fn depth_converts_bids_and_asks_as_arrays_of_price_levels() {
        let event = MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![PriceLevel { price: dec!(64999), quantity: dec!(1) }],
            asks: vec![PriceLevel { price: dec!(65001), quantity: dec!(2) }],
            sequence: Some(42),
            is_snapshot: true,
            timestamp: Utc::now(),
        });
        let DriftValue::Object(obj) = market_event_to_drift_value(&event) else {
            panic!("expected Object");
        };
        assert_eq!(get_str(&obj, "type"), "depth");
        let DriftValue::Array(bids) = obj.get("bids").unwrap() else { panic!("expected array") };
        assert_eq!(bids.len(), 1);
        assert_eq!(get_num(&obj, "sequence"), 42.0);
        assert!(matches!(obj.get("is_snapshot"), Some(DriftValue::Bool(true))));
    }

    #[test]
    fn bar_converts_all_ohlcv_fields() {
        let event = MarketEvent::Bar(Bar {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            open: dec!(64000),
            high: dec!(65500),
            low: dec!(63800),
            close: dec!(65000),
            volume: dec!(1234.5),
            interval: BarInterval::OneHour,
            open_time: Utc::now(),
            close_time: Utc::now(),
        });
        let DriftValue::Object(obj) = market_event_to_drift_value(&event) else {
            panic!("expected Object");
        };
        assert_eq!(get_str(&obj, "type"), "bar");
        assert_eq!(get_str(&obj, "interval"), "1h");
        assert_eq!(get_num(&obj, "close"), 65000.0);
    }
}
```

- [ ] **Step 2: Run the tests to see them pass (implementation is inline with test-writing above; confirm it compiles and passes)**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy convert:: -- --nocapture
```
Expected: `4 passed` (the four tests just added). Note: this crate has a genuine `use apexes_core::{Instrument as _, ...}` import that is not actually needed for these functions — remove it now if `cargo build` warns about an unused import; the venue/side/order-type conversions do not require the `Instrument` trait. Fix immediately if `cargo clippy` (Step 6) flags it.

- [ ] **Step 3: Add `position_to_drift_value` and `default_position_drift_value`**

Append to `crates/apexes-strategy/src/drift_dsl/convert.rs`, just above the `#[cfg(test)]`
module:

```rust
/// Converts a `Position` to the `DriftValue::Object` shape returned by
/// `ctx.get_position(symbol)`.
pub fn position_to_drift_value(position: &Position) -> DriftValue {
    let mut obj = HashMap::new();
    obj.insert("symbol".to_string(), string_value(position.symbol.clone()));
    obj.insert("quantity".to_string(), decimal_to_number(position.quantity));
    obj.insert(
        "average_entry_price".to_string(),
        decimal_to_number(position.average_entry_price),
    );
    DriftValue::Object(obj)
}

/// The default value `ctx.get_position(symbol)` returns when there is no
/// existing position for `symbol`: `{ symbol, quantity: 0,
/// average_entry_price: 0 }` (not `null` -- scripts can always read
/// `.quantity` without a null check).
pub fn default_position_drift_value(symbol: &str) -> DriftValue {
    let mut obj = HashMap::new();
    obj.insert("symbol".to_string(), string_value(symbol.to_string()));
    obj.insert("quantity".to_string(), DriftValue::Number(0.0));
    obj.insert("average_entry_price".to_string(), DriftValue::Number(0.0));
    DriftValue::Object(obj)
}
```

- [ ] **Step 4: Write tests for the two new functions**

Add to the `mod tests` block in the same file:

```rust
    #[test]
    fn position_to_drift_value_converts_all_fields() {
        let position = Position {
            symbol: "BTCUSDT".to_string(),
            quantity: dec!(2.5),
            average_entry_price: dec!(64500),
        };
        let DriftValue::Object(obj) = position_to_drift_value(&position) else {
            panic!("expected Object");
        };
        assert_eq!(get_str(&obj, "symbol"), "BTCUSDT");
        assert_eq!(get_num(&obj, "quantity"), 2.5);
        assert_eq!(get_num(&obj, "average_entry_price"), 64500.0);
    }

    #[test]
    fn default_position_drift_value_has_zero_quantity_not_null() {
        let DriftValue::Object(obj) = default_position_drift_value("ETHUSDT") else {
            panic!("expected Object");
        };
        assert_eq!(get_str(&obj, "symbol"), "ETHUSDT");
        assert_eq!(get_num(&obj, "quantity"), 0.0);
        assert_eq!(get_num(&obj, "average_entry_price"), 0.0);
    }
```

- [ ] **Step 5: Run tests to confirm they pass**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy convert:: -- --nocapture
```
Expected: `6 passed`.

- [ ] **Step 6: Add `order_intents_from_drift_value`**

Append to `crates/apexes-strategy/src/drift_dsl/convert.rs`, just above the `#[cfg(test)]`
module:

```rust
fn str_field<'a>(obj: &'a HashMap<String, DriftValue>, key: &str, fn_name: &str) -> Result<&'a str, String> {
    match obj.get(key) {
        Some(DriftValue::String(s)) => Ok(s.as_str()),
        Some(other) => Err(format!(
            "{fn_name}: field {key:?} must be a string, got {}",
            drift_value_type_name(other)
        )),
        None => Err(format!("{fn_name}: missing required field {key:?}")),
    }
}

fn num_field(obj: &HashMap<String, DriftValue>, key: &str, fn_name: &str) -> Result<f64, String> {
    match obj.get(key) {
        Some(DriftValue::Number(n)) => Ok(*n),
        Some(other) => Err(format!(
            "{fn_name}: field {key:?} must be a number, got {}",
            drift_value_type_name(other)
        )),
        None => Err(format!("{fn_name}: missing required field {key:?}")),
    }
}

fn optional_num_field(obj: &HashMap<String, DriftValue>, key: &str, fn_name: &str) -> Result<Option<f64>, String> {
    match obj.get(key) {
        None | Some(DriftValue::Null) | Some(DriftValue::Undefined) => Ok(None),
        Some(DriftValue::Number(n)) => Ok(Some(*n)),
        Some(other) => Err(format!(
            "{fn_name}: field {key:?} must be a number if present, got {}",
            drift_value_type_name(other)
        )),
    }
}

fn drift_value_type_name(value: &DriftValue) -> &'static str {
    match value {
        DriftValue::Null => "null",
        DriftValue::Undefined => "undefined",
        DriftValue::Bool(_) => "bool",
        DriftValue::Number(_) => "number",
        DriftValue::String(_) => "string",
        DriftValue::Array(_) => "array",
        DriftValue::Object(_) => "object",
        DriftValue::Function(_) => "function",
        DriftValue::NativeFunction(_) => "native function",
        DriftValue::Promise(_) => "promise",
        DriftValue::Regex { .. } => "regex",
    }
}

fn parse_side(s: &str, fn_name: &str) -> Result<Side, String> {
    match s.to_ascii_lowercase().as_str() {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        other => Err(format!("{fn_name}: unknown side {other:?} (expected \"buy\" or \"sell\")")),
    }
}

fn parse_order_type(s: &str, fn_name: &str) -> Result<OrderType, String> {
    match s.to_ascii_lowercase().as_str() {
        "market" => Ok(OrderType::Market),
        "limit" => Ok(OrderType::Limit),
        "stop" => Ok(OrderType::Stop),
        "stop_limit" => Ok(OrderType::StopLimit),
        other => Err(format!(
            "{fn_name}: unknown order_type {other:?} (expected \"market\", \"limit\", \"stop\", or \"stop_limit\")"
        )),
    }
}

fn parse_venue(s: &str, fn_name: &str) -> Result<Venue, String> {
    match s.to_ascii_lowercase().as_str() {
        "sse" => Ok(Venue::Sse),
        "szse" => Ok(Venue::Szse),
        "hkex" => Ok(Venue::Hkex),
        "nasdaq" => Ok(Venue::Nasdaq),
        "nyse" => Ok(Venue::Nyse),
        "binance" => Ok(Venue::Binance),
        "okx" => Ok(Venue::Okx),
        "coinbase" => Ok(Venue::Coinbase),
        other => Err(format!("{fn_name}: unknown venue {other:?}")),
    }
}

fn f64_to_decimal(value: f64, field: &str, fn_name: &str) -> Result<rust_decimal::Decimal, String> {
    rust_decimal::Decimal::try_from(value)
        .map_err(|_| format!("{fn_name}: field {field:?} is not a finite number ({value})"))
}

/// Parses a Drift Script strategy's `on_market_event` return value (expected
/// to be a `DriftValue::Array` of order-intent objects) into
/// `Vec<OrderIntent>`. Returns `Err(message)` on any structural or
/// validation failure -- the caller ([`crate::drift_dsl::DriftScriptStrategy`])
/// logs the message and returns an empty `Vec` rather than propagating it,
/// per the project's "single tick's strategy failure must not halt the
/// loop" requirement.
pub fn order_intents_from_drift_value(value: DriftValue) -> Result<Vec<OrderIntent>, String> {
    const FN: &str = "on_market_event return value";
    let DriftValue::Array(items) = value else {
        return Err(format!("{FN}: expected an array, got {}", drift_value_type_name(&value)));
    };

    items
        .into_iter()
        .enumerate()
        .map(|(i, item)| {
            let DriftValue::Object(obj) = item else {
                return Err(format!(
                    "{FN}[{i}]: expected an object, got {}",
                    drift_value_type_name(&item)
                ));
            };

            let symbol = str_field(&obj, "symbol", FN)?.to_string();
            let venue = parse_venue(str_field(&obj, "venue", FN)?, FN)?;
            let side = parse_side(str_field(&obj, "side", FN)?, FN)?;
            let order_type = parse_order_type(str_field(&obj, "order_type", FN)?, FN)?;
            let quantity = f64_to_decimal(num_field(&obj, "quantity", FN)?, "quantity", FN)?;

            let price = optional_num_field(&obj, "price", FN)?
                .map(|n| f64_to_decimal(n, "price", FN))
                .transpose()?;
            let stop_price = optional_num_field(&obj, "stop_price", FN)?
                .map(|n| f64_to_decimal(n, "stop_price", FN))
                .transpose()?;

            match order_type {
                OrderType::Limit | OrderType::StopLimit if price.is_none() => {
                    return Err(format!("{FN}[{i}]: order_type {order_type:?} requires a \"price\" field"));
                }
                OrderType::Stop | OrderType::StopLimit if stop_price.is_none() => {
                    return Err(format!(
                        "{FN}[{i}]: order_type {order_type:?} requires a \"stop_price\" field"
                    ));
                }
                _ => {}
            }

            Ok(OrderIntent { symbol, venue, side, order_type, quantity, price, stop_price })
        })
        .collect()
}
```

- [ ] **Step 7: Write tests covering valid input and every validation failure mode**

Add to the `mod tests` block:

```rust
    fn drift_object(fields: &[(&str, DriftValue)]) -> DriftValue {
        let mut obj = HashMap::new();
        for (k, v) in fields {
            obj.insert(k.to_string(), v.clone());
        }
        DriftValue::Object(obj)
    }

    #[test]
    fn valid_market_order_intent_parses() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("Binance")),
            ("side", string_value("BUY")),
            ("order_type", string_value("market")),
            ("quantity", DriftValue::Number(1.5)),
        ])]);
        let intents = order_intents_from_drift_value(value).unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].symbol, "BTCUSDT");
        assert_eq!(intents[0].venue, Venue::Binance);
        assert_eq!(intents[0].side, Side::Buy);
        assert_eq!(intents[0].order_type, OrderType::Market);
        assert_eq!(intents[0].quantity, dec!(1.5));
        assert_eq!(intents[0].price, None);
    }

    #[test]
    fn valid_limit_order_intent_with_price_parses() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("side", string_value("sell")),
            ("order_type", string_value("limit")),
            ("quantity", DriftValue::Number(1.0)),
            ("price", DriftValue::Number(65000.0)),
        ])]);
        let intents = order_intents_from_drift_value(value).unwrap();
        assert_eq!(intents[0].order_type, OrderType::Limit);
        assert_eq!(intents[0].price, Some(dec!(65000)));
    }

    #[test]
    fn non_array_top_level_is_rejected() {
        let err = order_intents_from_drift_value(DriftValue::Number(1.0)).unwrap_err();
        assert!(err.contains("expected an array"));
    }

    #[test]
    fn non_object_element_is_rejected() {
        let value = DriftValue::Array(vec![DriftValue::Number(1.0)]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("expected an object"));
    }

    #[test]
    fn missing_required_field_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("side", string_value("buy")),
            ("order_type", string_value("market")),
            // missing "quantity"
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("quantity"));
    }

    #[test]
    fn invalid_side_string_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("side", string_value("sideways")),
            ("order_type", string_value("market")),
            ("quantity", DriftValue::Number(1.0)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("unknown side"));
    }

    #[test]
    fn invalid_order_type_string_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("side", string_value("buy")),
            ("order_type", string_value("trailing_stop")),
            ("quantity", DriftValue::Number(1.0)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("unknown order_type"));
    }

    #[test]
    fn invalid_venue_string_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("kraken")),
            ("side", string_value("buy")),
            ("order_type", string_value("market")),
            ("quantity", DriftValue::Number(1.0)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("unknown venue"));
    }

    #[test]
    fn limit_order_missing_price_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("side", string_value("buy")),
            ("order_type", string_value("limit")),
            ("quantity", DriftValue::Number(1.0)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("requires a \"price\" field"));
    }

    #[test]
    fn stop_order_missing_stop_price_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("side", string_value("buy")),
            ("order_type", string_value("stop")),
            ("quantity", DriftValue::Number(1.0)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("requires a \"stop_price\" field"));
    }

    #[test]
    fn non_finite_quantity_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("side", string_value("buy")),
            ("order_type", string_value("market")),
            ("quantity", DriftValue::Number(f64::NAN)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("not a finite number"));
    }
```

- [ ] **Step 8: Run all convert.rs tests to confirm they pass**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy convert:: -- --nocapture
```
Expected: `17 passed` (6 from Steps 1-5 + 11 from Step 7 — recount exact number against your
actual test list before treating a mismatch as a failure; the important thing is 0 failed).

- [ ] **Step 9: Remove the unused `Instrument` import if clippy flags it**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-strategy --all-targets -- -D warnings
```
If this reports an unused import for `apexes_core::Instrument`, edit the `use` line at the top
of `convert.rs` to remove `Instrument as _,`:
```rust
use apexes_core::{MarketEvent, OrderType, Side, Venue};
```
Re-run clippy to confirm clean.

- [ ] **Step 10: Run fmt**

```bash
cd ~/src/oss/apexes
cargo fmt --check
```
If it reports diffs, run `cargo fmt` (not `--check`) to apply them, then re-run `--check` to
confirm clean.

- [ ] **Step 11: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-strategy/src/drift_dsl/convert.rs
git commit -S -s -m "feat: implement apexes-strategy <-> DriftValue conversion layer"
git push origin master
```

---

### Task 5: Implement `ctx` bindings and `DriftScriptStrategy`

**Files:**
- Modify: `crates/apexes-strategy/src/drift_dsl/bindings.rs`
- Modify: `crates/apexes-strategy/src/drift_dsl/strategy.rs`
- Create: `crates/apexes-strategy/tests/fixtures/always_buy.drift`
- Create: `crates/apexes-strategy/tests/fixtures/uses_math.drift`
- Create: `crates/apexes-strategy/tests/fixtures/uses_ctx_position.drift`
- Create: `crates/apexes-strategy/tests/fixtures/syntax_error.drift`
- Create: `crates/apexes-strategy/tests/fixtures/runtime_panic.drift`
- Create: `crates/apexes-strategy/tests/drift_script_strategy.rs`

**Interfaces:**
- Consumes: `drift_script::{DriftValue, Interpreter, ScriptParser}`, `drift_script_math::register_to`,
  `crate::drift_dsl::convert::*`, `crate::error::StrategyError`, `crate::strategy::{Strategy, StrategyContext}`.
- Produces: `pub struct DriftScriptStrategy` implementing `Strategy`, with
  `pub fn from_file(path: &std::path::Path) -> Result<Self, StrategyError>`.

- [ ] **Step 1: Implement the `ctx.*` native function bindings in `bindings.rs`**

Replace the content of `crates/apexes-strategy/src/drift_dsl/bindings.rs` with:

```rust
//! `ctx.*` native-function bindings exposed to Drift Script strategy
//! scripts: `get_position`, `get_balance`, `now`, `has_open_order`. Built
//! fresh on every `DriftScriptStrategy::on_market_event` call from owned
//! clones of the current `StrategyContext`'s borrowed maps, since
//! `DriftValue::NativeFunction` requires `'static` closures while
//! `StrategyContext<'a>`'s maps are only borrowed for `'a` (see design
//! spec §6).

use apexes_core::{Order, OrderId};
use chrono::{DateTime, Utc};
use drift_script::{DriftValue, NativeFn};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;

use crate::drift_dsl::convert::{default_position_drift_value, position_to_drift_value};
use crate::strategy::Position;

/// Builds the `ctx` object passed as the second argument to a strategy
/// script's `on_market_event(event, ctx)` function, from owned clones of
/// the current tick's state. `now` is baked in directly as a string field;
/// `get_position`/`get_balance`/`has_open_order` are `NativeFunction`
/// closures capturing owned clones of `positions`/`balances`/`open_orders`.
pub fn build_ctx_drift_value(
    positions: HashMap<String, Position>,
    balances: HashMap<String, Decimal>,
    open_orders: HashMap<OrderId, Order>,
    now: DateTime<Utc>,
) -> DriftValue {
    let mut obj: HashMap<String, DriftValue> = HashMap::new();
    obj.insert("now".to_string(), DriftValue::String(Arc::new(now.to_rfc3339())));
    obj.insert("get_position".to_string(), DriftValue::NativeFunction(get_position_fn(positions)));
    obj.insert("get_balance".to_string(), DriftValue::NativeFunction(get_balance_fn(balances)));
    obj.insert(
        "has_open_order".to_string(),
        DriftValue::NativeFunction(has_open_order_fn(open_orders)),
    );
    DriftValue::Object(obj)
}

fn first_string_arg(args: &[DriftValue]) -> Option<String> {
    match args.first() {
        Some(DriftValue::String(s)) => Some(s.as_str().to_string()),
        _ => None,
    }
}

fn get_position_fn(positions: HashMap<String, Position>) -> NativeFn {
    Arc::new(move |args| {
        let positions = positions.clone();
        Box::pin(async move {
            let Some(symbol) = first_string_arg(&args) else {
                return DriftValue::Undefined;
            };
            match positions.get(&symbol) {
                Some(position) => position_to_drift_value(position),
                None => default_position_drift_value(&symbol),
            }
        })
    })
}

fn get_balance_fn(balances: HashMap<String, Decimal>) -> NativeFn {
    Arc::new(move |args| {
        let balances = balances.clone();
        Box::pin(async move {
            let Some(asset) = first_string_arg(&args) else {
                return DriftValue::Undefined;
            };
            let value = balances.get(&asset).copied().unwrap_or(Decimal::ZERO);
            DriftValue::Number(value.try_into().unwrap_or(0.0))
        })
    })
}

fn has_open_order_fn(open_orders: HashMap<OrderId, Order>) -> NativeFn {
    Arc::new(move |args| {
        let open_orders = open_orders.clone();
        Box::pin(async move {
            let Some(symbol) = first_string_arg(&args) else {
                return DriftValue::Undefined;
            };
            let has_any = open_orders.values().any(|order| order.symbol == symbol);
            DriftValue::Bool(has_any)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{OrderStatus, OrderType, Side, Venue};
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn call(f: &NativeFn, args: Vec<DriftValue>) -> DriftValue {
        futures::executor::block_on(f(args))
    }

    #[test]
    fn get_position_returns_default_when_absent() {
        let f = get_position_fn(HashMap::new());
        let DriftValue::Object(obj) = call(&f, vec![DriftValue::String(Arc::new("BTCUSDT".to_string()))]) else {
            panic!("expected Object");
        };
        assert!(matches!(obj.get("quantity"), Some(DriftValue::Number(n)) if *n == 0.0));
    }

    #[test]
    fn get_position_returns_existing_position() {
        let mut positions = HashMap::new();
        positions.insert(
            "BTCUSDT".to_string(),
            Position { symbol: "BTCUSDT".to_string(), quantity: dec!(2), average_entry_price: dec!(64000) },
        );
        let f = get_position_fn(positions);
        let DriftValue::Object(obj) = call(&f, vec![DriftValue::String(Arc::new("BTCUSDT".to_string()))]) else {
            panic!("expected Object");
        };
        assert!(matches!(obj.get("quantity"), Some(DriftValue::Number(n)) if *n == 2.0));
    }

    #[test]
    fn get_balance_returns_zero_when_absent() {
        let f = get_balance_fn(HashMap::new());
        let result = call(&f, vec![DriftValue::String(Arc::new("USDT".to_string()))]);
        assert!(matches!(result, DriftValue::Number(n) if n == 0.0));
    }

    #[test]
    fn get_balance_returns_existing_balance() {
        let mut balances = HashMap::new();
        balances.insert("USDT".to_string(), dec!(10000));
        let f = get_balance_fn(balances);
        let result = call(&f, vec![DriftValue::String(Arc::new("USDT".to_string()))]);
        assert!(matches!(result, DriftValue::Number(n) if n == 10000.0));
    }

    #[test]
    fn has_open_order_false_when_none_match() {
        let f = has_open_order_fn(HashMap::new());
        let result = call(&f, vec![DriftValue::String(Arc::new("BTCUSDT".to_string()))]);
        assert!(matches!(result, DriftValue::Bool(false)));
    }

    #[test]
    fn has_open_order_true_when_any_side_matches() {
        let mut open_orders = HashMap::new();
        let id = OrderId(Uuid::new_v4());
        open_orders.insert(
            id,
            Order {
                id,
                exchange_order_id: None,
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                side: Side::Sell,
                order_type: OrderType::Limit,
                quantity: dec!(1),
                price: Some(dec!(65000)),
                stop_price: None,
                filled_quantity: dec!(0),
                status: OrderStatus::New,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        );
        let f = has_open_order_fn(open_orders);
        let result = call(&f, vec![DriftValue::String(Arc::new("BTCUSDT".to_string()))]);
        assert!(matches!(result, DriftValue::Bool(true)));
    }
}
```

**Note:** the `Order` literal above uses `apexes_core::Order`'s full, verified field list
(`id`, `exchange_order_id`, `symbol`, `venue`, `side`, `order_type`, `quantity`, `price`,
`stop_price`, `filled_quantity`, `status`, `created_at`, `updated_at`) — confirmed by reading
`crates/apexes-core/src/order.rs` directly. If a future refactor changes `Order`'s fields
before this task runs, update the literal to match.

- [ ] **Step 2: Run the bindings tests**

(`futures` and `uuid` dev-dependencies were already added to `crates/apexes-strategy/Cargo.toml`
in Task 2 Step 1, so no `Cargo.toml` change is needed here.)

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy bindings:: -- --nocapture
```
Expected: `6 passed`. If the `Order` literal in the last test doesn't compile because the real
struct has different/additional fields, fix the literal to match (per the note in Step 1) and
re-run.

- [ ] **Step 3: Implement `DriftScriptStrategy` in `strategy.rs`**

Replace the content of `crates/apexes-strategy/src/drift_dsl/strategy.rs` with:

```rust
//! [`DriftScriptStrategy`]: a [`crate::Strategy`] implementation that runs
//! a user-authored Drift Script (`.drift`) file as the strategy's decision
//! logic, so researchers can iterate on strategy behavior without a Rust
//! compile cycle.

use drift_script::{DriftValue, Interpreter, ScriptParser};
use std::path::{Path, PathBuf};

use crate::drift_dsl::bindings::build_ctx_drift_value;
use crate::drift_dsl::convert::{market_event_to_drift_value, order_intents_from_drift_value};
use crate::error::StrategyError;
use crate::strategy::{OrderIntent, Strategy, StrategyContext};

/// Runs a `.drift` script's top-level `on_market_event(event, ctx)`
/// function as a [`Strategy`] implementation. The script is parsed and its
/// top-level statements executed exactly once, at construction
/// ([`DriftScriptStrategy::from_file`]); `drift-script-math`'s `math.*`
/// bindings are registered automatically, so scripts can use
/// `math.mean()`/`math.stddev()`/etc. out of the box.
pub struct DriftScriptStrategy {
    interpreter: Interpreter,
    on_market_event_fn: DriftValue,
}

impl DriftScriptStrategy {
    /// Reads and parses `path`, executes its top-level statements (so a
    /// `function on_market_event(event, ctx) { ... }` declaration enters
    /// the interpreter's environment), then resolves that function's
    /// value. Returns `Err` if the file cannot be read, fails to parse, or
    /// does not define a callable `on_market_event` function at the top
    /// level -- construction fails closed, never producing a
    /// partially-usable instance.
    pub fn from_file(path: &Path) -> Result<Self, StrategyError> {
        let source = std::fs::read_to_string(path).map_err(|source| StrategyError::Io {
            path: PathBuf::from(path),
            source,
        })?;

        let stmts = ScriptParser::new().parse(&source).map_err(|e| StrategyError::Parse(e.to_string()))?;

        let mut interpreter = Interpreter::new();
        drift_script_math::register_to(&interpreter);

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

        Ok(Self { interpreter, on_market_event_fn })
    }
}

impl Strategy for DriftScriptStrategy {
    fn on_market_event(
        &mut self,
        event: &apexes_core::MarketEvent,
        ctx: &StrategyContext,
    ) -> Vec<OrderIntent> {
        let ctx_value = build_ctx_drift_value(
            ctx.positions.clone(),
            ctx.balances.clone(),
            ctx.open_orders.clone(),
            ctx.now,
        );
        let event_value = market_event_to_drift_value(event);

        let handle = tokio::runtime::Handle::current();
        let call_result =
            handle.block_on(self.interpreter.call(self.on_market_event_fn.clone(), vec![event_value, ctx_value]));

        match call_result {
            Ok(value) => match order_intents_from_drift_value(value) {
                Ok(intents) => intents,
                Err(msg) => {
                    tracing::error!("on_market_event returned an invalid value: {msg}");
                    vec![]
                }
            },
            Err(e) => {
                tracing::error!("on_market_event script error: {e}");
                vec![]
            }
        }
    }
}
```

**Note:** `ctx.positions.clone()`/`ctx.balances.clone()`/`ctx.open_orders.clone()` require
`HashMap<String, Position>: Clone`, `HashMap<String, Decimal>: Clone`, and
`HashMap<OrderId, Order>: Clone` — `Position` already derives `Clone` (confirmed in Task 2's
migrated code), `Decimal` and `OrderId` are `Copy`, and `Order` derives `Clone` in
`apexes-core` (confirmed by its existing `#[derive(Debug, Clone, PartialEq, ...)]` in
`crates/apexes-core/src/order.rs`) — no additional derives needed.

- [ ] **Step 4: Verify it compiles**

```bash
cd ~/src/oss/apexes
cargo build -p apexes-strategy
```
Expected: succeeds. `drift_script_math` needs to be added as a direct dependency if not already
present from Task 2's `Cargo.toml` (it is — confirm the `drift-script-math = { git = ... }` line
is present).

- [ ] **Step 5: Create the five `.drift` test fixture scripts**

```bash
mkdir -p ~/src/oss/apexes/crates/apexes-strategy/tests/fixtures
cat > ~/src/oss/apexes/crates/apexes-strategy/tests/fixtures/always_buy.drift << 'EOF'
function on_market_event(event, ctx) {
  return [{
    symbol: event.symbol,
    venue: event.venue,
    side: "buy",
    order_type: "market",
    quantity: 1,
  }];
}
EOF
cat > ~/src/oss/apexes/crates/apexes-strategy/tests/fixtures/uses_math.drift << 'EOF'
function on_market_event(event, ctx) {
  let prices = [event.price, event.price, event.price];
  let avg = math.mean(prices);
  return [{
    symbol: event.symbol,
    venue: event.venue,
    side: "buy",
    order_type: "market",
    quantity: avg > 0 ? 1 : 0,
  }];
}
EOF
cat > ~/src/oss/apexes/crates/apexes-strategy/tests/fixtures/uses_ctx_position.drift << 'EOF'
function on_market_event(event, ctx) {
  let position = ctx.get_position(event.symbol);
  let balance = ctx.get_balance("USDT");
  let has_open = ctx.has_open_order(event.symbol);

  if (position.quantity > 0) {
    return [];
  }
  if (has_open) {
    return [];
  }

  return [{
    symbol: event.symbol,
    venue: event.venue,
    side: "buy",
    order_type: "market",
    quantity: 1,
  }];
}
EOF
cat > ~/src/oss/apexes/crates/apexes-strategy/tests/fixtures/syntax_error.drift << 'EOF'
function on_market_event(event, ctx) {
  return [ {{{ this is not valid syntax
}
EOF
cat > ~/src/oss/apexes/crates/apexes-strategy/tests/fixtures/runtime_panic.drift << 'EOF'
function on_market_event(event, ctx) {
  return this_variable_does_not_exist.foo;
}
EOF
```

- [ ] **Step 6: Write the end-to-end driver test**

Create `crates/apexes-strategy/tests/drift_script_strategy.rs`:

```rust
use apexes_core::{MarketEvent, Side, TradeEvent, Venue};
use apexes_strategy::drift_dsl::DriftScriptStrategy;
use apexes_strategy::{Position, Strategy, StrategyContext};
use chrono::Utc;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::path::PathBuf;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

fn sample_trade_event() -> MarketEvent {
    MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        price: dec!(65000),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: Utc::now(),
    })
}

fn empty_ctx(now: chrono::DateTime<Utc>) -> (HashMap<String, Position>, HashMap<String, rust_decimal::Decimal>, HashMap<apexes_core::OrderId, apexes_core::Order>, chrono::DateTime<Utc>) {
    (HashMap::new(), HashMap::new(), HashMap::new(), now)
}

#[tokio::test]
async fn always_buy_returns_one_buy_intent() {
    let mut strategy = DriftScriptStrategy::from_file(&fixture_path("always_buy.drift")).unwrap();
    let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
    let ctx = StrategyContext { positions: &positions, balances: &balances, open_orders: &open_orders, now };
    let event = sample_trade_event();

    let intents = strategy.on_market_event(&event, &ctx);

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].symbol, "BTCUSDT");
    assert_eq!(intents[0].venue, Venue::Binance);
    assert_eq!(intents[0].side, Side::Buy);
}

#[tokio::test]
async fn uses_math_binding_computes_average_and_buys() {
    let mut strategy = DriftScriptStrategy::from_file(&fixture_path("uses_math.drift")).unwrap();
    let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
    let ctx = StrategyContext { positions: &positions, balances: &balances, open_orders: &open_orders, now };
    let event = sample_trade_event();

    let intents = strategy.on_market_event(&event, &ctx);

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].quantity, dec!(1));
}

#[tokio::test]
async fn uses_ctx_position_skips_when_already_long() {
    let mut strategy = DriftScriptStrategy::from_file(&fixture_path("uses_ctx_position.drift")).unwrap();
    let mut positions = HashMap::new();
    positions.insert(
        "BTCUSDT".to_string(),
        Position { symbol: "BTCUSDT".to_string(), quantity: dec!(1), average_entry_price: dec!(64000) },
    );
    let balances = HashMap::new();
    let open_orders = HashMap::new();
    let ctx = StrategyContext { positions: &positions, balances: &balances, open_orders: &open_orders, now: Utc::now() };
    let event = sample_trade_event();

    let intents = strategy.on_market_event(&event, &ctx);

    assert_eq!(intents.len(), 0, "should not buy again when already holding a position");
}

#[tokio::test]
async fn uses_ctx_position_buys_when_flat_and_no_open_orders() {
    let mut strategy = DriftScriptStrategy::from_file(&fixture_path("uses_ctx_position.drift")).unwrap();
    let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
    let ctx = StrategyContext { positions: &positions, balances: &balances, open_orders: &open_orders, now };
    let event = sample_trade_event();

    let intents = strategy.on_market_event(&event, &ctx);

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].side, Side::Buy);
}

#[tokio::test]
async fn syntax_error_script_fails_construction() {
    let result = DriftScriptStrategy::from_file(&fixture_path("syntax_error.drift"));
    assert!(result.is_err());
}

#[tokio::test]
async fn missing_entry_point_fails_construction() {
    let path = fixture_path("no_entry_point_temp.drift");
    std::fs::write(&path, "let x = 1;").unwrap();
    let result = DriftScriptStrategy::from_file(&path);
    std::fs::remove_file(&path).ok();
    assert!(matches!(result, Err(apexes_strategy::StrategyError::MissingEntryPoint)));
}

#[tokio::test]
async fn runtime_error_script_returns_empty_vec_not_panic() {
    let mut strategy = DriftScriptStrategy::from_file(&fixture_path("runtime_panic.drift")).unwrap();
    let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
    let ctx = StrategyContext { positions: &positions, balances: &balances, open_orders: &open_orders, now };
    let event = sample_trade_event();

    let intents = strategy.on_market_event(&event, &ctx);

    assert_eq!(intents, vec![], "runtime script error must yield an empty Vec, not panic");
}
```

**Note:** `DriftScriptStrategy::from_file` internally calls `tokio::runtime::Handle::current()`,
which requires an active tokio runtime on the calling thread — hence every test above is
`#[tokio::test]`, matching the design's documented constraint (design spec §七, last paragraph).

- [ ] **Step 7: Add `apexes-strategy`'s `lib.rs` re-export for the integration test to use `drift_dsl::DriftScriptStrategy` and `StrategyError`**

Confirm `crates/apexes-strategy/src/lib.rs` (from Task 3 Step 4) already has `pub mod drift_dsl;`
and `pub use error::StrategyError;` — the integration test references
`apexes_strategy::drift_dsl::DriftScriptStrategy` and `apexes_strategy::StrategyError` directly,
both already public from Task 3. No further `lib.rs` change needed.

- [ ] **Step 8: Run the end-to-end tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy --test drift_script_strategy -- --nocapture
```
Expected: `7 passed`. If `always_buy_returns_one_buy_intent` fails because the script's
`event.venue` string doesn't match a `parse_venue` case (e.g. script produces `"Binance"` but
`market_event_to_drift_value` emits lowercase `"binance"` and the script just echoes it back
unchanged — that's consistent, since `parse_venue` is case-insensitive), re-check the actual
failure message before assuming a bug; the case-insensitive matching in `convert.rs` should
handle whatever casing the script produces.

- [ ] **Step 9: Run the full `apexes-strategy` test suite**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy
```
Expected: all unit tests (`strategy.rs`, `convert.rs`, `bindings.rs`) plus the integration test
file all pass, 0 failed.

- [ ] **Step 10: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-strategy --all-targets -- -D warnings
cargo fmt --check
```
Expected: no warnings, no formatting diffs. Fix any issues (most likely: unused imports in
`strategy.rs` if a helper wasn't actually needed, or minor formatting) and re-run until clean.

- [ ] **Step 11: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-strategy/src/drift_dsl/bindings.rs crates/apexes-strategy/src/drift_dsl/strategy.rs crates/apexes-strategy/tests crates/apexes-strategy/Cargo.toml
git commit -S -s -m "feat: implement DriftScriptStrategy and ctx bindings for apexes-strategy"
git push origin master
```

---

### Task 6: Final whole-effort verification

**Files:** None created/modified — this task only runs verification commands and confirms the
end state.

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
Expected: all tests across all crates pass, including `apexes-core`, `apexes-market-data`,
`apexes-storage`, `apexes-backtest` (with its two migrated tests now sourced from
`apexes-strategy` but still exercised via `apexes-backtest`'s own existing tests that use
`Strategy`/`OrderIntent`), and the new `apexes-strategy` crate's unit + integration tests.
Report the exact total test count broken down by crate.

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

- [ ] **Step 5: Confirm no lingering references to the deleted `apexes-backtest::strategy` module**

```bash
cd ~/src/oss/apexes
grep -rn "apexes_backtest::strategy\|use crate::strategy::" crates/apexes-backtest/src
```
Expected: no output (zero matches) — confirms `apexes-backtest` no longer has its own
`strategy` module and every internal reference goes through the `apexes_strategy::*`
re-export at the crate root.

- [ ] **Step 6: Update `docs/FEATURES.md`**

Read `docs/FEATURES.md`, find row `2.1` (`Strategy development SDK (Rust native trait API +
PyO3 bindings for research)`, currently `🔲 Not Started`). Update its Status to `✅ Done` and
its Notes column to:
```
Strategy trait/StrategyContext/OrderIntent/Position migrated to new apexes-strategy crate (was in apexes-backtest); DriftScriptStrategy runs .drift scripts via drift-dsl. PyO3 bindings deferred to a future pass.
```
Leave rows `2.2`-`2.5` (factor library, multi-signal combination, parameter optimization,
strategy versioning) as `🔲 Not Started` — those are explicitly out of scope for this plan.

Update the Summary counts table at the bottom of the file (increment `✅ Done` by 1, decrement
`🔲 Not Started` by 1) and the "Current active work" paragraph to mention sub-project #4's
first phase is complete and name what's next (`#4.2` factor library, or `#5` risk engine, or
PyO3 bindings — whichever the user directs at that point; don't guess, just note the milestone
and leave the "what's next" decision unstated if unsure).

- [ ] **Step 7: Commit the FEATURES.md update**

```bash
cd ~/src/oss/apexes
git add docs/FEATURES.md
git commit -S -s -m "docs: mark strategy SDK Rust trait API + Drift Script integration complete in FEATURES.md"
git push origin master
```

- [ ] **Step 8: Report final summary**

Summarize: `apexes-strategy` crate's file list and line counts, total test count (broken down:
unit tests in `strategy.rs`/`convert.rs`/`bindings.rs`, integration tests in
`tests/drift_script_strategy.rs`), confirmation that `apexes-backtest`'s pre-existing tests
still pass unchanged after the migration, the `drift-dsl` commit SHA from Task 1, and the
`apexes` commit SHAs for Tasks 2-6. No further commit needed for this verification-only task.

---

## Plan Self-Review Notes

**Spec coverage:** Design spec §一 (scope: new `apexes-strategy` crate, `DriftScriptStrategy`,
`apexes-backtest` migration, `drift-dsl` increment, explicitly no PyO3) → Tasks 1-5 cover all
four in-scope items, Task 6 confirms nothing extra was added. §二 (crate structure, `drift_dsl`
submodule naming per the folder-rename correction) → Tasks 2-3 (note: this plan uses
`drift_dsl` as the submodule name throughout, matching the corrected spec). §三 (Strategy/
StrategyContext/OrderIntent/Position migrated verbatim) → Task 2 Steps 3-4 copy the exact
existing file content. §四 (`Interpreter::call()`) → Task 1. §五 (conversion layer, hand-written,
no serde) → Task 4. §六 (ctx bindings, clone-per-call lifecycle solution) → Task 5 Steps 1-3.
§七 (`DriftScriptStrategy` construction/execution flow, `block_on`, runtime-error-to-empty-Vec
behavior) → Task 5 Steps 4-5. §八 (`StrategyError`) → Task 3 Step 1. §九 (testing strategy: unit
tests for convert.rs, end-to-end `.drift` fixture tests, apexes-backtest migration
verification, full workspace verification) → Task 4 (unit), Task 5 (end-to-end), Task 6 (full
workspace + migration check). §十 (non-goals: no PyO3, no trading indicators, no hot-reload) —
confirmed none of Tasks 1-6 do any of these.

**Placeholder scan:** No "TBD"/"TODO" strings in any task. Every code step shows complete,
copy-pasteable code, including the `Order` literal in Task 5 Step 1's test (verified against
`crates/apexes-core/src/order.rs`'s real field list, including `filled_quantity`, before this
plan was finalized).

**Type consistency:** `Strategy`/`StrategyContext`/`OrderIntent`/`Position` field names and
types are identical across Task 2 (migration), Task 4 (`convert.rs` functions reference
`Position.symbol`/`.quantity`/`.average_entry_price`, `OrderIntent`'s 7 fields), and Task 5
(`bindings.rs`/`strategy.rs` construct/consume the same types) — cross-checked against the one
canonical definition copied into Task 2 Step 4. `StrategyError`'s four variants defined in Task
3 Step 1 are referenced identically in Task 5 Step 4 (`from_file`'s error returns) and Task 5
Step 7's `missing_entry_point_fails_construction` test. `drift_script::Interpreter::call()`'s
signature from Task 1 Step 2 (`pub async fn call(&mut self, func: DriftValue, args:
Vec<DriftValue>) -> InterpretResult`) matches its call site in Task 5 Step 4
(`self.interpreter.call(self.on_market_event_fn.clone(), vec![event_value, ctx_value])`).
