# Apexes #13c: Strategy Monitoring & Live Tuning Panel (+ row 8.3 message-bus min-viable + multi-process session architecture) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the design in
`docs/superpowers/specs/2026-08-05-apexes-13c-strategy-monitoring-tuning-design.md`:
a live-tunable, monitorable strategy session architecture built on three
new pieces — (1) `Strategy` trait extension (`current_params`/`set_param`,
both optional/default-implemented) plus a `ControlHandle` wired directly
into `apexes_backtest::unified_loop::run_unified_event_loop`'s existing
`UnifiedLoopConfig`, giving every one of `run_backtest`/
`run_paper_trading`/`run_live_trading` identical hot-tuning/pause/resume
behavior for free; (2) a new `apexes-market-data-gateway` binary crate
(row `8.3`'s minimum viable message bus: one shared real market-data
connection, real-time cross-process broadcast over WebSocket, optional
historical replay + async persistence both reusing the existing
`apexes-storage` ClickHouse layer verbatim); (3) a new `apexes-engine`
binary crate (one OS process per trading session, optional CPU affinity,
its own `axum` `/session/ws` control+snapshot server) that `apexes-ui`
spawns and supervises rather than hosting sessions in its own process.
`apexes-ui` itself becomes a pure client: a process supervisor plus two
new UI tabs (session launcher, monitoring/tuning).

**Architecture:** Fourteen sequential tasks in dependency order. Tasks
1-2 extend the shared core (`apexes-strategy`, `apexes-backtest`) with
zero behavioral impact on any existing caller (every new trait method
has a default impl; `UnifiedLoopConfig.control` defaults to `None`).
Tasks 3-5 build `apexes-market-data-gateway` end-to-end. Tasks 6-8 build
`apexes-engine` end-to-end, depending on Tasks 1-2's new types and
consuming (not modifying) the gateway's public WS protocol from Tasks
3-5. Tasks 9-11 extend `apexes-ui` into a process supervisor plus the
two new tabs, depending on everything before it. Task 12 is a small
documentation task (the Drift Script `param()` hot-reload authoring
convention). Task 13 is a full-workspace verification checkpoint. Task
14 is the final whole-branch review and `docs/FEATURES.md` update.

**Tech Stack:** Rust 2024, existing workspace deps reused throughout
(`axum`, `tokio-tungstenite`, `serde_json`, `clap`, `egui`/`eframe`,
`apexes-storage`, `apexes-message-bus`'s existing `MarketEventBus` code
is NOT reused directly by the new gateway — see Task 3's own note on
why). One genuinely new workspace dependency: `core_affinity` (CPU
affinity binding, first system-call-adjacent dependency this workspace
has ever taken on; used only in Tasks 5 and 7's `main.rs` files, guarded
so a failure to set affinity never aborts the process).

## Global Constraints

- Rust edition 2024, workspace-wide `cargo check`/
  `cargo clippy --all-targets -- -D warnings`/`cargo fmt --check` must
  stay clean after every task.
- No `unsafe` blocks. Never bare `.unwrap()`/`.expect()` in production
  code (test code may use them freely).
- **Zero behavioral impact on existing callers is required for Tasks 1-2.**
  `Strategy::current_params`/`Strategy::set_param` are default-implemented
  (return an empty map / a no-op `Ok(())` respectively) — no existing
  `Strategy` implementation anywhere in the workspace needs to change to
  keep compiling and behaving identically. `UnifiedLoopConfig.control:
  Option<ControlHandle>` defaults to `None` at every existing call site
  (`run_backtest`/`run_paper_trading`/`run_live_trading`'s own internal
  construction of `UnifiedLoopConfig` in Tasks 4/5 of the prior
  trading-loop-unification plan) — `None` must be a completely silent
  no-op, matching this project's established `risk_checker`/
  `market_condition_config`-style "absent = today's exact behavior"
  convention. Every pre-existing test in `apexes-strategy`/
  `apexes-backtest`/`apexes-paper`/`apexes-live-trading` must still pass
  UNCHANGED after Tasks 1-2.
- **Credentials never cross a process boundary over the network.**
  `LaunchConfig.credentials_ref` (Task 6) is a name/key only.
  `apexes-engine`'s own `main.rs` (Task 7) is the ONLY place that reads
  the actual secret (env var or local config file) and constructs
  `BinanceGatewayConfig`. `apexes-gateway-binance`'s own
  "never reads `std::env`" convention is NOT changed by this plan —
  only `apexes-engine`, a new caller, reads env/config-file secrets.
- **`apexes-storage`'s existing `StorageReader`/`StorageWriter` APIs are
  reused exactly as they exist today — no new methods, no schema
  changes.** `StorageWriter::write_batch` requires a homogeneous
  `MarketEvent` batch (rejects mixed variants with
  `StorageError::MixedBatch`) — the gateway's persistence buffer (Task
  4) MUST group by variant before calling it, not attempt to pass a
  mixed batch. `StorageReader::read_batch` takes `(symbol, venue,
  date_range: Range<NaiveDate>, event_type)` — day-granularity, not
  arbitrary-timestamp — the gateway's replay logic (Task 4) must convert
  a `replay_from: DateTime<Utc>` into the correct `NaiveDate` range and
  issue one `read_batch` call per subscribed `(symbol, event_type)`
  pair, merging results by timestamp before replaying them.
- **The new `apexes-market-data-gateway` does NOT reuse
  `apexes-message-bus::MarketEventBus`.** `MarketEventBus` is an
  in-process `tokio::sync::broadcast` wrapper (single process only, by
  design — see its own crate docs). The gateway needs a genuinely
  cross-process broadcast (WebSocket-based), which is a different
  mechanism, not an extension of `MarketEventBus`. `apexes-ui`'s
  existing `/ws` endpoint (13a) is NOT removed or changed by this plan —
  it continues to exist, but is re-pointed (Task 9) to subscribe to the
  gateway's `/market` endpoint instead of constructing its own
  `BinanceMarketDataProvider`/`MarketEventBus` directly, so `apexes-ui`
  is never the sole holder of the real upstream Binance connection after
  this plan.
- **`Strategy` trait's `set_param`/`current_params` operate on
  `Decimal` values only** (matching the existing Drift Script `param()`
  binding's own `Decimal`-only constraint) — no string/bool/enum
  parameter types in this plan's scope.
- **The `param()` Drift Script binding's hot-reload semantics are a
  real, deliberate authoring-convention change, not a bug to hide:** a
  script's TOP-LEVEL `param(...)` call (today's only usage pattern,
  exercised once at script-load time) continues to work exactly as
  before and does NOT support hot reload. Hot reload only works for a
  `param(...)` call made INSIDE `on_market_event`'s body (re-evaluated
  every tick). Both patterns must keep working; Task 1's tests must
  cover both, asserting the top-level pattern's value never changes
  after a `set_param` call and the inside-`on_market_event` pattern's
  value does.
- Every commit signed: `git commit -S -s`.
- Read the design spec in full before starting any task — it documents
  the full rationale for every architectural choice (why multi-process
  instead of in-process, why `ControlHandle` is built directly into
  `UnifiedLoopConfig` instead of a fourth pluggable trait, why the
  gateway doesn't reuse `MarketEventBus`, the exact pause semantics)
  and the two SVG communication-model diagrams
  (`docs/superpowers/specs/assets/2026-08-05-apexes-13c-communication-model-v2-multiprocess.svg`
  is the CURRENT architecture; the `-v1-inprocess-superseded.svg` file
  is kept only as a historical record of the rejected first design —
  do not implement against v1).

---

### Task 1: `apexes-strategy`: `Strategy` trait extension (`current_params`/`set_param`) + `DriftScriptStrategy` hot-reload support

**Files:**
- Modify: `crates/apexes-strategy/src/strategy.rs` (new default trait methods)
- Modify: `crates/apexes-strategy/src/drift_dsl/params.rs` (mutable shared param state, replacing the current immutable `overrides` map)
- Modify: `crates/apexes-strategy/src/drift_dsl/strategy.rs` (`DriftScriptStrategy` implements the two new trait methods)
- Modify: `crates/apexes-strategy/src/error.rs` (confirm `StrategyError` has a variant suitable for "unknown parameter name" — add one if not, e.g. `UnknownParam { name: String }`)
- Create: a small new `.drift` test fixture demonstrating the inside-`on_market_event` hot-reloadable pattern (alongside existing fixtures in `crates/apexes-strategy/tests/fixtures/`)

**Interfaces:**
- Consumes: `drift_script::{Interpreter, DriftValue}` (existing).
- Produces: `apexes_strategy::Strategy::{current_params, set_param}` (new default trait methods), `DriftScriptStrategy`'s concrete implementations of both.

- [ ] **Step 1: Add the two new default methods to `Strategy`**

```rust
// crates/apexes-strategy/src/strategy.rs -- add inside `pub trait Strategy: Send { ... }`

/// Reads all currently-tunable parameter values, for monitoring/
/// snapshot display. Default: no tunable parameters (empty map) --
/// existing `Strategy` implementations need no changes to keep
/// compiling and behaving identically.
fn current_params(&self) -> std::collections::HashMap<String, rust_decimal::Decimal> {
    std::collections::HashMap::new()
}

/// Hot-updates one parameter by name. Default: a no-op that always
/// succeeds -- implementations that don't support tuning don't need
/// to override this. `Err(StrategyError::UnknownParam { name })`
/// signals "this strategy has no parameter by this name" (for a
/// caller, e.g. a UI, to surface as an error rather than silently
/// accepting an update that does nothing).
fn set_param(
    &mut self,
    _name: &str,
    _value: rust_decimal::Decimal,
) -> Result<(), crate::error::StrategyError> {
    Ok(())
}
```

Read `crates/apexes-strategy/src/error.rs` first; if `StrategyError`
has no variant suitable for "unknown parameter name", add one
(`UnknownParam { name: String }`) following that enum's existing
`thiserror` conventions exactly (message format, derive list).

- [ ] **Step 2: Rework `params.rs` for mutable, shared parameter state**

Read the existing file in full first — today's `register_to` takes an
owned, immutable `overrides: HashMap<String, Decimal>` and the `param`
native function only ever reads it. Replace with a shared, mutable
store:

```rust
// crates/apexes-strategy/src/drift_dsl/params.rs -- reworked shape
use std::sync::{Arc, Mutex};

/// Shared, mutable store backing both the `param(name, default)`
/// Drift Script binding AND `DriftScriptStrategy::current_params`/
/// `set_param`. `Mutex` (not `RwLock`) -- per-tick lock hold times are
/// microsecond-scale map lookups/inserts, never held across an
/// `.await`, and `SetParam` writes are rare (operator-driven) relative
/// to `param()`'s own per-tick reads -- a plain `Mutex` avoids
/// `RwLock`'s extra complexity for a workload that doesn't need
/// reader/writer differentiation at this scale, per this project's
/// existing convention (see `MarginMonitor`/`MarketConditionMonitor`'s
/// own `Arc<Mutex<_>>`-then-`split()` pattern for the precedent,
/// though this case needs no `split()`
/// since there's only one consumer shape here, not several trait
/// adapters).
pub struct ParamStore {
    values: Mutex<HashMap<String, Decimal>>,
}

impl ParamStore {
    pub fn new(initial_overrides: HashMap<String, Decimal>) -> Self { .. }
    pub fn get_or_default(&self, name: &str, default: Decimal) -> Decimal { .. }
    pub fn set(&self, name: String, value: Decimal) { .. }
    pub fn snapshot(&self) -> HashMap<String, Decimal> { .. }
}

/// Installs the `param` global function into `interp`'s environment,
/// reading from `store` on every call (unchanged from before: a
/// script's TOP-LEVEL `param(...)` call reads the store's value AT
/// THAT MOMENT ONLY, since top-level statements execute exactly once
/// at script-load time -- this is what makes the top-level pattern
/// NOT hot-reloadable, a real and deliberate authoring-convention
/// distinction from calling `param(...)` inside `on_market_event`,
/// which re-reads the store on every tick).
pub fn register_to(interp: &Interpreter, store: Arc<ParamStore>) { .. }
```

`from_file_inner` (in `strategy.rs`, Step 3 below) now constructs one
`Arc<ParamStore>` per `DriftScriptStrategy` instance and keeps a clone
of it alongside `interpreter`/`on_market_event_fn` in the struct, so
`current_params`/`set_param` can read/write it directly without going
through the interpreter at all.

- [ ] **Step 3: Wire `ParamStore` into `DriftScriptStrategy`**

```rust
// crates/apexes-strategy/src/drift_dsl/strategy.rs
pub struct DriftScriptStrategy {
    interpreter: Interpreter,
    on_market_event_fn: DriftValue,
    params: Arc<ParamStore>,   // NEW
}

impl Strategy for DriftScriptStrategy {
    // existing on_market_event unchanged

    fn current_params(&self) -> HashMap<String, Decimal> {
        self.params.snapshot()
    }

    fn set_param(&mut self, name: &str, value: Decimal) -> Result<(), StrategyError> {
        // A script that never calls `param(name, ...)` for this exact
        // name has no way to know this name exists -- accept the
        // write anyway (store it), since a FUTURE param(name, ...)
        // call (e.g. the script gets edited and relaunched, or the
        // name is simply not yet reached in execution order) would
        // then see it. Returning UnknownParam here would require
        // tracking "names this script has ever declared", which
        // `ParamStore` intentionally does not do (see design spec
        // 5.3's default-implementation rationale: keep this simple).
        self.params.set(name.to_string(), value);
        Ok(())
    }
}
```

- [ ] **Step 4: New `.drift` fixture + tests**

Add `crates/apexes-strategy/tests/fixtures/hot_reloadable_param.drift`:
a minimal script whose `on_market_event` calls
`param("threshold", 0.2)` INSIDE the function body (not at top level),
using the result to decide whether to return a buy intent — so a test
can assert the decision changes after a `set_param` call.

New tests in `crates/apexes-strategy/src/drift_dsl/params.rs` and/or a
new integration test file:
- `top_level_param_call_does_not_hot_reload`: load a script with a
  top-level `let x = param("x", 1);`, call `set_param("x", 99)`,
  re-run `on_market_event`, assert behavior derived from `x` is
  UNCHANGED (still reflects `1`, not `99`) — proving the documented
  non-hot-reloadable behavior is real, not accidentally "fixed".
- `inside_on_market_event_param_call_does_hot_reload`: load
  `hot_reloadable_param.drift`, call `on_market_event` once (observe
  baseline behavior), call `set_param("threshold", <new value>)`, call
  `on_market_event` again, assert behavior changed.
- `current_params_reflects_last_set_value`: `set_param` a value, assert
  `current_params()` returns it.
- `set_param_on_unknown_name_still_succeeds` (per Step 3's design):
  assert `set_param` on a name the script never declared still returns
  `Ok(())` and is later visible via `current_params`.
- Default-trait-method regression test (in `strategy.rs`'s own test
  module or a new one): construct any existing test `Strategy`
  implementation that does NOT override `current_params`/`set_param`,
  assert `current_params()` returns an empty map and `set_param(..)`
  returns `Ok(())` — proving the defaults work for a caller that
  hasn't opted in.

- [ ] **Step 5: Verify**

Run: `cargo test -p apexes-strategy -- --test-threads=1`
Expected: PASS, every pre-existing test still passes unchanged, plus
all new tests from Step 4.

Run: `cargo build --workspace` (confirms no other crate's `Strategy`
implementations broke from the new trait methods — they shouldn't,
since both are default-implemented)
Expected: 0 errors.

Run: `cargo clippy -p apexes-strategy --all-targets -- -D warnings`
Expected: 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-strategy
git commit -S -s -m "feat(apexes-strategy): Strategy::current_params/set_param + DriftScriptStrategy hot-reloadable param() support"
```

---

### Task 2: `apexes-backtest`: `ControlHandle`/`SessionSnapshot`/`SessionCommand`, wired into `UnifiedLoopConfig`

**Files:**
- Create: `crates/apexes-backtest/src/control.rs`
- Modify: `crates/apexes-backtest/src/unified_loop.rs` (`UnifiedLoopConfig.control` field, per-tick handling)
- Modify: `crates/apexes-backtest/src/lib.rs` (module wiring, re-export)

**Interfaces:**
- Consumes: `apexes_strategy::Strategy::{current_params, set_param}` (Task 1), existing `PortfolioLedger`/`StrategyContext` types.
- Produces: `apexes_backtest::control::{ControlHandle, SessionSnapshot, SessionCommand}`.

- [ ] **Step 1: Write `control.rs`**

```rust
// crates/apexes-backtest/src/control.rs (new file)
//! [`ControlHandle`]: the live-tuning/monitoring control surface built
//! directly into `crate::unified_loop::run_unified_event_loop` (NOT a
//! fourth pluggable `apexes_strategy` trait -- see design spec
//! `2026-08-05-apexes-13c-strategy-monitoring-tuning-design.md` §5.1
//! for why). `None` on `UnifiedLoopConfig.control` is a silent no-op,
//! matching every other optional feature this loop supports.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use apexes_strategy::{Position, PositionMarginStatus};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tokio::sync::{mpsc, watch};

#[derive(Debug, Clone)]
pub enum SessionCommand {
    SetParam { name: String, value: Decimal },
    Pause,
    Resume,
    Stop,
}

#[derive(Debug, Clone)]
pub struct SessionSnapshot {
    pub positions: HashMap<String, Position>,
    pub balances: HashMap<String, Decimal>,
    pub equity: Decimal,
    pub realized_pnl: HashMap<String, Decimal>,
    pub margin_status: HashMap<String, PositionMarginStatus>,
    pub params: HashMap<String, Decimal>,
    pub paused: bool,
    pub timestamp: DateTime<Utc>,
}

/// Constructed by the caller (`apexes-engine`, Task 7) BEFORE calling
/// `run_unified_event_loop` -- the caller keeps the `mpsc::UnboundedSender<SessionCommand>`
/// and `watch::Receiver<SessionSnapshot>` halves for itself (e.g. to
/// wire into its own `axum` WS handler), passing this struct (which
/// holds the OTHER halves) into `UnifiedLoopConfig.control`.
pub struct ControlHandle {
    snapshot_tx: watch::Sender<SessionSnapshot>,
    command_rx: mpsc::UnboundedReceiver<SessionCommand>,
    min_interval: Duration,
    last_pushed: Option<Instant>,
    paused: bool,
}

impl ControlHandle {
    pub fn new(
        min_interval: Duration,
        initial_snapshot: SessionSnapshot,
    ) -> (Self, mpsc::UnboundedSender<SessionCommand>, watch::Receiver<SessionSnapshot>) {
        ..
    }

    /// Called once per tick by the unified loop (Step 2). Drains ALL
    /// pending commands (not just one -- a tick may have several
    /// queued), applying `SetParam` to `strategy` immediately (logging
    /// `tracing::warn!` on an `Err` from `Strategy::set_param`, never
    /// propagating it as a loop error -- an unknown param name is an
    /// operator mistake, not a system failure), and returns whether a
    /// `Stop` command was seen (the loop's caller checks this
    /// alongside its existing `stop_signal` check).
    pub(crate) fn drain_commands(&mut self, strategy: &mut dyn apexes_strategy::Strategy) -> DrainResult { .. }

    pub(crate) fn is_paused(&self) -> bool { self.paused }

    /// Pushes `snapshot` via the `watch` channel IF `min_interval` has
    /// elapsed since the last push (silently skips otherwise -- no
    /// queuing, `watch`'s own "only the latest value matters" semantics
    /// do the throttling naturally once this elapsed-time gate is
    /// applied at the call site).
    pub(crate) fn maybe_push_snapshot(&mut self, snapshot: SessionSnapshot) { .. }
}

pub(crate) struct DrainResult {
    pub stop_requested: bool,
}
```

- [ ] **Step 2: Wire into `UnifiedLoopConfig`/`run_unified_event_loop`**

Add `pub control: Option<ControlHandle>` to `UnifiedLoopConfig` (default
`None` at every existing caller — Task 4/5's `run_backtest`/
`run_paper_trading`/`run_live_trading` internal construction sites are
NOT modified by this task; they simply don't set this new field, which
compiles fine since it's the last-added field and Rust struct literals
require every field to be named — this means those three functions'
OWN construction of `UnifiedLoopConfig` DOES need a one-line addition
of `control: None` each; this is a mechanical, zero-behavior-change
edit, not a design change, and must NOT alter those functions' own
public `BacktestConfig`/`PaperTradingConfig`/`LiveTradingConfig` types
or any other line).

Inside `run_unified_event_loop`'s market-event arm, immediately after
the existing equity-point recording (end of that arm's body):

```rust
if let Some(control) = &mut config.control {
    let drain_result = control.drain_commands(&mut strategy);
    if drain_result.stop_requested {
        break; // exits the loop on this iteration, same as an
               // exhausted stop_signal -- returns the report
               // accumulated so far, not an error
    }
}
```

And BEFORE the `strategy.on_market_event(&event, &ctx)` call (the
existing per-intent loop), gate it on pause state:

```rust
let intents = if config.control.as_ref().is_some_and(|c| c.is_paused()) {
    Vec::new() // strategy not called at all -- no new intents this tick
} else {
    strategy.on_market_event(&event, &ctx)
};
```

Confirm this placement does NOT skip the funding-settlement/margin-status/
liquidation/de-risking steps that come BEFORE this point in the existing
per-tick sequence — those must run regardless of pause state (per the
design spec's exact pause semantics: "positions/PnL/margin keep
updating, only `on_market_event` is skipped").

After the per-intent loop (or at the same point as the existing
equity-point recording), if `config.control.is_some()`: construct a
`SessionSnapshot` from the current `ledger`/`margin_status`/
`strategy.current_params()`/pause state/`timestamp`, call
`maybe_push_snapshot`. Note: `PortfolioLedger::positions()`/
`balances()`/`realized_pnl()` all return `&HashMap<...>` (borrowed, not
owned) — `SessionSnapshot`'s fields are owned `HashMap`s (needed since
the `watch` channel sends by value across a task/thread boundary), so
this construction step requires `.clone()` on each of the three; this
is a real per-tick allocation cost, deemed acceptable given the
`min_interval` throttle already means this only happens at most once
per `min_interval` (default 500ms), not once per tick even on a
high-frequency feed — the throttle check (Step in `ControlHandle`)
should happen BEFORE constructing the `SessionSnapshot`, not after, to
avoid needlessly cloning data for a snapshot that will just be thrown
away by the throttle.

- [ ] **Step 3: `lib.rs` wiring**

Add `pub mod control;` and `pub use control::{ControlHandle, SessionCommand, SessionSnapshot};` to `crates/apexes-backtest/src/lib.rs`, matching this crate's existing `pub mod`/`pub use` grouping style (read the file first to place these consistently).

- [ ] **Step 4: Unit tests, isolated**

In `#[cfg(test)] mod tests` within `control.rs`:
- `min_interval_throttles_snapshot_pushes`: construct a `ControlHandle`
  with a long `min_interval`, call `maybe_push_snapshot` twice in quick
  succession, assert the `watch::Receiver` only observes the first
  value (the second was throttled away).
- `drain_commands_applies_set_param_to_strategy`: a fake `Strategy`
  recording `set_param` calls, send a `SetParam` command, call
  `drain_commands`, assert the fake strategy received it.
- `drain_commands_drains_multiple_queued_commands_in_one_call`: send 3
  commands before calling `drain_commands` once, assert all 3 were
  applied (not just the first).
- `pause_then_resume_toggles_is_paused`.
- `stop_command_sets_stop_requested`.

In `crates/apexes-backtest/src/unified_loop.rs`'s existing
`#[cfg(test)] mod tests`, new integration-style tests using a real
short-lived backtest run (mirroring this module's existing test
style/fixtures, per Task 4/5's own precedent in the prior plan):
- `paused_session_stops_producing_new_intents_but_keeps_updating_equity`:
  run a few ticks unpaused (strategy submits orders, equity changes),
  send `Pause`, run more ticks, assert NO new trades appear in the
  report's `trades` for the paused period but `equity_curve` still has
  new points recorded (from funding/mark-price movement, not from new
  fills) if the test fixture's data supports observing that
  distinction — otherwise assert at minimum that no new orders were
  submitted.
- `set_param_command_changes_strategy_behavior_next_tick`: a test
  `Strategy` whose `on_market_event` reads its own `current_params`-
  visible internal threshold; send `SetParam`, assert the NEXT tick's
  intent reflects the new value.
- `control_none_behaves_identically_to_before_this_task`: run the exact
  same short backtest with `control: None` both before and after this
  task's changes (or, since "before" no longer exists as separate code,
  assert this against a fixed expected numeric output the same way
  Task 7 of the prior plan's whole-branch review validated
  determinism) — confirming zero behavioral drift for the
  overwhelmingly common `control: None` case.

- [ ] **Step 5: Verify**

Run: `cargo test -p apexes-backtest -- --test-threads=1`
Expected: PASS, every pre-existing test (128+ from the prior plan's
final state) still passes UNCHANGED, plus all new tests.

Run: `cargo build --workspace`
Expected: 0 errors (confirms `run_paper_trading`/`run_live_trading`'s
own `UnifiedLoopConfig` construction sites compile with the new
`control: None` field added).

Run: `cargo clippy -p apexes-backtest --all-targets -- -D warnings`
Expected: 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-backtest
git commit -S -s -m "feat(apexes-backtest): ControlHandle/SessionSnapshot/SessionCommand wired into the unified event loop"
```

---

### Task 3: New workspace dependency `core_affinity`; `apexes-market-data-gateway` crate scaffold + subscriber-broadcast core

**Files:**
- Modify: `Cargo.toml` (workspace `members` + new `[workspace.dependencies]` entry for `core_affinity`)
- Create: `crates/apexes-market-data-gateway/Cargo.toml`
- Create: `crates/apexes-market-data-gateway/src/lib.rs`
- Create: `crates/apexes-market-data-gateway/src/protocol.rs`
- Create: `crates/apexes-market-data-gateway/src/gateway.rs`

**Interfaces:**
- Consumes: `apexes_core::{MarketEvent, Instrument, MarketDataProvider}` (existing), `apexes_market_data::BinanceMarketDataProvider` (existing).
- Produces: `apexes_market_data_gateway::protocol::{..}` (WS message shapes), `apexes_market_data_gateway::gateway::{Gateway, GatewayHandle}` (subscriber registry, broadcast core — no persistence/replay yet, that's Task 4; no `main.rs`/CLI/axum wiring yet, that's Task 5).

- [ ] **Step 1: Add the new crate to the workspace + `core_affinity` dependency**

```toml
# Cargo.toml -- append "crates/apexes-market-data-gateway" to workspace
# members (keep every existing entry exactly as-is), and add to
# [workspace.dependencies]:
core_affinity = "0.8"
```

Confirm the exact current `core_affinity` version on crates.io before
pinning (this plan's `"0.8"` is a placeholder — use whatever the latest
stable published version is at implementation time).

- [ ] **Step 2: Write `protocol.rs`**

The gateway's OWN wire protocol (distinct from `apexes-engine`'s
`/session/ws` protocol, Task 8 — these are two different endpoints on
two different processes):

```rust
// crates/apexes-market-data-gateway/src/protocol.rs
//! Wire protocol for the gateway's `/market` WebSocket endpoint.
//! Frames are `serde_json`-serialized `apexes_core::MarketEvent`
//! values directly (matching `apexes-ui`'s existing `/ws` 13a
//! convention) -- no new envelope type needed here (unlike
//! `apexes-engine`'s `/session/ws`, which genuinely needs one to
//! distinguish control vs. snapshot messages, see Task 8).
```

(This file may end up being very small or folded into `gateway.rs`
directly if a separate file isn't warranted once written — implementer's
call, but keep the module boundary conceptually clear per this crate's
own doc comments either way.)

- [ ] **Step 3: Write `gateway.rs`'s subscriber registry**

```rust
// crates/apexes-market-data-gateway/src/gateway.rs
//! [`Gateway`]: holds ONE real upstream `MarketDataProvider` connection
//! and broadcasts every event to N connected WebSocket clients over
//! `/market`. Deliberately NOT built on `apexes_message_bus::MarketEventBus`
//! (an in-process-only broadcast wrapper) -- see this crate's own
//! `lib.rs` docs and the design spec for the full rationale.

use apexes_core::{Instrument, MarketDataProvider, MarketEvent};
use tokio::sync::broadcast;

pub struct Gateway {
    provider: Box<dyn MarketDataProvider>,
    sender: broadcast::Sender<MarketEvent>,
}

impl Gateway {
    pub fn new(provider: Box<dyn MarketDataProvider>, capacity: usize) -> Self { .. }

    /// Subscribes to `instruments` from the real upstream provider ONCE
    /// (called exactly once, at process startup by `main.rs`, Task 5) --
    /// this crate does not support subscribing to a second, different
    /// instrument set later in the same process lifetime (matching
    /// `MarketEventBus`'s own fixed-instrument-set-per-connection
    /// constraint, for the same underlying reason: one upstream
    /// connection, one fixed subscription).
    pub async fn connect(&self, instruments: &[Instrument]) -> Result<(), apexes_core::ApexesError> { .. }

    /// Returns a fresh `broadcast::Receiver` for a newly-connecting WS
    /// client (Task 5's `axum` handler calls this per connection).
    pub fn subscribe(&self) -> broadcast::Receiver<MarketEvent> { .. }
}
```

`connect`'s internal loop (consuming the provider's own event stream and
`self.sender.send(event)`-ing each one, logging+continuing on a
`Lagged`-equivalent situation) mirrors `apexes_message_bus::MarketEventBus`'s
existing internal forwarding-task pattern closely enough to read that
crate's `bus.rs` first as a structural reference, even though this crate
does not depend on or wrap it directly.

- [ ] **Step 4: Unit tests, isolated (no real network, no axum yet)**

Using a fake `MarketDataProvider` (mirror
`apexes_message_bus::test_support::MockMarketDataProvider`'s existing
shape/pattern, or reuse it directly if `apexes-market-data-gateway` can
depend on `apexes-message-bus` just for that test-support module —
implementer's call, check `apexes-message-bus`'s `test_support` module's
current visibility/feature-gating before deciding):
- `subscribe_returns_a_receiver_that_sees_broadcast_events`.
- `multiple_subscribers_all_see_the_same_events` (the core "shared
  connection, multiple downstream consumers" property this whole crate
  exists to provide).
- `connect_can_only_be_called_once_per_instrument_set` (or whatever the
  implementer's actual chosen constraint enforcement looks like — at
  minimum, document and test the current behavior, whatever it ends up
  being, rather than leaving it unspecified).

- [ ] **Step 5: Verify**

Run: `cargo test -p apexes-market-data-gateway -- --test-threads=1`
Expected: PASS.

Run: `cargo build --workspace && cargo clippy -p apexes-market-data-gateway --all-targets -- -D warnings`
Expected: 0 errors, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/apexes-market-data-gateway
git commit -S -s -m "feat(apexes-market-data-gateway): crate scaffold + subscriber-broadcast core (row 8.3 min-viable, part 1)"
```

---

### Task 4: `apexes-market-data-gateway`: replay + async persistence (reusing `apexes-storage` verbatim)

**Files:**
- Create: `crates/apexes-market-data-gateway/src/replay.rs`
- Create: `crates/apexes-market-data-gateway/src/persistence.rs`
- Modify: `crates/apexes-market-data-gateway/Cargo.toml` (add `apexes-storage` dependency)
- Modify: `crates/apexes-market-data-gateway/src/gateway.rs` (wire persistence into the broadcast loop)

**Interfaces:**
- Consumes: `apexes_storage::{StorageReader, StorageWriter, reader::EventType}` (existing, unmodified).
- Produces: `apexes_market_data_gateway::{replay::replay_since, persistence::PersistenceBuffer}`.

- [ ] **Step 1: Write `replay.rs`**

```rust
// crates/apexes-market-data-gateway/src/replay.rs
//! Converts a client-supplied `replay_from: DateTime<Utc>` into the
//! day-granularity `apexes_storage::StorageReader::read_batch` calls
//! needed to reconstruct history for a fixed instrument set, merged
//! by timestamp. `read_batch` is per-`(symbol, venue, event_type)` --
//! this module issues one call per combination and merges the results,
//! since `apexes-storage` has no single "everything since timestamp X"
//! query (confirmed against its actual current API before writing this
//! -- do not assume a more convenient query shape exists).

use apexes_core::{Instrument, MarketEvent};
use apexes_storage::StorageReader;
use apexes_storage::reader::EventType;
use chrono::{DateTime, Utc};

/// Which `EventType`s to query for replay -- matches whatever
/// `MarketEvent` variants the gateway actually forwards over `/market`
/// (Trade/Tick at minimum, matching 13a's existing filtering
/// convention; confirm against Task 3's actual forwarding logic before
/// finalizing this list -- do not silently diverge).
pub async fn replay_since(
    reader: &StorageReader,
    instruments: &[Instrument],
    replay_from: DateTime<Utc>,
) -> Result<Vec<MarketEvent>, apexes_storage::error::StorageError> {
    // For each instrument, for each relevant EventType: convert
    // replay_from into a NaiveDate range (replay_from's own date
    // through today's date, inclusive), call read_batch, collect.
    // Merge all results by event timestamp before returning.
    ..
}
```

- [ ] **Step 2: Write `persistence.rs`**

```rust
// crates/apexes-market-data-gateway/src/persistence.rs
//! Buffers incoming `MarketEvent`s GROUPED BY VARIANT (required --
//! `StorageWriter::write_batch` rejects a mixed-variant slice with
//! `StorageError::MixedBatch`) and flushes each variant's buffer to
//! ClickHouse when it reaches a size threshold OR a time threshold,
//! whichever comes first. A flush failure is logged via
//! `tracing::error!` and the buffer is dropped (not retried, not
//! blocking the real-time forwarding path) -- monitoring availability
//! takes priority over historical completeness, matching
//! `apexes-message-bus`'s own established "log and continue, never
//! block the hot path" convention.

use apexes_core::MarketEvent;
use apexes_storage::StorageWriter;
use std::time::Duration;

pub struct PersistenceBuffer {
    writer: StorageWriter,
    // one Vec<MarketEvent> per variant, or an equivalent
    // discriminant-keyed structure -- implementer's choice of exact
    // shape, but MUST guarantee every write_batch call is homogeneous
}

impl PersistenceBuffer {
    pub fn new(writer: StorageWriter, max_batch_size: usize, max_batch_age: Duration) -> Self { .. }

    /// Called for every event the gateway forwards. Buffers it under
    /// the correct per-variant bucket; flushes that bucket if it just
    /// crossed `max_batch_size`. Does NOT block the caller on a
    /// network write for the common case (spawns the flush as its own
    /// task, or the caller is already inside an async context where
    /// this is naturally non-blocking -- implementer's call on the
    /// exact mechanism, but the real-time broadcast path per Task 3
    /// must never wait on a ClickHouse round-trip).
    pub async fn record(&mut self, event: &MarketEvent) { .. }

    /// Time-based flush, called periodically (e.g. from a
    /// `tokio::time::interval` loop in `main.rs`, Task 5) for buckets
    /// that have events older than `max_batch_age` but haven't hit
    /// `max_batch_size` yet.
    pub async fn flush_aged(&mut self) { .. }
}
```

- [ ] **Step 3: Wire into `gateway.rs`'s broadcast loop**

The `connect` method's internal forwarding loop (Task 3) now also calls
`persistence_buffer.record(&event)` for each event, alongside the
existing `self.sender.send(event)` broadcast — confirm this doesn't
introduce backpressure that could slow down real-time forwarding (per
Step 2's non-blocking requirement).

- [ ] **Step 4: Tests**

- `replay.rs`: unit test converting a `replay_from` timestamp into the
  correct `NaiveDate` range (boundary cases: exactly midnight UTC,
  a time later in the day), using a fake/seeded `StorageReader` if
  `apexes-storage` has test-support for that, or a real ClickHouse
  instance gated `#[ignore]` (mirroring this workspace's existing
  convention for storage-backed tests) if no lighter-weight fixture
  exists — check `apexes-storage`'s own test suite for its established
  pattern first.
- `persistence.rs`: unit test confirming events are grouped by variant
  before any `write_batch` call is made (a fake `StorageWriter`-like
  seam, or a real ClickHouse `#[ignore]`d test asserting no
  `MixedBatch` error ever occurs even when fed an interleaved sequence
  of Trade/Tick/MarkPrice events) — check both the size-triggered and
  age-triggered flush paths.

- [ ] **Step 5: Verify**

Run: `cargo test -p apexes-market-data-gateway -- --test-threads=1`
Expected: PASS (non-`#[ignore]`d tests). If a live ClickHouse instance
is available in this environment (confirm via `docker ps`, per this
project's established convention of checking rather than assuming),
also run: `cargo test -p apexes-market-data-gateway -- --ignored --test-threads=1`

Run: `cargo build --workspace && cargo clippy -p apexes-market-data-gateway --all-targets -- -D warnings`
Expected: 0 errors, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-market-data-gateway
git commit -S -s -m "feat(apexes-market-data-gateway): replay + async persistence via apexes-storage (row 8.3 min-viable, part 2)"
```

---

### Task 5: `apexes-market-data-gateway`: `main.rs` (CLI, CPU affinity, PID file, `axum` `/market` endpoint)

**Files:**
- Create: `crates/apexes-market-data-gateway/src/main.rs`
- Create: `crates/apexes-market-data-gateway/src/server.rs`
- Create: `crates/apexes-market-data-gateway/tests/replay_integration.rs`

**Interfaces:**
- Consumes: Task 3/4's `Gateway`/`PersistenceBuffer`/`replay_since`.
- Produces: the `apexes-market-data-gateway` binary; `axum` route `GET /market` (optional `?replay_from=<RFC3339>` query param), a PID file at a documented path.

- [ ] **Step 1: Write `server.rs`**

```rust
// crates/apexes-market-data-gateway/src/server.rs
//! `axum` app exposing `GET /market` -- upgrades to a WebSocket,
//! optionally replays history first (if `?replay_from=` is present,
//! via `crate::replay::replay_since`), then forwards live events from
//! `Gateway::subscribe()` as JSON text frames, matching
//! `apexes-ui`'s existing `/ws` (13a) frame format exactly (bare
//! `serde_json::to_string(&event)`, no envelope).

pub fn build_app(gateway: Arc<Gateway>, storage_reader: Option<Arc<StorageReader>>) -> Router { .. }
```

Parameterized over an already-constructed `Arc<Gateway>` (and an
optional `StorageReader` for replay — `None` disables replay entirely,
a client requesting it gets a graceful error/close rather than a panic),
mirroring `apexes-ui`'s own Task 2 (13a) precedent of a testable
app-builder function separate from `main.rs`'s real-provider
construction.

- [ ] **Step 2: Write `main.rs`**

```rust
// crates/apexes-market-data-gateway/src/main.rs
#[derive(Parser)]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long, value_delimiter = ',')]
    cpu_cores: Option<Vec<usize>>,
    #[arg(long)]
    clickhouse_url: Option<String>,   // None disables persistence/replay
    #[arg(long, value_delimiter = ',')]
    instruments: Vec<String>,          // symbol list, Binance Spot assumed for this crate's current scope
}
```

Startup sequence: (1) if `cpu_cores` provided, call `core_affinity`'s
pinning API for the current process, logging `tracing::warn!` (not
aborting) on failure per this plan's Global Constraints; (2) construct
`Gateway` wrapping a real `BinanceMarketDataProvider`, call `connect`
once for the configured instruments, exiting the process with a clear
error message on failure (mirroring `apexes-ui`'s own Task 5 (13a)
"bind/subscribe failure exits at startup" precedent); (3) if
`clickhouse_url` provided, construct `StorageWriter`/`StorageReader`
and wire the `PersistenceBuffer` in; (4) write the PID file (path:
`~/.apexes/market-data-gateway.pid`, creating the `~/.apexes/`
directory if it doesn't exist) containing this process's PID and the
bound port (two lines, or a small JSON object — implementer's choice,
but `apexes-ui`'s Task 9 supervisor needs to parse whatever format is
chosen, so document it clearly in this file's own doc comment as the
authoritative format); (5) bind and serve.

- [ ] **Step 3: Integration test**

`tests/replay_integration.rs`: real `127.0.0.1:0` bind, a fake
`MarketDataProvider` (no real Binance connection, per this workspace's
established testing convention), asserts a connecting WS client
receives forwarded live events; if a live ClickHouse instance is
available, a `#[ignore]`d test also asserting `?replay_from=` correctly
prepends historical events before switching to live forwarding (seed
ClickHouse via `apexes-storage::StorageWriter` directly in the test
setup, matching `apexes-storage`'s own test conventions).

- [ ] **Step 4: Verify**

Run: `cargo test -p apexes-market-data-gateway -- --test-threads=1`
Expected: PASS.

Run: `cargo build --workspace && cargo clippy -p apexes-market-data-gateway --all-targets -- -D warnings`
Expected: 0 errors, 0 warnings.

Manual smoke check (not an automated test, but confirm before
committing): `cargo run -p apexes-market-data-gateway -- --instruments BTCUSDT` starts, binds, writes a PID file — confirm the file's format is what Task 9 will expect to parse (cross-reference this task's chosen format with Task 9's brief before finalizing; if Task 9 hasn't been task-briefed yet at this point in execution, document the format clearly in this crate's own `main.rs` doc comment so Task 9's implementer can read it directly rather than guessing).

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-market-data-gateway
git commit -S -s -m "feat(apexes-market-data-gateway): main.rs -- CLI, CPU affinity, PID file, axum /market endpoint (row 8.3 min-viable, complete)"
```

---

### Task 6: `apexes-engine` crate scaffold: `LaunchConfig` + `strategy_loader.rs`

**Files:**
- Modify: `Cargo.toml` (workspace `members`)
- Create: `crates/apexes-engine/Cargo.toml`
- Create: `crates/apexes-engine/src/lib.rs`
- Create: `crates/apexes-engine/src/launch_config.rs`
- Create: `crates/apexes-engine/src/strategy_loader.rs`

**Interfaces:**
- Consumes: `apexes_strategy::{Strategy, drift_dsl::DriftScriptStrategy}` (existing + Task 1's extensions).
- Produces: `apexes_engine::launch_config::{LaunchConfig, SessionMode, StrategyRef}`, `apexes_engine::strategy_loader::{load_strategy, list_available_strategies}`.

- [ ] **Step 1: Add the new crate to the workspace**

```toml
# Cargo.toml -- append "crates/apexes-engine" to workspace members
```

- [ ] **Step 2: Write `launch_config.rs`**

```rust
// crates/apexes-engine/src/launch_config.rs
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SessionMode {
    Backtest { date_range: (NaiveDate, NaiveDate) },
    Paper,
    Live { credentials_ref: String, auto_derisk_live: bool, reconcile_interval: Duration },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum StrategyRef {
    DriftScript { path: PathBuf },
    RustNative { name: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LaunchConfig {
    pub mode: SessionMode,
    pub strategy_ref: StrategyRef,
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub cpu_cores: Option<Vec<usize>>,
    pub snapshot_min_interval: Duration,  // default 500ms, see serde default attr
    pub margin_tiers: HashMap<String, MarginTierTable>,
    pub market_condition_config: Option<MarketConditionConfig>,
    pub risk_free_return: Decimal,
    pub gateway_url: String,  // apexes-market-data-gateway's /market WS URL this engine subscribes to
}
```

Confirm every field name/type against `BacktestConfig`/
`PaperTradingConfig`/`LiveTradingConfig`'s ACTUAL current field lists
(read all three structs directly from source before finalizing this
shape — do not trust this brief's paraphrase, which may drift from the
real structs by the time this task executes) — `LaunchConfig` needs to
carry enough information for Task 7's `main.rs` to construct whichever
of the three config types the selected `SessionMode` needs.

- [ ] **Step 3: Write `strategy_loader.rs`**

```rust
// crates/apexes-engine/src/strategy_loader.rs
//! Loads a `Box<dyn Strategy>` from a `StrategyRef`. The Rust-native
//! table is a COMPILE-TIME FIXED list -- Rust has no runtime
//! reflection, so every selectable Rust-native strategy struct must be
//! manually added here by a developer (documented in this file's own
//! doc comment as the place to edit when adding a new one). This
//! module's `list_available_strategies` logic is ALSO used directly by
//! `apexes-ui` (Task 10) to populate its launcher dropdown WITHOUT
//! going through any running process -- see design spec §6.3's note on
//! why `GET /session/strategies` ended up not needing a server at all.

pub fn list_available_strategies(drift_script_dir: &Path) -> Vec<StrategyRef> {
    // scans drift_script_dir for *.drift files, PLUS the fixed
    // RUST_NATIVE_STRATEGIES table's names
}

pub fn load_strategy(strategy_ref: &StrategyRef) -> Result<Box<dyn Strategy>, EngineError> {
    match strategy_ref {
        StrategyRef::DriftScript { path } => { .. DriftScriptStrategy::from_file(path) .. }
        StrategyRef::RustNative { name } => { .. RUST_NATIVE_STRATEGIES.get(name.as_str()) .. }
    }
}

// The fixed table -- starts EMPTY or with only genuinely-existing
// test/demo Strategy implementations from elsewhere in the workspace
// (check whether any Rust-native (non-Drift-Script) Strategy impl
// outside test code actually exists anywhere in the workspace today;
// if none does, this table legitimately starts empty and this is
// documented, not a gap -- do not invent a placeholder strategy just
// to populate it).
static RUST_NATIVE_STRATEGIES: LazyLock<HashMap<&'static str, fn() -> Box<dyn Strategy>>> = ..;
```

Since `apexes-ui` (Task 10) needs this exact same logic, decide (and
document the decision in this task's commit message) whether
`apexes-ui` depends on `apexes-engine` directly to reuse
`strategy_loader.rs`, or whether the logic is small enough to
duplicate — given `apexes-ui` likely should NOT depend on
`apexes-engine` (a binary crate; `apexes-ui` spawning it as a
subprocess is the whole point, not linking against it), prefer
extracting `strategy_loader.rs`'s logic into a small new shared
library crate (e.g. `apexes-strategy-loader`) that BOTH
`apexes-engine` and `apexes-ui` depend on, rather than either
duplicating it or creating an unwanted binary-to-binary dependency.
If this changes this task's file list (extracting a new crate instead
of `apexes-engine/src/strategy_loader.rs`), that's an approved
deviation — document it in the commit message and update Task 10's
own understanding accordingly when that task starts.

- [ ] **Step 4: Tests**

- `list_available_strategies` returns both `.drift` files from a temp
  directory fixture and the (possibly empty) Rust-native table names.
- `load_strategy` for `DriftScript` succeeds for a real fixture file,
  fails with a clear error for a nonexistent path.
- `load_strategy` for `RustNative` succeeds for a registered name,
  fails with a clear error for an unregistered name.

- [ ] **Step 5: Verify**

Run: `cargo test -p apexes-engine -- --test-threads=1` (or the shared
strategy-loader crate's own name, if Step 3's extraction happened)
Expected: PASS.

Run: `cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 errors, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/apexes-engine  # + the extracted shared crate if Step 3 created one
git commit -S -s -m "feat(apexes-engine): crate scaffold -- LaunchConfig + strategy_loader"
```

---

### Task 7: `apexes-engine`: `main.rs` (mode dispatch, `ControlHandle` wiring, CPU affinity, credentials)

**Files:**
- Create: `crates/apexes-engine/src/main.rs`
- Create: `crates/apexes-engine/src/credentials.rs`
- Modify: `crates/apexes-engine/Cargo.toml` (dependencies on `apexes-backtest`, `apexes-paper`, `apexes-live-trading`, `apexes-gateway-binance`, `apexes-oms`, `core_affinity`, `apexes-message-bus` for `receiver_into_stream` if consuming the gateway's `/market` stream needs it)

**Interfaces:**
- Consumes: Task 2's `ControlHandle`/`SessionSnapshot`/`SessionCommand`, Task 6's `LaunchConfig`/`strategy_loader`, existing `run_backtest`/`run_paper_trading`/`run_live_trading` (all UNCHANGED public signatures).
- Produces: the `apexes-engine` binary.

- [ ] **Step 1: Write `credentials.rs`**

```rust
// crates/apexes-engine/src/credentials.rs
//! The ONLY place in this plan that reads secrets from the environment
//! or a local config file. `apexes-gateway-binance`'s own
//! `BinanceGatewayConfig` never reads `std::env` itself (an explicit,
//! documented design decision in that crate) -- this module is the
//! upstream caller that does, per this plan's Global Constraints.

pub fn resolve_binance_credentials(credentials_ref: &str) -> Result<BinanceGatewayConfig, EngineError> {
    // e.g. reads APEXES_CRED_<UPPERCASED_REF>_API_KEY /
    // APEXES_CRED_<UPPERCASED_REF>_API_SECRET env vars, or a local
    // TOML/JSON config file keyed by credentials_ref -- implementer's
    // choice of exact format, but document it clearly since Task 11's
    // UI launcher tab needs to present a matching set of
    // `credentials_ref` options to the user.
}
```

- [ ] **Step 2: Write `main.rs`**

CLI parsing (`clap`, matching this workspace's established style):
accepts a `--launch-config <path>` pointing at a JSON file containing a
serialized `LaunchConfig` (preferred over a giant flat CLI-arg surface,
per the design spec's own noted preference for "more robust" over
"simpler" — `apexes-ui`, Task 9, writes this file to a temp location
before spawning this process and passes its path).

Startup sequence: (1) parse CLI, read+deserialize `LaunchConfig`; (2)
if `cpu_cores` set, apply affinity (same pattern as Task 5, logging not
aborting on failure); (3) construct a `Registry` and whatever
mode-specific dependencies the selected `SessionMode` needs
(`MatchingEngine`+adapters for `Backtest`/`Paper`, or
`OrderManager`+`BinanceExecutionGateway`+adapters for `Live`, resolving
credentials via Step 1 only for `Live`); (4) connect to the gateway's
`/market` endpoint at `launch_config.gateway_url` (via
`tokio-tungstenite`, deserializing frames as `MarketEvent` — mirror
`apexes-ui`'s existing `client.rs` (13a) parsing logic closely, or
extract a tiny shared helper if that's cleaner, implementer's call);
(5) load the strategy via Task 6's `strategy_loader::load_strategy`;
(6) construct `ControlHandle` (Task 2) plus keep the `mpsc::Sender`/
`watch::Receiver` halves for Task 8's `axum` server; (7) call the
selected `run_backtest`/`run_paper_trading`/`run_live_trading` with
`UnifiedLoopConfig.control: Some(control_handle)`; (8) on completion
(graceful stop or the market stream ending), exit the process cleanly.

- [ ] **Step 3: Tests**

Given `main.rs` is largely wiring, most substantive logic already has
its own unit tests from Tasks 1/2/6 — this task's own tests focus on:
- `credentials.rs`: resolving a known env-var-backed `credentials_ref`
  succeeds; an unknown one fails with a clear error (use
  `std::env::set_var` inside a test — acceptable in test code per this
  project's `unsafe`-in-Rust-2024 caveat, since `std::env::set_var`
  itself isn't `unsafe` for reading in tests the way *removing* env
  vars across threads can be racy; follow whatever pattern
  `apexes-gateway-binance`'s own tests already use for env-dependent
  config, if any, otherwise avoid mutating process-global env state in
  parallel tests — prefer passing a fake lookup function/map instead of
  literally reading `std::env` in the unit test if that avoids
  test-isolation issues; implementer's judgment call, but must not
  introduce flaky parallel-test env-var races).
- A `#[ignore]`d full end-to-end integration test (requires a running
  gateway + Postgres for the `Live`/`Paper` modes, or none for
  `Backtest`): spawn `apexes-engine` as a real subprocess with a
  `Backtest`-mode `LaunchConfig` against a small synthetic historical
  data source, assert it runs to completion and exits 0 — this is the
  cheapest mode to test end-to-end without external infrastructure.

- [ ] **Step 4: Verify**

Run: `cargo test -p apexes-engine -- --test-threads=1`
Expected: PASS.

Run: `cargo build --workspace && cargo clippy -p apexes-engine --all-targets -- -D warnings`
Expected: 0 errors, 0 warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-engine
git commit -S -s -m "feat(apexes-engine): main.rs -- mode dispatch, ControlHandle wiring, CPU affinity, credentials"
```

---

### Task 8: `apexes-engine`: `/session/ws` protocol + `axum` server

**Files:**
- Create: `crates/apexes-engine/src/protocol.rs`
- Create: `crates/apexes-engine/src/server.rs`
- Modify: `crates/apexes-engine/src/main.rs` (spawn the `axum` server alongside the trading-loop task, write the engine's own PID file)

**Interfaces:**
- Consumes: the `mpsc::Sender<SessionCommand>`/`watch::Receiver<SessionSnapshot>` halves Task 7's `main.rs` kept.
- Produces: `apexes_engine::protocol::SessionMessage`, `GET /session/ws` (bidirectional).

- [ ] **Step 1: Write `protocol.rs`**

```rust
// crates/apexes-engine/src/protocol.rs
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum SessionMessage {
    Snapshot(apexes_backtest::SessionSnapshot),
    Command(apexes_backtest::SessionCommand),
}
```

Confirm `SessionSnapshot`/`SessionCommand` (Task 2) derive
`Serialize`/`Deserialize` — if they don't yet (Task 2's brief didn't
explicitly require it, since Task 2 has no serialization consumer of
its own), add the derives to `apexes-backtest/src/control.rs` as part
of THIS task. Confirmed (design-time check, before this task starts):
`apexes_strategy::Position` and `apexes_strategy::PositionMarginStatus`
currently have NEITHER `Serialize` nor `Deserialize` in their derive
lists (only `MarginMode`, a field inside `Position`, already has both)
— so this task must ALSO add `Serialize`/`Deserialize` to both
`Position` (`crates/apexes-strategy/src/strategy.rs`) and
`PositionMarginStatus` (`crates/apexes-strategy/src/margin_tier.rs`),
which is in scope for this task (small, additive derive-list changes,
zero behavior change) even though those files are outside this task's
originally-listed file set — update the file list accordingly when
executing.

- [ ] **Step 2: Write `server.rs`**

```rust
// crates/apexes-engine/src/server.rs
//! `axum` app exposing `GET /session/ws` -- on upgrade, spawns two
//! tasks per connection: one forwarding `watch::Receiver<SessionSnapshot>`
//! updates out as `SessionMessage::Snapshot` frames, one reading
//! incoming `SessionMessage::Command` frames and forwarding them into
//! the shared `mpsc::Sender<SessionCommand>` (the SAME sender the
//! trading-loop task's `ControlHandle` receives from -- multiple
//! connected UI clients can all send commands, all reaching the same
//! session; this is a deliberate simplification, not designed for
//! multi-client command *ordering* guarantees beyond "arrival order at
//! the mpsc channel").

pub fn build_app(
    command_tx: mpsc::UnboundedSender<SessionCommand>,
    snapshot_rx: watch::Receiver<SessionSnapshot>,
) -> Router { .. }
```

- [ ] **Step 3: Wire into `main.rs`**

After constructing `ControlHandle` (Task 7 Step 2), bind a listener
(port: CLI arg or ephemeral `127.0.0.1:0`, writing the actually-bound
port into this engine's own PID file so `apexes-ui`'s supervisor, Task
9, can read it back), spawn `axum::serve` as its own task, THEN call
the selected `run_*` function on the main task (or vice versa —
whichever ordering avoids a race where the trading loop starts pushing
snapshots before the server is ready to receive them; `watch` channels
tolerate this gracefully since a late subscriber just gets the current
value, so this is a minor robustness concern, not a correctness one).

Write the engine's own PID file at a path `apexes-ui` (Task 9) can
predict (e.g. derived from a session ID `apexes-ui` includes in the
`LaunchConfig` it writes, so the PID file's path is
`~/.apexes/sessions/<session-id>.pid`) — document this path convention
clearly, since Task 9 needs to read it back.

- [ ] **Step 4: Integration test**

Real `127.0.0.1:0` bind, real `tokio-tungstenite` client: connect,
receive at least one `Snapshot` message (from a fake/short trading-loop
run or a directly-driven `ControlHandle` in the test, without needing a
full `run_backtest`), send a `Command::SetParam`, assert (via a shared
test-observable, e.g. the same fake `Strategy` from Task 2's own tests)
it was applied.

- [ ] **Step 5: Verify**

Run: `cargo test -p apexes-engine -- --test-threads=1`
Expected: PASS.

Run: `cargo build --workspace && cargo clippy -p apexes-engine --all-targets -- -D warnings`
Expected: 0 errors, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-engine crates/apexes-backtest  # if Step 1's derives touched apexes-backtest
git commit -S -s -m "feat(apexes-engine): /session/ws protocol + axum server"
```

---

### Task 9: `apexes-ui`: process supervisor (spawn/discover gateway, spawn engines, reconnect+respawn state machine)

**Files:**
- Create: `crates/apexes-ui/src/supervisor.rs`
- Modify: `crates/apexes-ui/src/main.rs` (construct the supervisor at startup instead of directly building `MarketEventBus`)
- Modify: `crates/apexes-ui/Cargo.toml` (dependency on the shared strategy-loader crate from Task 6 Step 3, if extracted; `sysinfo` or equivalent for cross-platform "is this PID alive" checking)

**Interfaces:**
- Consumes: nothing new from other crates beyond what Tasks 5/7/8 already produced (the gateway/engine binaries themselves, invoked as subprocesses — NOT linked against as libraries).
- Produces: `apexes_ui::supervisor::{Supervisor, EngineHandle}`.

- [ ] **Step 1: Write `supervisor.rs`**

```rust
// crates/apexes-ui/src/supervisor.rs
//! Spawns and tracks the gateway process (one, shared, lazily started)
//! and engine processes (one per launched session) as real OS
//! subprocesses via `std::process::Command` -- `apexes-ui` never
//! `tokio::spawn`s a trading task in its own process (see design spec
//! §1.3 for why the earlier in-process design was rejected).

pub struct Supervisor {
    gateway_binary_path: PathBuf,
    engine_binary_path: PathBuf,
}

impl Supervisor {
    /// Checks `~/.apexes/market-data-gateway.pid` (the path
    /// `apexes-market-data-gateway`'s own `main.rs`, Task 5, documents
    /// as authoritative); if the PID recorded there is alive, returns
    /// its known port. Otherwise spawns a new gateway process (passing
    /// through any `cpu_cores`/`instruments` configuration this
    /// `Supervisor` was itself configured with), waits for its PID
    /// file to appear (with a timeout -- do not hang forever if the
    /// spawned process fails to start), and returns the newly-started
    /// port.
    pub async fn ensure_gateway_running(&self) -> Result<GatewayHandle, SupervisorError> { .. }

    /// Writes `launch_config` to a temp JSON file, spawns
    /// `apexes-engine --launch-config <path>` as a subprocess, waits
    /// for the engine's own PID file (path derived from a fresh
    /// session ID this method generates and includes in the written
    /// `LaunchConfig`) to appear, returns an `EngineHandle` (PID +
    /// `/session/ws` URL).
    pub async fn launch_session(&self, launch_config: &LaunchConfig) -> Result<EngineHandle, SupervisorError> { .. }

    /// Sends `SessionCommand::Stop` over the engine's `/session/ws`
    /// (preferred, graceful) -- if that connection is already down,
    /// falls back to sending the process a termination signal
    /// directly (platform-appropriate: SIGTERM on Unix via a suitable
    /// crate, or `TerminateProcess`-equivalent on Windows if this
    /// workspace targets it -- confirm this workspace's actual
    /// supported-platforms scope before over-building Windows-specific
    /// code; if this project is macOS/Linux-only in practice so far,
    /// document that assumption rather than silently skipping Windows
    /// support without saying so).
    pub async fn stop_session(&self, handle: &EngineHandle) -> Result<(), SupervisorError> { .. }
}

pub struct EngineHandle {
    pub pid: u32,
    pub session_ws_url: String,
    pub launch_config: LaunchConfig,  // retained for the respawn-on-repeated-failure path
}
```

- [ ] **Step 2: Reconnect-then-respawn state machine**

This lives where the CLIENT of `/session/ws` actually is (Task 11's UI
client-task extension), but the RESPAWN decision (calling
`launch_session` again with the SAME retained `LaunchConfig` after
reconnect attempts are exhausted) belongs in `supervisor.rs` as the
method the client-task code calls into:

```rust
/// Called by Task 11's client-task reconnect loop after
/// `MAX_RECONNECT_ATTEMPTS` (5, matching the design spec's confirmed
/// figure) consecutive failures using the EXISTING 13a
/// `BackoffState` (500ms doubling, 30s cap -- reused verbatim, not
/// reimplemented) to pace those attempts. Respawns a NEW engine
/// process from `handle.launch_config`, returning a NEW `EngineHandle`
/// (new PID, likely new port) for the caller to reconnect against.
pub async fn respawn_after_repeated_failure(&self, handle: &EngineHandle) -> Result<EngineHandle, SupervisorError> { .. }
```

- [ ] **Step 3: `main.rs` changes**

Replace the existing direct construction of
`MarketEventBus::new(Box::new(BinanceMarketDataProvider::new()))` with:
construct a `Supervisor`, call `ensure_gateway_running()`, then connect
`apexes-ui`'s OWN `/ws` (13a, unchanged endpoint/frame-format) as a
WebSocket CLIENT of the gateway's `/market` endpoint (reusing/adapting
the existing `client.rs` connect logic, Task 3 of the 13a plan) instead
of holding a `MarketEventBus` directly. This is the one required change
to 13a's existing `/ws` data path — the endpoint itself, its frame
format, and the `egui` table it feeds are all UNCHANGED; only WHERE the
underlying `MarketEvent` data comes from changes (gateway process
instead of in-process `MarketEventBus`).

- [ ] **Step 4: Tests**

- `ensure_gateway_running` reuses an existing alive PID (mock/fake PID
  file + a fake "is process alive" check, not a real spawned process,
  for a fast unit test).
- `ensure_gateway_running` spawns a new process when no valid PID file
  exists (this one plausibly needs to be a real subprocess spawn test —
  use a trivial test binary/script standing in for the real gateway
  binary if spawning the real (heavier, Binance-connecting) gateway
  binary in a unit test isn't practical; implementer's judgment, but
  document the choice).
- `launch_session` writes a well-formed `LaunchConfig` JSON file (unit
  test on the serialization step alone, without a real spawn).
- Reconnect-then-respawn state machine: unit test the DECISION logic
  (after N failures, respawn is triggered) using a fake/injectable
  clock or attempt-counter, without real network/process I/O.

- [ ] **Step 5: Verify**

Run: `cargo test -p apexes-ui -- --test-threads=1`
Expected: PASS, every pre-existing 13a test (server/client roundtrip,
egui render, app_state accumulator) still passes UNCHANGED.

Run: `cargo build --workspace && cargo clippy -p apexes-ui --all-targets -- -D warnings`
Expected: 0 errors, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-ui
git commit -S -s -m "feat(apexes-ui): process supervisor -- spawn/discover gateway, spawn engines, reconnect+respawn"
```

---

### Task 10: `apexes-ui`: session launcher tab

**Files:**
- Create: `crates/apexes-ui/src/launcher_ui.rs`
- Modify: `crates/apexes-ui/src/ui.rs` (add a tab/view switch between the existing market table and the new launcher)

**Interfaces:**
- Consumes: Task 6's `strategy_loader::list_available_strategies` (via the shared crate, if extracted), Task 9's `Supervisor::launch_session`.
- Produces: a new `egui` view.

- [ ] **Step 1: Write `launcher_ui.rs`**

Pure-logic state struct (mirroring `app_state.rs`'s existing
no-I/O-dependency pattern, per this crate's established convention) plus
an `egui`-rendering function: mode selector (radio/dropdown), strategy
dropdown (populated from `list_available_strategies`, called once at
tab-open or on a manual refresh button — not polled every frame),
instrument multi-select (start with the same 3 hardcoded Binance Spot
instruments 13a already has as a convenience default, but allow
adding/removing), initial balance inputs, `credentials_ref` dropdown
(populated from... — confirm with Task 7's `credentials.rs` what
`credentials_ref` values are actually enumerable, e.g. reading the same
config file's key list rather than requiring the user to type a raw
string blind), optional CPU-core input, a "Launch" button.

On "Launch" click: construct a `LaunchConfig`, call
`Supervisor::launch_session` (async, via the existing `tokio::runtime::Runtime`
`main.rs` already holds per 13a's established pattern for driving
background async work from the `eframe` update loop), on success switch
the active tab to Task 11's monitoring view with the returned
`EngineHandle`.

- [ ] **Step 2: Tests**

- Pure-logic unit tests for the launcher's own state struct (selection
  validation — e.g. "Launch" disabled until a strategy and at least one
  instrument are selected), no `egui` rendering dependency, mirroring
  `app_state.rs`'s existing testability pattern.
- An `egui_kittest` headless render test (mirroring 13a's existing
  `ui_renders_without_panicking.rs` precedent) asserting the launcher
  view renders without panicking for a representative state (some
  strategies listed, some selected, some not).

- [ ] **Step 3: Verify**

Run: `cargo test -p apexes-ui -- --test-threads=1`
Expected: PASS.

Run: `cargo build --workspace && cargo clippy -p apexes-ui --all-targets -- -D warnings`
Expected: 0 errors, 0 warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-ui
git commit -S -s -m "feat(apexes-ui): session launcher tab"
```

---

### Task 11: `apexes-ui`: monitoring/tuning tab + `client.rs` extension for `/session/ws`

**Files:**
- Modify: `crates/apexes-ui/src/client.rs` (new function/task consuming `/session/ws`, reusing the existing `BackoffState`)
- Create: `crates/apexes-ui/src/monitor_ui.rs`
- Modify: `crates/apexes-ui/src/ui.rs` (wire the new tab in, receive `EngineHandle` from Task 10's launcher)

**Interfaces:**
- Consumes: Task 8's `SessionMessage` protocol, Task 9's `Supervisor::respawn_after_repeated_failure`, the existing `BackoffState` (13a, unchanged).
- Produces: a new `egui` view; a new background task consuming
  `/session/ws`.

- [ ] **Step 1: Extend `client.rs`**

New function (alongside the existing 13a market-data client function,
not replacing it — `apexes-ui` now runs BOTH client tasks
concurrently: one for the gateway's `/market`, one for the active
session's `/session/ws`):

```rust
/// Connects to `session_ws_url`, forwards incoming `SessionMessage::Snapshot`
/// into a channel the UI reads, forwards outgoing `SessionCommand`s (from
/// a channel the UI writes into, e.g. when the user clicks Pause) as
/// `SessionMessage::Command` frames. Reuses the EXISTING `BackoffState`
/// (500ms doubling, 30s cap) for reconnects. After
/// `MAX_RECONNECT_ATTEMPTS` (5) consecutive failures, calls
/// `Supervisor::respawn_after_repeated_failure` and reconnects to the
/// NEW `EngineHandle`'s URL, resetting the backoff/attempt counter.
pub async fn run_session_client(
    initial_handle: EngineHandle,
    supervisor: Arc<Supervisor>,
    snapshot_tx: watch::Sender<Option<SessionSnapshot>>,
    mut command_rx: mpsc::UnboundedReceiver<SessionCommand>,
) { .. }
```

- [ ] **Step 2: Write `monitor_ui.rs`**

Pure-logic state (positions/balances/equity/realized_pnl/margin_status
table rows, derived from the latest `SessionSnapshot`) plus an `egui`
rendering function: the monitoring table, an editable parameter table
(one row per `params` entry, an input box + "Apply" button per row
sending `SessionCommand::SetParam`), Pause/Resume/Stop buttons sending
the corresponding `SessionCommand`s.

- [ ] **Step 3: Tests**

Mirroring Task 10's pattern: pure-logic unit tests for the monitor
state's snapshot-to-table-row transformation (no `egui` dependency),
plus an `egui_kittest` headless render test for the monitoring view
with a representative synthetic `SessionSnapshot`.

`client.rs`'s new `run_session_client` function: a unit/integration
test asserting the respawn path is triggered after exactly
`MAX_RECONNECT_ATTEMPTS` failures (using a fake/injectable connect
function or a real-but-immediately-refusing `127.0.0.1` port, not a
real spawned engine process, for a fast test).

- [ ] **Step 4: Verify**

Run: `cargo test -p apexes-ui -- --test-threads=1`
Expected: PASS, every pre-existing test (13a's own suite + Tasks
9-10's new tests) still passes UNCHANGED.

Run: `cargo build --workspace && cargo clippy -p apexes-ui --all-targets -- -D warnings`
Expected: 0 errors, 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-ui
git commit -S -s -m "feat(apexes-ui): monitoring/tuning tab + /session/ws client with reconnect+respawn"
```

---

### Task 12: Documentation: Drift Script `param()` hot-reload authoring convention

**Files:**
- Modify: a suitable existing Drift Script usage document if one exists
  (search `docs/` for an existing Drift Script authoring guide before
  creating a new file — this project's convention is to extend
  existing docs over creating new ones); if none exists, add a doc
  comment block to `crates/apexes-strategy/src/drift_dsl/params.rs`
  itself (already partially covers this from Task 1, expand it) rather
  than inventing a new top-level doc file for a single function's
  usage convention.

**Interfaces:** none (documentation-only task).

- [ ] **Step 1: Search for an existing home for this documentation**

Run: `file_grep` for "Drift Script" usage/authoring guidance across
`docs/`. If a suitable existing document exists, add a clearly-titled
subsection there. Otherwise, ensure `params.rs`'s own module-level doc
comment (already touched in Task 1) explicitly and clearly states: (a)
top-level `param(...)` calls do not hot-reload, (b) `param(...)` calls
inside `on_market_event` do, (c) a short code example of each pattern
side by side, (d) why this distinction exists (top-level statements
execute once at load time; `on_market_event` re-executes every tick).

- [ ] **Step 2: Commit**

```bash
git add <whatever files Step 1 touched>
git commit -S -s -m "docs: Drift Script param() hot-reload authoring convention"
```

---

### Task 13: Full-workspace verification checkpoint

**Files:** none (verification-only task).

**Interfaces:** none.

- [ ] **Step 1: Run the complete verification suite**

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo test --workspace -- --test-threads=1`
Expected: 0 failures. Record exact passed/ignored counts (via
`grep "test result:"` summed across every suite, per this project's
established convention of never estimating these numbers) for
comparison against this plan's own pre-Task-1 baseline (687 passed / 0
failed / 45 ignored, per the prior trading-loop-unification plan's
final state) — the total pass count must be equal to or greater than
baseline, never lower.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 2: Grep for leftover placeholders**

Run: `grep -rn "TODO\|FIXME\|todo!\|unimplemented!" crates/apexes-market-data-gateway/src crates/apexes-engine/src crates/apexes-ui/src crates/apexes-backtest/src/control.rs crates/apexes-strategy/src/drift_dsl/params.rs`
Expected: no matches.

- [ ] **Step 3: Confirm the two new binaries actually run**

Manual smoke test (not automated, but must be performed and reported
before this task is considered complete): `cargo run -p
apexes-market-data-gateway -- --instruments BTCUSDT` starts cleanly;
`cargo run -p apexes-engine -- --launch-config <a hand-written
Backtest-mode LaunchConfig JSON fixture>` runs a short backtest to
completion. Report the exact commands run and their output/exit codes
in this task's own notes (or the ledger, if using
subagent-driven-development).

- [ ] **Step 4: Commit** (only if Steps 1/2/3 required any fixes; otherwise this task produces no diff and can be skipped without a commit)

```bash
git add -A
git commit -S -s -m "chore: full-workspace verification checkpoint for #13c strategy monitoring & tuning plan"
```

---

### Task 14: Final whole-branch code review and `docs/FEATURES.md` update

**Files:**
- Modify: `docs/FEATURES.md`

**Interfaces:** none (documentation/review-only task).

- [ ] **Step 1: Run the complete verification suite one final time**

Run: `cargo build --workspace`, `cargo test --workspace -- --test-threads=1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`.
Expected: all clean.

- [ ] **Step 2: Escalate to a whole-branch review**

Dispatch an independent reviewer over the ENTIRE branch diff for this
plan (base: the commit before Task 1 started, head: Task 12's final
commit). Pay particular attention to:

- **Credentials never leave `apexes-engine`'s own process boundary** —
  grep the ENTIRE diff for anywhere `credentials_ref` resolution
  results (actual API keys/secrets) might be logged, included in a
  `SessionSnapshot`, or transmitted over `/session/ws` or `/market` —
  confirm zero such paths exist. This is the single most
  safety-critical property in this plan.
- **`UnifiedLoopConfig.control: None` is a genuine no-op** — re-verify
  by diffing `run_backtest`/`run_paper_trading`/`run_live_trading`'s
  own construction of `UnifiedLoopConfig` before/after Task 2, confirm
  the ONLY change is the one new field being set to `None`, nothing
  else shifted.
- **`Strategy::current_params`/`set_param`'s default implementations
  are truly behavior-preserving** — confirm no existing `Strategy`
  implementation anywhere in the workspace needed to change to keep
  compiling, and that every pre-existing test involving a `Strategy`
  still passes with unchanged assertions.
- **The pause semantics are correctly scoped** — re-verify against
  `unified_loop.rs`'s actual per-tick sequence that funding settlement/
  margin-status/liquidation/de-risking steps still run during a paused
  tick, and ONLY `strategy.on_market_event` itself is skipped — this
  was Task 2's most subtle placement decision.
- **`StorageWriter::write_batch`'s homogeneous-batch requirement is
  genuinely honored** by `apexes-market-data-gateway`'s persistence
  buffer under concurrent/interleaved event arrival, not just in the
  common case.
- **No behavior change to `apexes-ui`'s existing 13a `/ws` endpoint's
  external contract** (frame format, filtering to Trade/Tick) — only
  its internal data source changed (gateway process instead of direct
  `MarketEventBus`).
- **Process-spawn error handling** — confirm `Supervisor`'s spawn
  failure paths (gateway binary missing, engine binary missing, bind
  port already in use) produce clear errors surfaced to the UI, not a
  silent hang or an unhandled panic.

Fix any findings, get a fix-verification re-review, and only then
consider this plan complete.

- [ ] **Step 3: Update `docs/FEATURES.md`**

Find row `9.3` (`grep -n "| 9.3 |" docs/FEATURES.md`) and update its
Status column from `🔲 Not Started` to `✅ Done`, with Notes
summarizing: the multi-process session architecture
(`apexes-market-data-gateway` + `apexes-engine`, `apexes-ui` as a thin
supervisor/client), live parameter tuning via `Strategy::current_params`/
`set_param` + `ControlHandle` built into the unified event loop
(uniform across backtest/paper/live), pause/resume/stop, the session
launcher UI, and the monitoring UI. Also find row `8.3`
(`grep -n "| 8.3 |" docs/FEATURES.md`) and update its Status from
`🔲 Not Started` to `🚧 In Progress` (NOT `✅ Done` — this plan
delivers only the minimum-viable slice explicitly scoped in the design
spec §2.2, not NATS JetStream or genuine multi-machine validation),
with Notes stating precisely what exists (`apexes-market-data-gateway`:
real-time cross-process WebSocket broadcast + optional historical
replay/persistence via the existing `apexes-storage` ClickHouse layer)
and what remains open (NATS JetStream or an equivalent production-grade
transport, genuine multi-machine deployment validation, true
publish-subscribe multi-topic generality beyond the single
`MarketEvent` payload type).

Recompute the Summary counts table via actual
`grep -c "✅ Done"`/`grep -c "🚧 In Progress"`/`grep -c "🔲 Not Started"`
(subtract 2 from each raw `✅ Done`/`🔲 Not Started` count per this
project's standing convention for the legend row + summary table's own
count row; confirm whether `🚧 In Progress` needs the same 2-row
subtraction by checking whether that exact string also appears in the
legend and summary rows, the same way `✅ Done`/`🔲 Not Started` do —
do not assume, verify).

- [ ] **Step 3b: Refresh `docs/PLAN.md`, `README.md` and `AGENTS.md`**

- `docs/PLAN.md`: mark Tasks 9-14 done with commit ranges, move the 13c
  plan from "Now" to "Completed plans", set "Now" to the next
  sub-project (or "none"), update `_Last updated_`, and resolve or
  re-list every item under "Carry-forward technical debt" (the final
  review's findings that were deferred belong there).
- `README.md`: update the Status section (counts must equal the
  recomputed `FEATURES.md` Summary table; move 9.3 and the 8.3 slice
  into "Implemented"), and replace the "will start the other two itself
  once the supervisor work lands" sentence with how to actually launch
  the system.
- `AGENTS.md`: re-read sections 4 and 6 against the final code. Add any
  new architecture invariant this plan introduced (e.g. the supervisor's
  respawn rule), and correct anything the implementation changed.

- [ ] **Step 4: Commit**

```bash
git add docs/FEATURES.md docs/PLAN.md README.md AGENTS.md
git commit -S -s -m "docs: mark FEATURES.md row 9.3 (strategy monitoring & live tuning panel) Done, row 8.3 (message bus) In Progress; refresh PLAN/README/AGENTS"
```
