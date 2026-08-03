# Apexes: Unify the Backtest/Paper/Live Trading Loop (pre-`#13c` prerequisite refactor) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the design in
`docs/superpowers/specs/2026-08-06-apexes-trading-loop-unification-design.md`:
three new pluggable traits in `apexes-strategy`
(`OrderExecutor`/`LiquidationPolicy`/`SupplementalFillSource`), their
backtest (`MatchingEngine`-backed) and live-trading (`OrderManager`-backed)
implementations, and a single unified event-loop core that
`run_backtest`/`run_paper_trading`/`run_live_trading` all become thin
callers of — replacing today's two independently-maintained hand-written
loops (`apexes-backtest::event_loop::run_event_loop` and
`apexes-live-trading::run_live_trading`'s own loop body). This is
prerequisite groundwork for roadmap sub-project `#13c` (strategy
monitoring & live tuning panel), NOT part of `#13c` itself — this plan
adds no new user-facing monitoring/tuning feature; it only reduces the
event-loop implementation from two to one so `#13c`'s later hooks only
need to be added once.

**Architecture:** Seven sequential tasks in dependency order. Task 1
defines the three new traits in `apexes-strategy` (no behavior change
anywhere yet — pure new-type-definitions task). Task 2 implements the
backtest-side adapters (`MatchingEngineOrderExecutor`,
`MatchingEngineLiquidationPolicy`, a permanently-pending
`SupplementalFillSource`) in `apexes-backtest`, with their own isolated
unit tests, still not wired into `run_event_loop` yet. Task 3 implements
the live-trading-side adapters (`OrderManagerExecutor`,
`LiveLiquidationPolicy`, `LiveSupplementalFillSource`) in
`apexes-live-trading`, likewise isolated and unit-tested only. Task 4
builds the actual unified loop function and re-points `run_backtest`/
`run_paper_trading` at it (deleting the old `run_event_loop` body, not
keeping both). Task 5 re-points `run_live_trading` at the same unified
loop function, deleting its own hand-written loop body. Task 6 is a
full-workspace verification checkpoint. Task 7 is the final whole-branch
review plus `docs/FEATURES.md` update.

**Tech Stack:** Rust 2024, existing workspace deps only. No new external
crates required — `async-trait` (already a workspace dependency, used
by `apexes-ems::ChildOrderSink` and others) is reused for the two async
trait methods (`OrderExecutor::submit`, `SupplementalFillSource`'s
stream-producing methods where `async_trait` applies).

## Global Constraints

- Rust edition 2024, workspace-wide `cargo check`/
  `cargo clippy --all-targets -- -D warnings`/`cargo fmt --check` must
  stay clean after every task.
- No `unsafe` blocks. Never bare `.unwrap()`/`.expect()` in production
  code (test code may use them freely, matching existing convention).
- **This is a behavior-preserving refactor.** Every existing test in
  `apexes-backtest`, `apexes-paper`, and `apexes-live-trading` must pass
  UNCHANGED after this plan completes — no test's assertions may be
  edited to accommodate the refactor. If a test fails after Task 4/5,
  the fix belongs in the new unified loop / adapter code, never in the
  test.
- `run_backtest`'s, `run_paper_trading`'s, and `run_live_trading`'s
  public signatures (`BacktestConfig`, `PaperTradingConfig`,
  `LiveTradingConfig`, and each function's own parameter list) do not
  change. External callers of these three functions are unaffected by
  this plan.
- Risk-checking (`CompositeRiskChecker`'s `check`/`on_fill`/`on_tick`/
  `on_market_event`, and `MarketConditionMonitor`'s
  `PreTradeAdapter`/`EventAdapter`/`ReadHandle` trio) stays called
  directly from the unified loop body — it is NOT pushed down into
  `OrderExecutor`/`LiquidationPolicy`/`SupplementalFillSource`. Do not
  "simplify" by moving risk-check calls into the new traits; the design
  spec explicitly keeps this boundary where it is today.
- `OrderExecutor::open_orders()` returns an owned
  `HashMap<OrderId, Order>` (a clone), not a borrowed reference — this
  is a deliberate, documented tradeoff (see design spec §2.1's doc
  comment), not something to "fix" by trying to thread lifetimes
  through the trait.
- `LiquidationPolicy::should_auto_execute(LiquidationReason::MarginBreach)`
  must return `false` UNCONDITIONALLY in every live-trading
  implementation — not gated on any config flag. Only
  `LiquidationReason::ExtremeMarketCondition` is gated on
  `auto_derisk_live`. Getting this backwards (e.g. making
  `auto_derisk_live` also control `MarginBreach`) would be a real-money
  safety regression from today's behavior and must not happen.
- Read `docs/superpowers/specs/2026-08-06-apexes-trading-loop-unification-design.md`
  in full before starting Task 1 — it contains the full rationale for
  every trait method's shape (why `submit` returns `Vec<Fill>` instead
  of `Option<Vec<Fill>>`, why `on_market_event` is synchronous while
  `submit` is async, why `SupplementalFillSource` is a genuinely
  separate concept from `OrderExecutor::on_market_event`) — this plan's
  task briefs summarize but do not repeat that rationale in full.
- Every commit signed: `git commit -S -s`.

---

### Task 1: `apexes-strategy`: define `OrderExecutor`, `LiquidationPolicy`, `SupplementalFillSource`

**Files:**
- Create: `crates/apexes-strategy/src/order_executor.rs`
- Modify: `crates/apexes-strategy/src/lib.rs` (re-export the new module's public types)
- Modify: `crates/apexes-strategy/Cargo.toml` (add `futures` to `[dependencies]` if not already present — check first, `futures` is currently only a `[dev-dependencies]` entry in this crate)

**Interfaces:**
- Consumes: `apexes_core::{ApexesError, Fill, MarketEvent, Order, OrderId}` (all existing), `crate::{OrderIntent}` (existing, same crate).
- Produces: `apexes_strategy::{OrderExecutor, LiquidationPolicy, LiquidationReason, SupplementalFillSource}`.

- [ ] **Step 1: Confirm `futures` dependency status**

Run: `grep -A3 '\[dependencies\]' crates/apexes-strategy/Cargo.toml`
If `futures` is not listed under `[dependencies]` (only under
`[dev-dependencies]`), add `futures = { workspace = true }` to
`[dependencies]` — `SupplementalFillSource`'s method returns
`impl Stream<Item = Vec<Fill>>`, needed outside `#[cfg(test)]` code.

- [ ] **Step 2: Write `order_executor.rs`**

```rust
// crates/apexes-strategy/src/order_executor.rs (new file)
//! Three pluggable traits letting a single unified event-loop core
//! (see `apexes-backtest`'s unified loop, added by a later plan task)
//! drive either simulated (backtest/paper) or real (live-trading)
//! order execution without knowing which. See design spec
//! `docs/superpowers/specs/2026-08-06-apexes-trading-loop-unification-design.md`
//! §二 for full rationale.

use std::collections::HashMap;

use apexes_core::{ApexesError, Fill, MarketEvent, Order, OrderId};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use rust_decimal::Decimal;

use crate::{OrderIntent, Position};

/// Submits non-algo-routed `OrderIntent`s (Market/Limit/Stop/StopLimit)
/// and answers "what's currently open" — the execution backend a
/// unified event loop drives, abstracting over
/// `apexes-backtest::MatchingEngine` (synchronous simulated fills) and
/// `apexes-oms::OrderManager` (asynchronous, fills-arrive-later real
/// submission).
#[async_trait]
pub trait OrderExecutor: Send + Sync {
    /// Submits `intent`, returning the accepted `Order` plus any fills
    /// that happened immediately as a direct result of this call.
    /// `Vec::new()` is a completely normal, expected return (a resting
    /// Limit/Stop order in backtest that didn't cross the book yet;
    /// EVERY live-trading submission, since real fills always arrive
    /// later via `SupplementalFillSource` or `on_market_event`).
    ///
    /// `positions`/`balances`: required by `OrderManager::submit_order`
    /// (live trading) for its own `risk_checker` pre-trade check — a
    /// SECOND, independent check from the one the unified loop already
    /// performs directly, matching today's exact behavior (not new).
    /// `MatchingEngineOrderExecutor` (backtest) ignores both parameters
    /// entirely — `MatchingEngine` performs no risk checking of its
    /// own.
    async fn submit(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        now: DateTime<Utc>,
    ) -> Result<(Order, Vec<Fill>), ApexesError>;

    /// Gives this executor's local resting-order book a chance to react
    /// to a new `MarketEvent` on the SAME primary market-data stream the
    /// unified loop is already iterating (not an independent stream —
    /// see `SupplementalFillSource` for that). Returns any fills this
    /// produced, grouped by the original `OrderId`.
    ///
    /// Backtest: forwards to `MatchingEngine::on_trade_event`/
    /// `on_depth_update` depending on `event`'s variant.
    /// Live trading: always returns `Vec::new()` — live trading holds
    /// no local resting-order simulation to react to market data with.
    fn on_market_event(&self, event: &MarketEvent) -> Vec<(OrderId, Vec<Fill>)>;

    /// Every currently open (unfilled or partially-filled) order,
    /// keyed by `OrderId` — feeds `StrategyContext::open_orders`.
    /// Returns an owned `HashMap` (a clone each call); see this
    /// plan's Global Constraints for why that's a deliberate tradeoff,
    /// not a gap. `async fn` (not synchronous): `OrderManager::open_orders()`
    /// requires `.await`ing a `tokio::sync::Mutex` guard; the backtest
    /// implementation (a `std::sync::Mutex`) simply locks/clones/returns
    /// synchronously inside the `async fn` body without ever actually
    /// crossing an `.await` point.
    async fn open_orders(&self) -> HashMap<OrderId, Order>;
}

/// Distinguishes why a position is being force/partially-closed, since
/// live trading's auto-execution policy differs by reason (see
/// `LiquidationPolicy::should_auto_execute`'s doc comment).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiquidationReason {
    /// A single leveraged position's margin ratio breached its
    /// maintenance-margin threshold (`docs/FEATURES.md` row `6.4`).
    MarginBreach,
    /// Extreme market conditions triggered a proportional de-risking
    /// partial close (`docs/FEATURES.md` row `6.5`).
    ExtremeMarketCondition,
}

/// Decides whether a computed liquidation/de-risking order should
/// actually be submitted automatically, or merely logged
/// (monitor-only) for an operator to act on.
pub trait LiquidationPolicy: Send + Sync {
    fn should_auto_execute(&self, reason: LiquidationReason) -> bool;
}

/// A source of fills that is genuinely independent of the primary
/// market-event stream the unified loop iterates — e.g. live trading's
/// exchange push-update channel plus its periodic `reconcile()` poll,
/// merged into one stream. Backtest/paper have no such independent
/// source (all their fills come from `OrderExecutor::submit`'s return
/// value or `OrderExecutor::on_market_event`), so their implementation
/// is a stream that never yields.
pub trait SupplementalFillSource: Send {
    fn stream(&mut self) -> impl Stream<Item = Vec<Fill>> + Unpin + '_;
}
```

- [ ] **Step 3: Re-export from `lib.rs`**

Add to `crates/apexes-strategy/src/lib.rs`:
```rust
mod order_executor;
pub use order_executor::{LiquidationPolicy, LiquidationReason, OrderExecutor, SupplementalFillSource};
```
(Match this crate's existing `mod`/`pub use` style exactly — read the
existing `lib.rs` first via `file_read` to place these lines
consistently with how `margin_tier`/`strategy`/`venue_scorer` etc. are
already declared and re-exported.)

- [ ] **Step 4: Verify**

Run: `cargo check -p apexes-strategy`
Expected: 0 errors. (These are just new trait definitions — nothing
implements them yet, nothing calls them yet, so this should compile
trivially once syntax is correct.)

Run: `cargo clippy -p apexes-strategy --all-targets -- -D warnings`
Expected: 0 warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-strategy/src/order_executor.rs crates/apexes-strategy/src/lib.rs crates/apexes-strategy/Cargo.toml
git commit -S -s -m "feat(apexes-strategy): define OrderExecutor/LiquidationPolicy/SupplementalFillSource traits"
```

---

### Task 2: `apexes-backtest`: `MatchingEngine`-backed adapters

**Files:**
- Create: `crates/apexes-backtest/src/order_executor_adapter.rs`
- Modify: `crates/apexes-backtest/src/lib.rs` (re-export the new types if this crate's convention re-exports internal adapter types — check `ems_adapter`'s current re-export status first; if `MatchingEngineChildOrderSink` is NOT re-exported from `lib.rs` today, match that precedent and don't re-export the new types either, only make the module `pub`)
- Modify: `crates/apexes-backtest/Cargo.toml` (add `apexes-strategy`'s new traits are already available — this crate already depends on `apexes-strategy`, confirm via `grep apexes-strategy crates/apexes-backtest/Cargo.toml`, no new dependency needed)

**Interfaces:**
- Consumes: `crate::matching_engine::MatchingEngine` (existing), `apexes_strategy::{OrderExecutor, LiquidationPolicy, LiquidationReason, SupplementalFillSource}` (Task 1).
- Produces: `apexes_backtest::order_executor_adapter::{MatchingEngineOrderExecutor, BacktestLiquidationPolicy, NeverFillSupplementalFillSource}` (or equivalent names — exact naming is the implementer's call, but must be descriptive and match this crate's existing naming convention, e.g. `MatchingEngineChildOrderSink`'s pattern of `<Backend><TraitName>`).

- [ ] **Step 1: Read `MatchingEngine`'s existing methods before writing the adapter**

Read `crates/apexes-backtest/src/matching_engine.rs` in full (1633
lines — read in 2-3 chunks via `file_read`'s line ranges). Pay
particular attention to: `submit_market_order`/`submit_limit_order`/
`submit_stop_order`'s exact signatures and return types (all take
`&mut self`, so the adapter needs its own `Arc<Mutex<MatchingEngine>>`
exactly like `MatchingEngineChildOrderSink` already does — read
`crates/apexes-backtest/src/ems_adapter.rs` in full as the direct
structural precedent for lock-recovery style, matching
`event_loop.rs`'s existing `lock_engine` helper's poison-recovery
pattern), `on_trade_event`'s return shape
(`Vec<(OrderId, Vec<Fill>)>` — already exactly matches
`OrderExecutor::on_market_event`'s required return type), and
`on_depth_update`'s signature (returns nothing, updates internal book
state only — `on_market_event`'s `Depth` branch calls it and returns
`Vec::new()`).

- [ ] **Step 2: Write `MatchingEngineOrderExecutor`**

Wraps `Arc<Mutex<MatchingEngine>>` (the SAME shared instance
`run_event_loop` today constructs and also shares with
`MatchingEngineChildOrderSink` — the unified loop, built in Task 4,
must construct exactly one `MatchingEngine` and share it across the
EMS child-order-sink adapter AND this new executor adapter, exactly as
today's code shares it between the sink and the loop's own direct
calls).

Also owns its own `Mutex<HashMap<OrderId, Order>>` for `open_orders()`
— populated/updated at three points: (a) every `submit()` call inserts
the returned `Order`; (b) every `on_market_event()` call that produces
fills updates the corresponding entry's `filled_quantity`/`status`
(mirror today's `run_event_loop`'s exact update logic at the
`MarketEvent::Trade` match arm — copy its `filled_quantity`/`status`
computation verbatim, do not re-derive it); (c) nothing removes
entries on terminal status — confirm this matches today's behavior
(today's `open_orders: HashMap<OrderId, Order>` in `run_event_loop`
is also never pruned; read the whole function once more to confirm
before assuming, since preserving this exact behavior — however
unbounded — is required by this plan's behavior-preservation
constraint, not a defect this task is meant to fix).

`submit()`: `positions`/`balances` parameters are accepted (per Task
1's trait signature) but unused — `MatchingEngine` performs no risk
checking of its own. Dispatches on `intent.order_type` to
`submit_market_order`/`submit_limit_order`/`submit_stop_order` — copy
the exact match logic from `event_loop.rs`'s existing dispatch (the
`let submit_result = match intent.order_type { .. }` block), converting
each arm's return shape to the trait's uniform
`Result<(Order, Vec<Fill>), ApexesError>` (`Limit`/`Stop`/`StopLimit`
arms wrap their bare `Order` as `(order, vec![])`).

`on_market_event()`: `MarketEvent::Trade` arm calls
`on_trade_event`, returns its `Vec<(OrderId, Vec<Fill>)>` directly.
`MarketEvent::Depth` arm calls `on_depth_update`, returns
`Vec::new()`. All other variants (`Tick`/`Bar`/`MarkPrice`) return
`Vec::new()`.

`open_orders()`: `async fn`, body is
`self.open_orders.lock().unwrap_or_else(|p| p.into_inner()).clone()`
(no actual `.await` needed — `std::sync::Mutex`'s lock is synchronous;
the method is `async fn` only because the trait requires it for
trait-object uniformity with the live implementation).

- [ ] **Step 3: Write `BacktestLiquidationPolicy`**

Zero-sized type (or a unit struct). `should_auto_execute` returns
`true` unconditionally for both `LiquidationReason` variants —
matching today's backtest/paper behavior (auto-liquidate margin
breaches, auto-execute de-risking).

- [ ] **Step 4: Write `NeverFillSupplementalFillSource`**

Zero-sized type. `stream()` returns `futures::stream::pending::<Vec<Fill>>()`
boxed/pinned as required by the trait's `impl Stream<Item = Vec<Fill>> + Unpin + '_`
return type (use `Box::pin` if a bare `impl Stream` can't satisfy
`Unpin` directly — check `futures::stream::pending`'s actual `Unpin`
status first via `cargo doc` or the crate's docs.rs page before adding
unnecessary boxing).

- [ ] **Step 5: Unit tests, isolated (no full event loop)**

In `#[cfg(test)] mod tests` within the new file:
- `matching_engine_order_executor_submit_market_order_returns_immediate_fills`:
  construct a `MatchingEngineOrderExecutor` wrapping a fresh
  `MatchingEngine` seeded with a resting counter-side limit order (or
  depth), submit a crossing Market `OrderIntent`, assert the returned
  `Vec<Fill>` is non-empty.
- `matching_engine_order_executor_submit_limit_order_returns_no_immediate_fills`:
  submit a non-crossing Limit order, assert `Vec<Fill>` is empty and
  the returned `Order.status` is `New`.
- `matching_engine_order_executor_open_orders_reflects_submitted_orders`:
  submit two orders, assert `open_orders()` contains both by `OrderId`.
- `matching_engine_order_executor_on_market_event_fills_resting_order`:
  submit a resting Limit order, then call `on_market_event` with a
  crossing `MarketEvent::Trade`, assert the returned
  `Vec<(OrderId, Vec<Fill>)>` contains the expected fill.
- `backtest_liquidation_policy_always_auto_executes`: assert `true` for
  both `LiquidationReason` variants.
- `never_fill_supplemental_fill_source_never_yields`: poll the stream
  once with a short `tokio::time::timeout`, assert it times out
  (never resolves) rather than yielding `None`/a value.

- [ ] **Step 6: Verify**

Run: `cargo test -p apexes-backtest order_executor_adapter -- --test-threads=1`
Expected: PASS, all new tests green.

Run: `cargo build -p apexes-backtest && cargo clippy -p apexes-backtest --all-targets -- -D warnings`
Expected: 0 errors, 0 warnings.

- [ ] **Step 7: Commit**

```bash
git add crates/apexes-backtest/src/order_executor_adapter.rs crates/apexes-backtest/src/lib.rs
git commit -S -s -m "feat(apexes-backtest): MatchingEngine-backed OrderExecutor/LiquidationPolicy/SupplementalFillSource adapters"
```

---

### Task 3: `apexes-live-trading`: `OrderManager`-backed adapters

**Files:**
- Create: `crates/apexes-live-trading/src/order_executor_adapter.rs`
- Modify: `crates/apexes-live-trading/src/lib.rs` (module declaration, matching whatever visibility convention Task 2 established)

**Interfaces:**
- Consumes: `apexes_oms::{OrderManager, ExchangeUpdate}` (existing),
  `apexes_strategy::{OrderExecutor, LiquidationPolicy, LiquidationReason, SupplementalFillSource}` (Task 1).
- Produces: `apexes_live_trading::order_executor_adapter::{OrderManagerExecutor, LiveLiquidationPolicy, LiveSupplementalFillSource}` (or equivalent descriptive names, matching Task 2's naming convention).

- [ ] **Step 1: Write `OrderManagerExecutor`**

Wraps `Arc<tokio::sync::Mutex<OrderManager>>` (the same shared instance
`run_live_trading` already constructs). `submit()` calls
`order_manager.lock().await.submit_order(intent.clone(), positions, balances)`,
passing through the trait's own `positions`/`balances` parameters
(Task 1) unchanged. On success, wraps the returned `OrderId` by
looking up the freshly-inserted `Order` from
`order_manager.open_orders()` (needed because
`OrderManager::submit_order` returns only `OrderId`, but the trait
needs the full `Order`) and returns `(order, vec![])` (always empty
fills — real fills never arrive synchronously in live trading).

`on_market_event()`: always returns `Vec::new()` (live trading holds no
local resting-order simulation).

`open_orders()`: `async fn`, body is
`order_manager.lock().await.open_orders().clone()`.

- [ ] **Step 2: Write `LiveLiquidationPolicy`**

Holds `auto_derisk_live: bool` (copied from `LiveTradingConfig` at
construction time). `should_auto_execute`:
`LiquidationReason::MarginBreach => false` (unconditional, per this
plan's Global Constraints — do NOT read `auto_derisk_live` for this
arm). `LiquidationReason::ExtremeMarketCondition => self.auto_derisk_live`.

- [ ] **Step 3: Write `LiveSupplementalFillSource`**

Wraps the existing `exchange_updates` stream (already
`.chain(futures::stream::pending())`-guarded at `run_live_trading`'s
construction site — confirm this guarding is preserved, not
duplicated or dropped) plus a `tokio::time::Interval` built from
`reconcile_interval`, merged via `futures::stream::select`/
`tokio_stream::StreamExt::merge` (check which is already a workspace
dependency — `futures` is; confirm whether `tokio-stream` is already
a dependency of this crate before adding it, or implement the merge
manually via a `tokio::select!`-driven wrapper stream if simpler and
more consistent with this crate's existing style). Each `exchange_updates`
item is applied via
`order_manager.apply_exchange_update(...)` (as today) and its
resulting `Vec<Fill>` is what gets yielded; each reconcile tick's
`Vec<(OrderId, Vec<Fill>)>` is flattened (via `.flat_map(|(_, fills)| fills)`)
into a single `Vec<Fill>` before yielding — `OrderId` is not lost,
it's still present on each individual `Fill.order_id` field, matching
today's `apply_new_fills`'s existing handling.

- [ ] **Step 4: Unit tests, isolated**

Mirror Task 2 Step 5's structure but against `OrderManager`/mock
gateway fixtures (reuse this crate's existing test helpers — read
`crates/apexes-live-trading/tests/live_trading_integration.rs`'s
existing `registry_with`/mock-gateway helpers first rather than
reinventing them):
- `order_manager_executor_submit_always_returns_empty_fills`
- `order_manager_executor_open_orders_reflects_submitted_orders`
- `order_manager_executor_on_market_event_always_returns_empty`
- `live_liquidation_policy_margin_breach_always_false_regardless_of_auto_derisk_live`
  (construct with `auto_derisk_live: true` AND separately `false`,
  assert `MarginBreach` is `false` in BOTH cases — this is the single
  most safety-critical test in this task)
- `live_liquidation_policy_extreme_market_condition_follows_auto_derisk_live`
  (assert `true`/`false` follows the flag)
- `live_supplemental_fill_source_merges_exchange_updates_and_reconcile`

- [ ] **Step 5: Verify**

Run: `cargo test -p apexes-live-trading order_executor_adapter -- --test-threads=1`
Expected: PASS.

Run: `cargo build -p apexes-live-trading && cargo clippy -p apexes-live-trading --all-targets -- -D warnings`
Expected: 0 errors, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-live-trading/src/order_executor_adapter.rs crates/apexes-live-trading/src/lib.rs
git commit -S -s -m "feat(apexes-live-trading): OrderManager-backed OrderExecutor/LiquidationPolicy/SupplementalFillSource adapters"
```

---

### Task 4: Build the unified loop; re-point `run_backtest`/`run_paper_trading`

**Files:**
- Create: `crates/apexes-backtest/src/unified_loop.rs`
- Modify: `crates/apexes-backtest/src/event_loop.rs` (delete `run_event_loop`'s body once the unified loop supersedes it — see Step 3)
- Modify: `crates/apexes-backtest/src/runner.rs` (`run_backtest` calls the unified loop)
- Modify: `crates/apexes-paper/src/runner.rs` (`run_paper_trading` calls the unified loop)
- Modify: `crates/apexes-backtest/src/lib.rs` (module wiring)

**Interfaces:**
- Consumes: Task 1's three traits, Task 2's backtest adapters, all
  existing `apexes-backtest` internals (`PortfolioLedger`, `CostModel`,
  `LatencyModel`, `PerformanceTracker`, `BacktestClock`, `BacktestReport`).
- Produces: `apexes_backtest::unified_loop::{run_unified_event_loop, UnifiedLoopConfig}` (or names matching the design spec's §三 naming), with `run_backtest`/`run_paper_trading`'s existing public signatures unchanged.

- [ ] **Step 1: Re-read `event_loop.rs` in full**

This file is ~1149 lines; read it completely (in chunks) immediately
before starting this task, even though it was read during design —
confirm nothing has shifted since. Identify every piece of state
(`ledger`, `overall_tracker`, `per_symbol_trackers`,
`per_venue_trackers`, `last_prices`, `mark_prices`,
`last_funding_settlement`, `all_fills`, `recorded_initial_equity`,
`risk_checker`, `market_condition_monitor`) and every call sequence
per tick (funding settlement, margin status, liquidation check,
de-risking check, strategy dispatch, algo-order dispatch) that must be
preserved verbatim in the new function — this task is a structural
rewrite of the SAME logic through new abstractions, not a logic
change.

- [ ] **Step 2: Write `run_unified_event_loop`**

Signature per design spec §三 (adjust parameter order/packing only if
Rust's argument-count linting or ergonomics require it — prefer
keeping close to the design spec's proposed shape):

```rust
#[allow(clippy::too_many_arguments)]
pub async fn run_unified_event_loop(
    config: UnifiedLoopConfig,
    mut market_events: impl Stream<Item = MarketEvent> + Unpin,
    mut clock: BacktestClock,
    executor: Arc<dyn OrderExecutor>,
    liquidation_policy: Arc<dyn LiquidationPolicy>,
    mut supplemental_fills: impl SupplementalFillSource,
    venue_scorer: Arc<dyn VenueScorer>,
    volume_curve_provider: Option<Arc<dyn apexes_ems::VolumeCurveProvider>>,
    mut strategy: impl Strategy,
    mut stop_signal: Option<tokio::sync::oneshot::Receiver<()>>,
    latency_model: Option<LatencyModel>,
) -> Result<UnifiedLoopReport, ApexesError> { .. }
```

`UnifiedLoopConfig` carries exactly the fields common to both callers
today (`instruments`, `initial_balances`, `risk_free_return`,
`risk_checker`, `margin_tiers`, `market_condition_config`) — NOT
`cost_model`/`queue_position_factor` (backtest-only, stays on the
caller's own config type and gets baked into the `MatchingEngine` the
caller constructs before building the `OrderExecutor` Arc, not passed
through this shared config).

The `tokio::select!` body has two arms:
1. `market_events.next()` — port `event_loop.rs`'s ENTIRE per-tick
   sequence (Step 1's list), replacing every direct `MatchingEngine`/
   `matching_engine` call with the equivalent `executor.on_market_event(..)`/
   `executor.submit(..)` call, and every liquidation/de-risking
   auto-submit decision with
   `if liquidation_policy.should_auto_execute(reason) { executor.submit(..) } else { tracing::error!(..) }`.
   `StrategyContext::open_orders` is built from `executor.open_orders()`
   called fresh each tick (no local map).
2. `supplemental_fills.stream().next()` — apply the returned `Vec<Fill>`
   to the ledger/tracker/risk_checker exactly as `run_live_trading`'s
   existing `apply_new_fills` does today (port that function's body,
   generalized to not require `Option<Arc<CompositeRiskChecker>>`
   specifically if the unified loop's own `risk_checker` field has a
   slightly different type — reconcile types directly, don't wrap).

Preserve `stop_signal`'s exact today-semantics (checked once per top of
loop for the historical-replay-style caller; `run_live_trading`'s
own `tokio::select!`-embedded stop-signal-as-a-select-arm style is
ALSO valid and arguably better — if adopting it changes observable
behavior at all for `run_backtest`/`run_paper_trading`, keep the
existing check-at-top-of-loop style instead to avoid any behavioral
drift; only unify the STRUCTURE, not necessarily every micro-detail of
polling order, if doing so risks a behavior change existing tests
would catch).

- [ ] **Step 3: Delete `run_event_loop`'s old body; keep or remove `event_loop.rs`**

`crate::event_loop::run_event_loop` and `EventLoopConfig` are removed
entirely (not kept alongside the new unified loop as dead code) once
`run_backtest`/`run_paper_trading` are re-pointed in Step 4 — this
plan replaces the implementation, it does not add a second one
alongside it. Decide whether `event_loop.rs` the file is deleted
outright (with `unified_loop.rs` fully replacing it) or whether
`unified_loop.rs`'s content is placed directly into `event_loop.rs`
(renaming the module's exported function while keeping the file) —
either is acceptable; prefer whichever keeps `git blame`/history
clearest. If keeping `event_loop.rs` as the filename, update this
task's file list accordingly and note the actual choice made in the
commit message.

- [ ] **Step 4: Re-point `run_backtest`/`run_paper_trading`**

In `crates/apexes-backtest/src/runner.rs`'s `run_backtest`: construct
`MatchingEngineOrderExecutor`/`BacktestLiquidationPolicy`/
`NeverFillSupplementalFillSource` (Task 2) instead of building the old
`EventLoopConfig`, call `run_unified_event_loop` instead of
`run_event_loop`. Same in `crates/apexes-paper/src/runner.rs`'s
`run_paper_trading`. Both functions' own public signatures/config
struct fields are UNCHANGED (per Global Constraints) — only their
internal bodies change.

- [ ] **Step 5: Run every existing test for both crates**

Run: `cargo test -p apexes-backtest -- --test-threads=1`
Expected: PASS, EVERY test that existed before this task still passes,
with UNCHANGED assertions (if any test fails, the fix belongs in
Step 2/4's new code, never in the test — see Global Constraints).

Run: `cargo test -p apexes-paper -- --test-threads=1`
Expected: same.

Run: `cargo build --workspace` (confirms no downstream crate broke)
Expected: 0 errors.

Run: `cargo clippy -p apexes-backtest -p apexes-paper --all-targets -- -D warnings`
Expected: 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-backtest crates/apexes-paper
git commit -S -s -m "refactor(apexes-backtest,apexes-paper): unify run_backtest/run_paper_trading onto a single event-loop core"
```

---

### Task 5: Re-point `run_live_trading` onto the same unified loop

**Files:**
- Modify: `crates/apexes-live-trading/src/runner.rs` (delete the hand-written loop body, construct Task 3's adapters, call `apexes_backtest::unified_loop::run_unified_event_loop`)
- Modify: `crates/apexes-live-trading/Cargo.toml` (confirm `apexes-backtest` is already a dependency — it is, per this crate's existing `use apexes_backtest::{..}` imports; no new dependency expected)

**Interfaces:**
- Consumes: Task 4's `run_unified_event_loop`, Task 3's live adapters.
- Produces: `run_live_trading`'s existing public signature, unchanged.

- [ ] **Step 1: Re-read `runner.rs` in full**

~826 lines; confirm every piece of today's behavior this task must
preserve: the three `tokio::select!` arms' exact handling (market
event / exchange update / reconcile tick), the monitor-only logging
text for `MarginBreach`/`ExtremeMarketCondition` when auto-execution
is declined (these `tracing::error!` message strings are
operationally meaningful — preserve their wording, don't paraphrase),
`handle_market_event`'s helper function shape (may be superseded
entirely by the unified loop's own internal per-tick logic — if so,
delete it rather than leaving unreachable dead code).

- [ ] **Step 2: Re-point `run_live_trading`**

Construct `OrderManagerExecutor::new(Arc::clone(&order_manager))`,
`LiveLiquidationPolicy::new(config.auto_derisk_live)`,
`LiveSupplementalFillSource::new(exchange_updates, config.reconcile_interval)`
(Task 3), plus `UnifiedLoopConfig` from `LiveTradingConfig`'s existing
fields (`instruments`, `initial_balances`, `risk_free_return`,
`risk_checker` resolved from the registry exactly as today,
`margin_tiers`, `market_condition_config`). Call
`apexes_backtest::unified_loop::run_unified_event_loop` with
`clock: BacktestClock::new(ReplaySpeed::AsFastAsPossible)` (matching
today's comment on why live trading needs no artificial pacing) and
`latency_model: None` (live trading has no artificial submission
delay concept, matching today).

Map `UnifiedLoopReport`'s output fields to `LiveTradingReport`'s
existing `{overall, trades}` shape (or, if the unified loop's report
type IS `LiveTradingReport`-compatible/identical in shape to
`BacktestReport`, consider whether `LiveTradingReport` itself should
just become a type alias for the same report type — only do this if
it doesn't change `LiveTradingReport`'s existing public field names/
types that downstream callers rely on; confirm via `grep -rn LiveTradingReport crates/ --include=*.rs` that no external crate pattern-matches on it in a way a type-alias would break, which it wouldn't since aliases are transparent, but confirm field names match exactly before doing this simplification).

- [ ] **Step 3: Run every existing test**

Run: `cargo test -p apexes-live-trading -- --test-threads=1`
Expected: PASS, every existing unit test unchanged; the `#[ignore]`d
Postgres-backed integration tests in
`tests/live_trading_integration.rs` must still compile cleanly (run
them too if a live Postgres is available in this environment: `cargo test -p apexes-live-trading --test live_trading_integration -- --ignored --test-threads=1`).

Run: `cargo build --workspace && cargo clippy -p apexes-live-trading --all-targets -- -D warnings`
Expected: 0 errors, 0 warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-live-trading
git commit -S -s -m "refactor(apexes-live-trading): unify run_live_trading onto the shared event-loop core"
```

---

### Task 6: Full-workspace verification checkpoint

**Files:** none (verification-only task).

**Interfaces:** none.

- [ ] **Step 1: Run the complete verification suite**

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo test --workspace -- --test-threads=1`
Expected: 0 failures. Record the exact passed/ignored counts (via
`grep "test result:"` summed across every suite, per this project's
established convention of never estimating these numbers) for
comparison against the pre-this-plan baseline — the total pass count
should be equal to or greater than baseline (greater if any new tests
from Tasks 1-3/5 were added), never lower.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 2: Grep for leftover placeholders and dead code**

Run: `grep -rn "TODO\|FIXME\|todo!\|unimplemented!" crates/apexes-strategy/src/order_executor.rs crates/apexes-backtest/src/order_executor_adapter.rs crates/apexes-backtest/src/unified_loop.rs crates/apexes-live-trading/src/order_executor_adapter.rs crates/apexes-live-trading/src/runner.rs`
Expected: no matches.

Run: `grep -rn "run_event_loop\|EventLoopConfig" crates/ --include=*.rs`
Expected: no matches anywhere in the workspace (confirms the old
implementation was genuinely removed, not left as unreachable dead
code alongside the new one).

- [ ] **Step 3: Commit** (only if Step 1/2 required any fixes; otherwise this task produces no diff and can be skipped without a commit)

```bash
git add -A
git commit -S -s -m "chore: full-workspace verification checkpoint for trading-loop unification plan"
```

---

### Task 7: Final whole-branch code review and `docs/FEATURES.md` update

**Files:**
- Modify: `docs/FEATURES.md`

**Interfaces:** none (documentation/review-only task).

- [ ] **Step 1: Run the complete verification suite one final time**

Run: `cargo build --workspace`, `cargo test --workspace -- --test-threads=1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`.
Expected: all clean, matching this project's established
multi-sub-project-completion precedent.

- [ ] **Step 2: Escalate to a whole-branch review**

Per this project's established precedent, dispatch an independent
reviewer over the ENTIRE branch diff for this plan (base: the commit
before Task 1 started, head: Task 5's final commit). Pay particular
attention to:

- **`LiquidationPolicy::should_auto_execute(MarginBreach)` is
  unconditionally `false` in the live implementation** — re-derive
  this independently by reading `LiveLiquidationPolicy`'s actual code,
  not just this plan's description of it; confirm no code path lets
  `auto_derisk_live` or any other config influence this specific
  reason. This is the single most safety-critical property in this
  entire plan.
- **Every existing test's assertions are byte-for-byte unchanged** —
  diff every modified test file against its pre-plan version and
  confirm only non-assertion lines changed (imports, helper-function
  calls adapted to new construction patterns) — any changed
  `assert_eq!`/`assert!` value is a red flag requiring justification.
- **`OrderExecutor::open_orders()`'s clone-per-call behavior doesn't
  silently change strategy-visible semantics** — confirm
  `StrategyContext::open_orders` still reflects the SAME data a
  strategy would have seen pre-refactor at the same point in the same
  tick (order of operations matters: is `open_orders()` called before
  or after the current tick's own fills are applied? Compare directly
  against `event_loop.rs`'s/`runner.rs`'s pre-refactor call ordering).
- **The unified loop's two `tokio::select!` arms don't introduce a
  starvation or ordering bug** relative to today's three-arm
  `run_live_trading` `select!` (verify `SupplementalFillSource`'s
  internal merge of `exchange_updates`+`reconcile` doesn't change
  which of the two is preferred when both are ready simultaneously,
  relative to today's explicit three-arm `select!`'s own fairness
  behavior — `tokio::select!` is pseudo-random among ready arms by
  default, so confirm this property is preserved through the merge,
  not accidentally made deterministic/biased).
- **`apexes-backtest`'s and `apexes-paper`'s crates still build/pass
  with zero import of anything from `apexes-live-trading`** (dependency
  direction: `apexes-live-trading` depends on `apexes-backtest` for the
  shared unified loop, never the reverse — confirm via
  `cargo tree -p apexes-backtest -i apexes-live-trading` returning
  nothing).
- **No behavior change for any pre-existing spot-only /
  no-risk-checker / no-market-condition-config caller** — this plan is
  a pure structural refactor; every `None`/empty-map default path must
  produce byte-identical `BacktestReport`/`LiveTradingReport` output
  to before this plan, for the same fixed input sequence (spot-check
  this against at least one pre-existing deterministic backtest test's
  expected numeric output, if any such test exists).

Fix any findings, get a fix-verification re-review, and only then
consider this plan complete.

- [ ] **Step 3: Update `docs/FEATURES.md`**

This plan is prerequisite infrastructure for row `9.3`'s sub-project
`#13c`, not a roadmap row of its own — it does NOT get its own row or
status change in `docs/FEATURES.md`. Instead, update the "Current
active work" narrative paragraph (the same paragraph Tasks like
`#13a`/`#13b` appended to) with a short note: the event-loop
implementation `run_backtest`/`run_paper_trading`/`run_live_trading`
previously each drove was unified into a single shared core ahead of
starting `#13c`, via three new `apexes-strategy` traits
(`OrderExecutor`/`LiquidationPolicy`/`SupplementalFillSource`) — no
roadmap row's Status column changes as a result of this plan.

Recompute the Summary counts table via actual
`grep -c "✅ Done"`/`grep -c "🔲 Not Started"` (per this project's
explicit standing instruction: NEVER estimate these counts, always
recompute them from the real table contents) — subtract 2 from each
raw count (the legend row near the top of the file, AND the Summary
table's own count row). Confirm the counts are UNCHANGED from before
this plan (since no row's status changed) — if they differ, something
else in the file was accidentally touched; investigate before
committing.

- [ ] **Step 4: Commit**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: note trading-loop unification (pre-#13c prerequisite) in FEATURES.md"
```
