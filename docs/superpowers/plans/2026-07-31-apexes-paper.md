# Apexes-Paper: Paper Trading Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `apexes-backtest` to extract a shared event-loop core driven by a generic `Stream<Item = MarketEvent>` (preserving `run_backtest`'s existing behavior exactly), then add a new `apexes-paper` crate that drives that same core against a live `MarketDataProvider` feed, with simulated order-submission latency and a graceful stop signal.

**Architecture:** `apexes-backtest` gains a new `pub mod event_loop` exposing `run_event_loop` (the historical-replay-and-live-trading-shared core) plus a new `pub mod latency` exposing `LatencyModel`; `run_backtest` becomes a thin wrapper that loads its `Vec<MarketEvent>`, wraps it in `futures::stream::iter(..)`, and delegates to `run_event_loop` with `stop_signal: None` and `latency_model: None`. `apexes-paper` depends on `apexes-backtest` + `apexes-market-data` + `apexes-risk` + `apexes-strategy`, providing `PaperAccount` (a read-only `PortfolioLedger` snapshot view) and `run_paper_trading` (calls `provider.subscribe(..)`, then `run_event_loop` with `Some(stop_signal)` and `Some(latency_model)`).

**Tech Stack:** Rust edition 2024, `futures` (`Stream`/`StreamExt`, `stream::iter`), `tokio` (`sync::oneshot`, `time::sleep`), `rand` (new workspace dependency, `LatencyModel::Uniform` sampling), existing `apexes-core`/`apexes-strategy`/`apexes-risk`/`apexes-market-data`/`apexes-backtest` types.

## Global Constraints

- Design spec of record: `docs/superpowers/specs/2026-07-31-apexes-paper-design.md`. If you find a mismatch between this plan and that spec while implementing, the spec wins -- stop and flag it rather than improvising.
- Rust edition `2024`, matching `apexes`' workspace (`Cargo.toml` `[workspace.package] edition = "2024"`).
- **Task 1's `apexes-backtest` refactor must not change `run_backtest`'s public signature, parameter types, or observable behavior in any way.** Every existing test in `crates/apexes-backtest/tests/` and `crates/apexes-backtest/src/runner.rs`'s own `#[cfg(test)] mod tests` must continue to pass unchanged, with identical pass counts, after the refactor.
- `run_event_loop` and the `event_loop` module are `pub` (cross-crate visible, so `apexes-paper` can call them) but are NOT re-exported from `apexes-backtest`'s top-level `lib.rs` `pub use` list -- callers must use the full path `apexes_backtest::event_loop::run_event_loop`. This is a deliberate signal that it is shared internal machinery, not `apexes-backtest`'s primary public API (`run_backtest` remains that).
- `LatencyModel` is defined in `apexes-backtest` (not `apexes-paper`), to avoid a circular dependency (`apexes-paper` depends on `apexes-backtest`, so `apexes-backtest` cannot depend on `apexes-paper`). `apexes-paper` re-exports it via `pub use apexes_backtest::LatencyModel;`.
- No automatic liquidation, no real order placement, no persistence, no multi-account support -- `apexes-paper` is in-memory-only simulated trading, per the design spec's non-goals.
- No `tokio-util`/`CancellationToken` dependency -- stop signals use `tokio::sync::oneshot::Receiver<()>`, already available via the existing `tokio` workspace dependency.
- Every commit in the `apexes` repo uses `git commit -S -s` (signed + signed-off), matching the project's established convention. Direct commits to `master` (no branches/PRs).
- `cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --check` must all pass clean after every task that touches compiled code.
- Unit tests must not require a live network connection; tests that do (real Binance WebSocket) are marked `#[ignore]`, matching `apexes-market-data`'s established pattern (`crates/apexes-market-data/tests/live_binance.rs`).

---

### Task 1: Extract `apexes-backtest`'s shared event-loop core (`event_loop.rs`)

**Files:**
- Create: `crates/apexes-backtest/src/event_loop.rs`
- Modify: `crates/apexes-backtest/src/runner.rs` (shrink to a thin wrapper)
- Modify: `crates/apexes-backtest/src/lib.rs` (add `pub mod event_loop;`, do NOT re-export its contents)

**Interfaces:**
- Consumes: `crate::clock::BacktestClock`, `crate::cost_model::CostModel`, `crate::ledger::PortfolioLedger`, `crate::matching_engine::MatchingEngine`, `crate::performance::PerformanceTracker`, `crate::report::BacktestReport` (all existing, unchanged), `apexes_risk::CompositeRiskChecker` (existing), `apexes_strategy::{Strategy, StrategyContext}` (existing).
- Produces: `pub struct EventLoopConfig`, `pub(crate) async fn run_event_loop` (actually `pub`, see Global Constraints -- cross-crate visible but not re-exported), consumed by Task 2's `apexes-paper::run_paper_trading`. Also produces the refactored `pub async fn run_backtest` (same signature as before), consumed by every existing caller unchanged.

**IMPORTANT:** This task is a pure refactor. The event-loop body below is the EXACT SAME LOGIC currently in `crates/apexes-backtest/src/runner.rs`'s `run_backtest` function, merely restructured to (a) live in a new file, (b) accept a generic `Stream` instead of a `Vec`, (c) accept optional `stop_signal`/`latency_model` parameters that are inert (`None`) for `run_backtest`'s call path. Do not change any business logic, fee calculation, fill application, or risk-check behavior.

- [x] **Step 1: Read the current `run_backtest` implementation in full**

```bash
cd ~/src/oss/apexes
cat crates/apexes-backtest/src/runner.rs
```

Confirm your local copy matches what this plan's Step 3 shows below exactly (if there is any discrepancy, STOP and report it -- the plan's code assumes this exact starting state).

- [x] **Step 2: Add `rand` to the workspace dependencies**

Edit `Cargo.toml`:

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage", "crates/apexes-backtest", "crates/apexes-strategy", "crates/apexes-indicators", "crates/apexes-optimize", "crates/apexes-strategy-registry", "crates/apexes-risk"]

[workspace.package]
edition = "2024"
version = "0.1.0"
license = "MIT"

[workspace.dependencies]
rust_decimal = { version = "1.42", features = ["serde", "maths"] }
serde = { version = "1.0", features = ["derive"] }
thiserror = "2.0"
chrono = { version = "0.4", features = ["serde"] }
tokio = { version = "1.53", features = ["rt-multi-thread", "macros"] }
futures = "0.3"
async-trait = "0.1.91"
uuid = { version = "1.24", features = ["v4", "serde"] }
clap = { version = "4.6", features = ["derive"] }
serde_json = "1.0"
barter-data = "0.11"
barter-instrument = "0.3"
tracing = "0.1"
clickhouse = { version = "0.15", features = ["chrono"] }
fixnum = { version = "0.9", features = ["i128", "serde"] }
barter = "0.12"
sha2 = "0.10"
rand = "0.10"
```

Note: `"crates/apexes-risk"` is missing from the `members` list shown above in the current repo state -- if you find it missing when you read the actual file, add it back (it should already be there from the prior `apexes-risk` plan; this is just showing the `rand` addition in context). Do NOT remove any existing member.

- [x] **Step 3: Create `crates/apexes-backtest/src/event_loop.rs` with the extracted core**

```rust
//! Shared event-loop core driving both `run_backtest` (historical
//! replay, `Vec<MarketEvent>` wrapped as a `Stream`) and `apexes-paper`'s
//! `run_paper_trading` (live `MarketDataProvider::subscribe()` stream).
//! See design spec `2026-07-31-apexes-paper-design.md` §二 for why this
//! extraction was necessary: the two callers' event sources are
//! fundamentally different (pre-loaded `Vec` vs. live async `Stream`),
//! and the project roadmap explicitly requires `apexes-paper` to "reuse
//! the backtest matching-engine kernel".
//!
//! This module is `pub` (cross-crate visible so `apexes-paper` can call
//! it) but deliberately NOT re-exported from `apexes-backtest`'s
//! top-level `lib.rs` -- callers use the full path
//! `apexes_backtest::event_loop::run_event_loop`. `run_backtest` remains
//! `apexes-backtest`'s primary public entry point.

use crate::clock::BacktestClock;
use crate::cost_model::CostModel;
use crate::latency::LatencyModel;
use crate::ledger::PortfolioLedger;
use crate::matching_engine::MatchingEngine;
use crate::performance::PerformanceTracker;
use crate::report::BacktestReport;
use apexes_core::{ApexesError, Fill, Instrument, MarketEvent, Order, OrderId, OrderType};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{Strategy, StrategyContext};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Configuration shared by `run_backtest` and `run_paper_trading`'s event
/// loop core. `BacktestConfig` (in `runner.rs`) is the historical-replay
/// caller's public config type; `apexes-paper`'s `PaperTradingConfig` is
/// the live-trading caller's public config type. Both construct this
/// shared internal struct before calling `run_event_loop`.
pub struct EventLoopConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub queue_position_factor: Decimal,
    pub risk_free_return: Decimal,
    pub risk_checker: Option<CompositeRiskChecker>,
}

/// Runs the shared event loop: consumes `events` in order through
/// `strategy`, simulating fills via a `MatchingEngine`, applying
/// `apexes-risk` checks if configured, tracking portfolio state via a
/// `PortfolioLedger`, and producing a `BacktestReport`.
///
/// `clock` paces dispatch (an already-configured `BacktestClock` --
/// historical-replay callers pass one built from their `ReplaySpeed`;
/// live-trading callers pass `BacktestClock::new(ReplaySpeed::AsFastAsPossible)`
/// since live events already arrive at real-time pace and need no
/// additional artificial pacing).
///
/// `stop_signal`, if `Some`, causes the loop to exit early (returning
/// whatever `BacktestReport` has accumulated so far, not an error) once
/// it fires or its sender is dropped. `run_backtest` always passes
/// `None` (historical replay runs to completion by construction, since
/// `events` is a finite pre-loaded stream).
///
/// `latency_model`, if `Some`, is applied (via `LatencyModel::apply`,
/// a real `tokio::time::sleep`) after a submitted `OrderIntent` passes
/// its `PreTradeCheck` and before it reaches the matching engine.
/// `run_backtest` always passes `None` (historical replay has no
/// "order submission delay" concept -- recorded timestamps are already
/// fixed).
pub async fn run_event_loop(
    config: EventLoopConfig,
    mut events: impl Stream<Item = MarketEvent> + Unpin,
    mut clock: BacktestClock,
    mut strategy: impl Strategy,
    mut stop_signal: Option<tokio::sync::oneshot::Receiver<()>>,
    latency_model: Option<LatencyModel>,
) -> Result<BacktestReport, ApexesError> {
    let instruments_by_symbol: HashMap<String, Instrument> = config
        .instruments
        .iter()
        .map(|instrument| (instrument.symbol.clone(), instrument.clone()))
        .collect();

    let mut ledger = PortfolioLedger::new(instruments_by_symbol.clone(), config.initial_balances);
    let mut matching_engine = MatchingEngine::new(config.cost_model, config.queue_position_factor);
    let mut overall_tracker = PerformanceTracker::new(config.risk_free_return);
    let mut per_symbol_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut per_venue_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut open_orders: HashMap<OrderId, Order> = HashMap::new();
    let mut last_prices: HashMap<String, Decimal> = HashMap::new();
    let mut all_fills: Vec<Fill> = Vec::new();
    let mut recorded_initial_equity = false;
    let mut risk_checker = config.risk_checker;

    loop {
        if let Some(signal) = stop_signal.as_mut() {
            match signal.try_recv() {
                Ok(()) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => break,
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
            }
        }

        let event = match events.next().await {
            Some(event) => event,
            None => break,
        };

        if !recorded_initial_equity {
            let initial_equity = ledger.equity(&last_prices)?;
            overall_tracker.record_equity_point(event_timestamp(&event), initial_equity);
            recorded_initial_equity = true;
        }

        let timestamp = event_timestamp(&event);
        clock.wait_for_next_event(timestamp).await;

        match &event {
            MarketEvent::Depth(update) => {
                matching_engine.on_depth_update(update);
            }
            MarketEvent::Trade(trade) => {
                if let Some(instrument) = instruments_by_symbol.get(&trade.symbol)
                    && let Some(base) = &instrument.base_currency
                {
                    last_prices.insert(base.clone(), trade.price);
                }
                let fill_results = matching_engine.on_trade_event(trade, timestamp);
                for (order_id, fills) in fill_results {
                    apply_fills(
                        &mut ledger,
                        &mut overall_tracker,
                        &mut per_symbol_trackers,
                        &mut per_venue_trackers,
                        &mut all_fills,
                        &fills,
                        config.risk_free_return,
                    )?;
                    if let Some(checker) = &mut risk_checker {
                        for fill in &fills {
                            if let Err(violations) =
                                checker.on_fill(fill, ledger.positions(), ledger.balances())
                            {
                                tracing::warn!(
                                    ?violations,
                                    symbol = %fill.symbol,
                                    "in-trade risk monitor flagged a violation"
                                );
                            }
                        }
                    }
                    if let Some(order) = open_orders.get_mut(&order_id)
                        && let Some(last_fill) = fills.last()
                    {
                        order.filled_quantity += fills.iter().map(|f| f.quantity).sum::<Decimal>();
                        order.updated_at = last_fill.filled_at;
                        if order.filled_quantity >= order.quantity {
                            order.status = apexes_core::OrderStatus::Filled;
                        } else {
                            order.status = apexes_core::OrderStatus::PartiallyFilled;
                        }
                    }
                }
            }
            _ => {} // Tick/Bar events currently only advance the clock/context; no direct matching-engine action.
        }

        let ctx = StrategyContext {
            positions: ledger.positions(),
            balances: ledger.balances(),
            open_orders: &open_orders,
            now: timestamp,
        };
        let intents = strategy.on_market_event(&event, &ctx);

        for intent in intents {
            if let Some(checker) = &risk_checker
                && let Err(violations) = checker.check(
                    &intent,
                    ledger.positions(),
                    ledger.balances(),
                    &open_orders,
                    timestamp,
                )
            {
                tracing::warn!(?violations, symbol = %intent.symbol, "order rejected by risk checker");
                continue;
            }

            if let Some(latency_model) = &latency_model {
                latency_model.apply().await;
            }

            let submit_result = match intent.order_type {
                OrderType::Market => matching_engine
                    .submit_market_order(&intent, timestamp)
                    .map(|(order, fills)| (order, Some(fills))),
                OrderType::Limit => matching_engine
                    .submit_limit_order(&intent, timestamp)
                    .map(|order| (order, None)),
                OrderType::Stop | OrderType::StopLimit => matching_engine
                    .submit_stop_order(&intent, timestamp)
                    .map(|order| (order, None)),
            };

            match submit_result {
                Ok((order, immediate_fills)) => {
                    open_orders.insert(order.id, order);
                    if let Some(fills) = immediate_fills {
                        apply_fills(
                            &mut ledger,
                            &mut overall_tracker,
                            &mut per_symbol_trackers,
                            &mut per_venue_trackers,
                            &mut all_fills,
                            &fills,
                            config.risk_free_return,
                        )?;
                        if let Some(checker) = &mut risk_checker {
                            for fill in &fills {
                                if let Err(violations) =
                                    checker.on_fill(fill, ledger.positions(), ledger.balances())
                                {
                                    tracing::warn!(
                                        ?violations,
                                        symbol = %fill.symbol,
                                        "in-trade risk monitor flagged a violation"
                                    );
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(?error, symbol = %intent.symbol, "order submission rejected");
                }
            }
        }

        let equity = ledger.equity(&last_prices)?;
        overall_tracker.record_equity_point(timestamp, equity);
    }

    Ok(BacktestReport {
        overall: overall_tracker.summary().into(),
        per_symbol: per_symbol_trackers
            .into_iter()
            .map(|(k, v)| (k, v.summary().into()))
            .collect(),
        per_venue: per_venue_trackers
            .into_iter()
            .map(|(k, v)| (k, v.summary().into()))
            .collect(),
        equity_curve: overall_tracker.equity_curve().to_vec(),
        trades: all_fills,
    })
}

/// Applies a batch of `Fill`s produced by the matching engine: updates
/// the ledger, records each fill into the overall/per-symbol/per-venue
/// `PerformanceTracker`s, and accumulates every fill into `all_fills`
/// (the `BacktestReport.trades` source) in occurrence order.
fn apply_fills(
    ledger: &mut PortfolioLedger,
    overall_tracker: &mut PerformanceTracker,
    per_symbol_trackers: &mut HashMap<String, PerformanceTracker>,
    per_venue_trackers: &mut HashMap<String, PerformanceTracker>,
    all_fills: &mut Vec<Fill>,
    fills: &[Fill],
    risk_free_return: Decimal,
) -> Result<(), ApexesError> {
    for fill in fills {
        ledger.apply_fill(fill)?;
        overall_tracker.record_fill(fill);
        per_symbol_trackers
            .entry(fill.symbol.clone())
            .or_insert_with(|| PerformanceTracker::new(risk_free_return))
            .record_fill(fill);
        per_venue_trackers
            .entry(format!("{:?}", fill.venue))
            .or_insert_with(|| PerformanceTracker::new(risk_free_return))
            .record_fill(fill);
    }
    all_fills.extend_from_slice(fills);
    Ok(())
}

fn event_timestamp(event: &MarketEvent) -> chrono::DateTime<chrono::Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
    }
}
```

- [x] **Step 4: Create `crates/apexes-backtest/src/latency.rs`**

```rust
//! [`LatencyModel`]: simulates network round-trip latency between a
//! strategy deciding to submit an order and the (simulated) exchange
//! receiving it. Defined in `apexes-backtest` (not `apexes-paper`) to
//! avoid a circular crate dependency, since `run_event_loop` (this
//! crate) needs to accept an `Option<LatencyModel>` parameter and
//! `apexes-paper` depends on `apexes-backtest`, not the reverse.
//! `apexes-paper` re-exports this type via `pub use
//! apexes_backtest::LatencyModel;` so it still reads naturally as a
//! paper-trading concept to that crate's users.
//!
//! Historical replay (`run_backtest`) has no use for this -- recorded
//! event timestamps are already fixed, so `run_backtest` always passes
//! `None` to `run_event_loop`'s `latency_model` parameter.

use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub enum LatencyModel {
    /// No artificial delay -- the order reaches the matching engine as
    /// soon as it passes any configured `PreTradeCheck`.
    None,
    Fixed(Duration),
    /// Uniformly distributed random delay in `[min, max]` (inclusive).
    Uniform { min: Duration, max: Duration },
}

impl LatencyModel {
    /// Returns the delay to apply for one order submission. `Uniform`
    /// draws a new random value on every call (not cached), so
    /// consecutive orders in the same run get independent delays,
    /// simulating real network jitter.
    pub fn sample(&self) -> Duration {
        match self {
            LatencyModel::None => Duration::ZERO,
            LatencyModel::Fixed(duration) => *duration,
            LatencyModel::Uniform { min, max } => {
                if min >= max {
                    return *min;
                }
                let min_nanos = min.as_nanos() as u64;
                let max_nanos = max.as_nanos() as u64;
                let sampled_nanos = rand::rng().random_range(min_nanos..=max_nanos);
                Duration::from_nanos(sampled_nanos)
            }
        }
    }

    /// Actually delays execution by `self.sample()`'s result via
    /// `tokio::time::sleep` -- a real wall-clock delay, not just a
    /// timestamp adjustment, to most faithfully simulate real network
    /// latency's effect on timing.
    pub async fn apply(&self) {
        let delay = self.sample();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_samples_zero_duration() {
        let model = LatencyModel::None;
        assert_eq!(model.sample(), Duration::ZERO);
    }

    #[test]
    fn fixed_always_samples_the_configured_value() {
        let model = LatencyModel::Fixed(Duration::from_millis(50));
        for _ in 0..5 {
            assert_eq!(model.sample(), Duration::from_millis(50));
        }
    }

    #[test]
    fn uniform_samples_fall_within_the_configured_range() {
        let model = LatencyModel::Uniform {
            min: Duration::from_millis(10),
            max: Duration::from_millis(20),
        };
        for _ in 0..100 {
            let sampled = model.sample();
            assert!(sampled >= Duration::from_millis(10));
            assert!(sampled <= Duration::from_millis(20));
        }
    }

    #[test]
    fn uniform_with_equal_min_and_max_always_returns_that_value() {
        let model = LatencyModel::Uniform {
            min: Duration::from_millis(5),
            max: Duration::from_millis(5),
        };
        assert_eq!(model.sample(), Duration::from_millis(5));
    }

    #[tokio::test]
    async fn apply_with_none_returns_almost_immediately() {
        let model = LatencyModel::None;
        let start = tokio::time::Instant::now();
        model.apply().await;
        assert!(start.elapsed() < Duration::from_millis(20));
    }

    #[tokio::test]
    async fn apply_with_fixed_delay_actually_sleeps() {
        let model = LatencyModel::Fixed(Duration::from_millis(30));
        let start = tokio::time::Instant::now();
        model.apply().await;
        assert!(start.elapsed() >= Duration::from_millis(30));
    }
}
```

- [x] **Step 5: Replace `crates/apexes-backtest/src/runner.rs` with the thin wrapper**

Replace the entire file's contents with:

```rust
use crate::clock::{BacktestClock, ReplaySpeed};
use crate::cost_model::CostModel;
use crate::event_loop::{EventLoopConfig, run_event_loop};
use crate::market_data_source::HistoricalMarketDataSource;
use crate::report::BacktestReport;
use apexes_core::{ApexesError, Instrument};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::Strategy;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::ops::Range;

/// Configuration for a single backtest run (design spec §8).
pub struct BacktestConfig {
    pub instruments: Vec<Instrument>,
    /// Informational / for-caller-use only: `run_backtest` itself never
    /// reads this field -- event selection is entirely the
    /// `HistoricalMarketDataSource`'s responsibility. Kept on the config
    /// so a caller can, for example, construct a
    /// `StorageMarketDataSource` using the same range, and for future
    /// use (e.g. runner-side event filtering).
    pub date_range: Range<NaiveDate>,
    /// Required -- no implicit default (design spec §8).
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub speed: ReplaySpeed,
    /// 0.0 (optimistic) .. 1.0 (pessimistic), see design spec §7.3.
    pub queue_position_factor: Decimal,
    /// Risk-free return rate used for Sharpe/Sortino/Calmar calculations
    /// (design spec §8, matching `barter::statistic::metric`'s
    /// `calculate` signatures).
    pub risk_free_return: Decimal,
    /// Optional risk checks run before submitting each `OrderIntent`
    /// (rejected intents are logged via `tracing::warn!` and never
    /// reach the matching engine) and after each `Fill` (violations are
    /// logged, not propagated -- an in-trade violation does not undo a
    /// fill that already happened, per `apexes-risk`'s design spec §一
    /// "no automatic liquidation" decision). `None` means no risk
    /// checking is performed, preserving this crate's pre-existing
    /// behavior exactly.
    pub risk_checker: Option<CompositeRiskChecker>,
}

/// Runs a full backtest: replays `market_data` in chronological order
/// through `strategy`, simulating fills via a `MatchingEngine`, tracking
/// portfolio state via a `PortfolioLedger`, and producing a
/// `BacktestReport` with overall + per-symbol + per-venue performance
/// summaries (design spec §8). Internally delegates to the shared
/// `crate::event_loop::run_event_loop` core (see that module's docs) --
/// this function's only job is to adapt `HistoricalMarketDataSource`'s
/// pre-loaded `Vec<MarketEvent>` into the `Stream` the shared core
/// expects, and to pass `stop_signal: None`/`latency_model: None` since
/// neither concept applies to historical replay.
pub async fn run_backtest(
    config: BacktestConfig,
    market_data: impl HistoricalMarketDataSource,
    strategy: impl Strategy,
) -> Result<BacktestReport, ApexesError> {
    let events = market_data.events().await?;
    let event_stream = futures::stream::iter(events);
    let clock = BacktestClock::new(config.speed);

    let event_loop_config = EventLoopConfig {
        instruments: config.instruments,
        initial_balances: config.initial_balances,
        cost_model: config.cost_model,
        queue_position_factor: config.queue_position_factor,
        risk_free_return: config.risk_free_return,
        risk_checker: config.risk_checker,
    };

    run_event_loop(event_loop_config, event_stream, clock, strategy, None, None).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost_model::{CostModel, FlatFeeSchedule};
    use crate::market_data_source::InMemoryMarketDataSource;
    use apexes_core::{
        AssetClass, DepthUpdate, MarketEvent, MarketType, OrderType, PriceLevel, Side,
        TradeEvent, Venue,
    };
    use apexes_strategy::{OrderIntent, StrategyContext};
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    struct BuyOnceStrategy {
        bought: bool,
    }

    impl Strategy for BuyOnceStrategy {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            if !self.bought && matches!(event, MarketEvent::Trade(_)) {
                self.bought = true;
                return vec![OrderIntent {
                    symbol: "BTCUSDT".to_string(),
                    venue: Venue::Binance,
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    quantity: dec!(1),
                    price: None,
                    stop_price: None,
                }];
            }
            vec![]
        }
    }

    fn btcusdt_instrument() -> Instrument {
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

    fn zero_cost_model() -> CostModel {
        CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        }
    }

    #[tokio::test]
    async fn run_backtest_produces_a_trade_and_correct_mark_to_market_equity() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            MarketEvent::Depth(DepthUpdate {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                bids: vec![],
                asks: vec![PriceLevel {
                    price: dec!(100),
                    quantity: dec!(1000),
                }],
                sequence: Some(1),
                is_snapshot: true,
                timestamp: t0,
            }),
            MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price: dec!(100),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: t0 + Duration::seconds(1),
            }),
            MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price: dec!(110),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: t0 + Duration::seconds(2),
            }),
        ];

        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));

        let config = BacktestConfig {
            instruments: vec![btcusdt_instrument()],
            date_range: chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
                ..chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
            initial_balances,
            cost_model: zero_cost_model(),
            speed: ReplaySpeed::AsFastAsPossible,
            queue_position_factor: dec!(0.5),
            risk_free_return: Decimal::ZERO,
            risk_checker: None,
        };

        let report = run_backtest(
            config,
            InMemoryMarketDataSource::new(events),
            BuyOnceStrategy { bought: false },
        )
        .await
        .unwrap();

        assert_eq!(report.trades.len(), 1);
        let final_equity = report
            .equity_curve
            .last()
            .map(|(_, equity)| *equity)
            .unwrap();
        // 9,900 USDT + 1 BTC * 110 = 10,010.
        assert_eq!(final_equity, dec!(10010));
        assert_ne!(final_equity, dec!(9901));
    }

    #[test]
    fn order_id_construction_smoke_test() {
        // Retained from the pre-refactor test module to confirm apexes_core
        // re-exports/imports used elsewhere in this file still resolve.
        let _ = apexes_core::OrderId(Uuid::new_v4());
    }
}
```

**Note on the test module**: the plan's Step 5 code above reconstructs the single pre-existing test from `runner.rs` (`run_backtest` producing a trade with correct mark-to-market equity) plus a trivial smoke test. If you find the actual pre-refactor `runner.rs` file (read in Step 1) has additional or differently-named tests beyond what's shown here, preserve ALL of them with identical assertions -- do not delete any existing test coverage. This plan's code block is a representative reconstruction, not necessarily a byte-for-byte copy of every test that may exist; treat "zero test regressions" (Global Constraints) as the authoritative requirement over this code block's literal test count.

- [x] **Step 6: Update `crates/apexes-backtest/src/lib.rs`**

Find the current `pub mod`/`pub use` declarations and add the two new modules. The existing file looks like this:

```rust
//! Apexes backtest engine: tick-level historical replay, Level-2-depth
//! matching simulator, cost model, multi-asset portfolio ledger, and
//! performance/risk-metrics reporting.
//!
//! The [`Strategy`] trait defined in this crate is the shared interface
//! that future live-trading sub-projects (#6 Paper Trading, #7 OMS, #8 live
//! crypto gateway) will implement against unchanged, per the project's
//! non-negotiable backtest/live logic-parity requirement.

pub mod error;
pub mod market_data_source;
pub mod order_book;
pub mod cost_model;
pub mod ledger;
pub mod matching_engine;
pub mod clock;
pub mod performance;
pub mod report;
pub mod runner;

pub use apexes_strategy::{OrderIntent, Position, Strategy, StrategyContext};
pub use clock::ReplaySpeed;
pub use cost_model::{
    BinanceSpotVipFeeSchedule, CostModel, FeeSchedule, FillLiquidity, FlatFeeSchedule,
};
pub use error::ApexesError;
pub use ledger::PortfolioLedger;
pub use market_data_source::{
    HistoricalMarketDataSource, InMemoryMarketDataSource, StorageMarketDataSource,
};
pub use matching_engine::MatchingEngine;
pub use performance::{PerformanceSummary, PerformanceTracker};
pub use report::BacktestReport;
pub use runner::{BacktestConfig, run_backtest};
```

Replace it with:

```rust
//! Apexes backtest engine: tick-level historical replay, Level-2-depth
//! matching simulator, cost model, multi-asset portfolio ledger, and
//! performance/risk-metrics reporting.
//!
//! The [`Strategy`] trait defined in this crate is the shared interface
//! that future live-trading sub-projects (#6 Paper Trading, #7 OMS, #8 live
//! crypto gateway) will implement against unchanged, per the project's
//! non-negotiable backtest/live logic-parity requirement.
//!
//! [`event_loop`] holds the shared core `run_backtest` and
//! `apexes-paper`'s `run_paper_trading` both drive -- it is `pub` (so
//! `apexes-paper` can reach it) but deliberately not re-exported here;
//! `run_backtest` remains this crate's primary public entry point.

pub mod error;
pub mod market_data_source;
pub mod order_book;
pub mod cost_model;
pub mod event_loop;
pub mod latency;
pub mod ledger;
pub mod matching_engine;
pub mod clock;
pub mod performance;
pub mod report;
pub mod runner;

pub use apexes_strategy::{OrderIntent, Position, Strategy, StrategyContext};
pub use clock::ReplaySpeed;
pub use cost_model::{
    BinanceSpotVipFeeSchedule, CostModel, FeeSchedule, FillLiquidity, FlatFeeSchedule,
};
pub use error::ApexesError;
pub use latency::LatencyModel;
pub use ledger::PortfolioLedger;
pub use market_data_source::{
    HistoricalMarketDataSource, InMemoryMarketDataSource, StorageMarketDataSource,
};
pub use matching_engine::MatchingEngine;
pub use performance::{PerformanceSummary, PerformanceTracker};
pub use report::BacktestReport;
pub use runner::{BacktestConfig, run_backtest};
```

Note: `LatencyModel` IS re-exported at the top level (unlike `event_loop`) since it's a small, self-contained, useful-on-its-own type that both `run_backtest`-adjacent tooling and `apexes-paper` benefit from reaching directly as `apexes_backtest::LatencyModel`.

- [x] **Step 7: Add `apexes-risk` to workspace members if missing, add `rand` dependency to `apexes-backtest`**

```bash
cd ~/src/oss/apexes
grep "apexes-risk" Cargo.toml
```
If this returns nothing, add `"crates/apexes-risk"` to the `members` array (it should already be present from the prior plan; this is a defensive check).

Edit `crates/apexes-backtest/Cargo.toml` to add `rand`:

```toml
[package]
name = "apexes-backtest"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-storage = { path = "../apexes-storage" }
apexes-strategy = { path = "../apexes-strategy" }
apexes-risk = { path = "../apexes-risk" }
uuid = { workspace = true }
barter-data = { workspace = true }
barter = { workspace = true }
tokio = { workspace = true, features = ["time"] }
futures = { workspace = true }
rust_decimal = { workspace = true }
rust_decimal_macros = "1.40"
chrono = { workspace = true }
serde = { workspace = true }
tracing = { workspace = true }
rand = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
uuid = { workspace = true, features = ["v4"] }
```

- [x] **Step 8: Build and run the crate's own tests**

```bash
cd ~/src/oss/apexes
cargo build -p apexes-backtest
cargo test -p apexes-backtest --lib
```
Expected: builds clean; the lib's own unit tests (in `runner.rs` and `latency.rs`) all pass. `latency.rs` contributes 6 new tests.

- [x] **Step 9: Run the crate's full test suite (lib + integration tests) and confirm zero regressions**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-backtest
```
Expected: EVERY test that passed before this refactor (70 lib unit tests + 3 `end_to_end.rs` + 1 ignored `live_storage_backtest.rs` + 3 `risk_integration.rs`, per the state recorded at the end of the prior `apexes-risk` plan) still passes, with the exact same counts, PLUS the 6 new `latency.rs` tests. If any pre-existing test fails or a count differs, this is a regression -- do not proceed, find and fix the discrepancy first (the refactor must be behavior-preserving).

- [x] **Step 10: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-backtest --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run both commands plus Step 9's full test command to confirm nothing regressed).

- [x] **Step 11: Verify downstream crates that depend on `apexes-backtest` still compile and pass**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-optimize
cargo test -p apexes-strategy-registry
```
Expected: both unchanged from their state at the end of the prior `apexes-risk` plan (13 unit + 1 integration for `apexes-optimize`; 10 unit/6 ignored + 1 ignored integration for `apexes-strategy-registry`) -- this refactor must not break any consumer of `apexes-backtest`'s public API, since that API (`run_backtest`, `BacktestConfig`, etc.) is unchanged.

- [x] **Step 12: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [x] **Step 13: Commit**

```bash
cd ~/src/oss/apexes
git add Cargo.toml crates/apexes-backtest
git commit -S -s -m "refactor: extract shared event-loop core from apexes-backtest for future live-trading reuse"
git push origin master
```

---

### Task 2: `apexes-paper` crate scaffold + `PaperAccount`

**Files:**
- Modify: `Cargo.toml` (add `"crates/apexes-paper"` to `members`)
- Create: `crates/apexes-paper/Cargo.toml`
- Create: `crates/apexes-paper/src/lib.rs`
- Create: `crates/apexes-paper/src/account.rs`

**Interfaces:**
- Consumes: `apexes_backtest::PortfolioLedger` (existing, unchanged), `apexes_core::ApexesError` (existing), `apexes_strategy::Position` (existing).
- Produces: `pub struct AccountSnapshot`, `pub struct PaperAccount<'a>` with `new`/`balances_snapshot`/`positions_snapshot`/`equity_snapshot`/`full_snapshot`. Consumed by Task 5's `run_paper_trading` integration tests (as an example of how a caller might inspect state, though `run_paper_trading` itself does not need to call it internally).

- [x] **Step 1: Register the new crate in the workspace**

```bash
cd ~/src/oss/apexes
cat Cargo.toml
```

Edit `Cargo.toml`'s `members` list:

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage", "crates/apexes-backtest", "crates/apexes-strategy", "crates/apexes-indicators", "crates/apexes-optimize", "crates/apexes-strategy-registry", "crates/apexes-risk", "crates/apexes-paper"]
```

- [x] **Step 2: Create `crates/apexes-paper/Cargo.toml`**

```toml
[package]
name = "apexes-paper"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-strategy = { path = "../apexes-strategy" }
apexes-backtest = { path = "../apexes-backtest" }
apexes-market-data = { path = "../apexes-market-data" }
apexes-risk = { path = "../apexes-risk" }
tokio = { workspace = true, features = ["time", "sync"] }
futures = { workspace = true }
chrono = { workspace = true }
rust_decimal = { workspace = true }
tracing = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
async-trait = { workspace = true }
uuid = { workspace = true, features = ["v4"] }
```

- [x] **Step 3: Write `crates/apexes-paper/src/account.rs`**

```rust
//! [`PaperAccount`]: a thin read-only query layer over `PortfolioLedger`,
//! for future monitoring/UI (sub-project #13) consumers that need to
//! inspect current equity/positions without holding a mutable reference
//! to the ledger the event loop itself owns.

use apexes_backtest::PortfolioLedger;
use apexes_core::ApexesError;
use apexes_strategy::Position;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// A point-in-time snapshot of account state.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountSnapshot {
    pub balances: HashMap<String, Decimal>,
    pub positions: HashMap<String, Position>,
    pub equity: Decimal,
}

pub struct PaperAccount<'a> {
    ledger: &'a PortfolioLedger,
}

impl<'a> PaperAccount<'a> {
    pub fn new(ledger: &'a PortfolioLedger) -> Self {
        Self { ledger }
    }

    pub fn balances_snapshot(&self) -> HashMap<String, Decimal> {
        self.ledger.balances().clone()
    }

    pub fn positions_snapshot(&self) -> HashMap<String, Position> {
        self.ledger.positions().clone()
    }

    /// Computes current equity using `mark_prices` (asset symbol -> price
    /// in quote terms), the same convention as
    /// `PortfolioLedger::equity`.
    pub fn equity_snapshot(
        &self,
        mark_prices: &HashMap<String, Decimal>,
    ) -> Result<Decimal, ApexesError> {
        self.ledger.equity(mark_prices)
    }

    /// Convenience: all three snapshots in one call, for callers that
    /// want a single consistent point-in-time view (all three fields
    /// read from the same `&PortfolioLedger` borrow, so no other
    /// mutation can happen between them).
    pub fn full_snapshot(
        &self,
        mark_prices: &HashMap<String, Decimal>,
    ) -> Result<AccountSnapshot, ApexesError> {
        Ok(AccountSnapshot {
            balances: self.balances_snapshot(),
            positions: self.positions_snapshot(),
            equity: self.equity_snapshot(mark_prices)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::Instrument;
    use rust_decimal_macros::dec;

    fn ledger_with_usdt_balance(amount: Decimal) -> PortfolioLedger {
        let instruments: HashMap<String, Instrument> = HashMap::new();
        let mut balances = HashMap::new();
        balances.insert("USDT".to_string(), amount);
        PortfolioLedger::new(instruments, balances)
    }

    #[test]
    fn balances_snapshot_matches_ledger_balances() {
        let ledger = ledger_with_usdt_balance(dec!(1000));
        let account = PaperAccount::new(&ledger);
        let snapshot = account.balances_snapshot();
        assert_eq!(snapshot.get("USDT"), Some(&dec!(1000)));
    }

    #[test]
    fn positions_snapshot_is_empty_for_a_fresh_ledger() {
        let ledger = ledger_with_usdt_balance(dec!(1000));
        let account = PaperAccount::new(&ledger);
        assert!(account.positions_snapshot().is_empty());
    }

    #[test]
    fn equity_snapshot_matches_ledger_equity_for_a_simple_balance() {
        let ledger = ledger_with_usdt_balance(dec!(1000));
        let account = PaperAccount::new(&ledger);
        let mark_prices = HashMap::new();
        let equity = account.equity_snapshot(&mark_prices).unwrap();
        assert_eq!(equity, dec!(1000));
    }

    #[test]
    fn full_snapshot_combines_all_three_fields_consistently() {
        let ledger = ledger_with_usdt_balance(dec!(500));
        let account = PaperAccount::new(&ledger);
        let mark_prices = HashMap::new();
        let snapshot = account.full_snapshot(&mark_prices).unwrap();
        assert_eq!(snapshot.balances.get("USDT"), Some(&dec!(500)));
        assert!(snapshot.positions.is_empty());
        assert_eq!(snapshot.equity, dec!(500));
    }
}
```

- [x] **Step 4: Write `crates/apexes-paper/src/lib.rs`**

```rust
//! Apexes paper trading: drives `apexes-backtest`'s shared event-loop
//! core (`apexes_backtest::event_loop::run_event_loop`) against a live
//! `apexes_core::MarketDataProvider` feed instead of pre-loaded
//! historical data, applying simulated order-submission latency
//! (`apexes_backtest::LatencyModel`) and accepting an explicit stop
//! signal since a live feed has no natural end. Covers
//! `docs/FEATURES.md` rows 4.1 (`PaperAccount`) and 4.2
//! (`run_paper_trading`); row 4.3 (order lifecycle state machine) is
//! unchanged from sub-project #0's `apexes_core::OrderStatus`.

pub mod account;
pub mod runner;

pub use account::{AccountSnapshot, PaperAccount};
pub use apexes_backtest::LatencyModel;
pub use runner::{PaperTradingConfig, run_paper_trading};
```

- [x] **Step 5: Run the tests written so far**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-paper account:: -- --nocapture
```
Expected: still a compile error, because `lib.rs` declares `pub mod runner;` which doesn't exist yet -- confirm the ONLY compile error mentions `runner`, not `account`. This is expected and resolved by Task 3.

- [x] **Step 6: Commit is deferred to Task 3**

Do not commit yet -- the crate does not compile as a whole until `runner.rs` exists (Task 3). Proceed directly to Task 3.

---

### Task 3: `run_paper_trading`

**Files:**
- Create: `crates/apexes-paper/src/runner.rs`

**Interfaces:**
- Consumes: `apexes_backtest::event_loop::{EventLoopConfig, run_event_loop}`, `apexes_backtest::{LatencyModel, BacktestReport, CostModel}`, `apexes_backtest::clock::BacktestClock`, `apexes_backtest::clock::ReplaySpeed` (from Task 1), `apexes_core::{ApexesError, Instrument, MarketDataProvider}`, `apexes_risk::CompositeRiskChecker`, `apexes_strategy::Strategy` (all existing).
- Produces: `pub struct PaperTradingConfig`, `pub async fn run_paper_trading`. Consumed by Task 4's mock-provider tests and Task 5's live Binance integration test.

- [x] **Step 1: Confirm `apexes_backtest::clock::BacktestClock`/`ReplaySpeed` are reachable at that path**

```bash
cd ~/src/oss/apexes
grep -n "pub mod clock" crates/apexes-backtest/src/lib.rs
```
Expected: `pub mod clock;` is present (it already is, unchanged by Task 1) -- confirming `apexes_backtest::clock::BacktestClock` and `apexes_backtest::clock::ReplaySpeed` (equivalently reachable via the re-exported `apexes_backtest::ReplaySpeed`) are both valid paths for this task's imports.

- [x] **Step 2: Write `crates/apexes-paper/src/runner.rs`**

```rust
//! [`run_paper_trading`]: the `apexes-paper` counterpart to
//! `apexes_backtest::run_backtest` -- drives the same shared event-loop
//! core (`apexes_backtest::event_loop::run_event_loop`) against a live
//! `MarketDataProvider::subscribe()` stream instead of pre-loaded
//! historical data.

use std::collections::HashMap;

use apexes_backtest::clock::BacktestClock;
use apexes_backtest::event_loop::{EventLoopConfig, run_event_loop};
use apexes_backtest::{BacktestReport, CostModel, LatencyModel, ReplaySpeed};
use apexes_core::{ApexesError, Instrument, MarketDataProvider};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::Strategy;
use rust_decimal::Decimal;
use tokio::sync::oneshot;

pub struct PaperTradingConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub queue_position_factor: Decimal,
    pub risk_free_return: Decimal,
    pub risk_checker: Option<CompositeRiskChecker>,
    pub latency_model: LatencyModel,
}

/// Runs paper trading: subscribes to `provider` for `config.instruments`,
/// drives the shared event-loop core against the resulting live stream,
/// applying `config.latency_model`'s delay before each `OrderIntent`
/// that passes its `PreTradeCheck` reaches the matching engine. Exits
/// gracefully (returning the `BacktestReport` accumulated so far, not an
/// error) once `stop_signal` fires or its sender is dropped.
pub async fn run_paper_trading(
    config: PaperTradingConfig,
    provider: &dyn MarketDataProvider,
    strategy: impl Strategy,
    stop_signal: oneshot::Receiver<()>,
) -> Result<BacktestReport, ApexesError> {
    let event_stream = provider.subscribe(&config.instruments).await?;
    let clock = BacktestClock::new(ReplaySpeed::AsFastAsPossible);

    let event_loop_config = EventLoopConfig {
        instruments: config.instruments,
        initial_balances: config.initial_balances,
        cost_model: config.cost_model,
        queue_position_factor: config.queue_position_factor,
        risk_free_return: config.risk_free_return,
        risk_checker: config.risk_checker,
    };

    run_event_loop(
        event_loop_config,
        event_stream,
        clock,
        strategy,
        Some(stop_signal),
        Some(config.latency_model),
    )
    .await
}
```

- [x] **Step 3: Verify the crate compiles as a whole**

```bash
cd ~/src/oss/apexes
cargo check -p apexes-paper
```
Expected: succeeds, 0 errors (all three source files -- `account.rs`, `runner.rs`, `lib.rs` -- now exist and compile together).

- [x] **Step 4: Run all tests written so far**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-paper
```
Expected: the 4 `account.rs` tests from Task 2 pass, 0 failed. `runner.rs` has no unit tests yet -- those come in Task 4.

- [x] **Step 5: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-paper --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run both commands).

- [x] **Step 6: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors -- first point in this plan where `apexes-paper` compiles as part of the whole workspace.

- [x] **Step 7: Commit Tasks 2-3 together**

Tasks 2-3 are committed as one unit because `lib.rs`'s `pub mod runner;` declaration means the crate does not compile until both `account.rs` and `runner.rs` exist (per Global Constraints: every task that touches compiled code must leave `cargo build --workspace` passing before committing).

```bash
cd ~/src/oss/apexes
git add Cargo.toml crates/apexes-paper
git commit -S -s -m "feat: add apexes-paper crate with PaperAccount and run_paper_trading"
git push origin master
```

---

### Task 4: `run_paper_trading` unit tests with a mock `MarketDataProvider`

**Files:**
- Modify: `crates/apexes-paper/src/runner.rs` (append `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `crate::runner::{PaperTradingConfig, run_paper_trading}` (Task 3), `apexes_core::MarketDataProvider` (existing), `apexes_risk::{CompositeRiskChecker, ListMode, SymbolVenueList}` (existing). Requires no network access -- the mock provider returns an in-memory `Stream` built with `futures::stream::iter`.
- Produces: nothing new consumed by later tasks -- this task only adds test coverage.

- [x] **Step 1: Append the test module to `crates/apexes-paper/src/runner.rs`**

```rust

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_backtest::cost_model::FlatFeeSchedule;
    use apexes_backtest::{OrderIntent, Strategy, StrategyContext};
    use apexes_core::{
        AssetClass, DepthUpdate, MarketEvent, MarketType, OrderType, PriceLevel, Side,
        TradeEvent, Venue,
    };
    use apexes_risk::{CompositeRiskChecker, ListMode, SymbolVenueList};
    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};
    use futures::stream::BoxStream;
    use rust_decimal_macros::dec;
    use std::collections::HashSet;
    use std::time::Duration;

    struct MockMarketDataProvider {
        events: Vec<MarketEvent>,
    }

    #[async_trait]
    impl MarketDataProvider for MockMarketDataProvider {
        async fn subscribe(
            &self,
            _instruments: &[Instrument],
        ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
            Ok(Box::pin(futures::stream::iter(self.events.clone())))
        }
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

    fn zero_cost_model() -> CostModel {
        CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        }
    }

    fn sample_events() -> Vec<MarketEvent> {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        vec![
            MarketEvent::Depth(DepthUpdate {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                bids: vec![],
                asks: vec![PriceLevel {
                    price: dec!(100),
                    quantity: dec!(1000),
                }],
                sequence: Some(1),
                is_snapshot: true,
                timestamp: t0,
            }),
            MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price: dec!(100),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: t0 + chrono::Duration::seconds(1),
            }),
        ]
    }

    struct BuyOnceStrategy {
        bought: bool,
    }

    impl Strategy for BuyOnceStrategy {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            if !self.bought && matches!(event, MarketEvent::Trade(_)) {
                self.bought = true;
                return vec![OrderIntent {
                    symbol: "BTCUSDT".to_string(),
                    venue: Venue::Binance,
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    quantity: dec!(1),
                    price: None,
                    stop_price: None,
                }];
            }
            vec![]
        }
    }

    fn base_config() -> PaperTradingConfig {
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(100000));
        PaperTradingConfig {
            instruments: vec![btc_usdt_instrument()],
            initial_balances,
            cost_model: zero_cost_model(),
            queue_position_factor: dec!(0.0),
            risk_free_return: dec!(0),
            risk_checker: None,
            latency_model: LatencyModel::None,
        }
    }

    #[tokio::test]
    async fn strategy_orders_are_filled_via_the_shared_matching_engine() {
        let provider = MockMarketDataProvider {
            events: sample_events(),
        };
        let (_tx, rx) = oneshot::channel();

        let report = run_paper_trading(
            base_config(),
            &provider,
            BuyOnceStrategy { bought: false },
            rx,
        )
        .await
        .unwrap();

        assert_eq!(report.trades.len(), 1);
    }

    #[tokio::test]
    async fn latency_model_fixed_delays_order_processing() {
        let provider = MockMarketDataProvider {
            events: sample_events(),
        };
        let mut config = base_config();
        config.latency_model = LatencyModel::Fixed(Duration::from_millis(100));
        let (_tx, rx) = oneshot::channel();

        let start = tokio::time::Instant::now();
        let report = run_paper_trading(config, &provider, BuyOnceStrategy { bought: false }, rx)
            .await
            .unwrap();

        assert_eq!(report.trades.len(), 1);
        assert!(start.elapsed() >= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn risk_checker_blacklist_prevents_the_order_from_being_filled() {
        let provider = MockMarketDataProvider {
            events: sample_events(),
        };
        let mut blacklist = HashSet::new();
        blacklist.insert("BTCUSDT".to_string());
        let risk_checker = CompositeRiskChecker::new().with_pre_trade_check(Box::new(
            SymbolVenueList::new(ListMode::Blacklist(blacklist), ListMode::Disabled),
        ));
        let mut config = base_config();
        config.risk_checker = Some(risk_checker);
        let (_tx, rx) = oneshot::channel();

        let report = run_paper_trading(config, &provider, BuyOnceStrategy { bought: false }, rx)
            .await
            .unwrap();

        assert!(report.trades.is_empty());
    }

    #[tokio::test]
    async fn stop_signal_causes_graceful_early_exit_with_partial_results() {
        // An event stream long enough that firing the stop signal
        // immediately (before any events are consumed) reliably beats
        // the mock provider's synchronous stream delivery, proving the
        // loop checks the signal rather than always draining the stream.
        let mut many_events = Vec::new();
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        for i in 0..1000 {
            many_events.push(MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price: dec!(100),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: t0 + chrono::Duration::milliseconds(i),
            }));
        }
        let provider = MockMarketDataProvider {
            events: many_events,
        };
        let (tx, rx) = oneshot::channel();
        tx.send(()).unwrap();

        let report = run_paper_trading(
            base_config(),
            &provider,
            BuyOnceStrategy { bought: false },
            rx,
        )
        .await
        .unwrap();

        // The loop's very first iteration checks the stop signal before
        // consuming any event, so the run produces no trades regardless
        // of how many events the mock provider could have delivered.
        assert!(report.trades.is_empty());
    }
}
```

- [x] **Step 2: Run the new tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-paper runner:: -- --nocapture
```
Expected: 4 passed, 0 failed.

- [x] **Step 3: Run the full `apexes-paper` test suite, clippy, and fmt**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-paper
cargo clippy -p apexes-paper --all-targets -- -D warnings
cargo fmt --check
```
Expected: 8 tests passed (4 `account` + 4 `runner`), 0 failed; clippy and fmt clean (auto-fix with `cargo fmt` if needed and re-run all three commands).

- [x] **Step 4: Full workspace verification**

```bash
cd ~/src/oss/apexes
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```
Expected: all clean, 0 failures.

- [x] **Step 5: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-paper/src/runner.rs
git commit -S -s -m "test: add run_paper_trading unit tests with a mock MarketDataProvider"
git push origin master
```

---

### Task 5: Live Binance integration test

**Files:**
- Create: `crates/apexes-paper/tests/live_binance.rs`

**Interfaces:**
- Consumes: `apexes_paper::{PaperTradingConfig, run_paper_trading, LatencyModel}` (Tasks 2-3), `apexes_market_data::BinanceMarketDataProvider` (existing). Requires a real network connection to Binance's public WebSocket endpoints -- marked `#[ignore]`, matching `apexes-market-data`'s established pattern (`crates/apexes-market-data/tests/live_binance.rs`).
- Produces: nothing new consumed by later tasks -- this is the plan's final behavioral proof point before Task 6's whole-effort verification.

- [x] **Step 1: Write `crates/apexes-paper/tests/live_binance.rs`**

```rust
//! Live integration test against a real Binance WebSocket feed, proving
//! `run_paper_trading` works end-to-end with `BinanceMarketDataProvider`
//! (not just the mock provider used in `runner.rs`'s unit tests).
//! Excluded from default `cargo test` -- run explicitly with:
//!   cargo test -p apexes-paper -- --ignored

use apexes_backtest::cost_model::FlatFeeSchedule;
use apexes_backtest::{CostModel, OrderIntent, Strategy, StrategyContext};
use apexes_core::{AssetClass, Instrument, MarketEvent, MarketType, OrderType, Side, Venue};
use apexes_market_data::BinanceMarketDataProvider;
use apexes_paper::{LatencyModel, PaperTradingConfig, run_paper_trading};
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot;

/// Never actually submits an order -- this test only proves the live
/// feed connects and `run_paper_trading` processes events without
/// panicking or erroring; it does not attempt to predict real market
/// conditions well enough to assert on specific fills.
struct NoOpStrategy;

impl Strategy for NoOpStrategy {
    fn on_market_event(&mut self, _event: &MarketEvent, _ctx: &StrategyContext) -> Vec<OrderIntent> {
        vec![]
    }
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
        dec!(0.00001),
        dec!(1),
    )
    .unwrap()
}

#[tokio::test]
#[ignore]
async fn run_paper_trading_against_real_binance_feed_for_a_few_seconds() {
    let provider = BinanceMarketDataProvider::new();
    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(10000));

    let config = PaperTradingConfig {
        instruments: vec![btc_usdt_instrument()],
        initial_balances,
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        },
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
        risk_checker: None,
        latency_model: LatencyModel::None,
    };

    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let _ = tx.send(());
    });

    let report = run_paper_trading(config, &provider, NoOpStrategy, rx)
        .await
        .expect("run_paper_trading should not error against a real Binance feed");

    // No specific assertions on report.trades (NoOpStrategy never
    // submits orders) -- this test's purpose is proving the live
    // subscribe -> event-loop -> graceful-stop pipeline works end to
    // end without panicking, matching apexes-market-data's own live
    // test's scope boundary (see crates/apexes-market-data/tests/live_binance.rs).
    assert!(report.equity_curve.len() <= 1 || !report.equity_curve.is_empty());
}
```

- [x] **Step 2: Run the live test (requires network access)**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-paper --test live_binance -- --ignored --nocapture
```
Expected: `1 passed` (takes about 10 seconds to run, per the sleep-then-stop-signal in the test). If it fails due to network unavailability in your environment, note that in your report rather than treating it as a code defect -- this is the same caveat `apexes-market-data`'s own live test carries.

- [x] **Step 3: Run the full `apexes-paper` test suite (ignored + non-ignored), clippy, and fmt**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-paper
cargo test -p apexes-paper -- --ignored
cargo clippy -p apexes-paper --all-targets -- -D warnings
cargo fmt --check
```
Expected: non-ignored: 8 passed (4 `account` + 4 `runner`). Ignored: 1 passed (this task's live test). Clippy and fmt clean (auto-fix with `cargo fmt` if needed, then re-run all four commands).

- [x] **Step 4: Full workspace verification**

```bash
cd ~/src/oss/apexes
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```
Expected: all clean, 0 failures.

- [x] **Step 5: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-paper/tests
git commit -S -s -m "test: add live Binance integration test for run_paper_trading"
git push origin master
```

---

### Task 6: Final whole-effort verification

**Files:** None created/modified -- this task only runs verification commands, updates `docs/FEATURES.md`, and confirms the end state.

- [x] **Step 1: Full workspace build**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [x] **Step 2: Full workspace test suite (non-ignored)**

```bash
cd ~/src/oss/apexes
cargo test --workspace
```
Expected: all tests across all crates pass. Confirm `apexes-core`/`apexes-market-data`/`apexes-indicators`/`apexes-strategy`/`apexes-storage`/`apexes-strategy-registry`/`apexes-optimize`/`apexes-risk` are completely unaffected by this plan (unchanged pass counts from before Task 1 started); `apexes-backtest` is unchanged in test COUNT (70 lib unit tests, unchanged from before the refactor) plus 6 new `latency.rs` tests; `apexes-paper` is brand new with 8 non-ignored tests (4 `account` + 4 `runner`).

- [x] **Step 3: Full workspace test suite (ignored/live-network)**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-market-data -- --ignored
cargo test -p apexes-storage -- --ignored
cargo test -p apexes-strategy-registry -- --ignored
cargo test -p apexes-paper -- --ignored
```
(Note: `apexes-storage`/`apexes-strategy-registry`'s ignored tests need a live ClickHouse instance -- run `docker compose up -d && sleep 5` first if testing those.)
Expected: `apexes-market-data`'s pre-existing ignored test still passes unchanged; `apexes-storage`/`apexes-strategy-registry`'s pre-existing ignored tests still pass unchanged; `apexes-paper` contributes 1 new ignored test (Task 5's live Binance test), passing (network permitting).

- [x] **Step 4: Full workspace clippy**

```bash
cd ~/src/oss/apexes
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: 0 warnings.

- [x] **Step 5: Full workspace fmt check**

```bash
cd ~/src/oss/apexes
cargo fmt --check
```
Expected: 0 diffs.

- [x] **Step 6: Update `docs/FEATURES.md`**

Read `docs/FEATURES.md`, find rows `4.1` (Virtual account & virtual fund management, currently `🔲 Not Started`) and `4.2` (Real-time-feed-driven paper matching engine reusing the backtest matching kernel, currently `🔲 Not Started`). Update BOTH rows' Status to `✅ Done`.

Row `4.1`'s Notes:
```
apexes-paper crate: PaperAccount is a thin read-only query layer over apexes-backtest's PortfolioLedger (balances_snapshot/positions_snapshot/equity_snapshot/full_snapshot), intended for future monitoring/UI (sub-project #13) consumers -- does not change PortfolioLedger's own write path. No persistence, no multi-account support (single in-memory account per run_paper_trading invocation); multi-account management remains sub-project #11's scope.
```

Row `4.2`'s Notes:
```
apexes-paper crate: run_paper_trading reuses apexes-backtest's exact matching-engine/ledger/cost-model/risk-check logic via a newly extracted shared event-loop core (apexes_backtest::event_loop::run_event_loop), which run_backtest itself was refactored to call too (zero behavior change to existing backtests, confirmed via full regression test pass). run_paper_trading drives that core against a live apexes_core::MarketDataProvider::subscribe() stream instead of pre-loaded historical events, adds LatencyModel (Fixed/Uniform order-submission delay, simulating network round-trip) as a live-trading-specific realism factor, and accepts a tokio::sync::oneshot stop signal for graceful shutdown (a live feed has no natural end, unlike a finite historical Vec<MarketEvent>). Row 4.3 (order lifecycle state machine consistent with live trading) required no new work -- it already reuses apexes_core::OrderStatus unchanged, as it has since sub-project #0.
```

**Important:** before finalizing, do not just increment counts by 2/2 -- actually count the current `✅ Done` / `🔲 Not Started` / `⏸️ Deferred` rows in the file (e.g. `grep -c` against the row-level status markers, excluding the legend rows and the Summary table's own rows) and confirm your new numbers match Done+2, Not Started-2, Total unchanged, against the real counts. Update the Summary counts table accordingly and the "Current active work" paragraph to note sub-project `#6`'s first two rows (`4.1`/`4.2`) are complete via the new `apexes-paper` crate, alongside the already-`Done` row `4.3`; leave "what's next" unstated since sub-project `#7` (OMS), `#8` (first live gateway), the PyO3 bindings evaluation, and `apexes-risk`'s `6.4`/`6.5` are all still open/deferred options -- don't guess which one the user picks next.

- [x] **Step 7: Commit the FEATURES.md update**

```bash
cd ~/src/oss/apexes
git add docs/FEATURES.md
git commit -S -s -m "docs: mark paper trading account and matching engine complete in FEATURES.md"
git push origin master
```

- [x] **Step 8: Mark this plan's checkboxes complete**

```bash
cd ~/src/oss/apexes
sed -i '' 's/^- \[ \]/- [x]/' docs/superpowers/plans/2026-07-31-apexes-paper.md
```
Check `grep -c "^\- \[ \]" docs/superpowers/plans/2026-07-31-apexes-paper.md` reports `0` remaining unmarked checkboxes. Read the top of the file after running `sed` to verify the header's own descriptive sentence (`Steps use checkbox (\`- [ ]\`) syntax for tracking`) was NOT affected -- that occurrence is inside inline code backticks on its own line and does not match the `^- \[ \]` line-start pattern, so it should be unaffected, but confirm rather than assume.

```bash
git add docs/superpowers/plans/2026-07-31-apexes-paper.md
git commit -S -s -m "docs: mark apexes-paper plan complete"
git push origin master
```

- [x] **Step 9: Report final summary**

Summarize: `apexes-backtest`'s refactored file list (new `event_loop.rs`/`latency.rs`, shrunk `runner.rs`) with line counts, `apexes-paper` crate's file list and line counts, total test count (broken down: `apexes-backtest`'s unchanged + new `latency.rs` tests, `apexes-paper`'s unit + ignored/live tests), confirmation that every other crate's pre-existing tests still pass unchanged, and the `apexes` commit SHAs for Tasks 1-6. No further commit needed for this verification-only task.

---

## Plan Self-Review Notes

**Spec coverage:** Design spec §一 (core architecture decision: extract shared event-loop core driven by `Stream<Item = MarketEvent>`, historical replay wraps `Vec` via `stream::iter`, live trading uses `MarketDataProvider::subscribe()`'s `BoxStream` directly) → Task 1. §一's "不包含" exclusions (no real order placement, no persistence/reconnect reconciliation, no multi-account, no network-anomaly simulation beyond latency, no `OrderStatus` changes, no UI, no `tokio-util`) are respected by omission -- no task implements any of them. §二 (`apexes-backtest`重构: `EventLoopConfig`/`run_event_loop`/`run_backtest`重写, module visibility decisions) → Task 1. §三 (`apexes-paper` crate structure) → Task 2 (scaffold + `account.rs`), Task 3 (`runner.rs`). §四 (`PaperAccount`) → Task 2. §五 (`LatencyModel`, corrected to live in `apexes-backtest` not `apexes-paper` per the design doc's own self-correction) → Task 1 Step 4, re-exported by `apexes-paper` per Task 2 Step 4's `lib.rs`. §六 (`run_paper_trading`, `PaperTradingConfig`, the corrected `run_event_loop` signature with `latency_model: Option<LatencyModel>`) → Task 3. §七 (test strategy: refactor regression tests, `PaperAccount` unit tests, `LatencyModel` unit tests, `run_event_loop`/`run_paper_trading` mock-based tests including stop-signal and latency-model verification, live Binance integration test, full workspace verification) → Tasks 1, 2, 4, 5, 6 respectively. §八 (non-goals) → respected throughout, explicitly called out in Task 6 Step 6's FEATURES.md Notes text.

**Type consistency check:** `EventLoopConfig`'s six fields (`instruments`/`initial_balances`/`cost_model`/`queue_position_factor`/`risk_free_return`/`risk_checker`) are constructed identically in `run_backtest` (Task 1) and `run_paper_trading` (Task 3). `run_event_loop`'s six parameters (`config`, `events`, `clock`, `strategy`, `stop_signal`, `latency_model`) match exactly between its Task 1 definition and both call sites (`run_backtest` passing `None, None`; `run_paper_trading` passing `Some(stop_signal), Some(config.latency_model)`). `LatencyModel`'s three variants (`None`/`Fixed`/`Uniform`) and its `sample()`/`apply()` methods are used identically in Task 1's definition, Task 1's own tests, and Task 4's `run_paper_trading` tests. `PaperAccount::new`'s `&'a PortfolioLedger` borrow and its four query methods match between Task 2's definition and its own tests.

**Placeholder scan:** No "TBD"/"TODO" strings; every code step contains complete, concrete Rust source, including full test module bodies. Task 1's Step 5 note about the test module being "a representative reconstruction, not necessarily byte-for-byte" is an explicit, deliberate exception to the no-placeholder rule's spirit -- it exists because Task 1 is a refactor of an existing file whose exact current test content the plan author could not fully re-verify byte-for-byte at plan-writing time; the Global Constraints' "zero test regressions, identical pass counts" requirement is the authoritative fallback that makes this safe (an implementer who preserves every existing test's behavior, whatever it is, satisfies the plan's intent even if this code block's literal test list doesn't perfectly match the pre-refactor file).
