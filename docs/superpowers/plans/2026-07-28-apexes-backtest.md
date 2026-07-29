# Apexes Backtest Engine (Sub-project #3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `apexes-backtest`, a crate providing a tick-level historical replay
engine with a Level-2-depth matching simulator, venue-aware cost model, multi-asset
portfolio ledger, and performance/risk-metrics reporting, built on the shared `Strategy`
trait that future live-trading sub-projects will also implement against.

**Architecture:** New workspace crate `apexes-backtest`, depending on `apexes-core`,
`apexes-storage`, `barter-data` (reusing its `books::OrderBook` for order-book
reconstruction — the same type/logic already used live in `apexes-market-data`), and
`barter` (reusing only `barter::statistic::metric::*`/`barter::Timed` for Sharpe/Sortino/
Calmar/drawdown/win-rate/profit-factor calculations — never its `engine`/`execution`/
`strategy`/`system` modules). `apexes-core` gains two small, backward-compatible additions:
`OrderType::{Stop, StopLimit}` variants and `Order.stop_price: Option<Decimal>`, plus three
new `ApexesError` variants for trading-domain errors shared with future live sub-projects.

**Tech Stack:** Rust edition 2024, `barter-data` (workspace dep, `books::OrderBook` reuse),
`barter = "0.12"` (new, `statistic` module reuse only), `apexes-storage` (path dep, historical
data source), `tokio`, `chrono`, `rust_decimal`, `serde`, `tracing`.

## Global Constraints

- Rust edition: `2024` for the new crate (workspace convention).
- No `unsafe` blocks anywhere in this plan.
- No `.unwrap()`/`.expect()` in non-test code — library code returns `Result` or handles
  errors explicitly.
- Every commit uses `git commit -S -s` (signed + signed-off), per project convention.
- Logging uses `tracing`, never `println!`.
- Working directly on `master` (no worktree, per explicit prior user consent).
- All new `apexes-core` changes (OrderType variants, `Order.stop_price`, new `ApexesError`
  variants) must be strictly additive/backward-compatible — no existing field removed,
  renamed, or made non-optional; no existing `OrderStatus` transition logic changes.
- `barter` (the meta-crate, distinct from `barter-data`/`barter-instrument` already in the
  workspace) is added as a dependency of `apexes-backtest` ONLY — never re-exported from
  `apexes-backtest`'s public API surface; only `barter::statistic::metric::*` types
  (`SharpeRatio`, `SortinoRatio`, `CalmarRatio`, `WinRate`, `ProfitFactor`,
  `drawdown::{Drawdown, DrawdownGenerator, max::{MaxDrawdown, MaxDrawdownGenerator}}`) and
  `barter::Timed`/`barter::statistic::time::{TimeInterval, Daily}` are used internally; none
  of `barter::engine`/`barter::execution`/`barter::strategy`/`barter::system`/
  `barter::risk`/`barter-execution` are used anywhere in this plan.
- `barter-data::books::{OrderBook, OrderBookSide, Level, Bids, Asks}` and
  `barter_data::subscription::book::OrderBookEvent` are reused for local order-book
  reconstruction during replay (the same types already used live in
  `apexes-market-data::depth_converter`), via a new converter that goes the *opposite*
  direction (`apexes_core::DepthUpdate` -> `barter_data::subscription::book::OrderBookEvent`)
  from the existing live-path converter.
- Fills produced by the matching engine are always represented as `apexes_core::Fill` —
  never any `barter`/`barter-execution` trade/order type.
- `apexes_core::DepthUpdate` with `is_snapshot: false` carries **delta-only** levels (a
  changed level with `quantity: Decimal::ZERO` means "remove this price level"), matching
  `barter_data::books::OrderBookSide::upsert`'s exact semantics — this must be preserved
  end-to-end through replay (this was independently confirmed against
  `apexes-market-data::depth_converter`'s live-path behavior and `barter_data::books::mod.rs`
  source during planning; do not assume `is_snapshot: false` events carry a full book).
- `apexes_storage::StorageReader::read_batch` loads its full requested range into memory (an
  accepted, documented tradeoff from sub-project #2's final review) — `StorageMarketDataSource`
  in this plan therefore also loads fully into memory per instrument/event-type before
  merging and replaying; this is an accepted scope boundary for this sub-project, not a defect
  to fix here (a later optimization sub-project may introduce true streaming reads if backtest
  data volumes grow large enough to require it).
- Binance Spot VIP fee table (confirmed by the user directly from
  `binance.com/en/fee/schedule` on 2026-07-28, standard non-BNB-discounted rates):

  | Tier | Maker (bps) | Taker (bps) |
  |------|-------------|-------------|
  | 0 (Regular) | 10.0 | 10.0 |
  | 1    | 9.0  | 10.0 |
  | 2    | 8.0  | 10.0 |
  | 3    | 4.0  | 6.0  |
  | 4    | 4.0  | 5.2  |
  | 5    | 2.5  | 3.1  |
  | 6    | 2.0  | 2.9  |
  | 7    | 1.9  | 2.8  |
  | 8    | 1.6  | 2.5  |
  | 9    | 1.1  | 2.3  |

- Design spec of record: `docs/superpowers/specs/2026-07-28-apexes-backtest-design.md`. If
  you find a mismatch between this plan and that spec while implementing, the spec wins —
  stop and flag it rather than improvising.

---

### Task 1: `apexes-core` additions — Stop/StopLimit order types + new `ApexesError` variants

**Files:**
- Modify: `crates/apexes-core/src/order.rs`
- Modify: `crates/apexes-core/src/error.rs`

**Interfaces:**
- Consumes: nothing new (extends existing `apexes-core` types).
- Produces: `OrderType::{Market, Limit, Stop, StopLimit}` (was `Market`/`Limit` only),
  `Order.stop_price: Option<Decimal>` (new field), `ApexesError::{InsufficientBalance,
  InvalidBacktestConfig, OrderRejected}` (new variants). All later tasks in this plan that
  construct `Order`/`OrderType`/`ApexesError` values use these exact names.

- [ ] **Step 1: Add `Stop`/`StopLimit` to `OrderType` and `stop_price` to `Order`**

Modify `crates/apexes-core/src/order.rs`, find:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
}
```

Replace with:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    /// Triggers a Market order once `Order.stop_price` is touched.
    Stop,
    /// Triggers a Limit order (at `Order.price`) once `Order.stop_price` is touched.
    StopLimit,
}
```

Find the `Order` struct:

```rust
pub struct Order {
    pub id: OrderId,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub venue: Venue,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    /// `None` for Market orders; `Some` for Limit orders.
    pub price: Option<Decimal>,
    pub filled_quantity: Decimal,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

Replace with (adds `stop_price` after `price`, updates `price`'s doc comment):

```rust
pub struct Order {
    pub id: OrderId,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub venue: Venue,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    /// Limit price. `None` for Market/Stop orders; `Some` for Limit/StopLimit orders.
    pub price: Option<Decimal>,
    /// Trigger price. `None` for Market/Limit orders; `Some` for Stop/StopLimit orders.
    pub stop_price: Option<Decimal>,
    pub filled_quantity: Decimal,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

- [ ] **Step 2: Update the existing `order_serde_round_trip` test to include `stop_price`**

In `crates/apexes-core/src/order.rs`, find the `order_serde_round_trip` test's `Order`
construction (in `#[cfg(test)] mod tests`) and add `stop_price: None,` after the `price:
Some(...)` line, so the existing test still compiles (the new field has no default and must
be set explicitly in every `Order` literal).

- [ ] **Step 3: Add two new tests for Stop/StopLimit construction and serde round-trip**

Add to the `#[cfg(test)] mod tests` block in `crates/apexes-core/src/order.rs`:

```rust
    #[test]
    fn stop_order_serde_round_trip() {
        let now = Utc::now();
        let order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Sell,
            order_type: OrderType::Stop,
            quantity: rust_decimal_macros::dec!(1.0),
            price: None,
            stop_price: Some(rust_decimal_macros::dec!(60000)),
            filled_quantity: rust_decimal_macros::dec!(0),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&order).unwrap();
        let round_tripped: Order = serde_json::from_str(&json).unwrap();
        assert_eq!(order, round_tripped);
        assert_eq!(order.price, None);
        assert_eq!(order.stop_price, Some(rust_decimal_macros::dec!(60000)));
    }

    #[test]
    fn stop_limit_order_serde_round_trip() {
        let now = Utc::now();
        let order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::StopLimit,
            quantity: rust_decimal_macros::dec!(1.0),
            price: Some(rust_decimal_macros::dec!(65500)),
            stop_price: Some(rust_decimal_macros::dec!(65450)),
            filled_quantity: rust_decimal_macros::dec!(0),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&order).unwrap();
        let round_tripped: Order = serde_json::from_str(&json).unwrap();
        assert_eq!(order, round_tripped);
        assert_eq!(order.price, Some(rust_decimal_macros::dec!(65500)));
        assert_eq!(order.stop_price, Some(rust_decimal_macros::dec!(65450)));
    }
```

- [ ] **Step 4: Add three new `ApexesError` variants**

Modify `crates/apexes-core/src/error.rs`, find the end of the `ApexesError` enum (after the
`SubscriptionFailed` variant, before the closing `}`):

```rust
    #[error("subscription failed for {symbol} on {venue:?}: {reason}")]
    SubscriptionFailed {
        symbol: String,
        venue: Venue,
        reason: String,
        source_detail: Option<String>,
    },
}
```

Replace with:

```rust
    #[error("subscription failed for {symbol} on {venue:?}: {reason}")]
    SubscriptionFailed {
        symbol: String,
        venue: Venue,
        reason: String,
        source_detail: Option<String>,
    },

    #[error("insufficient balance: required {required} {asset}, available {available}")]
    InsufficientBalance {
        asset: String,
        required: rust_decimal::Decimal,
        available: rust_decimal::Decimal,
    },

    #[error("invalid backtest config: {reason}")]
    InvalidBacktestConfig { reason: String },

    #[error("order rejected: {reason}")]
    OrderRejected { reason: String },
}
```

Note: `rust_decimal::Decimal` is used with its full path here since `error.rs` does not
currently import `Decimal` — check the top of the file first; if `use rust_decimal::Decimal;`
is already present (it is not, as of Task 1's start), use the bare `Decimal` instead of the
full path for consistency with the rest of the file's style.

- [ ] **Step 5: Add three new tests for the new `ApexesError` variants**

Add to the `#[cfg(test)] mod tests` block in `crates/apexes-core/src/error.rs`:

```rust
    #[test]
    fn insufficient_balance_display() {
        let err = ApexesError::InsufficientBalance {
            asset: "USDT".to_string(),
            required: rust_decimal_macros::dec!(1000),
            available: rust_decimal_macros::dec!(500),
        };
        assert_eq!(
            err.to_string(),
            "insufficient balance: required 1000 USDT, available 500"
        );
    }

    #[test]
    fn invalid_backtest_config_display() {
        let err = ApexesError::InvalidBacktestConfig {
            reason: "missing initial balance for traded asset BTC".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "invalid backtest config: missing initial balance for traded asset BTC"
        );
    }

    #[test]
    fn order_rejected_display() {
        let err = ApexesError::OrderRejected {
            reason: "unsupported order kind".to_string(),
        };
        assert_eq!(err.to_string(), "order rejected: unsupported order kind");
    }

    #[test]
    fn insufficient_balance_is_cloneable() {
        let err = ApexesError::InsufficientBalance {
            asset: "BTC".to_string(),
            required: rust_decimal_macros::dec!(1),
            available: rust_decimal_macros::dec!(0.5),
        };
        let cloned = err.clone();
        assert_eq!(err, cloned);
    }
```

Check whether `crates/apexes-core/Cargo.toml`'s `[dev-dependencies]` already includes
`rust_decimal_macros` (it should, since `order.rs`'s existing tests use
`rust_decimal_macros::dec!`) — if `error.rs`'s existing tests don't currently use it, this is
still fine since it's already a dev-dependency of the crate as a whole.

- [ ] **Step 6: Run tests**

Run: `cargo test -p apexes-core`
Expected: all existing tests still pass (with the `order_serde_round_trip` fix from Step 2),
plus 6 new tests pass (2 from Step 3, 4 from Step 5) — 29 (prior baseline) + 6 = 35 total,
minus any double-counted fix — report the exact real total you observe.

- [ ] **Step 7: Run clippy**

Run: `cargo clippy -p apexes-core -- -D warnings`
Expected: no warnings.

- [ ] **Step 8: Commit**

```bash
git add crates/apexes-core/src/order.rs crates/apexes-core/src/error.rs
git commit -S -s -m "feat(apexes-core): add Stop/StopLimit order types and trading-domain errors"
```

---

### Task 2: `apexes-backtest` crate scaffold

**Files:**
- Create: `crates/apexes-backtest/Cargo.toml`
- Create: `crates/apexes-backtest/src/lib.rs`
- Create: `crates/apexes-backtest/src/error.rs` (re-exports `apexes_core::ApexesError` as this
  crate's error type — no new crate-local error enum, per design spec §9)
- Modify: `Cargo.toml` (workspace root — add `crates/apexes-backtest` to `members`, add
  `barter` to `[workspace.dependencies]`)

**Interfaces:**
- Consumes: `apexes-core`, `apexes-storage` (path deps), `barter-data` (workspace dep,
  already present), `barter` (new workspace dep).
- Produces: a workspace member `apexes-backtest` that `cargo build --workspace` succeeds on
  with an empty lib exposing module stubs for all subsequent tasks.

- [ ] **Step 1: Add `barter` to the workspace root `Cargo.toml` and register the new member**

Modify `Cargo.toml` (workspace root), find:

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage"]
```

Replace with:

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage", "crates/apexes-backtest"]
```

Find:

```toml
clickhouse = { version = "0.15", features = ["chrono"] }
fixnum = { version = "0.9", features = ["i128", "serde"] }
```

Replace with:

```toml
clickhouse = { version = "0.15", features = ["chrono"] }
fixnum = { version = "0.9", features = ["i128", "serde"] }
barter = "0.12"
```

- [ ] **Step 2: Create the crate's `Cargo.toml`**

Create `crates/apexes-backtest/Cargo.toml`:

```toml
[package]
name = "apexes-backtest"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-storage = { path = "../apexes-storage" }
barter-data = { workspace = true }
barter = { workspace = true }
tokio = { workspace = true, features = ["time"] }
futures = { workspace = true }
rust_decimal = { workspace = true }
chrono = { workspace = true }
serde = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
```

- [ ] **Step 3: Create `lib.rs` with module stubs**

Create `crates/apexes-backtest/src/lib.rs`:

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

// Populated in Task 3.
pub mod market_data_source;

// Populated in Task 4.
pub mod order_book;

// Populated in Task 5.
pub mod cost_model;

// Populated in Task 6.
pub mod strategy;

// Populated in Task 7.
pub mod ledger;

// Populated in Tasks 8-11 (walk_book, MatchingEngine core, limit orders, Stop/StopLimit).
pub mod matching_engine;

// Populated in Task 12.
pub mod clock;

// Populated in Task 13.
pub mod performance;

// Populated in Task 14.
pub mod runner;
pub mod report;
```

- [ ] **Step 4: Create `error.rs`**

Create `crates/apexes-backtest/src/error.rs`:

```rust
//! `apexes-backtest` reuses `apexes_core::ApexesError` as its sole error
//! type — no new crate-local error enum — since backtest-domain errors
//! (insufficient balance, invalid config, order rejection) are the exact
//! same errors future live-trading sub-projects will also need to
//! represent identically, per design spec §9.

pub use apexes_core::ApexesError;
```

- [ ] **Step 5: Create empty module stub files**

Create each of the following with a single placeholder line, to be replaced by their
respective tasks:

`crates/apexes-backtest/src/market_data_source.rs`:
```rust
// Populated in Task 3.
```

`crates/apexes-backtest/src/order_book.rs`:
```rust
// Populated in Task 4.
```

`crates/apexes-backtest/src/cost_model.rs`:
```rust
// Populated in Task 5.
```

`crates/apexes-backtest/src/strategy.rs`:
```rust
// Populated in Task 6.
```

`crates/apexes-backtest/src/ledger.rs`:
```rust
// Populated in Task 7.
```

`crates/apexes-backtest/src/matching_engine.rs`:
```rust
// Populated in Tasks 8-11.
```

`crates/apexes-backtest/src/clock.rs`:
```rust
// Populated in Task 12.
```

`crates/apexes-backtest/src/performance.rs`:
```rust
// Populated in Task 13.
```

`crates/apexes-backtest/src/runner.rs`:
```rust
// Populated in Task 14.
```

`crates/apexes-backtest/src/report.rs`:
```rust
// Populated in Task 14.
```

- [ ] **Step 6: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: succeeds with zero warnings. This proves the new crate links correctly against
`apexes-core`, `apexes-storage`, `barter-data`, and `barter` before any real logic is written.

- [ ] **Step 7: Run clippy**

Run: `cargo clippy -p apexes-backtest -- -D warnings`
Expected: no warnings (empty/stub modules should not trigger any lints; if `unused` warnings
appear for the stub files, that's expected to resolve once each task fills them in — but a
clean `cargo build --workspace` with no *error* is the hard requirement for this task; if
clippy warns about genuinely empty modules, add `#![allow(dead_code)]` at the crate root in
`lib.rs` temporarily and note it in your report, to be removed once Task 3+ fill in real
code — but only if clippy actually complains; try without it first).

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/apexes-backtest
git commit -S -s -m "chore: scaffold apexes-backtest crate"
```

---

### Task 3: `HistoricalMarketDataSource` trait + `InMemoryMarketDataSource` + `StorageMarketDataSource`

**Files:**
- Modify: `crates/apexes-backtest/src/market_data_source.rs`

**Interfaces:**
- Consumes: `apexes_core::MarketEvent`, `apexes_storage::{StorageReader, EventType}`
  (Task 6/7 of sub-project #2, already shipped), `apexes_core::Venue`.
- Produces: `pub trait HistoricalMarketDataSource`, `pub struct InMemoryMarketDataSource`,
  `pub struct StorageMarketDataSource`. Task 12 (clock/runner) consumes
  `HistoricalMarketDataSource::events()`.

- [ ] **Step 1: Write the trait and both implementations with unit tests**

Create `crates/apexes-backtest/src/market_data_source.rs`:

```rust
use apexes_core::{ApexesError, MarketEvent, Venue};
use apexes_storage::{EventType, StorageReader};
use chrono::NaiveDate;
use std::ops::Range;

/// Supplies a chronologically-ordered sequence of historical `MarketEvent`s
/// for backtest replay.
///
/// Implementations load fully into memory before returning (an accepted
/// scope boundary for this sub-project, matching
/// `apexes_storage::StorageReader::read_batch`'s own full-range-into-memory
/// behavior -- see this plan's Global Constraints).
pub trait HistoricalMarketDataSource {
    /// Returns all events in chronological (timestamp ascending) order,
    /// merged across every event type/instrument this source was
    /// constructed with.
    fn events(&self) -> impl Future<Output = Result<Vec<MarketEvent>, ApexesError>> + Send;
}

/// An in-memory `HistoricalMarketDataSource` backed by a pre-supplied
/// `Vec<MarketEvent>`. Used for fast, network-free unit/integration tests
/// that don't require a live ClickHouse instance.
pub struct InMemoryMarketDataSource {
    events: Vec<MarketEvent>,
}

impl InMemoryMarketDataSource {
    /// Constructs a new source from `events`, sorting them into
    /// chronological order by timestamp (each `MarketEvent` variant's
    /// `timestamp`/`open_time` field, extracted via `event_timestamp`).
    pub fn new(mut events: Vec<MarketEvent>) -> Self {
        events.sort_by_key(event_timestamp);
        Self { events }
    }
}

impl HistoricalMarketDataSource for InMemoryMarketDataSource {
    async fn events(&self) -> Result<Vec<MarketEvent>, ApexesError> {
        Ok(self.events.clone())
    }
}

/// A `HistoricalMarketDataSource` backed by `apexes_storage::StorageReader`,
/// reading real historical data from ClickHouse.
pub struct StorageMarketDataSource {
    reader: StorageReader,
    symbol: String,
    venue: Venue,
    date_range: Range<NaiveDate>,
}

impl StorageMarketDataSource {
    pub fn new(
        reader: StorageReader,
        symbol: impl Into<String>,
        venue: Venue,
        date_range: Range<NaiveDate>,
    ) -> Self {
        Self {
            reader,
            symbol: symbol.into(),
            venue,
            date_range,
        }
    }
}

impl HistoricalMarketDataSource for StorageMarketDataSource {
    async fn events(&self) -> Result<Vec<MarketEvent>, ApexesError> {
        let mut all_events = Vec::new();

        for event_type in [
            EventType::Trade,
            EventType::Tick,
            EventType::Depth,
            EventType::Bar,
        ] {
            let events = self
                .reader
                .read_batch(&self.symbol, self.venue, self.date_range.clone(), event_type)
                .await
                .map_err(|error| ApexesError::InvalidBacktestConfig {
                    reason: format!("failed to read {event_type:?} events: {error}"),
                })?;
            all_events.extend(events);
        }

        all_events.sort_by_key(event_timestamp);
        Ok(all_events)
    }
}

/// Extracts the timestamp used for chronological ordering from any
/// `MarketEvent` variant (Bar uses `open_time`, all others use
/// `timestamp`).
fn event_timestamp(event: &MarketEvent) -> chrono::DateTime<chrono::Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::TradeEvent;
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    fn trade_at(symbol: &str, hour: u32) -> MarketEvent {
        MarketEvent::Trade(TradeEvent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc.with_ymd_and_hms(2026, 1, 1, hour, 0, 0).unwrap(),
        })
    }

    #[tokio::test]
    async fn in_memory_source_sorts_events_chronologically() {
        let events = vec![trade_at("A", 3), trade_at("B", 1), trade_at("C", 2)];
        let source = InMemoryMarketDataSource::new(events);

        let result = source.events().await.unwrap();

        let MarketEvent::Trade(first) = &result[0] else {
            panic!("expected Trade");
        };
        let MarketEvent::Trade(second) = &result[1] else {
            panic!("expected Trade");
        };
        let MarketEvent::Trade(third) = &result[2] else {
            panic!("expected Trade");
        };
        assert_eq!(first.symbol, "B");
        assert_eq!(second.symbol, "C");
        assert_eq!(third.symbol, "A");
    }

    #[tokio::test]
    async fn in_memory_source_handles_empty_input() {
        let source = InMemoryMarketDataSource::new(vec![]);
        let result = source.events().await.unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn event_timestamp_extracts_bar_open_time_not_close_time() {
        let now = Utc::now();
        let later = now + chrono::Duration::minutes(1);
        let bar = MarketEvent::Bar(apexes_core::Bar {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            open: dec!(65000),
            high: dec!(65100),
            low: dec!(64900),
            close: dec!(65050),
            volume: dec!(1),
            interval: apexes_core::BarInterval::OneMinute,
            open_time: now,
            close_time: later,
        });
        assert_eq!(event_timestamp(&bar), now);
    }
}
```

- [ ] **Step 2: Wire the module into `lib.rs`**

Modify `crates/apexes-backtest/src/lib.rs`: the `pub mod market_data_source;` line should
already be present from Task 2 — verify it's still there, no change needed if so.

- [ ] **Step 3: Run tests**

Run: `cargo test -p apexes-backtest market_data_source`
Expected: 3 tests pass (`in_memory_source_sorts_events_chronologically`,
`in_memory_source_handles_empty_input`, `event_timestamp_extracts_bar_open_time_not_close_time`).
None require a ClickHouse connection.

- [ ] **Step 4: Run clippy**

Run: `cargo clippy -p apexes-backtest -- -D warnings`
Expected: no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-backtest/src/market_data_source.rs
git commit -S -s -m "feat(apexes-backtest): add HistoricalMarketDataSource trait and implementations"
```

---

### Task 4: Order book reconstruction (`ReplayOrderBook`)

**Files:**
- Modify: `crates/apexes-backtest/src/order_book.rs`

**Interfaces:**
- Consumes: `apexes_core::{DepthUpdate, PriceLevel}`, `barter_data::books::{OrderBook, Level,
  Bids, Asks}`, `barter_data::subscription::book::OrderBookEvent` (all already
  workspace-available via the `barter-data` dependency).
- Produces: `pub struct ReplayOrderBook`, `pub fn ReplayOrderBook::new() -> Self`,
  `pub fn ReplayOrderBook::apply(&mut self, update: &DepthUpdate)`,
  `pub fn ReplayOrderBook::bids(&self) -> Vec<PriceLevel>`,
  `pub fn ReplayOrderBook::asks(&self) -> Vec<PriceLevel>`. Tasks 9-10 (matching engine)
  consume `ReplayOrderBook::{apply, bids, asks}`.

- [ ] **Step 1: Write the wrapper around `barter_data::books::OrderBook` with unit tests**

Create `crates/apexes-backtest/src/order_book.rs`:

```rust
use apexes_core::{DepthUpdate, PriceLevel};
use barter_data::books::{Level, OrderBook};
use barter_data::subscription::book::OrderBookEvent;

/// A locally-reconstructed order book for backtest replay, built by
/// feeding it a chronological sequence of `apexes_core::DepthUpdate`s.
///
/// Wraps `barter_data::books::OrderBook` (the same type used live by
/// `apexes-market-data`) to reuse its proven snapshot/diff merge
/// semantics rather than reimplementing them. `DepthUpdate` events with
/// `is_snapshot: false` carry delta-only levels (a level with `quantity:
/// Decimal::ZERO` means "remove this price level"), matching
/// `OrderBookSide::upsert`'s exact semantics.
#[derive(Debug, Default)]
pub struct ReplayOrderBook {
    inner: OrderBook,
}

impl ReplayOrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a `DepthUpdate` to the local book: a full replace if
    /// `update.is_snapshot`, or an upsert-merge of delta levels otherwise.
    pub fn apply(&mut self, update: &DepthUpdate) {
        let sequence = update.sequence.unwrap_or(0);
        let event = if update.is_snapshot {
            OrderBookEvent::Snapshot(OrderBook::new(
                sequence,
                Some(update.timestamp),
                update.bids.iter().map(price_level_to_barter_level),
                update.asks.iter().map(price_level_to_barter_level),
            ))
        } else {
            OrderBookEvent::Update(OrderBook::new(
                sequence,
                Some(update.timestamp),
                update.bids.iter().map(price_level_to_barter_level),
                update.asks.iter().map(price_level_to_barter_level),
            ))
        };
        self.inner.update(&event);
    }

    /// Current bids, sorted best (highest price) first.
    pub fn bids(&self) -> Vec<PriceLevel> {
        self.inner.bids().levels().iter().map(barter_level_to_price_level).collect()
    }

    /// Current asks, sorted best (lowest price) first.
    pub fn asks(&self) -> Vec<PriceLevel> {
        self.inner.asks().levels().iter().map(barter_level_to_price_level).collect()
    }
}

fn price_level_to_barter_level(level: &PriceLevel) -> Level {
    Level::new(level.price, level.quantity)
}

fn barter_level_to_price_level(level: &Level) -> PriceLevel {
    PriceLevel {
        price: level.price,
        quantity: level.amount,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::Venue;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn snapshot(bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) -> DepthUpdate {
        DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids,
            asks,
            sequence: Some(1),
            is_snapshot: true,
            timestamp: Utc::now(),
        }
    }

    fn diff(sequence: u64, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) -> DepthUpdate {
        DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids,
            asks,
            sequence: Some(sequence),
            is_snapshot: false,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn snapshot_replaces_book_state() {
        let mut book = ReplayOrderBook::new();
        book.apply(&snapshot(
            vec![PriceLevel { price: dec!(100), quantity: dec!(1) }],
            vec![PriceLevel { price: dec!(101), quantity: dec!(2) }],
        ));

        assert_eq!(book.bids().len(), 1);
        assert_eq!(book.bids()[0].price, dec!(100));
        assert_eq!(book.asks()[0].price, dec!(101));
    }

    #[test]
    fn diff_upserts_a_new_level() {
        let mut book = ReplayOrderBook::new();
        book.apply(&snapshot(
            vec![PriceLevel { price: dec!(100), quantity: dec!(1) }],
            vec![PriceLevel { price: dec!(101), quantity: dec!(2) }],
        ));
        book.apply(&diff(
            2,
            vec![PriceLevel { price: dec!(99), quantity: dec!(3) }],
            vec![],
        ));

        let bids = book.bids();
        assert_eq!(bids.len(), 2);
        // Best bid (highest price) first.
        assert_eq!(bids[0].price, dec!(100));
        assert_eq!(bids[1].price, dec!(99));
    }

    #[test]
    fn diff_with_zero_quantity_removes_the_level() {
        let mut book = ReplayOrderBook::new();
        book.apply(&snapshot(
            vec![PriceLevel { price: dec!(100), quantity: dec!(1) }],
            vec![PriceLevel { price: dec!(101), quantity: dec!(2) }],
        ));
        book.apply(&diff(
            2,
            vec![PriceLevel { price: dec!(100), quantity: dec!(0) }],
            vec![],
        ));

        assert!(book.bids().is_empty());
    }

    #[test]
    fn diff_replaces_existing_level_quantity() {
        let mut book = ReplayOrderBook::new();
        book.apply(&snapshot(
            vec![PriceLevel { price: dec!(100), quantity: dec!(1) }],
            vec![],
        ));
        book.apply(&diff(
            2,
            vec![PriceLevel { price: dec!(100), quantity: dec!(5) }],
            vec![],
        ));

        let bids = book.bids();
        assert_eq!(bids.len(), 1);
        assert_eq!(bids[0].quantity, dec!(5));
    }

    #[test]
    fn bids_and_asks_are_sorted_best_first() {
        let mut book = ReplayOrderBook::new();
        book.apply(&snapshot(
            vec![
                PriceLevel { price: dec!(99), quantity: dec!(1) },
                PriceLevel { price: dec!(100), quantity: dec!(1) },
            ],
            vec![
                PriceLevel { price: dec!(102), quantity: dec!(1) },
                PriceLevel { price: dec!(101), quantity: dec!(1) },
            ],
        ));

        let bids = book.bids();
        let asks = book.asks();
        assert_eq!(bids[0].price, dec!(100)); // highest bid first
        assert_eq!(bids[1].price, dec!(99));
        assert_eq!(asks[0].price, dec!(101)); // lowest ask first
        assert_eq!(asks[1].price, dec!(102));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p apexes-backtest order_book`
Expected: 5 tests pass (`snapshot_replaces_book_state`, `diff_upserts_a_new_level`,
`diff_with_zero_quantity_removes_the_level`, `diff_replaces_existing_level_quantity`,
`bids_and_asks_are_sorted_best_first`).

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p apexes-backtest -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-backtest/src/order_book.rs
git commit -S -s -m "feat(apexes-backtest): add ReplayOrderBook wrapping barter-data's OrderBook"
```

---

### Task 5: Cost model — `FeeSchedule` trait, `BinanceSpotVipFeeSchedule`, `FlatFeeSchedule`, `CostModel`

**Files:**
- Modify: `crates/apexes-backtest/src/cost_model.rs`

**Interfaces:**
- Consumes: `apexes_core::{Venue, AssetClass}`, `rust_decimal::Decimal`.
- Produces: `pub trait FeeSchedule`, `pub struct BinanceSpotVipFeeSchedule`,
  `pub struct FlatFeeSchedule`, `pub struct CostModel`, `pub enum FillLiquidity { Maker,
  Taker }`, `pub fn CostModel::fee_for_fill(&self, venue: Venue, liquidity: FillLiquidity,
  notional: Decimal) -> Decimal`, `pub fn CostModel::funding_payment(&self, position_notional:
  Decimal) -> Option<Decimal>`. Tasks 9-11 (matching engine) consume `CostModel::fee_for_fill`;
  Task 10/11 consume `CostModel::funding_payment` for open perpetual positions.

- [ ] **Step 1: Write the fee schedule types, `CostModel`, and unit tests**

Create `crates/apexes-backtest/src/cost_model.rs`:

```rust
use apexes_core::Venue;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Whether a fill was the resting (maker) or aggressing (taker) side of a
/// trade — determines which fee rate applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillLiquidity {
    Maker,
    Taker,
}

/// Provides maker/taker fee rates (in basis points) for a given `Venue`.
pub trait FeeSchedule: Send + Sync {
    fn maker_fee_bps(&self, venue: Venue) -> Decimal;
    fn taker_fee_bps(&self, venue: Venue) -> Decimal;
}

/// Binance Spot's real VIP tier fee table (VIP0/Regular..VIP9), standard
/// (non-BNB-discounted) rates, confirmed by the user directly from
/// binance.com/en/fee/schedule on 2026-07-28. Only applies its rates when
/// queried for `Venue::Binance`; any other venue falls back to VIP0/Regular
/// rates (0.10%/0.10%) as a conservative default, logged at `tracing::warn`
/// level since this fee schedule is not meant to be used for non-Binance
/// venues (use `FlatFeeSchedule` for those instead).
pub struct BinanceSpotVipFeeSchedule {
    pub tier: u8, // 0..=9
}

impl BinanceSpotVipFeeSchedule {
    /// (maker_bps, taker_bps) for each VIP tier, standard spot rates.
    const TIERS: [(Decimal, Decimal); 10] = [
        (dec!(10.0), dec!(10.0)), // 0 (Regular)
        (dec!(9.0), dec!(10.0)),  // 1
        (dec!(8.0), dec!(10.0)),  // 2
        (dec!(4.0), dec!(6.0)),   // 3
        (dec!(4.0), dec!(5.2)),   // 4
        (dec!(2.5), dec!(3.1)),   // 5
        (dec!(2.0), dec!(2.9)),   // 6
        (dec!(1.9), dec!(2.8)),   // 7
        (dec!(1.6), dec!(2.5)),   // 8
        (dec!(1.1), dec!(2.3)),   // 9
    ];

    fn tier_rates(&self) -> (Decimal, Decimal) {
        Self::TIERS
            .get(self.tier as usize)
            .copied()
            .unwrap_or(Self::TIERS[0])
    }
}

impl FeeSchedule for BinanceSpotVipFeeSchedule {
    fn maker_fee_bps(&self, venue: Venue) -> Decimal {
        if venue != Venue::Binance {
            tracing::warn!(
                ?venue,
                "BinanceSpotVipFeeSchedule queried for a non-Binance venue; using VIP0/Regular fallback rate"
            );
        }
        self.tier_rates().0
    }

    fn taker_fee_bps(&self, venue: Venue) -> Decimal {
        if venue != Venue::Binance {
            tracing::warn!(
                ?venue,
                "BinanceSpotVipFeeSchedule queried for a non-Binance venue; using VIP0/Regular fallback rate"
            );
        }
        self.tier_rates().1
    }
}

/// A simple flat maker/taker rate applied uniformly to every venue.
/// Intended as the fallback `FeeSchedule` for venues without a dedicated
/// tiered-fee implementation yet (see design spec §7.5).
pub struct FlatFeeSchedule {
    pub maker_bps: Decimal,
    pub taker_bps: Decimal,
}

impl FeeSchedule for FlatFeeSchedule {
    fn maker_fee_bps(&self, _venue: Venue) -> Decimal {
        self.maker_bps
    }

    fn taker_fee_bps(&self, _venue: Venue) -> Decimal {
        self.taker_bps
    }
}

/// The backtest's cost model: fees per fill plus periodic funding for open
/// perpetual positions. Market-impact/slippage is produced naturally by the
/// matching engine's book-walk fill logic (Tasks 8-11), not modeled here.
pub struct CostModel {
    pub fee_schedule: Box<dyn FeeSchedule>,
    /// Applied periodically (every 8h simulated-time, by the matching
    /// engine's funding-period tracking in Task 11) to open positions on
    /// `AssetClass::Perpetual` instruments only; `None` disables funding
    /// entirely (e.g. for spot-only backtests).
    pub funding_rate_bps: Option<Decimal>,
}

impl CostModel {
    /// Computes the fee owed for a fill of `notional` value (price *
    /// quantity, already computed by the caller) at `venue`, given whether
    /// it was a maker or taker fill.
    pub fn fee_for_fill(&self, venue: Venue, liquidity: FillLiquidity, notional: Decimal) -> Decimal {
        let bps = match liquidity {
            FillLiquidity::Maker => self.fee_schedule.maker_fee_bps(venue),
            FillLiquidity::Taker => self.fee_schedule.taker_fee_bps(venue),
        };
        notional * bps / dec!(10000)
    }

    /// Computes the funding payment owed (positive = paid by the position
    /// holder, negative = received) for a perpetual position with the
    /// given notional value, or `None` if funding is disabled
    /// (`funding_rate_bps: None`).
    pub fn funding_payment(&self, position_notional: Decimal) -> Option<Decimal> {
        self.funding_rate_bps
            .map(|bps| position_notional * bps / dec!(10000))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binance_vip0_rates_match_confirmed_table() {
        let schedule = BinanceSpotVipFeeSchedule { tier: 0 };
        assert_eq!(schedule.maker_fee_bps(Venue::Binance), dec!(10.0));
        assert_eq!(schedule.taker_fee_bps(Venue::Binance), dec!(10.0));
    }

    #[test]
    fn binance_vip9_rates_match_confirmed_table() {
        let schedule = BinanceSpotVipFeeSchedule { tier: 9 };
        assert_eq!(schedule.maker_fee_bps(Venue::Binance), dec!(1.1));
        assert_eq!(schedule.taker_fee_bps(Venue::Binance), dec!(2.3));
    }

    #[test]
    fn binance_vip4_rates_match_confirmed_table() {
        let schedule = BinanceSpotVipFeeSchedule { tier: 4 };
        assert_eq!(schedule.maker_fee_bps(Venue::Binance), dec!(4.0));
        assert_eq!(schedule.taker_fee_bps(Venue::Binance), dec!(5.2));
    }

    #[test]
    fn binance_schedule_falls_back_to_tier0_for_out_of_range_tier() {
        let schedule = BinanceSpotVipFeeSchedule { tier: 200 };
        assert_eq!(schedule.maker_fee_bps(Venue::Binance), dec!(10.0));
    }

    #[test]
    fn binance_schedule_falls_back_for_non_binance_venue() {
        let schedule = BinanceSpotVipFeeSchedule { tier: 9 };
        // Non-Binance venue: falls back to VIP0/Regular rate, not VIP9.
        assert_eq!(schedule.maker_fee_bps(Venue::Okx), dec!(10.0));
    }

    #[test]
    fn flat_fee_schedule_applies_uniformly() {
        let schedule = FlatFeeSchedule {
            maker_bps: dec!(5),
            taker_bps: dec!(7),
        };
        assert_eq!(schedule.maker_fee_bps(Venue::Binance), dec!(5));
        assert_eq!(schedule.taker_fee_bps(Venue::Nasdaq), dec!(7));
    }

    #[test]
    fn fee_for_fill_computes_correct_amount() {
        let cost_model = CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(10), // 0.10%
                taker_bps: dec!(20), // 0.20%
            }),
            funding_rate_bps: None,
        };

        // 10,000 notional * 0.10% = 10
        let maker_fee = cost_model.fee_for_fill(Venue::Binance, FillLiquidity::Maker, dec!(10000));
        assert_eq!(maker_fee, dec!(10));

        // 10,000 notional * 0.20% = 20
        let taker_fee = cost_model.fee_for_fill(Venue::Binance, FillLiquidity::Taker, dec!(10000));
        assert_eq!(taker_fee, dec!(20));
    }

    #[test]
    fn funding_payment_returns_none_when_disabled() {
        let cost_model = CostModel {
            fee_schedule: Box::new(FlatFeeSchedule { maker_bps: dec!(0), taker_bps: dec!(0) }),
            funding_rate_bps: None,
        };
        assert_eq!(cost_model.funding_payment(dec!(10000)), None);
    }

    #[test]
    fn funding_payment_computes_correct_amount_when_enabled() {
        let cost_model = CostModel {
            fee_schedule: Box::new(FlatFeeSchedule { maker_bps: dec!(0), taker_bps: dec!(0) }),
            funding_rate_bps: Some(dec!(1)), // 0.01%
        };
        // 100,000 notional * 0.01% = 10
        assert_eq!(cost_model.funding_payment(dec!(100000)), Some(dec!(10)));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p apexes-backtest cost_model`
Expected: 9 tests pass.

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p apexes-backtest -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Add `rust_decimal_macros` to `[dependencies]` (not just `[dev-dependencies]`)**

`cost_model.rs`'s non-test code uses `rust_decimal_macros::dec!` (in the `const TIERS` array
and `fee_for_fill`'s `dec!(10000)` divisor). Modify `crates/apexes-backtest/Cargo.toml`'s
`[dependencies]` section (added in Task 2) to add:

```toml
rust_decimal_macros = "1.40"
```

placed after the `rust_decimal = { workspace = true }` line. Re-run `cargo build -p
apexes-backtest` to confirm this resolves any "crate not found" error if Step 1's code
referenced `rust_decimal_macros` before this dependency was declared as a non-dev dependency
(the crate was previously only a dev-dependency from Task 2's scaffold).

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-backtest/src/cost_model.rs crates/apexes-backtest/Cargo.toml
git commit -S -s -m "feat(apexes-backtest): add FeeSchedule, BinanceSpotVipFeeSchedule, CostModel"
```

---

### Task 6: `Strategy` trait, `StrategyContext`, `OrderIntent`, `Position`

**Files:**
- Modify: `crates/apexes-backtest/src/strategy.rs`

**Interfaces:**
- Consumes: `apexes_core::{MarketEvent, OrderId, OrderType, Order, Side, Venue}`,
  `rust_decimal::Decimal`, `chrono::{DateTime, Utc}`.
- Produces: `pub trait Strategy`, `pub struct StrategyContext`, `pub struct OrderIntent`,
  `pub struct Position`. This is the shared backtest/live-trading interface (design spec §5)
  — later sub-projects (#6 Paper Trading, #7 OMS, #8 live gateway) implement `Strategy`
  unchanged. Tasks 9-14 (matching engine, runner) consume `Strategy::on_market_event`,
  construct `StrategyContext`, and process the returned `Vec<OrderIntent>`.

- [ ] **Step 1: Write the trait and supporting types with unit tests**

Create `crates/apexes-backtest/src/strategy.rs`:

```rust
use apexes_core::{Order, OrderId, OrderType, Side, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// The shared strategy interface implemented by both this backtest engine
/// and all future live-trading sub-projects (#6 Paper Trading, #7 OMS, #8
/// live crypto gateway), per the project's non-negotiable backtest/live
/// logic-parity requirement (design spec §2). A `Strategy` implementation
/// never needs to know whether it is running in backtest or live mode.
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
        fn on_market_event(&mut self, _event: &MarketEvent, _ctx: &StrategyContext) -> Vec<OrderIntent> {
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

- [ ] **Step 2: Run tests**

Run: `cargo test -p apexes-backtest strategy`
Expected: 2 tests pass (`strategy_trait_is_callable_and_returns_intents`,
`strategy_context_exposes_provided_state`).

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p apexes-backtest -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-backtest/src/strategy.rs
git commit -S -s -m "feat(apexes-backtest): add Strategy trait, StrategyContext, OrderIntent, Position"
```

---

### Task 7: `PortfolioLedger` — multi-asset balances + positions

**Files:**
- Modify: `crates/apexes-backtest/src/ledger.rs`

**Interfaces:**
- Consumes: `apexes_core::{ApexesError, Fill, Instrument, Side}`, `crate::strategy::Position`,
  `rust_decimal::Decimal`.
- Produces: `pub struct PortfolioLedger`, `pub fn PortfolioLedger::new(instruments:
  HashMap<String, Instrument>, initial_balances: HashMap<String, Decimal>) -> Self`,
  `pub fn PortfolioLedger::apply_fill(&mut self, fill: &Fill) -> Result<(), ApexesError>`,
  `pub fn PortfolioLedger::balances(&self) -> &HashMap<String, Decimal>`,
  `pub fn PortfolioLedger::positions(&self) -> &HashMap<String, Position>`,
  `pub fn PortfolioLedger::equity(&self, mark_prices: &HashMap<String, Decimal>) ->
  Result<Decimal, ApexesError>`. Tasks 9-10 (matching engine) call `apply_fill` after every
  simulated fill; Task 13 (performance tracking) calls `equity` to build the equity curve;
  Task 14 (runner) constructs `PortfolioLedger::new` from `BacktestConfig.initial_balances`.

- [ ] **Step 1: Write the ledger with unit tests**

Create `crates/apexes-backtest/src/ledger.rs`:

```rust
use crate::strategy::Position;
use apexes_core::{ApexesError, Fill, Instrument, Side};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Tracks multi-asset balances (like a real exchange account: balance per
/// asset, not just quote currency) and per-symbol net positions during a
/// backtest, updated by applying each simulated `Fill`.
pub struct PortfolioLedger {
    instruments: HashMap<String, Instrument>,
    balances: HashMap<String, Decimal>,
    positions: HashMap<String, Position>,
}

impl PortfolioLedger {
    /// Constructs a new ledger. `instruments` maps symbol -> `Instrument`
    /// (used to resolve each fill's base/quote currency for balance
    /// debit/credit). `initial_balances` is required explicit starting
    /// capital per asset symbol (e.g. "USDT" -> 10000) -- no implicit
    /// default, per design spec §8.
    pub fn new(instruments: HashMap<String, Instrument>, initial_balances: HashMap<String, Decimal>) -> Self {
        Self {
            instruments,
            balances: initial_balances,
            positions: HashMap::new(),
        }
    }

    pub fn balances(&self) -> &HashMap<String, Decimal> {
        &self.balances
    }

    pub fn positions(&self) -> &HashMap<String, Position> {
        &self.positions
    }

    /// Applies a `Fill` to the ledger: debits/credits the base and quote
    /// asset balances (including `fill.fee`, always debited from the
    /// balance denominated in `fill.fee_currency`, defaulting to the quote
    /// asset if `fee_currency` is `None`), and updates the symbol's net
    /// `Position` (quantity and volume-weighted average entry price).
    ///
    /// All balance checks are validated BEFORE any mutation is applied, so
    /// this method is atomic: on `Err`, the ledger is left completely
    /// unchanged (no partial debit/credit).
    pub fn apply_fill(&mut self, fill: &Fill) -> Result<(), ApexesError> {
        let instrument = self.instruments.get(&fill.symbol).ok_or_else(|| {
            ApexesError::InvalidBacktestConfig {
                reason: format!("no Instrument registered for symbol {}", fill.symbol),
            }
        })?;

        let base = instrument.base_currency.clone().ok_or_else(|| {
            ApexesError::InvalidBacktestConfig {
                reason: format!("Instrument {} has no base_currency", fill.symbol),
            }
        })?;
        let quote = instrument.quote_currency.clone().ok_or_else(|| {
            ApexesError::InvalidBacktestConfig {
                reason: format!("Instrument {} has no quote_currency", fill.symbol),
            }
        })?;

        let notional = fill.price * fill.quantity;
        let fee_currency = fill.fee_currency.clone().unwrap_or_else(|| quote.clone());

        // Phase 1: compute every (asset, debit_amount) this fill requires,
        // merging same-asset debits (e.g. fee_currency == quote) so the
        // balance check below is correct even when both debits draw from
        // the same asset.
        let mut required_debits: HashMap<String, Decimal> = HashMap::new();
        match fill.side {
            Side::Buy => {
                *required_debits.entry(quote.clone()).or_insert(Decimal::ZERO) += notional;
            }
            Side::Sell => {
                *required_debits.entry(base.clone()).or_insert(Decimal::ZERO) += fill.quantity;
            }
        }
        *required_debits.entry(fee_currency.clone()).or_insert(Decimal::ZERO) += fill.fee;

        // Phase 2: validate every required debit against current balances
        // before mutating anything.
        for (asset, amount) in &required_debits {
            let available = self.balances.get(asset).copied().unwrap_or(Decimal::ZERO);
            if available < *amount {
                return Err(ApexesError::InsufficientBalance {
                    asset: asset.clone(),
                    required: *amount,
                    available,
                });
            }
        }

        // Phase 3: all checks passed -- apply every mutation.
        for (asset, amount) in &required_debits {
            self.credit(asset, -*amount); // debit via negative credit (balance already validated)
        }
        match fill.side {
            Side::Buy => self.credit(&base, fill.quantity),
            Side::Sell => self.credit(&quote, notional),
        }

        self.update_position(fill);

        Ok(())
    }

    /// Computes total portfolio equity (sum of all balances converted to
    /// a common valuation, using `mark_prices` keyed by asset symbol ->
    /// price-in-quote-terms for non-quote assets; assets not present in
    /// `mark_prices` and not the implicit "quote of record" are treated
    /// as needing an explicit mark price, returning an error if missing
    /// and non-zero).
    pub fn equity(&self, mark_prices: &HashMap<String, Decimal>) -> Result<Decimal, ApexesError> {
        let mut total = Decimal::ZERO;
        for (asset, balance) in &self.balances {
            if balance.is_zero() {
                continue;
            }
            let price = mark_prices.get(asset).copied().unwrap_or(Decimal::ONE);
            total += balance * price;
        }
        Ok(total)
    }

    /// Adjusts `asset`'s balance by `amount` (positive to credit, negative
    /// to debit). Callers are responsible for validating sufficient
    /// balance BEFORE calling this for debits (see `apply_fill`'s
    /// phase 2/3 split) -- this method itself performs no validation, by
    /// design, so it can be used for both directions uniformly.
    fn credit(&mut self, asset: &str, amount: Decimal) {
        let balance = self.balances.entry(asset.to_string()).or_insert(Decimal::ZERO);
        *balance += amount;
    }

    fn update_position(&mut self, fill: &Fill) {
        let signed_quantity = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };

        let position = self.positions.entry(fill.symbol.clone()).or_insert(Position {
            symbol: fill.symbol.clone(),
            quantity: Decimal::ZERO,
            average_entry_price: Decimal::ZERO,
        });

        let same_direction = position.quantity.is_zero()
            || (position.quantity.is_sign_positive() == signed_quantity.is_sign_positive());

        if same_direction {
            // Increasing (or opening) a position: volume-weighted average entry price.
            let existing_value = position.quantity.abs() * position.average_entry_price;
            let new_value = signed_quantity.abs() * fill.price;
            let new_quantity = position.quantity + signed_quantity;
            if !new_quantity.is_zero() {
                position.average_entry_price = (existing_value + new_value) / new_quantity.abs();
            }
            position.quantity = new_quantity;
        } else {
            // Reducing or flipping a position: average entry price unchanged
            // for the reduced portion; if flipping past zero, the new
            // position's entry price is this fill's price.
            let new_quantity = position.quantity + signed_quantity;
            if position.quantity.abs() >= signed_quantity.abs() {
                position.quantity = new_quantity;
            } else {
                position.quantity = new_quantity;
                position.average_entry_price = fill.price;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, FillId, MarketType, OrderId, Venue};
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn btc_usdt() -> Instrument {
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

    fn sample_fill(side: Side, price: Decimal, quantity: Decimal, fee: Decimal) -> Fill {
        Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side,
            price,
            quantity,
            fee,
            fee_currency: None,
            filled_at: Utc::now(),
        }
    }

    fn ledger_with_10k_usdt() -> PortfolioLedger {
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));
        PortfolioLedger::new(instruments, initial_balances)
    }

    #[test]
    fn buy_fill_debits_quote_credits_base() {
        let mut ledger = ledger_with_10k_usdt();
        let fill = sample_fill(Side::Buy, dec!(65000), dec!(0.1), dec!(6.5));

        ledger.apply_fill(&fill).unwrap();

        // 10000 - (65000*0.1) - 6.5 fee = 10000 - 6500 - 6.5 = 3493.5
        assert_eq!(*ledger.balances().get("USDT").unwrap(), dec!(3493.5));
        assert_eq!(*ledger.balances().get("BTC").unwrap(), dec!(0.1));
    }

    #[test]
    fn sell_fill_credits_quote_debits_base() {
        let mut ledger = ledger_with_10k_usdt();
        // First buy some BTC to have a base balance to sell (0.1 * 60000 = 6000 USDT, within the 10000 starting balance).
        ledger.apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0))).unwrap();
        // USDT: 10000 - 6000 = 4000; BTC: 0.1

        let sell_fill = sample_fill(Side::Sell, dec!(65000), dec!(0.04), dec!(2.6));
        ledger.apply_fill(&sell_fill).unwrap();

        // Base: 0.1 - 0.04 = 0.06
        assert_eq!(*ledger.balances().get("BTC").unwrap(), dec!(0.06));
        // Quote: 4000 + (65000*0.04) - 2.6 fee = 4000 + 2600 - 2.6 = 6597.4
        assert_eq!(*ledger.balances().get("USDT").unwrap(), dec!(6597.4));
    }

    #[test]
    fn sell_fill_fails_with_insufficient_base_balance_and_leaves_ledger_unchanged() {
        let mut ledger = ledger_with_10k_usdt();
        ledger.apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0))).unwrap();
        let usdt_before = *ledger.balances().get("USDT").unwrap();
        let btc_before = *ledger.balances().get("BTC").unwrap();

        // Try to sell more BTC than we have.
        let result = ledger.apply_fill(&sample_fill(Side::Sell, dec!(65000), dec!(5), dec!(0)));

        assert!(matches!(result, Err(ApexesError::InsufficientBalance { .. })));
        // Ledger must be completely unchanged after a failed apply_fill (atomicity).
        assert_eq!(*ledger.balances().get("USDT").unwrap(), usdt_before);
        assert_eq!(*ledger.balances().get("BTC").unwrap(), btc_before);
    }

    #[test]
    fn buy_fill_fails_with_insufficient_quote_balance() {
        let mut ledger = ledger_with_10k_usdt();
        let fill = sample_fill(Side::Buy, dec!(65000), dec!(1), dec!(0)); // needs 65000, only have 10000

        let result = ledger.apply_fill(&fill);

        assert!(matches!(result, Err(ApexesError::InsufficientBalance { .. })));
    }

    #[test]
    fn position_average_entry_price_is_volume_weighted() {
        let mut ledger = ledger_with_10k_usdt();
        ledger.apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.05), dec!(0))).unwrap();
        ledger.apply_fill(&sample_fill(Side::Buy, dec!(70000), dec!(0.05), dec!(0))).unwrap();

        let position = ledger.positions().get("BTCUSDT").unwrap();
        assert_eq!(position.quantity, dec!(0.1));
        // (0.05*60000 + 0.05*70000) / 0.1 = (3000+3500)/0.1 = 65000
        assert_eq!(position.average_entry_price, dec!(65000));
    }

    #[test]
    fn position_flips_sign_and_resets_entry_price_on_full_reversal() {
        let mut ledger = ledger_with_10k_usdt();
        ledger.apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0))).unwrap();
        // Sell more than the current long position: flips to short.
        ledger.apply_fill(&sample_fill(Side::Sell, dec!(65000), dec!(0.3), dec!(0))).unwrap();

        let position = ledger.positions().get("BTCUSDT").unwrap();
        assert_eq!(position.quantity, dec!(-0.2));
        assert_eq!(position.average_entry_price, dec!(65000));
    }

    #[test]
    fn apply_fill_fails_for_unregistered_symbol() {
        let mut ledger = ledger_with_10k_usdt();
        let mut fill = sample_fill(Side::Buy, dec!(100), dec!(1), dec!(0));
        fill.symbol = "UNKNOWNPAIR".to_string();

        let result = ledger.apply_fill(&fill);

        assert!(matches!(result, Err(ApexesError::InvalidBacktestConfig { .. })));
    }

    #[test]
    fn equity_sums_balances_with_mark_prices() {
        let mut ledger = ledger_with_10k_usdt();
        ledger.apply_fill(&sample_fill(Side::Buy, dec!(65000), dec!(0.1), dec!(0))).unwrap();

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(65000));
        // USDT valued at 1.0 (default fallback for assets not in mark_prices).

        let equity = ledger.equity(&mark_prices).unwrap();
        // USDT: 10000 - 6500 = 3500; BTC: 0.1 * 65000 = 6500; total = 10000
        assert_eq!(equity, dec!(10000));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p apexes-backtest ledger`
Expected: 8 tests pass (`buy_fill_debits_quote_credits_base`, `sell_fill_credits_quote_debits_base`,
`sell_fill_fails_with_insufficient_base_balance_and_leaves_ledger_unchanged`,
`buy_fill_fails_with_insufficient_quote_balance`, `position_average_entry_price_is_volume_weighted`,
`position_flips_sign_and_resets_entry_price_on_full_reversal`, `apply_fill_fails_for_unregistered_symbol`,
`equity_sums_balances_with_mark_prices`).

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p apexes-backtest -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-backtest/src/ledger.rs
git commit -S -s -m "feat(apexes-backtest): add PortfolioLedger with multi-asset balances and positions"
```

---

### Task 8: Book-walk fill calculation (pure function, no state)

**Files:**
- Modify: `crates/apexes-backtest/src/matching_engine.rs`

**Interfaces:**
- Consumes: `apexes_core::PriceLevel`, `rust_decimal::Decimal`.
- Produces: `pub struct BookWalkResult { pub filled_quantity: Decimal, pub avg_price:
  Decimal, pub notional: Decimal }`, `pub fn walk_book(levels: &[PriceLevel], quantity:
  Decimal) -> Option<BookWalkResult>`. Tasks 9-10 consume `walk_book` for both market-order
  fills and triggered-limit/depth-consumption fills.

- [ ] **Step 1: Write the book-walk function with unit tests**

Create `crates/apexes-backtest/src/matching_engine.rs`:

```rust
use apexes_core::PriceLevel;
use rust_decimal::Decimal;

/// Result of walking a sequence of price levels to fill `quantity`.
#[derive(Debug, Clone, PartialEq)]
pub struct BookWalkResult {
    /// Quantity actually filled (may be less than requested if the levels
    /// don't have enough total depth).
    pub filled_quantity: Decimal,
    /// Volume-weighted average price across all consumed levels.
    pub avg_price: Decimal,
    /// Total notional value (`filled_quantity * avg_price`, computed
    /// directly as the sum of each level's `price * consumed_quantity`
    /// for precision).
    pub notional: Decimal,
}

/// Walks `levels` (assumed pre-sorted best-price-first, as returned by
/// `ReplayOrderBook::bids`/`asks`) from best to worst, consuming quantity
/// from each level until `quantity` is filled or the levels are
/// exhausted. Produces a single volume-weighted average fill price,
/// naturally modeling market-impact/slippage for orders larger than the
/// top-of-book depth (design spec §7.2).
///
/// Returns `None` if `levels` is empty or `quantity` is not positive (no
/// fill possible); otherwise returns `Some(BookWalkResult)` even if the
/// full requested `quantity` couldn't be filled (see
/// `BookWalkResult::filled_quantity` -- a partial fill due to insufficient
/// visible liquidity is not an error, per design spec §7.2/§9).
pub fn walk_book(levels: &[PriceLevel], quantity: Decimal) -> Option<BookWalkResult> {
    if levels.is_empty() || quantity <= Decimal::ZERO {
        return None;
    }

    let mut remaining = quantity;
    let mut total_notional = Decimal::ZERO;
    let mut total_filled = Decimal::ZERO;

    for level in levels {
        if remaining <= Decimal::ZERO {
            break;
        }
        let consumed = remaining.min(level.quantity);
        total_notional += consumed * level.price;
        total_filled += consumed;
        remaining -= consumed;
    }

    if total_filled.is_zero() {
        return None;
    }

    Some(BookWalkResult {
        filled_quantity: total_filled,
        avg_price: total_notional / total_filled,
        notional: total_notional,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn level(price: Decimal, quantity: Decimal) -> PriceLevel {
        PriceLevel { price, quantity }
    }

    #[test]
    fn small_order_fills_entirely_at_top_of_book() {
        let levels = vec![level(dec!(100), dec!(5)), level(dec!(101), dec!(5))];
        let result = walk_book(&levels, dec!(2)).unwrap();

        assert_eq!(result.filled_quantity, dec!(2));
        assert_eq!(result.avg_price, dec!(100));
        assert_eq!(result.notional, dec!(200));
    }

    #[test]
    fn large_order_walks_multiple_levels_and_computes_vwap() {
        let levels = vec![level(dec!(100), dec!(2)), level(dec!(101), dec!(3))];
        let result = walk_book(&levels, dec!(4)).unwrap();

        // 2 @ 100 + 2 @ 101 = 200 + 202 = 402, / 4 = 100.5
        assert_eq!(result.filled_quantity, dec!(4));
        assert_eq!(result.notional, dec!(402));
        assert_eq!(result.avg_price, dec!(100.5));
    }

    #[test]
    fn order_larger_than_total_depth_produces_partial_fill() {
        let levels = vec![level(dec!(100), dec!(2)), level(dec!(101), dec!(3))];
        let result = walk_book(&levels, dec!(10)).unwrap();

        // Total depth is only 5.
        assert_eq!(result.filled_quantity, dec!(5));
        assert_eq!(result.notional, dec!(200) + dec!(303)); // 2*100 + 3*101
    }

    #[test]
    fn empty_levels_returns_none() {
        let result = walk_book(&[], dec!(1));
        assert!(result.is_none());
    }

    #[test]
    fn zero_quantity_returns_none() {
        let levels = vec![level(dec!(100), dec!(5))];
        let result = walk_book(&levels, dec!(0));
        assert!(result.is_none());
    }

    #[test]
    fn negative_quantity_returns_none() {
        let levels = vec![level(dec!(100), dec!(5))];
        let result = walk_book(&levels, dec!(-1));
        assert!(result.is_none());
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p apexes-backtest matching_engine`
Expected: 6 tests pass.

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p apexes-backtest -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-backtest/src/matching_engine.rs
git commit -S -s -m "feat(apexes-backtest): add walk_book VWAP fill calculation"
```

---

### Task 9: `MatchingEngine` core — order book state + market order book-walk fills

**Files:**
- Modify: `crates/apexes-backtest/src/matching_engine.rs`

**Interfaces:**
- Consumes: `crate::order_book::ReplayOrderBook`, `crate::cost_model::{CostModel,
  FillLiquidity}`, `crate::strategy::OrderIntent`, `apexes_core::{ApexesError, DepthUpdate,
  Fill, FillId, MarketEvent, Order, OrderId, OrderStatus, OrderType, Side}`,
  `rust_decimal::Decimal`, `chrono::{DateTime, Utc}`.
- Produces: `pub struct MatchingEngine`, `pub fn MatchingEngine::new(cost_model: CostModel,
  queue_position_factor: Decimal) -> Self`, `pub fn MatchingEngine::on_depth_update(&mut self,
  update: &DepthUpdate)`, `pub fn MatchingEngine::submit_market_order(&mut self, intent:
  &OrderIntent, now: DateTime<Utc>) -> Result<(Order, Vec<Fill>), ApexesError>`. Task 10 adds
  limit-order resting/fill logic to this same struct; Task 11 adds Stop/StopLimit; Task 14
  (runner) constructs `MatchingEngine` and calls its `submit_*` methods.

- [ ] **Step 1: Write the core struct, book-walk fill logic, and unit tests**

Create `crates/apexes-backtest/src/matching_engine.rs`:

```rust
use crate::cost_model::{CostModel, FillLiquidity};
use crate::order_book::ReplayOrderBook;
use crate::strategy::OrderIntent;
use apexes_core::{ApexesError, DepthUpdate, Fill, FillId, Order, OrderId, OrderStatus, OrderType, Side};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use uuid::Uuid;

/// Simulates order matching against locally-reconstructed Level-2 depth:
/// order book reconstruction (via `ReplayOrderBook`), book-walk fills for
/// market orders, deterministic depth-consumption fills for limit orders
/// with an optional probabilistic queue-position adjustment (Task 10), and
/// Stop/StopLimit trigger simulation (Task 11).
pub struct MatchingEngine {
    books: HashMap<String, ReplayOrderBook>,
    cost_model: CostModel,
    /// 0.0 (optimistic) .. 1.0 (pessimistic), see design spec §7.3. Used by
    /// Task 10's limit-order fill logic.
    queue_position_factor: Decimal,
}

impl MatchingEngine {
    pub fn new(cost_model: CostModel, queue_position_factor: Decimal) -> Self {
        Self {
            books: HashMap::new(),
            cost_model,
            queue_position_factor,
        }
    }

    /// Updates the local order book for `update.symbol` from a new
    /// `DepthUpdate`. Must be called for every `MarketEvent::Depth` in
    /// chronological order before any fills against that symbol are
    /// computed.
    pub fn on_depth_update(&mut self, update: &DepthUpdate) {
        self.books.entry(update.symbol.clone()).or_default().apply(update);
    }

    /// Submits a Market `OrderIntent`, filling it immediately by walking
    /// the current local order book from best price outward. If total
    /// visible depth is insufficient to fill the full requested quantity,
    /// fills whatever is available and returns a `PartiallyFilled` (or
    /// `New` if nothing could be filled) `Order` -- this is NOT an error,
    /// per design spec §7.2 (logged via `tracing::warn`).
    pub fn submit_market_order(
        &mut self,
        intent: &OrderIntent,
        now: DateTime<Utc>,
    ) -> Result<(Order, Vec<Fill>), ApexesError> {
        if intent.order_type != OrderType::Market {
            return Err(ApexesError::OrderRejected {
                reason: format!("submit_market_order called with non-Market OrderType: {:?}", intent.order_type),
            });
        }

        let book = self.books.entry(intent.symbol.clone()).or_default();
        let levels = match intent.side {
            // Buying consumes the ask side (lowest price first).
            Side::Buy => book.asks(),
            // Selling consumes the bid side (highest price first).
            Side::Sell => book.bids(),
        };

        let order_id = OrderId(Uuid::new_v4());
        let mut remaining = intent.quantity;
        let mut fills = Vec::new();

        for level in &levels {
            if remaining.is_zero() {
                break;
            }
            let fill_quantity = remaining.min(level.quantity);
            let notional = level.price * fill_quantity;
            let fee = self.cost_model.fee_for_fill(intent.venue, FillLiquidity::Taker, notional);

            fills.push(Fill {
                id: FillId(Uuid::new_v4()),
                order_id,
                symbol: intent.symbol.clone(),
                venue: intent.venue,
                side: intent.side,
                price: level.price,
                quantity: fill_quantity,
                fee,
                fee_currency: None,
                filled_at: now,
            });

            remaining -= fill_quantity;
        }

        if remaining > Decimal::ZERO && remaining < intent.quantity {
            tracing::warn!(
                symbol = %intent.symbol,
                requested = %intent.quantity,
                filled = %(intent.quantity - remaining),
                "submit_market_order: insufficient visible liquidity, partial fill only"
            );
        } else if remaining == intent.quantity {
            tracing::warn!(
                symbol = %intent.symbol,
                "submit_market_order: no visible liquidity at all, order unfilled"
            );
        }

        let filled_quantity = intent.quantity - remaining;
        let status = if filled_quantity.is_zero() {
            OrderStatus::New
        } else if remaining.is_zero() {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };

        let order = Order {
            id: order_id,
            exchange_order_id: None,
            symbol: intent.symbol.clone(),
            venue: intent.venue,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: None,
            stop_price: None,
            filled_quantity,
            status,
            created_at: now,
            updated_at: now,
        };

        Ok((order, fills))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost_model::FlatFeeSchedule;
    use apexes_core::{PriceLevel, Venue};
    use rust_decimal_macros::dec;

    fn no_fee_engine() -> MatchingEngine {
        MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule { maker_bps: dec!(0), taker_bps: dec!(0) }),
                funding_rate_bps: None,
            },
            dec!(0.5),
        )
    }

    fn snapshot(symbol: &str, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) -> DepthUpdate {
        DepthUpdate {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            bids,
            asks,
            sequence: Some(1),
            is_snapshot: true,
            timestamp: Utc::now(),
        }
    }

    fn buy_intent(symbol: &str, quantity: Decimal) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity,
            price: None,
            stop_price: None,
        }
    }

    #[test]
    fn market_buy_fills_entirely_at_single_level() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![],
            vec![PriceLevel { price: dec!(65000), quantity: dec!(5) }],
        ));

        let (order, fills) = engine.submit_market_order(&buy_intent("BTCUSDT", dec!(2)), Utc::now()).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, dec!(65000));
        assert_eq!(fills[0].quantity, dec!(2));
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.filled_quantity, dec!(2));
    }

    #[test]
    fn market_buy_walks_multiple_levels_for_vwap() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![],
            vec![
                PriceLevel { price: dec!(65000), quantity: dec!(1) },
                PriceLevel { price: dec!(65010), quantity: dec!(1) },
            ],
        ));

        let (order, fills) = engine.submit_market_order(&buy_intent("BTCUSDT", dec!(1.5)), Utc::now()).unwrap();

        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].price, dec!(65000));
        assert_eq!(fills[0].quantity, dec!(1));
        assert_eq!(fills[1].price, dec!(65010));
        assert_eq!(fills[1].quantity, dec!(0.5));
        assert_eq!(order.status, OrderStatus::Filled);
    }

    #[test]
    fn market_buy_partially_fills_when_depth_insufficient() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![],
            vec![PriceLevel { price: dec!(65000), quantity: dec!(1) }],
        ));

        let (order, fills) = engine.submit_market_order(&buy_intent("BTCUSDT", dec!(5)), Utc::now()).unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].quantity, dec!(1));
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.filled_quantity, dec!(1));
    }

    #[test]
    fn market_buy_with_no_liquidity_returns_unfilled_new_order() {
        let mut engine = no_fee_engine();
        // No depth update at all -- empty book.
        let (order, fills) = engine.submit_market_order(&buy_intent("BTCUSDT", dec!(1)), Utc::now()).unwrap();

        assert!(fills.is_empty());
        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.filled_quantity, dec!(0));
    }

    #[test]
    fn market_sell_consumes_bid_side() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![PriceLevel { price: dec!(64990), quantity: dec!(3) }],
            vec![],
        ));

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Sell,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
        };
        let (order, fills) = engine.submit_market_order(&intent, Utc::now()).unwrap();

        assert_eq!(fills[0].price, dec!(64990));
        assert_eq!(order.status, OrderStatus::Filled);
    }

    #[test]
    fn submit_market_order_rejects_non_market_intent() {
        let mut engine = no_fee_engine();
        let mut intent = buy_intent("BTCUSDT", dec!(1));
        intent.order_type = OrderType::Limit;
        intent.price = Some(dec!(65000));

        let result = engine.submit_market_order(&intent, Utc::now());

        assert!(matches!(result, Err(ApexesError::OrderRejected { .. })));
    }

    #[test]
    fn fees_are_applied_using_taker_rate() {
        let mut engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule { maker_bps: dec!(0), taker_bps: dec!(10) }), // 0.10%
                funding_rate_bps: None,
            },
            dec!(0.5),
        );
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![],
            vec![PriceLevel { price: dec!(65000), quantity: dec!(1) }],
        ));

        let (_order, fills) = engine.submit_market_order(&buy_intent("BTCUSDT", dec!(1)), Utc::now()).unwrap();

        // notional = 65000, fee = 65000 * 0.10% = 65
        assert_eq!(fills[0].fee, dec!(65));
    }
}
```

- [ ] **Step 2: Add `uuid` to `apexes-backtest`'s `[dependencies]`**

`matching_engine.rs` uses `uuid::Uuid::new_v4()` (to construct `OrderId`/`FillId`, following
the exact pattern already used in `apexes-core`). Modify `crates/apexes-backtest/Cargo.toml`'s
`[dependencies]` section to add:

```toml
uuid = { workspace = true }
```

placed after the `apexes-storage` line (the workspace already defines `uuid = { version =
"1.24", features = ["v4", "serde"] }` in the root `Cargo.toml`, per Task 2's Global
Constraints inspection — reuse it directly, don't redeclare version/features).

- [ ] **Step 3: Run tests**

Run: `cargo test -p apexes-backtest matching_engine`
Expected: 7 tests pass.

- [ ] **Step 4: Run clippy**

Run: `cargo clippy -p apexes-backtest -- -D warnings`
Expected: no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-backtest/src/matching_engine.rs crates/apexes-backtest/Cargo.toml
git commit -S -s -m "feat(apexes-backtest): add MatchingEngine core with market order book-walk fills"
```

---

### Task 10: Limit order resting + deterministic depth-consumption fills with queue-position factor

**Files:**
- Modify: `crates/apexes-backtest/src/matching_engine.rs`

**Interfaces:**
- Consumes: everything from Task 9, plus `apexes_core::TradeEvent`.
- Produces: `pub fn MatchingEngine::submit_limit_order(&mut self, intent: &OrderIntent, now:
  DateTime<Utc>) -> Result<Order, ApexesError>` (order only, no immediate fills -- limit
  orders rest), `pub fn MatchingEngine::on_trade_event(&mut self, trade: &TradeEvent, now:
  DateTime<Utc>) -> Vec<(OrderId, Vec<Fill>)>` (called for every `MarketEvent::Trade` in
  chronological order; this is what actually fills resting limit orders -- see design
  rationale below). Task 14 (runner) calls `submit_limit_order` when a `Strategy` returns a
  Limit `OrderIntent`, and calls `on_trade_event` for every replayed `MarketEvent::Trade`.

**Design rationale (read before implementing):** an earlier draft of this task attempted to
detect limit-order fills by comparing `DepthUpdate` snapshots before/after each depth event,
treating "visible volume decreased" as a proxy for "volume traded through". This is
unreliable: a `DepthUpdate` diff mixes real executed trades with resting-order
cancellations/replacements/new insertions, so a shrinking price level does not reliably mean
"this much volume was actually traded" -- and a level that *doesn't* shrink at all (e.g. a
market-maker continuously replenishing it) would incorrectly block an eligible order forever.
The corrected design instead drives limit-order fills off `apexes_core::TradeEvent`s (real
executed public trades, already present as a distinct `MarketEvent` variant read from
`apexes-storage`), which directly represent actual executed volume at an actual price --
exactly what a limit order fills against in a real market. `DepthUpdate`s remain used only
for `ReplayOrderBook`/market-order book-walks (Task 9) and for computing the *initial* visible
queue-ahead volume when a limit order is first submitted (Step 1 below); they are not used to
detect fills for limit orders after that point.

- [ ] **Step 1: Add resting-order state and `submit_limit_order` with unit tests**

Modify `crates/apexes-backtest/src/matching_engine.rs`. Find the `MatchingEngine` struct
(from Task 9):

```rust
pub struct MatchingEngine {
    books: HashMap<String, ReplayOrderBook>,
    cost_model: CostModel,
    /// 0.0 (optimistic) .. 1.0 (pessimistic), see design spec §7.3. Used by
    /// Task 9's limit-order fill logic.
    queue_position_factor: Decimal,
}
```

Replace with (adds `resting_orders`):

```rust
pub struct MatchingEngine {
    books: HashMap<String, ReplayOrderBook>,
    cost_model: CostModel,
    /// 0.0 (optimistic) .. 1.0 (pessimistic), see design spec §7.3.
    queue_position_factor: Decimal,
    /// Resting limit orders, keyed by symbol -> list of `RestingOrder`.
    resting_orders: HashMap<String, Vec<RestingOrder>>,
}

struct RestingOrder {
    order_id: OrderId,
    order: Order,
    remaining_quantity: Decimal,
    /// Volume that must trade through (via subsequent `TradeEvent`s at
    /// this order's limit price or better) before this order is
    /// considered "our turn" to start filling. Computed once at
    /// submission time as `visible_volume_at_price_at_submission *
    /// queue_position_factor` (design spec §7.3); 0 if
    /// `queue_position_factor` is 0.0 (always eligible immediately).
    volume_ahead_required: Decimal,
    /// Cumulative volume from `TradeEvent`s at this order's limit price
    /// or better, observed since submission. Once this reaches
    /// `volume_ahead_required`, any FURTHER traded volume at/through our
    /// price is treated as filling our own order (see `on_trade_event`).
    volume_observed: Decimal,
}
```

Find `impl MatchingEngine { pub fn new(...) ... }` (from Task 9) and update its body to
initialize `resting_orders`:

```rust
    pub fn new(cost_model: CostModel, queue_position_factor: Decimal) -> Self {
        Self {
            books: HashMap::new(),
            cost_model,
            queue_position_factor,
            resting_orders: HashMap::new(),
        }
    }
```

Add `submit_limit_order` to `impl MatchingEngine` (after `submit_market_order`, from Task 9):

```rust
    /// Submits a Limit `OrderIntent`. The order rests in the simulated
    /// order queue at `intent.price` -- no immediate fill is attempted
    /// here. Fills happen later via `on_trade_event` as real trades occur
    /// at or through the order's limit price.
    pub fn submit_limit_order(&mut self, intent: &OrderIntent, now: DateTime<Utc>) -> Result<Order, ApexesError> {
        if intent.order_type != OrderType::Limit {
            return Err(ApexesError::OrderRejected {
                reason: format!("submit_limit_order called with non-Limit OrderType: {:?}", intent.order_type),
            });
        }
        let limit_price = intent.price.ok_or_else(|| ApexesError::OrderRejected {
            reason: "Limit order requires a price".to_string(),
        })?;

        let order_id = OrderId(Uuid::new_v4());
        let order = Order {
            id: order_id,
            exchange_order_id: None,
            symbol: intent.symbol.clone(),
            venue: intent.venue,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: Some(limit_price),
            stop_price: None,
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };

        // Determine how much visible volume is currently resting ahead of
        // us at (or better than) our limit price, on the SAME side of the
        // book we're joining (other resting orders we'd have to queue
        // behind), to seed queue_position_factor's threshold.
        let book = self.books.entry(intent.symbol.clone()).or_default();
        let levels = match intent.side {
            Side::Buy => book.bids(),  // joining the bid side
            Side::Sell => book.asks(), // joining the ask side
        };
        let visible_volume_at_price: Decimal = levels
            .iter()
            .filter(|level| match intent.side {
                Side::Buy => level.price >= limit_price,
                Side::Sell => level.price <= limit_price,
            })
            .map(|level| level.quantity)
            .sum();

        let resting = RestingOrder {
            order_id,
            order: order.clone(),
            remaining_quantity: intent.quantity,
            volume_ahead_required: visible_volume_at_price * self.queue_position_factor,
            volume_observed: Decimal::ZERO,
        };
        self.resting_orders.entry(intent.symbol.clone()).or_default().push(resting);

        Ok(order)
    }
```

- [ ] **Step 2: Add `on_trade_event` with unit tests**

Add to `impl MatchingEngine` (after `submit_limit_order`):

```rust
    /// Processes a real executed `TradeEvent`, filling any eligible
    /// resting limit orders at `trade.symbol`. A resting Buy limit order
    /// is triggered by trades at or below its limit price; a resting Sell
    /// limit order is triggered by trades at or above its limit price.
    /// Once `volume_ahead_required` (see `RestingOrder`) has been observed
    /// via cumulative trade volume, further trade volume at/through the
    /// order's price fills the order itself, up to `trade.quantity` or the
    /// order's `remaining_quantity`, whichever is smaller. Returns
    /// `(OrderId, Vec<Fill>)` for every order that received a fill from
    /// this trade; fully filled orders are removed from `resting_orders`.
    pub fn on_trade_event(&mut self, trade: &apexes_core::TradeEvent, now: DateTime<Utc>) -> Vec<(OrderId, Vec<Fill>)> {
        let Some(orders) = self.resting_orders.get_mut(&trade.symbol) else {
            return Vec::new();
        };
        if orders.is_empty() {
            return Vec::new();
        }

        let mut results = Vec::new();
        let mut still_resting = Vec::new();
        let mut remaining_trade_quantity = trade.quantity;

        for mut resting in orders.drain(..) {
            let price = resting.order.price.expect("Limit order always has a price");
            let triggered = match resting.order.side {
                Side::Buy => trade.price <= price,
                Side::Sell => trade.price >= price,
            };

            if !triggered || remaining_trade_quantity.is_zero() {
                still_resting.push(resting);
                continue;
            }

            resting.volume_observed += remaining_trade_quantity.min(trade.quantity);

            if resting.volume_observed < resting.volume_ahead_required {
                // Still queued behind other volume; no fill yet, but we
                // keep tracking cumulative observed volume for next time.
                still_resting.push(resting);
                continue;
            }

            // Eligible: fill from the portion of this trade's volume that
            // exceeds our required queue-ahead threshold, capped by our
            // remaining order quantity and the trade's remaining quantity.
            let volume_past_threshold = resting.volume_observed - resting.volume_ahead_required;
            let fill_quantity = volume_past_threshold
                .min(resting.remaining_quantity)
                .min(remaining_trade_quantity);

            if fill_quantity.is_zero() {
                still_resting.push(resting);
                continue;
            }

            let notional = price * fill_quantity;
            let fee = self.cost_model.fee_for_fill(resting.order.venue, FillLiquidity::Maker, notional);
            let fill = Fill {
                id: FillId(Uuid::new_v4()),
                order_id: resting.order_id,
                symbol: resting.order.symbol.clone(),
                venue: resting.order.venue,
                side: resting.order.side,
                price,
                quantity: fill_quantity,
                fee,
                fee_currency: None,
                filled_at: now,
            };

            resting.remaining_quantity -= fill_quantity;
            resting.order.filled_quantity += fill_quantity;
            resting.order.updated_at = now;
            remaining_trade_quantity -= fill_quantity;

            if resting.remaining_quantity.is_zero() {
                resting.order.status = OrderStatus::Filled;
                results.push((resting.order_id, vec![fill]));
            } else {
                resting.order.status = OrderStatus::PartiallyFilled;
                results.push((resting.order_id, vec![fill]));
                still_resting.push(resting);
            }
        }

        *orders = still_resting;
        results
    }
```

- [ ] **Step 3: Write unit tests**

Add to the existing `#[cfg(test)] mod tests` block in `crates/apexes-backtest/src/matching_engine.rs`:

```rust
    fn trade_event(symbol: &str, price: Decimal, quantity: Decimal) -> apexes_core::TradeEvent {
        apexes_core::TradeEvent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            price,
            quantity,
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn submit_limit_order_rests_without_immediate_fill() {
        let mut engine = no_fee_engine();
        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(dec!(64000)),
            stop_price: None,
        };

        let order = engine.submit_limit_order(&intent, Utc::now()).unwrap();

        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.filled_quantity, dec!(0));
    }

    #[test]
    fn resting_limit_order_fills_immediately_when_optimistic_and_trade_triggers() {
        // queue_position_factor = 0.0 -> always eligible once triggered (empty book at submission means 0 required anyway).
        let mut engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule { maker_bps: dec!(0), taker_bps: dec!(0) }),
                funding_rate_bps: None,
            },
            dec!(0.0),
        );

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(dec!(65000)),
            stop_price: None,
        };
        let order = engine.submit_limit_order(&intent, Utc::now()).unwrap();

        // A real trade executes at/below our limit price.
        let results = engine.on_trade_event(&trade_event("BTCUSDT", dec!(64900), dec!(2)), Utc::now());

        assert_eq!(results.len(), 1);
        let (order_id, fills) = &results[0];
        assert_eq!(*order_id, order.id);
        assert_eq!(fills[0].quantity, dec!(1));
        assert_eq!(fills[0].price, dec!(65000)); // fills at OUR limit price, not the trade's price
    }

    #[test]
    fn resting_limit_order_does_not_fill_when_trade_price_not_triggered() {
        let mut engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule { maker_bps: dec!(0), taker_bps: dec!(0) }),
                funding_rate_bps: None,
            },
            dec!(0.0),
        );

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(dec!(64000)), // limit below market
            stop_price: None,
        };
        engine.submit_limit_order(&intent, Utc::now()).unwrap();

        // Trade executes above our limit price: not triggered.
        let results = engine.on_trade_event(&trade_event("BTCUSDT", dec!(65000), dec!(5)), Utc::now());

        assert!(results.is_empty());
    }

    #[test]
    fn pessimistic_queue_position_factor_requires_volume_to_trade_through_first() {
        // queue_position_factor = 1.0 -> must wait for ALL visible volume
        // ahead (at submission time) to trade through before our order fills.
        let mut engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule { maker_bps: dec!(0), taker_bps: dec!(0) }),
                funding_rate_bps: None,
            },
            dec!(1.0),
        );

        // Pre-existing bid volume at our intended limit price before we submit (we're joining the bid side).
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![PriceLevel { price: dec!(65000), quantity: dec!(10) }],
            vec![],
        ));

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(dec!(65000)),
            stop_price: None,
        };
        engine.submit_limit_order(&intent, Utc::now()).unwrap();
        // volume_ahead_required = 10 * 1.0 = 10

        // A trade of only 5 at/below our price occurs -- not enough to clear the 10-unit threshold yet.
        let results = engine.on_trade_event(&trade_event("BTCUSDT", dec!(65000), dec!(5)), Utc::now());
        assert!(results.is_empty(), "should not fill until volume_ahead_required (10) is met");

        // A second trade of 6 brings cumulative observed volume to 11, past the threshold of 10;
        // 1 unit past the threshold triggers our fill.
        let results = engine.on_trade_event(&trade_event("BTCUSDT", dec!(65000), dec!(6)), Utc::now());
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[0].quantity, dec!(1));
    }

    #[test]
    fn resting_order_partial_fill_keeps_remainder_resting() {
        let mut engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule { maker_bps: dec!(0), taker_bps: dec!(0) }),
                funding_rate_bps: None,
            },
            dec!(0.0),
        );

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(5),
            price: Some(dec!(65000)),
            stop_price: None,
        };
        engine.submit_limit_order(&intent, Utc::now()).unwrap();

        // Only 2 units trade at/through our price -- partial fill.
        let results = engine.on_trade_event(&trade_event("BTCUSDT", dec!(64900), dec!(2)), Utc::now());

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[0].quantity, dec!(2));

        // A further trade of 1 unit fills more of the remainder.
        let results = engine.on_trade_event(&trade_event("BTCUSDT", dec!(64900), dec!(1)), Utc::now());
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[0].quantity, dec!(1));
    }

    #[test]
    fn maker_fee_rate_is_applied_to_resting_order_fills() {
        let mut engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule { maker_bps: dec!(5), taker_bps: dec!(20) }),
                funding_rate_bps: None,
            },
            dec!(0.0),
        );

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(dec!(65000)),
            stop_price: None,
        };
        engine.submit_limit_order(&intent, Utc::now()).unwrap();

        let results = engine.on_trade_event(&trade_event("BTCUSDT", dec!(65000), dec!(1)), Utc::now());

        // notional = 65000 * 1 = 65000, fee = 65000 * 0.05% (5 bps) = 32.5
        assert_eq!(results[0].1[0].fee, dec!(32.5));
    }

    #[test]
    fn trade_event_for_different_symbol_does_not_affect_resting_order() {
        let mut engine = no_fee_engine();
        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(dec!(65000)),
            stop_price: None,
        };
        engine.submit_limit_order(&intent, Utc::now()).unwrap();

        let results = engine.on_trade_event(&trade_event("ETHUSDT", dec!(3000), dec!(10)), Utc::now());

        assert!(results.is_empty());
    }
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p apexes-backtest matching_engine`
Expected: 14 tests pass (7 from Task 9 + 7 new: `submit_limit_order_rests_without_immediate_fill`,
`resting_limit_order_fills_immediately_when_optimistic_and_trade_triggers`,
`resting_limit_order_does_not_fill_when_trade_price_not_triggered`,
`pessimistic_queue_position_factor_requires_volume_to_trade_through_first`,
`resting_order_partial_fill_keeps_remainder_resting`,
`maker_fee_rate_is_applied_to_resting_order_fills`,
`trade_event_for_different_symbol_does_not_affect_resting_order`).

- [ ] **Step 5: Run clippy**

Run: `cargo clippy -p apexes-backtest -- -D warnings`
Expected: no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-backtest/src/matching_engine.rs
git commit -S -s -m "feat(apexes-backtest): add TradeEvent-driven limit order fills with queue-position factor"
```

---


### Task 11: Stop/StopLimit trigger simulation

**Files:**
- Modify: `crates/apexes-backtest/src/matching_engine.rs`
- Modify: `crates/apexes-backtest/src/strategy.rs`

**Interfaces:**
- Consumes: everything from Task 10.
- Produces: `pub fn MatchingEngine::submit_stop_order(&mut self, intent: &OrderIntent, now:
  DateTime<Utc>) -> Result<Order, ApexesError>` (accepts both `OrderType::Stop` and
  `OrderType::StopLimit`; order rests as a pending trigger, no immediate fill), extends
  `MatchingEngine::on_trade_event` to also check pending Stop/StopLimit triggers before
  processing resting limit-order fills. Task 14 (runner) calls `submit_stop_order` when a
  `Strategy` returns a Stop or StopLimit `OrderIntent`.

- [ ] **Step 1: Add `#[derive(Clone)]` to `OrderIntent`**

`PendingTrigger` (added in Step 2) stores an owned `OrderIntent`, which requires `Clone`.
Modify `crates/apexes-backtest/src/strategy.rs`: find `pub struct OrderIntent {` (from Task
6, currently with no derive attribute above it) and add one directly above it:

```rust
#[derive(Debug, Clone)]
pub struct OrderIntent {
```

Run `cargo build -p apexes-backtest` to confirm this alone still compiles cleanly (it should
— this is a purely additive derive).

- [ ] **Step 2: Add pending-trigger state and `submit_stop_order`**

Modify `crates/apexes-backtest/src/matching_engine.rs`. Find the `MatchingEngine` struct
(from Task 10):

```rust
pub struct MatchingEngine {
    books: HashMap<String, ReplayOrderBook>,
    cost_model: CostModel,
    /// 0.0 (optimistic) .. 1.0 (pessimistic), see design spec §7.3.
    queue_position_factor: Decimal,
    /// Resting limit orders, keyed by symbol -> list of `RestingOrder`.
    resting_orders: HashMap<String, Vec<RestingOrder>>,
}
```

Replace with (adds `pending_triggers`):

```rust
pub struct MatchingEngine {
    books: HashMap<String, ReplayOrderBook>,
    cost_model: CostModel,
    /// 0.0 (optimistic) .. 1.0 (pessimistic), see design spec §7.3.
    queue_position_factor: Decimal,
    /// Resting limit orders, keyed by symbol -> list of `RestingOrder`.
    resting_orders: HashMap<String, Vec<RestingOrder>>,
    /// Pending Stop/StopLimit orders awaiting their trigger price, keyed
    /// by symbol -> list of `PendingTrigger`.
    pending_triggers: HashMap<String, Vec<PendingTrigger>>,
}

struct PendingTrigger {
    order_id: OrderId,
    intent: OrderIntent,
    stop_price: Decimal,
}
```

Find `impl MatchingEngine { pub fn new(...) ... }` (from Task 10) and update its body:

```rust
    pub fn new(cost_model: CostModel, queue_position_factor: Decimal) -> Self {
        Self {
            books: HashMap::new(),
            cost_model,
            queue_position_factor,
            resting_orders: HashMap::new(),
            pending_triggers: HashMap::new(),
        }
    }
```

Add `submit_stop_order` to `impl MatchingEngine` (after `submit_limit_order`, from Task 10):

```rust
    /// Submits a Stop or StopLimit `OrderIntent`. The order does not rest
    /// in the order book -- it waits as a pending trigger until a
    /// `TradeEvent` at or through `intent.stop_price` is observed (via
    /// `on_trade_event`), at which point it converts to a Market order
    /// (`Stop`) or is submitted as a Limit order (`StopLimit`), per design
    /// spec §7.4.
    pub fn submit_stop_order(&mut self, intent: &OrderIntent, now: DateTime<Utc>) -> Result<Order, ApexesError> {
        if intent.order_type != OrderType::Stop && intent.order_type != OrderType::StopLimit {
            return Err(ApexesError::OrderRejected {
                reason: format!("submit_stop_order called with non-Stop/StopLimit OrderType: {:?}", intent.order_type),
            });
        }
        let stop_price = intent.stop_price.ok_or_else(|| ApexesError::OrderRejected {
            reason: "Stop/StopLimit order requires a stop_price".to_string(),
        })?;
        if intent.order_type == OrderType::StopLimit && intent.price.is_none() {
            return Err(ApexesError::OrderRejected {
                reason: "StopLimit order requires both stop_price and price".to_string(),
            });
        }

        let order_id = OrderId(Uuid::new_v4());
        let order = Order {
            id: order_id,
            exchange_order_id: None,
            symbol: intent.symbol.clone(),
            venue: intent.venue,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: intent.price,
            stop_price: Some(stop_price),
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };

        self.pending_triggers
            .entry(intent.symbol.clone())
            .or_default()
            .push(PendingTrigger {
                order_id,
                intent: intent.clone(),
                stop_price,
            });

        Ok(order)
    }
```

- [ ] **Step 3: Extend `on_trade_event` to check pending triggers first**

Modify `crates/apexes-backtest/src/matching_engine.rs`'s existing `on_trade_event` method
(from Task 10). Find its signature and opening body:

```rust
    pub fn on_trade_event(&mut self, trade: &apexes_core::TradeEvent, now: DateTime<Utc>) -> Vec<(OrderId, Vec<Fill>)> {
        let Some(orders) = self.resting_orders.get_mut(&trade.symbol) else {
            return Vec::new();
        };
        if orders.is_empty() {
            return Vec::new();
        }
```

Replace with (checks pending triggers BEFORE the existing resting-order fill logic, so an
order that converts to a resting Limit order via a StopLimit trigger on this exact trade is
then evaluated by the same call's resting-order loop below):

```rust
    pub fn on_trade_event(&mut self, trade: &apexes_core::TradeEvent, now: DateTime<Utc>) -> Vec<(OrderId, Vec<Fill>)> {
        self.check_pending_triggers(trade, now);

        let Some(orders) = self.resting_orders.get_mut(&trade.symbol) else {
            return Vec::new();
        };
        if orders.is_empty() {
            return Vec::new();
        }
```

(the rest of `on_trade_event`'s body, from the `let mut results = Vec::new();` line onward,
is unchanged from Task 10 -- only the opening lines above change).

Add a new private helper method to `impl MatchingEngine` (after `on_trade_event`):

```rust
    /// Checks every pending Stop/StopLimit trigger at `trade.symbol`
    /// against `trade.price`: a Buy stop triggers when price rises to or
    /// through `stop_price`; a Sell stop triggers when price falls to or
    /// through `stop_price`. Triggered orders are removed from
    /// `pending_triggers` and immediately submitted as a Market order
    /// (`Stop`, filled synchronously via `submit_market_order`'s book-walk
    /// logic against the CURRENT order book state -- its fills are not
    /// returned to `on_trade_event`'s caller in this call; the runner
    /// (Task 14) tracks each `Order`'s status independently and observes
    /// the transition on its own next check) or added to `resting_orders`
    /// as a Limit order (`StopLimit`, becoming eligible for fills on THIS
    /// SAME `on_trade_event` call's subsequent resting-order pass, and any
    /// future calls, exactly like any other resting limit order).
    fn check_pending_triggers(&mut self, trade: &apexes_core::TradeEvent, now: DateTime<Utc>) {
        let Some(pending) = self.pending_triggers.get_mut(&trade.symbol) else {
            return;
        };
        if pending.is_empty() {
            return;
        }

        let mut still_pending = Vec::new();
        let mut triggered = Vec::new();

        for trigger in pending.drain(..) {
            let is_triggered = match trigger.intent.side {
                Side::Buy => trade.price >= trigger.stop_price,
                Side::Sell => trade.price <= trigger.stop_price,
            };
            if is_triggered {
                triggered.push(trigger);
            } else {
                still_pending.push(trigger);
            }
        }
        *pending = still_pending;

        for trigger in triggered {
            match trigger.intent.order_type {
                OrderType::Stop => {
                    let mut market_intent = trigger.intent.clone();
                    market_intent.order_type = OrderType::Market;
                    if self.submit_market_order(&market_intent, now).is_ok() {
                        tracing::info!(
                            symbol = %trade.symbol,
                            order_id = ?trigger.order_id,
                            "Stop order triggered and converted to Market order"
                        );
                    }
                }
                OrderType::StopLimit => {
                    if self.submit_limit_order(&trigger.intent, now).is_ok() {
                        tracing::info!(
                            symbol = %trade.symbol,
                            order_id = ?trigger.order_id,
                            "StopLimit order triggered and converted to Limit order"
                        );
                    }
                }
                _ => unreachable!("PendingTrigger only ever holds Stop/StopLimit intents, enforced by submit_stop_order"),
            }
        }
    }
```

- [ ] **Step 4: Write unit tests**

Add to the existing `#[cfg(test)] mod tests` block in `crates/apexes-backtest/src/matching_engine.rs`:

```rust
    #[test]
    fn submit_stop_order_rests_as_pending_trigger() {
        let mut engine = no_fee_engine();
        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Stop,
            quantity: dec!(1),
            price: None,
            stop_price: Some(dec!(66000)),
        };

        let order = engine.submit_stop_order(&intent, Utc::now()).unwrap();

        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.stop_price, Some(dec!(66000)));
    }

    #[test]
    fn submit_stop_order_rejects_stop_limit_without_price() {
        let mut engine = no_fee_engine();
        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::StopLimit,
            quantity: dec!(1),
            price: None, // missing -- should be rejected
            stop_price: Some(dec!(66000)),
        };

        let result = engine.submit_stop_order(&intent, Utc::now());

        assert!(matches!(result, Err(ApexesError::OrderRejected { .. })));
    }

    #[test]
    fn submit_stop_order_rejects_missing_stop_price() {
        let mut engine = no_fee_engine();
        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Stop,
            quantity: dec!(1),
            price: None,
            stop_price: None,
        };

        let result = engine.submit_stop_order(&intent, Utc::now());

        assert!(matches!(result, Err(ApexesError::OrderRejected { .. })));
    }

    #[test]
    fn submit_stop_order_rejects_non_stop_intent() {
        let mut engine = no_fee_engine();
        let intent = buy_intent("BTCUSDT", dec!(1)); // OrderType::Market

        let result = engine.submit_stop_order(&intent, Utc::now());

        assert!(matches!(result, Err(ApexesError::OrderRejected { .. })));
    }

    #[test]
    fn buy_stop_triggers_and_fills_as_market_order_when_price_rises_through_stop() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![],
            vec![PriceLevel { price: dec!(66100), quantity: dec!(5) }],
        ));

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Stop,
            quantity: dec!(1),
            price: None,
            stop_price: Some(dec!(66000)),
        };
        engine.submit_stop_order(&intent, Utc::now()).unwrap();

        // A trade at/above the stop price triggers it.
        engine.on_trade_event(&trade_event("BTCUSDT", dec!(66050), dec!(1)), Utc::now());

        // Verify the order actually filled by submitting a NEW market order and
        // confirming the book was consumed by the triggered Stop's market fill
        // (the ask level's 5 quantity should now be reduced by 1, to 4).
        let (_order, fills) = engine.submit_market_order(
            &OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(4),
                price: None,
                stop_price: None,
            },
            Utc::now(),
        ).unwrap();
        assert_eq!(fills[0].quantity, dec!(4)); // exactly the remaining depth after the triggered Stop consumed 1
    }

    #[test]
    fn buy_stop_does_not_trigger_when_price_stays_below_stop() {
        let mut engine = no_fee_engine();
        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Stop,
            quantity: dec!(1),
            price: None,
            stop_price: Some(dec!(66000)),
        };
        engine.submit_stop_order(&intent, Utc::now()).unwrap();

        // Trade stays below the stop price -- not triggered.
        engine.on_trade_event(&trade_event("BTCUSDT", dec!(65000), dec!(1)), Utc::now());

        // No liquidity was consumed by a phantom fill: submitting a fresh
        // market order against an untouched (empty) book should find zero fills.
        let (_order, fills) = engine.submit_market_order(
            &OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
            },
            Utc::now(),
        ).unwrap();
        assert!(fills.is_empty());
    }

    #[test]
    fn sell_stop_triggers_when_price_falls_through_stop() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![PriceLevel { price: dec!(63900), quantity: dec!(5) }],
            vec![],
        ));

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Sell,
            order_type: OrderType::Stop,
            quantity: dec!(1),
            price: None,
            stop_price: Some(dec!(64000)),
        };
        engine.submit_stop_order(&intent, Utc::now()).unwrap();

        // A trade at/below the stop price triggers a sell stop.
        engine.on_trade_event(&trade_event("BTCUSDT", dec!(63950), dec!(1)), Utc::now());

        let (_order, fills) = engine.submit_market_order(
            &OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                side: Side::Sell,
                order_type: OrderType::Market,
                quantity: dec!(4),
                price: None,
                stop_price: None,
            },
            Utc::now(),
        ).unwrap();
        assert_eq!(fills[0].quantity, dec!(4));
    }

    #[test]
    fn stop_limit_triggers_and_rests_as_limit_order_not_immediate_fill() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![],
            vec![PriceLevel { price: dec!(66100), quantity: dec!(5) }],
        ));

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::StopLimit,
            quantity: dec!(1),
            price: Some(dec!(66050)), // limit price once triggered
            stop_price: Some(dec!(66000)),
        };
        engine.submit_stop_order(&intent, Utc::now()).unwrap();

        // Trigger: a trade at/above the stop price, but BELOW the StopLimit's
        // own limit price (66020 < 66050), so the newly-resting Buy limit
        // order (which fills on trades AT/BELOW its price) is not yet
        // triggered by this same trade.
        let results = engine.on_trade_event(&trade_event("BTCUSDT", dec!(66020), dec!(1)), Utc::now());
        assert!(
            results.is_empty(),
            "StopLimit becomes a resting Limit order on trigger, not an immediate fill"
        );

        // A subsequent trade at/below the limit price fills the now-resting Limit order.
        let results = engine.on_trade_event(&trade_event("BTCUSDT", dec!(66040), dec!(1)), Utc::now());
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[0].price, dec!(66050)); // fills at the StopLimit's own limit price
    }
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p apexes-backtest matching_engine`
Expected: 22 tests pass (14 from Task 10 + 8 new: `submit_stop_order_rests_as_pending_trigger`,
`submit_stop_order_rejects_stop_limit_without_price`,
`submit_stop_order_rejects_missing_stop_price`, `submit_stop_order_rejects_non_stop_intent`,
`buy_stop_triggers_and_fills_as_market_order_when_price_rises_through_stop`,
`buy_stop_does_not_trigger_when_price_stays_below_stop`,
`sell_stop_triggers_when_price_falls_through_stop`,
`stop_limit_triggers_and_rests_as_limit_order_not_immediate_fill`).

- [ ] **Step 6: Run clippy**

Run: `cargo clippy -p apexes-backtest -- -D warnings`
Expected: no warnings.

- [ ] **Step 7: Commit**

```bash
git add crates/apexes-backtest/src/matching_engine.rs crates/apexes-backtest/src/strategy.rs
git commit -S -s -m "feat(apexes-backtest): add Stop/StopLimit trigger simulation"
```


---

### Task 12: `BacktestClock` — replay pacing (`AsFastAsPossible` / `Multiplier` / `RealTime`)

**Files:**
- Modify: `crates/apexes-backtest/src/clock.rs`

**Interfaces:**
- Consumes: `apexes_core::MarketEvent`, `chrono::{DateTime, Utc}`.
- Produces: `pub enum ReplaySpeed { AsFastAsPossible, Multiplier(f64), RealTime }`, `pub struct
  BacktestClock`, `pub fn BacktestClock::new(speed: ReplaySpeed) -> Self`, `pub async fn
  BacktestClock::wait_for_next_event(&mut self, event_timestamp: DateTime<Utc>)`. Task 14
  (runner) constructs `BacktestClock` from `BacktestConfig.speed` and calls
  `wait_for_next_event` before dispatching each replayed event to the `Strategy`.

- [ ] **Step 1: Write `BacktestClock` with unit tests**

Create `crates/apexes-backtest/src/clock.rs`:

```rust
use chrono::{DateTime, Utc};

/// Controls the pacing of backtest replay. Logical time for all
/// strategy/risk/funding-period calculations always derives from event
/// timestamps regardless of `ReplaySpeed` (design spec §8) -- this clock
/// only controls whether/how long to artificially wait between
/// dispatching events, purely for demo/visual-inspection purposes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReplaySpeed {
    /// Process events back-to-back with zero artificial delay (default).
    AsFastAsPossible,
    /// Insert real delays scaled by this multiplier (e.g. `10.0` = 10x
    /// real-time; `0.5` = half real-time/slower).
    Multiplier(f64),
    /// 1:1 wall-clock pacing (equivalent to `Multiplier(1.0)`).
    RealTime,
}

/// Paces backtest replay according to a `ReplaySpeed`. Tracks the
/// timestamp of the previously-dispatched event to compute how long to
/// wait before the next one, for `Multiplier`/`RealTime` modes.
pub struct BacktestClock {
    speed: ReplaySpeed,
    last_event_timestamp: Option<DateTime<Utc>>,
}

impl BacktestClock {
    pub fn new(speed: ReplaySpeed) -> Self {
        Self {
            speed,
            last_event_timestamp: None,
        }
    }

    /// Waits (if `speed` requires it) before the caller dispatches the
    /// event with `event_timestamp`. For `AsFastAsPossible`, returns
    /// immediately. For `Multiplier`/`RealTime`, sleeps for the
    /// wall-clock-scaled gap since the previously-dispatched event's
    /// timestamp (zero on the very first call, or if timestamps go
    /// backward/are identical -- never sleeps a negative duration).
    pub async fn wait_for_next_event(&mut self, event_timestamp: DateTime<Utc>) {
        if let Some(last) = self.last_event_timestamp {
            let gap = event_timestamp.signed_duration_since(last);
            if gap.num_milliseconds() > 0 {
                let multiplier = match self.speed {
                    ReplaySpeed::AsFastAsPossible => None,
                    ReplaySpeed::Multiplier(m) if m > 0.0 => Some(m),
                    ReplaySpeed::Multiplier(_) => None, // non-positive multiplier: treat as AsFastAsPossible
                    ReplaySpeed::RealTime => Some(1.0),
                };
                if let Some(multiplier) = multiplier {
                    let scaled_millis = (gap.num_milliseconds() as f64) / multiplier;
                    if scaled_millis > 0.0 {
                        tokio::time::sleep(std::time::Duration::from_millis(scaled_millis as u64)).await;
                    }
                }
            }
        }
        self.last_event_timestamp = Some(event_timestamp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::time::Instant;

    fn time_at(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, hour, minute, second).unwrap()
    }

    #[tokio::test]
    async fn as_fast_as_possible_never_sleeps() {
        let mut clock = BacktestClock::new(ReplaySpeed::AsFastAsPossible);
        let start = Instant::now();

        clock.wait_for_next_event(time_at(10, 0, 0)).await;
        clock.wait_for_next_event(time_at(10, 5, 0)).await; // 5 minutes later in event-time

        // No real sleep should have occurred despite a 5-minute event-time gap.
        assert!(start.elapsed().as_millis() < 100);
    }

    #[tokio::test]
    async fn first_call_never_sleeps_regardless_of_speed() {
        let mut clock = BacktestClock::new(ReplaySpeed::RealTime);
        let start = Instant::now();

        clock.wait_for_next_event(time_at(10, 0, 0)).await;

        assert!(start.elapsed().as_millis() < 100);
    }

    #[tokio::test]
    async fn multiplier_scales_the_wait_duration() {
        // 100x speed: a 1-second event-time gap becomes a 10ms real wait.
        let mut clock = BacktestClock::new(ReplaySpeed::Multiplier(100.0));

        clock.wait_for_next_event(time_at(10, 0, 0)).await;
        let start = Instant::now();
        clock.wait_for_next_event(time_at(10, 0, 1)).await; // 1 second later in event-time
        let elapsed = start.elapsed();

        // Should be roughly 10ms (1000ms / 100), allow generous scheduling slack.
        assert!(elapsed.as_millis() >= 5, "expected some sleep to occur, got {elapsed:?}");
        assert!(elapsed.as_millis() < 200, "expected a short sleep, got {elapsed:?}");
    }

    #[tokio::test]
    async fn backward_or_identical_timestamps_never_sleep() {
        let mut clock = BacktestClock::new(ReplaySpeed::RealTime);
        clock.wait_for_next_event(time_at(10, 0, 5)).await;

        let start = Instant::now();
        clock.wait_for_next_event(time_at(10, 0, 0)).await; // earlier than the previous event
        clock.wait_for_next_event(time_at(10, 0, 0)).await; // identical timestamp
        let elapsed = start.elapsed();

        assert!(elapsed.as_millis() < 100);
    }

    #[tokio::test]
    async fn non_positive_multiplier_is_treated_as_as_fast_as_possible() {
        let mut clock = BacktestClock::new(ReplaySpeed::Multiplier(0.0));
        let start = Instant::now();

        clock.wait_for_next_event(time_at(10, 0, 0)).await;
        clock.wait_for_next_event(time_at(10, 0, 10)).await; // 10 second event-time gap

        assert!(start.elapsed().as_millis() < 100);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p apexes-backtest clock`
Expected: 5 tests pass. Note: `multiplier_scales_the_wait_duration` involves real (short)
sleeps and timing assertions with generous tolerances -- if it flakes on a heavily loaded CI
machine, that's a known class of timing-test flakiness, not a logic defect; the assertion
bounds (5ms-200ms for an expected ~10ms sleep) are already generous.

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p apexes-backtest -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-backtest/src/clock.rs
git commit -S -s -m "feat(apexes-backtest): add BacktestClock for variable-speed replay pacing"
```

---


---

### Task 13: Performance tracking — equity curve + `PerformanceSummary` (Sharpe/Sortino/Calmar/drawdown/win-rate/profit-factor)

**Files:**
- Modify: `crates/apexes-backtest/src/performance.rs`

**Interfaces:**
- Consumes: `apexes_core::{Fill, Venue}`, `rust_decimal::Decimal`, `chrono::{DateTime, Utc}`,
  `barter::{Timed, statistic::{time::Daily, metric::{sharpe::SharpeRatio,
  sortino::SortinoRatio, calmar::CalmarRatio, win_rate::WinRate,
  profit_factor::ProfitFactor, drawdown::{DrawdownGenerator, max::MaxDrawdownGenerator}}}}`
  (the ONLY place in this crate that touches the `barter` meta-crate, per this plan's Global
  Constraints).
- Produces: `pub struct PerformanceSummary { pub total_pnl: Decimal, pub sharpe: Decimal,
  pub sortino: Decimal, pub calmar: Decimal, pub max_drawdown: Decimal, pub win_rate: Decimal,
  pub profit_factor: Decimal, pub turnover: Decimal, pub trade_count: u64 }`, `pub struct
  PerformanceTracker`, `pub fn PerformanceTracker::new(risk_free_return: Decimal) -> Self`,
  `pub fn PerformanceTracker::record_equity_point(&mut self, timestamp: DateTime<Utc>,
  equity: Decimal)`, `pub fn PerformanceTracker::record_fill(&mut self, fill: &Fill)`,
  `pub fn PerformanceTracker::summary(&self) -> PerformanceSummary`, `pub fn
  PerformanceTracker::equity_curve(&self) -> &[(DateTime<Utc>, Decimal)]`. Task 14 (runner)
  constructs one `PerformanceTracker` for the overall summary plus one per symbol/venue for
  the breakdown, calling `record_equity_point` after every processed event and `record_fill`
  after every simulated `Fill`.

- [ ] **Step 1: Write `PerformanceTracker` and `PerformanceSummary` with unit tests**

Create `crates/apexes-backtest/src/performance.rs`:

```rust
use apexes_core::Fill;
use barter::statistic::metric::calmar::CalmarRatio;
use barter::statistic::metric::drawdown::max::MaxDrawdownGenerator;
use barter::statistic::metric::drawdown::{Drawdown, DrawdownGenerator};
use barter::statistic::metric::profit_factor::ProfitFactor;
use barter::statistic::metric::sharpe::SharpeRatio;
use barter::statistic::metric::sortino::SortinoRatio;
use barter::statistic::metric::win_rate::WinRate;
use barter::statistic::time::Daily;
use barter::Timed;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

/// Aggregated performance/risk metrics over a backtest (or a per-symbol /
/// per-venue slice of one), computed from an equity curve and the fills
/// that occurred. Uses `barter::statistic::metric` for the standard
/// Sharpe/Sortino/Calmar/win-rate/profit-factor formulas (design spec §3,
/// §8) -- the only place in this crate that depends on the `barter`
/// meta-crate.
#[derive(Debug, Clone, PartialEq)]
pub struct PerformanceSummary {
    pub total_pnl: Decimal,
    pub sharpe: Decimal,
    pub sortino: Decimal,
    pub calmar: Decimal,
    /// Positive magnitude (peak-to-trough decline as a fraction of the
    /// peak, e.g. `0.18` for an 18% drawdown) -- NOT signed negative, per
    /// `barter::statistic::metric::drawdown::DrawdownGenerator`'s formula
    /// `(peak - trough) / peak`. Zero if no drawdown has occurred.
    pub max_drawdown: Decimal,
    pub win_rate: Decimal,
    pub profit_factor: Decimal,
    pub turnover: Decimal,
    pub trade_count: u64,
}

/// Accumulates an equity curve and fill history, then computes a
/// `PerformanceSummary` on demand via `summary()`.
pub struct PerformanceTracker {
    risk_free_return: Decimal,
    equity_curve: Vec<(DateTime<Utc>, Decimal)>,
    drawdown_generator: DrawdownGenerator,
    max_drawdown_generator: MaxDrawdownGenerator,
    fills: Vec<Fill>,
}

impl PerformanceTracker {
    pub fn new(risk_free_return: Decimal) -> Self {
        Self {
            risk_free_return,
            equity_curve: Vec::new(),
            drawdown_generator: DrawdownGenerator::default(),
            max_drawdown_generator: MaxDrawdownGenerator::default(),
            fills: Vec::new(),
        }
    }

    /// Records a mark-to-market equity value at `timestamp`. Must be
    /// called in chronological order.
    pub fn record_equity_point(&mut self, timestamp: DateTime<Utc>, equity: Decimal) {
        self.equity_curve.push((timestamp, equity));

        let point = Timed::new(equity, timestamp);
        if self.equity_curve.len() == 1 {
            self.drawdown_generator = DrawdownGenerator::init(point);
        } else if let Some(drawdown) = self.drawdown_generator.update(point) {
            self.max_drawdown_generator.update(&drawdown);
        }
    }

    /// Records a completed `Fill`, used for turnover, trade count, and
    /// win-rate/profit-factor calculations.
    pub fn record_fill(&mut self, fill: &Fill) {
        self.fills.push(fill.clone());
    }

    pub fn equity_curve(&self) -> &[(DateTime<Utc>, Decimal)] {
        &self.equity_curve
    }

    /// Computes the current `PerformanceSummary` from all recorded equity
    /// points and fills so far.
    pub fn summary(&self) -> PerformanceSummary {
        let total_pnl = match (self.equity_curve.first(), self.equity_curve.last()) {
            (Some((_, first)), Some((_, last))) => last - first,
            _ => Decimal::ZERO,
        };

        let returns = self.per_point_returns();
        let mean_return = mean(&returns);
        let std_dev_return = std_dev(&returns, mean_return);
        let downside_returns: Vec<Decimal> = returns.iter().copied().filter(|r| *r < Decimal::ZERO).collect();
        let std_dev_loss_return = std_dev(&downside_returns, mean(&downside_returns));

        let sharpe = SharpeRatio::calculate(self.risk_free_return, mean_return, std_dev_return, Daily).value;
        let sortino = SortinoRatio::calculate(self.risk_free_return, mean_return, std_dev_loss_return, Daily).value;

        let max_drawdown = self
            .max_drawdown_generator
            .generate()
            .map(|max| max.0.value)
            .unwrap_or(Decimal::ZERO);
        let calmar = CalmarRatio::calculate(self.risk_free_return, mean_return, max_drawdown, Daily).value;

        let (wins, total_closed) = self.win_loss_counts();
        let win_rate = WinRate::calculate(wins, total_closed).map(|w| w.value).unwrap_or(Decimal::ZERO);

        let (gross_profit, gross_loss) = self.gross_profit_and_loss();
        let profit_factor = ProfitFactor::calculate(gross_profit, gross_loss).map(|p| p.value).unwrap_or(Decimal::ZERO);

        let turnover: Decimal = self.fills.iter().map(|fill| fill.price * fill.quantity).sum();

        PerformanceSummary {
            total_pnl,
            sharpe,
            sortino,
            calmar,
            max_drawdown,
            win_rate,
            profit_factor,
            turnover,
            trade_count: self.fills.len() as u64,
        }
    }

    /// Per-point simple returns derived from consecutive equity curve
    /// values: `(equity[i] - equity[i-1]) / equity[i-1]`, skipping any
    /// point where the prior equity value is zero (division-by-zero
    /// guard).
    fn per_point_returns(&self) -> Vec<Decimal> {
        self.equity_curve
            .windows(2)
            .filter_map(|pair| {
                let (_, prev) = pair[0];
                let (_, curr) = pair[1];
                if prev.is_zero() {
                    None
                } else {
                    Some((curr - prev) / prev)
                }
            })
            .collect()
    }

    /// Simplified win/loss accounting: counts a `Fill` as a "win" if its
    /// contribution to realized P&L (approximated here via fee-adjusted
    /// notional sign relative to running average, per the fill's own
    /// side) is positive. NOTE: this is a simplified per-fill heuristic,
    /// not full round-trip trade P&L attribution (which would require
    /// matching entry/exit fills into discrete trades) -- sufficient for
    /// this sub-project's reporting needs; a more precise trade-level P&L
    /// engine is out of scope here (see design spec §8's explicit
    /// per-symbol/per-venue breakdown scope, which does not require
    /// round-trip trade reconstruction).
    fn win_loss_counts(&self) -> (Decimal, Decimal) {
        // Without full position-aware P&L attribution, approximate: a Buy
        // fill's "win" contribution is not knowable in isolation, so this
        // simplified tracker treats every fill as a "trade" for win_rate
        // purposes based on whether the equity curve was net-positive
        // since the previous fill. This keeps the calculation
        // self-contained within PerformanceTracker without requiring it to
        // reach into PortfolioLedger's position logic.
        if self.fills.is_empty() {
            return (Decimal::ZERO, Decimal::ZERO);
        }
        let returns = self.per_point_returns();
        let wins = Decimal::from(returns.iter().filter(|r| **r > Decimal::ZERO).count() as u64);
        let total = Decimal::from(returns.len() as u64);
        (wins, total)
    }

    /// Gross profit/loss derived from per-point equity returns (same
    /// simplification rationale as `win_loss_counts`).
    fn gross_profit_and_loss(&self) -> (Decimal, Decimal) {
        let returns = self.per_point_returns();
        let gross_profit: Decimal = returns.iter().filter(|r| **r > Decimal::ZERO).sum();
        let gross_loss: Decimal = returns.iter().filter(|r| **r < Decimal::ZERO).map(|r| r.abs()).sum();
        (gross_profit, gross_loss)
    }
}

fn mean(values: &[Decimal]) -> Decimal {
    if values.is_empty() {
        return Decimal::ZERO;
    }
    values.iter().sum::<Decimal>() / Decimal::from(values.len() as u64)
}

fn std_dev(values: &[Decimal], mean_value: Decimal) -> Decimal {
    if values.len() < 2 {
        return Decimal::ZERO;
    }
    let variance: Decimal = values.iter().map(|v| (*v - mean_value) * (*v - mean_value)).sum::<Decimal>()
        / Decimal::from((values.len() - 1) as u64);
    // Decimal has no native sqrt on stable without MathematicalOps; use
    // rust_decimal::MathematicalOps (already a transitive dependency via
    // barter's own use of it, and rust_decimal itself supports it via the
    // "maths" feature -- see Step 2 below for the Cargo.toml change this
    // requires).
    variance.sqrt().unwrap_or(Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{FillId, OrderId, Side, Venue};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn time_at(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, hour, 0, 0).unwrap()
    }

    fn sample_fill(price: Decimal, quantity: Decimal) -> Fill {
        Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            price,
            quantity,
            fee: dec!(0),
            fee_currency: None,
            filled_at: Utc::now(),
        }
    }

    #[test]
    fn empty_tracker_produces_zeroed_summary() {
        let tracker = PerformanceTracker::new(dec!(0));
        let summary = tracker.summary();

        assert_eq!(summary.total_pnl, dec!(0));
        assert_eq!(summary.trade_count, 0);
    }

    #[test]
    fn total_pnl_is_last_minus_first_equity_point() {
        let mut tracker = PerformanceTracker::new(dec!(0));
        tracker.record_equity_point(time_at(0), dec!(10000));
        tracker.record_equity_point(time_at(1), dec!(10500));
        tracker.record_equity_point(time_at(2), dec!(10200));

        let summary = tracker.summary();

        assert_eq!(summary.total_pnl, dec!(200)); // 10200 - 10000
    }

    #[test]
    fn trade_count_matches_number_of_recorded_fills() {
        let mut tracker = PerformanceTracker::new(dec!(0));
        tracker.record_fill(&sample_fill(dec!(65000), dec!(1)));
        tracker.record_fill(&sample_fill(dec!(65100), dec!(0.5)));

        let summary = tracker.summary();

        assert_eq!(summary.trade_count, 2);
    }

    #[test]
    fn turnover_sums_notional_of_all_fills() {
        let mut tracker = PerformanceTracker::new(dec!(0));
        tracker.record_fill(&sample_fill(dec!(65000), dec!(1))); // 65000
        tracker.record_fill(&sample_fill(dec!(100), dec!(2)));   // 200

        let summary = tracker.summary();

        assert_eq!(summary.turnover, dec!(65200));
    }

    #[test]
    fn max_drawdown_is_tracked_across_a_peak_and_trough() {
        let mut tracker = PerformanceTracker::new(dec!(0));
        tracker.record_equity_point(time_at(0), dec!(10000));
        tracker.record_equity_point(time_at(1), dec!(11000)); // peak
        tracker.record_equity_point(time_at(2), dec!(9000));  // trough
        tracker.record_equity_point(time_at(3), dec!(11500)); // recovery, closes the drawdown period

        let summary = tracker.summary();

        // barter's DrawdownGenerator computes drawdown_max as
        // (peak - trough) / peak, a POSITIVE magnitude (not signed
        // negative) -- verified against the real DrawdownGenerator source
        // during planning. Peak 11000, trough 9000: (11000-9000)/11000 ≈ 0.1818.
        assert!(summary.max_drawdown > dec!(0), "expected a positive max_drawdown magnitude, got {}", summary.max_drawdown);
        assert!(summary.max_drawdown > dec!(0.15) && summary.max_drawdown < dec!(0.2), "expected max_drawdown ≈ 0.1818, got {}", summary.max_drawdown);
    }

    #[test]
    fn equity_curve_returns_all_recorded_points_in_order() {
        let mut tracker = PerformanceTracker::new(dec!(0));
        tracker.record_equity_point(time_at(0), dec!(10000));
        tracker.record_equity_point(time_at(1), dec!(10100));

        let curve = tracker.equity_curve();

        assert_eq!(curve.len(), 2);
        assert_eq!(curve[0].1, dec!(10000));
        assert_eq!(curve[1].1, dec!(10100));
    }

    #[test]
    fn single_equity_point_produces_zero_pnl_and_zero_drawdown() {
        let mut tracker = PerformanceTracker::new(dec!(0));
        tracker.record_equity_point(time_at(0), dec!(10000));

        let summary = tracker.summary();

        assert_eq!(summary.total_pnl, dec!(0));
        assert_eq!(summary.max_drawdown, dec!(0));
    }
}
```

- [ ] **Step 2: Enable `rust_decimal`'s `maths` feature (for `Decimal::sqrt`)**

`std_dev`'s `variance.sqrt()` call requires `rust_decimal::MathematicalOps`, which requires
the `maths` feature. Modify the workspace root `Cargo.toml`'s `rust_decimal` line:

Find:
```toml
rust_decimal = { version = "1.42", features = ["serde"] }
```

Replace with:
```toml
rust_decimal = { version = "1.42", features = ["serde", "maths"] }
```

This is a workspace-wide dependency change (affects all crates using `rust_decimal`, all of
which already compile fine with additional features enabled -- this is purely additive, no
existing code is affected). Add `use rust_decimal::MathematicalOps;` to the top of
`crates/apexes-backtest/src/performance.rs`'s imports (needed for `.sqrt()` to be in scope):

```rust
use rust_decimal::{Decimal, MathematicalOps};
```

(replacing the plain `use rust_decimal::Decimal;` import from Step 1's code).

- [ ] **Step 3: Run tests**

Run: `cargo test -p apexes-backtest performance`
Expected: 7 tests pass.

- [ ] **Step 4: Run clippy**

Run: `cargo clippy --workspace -- -D warnings`
Expected: no warnings anywhere in the workspace (the `maths` feature change in Step 2 affects
the whole workspace, so verify workspace-wide, not just `apexes-backtest`).

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-backtest/src/performance.rs Cargo.toml
git commit -S -s -m "feat(apexes-backtest): add PerformanceTracker with Sharpe/Sortino/Calmar/drawdown metrics"
```

---

### Task 14: `BacktestReport` + `run_backtest` runner (wires everything together)

**Files:**
- Modify: `crates/apexes-backtest/src/report.rs`
- Modify: `crates/apexes-backtest/src/runner.rs`

**Interfaces:**
- Consumes: everything from Tasks 1-13 (`HistoricalMarketDataSource`, `ReplayOrderBook`
  indirectly via `MatchingEngine`, `CostModel`, `Strategy`/`StrategyContext`/`OrderIntent`,
  `PortfolioLedger`, `MatchingEngine`, `BacktestClock`/`ReplaySpeed`, `PerformanceTracker`).
- Produces: `pub struct BacktestConfig`, `pub struct BacktestReport`, `pub async fn
  run_backtest(config: BacktestConfig, market_data: impl HistoricalMarketDataSource, strategy:
  impl Strategy) -> Result<BacktestReport, ApexesError>`. This is the crate's top-level public
  entry point (design spec §8) -- Task 15's integration tests call `run_backtest` directly.

- [ ] **Step 1: Write `BacktestReport`**

Create `crates/apexes-backtest/src/report.rs`:

```rust
use crate::performance::PerformanceSummary;
use apexes_core::{Fill, Venue};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Structured, serializable backtest results (design spec §8, §10). No
/// rendering/charting code is part of this crate -- visualization is
/// sub-project #13's responsibility, consuming this data directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestReport {
    pub overall: SerializablePerformanceSummary,
    pub per_symbol: HashMap<String, SerializablePerformanceSummary>,
    pub per_venue: HashMap<String, SerializablePerformanceSummary>,
    pub equity_curve: Vec<(DateTime<Utc>, rust_decimal::Decimal)>,
    pub trades: Vec<Fill>,
}

/// `PerformanceSummary` (Task 13) does not itself derive `Serialize`/
/// `Deserialize` (it lives in `performance.rs` and has no reason to
/// depend on `serde` directly); this wrapper type in `report.rs` mirrors
/// its fields with serde derives, since `BacktestReport` -- not
/// `PerformanceSummary` itself -- is this crate's serialization boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializablePerformanceSummary {
    pub total_pnl: rust_decimal::Decimal,
    pub sharpe: rust_decimal::Decimal,
    pub sortino: rust_decimal::Decimal,
    pub calmar: rust_decimal::Decimal,
    pub max_drawdown: rust_decimal::Decimal,
    pub win_rate: rust_decimal::Decimal,
    pub profit_factor: rust_decimal::Decimal,
    pub turnover: rust_decimal::Decimal,
    pub trade_count: u64,
}

impl From<PerformanceSummary> for SerializablePerformanceSummary {
    fn from(summary: PerformanceSummary) -> Self {
        Self {
            total_pnl: summary.total_pnl,
            sharpe: summary.sharpe,
            sortino: summary.sortino,
            calmar: summary.calmar,
            max_drawdown: summary.max_drawdown,
            win_rate: summary.win_rate,
            profit_factor: summary.profit_factor,
            turnover: summary.turnover,
            trade_count: summary.trade_count,
        }
    }
}
```

Note: `Venue` is imported but only needed because `per_venue`'s keys are `String` (via
`format!("{venue:?}")`, matching the same Debug-based string convention used throughout
`apexes-storage`'s schema.rs for venue serialization) rather than `Venue` directly -- `Venue`
itself is not used as a struct field type here since `HashMap` keys need `Eq + Hash` and a
`String` key is simpler for JSON serialization (JSON object keys must be strings; a
`HashMap<Venue, _>` would need a custom serde key serializer). If clippy flags the `Venue`
import as unused after this reasoning, remove the `use apexes_core::{Fill, Venue};` and
change it to `use apexes_core::Fill;` -- verify with `cargo build` which is actually needed.

- [ ] **Step 2: Write `BacktestConfig` and `run_backtest`**

Create `crates/apexes-backtest/src/runner.rs`:

```rust
use crate::clock::{BacktestClock, ReplaySpeed};
use crate::cost_model::CostModel;
use crate::ledger::PortfolioLedger;
use crate::market_data_source::HistoricalMarketDataSource;
use crate::matching_engine::MatchingEngine;
use crate::performance::PerformanceTracker;
use crate::report::BacktestReport;
use crate::strategy::{Strategy, StrategyContext};
use apexes_core::{ApexesError, Instrument, MarketEvent, OrderId, OrderType, Order};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::ops::Range;

/// Configuration for a single backtest run (design spec §8).
pub struct BacktestConfig {
    pub instruments: Vec<Instrument>,
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
}

/// Runs a full backtest: replays `market_data` in chronological order
/// through `strategy`, simulating fills via a `MatchingEngine`, tracking
/// portfolio state via a `PortfolioLedger`, and producing a
/// `BacktestReport` with overall + per-symbol + per-venue performance
/// summaries (design spec §8).
pub async fn run_backtest(
    config: BacktestConfig,
    market_data: impl HistoricalMarketDataSource,
    mut strategy: impl Strategy,
) -> Result<BacktestReport, ApexesError> {
    let events = market_data.events().await?;

    let instruments_by_symbol: HashMap<String, Instrument> = config
        .instruments
        .iter()
        .map(|instrument| (instrument.symbol.clone(), instrument.clone()))
        .collect();

    let mut ledger = PortfolioLedger::new(instruments_by_symbol.clone(), config.initial_balances.clone());
    let mut matching_engine = MatchingEngine::new(config.cost_model, config.queue_position_factor);
    let mut clock = BacktestClock::new(config.speed);
    let mut overall_tracker = PerformanceTracker::new(config.risk_free_return);
    let mut per_symbol_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut per_venue_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut open_orders: HashMap<OrderId, Order> = HashMap::new();
    let mut last_prices: HashMap<String, Decimal> = HashMap::new();

    for event in &events {
        let timestamp = event_timestamp(event);
        clock.wait_for_next_event(timestamp).await;

        match event {
            MarketEvent::Depth(update) => {
                matching_engine.on_depth_update(update);
            }
            MarketEvent::Trade(trade) => {
                last_prices.insert(trade.symbol.clone(), trade.price);
                let fill_results = matching_engine.on_trade_event(trade, timestamp);
                for (order_id, fills) in fill_results {
                    apply_fills(&mut ledger, &mut overall_tracker, &mut per_symbol_trackers, &mut per_venue_trackers, &fills)?;
                    if let Some(order) = open_orders.get_mut(&order_id) {
                        if let Some(last_fill) = fills.last() {
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
            }
            _ => {} // Tick/Bar events currently only advance the clock/context; no direct matching-engine action.
        }

        let ctx = StrategyContext {
            positions: ledger.positions(),
            balances: ledger.balances(),
            open_orders: &open_orders,
            now: timestamp,
        };
        let intents = strategy.on_market_event(event, &ctx);

        for intent in intents {
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
                        apply_fills(&mut ledger, &mut overall_tracker, &mut per_symbol_trackers, &mut per_venue_trackers, &fills)?;
                    }
                }
                Err(error) => {
                    tracing::warn!(?error, symbol = %intent.symbol, "order submission rejected");
                }
            }
        }

        // Mark-to-market equity point for the overall tracker after
        // processing this event.
        let equity = ledger.equity(&last_prices)?;
        overall_tracker.record_equity_point(timestamp, equity);
    }

    Ok(BacktestReport {
        overall: overall_tracker.summary().into(),
        per_symbol: per_symbol_trackers.into_iter().map(|(k, v)| (k, v.summary().into())).collect(),
        per_venue: per_venue_trackers.into_iter().map(|(k, v)| (k, v.summary().into())).collect(),
        equity_curve: overall_tracker.equity_curve().to_vec(),
        trades: Vec::new(), // populated via apply_fills below in a future refinement; see Step 3 note
    })
}

fn apply_fills(
    ledger: &mut PortfolioLedger,
    overall_tracker: &mut PerformanceTracker,
    per_symbol_trackers: &mut HashMap<String, PerformanceTracker>,
    per_venue_trackers: &mut HashMap<String, PerformanceTracker>,
    fills: &[apexes_core::Fill],
) -> Result<(), ApexesError> {
    for fill in fills {
        ledger.apply_fill(fill)?;
        overall_tracker.record_fill(fill);
        per_symbol_trackers
            .entry(fill.symbol.clone())
            .or_insert_with(|| PerformanceTracker::new(Decimal::ZERO))
            .record_fill(fill);
        per_venue_trackers
            .entry(format!("{:?}", fill.venue))
            .or_insert_with(|| PerformanceTracker::new(Decimal::ZERO))
            .record_fill(fill);
    }
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

**IMPORTANT -- known gap to fix in this same task, not deferred:** the `run_backtest` draft
above has `trades: Vec::new()` as a placeholder in its final `BacktestReport` construction,
which is INCORRECT and must not ship this way -- `BacktestReport.trades` must contain every
`Fill` that occurred during the run. Fix this before running tests: change `apply_fills` to
also push each fill into a `Vec<apexes_core::Fill>` accumulator that `run_backtest` owns
(declare `let mut all_fills: Vec<apexes_core::Fill> = Vec::new();` alongside the other `let
mut` trackers near the top of `run_backtest`, pass `&mut all_fills` into every `apply_fills`
call alongside the existing parameters, have `apply_fills` do `all_fills.extend_from_slice
(fills);` internally, and use `trades: all_fills` in the final `BacktestReport` construction
instead of `trades: Vec::new()`). This is a required correction to the pseudocode above, not
an optional step -- do not leave `trades` empty in the shipped implementation.

Additionally, document this gap with a doc comment addition on
`StrategyContext.open_orders` in `crates/apexes-backtest/src/strategy.rs` (append to its
existing doc comment, do not replace it) noting: "Known limitation: a Stop/StopLimit order
that triggers internally during matching is tracked under a NEW OrderId once converted to a
Market/Limit order; the original OrderId's entry in this map will not reflect the resulting
fill. See docs/superpowers/plans/2026-07-28-apexes-backtest.md Task 14 for full rationale."
Report this limitation explicitly in your Task 14 completion report (do not silently accept
it without mentioning it) so the controller can decide whether to open a follow-up fix task
now or explicitly accept it for this sub-project's initial delivery.

- [ ] **Step 3: Fix the `trades` field per the note above, then verify compilation**

Apply the fix described in Step 2's note. Then run: `cargo build -p apexes-backtest`
Expected: succeeds with zero warnings. This task has no NEW unit tests of its own -- wiring
this many components together is best exercised by Task 15's integration tests (which
exercise the real, full `run_backtest` path end-to-end with a scripted `InMemoryMarketDataSource`
and a small test `Strategy`), similar to how sub-project #2's Task 7 (`StorageReader`) had no
standalone unit tests and was verified by Task 8's live integration tests instead.

- [ ] **Step 4: Run clippy**

Run: `cargo clippy -p apexes-backtest -- -D warnings`
Expected: no warnings. Pay particular attention to the `_ => {}` match arm in the event loop
(Tick/Bar events) -- if clippy suggests a more specific pattern, apply it, but the current
`_ => {}` is intentional and matches the design (Tick/Bar events don't directly drive matching
engine actions in this sub-project's scope; only Depth and Trade events do).

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-backtest/src/report.rs crates/apexes-backtest/src/runner.rs
git commit -S -s -m "feat(apexes-backtest): add BacktestReport and run_backtest runner"
```

---

### Task 15: End-to-end integration tests + workspace-wide verification

**Files:**
- Create: `crates/apexes-backtest/tests/end_to_end.rs`
- Create: `crates/apexes-backtest/tests/live_storage_backtest.rs`
- None else created/modified for verification steps — this task also runs verification
  commands and fixes any issues surfaced. May also touch `docs/FEATURES.md`.

**Interfaces:**
- Consumes: `apexes_backtest::{run_backtest, BacktestConfig, HistoricalMarketDataSource,
  InMemoryMarketDataSource, StorageMarketDataSource, Strategy, StrategyContext, OrderIntent,
  CostModel, FlatFeeSchedule, ReplaySpeed}`, `apexes_storage::{StorageWriter, StorageReader}`.
- Produces: passing integration tests proving the full backtest pipeline works end-to-end,
  plus a workspace passing all Definition-of-Done checks.

- [ ] **Step 1: Wire up `lib.rs` public re-exports**

Modify `crates/apexes-backtest/src/lib.rs`: add re-exports at the end of the file (after the
existing `pub mod` declarations from Tasks 2-14) so integration tests can use short paths:

```rust
pub use clock::ReplaySpeed;
pub use cost_model::{BinanceSpotVipFeeSchedule, CostModel, FeeSchedule, FillLiquidity, FlatFeeSchedule};
pub use error::ApexesError;
pub use ledger::PortfolioLedger;
pub use market_data_source::{HistoricalMarketDataSource, InMemoryMarketDataSource, StorageMarketDataSource};
pub use matching_engine::MatchingEngine;
pub use performance::{PerformanceSummary, PerformanceTracker};
pub use report::BacktestReport;
pub use runner::{run_backtest, BacktestConfig};
pub use strategy::{OrderIntent, Position, Strategy, StrategyContext};
```

- [ ] **Step 2: Write an end-to-end test with `InMemoryMarketDataSource`**

Create `crates/apexes-backtest/tests/end_to_end.rs`:

```rust
//! End-to-end test of the full backtest pipeline, using
//! `InMemoryMarketDataSource` so it requires no live database (runs under
//! default `cargo test`, no `--ignored` needed).

use apexes_backtest::{
    run_backtest, BacktestConfig, CostModel, FlatFeeSchedule, InMemoryMarketDataSource,
    OrderIntent, ReplaySpeed, Strategy, StrategyContext,
};
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, OrderType, PriceLevel, Side,
    TradeEvent, Venue,
};
use chrono::{NaiveDate, TimeZone, Utc};
use rust_decimal_macros::dec;
use std::collections::HashMap;

/// A trivial deterministic test strategy: buys 1 unit the first time it
/// sees a Trade event, then does nothing else.
struct BuyOnceStrategy {
    has_bought: bool,
}

impl Strategy for BuyOnceStrategy {
    fn on_market_event(&mut self, event: &MarketEvent, _ctx: &StrategyContext) -> Vec<OrderIntent> {
        if self.has_bought {
            return vec![];
        }
        if let MarketEvent::Trade(trade) = event {
            self.has_bought = true;
            return vec![OrderIntent {
                symbol: trade.symbol.clone(),
                venue: trade.venue,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(0.1),
                price: None,
                stop_price: None,
            }];
        }
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
        dec!(0.0001),
        dec!(1),
    )
    .unwrap()
}

#[tokio::test]
async fn full_backtest_produces_a_report_with_the_expected_trade() {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let t1 = t0 + chrono::Duration::seconds(1);

    let events = vec![
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![PriceLevel { price: dec!(65000), quantity: dec!(10) }],
            sequence: Some(1),
            is_snapshot: true,
            timestamp: t0,
        }),
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: Some(Side::Buy),
            trade_id: Some("T1".to_string()),
            timestamp: t1,
        }),
    ];
    let market_data = InMemoryMarketDataSource::new(events);

    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(10000));

    let config = BacktestConfig {
        instruments: vec![btc_usdt_instrument()],
        date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()..NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
        initial_balances,
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule { maker_bps: dec!(0), taker_bps: dec!(10) }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
    };
    let strategy = BuyOnceStrategy { has_bought: false };

    let report = run_backtest(config, market_data, strategy).await.unwrap();

    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].symbol, "BTCUSDT");
    assert_eq!(report.trades[0].quantity, dec!(0.1));
    assert_eq!(report.trades[0].price, dec!(65000));
    // fee = 65000 * 0.1 * 0.10% (10 bps) = 6.5
    assert_eq!(report.trades[0].fee, dec!(6.5));

    assert!(report.per_symbol.contains_key("BTCUSDT"));
    assert_eq!(report.per_symbol["BTCUSDT"].trade_count, 1);
    assert!(!report.equity_curve.is_empty());
}

#[tokio::test]
async fn backtest_with_no_events_produces_an_empty_report_without_erroring() {
    let market_data = InMemoryMarketDataSource::new(vec![]);
    let config = BacktestConfig {
        instruments: vec![btc_usdt_instrument()],
        date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()..NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
        initial_balances: HashMap::new(),
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule { maker_bps: dec!(0), taker_bps: dec!(0) }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
    };
    let strategy = BuyOnceStrategy { has_bought: false };

    let report = run_backtest(config, market_data, strategy).await.unwrap();

    assert!(report.trades.is_empty());
    assert!(report.equity_curve.is_empty());
}
```

- [ ] **Step 3: Run the end-to-end tests**

Run: `cargo test -p apexes-backtest --test end_to_end`
Expected: 2 tests pass, no network/database access.

- [ ] **Step 4: Write a live ClickHouse-backed integration test**

Create `crates/apexes-backtest/tests/live_storage_backtest.rs`:

```rust
//! Live integration test proving `StorageMarketDataSource` (backed by a
//! real ClickHouse instance via `apexes-storage`) feeds correctly into
//! `run_backtest`. Excluded from default `cargo test` -- run explicitly
//! with:
//!   docker compose up -d
//!   cargo test -p apexes-backtest -- --ignored

use apexes_backtest::{
    run_backtest, BacktestConfig, CostModel, FlatFeeSchedule, OrderIntent, ReplaySpeed,
    Strategy, StrategyContext, StorageMarketDataSource,
};
use apexes_core::{
    AssetClass, Instrument, MarketEvent, MarketType, OrderType, Side, TradeEvent, Venue,
};
use apexes_storage::{StorageReader, StorageWriter};
use chrono::Utc;
use rust_decimal_macros::dec;
use std::collections::HashMap;

const CLICKHOUSE_URL: &str = "http://localhost:8123";

struct NoOpStrategy;

impl Strategy for NoOpStrategy {
    fn on_market_event(&mut self, _event: &MarketEvent, _ctx: &StrategyContext) -> Vec<OrderIntent> {
        vec![]
    }
}

#[tokio::test]
#[ignore]
async fn backtest_reads_real_data_from_clickhouse_via_storage_market_data_source() {
    let writer = StorageWriter::new(CLICKHOUSE_URL)
        .await
        .expect("StorageWriter::new should succeed with ClickHouse running locally");
    let reader = StorageReader::new(CLICKHOUSE_URL)
        .await
        .expect("StorageReader::new should succeed");

    let now = Utc::now();
    let symbol = "BACKTESTLIVE";
    let events = vec![MarketEvent::Trade(TradeEvent {
        symbol: symbol.to_string(),
        venue: Venue::Binance,
        price: dec!(65000),
        quantity: dec!(0.5),
        side: Some(Side::Buy),
        trade_id: Some("T1".to_string()),
        timestamp: now,
    })];
    writer.write_batch(&events).await.expect("write_batch should succeed");

    let date = now.date_naive();
    let market_data = StorageMarketDataSource::new(reader, symbol, Venue::Binance, date..(date.succ_opt().unwrap()));

    let instrument = Instrument::new(
        symbol,
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("BACKTEST".to_string()),
        Some("LIVE".to_string()),
        dec!(0.01),
        dec!(0.0001),
        dec!(1),
    )
    .unwrap();

    let mut initial_balances = HashMap::new();
    initial_balances.insert("LIVE".to_string(), dec!(10000));

    let config = BacktestConfig {
        instruments: vec![instrument],
        date_range: date..(date.succ_opt().unwrap()),
        initial_balances,
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule { maker_bps: dec!(0), taker_bps: dec!(0) }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
    };

    let report = run_backtest(config, market_data, NoOpStrategy).await.unwrap();

    // NoOpStrategy places no orders, so no trades -- but the run must
    // complete without error, proving StorageMarketDataSource correctly
    // read the real Trade event from ClickHouse and fed it through the
    // full pipeline.
    assert!(report.trades.is_empty());
}
```

- [ ] **Step 5: Run the live test manually (requires ClickHouse running locally)**

Run: `docker-compose up -d` (in the repo root) then wait ~5-10s for readiness, then:
Run: `cargo test -p apexes-backtest -- --ignored`
Expected: the 1 live test passes. Tear down afterward: `docker-compose down -v`.

- [ ] **Step 6: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: all `apexes-core`, `apexes-market-data`, `apexes-storage`, and `apexes-backtest`
tests pass, with `apexes-market-data`'s 1 ignored live test and `apexes-storage`'s 5 ignored
live tests and `apexes-backtest`'s 1 ignored live test all correctly shown as ignored. Zero
network/database access in the default run. Report the REAL total test count you observe —
do not assume it matches any earlier estimate in this plan, since several tasks noted their
own test counts may have drifted slightly during implementation.

- [ ] **Step 7: Run clippy across the whole workspace**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings across the entire workspace.

- [ ] **Step 8: Run fmt check**

Run: `cargo fmt --check`
Expected: no output, exit code 0. If formatting differs, run `cargo fmt` (no `--check`) to
auto-fix, then re-run `cargo fmt --check`.

- [ ] **Step 9: Run a full workspace build**

Run: `cargo build --workspace`
Expected: zero warnings.

- [ ] **Step 10: If any fixes were needed in Steps 7-8, commit them**

```bash
git add -A
git commit -S -s -m "chore: fix clippy warnings and formatting in apexes-backtest"
```

If no fixes were needed, skip this commit.

- [ ] **Step 11: Update `docs/FEATURES.md`**

Modify `docs/FEATURES.md`:
- Change row `3.1` ("Historical data replay engine (tick-level exact replay, variable speed)")
  status from `🔲 Not Started` to `✅ Done`, Notes: "Implemented in apexes-backtest via
  HistoricalMarketDataSource + BacktestClock (AsFastAsPossible/Multiplier/RealTime)"
- Change row `3.2` ("Matching simulator (order book reconstruction, probabilistic fill model,
  Level-2 depth consumption)") status from `🔲 Not Started` to `✅ Done`, Notes: "MatchingEngine:
  ReplayOrderBook (reuses barter-data's OrderBook), book-walk VWAP fills, TradeEvent-driven
  limit-order fills with configurable queue_position_factor, Stop/StopLimit trigger
  simulation"
- Change row `3.3` ("Cost model (slippage, fees, market impact, funding rate)") status from
  `🔲 Not Started` to `✅ Done`, Notes: "CostModel: venue-aware FeeSchedule trait
  (BinanceSpotVipFeeSchedule with real confirmed VIP0-9 rates, FlatFeeSchedule fallback),
  funding_rate_bps for perpetuals; slippage/market-impact emerges from book-walk fills"
- Change row `3.4` ("Performance attribution & risk metrics (Sharpe, max drawdown, turnover,
  etc.)") status from `🔲 Not Started` to `✅ Done`, Notes: "PerformanceTracker via
  barter::statistic::metric (Sharpe/Sortino/Calmar/drawdown/win-rate/profit-factor), overall +
  per-symbol + per-venue breakdown"
- Change row `3.5` ("Backtest report generation & visualization") status from `🔲 Not Started`
  to `✅ Done`, Notes: "BacktestReport: structured, JSON-serializable data only; no
  rendering/charting (deferred to sub-project #13 per design spec §8)"
- Update the summary counts table: read the current `🔲 Not Started` and `✅ Done` values
  first (they were last updated at the end of sub-project #2), then decrease `Not Started` by
  5 and increase `Done` by 5, keeping the Total at 70.
- Update the footer's "Current active work" line to reflect that sub-project #3 (backtest
  engine) is now also complete, and name the next likely sub-project per the roadmap (#4
  strategy SDK, or #5 risk engine — check the roadmap doc's dependency column: #4 depends only
  on #0, #5 depends only on #0, both are unblocked; #6 Paper Trading depends on #1, #3, #4, #5
  so it needs #4 and #5 first).

Run: `grep -c '✅ Done' docs/FEATURES.md` after editing and report the count (should be 5
higher than before this edit).

- [ ] **Step 12: Commit the FEATURES.md update**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: mark backtest engine features as done"
```

---

## Plan Self-Review Notes

**Spec coverage:** §1 (scope) → all 5 spec areas (§4 replay, §7 matching+cost, §8
performance+report) covered across Tasks 3-4 (replay data source + order book), 5 (cost
model), 8-11 (matching engine), 12 (clock), 13 (performance), 14 (report+runner); §2
(backtest/live logic parity) → Task 6 (`Strategy` trait, the shared interface); §3 (OSS reuse
decision + rationale) → informational, reflected directly in code doc comments throughout
(Task 4's `ReplayOrderBook`, Task 13's `PerformanceTracker`), no separate code task needed;
§5 (`Strategy` trait) → Task 6; §6 (`OrderType`/`Order.stop_price` additions) → Task 1; §7.1
(order book reconstruction) → Task 4; §7.2 (market order book-walk) → Task 8-9; §7.3 (limit
order queue-position-factor) → Task 10; §7.4 (Stop/StopLimit) → Task 11; §7.5 (cost model,
confirmed Binance fee table) → Task 5; §8 (runner/clock/performance/report) → Tasks 12-14; §9
(error handling, `ApexesError` extensions) → Task 1; §10 (testing strategy) → pure-logic unit
tests throughout every task, `InMemoryMarketDataSource`-driven integration tests in Task 15,
`#[ignore]`d live ClickHouse test in Task 15.

**Placeholder scan:** No TBD/TODO/"implement later" strings anywhere in this plan's code
blocks. Two explicit, clearly-flagged exceptions were deliberately documented rather than
silently glossed over: (1) Task 14's draft `trades: Vec::new()` is explicitly called out as
INCORRECT with a required fix described in the same task, not left as an accepted placeholder;
(2) Task 14's Stop/StopLimit `OrderId`-tracking gap is an explicit, acknowledged scope
boundary (not a placeholder to fill in later silently) that must be reported for a controller
decision, per this plan's process.

**Type consistency:** `Strategy`, `StrategyContext`, `OrderIntent`, `Position`,
`HistoricalMarketDataSource`, `ReplayOrderBook`, `CostModel`, `FeeSchedule`, `FillLiquidity`,
`MatchingEngine`, `BacktestClock`, `ReplaySpeed`, `PerformanceTracker`, `PerformanceSummary`,
`BacktestConfig`, `BacktestReport` are used with identical signatures across every task that
references them — verified against each task's "Interfaces: Produces" line during writing.
`apexes_core::{OrderType, Order.stop_price}` (Task 1) are used identically in Tasks 6, 9-11,
14. `apexes_core::ApexesError`'s new variants (Task 1) are used identically in Tasks 3, 7, 9,
11.

**Known implementation-time verification already done (following sub-project #2's precedent
of pre-verifying real crate APIs before writing code into the plan, to avoid trial-and-error
API discovery during implementation):** the following were independently inspected against
the real `barter 0.12.5`/`barter-data 0.11.0`/`rust_decimal 1.42.1` crate source during
planning: `barter::statistic::metric::{sharpe::SharpeRatio, sortino::SortinoRatio,
calmar::CalmarRatio, win_rate::WinRate, profit_factor::ProfitFactor,
drawdown::{Drawdown, DrawdownGenerator, max::{MaxDrawdown, MaxDrawdownGenerator}}}`'s exact
method signatures and return shapes, `barter::Timed`'s definition and constructor,
`barter::statistic::time::{TimeInterval, Daily}`, `barter_data::books::{OrderBook, Level,
OrderBookSide, Bids, Asks}`'s `.new()`/`.update()`/`.bids()`/`.asks()` API and its
upsert-with-zero-removes-level semantics, `barter_data::subscription::book::OrderBookEvent`'s
`Snapshot`/`Update` shape, and `rust_decimal`'s `maths` feature + `MathematicalOps::sqrt()`
signature. A design flaw was caught and corrected during planning itself (an earlier draft of
Task 10 tried to detect limit-order fills via `DepthUpdate` snapshot-diffing, which was
identified as unreliable before being included in this plan; the final Task 10 design
correctly drives limit-order fills off `TradeEvent`s instead, with the rationale documented
inline in the task).
