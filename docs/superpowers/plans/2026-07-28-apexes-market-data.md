# Apexes Market Data (Sub-project #1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `BinanceMarketDataProvider`, a real `apexes_core::MarketDataProvider` implementation that streams `Trade`, `Tick`, `Depth`, and self-aggregated `Bar` events for Binance spot symbols, built on top of `barter-data`'s WebSocket transport, message parsing, and order-book synchronization.

**Architecture:** New workspace crate `apexes-market-data`. `BinanceMarketDataProvider::subscribe()` filters instruments to Binance-Spot-only, then uses `barter_data::streams::Streams::<Kind>::builder()` to establish three subscriptions (`PublicTrades`, `OrderBooksL1`, `OrderBooksL2`) covering all filtered symbols, each with `barter-data`'s built-in auto-reconnect. Three tokio tasks convert each subscription's output into `apexes_core::MarketEvent` and push into a shared `mpsc` channel; the trade task also drives a pure, unit-tested `BarAggregator` to emit `MarketEvent::Bar`. A pure `DepthConverter` maps `barter-data`'s `OrderBook`/`OrderBookEvent` into `apexes_core::DepthUpdate`.

**Tech Stack:** Rust edition 2024, `barter-data = "0.11"`, `barter-instrument = "0.3"`, `tokio`, `futures`, `async-trait`, `tracing` (logging, per project convention — never `println!`).

## Global Constraints

- Rust edition: `2024` for the new crate (workspace convention).
- All price/quantity fields on Apexes types use `rust_decimal::Decimal` — `barter-data`'s `PublicTrade.price`/`.amount` are `f64` and must be converted to `Decimal` at the conversion boundary (via `Decimal::from_f64_retain`, falling back to `Decimal::ZERO` with a `tracing::warn!` if conversion fails — `f64::NAN`/`inf` are the only failure cases for realistic price/amount values).
- No `unsafe` blocks anywhere in this plan.
- No `.unwrap()`/`.expect()` in non-test code — library code returns `Result` or handles the `None`/`Err` case explicitly (e.g. skip-and-log).
- Every commit uses `git commit -S -s` (signed + signed-off), per project convention.
- Logging uses `tracing` (`warn!`/`debug!`/`info!`), never `println!`.
- Crate version floors (from crates.io, checked 2026-07-28): `barter-data = "0.11"`, `barter-instrument = "0.3"`. Existing workspace deps (`tokio = "1.53"`, `futures = "0.3"`, `async-trait = "0.1.91"`, `rust_decimal = "1.42"`, `chrono = "0.4"`, `serde = "1.0"`) are already pinned in the workspace root `Cargo.toml` and reused via `{ workspace = true }`.
- `subscribe()` filters instruments to `venue == Venue::Binance` **and** `asset_class == AssetClass::Spot`; non-matching instruments are skipped with `tracing::warn!`; if the filtered list is empty, return `Err(ApexesError::SubscriptionFailed { .. })`.
- `subscribe()` returns `Ok` as soon as tasks are spawned — it does not wait for any WebSocket handshake to complete. Reconnect (including for initial-connect failures) is entirely handled by `barter-data`'s internal `Streams::builder().init()` mechanism (default policy: 125ms initial / 2x multiplier / 60s cap, no jitter) — this plan writes zero custom reconnect/backoff code.
- `BarAggregator` skips empty intervals (no synthetic "flat" bars) and defensively drops out-of-order trades that fall into an already-closed bucket (logged via `tracing::warn!`), never mutating an already-emitted `Bar`.
- `ticker_task` skips emitting a `Tick` when `barter-data`'s `OrderBookL1.best_bid` or `.best_ask` is `None` (logged via `tracing::debug!`, not `warn!` — an empty book side is a legitimate rare market state).
- Live-Binance integration tests are marked `#[ignore]` and excluded from default `cargo test`/`cargo test --workspace`; run explicitly via `cargo test -p apexes-market-data -- --ignored`.
- Design spec of record: `docs/superpowers/specs/2026-07-28-apexes-market-data-design.md`. If you find a mismatch between this plan and that spec while implementing, the spec wins — stop and flag it rather than improvising.

---

### Task 1: Crate scaffold + workspace registration

**Files:**
- Create: `crates/apexes-market-data/Cargo.toml`
- Create: `crates/apexes-market-data/src/lib.rs`
- Modify: `Cargo.toml` (workspace root — add `crates/apexes-market-data` to `members`, add `barter-data`/`barter-instrument` to `[workspace.dependencies]`)

**Interfaces:**
- Consumes: `apexes-core` (path dependency, unused directly by any code yet — this task proves the link works)
- Produces: a workspace member `apexes-market-data` that `cargo build --workspace` succeeds on with an empty lib, `barter-data`/`barter-instrument` available as workspace dependencies for later tasks.

- [ ] **Step 1: Add `barter-data`/`barter-instrument` to the workspace root `Cargo.toml` and register the new member**

Modify `Cargo.toml` (workspace root):

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data"]

[workspace.package]
edition = "2024"
version = "0.1.0"
license = "MIT"

[workspace.dependencies]
rust_decimal = { version = "1.42", features = ["serde"] }
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
```

- [ ] **Step 2: Create `crates/apexes-market-data/Cargo.toml`**

```toml
[package]
name = "apexes-market-data"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
barter-data = { workspace = true }
barter-instrument = { workspace = true }
tokio = { workspace = true, features = ["time"] }
futures = { workspace = true }
async-trait = { workspace = true }
rust_decimal = { workspace = true }
chrono = { workspace = true }
serde = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util", "macros"] }
```

- [ ] **Step 3: Create `crates/apexes-market-data/src/lib.rs`**

```rust
//! Apexes market data ingestion: real `apexes_core::MarketDataProvider`
//! implementations. This crate wires exchange-specific WebSocket feeds
//! (via `barter-data`) into Apexes's normalized `MarketEvent` model.

pub mod bar_aggregator;
pub mod depth_converter;
pub mod provider;

pub use provider::BinanceMarketDataProvider;
```

Note: this `lib.rs` declares modules (`bar_aggregator`, `depth_converter`, `provider`) that don't exist yet — they're created in Tasks 2, 3, and 4-7 respectively. To keep this task's own build green, create empty placeholder files now:

Create `crates/apexes-market-data/src/bar_aggregator.rs`:
```rust
// Populated in Task 2.
```

Create `crates/apexes-market-data/src/depth_converter.rs`:
```rust
// Populated in Task 3.
```

Create `crates/apexes-market-data/src/provider.rs`:
```rust
// Populated in Tasks 4-7.
pub struct BinanceMarketDataProvider;
```

- [ ] **Step 4: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: `Compiling apexes-market-data v0.1.0 (...)` then `Finished` — success, zero warnings. (This will pull and compile `barter-data`/`barter-instrument` and their transitive dependencies for the first time — expect this to take noticeably longer than previous builds; that's expected, not an error.)

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/apexes-market-data
git commit -S -s -m "chore: scaffold apexes-market-data crate in workspace"
```

---

### Task 2: `BarAggregator`

**Files:**
- Modify: `crates/apexes-market-data/src/bar_aggregator.rs`

**Interfaces:**
- Consumes: `apexes_core::{Bar, BarInterval}` (from `apexes-core`, already implemented)
- Produces: `pub struct BarAggregator` with `pub fn new(symbol: impl Into<String>, venue: apexes_core::Venue) -> Self` and `pub fn on_trade(&mut self, price: Decimal, quantity: Decimal, time: DateTime<Utc>) -> Vec<Bar>`. Later tasks (`trade_task`, Task 6) call `on_trade()` for every incoming trade and forward each returned `Bar` as `MarketEvent::Bar`.

This is the most logic-heavy task in this sub-project — get the interval-bucketing right, including the gap-skipping and out-of-order-drop behavior from the spec.

- [ ] **Step 1: Write the failing tests first**

Create `crates/apexes-market-data/src/bar_aggregator.rs`:

```rust
use apexes_core::{Bar, BarInterval, Venue};
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// All six `BarInterval` variants this aggregator maintains a bucket for.
const ALL_INTERVALS: [BarInterval; 6] = [
    BarInterval::OneSecond,
    BarInterval::OneMinute,
    BarInterval::FiveMinutes,
    BarInterval::FifteenMinutes,
    BarInterval::OneHour,
    BarInterval::OneDay,
];

fn interval_seconds(interval: BarInterval) -> i64 {
    match interval {
        BarInterval::OneSecond => 1,
        BarInterval::OneMinute => 60,
        BarInterval::FiveMinutes => 5 * 60,
        BarInterval::FifteenMinutes => 15 * 60,
        BarInterval::OneHour => 60 * 60,
        BarInterval::OneDay => 24 * 60 * 60,
    }
}

/// The interval-aligned bucket boundaries `[start, end)` for `time` at the given interval.
///
/// Computed by subtracting the remainder-into-the-current-bucket directly
/// from `time` (rather than reconstructing a `DateTime` from epoch seconds),
/// so this never needs to handle a theoretically-invalid timestamp -- `time`
/// is already a valid `DateTime<Utc>`, and subtracting a `Duration` from a
/// valid `DateTime<Utc>` cannot fail.
fn bucket_bounds(interval: BarInterval, time: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    let secs = interval_seconds(interval);
    let remainder_secs = time.timestamp().rem_euclid(secs);
    let start = time - chrono::Duration::seconds(remainder_secs);
    let end = start + chrono::Duration::seconds(secs);
    (start, end)
}

#[derive(Debug, Clone)]
struct OpenBar {
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
}

impl OpenBar {
    fn new(price: Decimal, quantity: Decimal, open_time: DateTime<Utc>, close_time: DateTime<Utc>) -> Self {
        Self {
            open: price,
            high: price,
            low: price,
            close: price,
            volume: quantity,
            open_time,
            close_time,
        }
    }

    fn accumulate(&mut self, price: Decimal, quantity: Decimal) {
        if price > self.high {
            self.high = price;
        }
        if price < self.low {
            self.low = price;
        }
        self.close = price;
        self.volume += quantity;
    }

    fn into_bar(self, symbol: String, venue: Venue, interval: BarInterval) -> Bar {
        Bar {
            symbol,
            venue,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            interval,
            open_time: self.open_time,
            close_time: self.close_time,
        }
    }
}

/// Aggregates a symbol's trade stream into OHLCV `Bar`s across all six
/// `BarInterval`s in parallel. Empty intervals are skipped (no synthetic
/// bars); out-of-order trades landing in an already-closed bucket are
/// dropped defensively.
pub struct BarAggregator {
    symbol: String,
    venue: Venue,
    open_bars: HashMap<BarInterval, OpenBar>,
    /// The end boundary of the last-closed bucket per interval, used to
    /// detect and drop late/out-of-order trades.
    last_closed_end: HashMap<BarInterval, DateTime<Utc>>,
}

impl BarAggregator {
    pub fn new(symbol: impl Into<String>, venue: Venue) -> Self {
        Self {
            symbol: symbol.into(),
            venue,
            open_bars: HashMap::new(),
            last_closed_end: HashMap::new(),
        }
    }

    /// Feeds one trade into every interval's aggregation bucket. Returns
    /// zero or more `Bar`s that closed as a result of this trade (one per
    /// interval that crossed a boundary and had at least one trade in it).
    pub fn on_trade(&mut self, price: Decimal, quantity: Decimal, time: DateTime<Utc>) -> Vec<Bar> {
        let mut closed_bars = Vec::new();

        for interval in ALL_INTERVALS {
            if let Some(closed_end) = self.last_closed_end.get(&interval) {
                if time < *closed_end {
                    tracing::warn!(
                        symbol = %self.symbol,
                        ?interval,
                        trade_time = %time,
                        closed_bucket_end = %closed_end,
                        "dropping out-of-order trade: falls into an already-closed bar bucket"
                    );
                    continue;
                }
            }

            let (bucket_start, bucket_end) = bucket_bounds(interval, time);

            let open_bucket_is_stale = self
                .open_bars
                .get(&interval)
                .map(|open_bar| open_bar.open_time != bucket_start)
                .unwrap_or(false);

            if open_bucket_is_stale {
                // Trade belongs to a later bucket than the currently open one:
                // close the open bucket, and skip any empty buckets in between.
                // `remove` returning `Some` here is guaranteed by
                // `open_bucket_is_stale` only being true when `get` above
                // already found an entry -- `if let` (not `.expect()`) still
                // handles the theoretical `None` case defensively rather
                // than panicking.
                if let Some(previous) = self.open_bars.remove(&interval) {
                    closed_bars.push(previous.into_bar(self.symbol.clone(), self.venue, interval));
                    // Everything before the new trade's bucket start is now "passed" --
                    // either closed (like the bucket we just removed) or skipped as
                    // empty -- so any trade landing before it from here on is late.
                    self.last_closed_end.insert(interval, bucket_start);
                }
            }

            match self.open_bars.get_mut(&interval) {
                Some(open_bar) => open_bar.accumulate(price, quantity),
                None => {
                    self.open_bars.insert(
                        interval,
                        OpenBar::new(price, quantity, bucket_start, bucket_end),
                    );
                }
            }
        }

        closed_bars
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn t(epoch_secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(epoch_secs, 0).single().unwrap()
    }

    #[test]
    fn single_trade_accumulates_but_does_not_close() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        let closed = agg.on_trade(dec!(65000), dec!(1.0), t(0));
        assert!(closed.is_empty(), "a single trade with no boundary crossing should not close any bar");
    }

    #[test]
    fn second_trade_in_next_one_minute_bucket_closes_the_first() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        agg.on_trade(dec!(65000), dec!(1.0), t(0));
        agg.on_trade(dec!(65100), dec!(2.0), t(30));
        // Still within [0, 60) for OneMinute -- should not close yet.
        let closed = agg.on_trade(dec!(65200), dec!(0.5), t(65));

        let one_min_bar = closed
            .iter()
            .find(|b| b.interval == BarInterval::OneMinute)
            .expect("OneMinute bar should have closed when trade at t=65 crossed the [0,60) boundary");

        // Only the trades at t(0) (price 65000) and t(30) (price 65100) are inside
        // the closed [0,60) bucket; the trade at t(65) (price 65200) opened the
        // *next* bucket and is not part of this bar.
        assert_eq!(one_min_bar.open, dec!(65000));
        assert_eq!(one_min_bar.high, dec!(65100));
        assert_eq!(one_min_bar.low, dec!(65000));
        assert_eq!(one_min_bar.close, dec!(65100));
        assert_eq!(one_min_bar.volume, dec!(3.0));
    }

    #[test]
    fn one_minute_bar_ohlcv_correctness() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        agg.on_trade(dec!(100), dec!(1), t(0));  // open
        agg.on_trade(dec!(110), dec!(2), t(10)); // high
        agg.on_trade(dec!(90), dec!(3), t(20));  // low
        agg.on_trade(dec!(105), dec!(4), t(30)); // close (within bucket)
        let closed = agg.on_trade(dec!(200), dec!(1), t(61)); // crosses into next OneMinute bucket

        let bar = closed
            .into_iter()
            .find(|b| b.interval == BarInterval::OneMinute)
            .expect("OneMinute bar should close");

        assert_eq!(bar.open, dec!(100));
        assert_eq!(bar.high, dec!(110));
        assert_eq!(bar.low, dec!(90));
        assert_eq!(bar.close, dec!(105));
        assert_eq!(bar.volume, dec!(1 + 2 + 3 + 4));
        assert_eq!(bar.symbol, "BTCUSDT");
        assert_eq!(bar.venue, Venue::Binance);
    }

    #[test]
    fn multi_interval_gap_skips_empty_buckets() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        agg.on_trade(dec!(100), dec!(1), t(0));
        // t=185 is 3 minutes and 5 seconds later: for OneMinute, this crosses
        // [0,60) -> [60,120) -> [120,180) -> lands in [180,240).
        // [60,120) and [120,180) have zero trades and must NOT produce bars.
        let closed = agg.on_trade(dec!(200), dec!(1), t(185));

        let one_min_bars: Vec<_> = closed
            .iter()
            .filter(|b| b.interval == BarInterval::OneMinute)
            .collect();

        assert_eq!(
            one_min_bars.len(),
            1,
            "only the [0,60) bucket had trades; the two empty buckets in between must not produce bars"
        );
        assert_eq!(one_min_bars[0].open_time, t(0));
    }

    #[test]
    fn out_of_order_trade_after_close_is_dropped_not_applied() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        agg.on_trade(dec!(100), dec!(1), t(0));
        let closed = agg.on_trade(dec!(200), dec!(1), t(65)); // closes [0,60) OneMinute bucket
        let closed_bar = closed
            .into_iter()
            .find(|b| b.interval == BarInterval::OneMinute)
            .expect("bucket should have closed");
        assert_eq!(closed_bar.close, dec!(100));

        // A late trade with a timestamp inside the already-closed [0,60) bucket arrives.
        let late_closed = agg.on_trade(dec!(999), dec!(50), t(30));
        assert!(
            late_closed.is_empty(),
            "a late/out-of-order trade landing in an already-closed bucket must be dropped, not produce a new bar"
        );
    }

    #[test]
    fn all_six_intervals_are_maintained_independently() {
        let mut agg = BarAggregator::new("BTCUSDT", Venue::Binance);
        agg.on_trade(dec!(100), dec!(1), t(0));
        // Advance past OneSecond and OneMinute boundaries but nothing else.
        let closed = agg.on_trade(dec!(101), dec!(1), t(61));

        let closed_intervals: std::collections::HashSet<_> =
            closed.iter().map(|b| b.interval).collect();

        assert!(closed_intervals.contains(&BarInterval::OneSecond));
        assert!(closed_intervals.contains(&BarInterval::OneMinute));
        assert!(!closed_intervals.contains(&BarInterval::FiveMinutes));
        assert!(!closed_intervals.contains(&BarInterval::OneHour));
        assert!(!closed_intervals.contains(&BarInterval::OneDay));
    }
}
```

- [ ] **Step 2: Add `rust_decimal_macros` as a dev-dependency**

Modify `crates/apexes-market-data/Cargo.toml`, add to `[dev-dependencies]`:

```toml
rust_decimal_macros = "1.40"
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p apexes-market-data bar_aggregator`
Expected: 6 tests pass (`single_trade_accumulates_but_does_not_close`, `second_trade_in_next_one_minute_bucket_closes_the_first`, `one_minute_bar_ohlcv_correctness`, `multi_interval_gap_skips_empty_buckets`, `out_of_order_trade_after_close_is_dropped_not_applied`, `all_six_intervals_are_maintained_independently`).

- [ ] **Step 4: Run clippy**

Run: `cargo clippy -p apexes-market-data -- -D warnings`
Expected: no warnings. If clippy flags `into_bar`/`accumulate` or similar as unused (since `provider.rs`/`trade_task.rs` don't consume `BarAggregator` until later tasks), that's expected for a lib crate mid-plan and should not trigger under `-D warnings` — only true logic/style lints should surface; if one does, fix it inline rather than suppressing with `#[allow]`.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-market-data/src/bar_aggregator.rs crates/apexes-market-data/Cargo.toml
git commit -S -s -m "feat(apexes-market-data): add BarAggregator with gap-skipping and out-of-order handling"
```

---

### Task 3: `DepthConverter`

**Files:**
- Modify: `crates/apexes-market-data/src/depth_converter.rs`

**Interfaces:**
- Consumes: `barter_data::books::{OrderBook, OrderBookEvent}` (external crate), `apexes_core::{DepthUpdate, PriceLevel, Venue}`
- Produces: `pub fn convert(symbol: &str, venue: Venue, event: &OrderBookEvent) -> DepthUpdate`. Task 5 (`depth_task`) calls this for every `OrderBookEvent` barter-data delivers.

- [ ] **Step 1: Write the failing tests first**

Create `crates/apexes-market-data/src/depth_converter.rs`:

```rust
use apexes_core::{DepthUpdate, PriceLevel, Venue};
use barter_data::books::{Level as BarterLevel, OrderBook, OrderBookEvent};
use chrono::Utc;
use rust_decimal::Decimal;

fn convert_level(level: &BarterLevel) -> PriceLevel {
    PriceLevel {
        price: level.price,
        quantity: level.amount,
    }
}

fn convert_book(symbol: &str, venue: Venue, book: &OrderBook, is_snapshot: bool) -> DepthUpdate {
    DepthUpdate {
        symbol: symbol.to_string(),
        venue,
        bids: book.bids().levels().iter().map(convert_level).collect(),
        asks: book.asks().levels().iter().map(convert_level).collect(),
        sequence: Some(book.sequence()),
        is_snapshot,
        timestamp: book.time_engine().unwrap_or_else(Utc::now),
    }
}

/// Converts a `barter-data` `OrderBookEvent` (already synchronized and
/// gap-checked by `barter-data` itself) into an Apexes `DepthUpdate`.
pub fn convert(symbol: &str, venue: Venue, event: &OrderBookEvent) -> DepthUpdate {
    match event {
        OrderBookEvent::Snapshot(book) => convert_book(symbol, venue, book, true),
        OrderBookEvent::Update(book) => convert_book(symbol, venue, book, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn sample_book(sequence: u64) -> OrderBook {
        OrderBook::new(
            sequence,
            Some(Utc::now()),
            vec![BarterLevel { price: dec!(64999.5), amount: dec!(1.5) }],
            vec![BarterLevel { price: dec!(65000.5), amount: dec!(2.0) }],
        )
    }

    #[test]
    fn snapshot_event_converts_with_is_snapshot_true() {
        let book = sample_book(42);
        let event = OrderBookEvent::Snapshot(book);

        let update = convert("BTCUSDT", Venue::Binance, &event);

        assert_eq!(update.symbol, "BTCUSDT");
        assert_eq!(update.venue, Venue::Binance);
        assert!(update.is_snapshot);
        assert_eq!(update.sequence, Some(42));
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.bids[0].price, dec!(64999.5));
        assert_eq!(update.bids[0].quantity, dec!(1.5));
        assert_eq!(update.asks.len(), 1);
        assert_eq!(update.asks[0].price, dec!(65000.5));
        assert_eq!(update.asks[0].quantity, dec!(2.0));
    }

    #[test]
    fn update_event_converts_with_is_snapshot_false() {
        let book = sample_book(43);
        let event = OrderBookEvent::Update(book);

        let update = convert("BTCUSDT", Venue::Binance, &event);

        assert!(!update.is_snapshot);
        assert_eq!(update.sequence, Some(43));
    }

    #[test]
    fn empty_book_sides_convert_to_empty_vecs() {
        let book = OrderBook::new(1, Some(Utc::now()), Vec::<BarterLevel>::new(), Vec::<BarterLevel>::new());
        let event = OrderBookEvent::Snapshot(book);

        let update = convert("ETHUSDT", Venue::Binance, &event);

        assert!(update.bids.is_empty());
        assert!(update.asks.is_empty());
    }
}
```

**Note for implementer:** the exact method names for accessing `OrderBookSide`'s underlying levels (`.levels()` in this draft) must be verified against the actual `barter-data 0.11` API once the dependency is fetched (Task 1). If `OrderBookSide` exposes a different accessor (e.g. a `Deref<Target = [Level]>` or an `iter()` method instead of `.levels()`), adjust `convert_level`'s call site accordingly — the conversion logic itself (mapping `price`/`amount` to `PriceLevel { price, quantity }`) does not change, only the accessor method name might. Run `cargo doc -p barter-data --open` or inspect `~/.cargo/registry/src/*/barter-data-0.11.0/src/books/mod.rs` directly if the exact accessor is unclear.

- [ ] **Step 2: Run tests to verify they pass (fixing any accessor-name mismatch first per the note above)**

Run: `cargo test -p apexes-market-data depth_converter`
Expected: 3 tests pass (`snapshot_event_converts_with_is_snapshot_true`, `update_event_converts_with_is_snapshot_false`, `empty_book_sides_convert_to_empty_vecs`).

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p apexes-market-data -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-market-data/src/depth_converter.rs
git commit -S -s -m "feat(apexes-market-data): add DepthConverter for barter-data OrderBookEvent -> DepthUpdate"
```

---

### Task 4: `BinanceMarketDataProvider` skeleton + instrument filtering

**Files:**
- Modify: `crates/apexes-market-data/src/provider.rs`

**Interfaces:**
- Consumes: `apexes_core::{Instrument, MarketDataProvider, MarketEvent, ApexesError, Venue, AssetClass}`
- Produces: `pub struct BinanceMarketDataProvider` implementing `apexes_core::MarketDataProvider`. This task implements only the filtering logic (Step 1 of the spec's `subscribe()` flow) and the trait skeleton; the actual `barter-data` subscription wiring (Steps 2-5) is Tasks 5-7. To keep this task's tests independent of network/tokio-task complexity, filtering is extracted into a standalone, unit-testable function.

- [ ] **Step 1: Write the failing tests for the filtering logic**

Create `crates/apexes-market-data/src/provider.rs` (overwriting the Task 1 placeholder):

```rust
use apexes_core::{ApexesError, AssetClass, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::BoxStream;

/// Filters `instruments` down to those tradable via this provider: Binance
/// venue and Spot asset class. Non-matching instruments are skipped with a
/// warning rather than causing the whole call to fail.
fn filter_binance_spot_instruments(instruments: &[Instrument]) -> Vec<&Instrument> {
    instruments
        .iter()
        .filter(|instrument| {
            if instrument.venue != Venue::Binance {
                tracing::warn!(
                    symbol = %instrument.symbol,
                    venue = ?instrument.venue,
                    "skipping instrument: BinanceMarketDataProvider only supports Venue::Binance"
                );
                return false;
            }
            if instrument.asset_class != AssetClass::Spot {
                tracing::warn!(
                    symbol = %instrument.symbol,
                    asset_class = ?instrument.asset_class,
                    "skipping instrument: BinanceMarketDataProvider only supports AssetClass::Spot"
                );
                return false;
            }
            true
        })
        .collect()
}

/// A `MarketDataProvider` backed by Binance Spot public WebSocket feeds,
/// via `barter-data`. Produces `Trade`, `Tick`, `Depth`, and self-aggregated
/// `Bar` events.
pub struct BinanceMarketDataProvider;

impl BinanceMarketDataProvider {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BinanceMarketDataProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MarketDataProvider for BinanceMarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let filtered = filter_binance_spot_instruments(instruments);

        if filtered.is_empty() {
            return Err(ApexesError::SubscriptionFailed {
                symbol: instruments
                    .first()
                    .map(|i| i.symbol.clone())
                    .unwrap_or_default(),
                venue: Venue::Binance,
                reason: "no instruments matched Venue::Binance + AssetClass::Spot after filtering".to_string(),
                source_detail: None,
            });
        }

        // Task 7 replaces this with the real subscription wiring.
        unimplemented!("subscription wiring lands in Task 7")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::MarketType;
    use rust_decimal_macros::dec;

    fn binance_spot_instrument(symbol: &str) -> Instrument {
        Instrument::new(
            symbol,
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            None,
            None,
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap()
    }

    fn non_binance_instrument() -> Instrument {
        Instrument::new(
            "600519",
            MarketType::AShare,
            Venue::Sse,
            AssetClass::Equity,
            None,
            None,
            dec!(0.01),
            dec!(100),
            dec!(1),
        )
        .unwrap()
    }

    fn binance_perpetual_instrument() -> Instrument {
        Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Perpetual,
            None,
            None,
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap()
    }

    #[test]
    fn filters_out_non_binance_venue() {
        let instruments = vec![non_binance_instrument(), binance_spot_instrument("BTCUSDT")];
        let filtered = filter_binance_spot_instruments(&instruments);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].symbol, "BTCUSDT");
    }

    #[test]
    fn filters_out_non_spot_asset_class() {
        let instruments = vec![binance_perpetual_instrument(), binance_spot_instrument("ETHUSDT")];
        let filtered = filter_binance_spot_instruments(&instruments);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].symbol, "ETHUSDT");
    }

    #[test]
    fn keeps_binance_spot_instruments() {
        let instruments = vec![binance_spot_instrument("BTCUSDT"), binance_spot_instrument("ETHUSDT")];
        let filtered = filter_binance_spot_instruments(&instruments);
        assert_eq!(filtered.len(), 2);
    }

    #[tokio::test]
    async fn subscribe_returns_subscription_failed_when_all_instruments_filtered_out() {
        let provider = BinanceMarketDataProvider::new();
        let instruments = vec![non_binance_instrument()];

        let result = provider.subscribe(&instruments).await;

        assert!(matches!(result, Err(ApexesError::SubscriptionFailed { .. })));
    }
}
```

- [ ] **Step 2: Run tests to verify they pass (the `unimplemented!` path is only reached by the empty-filter test, which returns `Err` before hitting it)**

Run: `cargo test -p apexes-market-data provider`
Expected: 4 tests pass (`filters_out_non_binance_venue`, `filters_out_non_spot_asset_class`, `keeps_binance_spot_instruments`, `subscribe_returns_subscription_failed_when_all_instruments_filtered_out`).

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p apexes-market-data -- -D warnings`
Expected: no warnings. (`unimplemented!()` in non-test code is intentionally present as a placeholder for Task 7 — clippy does not flag `unimplemented!()` under `-D warnings` by default, but if it does in your toolchain version, that's expected until Task 7 replaces it; do not silence it with `#[allow]`, just proceed since this task's own tests never reach that line.)

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-market-data/src/provider.rs
git commit -S -s -m "feat(apexes-market-data): add BinanceMarketDataProvider skeleton with instrument filtering"
```

---

### Task 5: `trade_task` (Trade + Bar events)

**Files:**
- Create: `crates/apexes-market-data/src/trade_task.rs`
- Modify: `crates/apexes-market-data/src/lib.rs` (add `pub mod trade_task;`)

**Interfaces:**
- Consumes: `barter_data::{streams::Streams, subscription::trade::{PublicTrades, PublicTrade}, exchange::binance::spot::BinanceSpot}`, `apexes_core::{MarketEvent, TradeEvent, Side}`, `crate::bar_aggregator::BarAggregator` (Task 2)
- Produces: `pub async fn run(symbols: Vec<String>, tx: tokio::sync::mpsc::UnboundedSender<MarketEvent>)` — spawned as a tokio task by `provider.rs` in Task 7. Converts `f64` price/amount to `Decimal`, drives `BarAggregator` per symbol, and pushes both `MarketEvent::Trade` and any closed `MarketEvent::Bar` into `tx`.

- [ ] **Step 1: Write the module**

Create `crates/apexes-market-data/src/trade_task.rs`:

```rust
use crate::bar_aggregator::BarAggregator;
use apexes_core::{MarketEvent, Side as ApexesSide, TradeEvent, Venue};
use barter_data::{
    exchange::binance::spot::BinanceSpot,
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscription::trade::PublicTrades,
};
use barter_instrument::{Side as BarterSide, instrument::market_data::kind::MarketDataInstrumentKind};
use futures::StreamExt;
use rust_decimal::Decimal;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

fn convert_side(side: BarterSide) -> ApexesSide {
    match side {
        BarterSide::Buy => ApexesSide::Buy,
        BarterSide::Sell => ApexesSide::Sell,
    }
}

/// Converts an `f64` price/amount from `barter-data` into `Decimal`. Falls
/// back to `Decimal::ZERO` with a warning on non-finite input (NaN/inf),
/// which should not occur for real exchange data but is handled explicitly
/// rather than panicking.
fn f64_to_decimal(value: f64, field_name: &str) -> Decimal {
    Decimal::from_f64_retain(value).unwrap_or_else(|| {
        tracing::warn!(value, field_name, "failed to convert f64 to Decimal (non-finite?), using zero");
        Decimal::ZERO
    })
}

/// Splits a Binance spot symbol like "BTCUSDT" into (base, quote) pairs
/// for barter-data's subscription tuple syntax. This is a heuristic split
/// for the fixed set of quote currencies this sub-project targets.
fn split_symbol(symbol: &str) -> Option<(String, String)> {
    const KNOWN_QUOTES: [&str; 3] = ["USDT", "USDC", "BUSD"];
    for quote in KNOWN_QUOTES {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return Some((base.to_lowercase(), quote.to_lowercase()));
            }
        }
    }
    None
}

/// Subscribes to Binance Spot public trades for `symbols`, converts each
/// trade into `MarketEvent::Trade`, and drives a per-symbol `BarAggregator`
/// to also emit `MarketEvent::Bar` when intervals close. Runs until the
/// underlying `barter-data` stream ends (which, given `barter-data`'s
/// internal auto-reconnect, is effectively "forever" under normal operation).
pub async fn run(symbols: Vec<String>, tx: UnboundedSender<MarketEvent>) {
    let subscriptions: Vec<_> = symbols
        .iter()
        .filter_map(|symbol| {
            let (base, quote) = split_symbol(symbol)?;
            Some((BinanceSpot::default(), base, quote, MarketDataInstrumentKind::Spot, PublicTrades))
        })
        .collect();

    if subscriptions.is_empty() {
        tracing::warn!(?symbols, "trade_task: no valid Binance spot subscriptions derived from symbols");
        return;
    }

    let streams = match Streams::<PublicTrades>::builder().subscribe(subscriptions).init().await {
        Ok(streams) => streams,
        Err(error) => {
            tracing::warn!(?error, "trade_task: failed to initialize barter-data PublicTrades stream");
            return;
        }
    };

    let mut aggregators: HashMap<String, BarAggregator> = symbols
        .iter()
        .map(|symbol| (symbol.clone(), BarAggregator::new(symbol.clone(), Venue::Binance)))
        .collect();

    let mut merged = streams.select_all();
    while let Some(event) = merged.next().await {
        let market_event = match event {
            barter_data::streams::reconnect::Event::Item(Ok(market_event)) => market_event,
            barter_data::streams::reconnect::Event::Item(Err(error)) => {
                tracing::warn!(?error, "trade_task: received error item from barter-data stream");
                continue;
            }
            barter_data::streams::reconnect::Event::Reconnecting(exchange) => {
                tracing::info!(?exchange, "trade_task: barter-data stream reconnecting");
                continue;
            }
        };

        let symbol = market_event.instrument.to_string().to_uppercase();
        let trade: barter_data::subscription::trade::PublicTrade = market_event.kind;

        let price = f64_to_decimal(trade.price, "price");
        let quantity = f64_to_decimal(trade.amount, "amount");
        let timestamp = market_event.time_exchange;

        let apexes_trade = MarketEvent::Trade(TradeEvent {
            symbol: symbol.clone(),
            venue: Venue::Binance,
            price,
            quantity,
            side: Some(convert_side(trade.side)),
            trade_id: Some(trade.id.clone()),
            timestamp,
        });

        if tx.send(apexes_trade).is_err() {
            tracing::warn!("trade_task: output channel closed, stopping");
            return;
        }

        if let Some(aggregator) = aggregators.get_mut(&symbol) {
            for bar in aggregator.on_trade(price, quantity, timestamp) {
                if tx.send(MarketEvent::Bar(bar)).is_err() {
                    tracing::warn!("trade_task: output channel closed, stopping");
                    return;
                }
            }
        }
    }
}
```

**Note for implementer:** the exact type of `market_event.instrument` (used here via `.to_string().to_uppercase()` to recover the symbol) depends on `barter-data 0.11`'s `MarketDataInstrument` `Display` implementation — verify this actually round-trips to e.g. `"BTCUSDT"` by inspecting `barter_instrument::instrument::market_data::MarketDataInstrument`'s `Display` impl once the dependency is available (Task 1). If it doesn't produce the expected format, use the `symbols` list index correlation instead (each `Streams::builder().subscribe([...])` call preserves the tuple order, though `select_all()` merges across symbols so this may require keying by `market_event.exchange`/a different correlation strategy — flag this as a concern in your task report if the `Display`-based approach doesn't work cleanly, and use whatever `barter-data` actually exposes for recovering the original symbol string from a `MarketEvent`).

- [ ] **Step 2: Wire the module into `lib.rs`**

Modify `crates/apexes-market-data/src/lib.rs`:

```rust
//! Apexes market data ingestion: real `apexes_core::MarketDataProvider`
//! implementations. This crate wires exchange-specific WebSocket feeds
//! (via `barter-data`) into Apexes's normalized `MarketEvent` model.

pub mod bar_aggregator;
pub mod depth_converter;
pub mod provider;
pub mod trade_task;

pub use provider::BinanceMarketDataProvider;
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p apexes-market-data`
Expected: succeeds with zero warnings. This task has no new unit tests of its own (its logic — symbol splitting, side/decimal conversion — is exercised indirectly by the live integration test in Task 8; the pure `split_symbol`/`f64_to_decimal` helpers are simple enough that dedicated unit tests are optional, but if `cargo clippy` or code review flags them as needing coverage, add straightforward unit tests for `split_symbol("BTCUSDT") == Some(("btc", "usdt"))` and `f64_to_decimal(f64::NAN, "x") == Decimal::ZERO`).

- [ ] **Step 4: Add unit tests for `split_symbol` and `f64_to_decimal`**

Append to `crates/apexes-market-data/src/trade_task.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_symbol_handles_usdt_pairs() {
        assert_eq!(split_symbol("BTCUSDT"), Some(("btc".to_string(), "usdt".to_string())));
    }

    #[test]
    fn split_symbol_handles_usdc_pairs() {
        assert_eq!(split_symbol("ETHUSDC"), Some(("eth".to_string(), "usdc".to_string())));
    }

    #[test]
    fn split_symbol_returns_none_for_unknown_quote() {
        assert_eq!(split_symbol("BTCXYZ"), None);
    }

    #[test]
    fn f64_to_decimal_converts_normal_values() {
        assert_eq!(f64_to_decimal(65000.5, "price"), Decimal::try_from(65000.5).unwrap());
    }

    #[test]
    fn f64_to_decimal_falls_back_to_zero_on_nan() {
        assert_eq!(f64_to_decimal(f64::NAN, "price"), Decimal::ZERO);
    }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p apexes-market-data trade_task`
Expected: 5 tests pass.

- [ ] **Step 6: Run clippy**

Run: `cargo clippy -p apexes-market-data -- -D warnings`
Expected: no warnings.

- [ ] **Step 7: Commit**

```bash
git add crates/apexes-market-data/src/trade_task.rs crates/apexes-market-data/src/lib.rs
git commit -S -s -m "feat(apexes-market-data): add trade_task producing Trade and Bar events"
```

---

### Task 6: `ticker_task` (Tick events)

**Files:**
- Create: `crates/apexes-market-data/src/ticker_task.rs`
- Modify: `crates/apexes-market-data/src/lib.rs` (add `pub mod ticker_task;`)

**Interfaces:**
- Consumes: `barter_data::{streams::Streams, subscription::book::{OrderBooksL1, OrderBookL1}, exchange::binance::spot::BinanceSpot}`, `apexes_core::{MarketEvent, Tick, Venue}`
- Produces: `pub async fn run(symbols: Vec<String>, tx: tokio::sync::mpsc::UnboundedSender<MarketEvent>)` — spawned by `provider.rs` in Task 7.

- [ ] **Step 1: Write the module**

Create `crates/apexes-market-data/src/ticker_task.rs`:

```rust
use apexes_core::{MarketEvent, Tick, Venue};
use barter_data::{
    exchange::binance::spot::BinanceSpot,
    streams::Streams,
    subscription::book::OrderBooksL1,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures::StreamExt;
use tokio::sync::mpsc::UnboundedSender;

/// Splits a Binance spot symbol like "BTCUSDT" into (base, quote) pairs.
/// Duplicated from `trade_task` deliberately -- see Task 6 self-review note
/// on whether to extract a shared `symbol.rs` module.
fn split_symbol(symbol: &str) -> Option<(String, String)> {
    const KNOWN_QUOTES: [&str; 3] = ["USDT", "USDC", "BUSD"];
    for quote in KNOWN_QUOTES {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return Some((base.to_lowercase(), quote.to_lowercase()));
            }
        }
    }
    None
}

/// Subscribes to Binance Spot bookTicker (best bid/ask) for `symbols` and
/// converts each update into `MarketEvent::Tick`. Updates where either side
/// of the book is empty (`best_bid`/`best_ask` is `None`) are skipped --
/// see design spec §3 "ticker_task 的空盘口处理".
pub async fn run(symbols: Vec<String>, tx: UnboundedSender<MarketEvent>) {
    let subscriptions: Vec<_> = symbols
        .iter()
        .filter_map(|symbol| {
            let (base, quote) = split_symbol(symbol)?;
            Some((BinanceSpot::default(), base, quote, MarketDataInstrumentKind::Spot, OrderBooksL1))
        })
        .collect();

    if subscriptions.is_empty() {
        tracing::warn!(?symbols, "ticker_task: no valid Binance spot subscriptions derived from symbols");
        return;
    }

    let streams = match Streams::<OrderBooksL1>::builder().subscribe(subscriptions).init().await {
        Ok(streams) => streams,
        Err(error) => {
            tracing::warn!(?error, "ticker_task: failed to initialize barter-data OrderBooksL1 stream");
            return;
        }
    };

    let mut merged = streams.select_all();
    while let Some(event) = merged.next().await {
        let market_event = match event {
            barter_data::streams::reconnect::Event::Item(Ok(market_event)) => market_event,
            barter_data::streams::reconnect::Event::Item(Err(error)) => {
                tracing::warn!(?error, "ticker_task: received error item from barter-data stream");
                continue;
            }
            barter_data::streams::reconnect::Event::Reconnecting(exchange) => {
                tracing::info!(?exchange, "ticker_task: barter-data stream reconnecting");
                continue;
            }
        };

        let symbol = market_event.instrument.to_string().to_uppercase();
        let l1: barter_data::subscription::book::OrderBookL1 = market_event.kind;

        let (Some(best_bid), Some(best_ask)) = (l1.best_bid, l1.best_ask) else {
            tracing::debug!(symbol = %symbol, "ticker_task: skipping update with empty book side");
            continue;
        };

        let tick = MarketEvent::Tick(Tick {
            symbol,
            venue: Venue::Binance,
            bid_price: best_bid.price,
            bid_size: best_bid.amount,
            ask_price: best_ask.price,
            ask_size: best_ask.amount,
            timestamp: market_event.time_exchange,
        });

        if tx.send(tick).is_err() {
            tracing::warn!("ticker_task: output channel closed, stopping");
            return;
        }
    }
}
```

- [ ] **Step 2: Wire the module into `lib.rs`**

Modify `crates/apexes-market-data/src/lib.rs`:

```rust
pub mod bar_aggregator;
pub mod depth_converter;
pub mod provider;
pub mod ticker_task;
pub mod trade_task;

pub use provider::BinanceMarketDataProvider;
```

- [ ] **Step 3: Add a unit test for `split_symbol` (duplicated logic, same as Task 5)**

Append to `crates/apexes-market-data/src/ticker_task.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_symbol_handles_usdt_pairs() {
        assert_eq!(split_symbol("BTCUSDT"), Some(("btc".to_string(), "usdt".to_string())));
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p apexes-market-data ticker_task`
Expected: 1 test passes.

- [ ] **Step 5: Run clippy**

Run: `cargo clippy -p apexes-market-data -- -D warnings`
Expected: no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-market-data/src/ticker_task.rs crates/apexes-market-data/src/lib.rs
git commit -S -s -m "feat(apexes-market-data): add ticker_task producing Tick events"
```

**Self-review note carried into Task 7:** `split_symbol` is now duplicated across `trade_task.rs` and `ticker_task.rs` (and will be needed again by `depth_task.rs` in Task 7). This is a DRY violation the plan is deliberately allowing for now to keep Tasks 5-6 independently reviewable without introducing a shared module before Task 7 makes the duplication's shape fully clear; Task 7 extracts a shared `symbol.rs` module as its first step, consolidating all three copies.

---

### Task 7: `depth_task` + full `BinanceMarketDataProvider::subscribe()` wiring

**Files:**
- Create: `crates/apexes-market-data/src/symbol.rs` (extracted shared `split_symbol`, per Task 6's self-review note)
- Create: `crates/apexes-market-data/src/depth_task.rs`
- Modify: `crates/apexes-market-data/src/trade_task.rs` (remove local `split_symbol`, use `crate::symbol::split_symbol`)
- Modify: `crates/apexes-market-data/src/ticker_task.rs` (remove local `split_symbol`, use `crate::symbol::split_symbol`)
- Modify: `crates/apexes-market-data/src/provider.rs` (replace the `unimplemented!()` with real task-spawning)
- Modify: `crates/apexes-market-data/src/lib.rs` (add `pub mod symbol; pub mod depth_task;`)

**Interfaces:**
- Consumes: `crate::{bar_aggregator, depth_converter, trade_task, ticker_task}` (Tasks 2-6), `barter_data::{streams::Streams, subscription::book::OrderBooksL2}`
- Produces: `BinanceMarketDataProvider::subscribe()` fully implemented per the design spec's architecture (§3): spawns `trade_task::run`, `ticker_task::run`, `depth_task::run` as tokio tasks feeding one shared `mpsc` channel, returns the receiver as a `BoxStream`.

- [ ] **Step 1: Extract `symbol.rs`**

Create `crates/apexes-market-data/src/symbol.rs`:

```rust
/// Splits a Binance spot symbol like "BTCUSDT" into (base, quote) pairs
/// for barter-data's subscription tuple syntax. This is a heuristic split
/// for the fixed set of quote currencies this sub-project targets.
pub fn split_symbol(symbol: &str) -> Option<(String, String)> {
    const KNOWN_QUOTES: [&str; 3] = ["USDT", "USDC", "BUSD"];
    for quote in KNOWN_QUOTES {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return Some((base.to_lowercase(), quote.to_lowercase()));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_symbol_handles_usdt_pairs() {
        assert_eq!(split_symbol("BTCUSDT"), Some(("btc".to_string(), "usdt".to_string())));
    }

    #[test]
    fn split_symbol_handles_usdc_pairs() {
        assert_eq!(split_symbol("ETHUSDC"), Some(("eth".to_string(), "usdc".to_string())));
    }

    #[test]
    fn split_symbol_returns_none_for_unknown_quote() {
        assert_eq!(split_symbol("BTCXYZ"), None);
    }
}
```

- [ ] **Step 2: Update `trade_task.rs` to use the shared module**

In `crates/apexes-market-data/src/trade_task.rs`, remove the local `split_symbol` function definition (lines defining `fn split_symbol(...)`) and its now-duplicate unit tests (`split_symbol_handles_usdt_pairs`, `split_symbol_handles_usdc_pairs`, `split_symbol_returns_none_for_unknown_quote` in its `#[cfg(test)] mod tests`), and add at the top of the file:

```rust
use crate::symbol::split_symbol;
```

Keep the `f64_to_decimal` function and its tests (`f64_to_decimal_converts_normal_values`, `f64_to_decimal_falls_back_to_zero_on_nan`) — those are not duplicated elsewhere.

- [ ] **Step 3: Update `ticker_task.rs` to use the shared module**

In `crates/apexes-market-data/src/ticker_task.rs`, remove the local `split_symbol` function and its test module entirely, and add at the top of the file:

```rust
use crate::symbol::split_symbol;
```

- [ ] **Step 4: Write `depth_task.rs`**

Create `crates/apexes-market-data/src/depth_task.rs`:

```rust
use crate::depth_converter;
use crate::symbol::split_symbol;
use apexes_core::{MarketEvent, Venue};
use barter_data::{
    exchange::binance::spot::BinanceSpot,
    streams::Streams,
    subscription::book::OrderBooksL2,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures::StreamExt;
use tokio::sync::mpsc::UnboundedSender;

/// Subscribes to Binance Spot L2 order book updates for `symbols`
/// (snapshot + incremental, synchronized and gap-checked by `barter-data`
/// itself) and converts each event into `MarketEvent::Depth` via
/// `depth_converter::convert`.
pub async fn run(symbols: Vec<String>, tx: UnboundedSender<MarketEvent>) {
    let subscriptions: Vec<_> = symbols
        .iter()
        .filter_map(|symbol| {
            let (base, quote) = split_symbol(symbol)?;
            Some((BinanceSpot::default(), base, quote, MarketDataInstrumentKind::Spot, OrderBooksL2))
        })
        .collect();

    if subscriptions.is_empty() {
        tracing::warn!(?symbols, "depth_task: no valid Binance spot subscriptions derived from symbols");
        return;
    }

    let streams = match Streams::<OrderBooksL2>::builder().subscribe(subscriptions).init().await {
        Ok(streams) => streams,
        Err(error) => {
            tracing::warn!(?error, "depth_task: failed to initialize barter-data OrderBooksL2 stream");
            return;
        }
    };

    let mut merged = streams.select_all();
    while let Some(event) = merged.next().await {
        let market_event = match event {
            barter_data::streams::reconnect::Event::Item(Ok(market_event)) => market_event,
            barter_data::streams::reconnect::Event::Item(Err(error)) => {
                tracing::warn!(?error, "depth_task: received error item from barter-data stream");
                continue;
            }
            barter_data::streams::reconnect::Event::Reconnecting(exchange) => {
                tracing::info!(?exchange, "depth_task: barter-data stream reconnecting (next update will be is_snapshot: true)");
                continue;
            }
        };

        let symbol = market_event.instrument.to_string().to_uppercase();
        let depth_update = depth_converter::convert(&symbol, Venue::Binance, &market_event.kind);

        if tx.send(MarketEvent::Depth(depth_update)).is_err() {
            tracing::warn!("depth_task: output channel closed, stopping");
            return;
        }
    }
}
```

- [ ] **Step 5: Wire `symbol` and `depth_task` modules into `lib.rs`**

Modify `crates/apexes-market-data/src/lib.rs`:

```rust
//! Apexes market data ingestion: real `apexes_core::MarketDataProvider`
//! implementations. This crate wires exchange-specific WebSocket feeds
//! (via `barter-data`) into Apexes's normalized `MarketEvent` model.

pub mod bar_aggregator;
pub mod depth_converter;
pub mod depth_task;
pub mod provider;
pub mod symbol;
pub mod ticker_task;
pub mod trade_task;

pub use provider::BinanceMarketDataProvider;
```

- [ ] **Step 6: Replace `provider.rs`'s `unimplemented!()` with real task-spawning**

Modify `crates/apexes-market-data/src/provider.rs`. Replace the entire `subscribe` method body:

```rust
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let filtered = filter_binance_spot_instruments(instruments);

        if filtered.is_empty() {
            return Err(ApexesError::SubscriptionFailed {
                symbol: instruments
                    .first()
                    .map(|i| i.symbol.clone())
                    .unwrap_or_default(),
                venue: Venue::Binance,
                reason: "no instruments matched Venue::Binance + AssetClass::Spot after filtering".to_string(),
                source_detail: None,
            });
        }

        let symbols: Vec<String> = filtered.iter().map(|i| i.symbol.clone()).collect();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(crate::trade_task::run(symbols.clone(), tx.clone()));
        tokio::spawn(crate::ticker_task::run(symbols.clone(), tx.clone()));
        tokio::spawn(crate::depth_task::run(symbols, tx));

        Ok(Box::pin(tokio_stream::wrappers::UnboundedReceiverStream::new(rx)))
    }
```

Add the required imports at the top of `provider.rs` (alongside the existing ones):

```rust
use tokio_stream::wrappers::UnboundedReceiverStream;
```

- [ ] **Step 7: Add `tokio-stream` dependency**

Modify `crates/apexes-market-data/Cargo.toml`, add to `[dependencies]`:

```toml
tokio-stream = "0.1"
```

- [ ] **Step 8: Run the full test suite**

Run: `cargo test -p apexes-market-data`
Expected: all previously-passing tests still pass (bar_aggregator: 6, depth_converter: 3, provider: 4, trade_task: 2 remaining after dedup — `f64_to_decimal_converts_normal_values`, `f64_to_decimal_falls_back_to_zero_on_nan` — ticker_task: 0 remaining after dedup, symbol: 3). Total: 18 tests passing (exact count may shift slightly depending on how clippy/compiler feedback shapes final file contents during implementation — the important invariant is that nothing that passed before this task regresses).

- [ ] **Step 9: Run clippy**

Run: `cargo clippy -p apexes-market-data -- -D warnings`
Expected: no warnings. The `unimplemented!()` from Task 4 is now gone, so any earlier tolerance for it no longer applies — this must be fully clean.

- [ ] **Step 10: Run a full workspace build**

Run: `cargo build --workspace`
Expected: zero warnings across all crates.

- [ ] **Step 11: Commit**

```bash
git add crates/apexes-market-data/src/symbol.rs crates/apexes-market-data/src/depth_task.rs crates/apexes-market-data/src/trade_task.rs crates/apexes-market-data/src/ticker_task.rs crates/apexes-market-data/src/provider.rs crates/apexes-market-data/src/lib.rs crates/apexes-market-data/Cargo.toml
git commit -S -s -m "feat(apexes-market-data): wire BinanceMarketDataProvider::subscribe() with depth_task and shared symbol module"
```

---

### Task 8: Live Binance integration test

**Files:**
- Create: `crates/apexes-market-data/tests/live_binance.rs`

**Interfaces:**
- Consumes: `apexes_market_data::BinanceMarketDataProvider`, `apexes_core::{Instrument, MarketDataProvider, MarketType, Venue, AssetClass}`
- Produces: an `#[ignore]`d integration test proving the whole pipeline works end-to-end against real Binance.

- [ ] **Step 1: Write the integration test**

Create `crates/apexes-market-data/tests/live_binance.rs`:

```rust
//! Live integration test against real Binance WebSocket endpoints.
//! Excluded from default `cargo test` -- run explicitly with:
//!   cargo test -p apexes-market-data -- --ignored

use apexes_core::{AssetClass, Instrument, MarketDataProvider, MarketEvent, MarketType, Venue};
use apexes_market_data::BinanceMarketDataProvider;
use futures::StreamExt;
use rust_decimal_macros::dec;
use std::time::Duration;

#[tokio::test]
#[ignore]
async fn subscribing_to_btcusdt_produces_all_four_market_event_variants() {
    let provider = BinanceMarketDataProvider::new();
    let instrument = Instrument::new(
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
    .unwrap();

    let mut stream = provider
        .subscribe(&[instrument])
        .await
        .expect("subscribe should succeed for a valid Binance Spot instrument");

    let mut saw_trade = false;
    let mut saw_tick = false;
    let mut saw_depth = false;
    let mut saw_bar = false;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);

    while tokio::time::Instant::now() < deadline && !(saw_trade && saw_tick && saw_depth && saw_bar) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(event)) => match event {
                MarketEvent::Trade(_) => saw_trade = true,
                MarketEvent::Tick(_) => saw_tick = true,
                MarketEvent::Depth(_) => saw_depth = true,
                MarketEvent::Bar(_) => saw_bar = true,
            },
            Ok(None) => break,
            Err(_timeout) => break,
        }
    }

    assert!(saw_trade, "expected at least one MarketEvent::Trade within 90s");
    assert!(saw_tick, "expected at least one MarketEvent::Tick within 90s");
    assert!(saw_depth, "expected at least one MarketEvent::Depth within 90s");
    assert!(
        saw_bar,
        "expected at least one MarketEvent::Bar within 90s (requires at least one OneSecond interval boundary crossed with a trade before it)"
    );
}
```

- [ ] **Step 2: Verify the test is excluded from default runs**

Run: `cargo test -p apexes-market-data`
Expected: `subscribing_to_btcusdt_produces_all_four_market_event_variants` does NOT run (shown as `1 ignored` in the summary), and no network access occurs.

- [ ] **Step 3: Run the live test manually (requires network access to Binance)**

Run: `cargo test -p apexes-market-data -- --ignored`
Expected: `subscribing_to_btcusdt_produces_all_four_market_event_variants ... ok` (may take up to 90 seconds; if it fails due to no network access in the current environment, that is expected and acceptable for this step — the test's correctness is what matters, not whether this particular execution environment has internet access. Report the outcome honestly in your task report either way.)

- [ ] **Step 4: Run clippy**

Run: `cargo clippy -p apexes-market-data --tests -- -D warnings`
Expected: no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-market-data/tests/live_binance.rs
git commit -S -s -m "test(apexes-market-data): add ignored live Binance integration test"
```

---

### Task 9: Workspace-wide verification

**Files:**
- None created/modified — this task only runs verification commands and fixes any issues surfaced. May also touch `docs/FEATURES.md`.

**Interfaces:**
- Consumes: the entire `apexes-market-data` crate built in Tasks 1-8.
- Produces: a workspace passing all Definition-of-Done checks from the design spec §9.

- [ ] **Step 1: Run the full test suite (default, no `--ignored`)**

Run: `cargo test --workspace`
Expected: all `apexes-core` tests (29, per the foundation-skeleton sub-project) still pass, plus all `apexes-market-data` unit tests from Tasks 2-7 pass, plus the Task 8 integration test shows as ignored (not run). Zero network access. Zero failures.

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings across the entire workspace (including `--tests` for the integration test file).

- [ ] **Step 3: Run fmt check**

Run: `cargo fmt --check`
Expected: no output, exit code 0. If formatting differs, run `cargo fmt` (no `--check`) to auto-fix, then re-run `cargo fmt --check`.

- [ ] **Step 4: Run a full build**

Run: `cargo build --workspace`
Expected: zero warnings.

- [ ] **Step 5: If any fixes were needed in Steps 2-3, commit them**

```bash
git add -A
git commit -S -s -m "chore: fix clippy warnings and formatting in apexes-market-data"
```

If no fixes were needed, skip this commit.

- [ ] **Step 6: Update `docs/FEATURES.md`**

Modify `docs/FEATURES.md`: change status for row `1.1.1` ("CEX REST/WebSocket market data (Binance, OKX, Coinbase, etc.)") from `🔲 Not Started` to `✅ Done`, and update its Notes column to: `Binance Spot implemented via apexes-market-data (BinanceMarketDataProvider); OKX/Coinbase/other CEX deferred`. Update the summary counts table: `🔲 Not Started` 59 → 58, `✅ Done` (current value, from the foundation-skeleton sub-project's prior update) +1.

Run: `grep -c '✅ Done' docs/FEATURES.md` after editing and note the count for your report — it should be one higher than before this edit.

- [ ] **Step 7: Commit the FEATURES.md update**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: mark CEX market data (Binance) feature as done"
```

---

## Plan Self-Review Notes

**Spec coverage:** §1 (scope) → Task 1 (crate scaffold), Task 4 (Spot/Binance filtering); §2 (barter-data reuse decisions) → reflected throughout Tasks 3, 5-7's use of `barter_data::streams::Streams`/`books`/reconnect APIs, no custom reconnect code written anywhere in this plan; §3 (architecture) → Tasks 4-7 (provider skeleton, trade_task, ticker_task, depth_task, final wiring); §4 (instrument filtering/error handling) → Task 4; §5 (reconnect — now barter-data's default, no custom code) → Tasks 5-7 rely on `Streams::builder().init()`'s automatic behavior, explicitly documented in each task's module as "handled by barter-data"; §6 (BarAggregator design) → Task 2 (gap-skipping, out-of-order-drop, all 6 intervals); §7 (testing plan) → Task 2/3 unit tests, Task 8 live integration test with `#[ignore]`; §8 (dependencies) → Task 1 (note: `rand` and `async-trait`'s trait-object usage in `provider.rs` are both present per spec's final dependency list); §9 (DoD) → Task 9.

**Placeholder scan:** No TBD/TODO/"implement later" strings. The one `unimplemented!()` in Task 4 is intentional and explicitly resolved by Task 7 — not a plan-authoring placeholder, but a genuine intermediate state between two tasks, called out explicitly in both tasks' text.

**Type consistency:** `BinanceMarketDataProvider`, `filter_binance_spot_instruments`, `BarAggregator::on_trade`, `depth_converter::convert`, `split_symbol` are used with identical signatures everywhere they're referenced across tasks — verified against each task's "Interfaces: Produces" line. One deliberate exception, called out explicitly in-plan: `split_symbol` is temporarily duplicated in Tasks 5-6 and consolidated into `crate::symbol` in Task 7 — this is flagged in Task 6's closing self-review note and resolved in Task 7 Steps 1-3, not a bug.

**Known implementation-time uncertainties flagged for the implementer:** two spots in this plan (Task 3's `OrderBookSide` level-accessor method name, and Task 5/6/7's `market_event.instrument` symbol-recovery approach) depend on `barter-data 0.11`'s exact public API surface, which could not be fully verified without the crate physically present in `Cargo.lock` (it's added in Task 1). Both are called out explicitly in-task with instructions on how to resolve the ambiguity by inspecting the actual dependency once fetched, rather than guessing silently.
