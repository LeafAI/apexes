# Apexes: Mark-Price Data Pipeline & General Ingestion Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the design in `docs/superpowers/specs/2026-08-02-apexes-mark-price-pipeline-design.md`: real historical/live mark-price data for Binance USDS-M Perpetual instruments (a new `MarketEvent::MarkPrice` variant, ingestion, ClickHouse storage, backtest replay), plus the project's first general-purpose live-market-data-to-ClickHouse ingestion loop — both prerequisites for roadmap sub-project `#5`'s remaining scope (`docs/FEATURES.md` row `6.4`, margin & leverage monitoring).

**Architecture:** Four sequential phases (dependency order: the core type must exist before anything produces/consumes it). Phase 1 adds `MarketEvent::MarkPrice` to `apexes-core` and updates every exhaustive match over `MarketEvent` across the workspace. Phase 2 extends `apexes-market-data`: parameterizes the existing Spot-only ingestion tasks to also support `AssetClass::Perpetual` via Binance's USDS-M Futures API, adds a new hand-rolled-WebSocket `mark_price_task`, and extends `BinanceMarketDataProvider::subscribe` to route Perpetual instruments to all four tasks. Phase 3 adds `MarkPriceRow`/the `mark_prices` table to `apexes-storage`. Phase 4 adds the general-purpose `run_ingestion_loop` (in `apexes-market-data`, depending on `apexes-storage`) and verifies `apexes-backtest`'s existing `StorageMarketDataSource` replays `MarkPrice` events correctly with no structural change needed.

**Tech Stack:** Rust 2024, existing workspace deps only (`barter-data` 0.11's `BinanceFuturesUsd` type already supports `PublicTrades`/`OrderBooksL1`/`OrderBooksL2` for USDS-M Futures; `tokio-tungstenite` is already a workspace dependency, used by `apexes-gateway-binance::user_data_stream` for the same hand-rolled-WebSocket pattern this plan's `mark_price_task` follows). No new external crates.

## Global Constraints

- Rust edition 2024, workspace-wide `cargo check`/`cargo clippy --all-targets -- -D warnings`/`cargo fmt --check` must stay clean after every task.
- No `unsafe` blocks. Never bare `.unwrap()`/`.expect()` in production code (test code may use them freely, matching existing convention).
- `thiserror` for all new error types; `serde`/`chrono`/`rust_decimal` conventions match existing crates exactly (e.g. `Decimal` for all price fields, `DateTime<Utc>` for all timestamps).
- Every commit signed: `git commit -S -s`.
- Existing Spot behavior must not change for any task in this plan — every extension is additive (Perpetual support alongside Spot, a new event variant alongside existing ones, a new table alongside existing ones).
- Full design rationale (why mark price needs its own pipeline instead of reusing last-trade price, why the general ingestion loop is in scope here, why `barter-data` can't provide mark price itself) is in the design spec — read it before starting if anything below is unclear on *why*, not just *what*.

---

## Phase 1: `MarketEvent::MarkPrice` core type

### Task 1: Add `MarketEvent::MarkPrice(MarkPriceUpdate)` to `apexes-core`

**Files:**
- Modify: `crates/apexes-core/src/market_data.rs`

**Interfaces:**
- Produces: `apexes_core::MarketEvent::MarkPrice(MarkPriceUpdate)` variant; `apexes_core::MarkPriceUpdate { symbol: String, venue: Venue, mark_price: Decimal, timestamp: DateTime<Utc> }` (all fields `pub`).

- [ ] **Step 1: Add the `MarkPriceUpdate` struct and `MarketEvent` variant**

```rust
// crates/apexes-core/src/market_data.rs -- change the MarketEvent enum:
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketEvent {
    Tick(Tick),
    Depth(DepthUpdate),
    Bar(Bar),
    Trade(TradeEvent),
    MarkPrice(MarkPriceUpdate),
}
```

Add this new struct after `TradeEvent`'s definition (before the `MarketDataProvider` trait):

```rust
/// A perpetual/futures instrument's current mark price -- the
/// index-and-funding-basis-adjusted price real exchanges use for
/// margin ratio / liquidation / unrealized PnL calculations,
/// DELIBERATELY separate from `TradeEvent.price` (last trade) or
/// order-book mid, both of which are more easily manipulated/noisy
/// for that purpose. Meaningless for `AssetClass::Spot` instruments --
/// never emitted for them. See
/// `docs/superpowers/specs/2026-08-02-apexes-mark-price-pipeline-design.md`
/// for the full rationale.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkPriceUpdate {
    pub symbol: String,
    pub venue: Venue,
    pub mark_price: Decimal,
    pub timestamp: DateTime<Utc>,
}
```

- [ ] **Step 2: Write a serde round-trip test**

Add to `crates/apexes-core/src/market_data.rs`'s existing `#[cfg(test)] mod tests`, following the exact pattern of `trade_event_serde_round_trip`:

```rust
    #[test]
    fn mark_price_event_serde_round_trip() {
        let event = MarketEvent::MarkPrice(MarkPriceUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            mark_price: dec!(65012.34),
            timestamp: Utc::now(),
        });
        let json = serde_json::to_string(&event).unwrap();
        let round_tripped: MarketEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, round_tripped);
    }
```

- [ ] **Step 3: Run this crate's tests**

Run: `cargo test -p apexes-core`
Expected: PASS, including the new `mark_price_event_serde_round_trip` test. This step will initially FAIL TO COMPILE the rest of the workspace (every exhaustive `match` over `MarketEvent` elsewhere) -- that is expected and fixed by Task 2. Confirm `apexes-core` itself compiles and its own tests pass with `cargo test -p apexes-core` (not `cargo test --workspace`, which will still fail until Task 2 lands).

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-core/src/market_data.rs
git commit -S -s -m "feat(apexes-core): add MarketEvent::MarkPrice(MarkPriceUpdate) variant"
```

---

### Task 2: Update every exhaustive `match` over `MarketEvent` across the workspace for the new variant

**Files:**
- Modify: `crates/apexes-backtest/src/market_data_source.rs`
- Modify: `crates/apexes-backtest/src/event_loop.rs`
- Modify: `crates/apexes-ems/src/in_memory/mod.rs`
- Modify: `crates/apexes-strategy/src/drift_dsl/convert.rs`
- Modify: `crates/apexes-storage/src/writer.rs`
- Modify: `crates/apexes-storage/src/reader.rs`
- Modify: `crates/apexes-market-data/tests/live_binance.rs`

**Interfaces:**
- Consumes: `apexes_core::MarketEvent::MarkPrice` (Task 1).
- Produces: no new public interfaces -- this task only makes the workspace compile again against the widened enum.

Before starting, re-run this exact search to confirm the current call-site list (the plan's authors ran it during drafting; re-confirm it hasn't drifted since):

```bash
grep -rn "MarketEvent::Tick(tick) => tick.timestamp" crates/
```

(This exact line appears in every `event_timestamp` helper across the workspace and is the fastest way to find all of them.)

- [ ] **Step 1: Fix `apexes-backtest/src/market_data_source.rs`'s `event_timestamp` helper**

```rust
// crates/apexes-backtest/src/market_data_source.rs -- change event_timestamp:
fn event_timestamp(event: &MarketEvent) -> chrono::DateTime<chrono::Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
        MarketEvent::MarkPrice(mark_price) => mark_price.timestamp,
    }
}
```

Also extend this file's `EventType`-driven `StorageMarketDataSource::events()` loop to also request `EventType::MarkPrice` (this will not compile until Task 4 adds that `EventType` variant -- come back to this specific edit when doing Task 4's own work; for THIS task, only fix `event_timestamp` so this file compiles standalone again):

```rust
// (Leave the `for event_type in [...]` array in `events()` untouched in
// this task -- Task 4 adds `EventType::MarkPrice` to it.)
```

- [ ] **Step 2: Fix `apexes-backtest/src/event_loop.rs`'s `event_timestamp` helper**

```rust
// crates/apexes-backtest/src/event_loop.rs -- change event_timestamp (near the bottom of the file):
fn event_timestamp(event: &MarketEvent) -> chrono::DateTime<chrono::Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
        MarketEvent::MarkPrice(mark_price) => mark_price.timestamp,
    }
}
```

The OTHER `match &event { MarketEvent::Depth(..) => ..., MarketEvent::Trade(..) => ..., _ => {} }` block earlier in this same file (the one driving `MatchingEngine::on_depth_update`/`on_trade_event`) already has a catch-all `_ => {}` arm -- no change needed there; `MarketEvent::MarkPrice` falls into that catch-all automatically (matches this task's design intent: a `MarkPrice` event currently has no direct matching-engine/ledger effect, exactly like `Tick`/`Bar` today).

- [ ] **Step 3: Fix `apexes-ems/src/in_memory/mod.rs`'s `event_timestamp` helper**

```rust
// crates/apexes-ems/src/in_memory/mod.rs -- change event_timestamp:
fn event_timestamp(event: &MarketEvent) -> DateTime<Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
        MarketEvent::MarkPrice(mark_price) => mark_price.timestamp,
    }
}
```

- [ ] **Step 4: Fix `apexes-strategy/src/drift_dsl/convert.rs`'s `market_event_to_drift_value`**

This function converts every `MarketEvent` variant into a `DriftValue::Object` for Drift Script strategies. Add a new match arm following the exact same field-naming convention as the existing `MarketEvent::Trade` arm:

```rust
// crates/apexes-strategy/src/drift_dsl/convert.rs -- add a new arm to
// market_event_to_drift_value's match, after the existing
// MarketEvent::Trade(...) arm:
        MarketEvent::MarkPrice(MarkPriceUpdate {
            symbol,
            venue,
            mark_price,
            timestamp,
        }) => {
            obj.insert("type".to_string(), string_value("mark_price"));
            obj.insert("symbol".to_string(), string_value(symbol.clone()));
            obj.insert("venue".to_string(), string_value(venue_to_str(*venue)));
            obj.insert("mark_price".to_string(), decimal_to_number(*mark_price));
            obj.insert(
                "timestamp".to_string(),
                string_value(timestamp.to_rfc3339()),
            );
        }
```

Add `MarkPriceUpdate` to this file's existing `use apexes_core::{...}` import list (check the current import line via `file_read` before editing -- it already imports `Tick`, `DepthUpdate`, `Bar`, `TradeEvent` by destructured-name for the exact same pattern).

- [ ] **Step 5: Fix `apexes-storage/src/writer.rs`'s `variant_of` and `BatchVariant`**

This step ONLY makes the file compile against the widened enum -- it does NOT yet add real mark-price writing support (that is Task 5's job, which depends on Task 3's `MarkPriceRow` existing first). Add a placeholder-safe arm that is never actually reached by `write_batch`'s current callers, since `write_batch` requires a homogeneous slice and no caller yet produces one of `MarkPrice`s:

```rust
// crates/apexes-storage/src/writer.rs -- add to BatchVariant and variant_of ONLY:
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchVariant {
    Trade,
    Tick,
    Depth,
    Bar,
    MarkPrice,
}

fn variant_of(event: &MarketEvent) -> BatchVariant {
    match event {
        MarketEvent::Trade(_) => BatchVariant::Trade,
        MarketEvent::Tick(_) => BatchVariant::Tick,
        MarketEvent::Depth(_) => BatchVariant::Depth,
        MarketEvent::Bar(_) => BatchVariant::Bar,
        MarketEvent::MarkPrice(_) => BatchVariant::MarkPrice,
    }
}
```

`write_batch`'s own `match variant { ... }` dispatch (`BatchVariant::Trade => self.write_trades(events).await`, etc.) will now fail to compile with a non-exhaustive-match error -- add a temporary arm that returns a clear "not yet implemented" error rather than silently mis-writing data, to be replaced by Task 5's real `write_mark_prices`:

```rust
// crates/apexes-storage/src/writer.rs -- write_batch's match, add:
            BatchVariant::MarkPrice => Err(StorageError::QueryFailed {
                reason: "MarkPrice writing not yet implemented (see Task 5)".to_string(),
            }),
```

- [ ] **Step 6: Fix `apexes-storage/src/reader.rs`'s `EventType` enum**

Same "compile now, implement for real in Task 4" split as Step 5. Add the enum variant only:

```rust
// crates/apexes-storage/src/reader.rs -- add to EventType:
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Trade,
    Tick,
    Depth,
    Bar,
    MarkPrice,
}
```

`read_batch`'s `match event_type { ... }` dispatch will now fail to compile -- add a temporary arm:

```rust
// crates/apexes-storage/src/reader.rs -- read_batch's match, add:
            EventType::MarkPrice => Err(StorageError::QueryFailed {
                reason: "MarkPrice reading not yet implemented (see Task 4)".to_string(),
            }),
```

- [ ] **Step 7: Fix `apexes-market-data/tests/live_binance.rs`'s exhaustive match**

```rust
// crates/apexes-market-data/tests/live_binance.rs -- change the match arm list:
            Ok(Some(event)) => match event {
                MarketEvent::Trade(_) => saw_trade = true,
                MarketEvent::Tick(_) => saw_tick = true,
                MarketEvent::Depth(_) => saw_depth = true,
                MarketEvent::Bar(_) => saw_bar = true,
                MarketEvent::MarkPrice(_) => {} // Spot-only test; MarkPrice never emitted for Spot instruments.
            },
```

- [ ] **Step 8: Run the full workspace build and tests**

Run: `cargo build --workspace`
Expected: 0 errors (the whole workspace compiles again).

Run: `cargo test --workspace -- --test-threads=1`
Expected: 0 failures (same failures as documented in `.superpowers/sdd/progress.md` for pre-existing environmental-limitation `#[ignore]`d tests are fine; nothing NEW should fail).

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 9: Commit**

```bash
git add crates/apexes-backtest/src/market_data_source.rs crates/apexes-backtest/src/event_loop.rs crates/apexes-ems/src/in_memory/mod.rs crates/apexes-strategy/src/drift_dsl/convert.rs crates/apexes-storage/src/writer.rs crates/apexes-storage/src/reader.rs crates/apexes-market-data/tests/live_binance.rs
git commit -S -s -m "fix: handle MarketEvent::MarkPrice in every exhaustive match across the workspace"
```

---

## Phase 2: `apexes-market-data` -- Perpetual ingestion + `mark_price_task`

### Task 3: Parameterize `trade_task`/`depth_task`/`ticker_task` over exchange (Spot vs. Perpetual)

**Files:**
- Modify: `crates/apexes-market-data/src/trade_task.rs`
- Modify: `crates/apexes-market-data/src/depth_task.rs`
- Modify: `crates/apexes-market-data/src/ticker_task.rs`

**Interfaces:**
- Consumes: `barter_data::exchange::binance::{spot::BinanceSpot, futures::BinanceFuturesUsd}` (both already implement the `StreamSelector` traits these tasks' `Streams::<Kind>::builder()` calls require -- confirmed during this plan's drafting via `barter-data` 0.11's own source).
- Produces: `crate::trade_task::run(symbols: Vec<String>, exchange: BinanceExchangeKind, tx: UnboundedSender<MarketEvent>)`, `crate::depth_task::run(symbols: Vec<String>, exchange: BinanceExchangeKind, tx: UnboundedSender<MarketEvent>)`, `crate::ticker_task::run(symbols: Vec<String>, exchange: BinanceExchangeKind, tx: UnboundedSender<MarketEvent>)` -- all three gain a new `exchange` parameter; a new `crate::provider::BinanceExchangeKind` enum (defined in this task, in `provider.rs`, since it's shared by all three tasks and by `provider.rs` itself).

**Design decision, settled here:** rather than making each task generic over a `barter-data` exchange type parameter (which would require threading complex trait bounds through every task function's signature), each task takes a simple `BinanceExchangeKind` enum parameter and internally branches on it when constructing its subscription tuples -- keeping the public function signatures simple or (`Vec<String>, BinanceExchangeKind, UnboundedSender<MarketEvent>`) while the internal `Streams::<Kind>::builder()` call still needs concrete types on both sides of the branch. Since `Streams::<PublicTrades>::builder().subscribe(...)` requires ALL subscriptions in one `subscribe()` call to share the same `Exchange` type parameter, a single `subscribe()` call cannot mix `BinanceSpot` and `BinanceFuturesUsd` subscriptions -- each task's `run` function is called ONCE per exchange kind (Spot instruments in one call, Perpetual instruments in another), each producing its own `Streams` instance internally, then `select_all()`-merged. This matches `provider.rs`'s existing per-task-per-`subscribe()`-call structure (Step 3 of this task shows exactly how `provider.rs` calls each task twice).

- [ ] **Step 1: Define `BinanceExchangeKind` in `provider.rs`**

```rust
// crates/apexes-market-data/src/provider.rs -- add near the top, before filter_binance_spot_instruments:

/// Which Binance market this instrument's data comes from -- Spot public
/// market data and USDS-M Perpetual Futures public market data are
/// separate `barter-data` exchange types (`BinanceSpot` vs.
/// `BinanceFuturesUsd`) with separate WebSocket hosts, so every
/// ingestion task takes this enum to know which one to use per
/// subscription batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceExchangeKind {
    Spot,
    Perpetual,
}
```

- [ ] **Step 2: Parameterize `trade_task::run`**

```rust
// crates/apexes-market-data/src/trade_task.rs -- change imports and run's signature:
use crate::bar_aggregator::BarAggregator;
use crate::provider::BinanceExchangeKind;
use crate::symbol::{split_symbol, symbol_of};
use apexes_core::{MarketEvent, Side as ApexesSide, TradeEvent, Venue};
use barter_data::{
    exchange::binance::{futures::BinanceFuturesUsd, spot::BinanceSpot},
    streams::Streams,
    subscription::trade::PublicTrades,
};
use barter_instrument::{
    Side as BarterSide, instrument::market_data::kind::MarketDataInstrumentKind,
};
use futures::StreamExt;
use rust_decimal::Decimal;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

// (convert_side/f64_to_decimal unchanged)

/// Subscribes to Binance public trades for `symbols` on `exchange`
/// (`BinanceExchangeKind::Spot` or `::Perpetual`), converts each trade
/// into `MarketEvent::Trade`, and drives a per-symbol `BarAggregator`
/// to also emit `MarketEvent::Bar` when intervals close. Runs until the
/// underlying `barter-data` stream ends (which, given `barter-data`'s
/// internal auto-reconnect, is effectively "forever" under normal
/// operation).
pub async fn run(symbols: Vec<String>, exchange: BinanceExchangeKind, tx: UnboundedSender<MarketEvent>) {
    let kind = match exchange {
        BinanceExchangeKind::Spot => MarketDataInstrumentKind::Spot,
        BinanceExchangeKind::Perpetual => MarketDataInstrumentKind::Perpetual,
    };

    let mut aggregators: HashMap<String, BarAggregator> = symbols
        .iter()
        .map(|symbol| {
            (
                symbol.clone(),
                BarAggregator::new(symbol.clone(), Venue::Binance),
            )
        })
        .collect();

    let mut merged = match exchange {
        BinanceExchangeKind::Spot => {
            let subscriptions: Vec<_> = build_subscriptions(&symbols, kind, |base, quote, k| {
                (BinanceSpot::default(), base, quote, k, PublicTrades)
            });
            if subscriptions.is_empty() {
                tracing::warn!(?symbols, "trade_task: no valid Binance spot subscriptions derived from symbols");
                return;
            }
            match Streams::<PublicTrades>::builder().subscribe(subscriptions).init().await {
                Ok(streams) => streams.select_all(),
                Err(error) => {
                    tracing::warn!(?error, "trade_task: failed to initialize barter-data PublicTrades stream (spot)");
                    return;
                }
            }
        }
        BinanceExchangeKind::Perpetual => {
            let subscriptions: Vec<_> = build_subscriptions(&symbols, kind, |base, quote, k| {
                (BinanceFuturesUsd::default(), base, quote, k, PublicTrades)
            });
            if subscriptions.is_empty() {
                tracing::warn!(?symbols, "trade_task: no valid Binance perpetual subscriptions derived from symbols");
                return;
            }
            match Streams::<PublicTrades>::builder().subscribe(subscriptions).init().await {
                Ok(streams) => streams.select_all(),
                Err(error) => {
                    tracing::warn!(?error, "trade_task: failed to initialize barter-data PublicTrades stream (perpetual)");
                    return;
                }
            }
        }
    };

    while let Some(event) = merged.next().await {
        // ... (loop body UNCHANGED from the existing implementation)
    }
}

/// Builds one subscription tuple per valid symbol via `make_tuple`,
/// skipping (with a warning) any symbol whose quote currency isn't
/// recognized by `split_symbol`. Shared helper so the Spot/Perpetual
/// branches above don't duplicate this filter-and-log logic.
fn build_subscriptions<T>(
    symbols: &[String],
    kind: MarketDataInstrumentKind,
    make_tuple: impl Fn(String, String, MarketDataInstrumentKind) -> T,
) -> Vec<T> {
    symbols
        .iter()
        .filter_map(|symbol| {
            let (base, quote) = match split_symbol(symbol) {
                Some(pair) => pair,
                None => {
                    tracing::warn!(%symbol, "trade_task: skipping symbol with unrecognized quote currency");
                    return None;
                }
            };
            Some(make_tuple(base, quote, kind))
        })
        .collect()
}
```

**Important type-inference note for the implementer:** `Streams::<PublicTrades>::builder().subscribe(subscriptions)` requires all elements of `subscriptions` to share one concrete `Exchange` type at compile time -- the `match exchange { ... }` structure above achieves this by keeping each branch's `subscriptions: Vec<_>` monomorphized to a SINGLE tuple type (`(BinanceSpot, ..., PublicTrades)` in one arm, `(BinanceFuturesUsd, ..., PublicTrades)` in the other), never mixed. If the actual `barter-data` API surface makes `build_subscriptions`'s generic-over-tuple-type signature awkward to compile (e.g. the tuple element ordering/count doesn't unify across both closures the same way), it is acceptable to inline the tuple construction directly in each `match` arm instead of factoring out `build_subscriptions` -- verify by running `cargo check -p apexes-market-data` early and adjust the exact factoring if needed; the BEHAVIORAL requirement (one `Streams` init per exchange kind, correctly filtered/logged) is what must hold, not this exact code shape.

- [ ] **Step 3: Parameterize `depth_task::run`**

Apply the identical pattern (branch on `exchange`, build `BinanceSpot` or `BinanceFuturesUsd` subscriptions, `select_all()`-merge) to `depth_task.rs`, using `OrderBooksL2` instead of `PublicTrades`:

```rust
// crates/apexes-market-data/src/depth_task.rs -- change imports and run's signature:
use crate::depth_converter;
use crate::provider::BinanceExchangeKind;
use crate::symbol::{split_symbol, symbol_of};
use apexes_core::{MarketEvent, Venue};
use barter_data::{
    exchange::binance::{futures::BinanceFuturesUsd, spot::BinanceSpot},
    streams::Streams,
    subscription::book::OrderBooksL2,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures::StreamExt;
use tokio::sync::mpsc::UnboundedSender;

pub async fn run(symbols: Vec<String>, exchange: BinanceExchangeKind, tx: UnboundedSender<MarketEvent>) {
    // Same Spot/Perpetual match-and-init structure as trade_task::run,
    // using OrderBooksL2 instead of PublicTrades. Loop body (converting
    // each event via depth_converter::convert and sending) is UNCHANGED.
}
```

- [ ] **Step 4: Parameterize `ticker_task::run`**

Same pattern again, using `OrderBooksL1`:

```rust
// crates/apexes-market-data/src/ticker_task.rs -- change imports and run's signature:
use crate::provider::BinanceExchangeKind;
use crate::symbol::{split_symbol, symbol_of};
use apexes_core::{MarketEvent, Tick, Venue};
use barter_data::{
    exchange::binance::{futures::BinanceFuturesUsd, spot::BinanceSpot},
    streams::Streams,
    subscription::book::OrderBooksL1,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures::StreamExt;
use tokio::sync::mpsc::UnboundedSender;

pub async fn run(symbols: Vec<String>, exchange: BinanceExchangeKind, tx: UnboundedSender<MarketEvent>) {
    // Same Spot/Perpetual match-and-init structure, using OrderBooksL1.
    // Loop body (converting each event, skipping empty-book-side
    // updates) is UNCHANGED.
}
```

- [ ] **Step 5: Update this crate's existing unit tests to pass the new parameter**

`cargo check -p apexes-market-data --tests` will show every existing call site of `trade_task::run`/`depth_task::run`/`ticker_task::run` that needs a `BinanceExchangeKind::Spot` argument added (Task 4 will be the one to add REAL Perpetual call sites in `provider.rs`; for this task, existing tests/call sites all keep their current Spot-only behavior, just passing `BinanceExchangeKind::Spot` explicitly now). `provider.rs`'s existing `subscribe()` body (Step 1's `spawn_task_thread("trade_task", ...)` etc.) also needs `BinanceExchangeKind::Spot` added to each call for now -- Task 4 changes this further.

- [ ] **Step 6: Run this crate's tests, clippy, and fmt**

Run: `cargo test -p apexes-market-data`
Expected: PASS, 0 failures (behavior unchanged for Spot, just an added parameter).

Run: `cargo clippy -p apexes-market-data --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 7: Commit**

```bash
git add crates/apexes-market-data/src/trade_task.rs crates/apexes-market-data/src/depth_task.rs crates/apexes-market-data/src/ticker_task.rs crates/apexes-market-data/src/provider.rs
git commit -S -s -m "feat(apexes-market-data): parameterize trade/depth/ticker tasks over Spot vs Perpetual exchange"
```

---

### Task 4: `mark_price_task.rs` (new) + `BinanceMarketDataProvider::subscribe` Perpetual support

**Files:**
- Create: `crates/apexes-market-data/src/mark_price_task.rs`
- Modify: `crates/apexes-market-data/src/lib.rs`
- Modify: `crates/apexes-market-data/src/provider.rs`

**Interfaces:**
- Consumes: `apexes_core::MarketEvent::MarkPrice`/`MarkPriceUpdate` (Task 1), `crate::provider::BinanceExchangeKind` (Task 3), the parameterized `trade_task`/`depth_task`/`ticker_task::run` (Task 3).
- Produces: `crate::mark_price_task::run(symbols: Vec<String>, tx: UnboundedSender<MarketEvent>)` (Perpetual only -- no `BinanceExchangeKind` parameter needed since mark price is meaningless for Spot); `BinanceMarketDataProvider::subscribe` now accepts `Venue::Binance` + `AssetClass::Perpetual` instruments in addition to `AssetClass::Spot`.

**Design decision, settled here:** `mark_price_task` is a hand-rolled raw WebSocket client (via `tokio-tungstenite`, already a workspace dependency used identically by `apexes_gateway_binance::user_data_stream`), NOT a `barter-data`-based task like the other three -- `barter-data` 0.11 has no mark-price subscription kind at all (confirmed via inspecting its `subscription::mod.rs`'s `SubKind` enum during this plan's drafting). Binance's mark-price stream is `wss://fstream.binance.com/stream?streams=<symbol_lower>@markPrice@1s` (combined-stream syntax, `@1s` for 1-second update speed rather than the default 3s) for a single symbol, or the multi-stream combined endpoint for multiple symbols (`streams=btcusdt@markPrice@1s/ethusdt@markPrice@1s`).

- [ ] **Step 1: Write `mark_price_task.rs`**

```rust
// crates/apexes-market-data/src/mark_price_task.rs (new file)
//! [`run`]: subscribes to Binance's public `<symbol>@markPrice@1s`
//! combined WebSocket stream for `symbols` (USDS-M Perpetual only),
//! converting each message into `MarketEvent::MarkPrice`. Hand-rolled
//! raw WebSocket (via `tokio-tungstenite`, mirroring
//! `apexes_gateway_binance::user_data_stream`'s existing pattern) --
//! `barter-data` 0.11 has no mark-price subscription kind at all. See
//! `docs/superpowers/specs/2026-08-02-apexes-mark-price-pipeline-design.md`
//! §3.2 for the raw message shape.

use apexes_core::{MarketEvent, MarkPriceUpdate, Venue};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::Message;

const WS_BASE_URL: &str = "wss://fstream.binance.com/stream";
/// Delay before attempting to reconnect after the WebSocket connection
/// drops -- avoids a tight reconnect loop hammering Binance during an
/// outage. `barter-data`'s tasks get equivalent backoff for free from
/// the library; this hand-rolled task must implement its own.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// Runs until `symbols` is empty (returns immediately, matching the
/// other tasks' "empty subscription list -> warn and return" behavior)
/// or forever otherwise, reconnecting automatically on any WebSocket
/// error or unexpected close.
pub async fn run(symbols: Vec<String>, tx: UnboundedSender<MarketEvent>) {
    if symbols.is_empty() {
        tracing::warn!("mark_price_task: no symbols provided, nothing to subscribe to");
        return;
    }

    let stream_names: Vec<String> = symbols
        .iter()
        .map(|s| format!("{}@markPrice@1s", s.to_lowercase()))
        .collect();
    let url = format!("{WS_BASE_URL}?streams={}", stream_names.join("/"));

    loop {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((socket, _response)) => {
                tracing::info!(?symbols, "mark_price_task: connected");
                let (_write, mut read) = socket.split();
                loop {
                    match read.next().await {
                        Some(Ok(Message::Text(text))) => {
                            if let Some(event) = parse_mark_price_message(&text) {
                                if tx.send(event).is_err() {
                                    tracing::warn!("mark_price_task: output channel closed, stopping");
                                    return;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            tracing::warn!(?error, "mark_price_task: WebSocket error, reconnecting");
                            break;
                        }
                        None => {
                            tracing::warn!("mark_price_task: WebSocket stream ended, reconnecting");
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(?error, "mark_price_task: failed to connect, retrying");
            }
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// Parses one raw combined-stream text message
/// (`{"stream": "<symbol>@markPrice@1s", "data": {"e": "markPriceUpdate",
/// "E": ..., "s": "BTCUSDT", "p": "...", ...}}`) into a
/// `MarketEvent::MarkPrice`, or `None` if the message isn't a
/// `markPriceUpdate` event or is malformed. Errors are logged, not
/// panicked on -- a single malformed message must not crash ingestion.
fn parse_mark_price_message(raw: &str) -> Option<MarketEvent> {
    let outer: Value = serde_json::from_str(raw).ok()?;
    let data = outer.get("data")?;

    if data.get("e")?.as_str()? != "markPriceUpdate" {
        return None;
    }

    let symbol = data.get("s")?.as_str()?.to_string();
    let mark_price_str = data.get("p")?.as_str()?;
    let mark_price = Decimal::from_str(mark_price_str).ok()?;
    let event_time_ms = data.get("E")?.as_i64()?;
    let timestamp = DateTime::<Utc>::from_timestamp_millis(event_time_ms)?;

    Some(MarketEvent::MarkPrice(MarkPriceUpdate {
        symbol,
        venue: Venue::Binance,
        mark_price,
        timestamp,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_message() -> String {
        r#"{"stream":"btcusdt@markPrice@1s","data":{"e":"markPriceUpdate","E":1234567890000,"s":"BTCUSDT","p":"65012.34000000","P":"65010.00000000","r":"0.00010000","T":1234598400000}}"#.to_string()
    }

    #[test]
    fn parses_a_valid_mark_price_update() {
        let event = parse_mark_price_message(&sample_message()).expect("should parse");
        let MarketEvent::MarkPrice(update) = event else {
            panic!("expected MarketEvent::MarkPrice");
        };
        assert_eq!(update.symbol, "BTCUSDT");
        assert_eq!(update.venue, Venue::Binance);
        assert_eq!(update.mark_price, Decimal::from_str("65012.34000000").unwrap());
        assert_eq!(
            update.timestamp,
            DateTime::<Utc>::from_timestamp_millis(1234567890000).unwrap()
        );
    }

    #[test]
    fn returns_none_for_a_non_mark_price_event_type() {
        let raw = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1234567890000,"s":"BTCUSDT"}}"#;
        assert!(parse_mark_price_message(raw).is_none());
    }

    #[test]
    fn returns_none_for_malformed_json() {
        assert!(parse_mark_price_message("not json").is_none());
    }

    #[test]
    fn returns_none_when_price_field_is_not_a_valid_decimal() {
        let raw = r#"{"stream":"btcusdt@markPrice@1s","data":{"e":"markPriceUpdate","E":1234567890000,"s":"BTCUSDT","p":"not-a-number"}}"#;
        assert!(parse_mark_price_message(raw).is_none());
    }
}
```

- [ ] **Step 2: Register the new module**

```rust
// crates/apexes-market-data/src/lib.rs -- add:
pub mod mark_price_task;
```

- [ ] **Step 3: Extend `filter_binance_spot_instruments` to accept Perpetual, and wire all four tasks into `subscribe`**

```rust
// crates/apexes-market-data/src/provider.rs -- rename and extend the filter function:

/// Filters `instruments` down to those tradable via this provider:
/// `Venue::Binance` + (`AssetClass::Spot` or `AssetClass::Perpetual`).
/// Non-matching instruments are skipped with a warning rather than
/// causing the whole call to fail. Returns `(spot_instruments,
/// perpetual_instruments)` so `subscribe` can route each group to its
/// own `BinanceExchangeKind`.
fn filter_binance_instruments(instruments: &[Instrument]) -> (Vec<&Instrument>, Vec<&Instrument>) {
    let mut spot = Vec::new();
    let mut perpetual = Vec::new();
    for instrument in instruments {
        if instrument.venue != Venue::Binance {
            tracing::warn!(
                symbol = %instrument.symbol,
                venue = ?instrument.venue,
                "skipping instrument: BinanceMarketDataProvider only supports Venue::Binance"
            );
            continue;
        }
        match instrument.asset_class {
            AssetClass::Spot => spot.push(instrument),
            AssetClass::Perpetual => perpetual.push(instrument),
            other => {
                tracing::warn!(
                    symbol = %instrument.symbol,
                    asset_class = ?other,
                    "skipping instrument: BinanceMarketDataProvider only supports AssetClass::Spot and AssetClass::Perpetual"
                );
            }
        }
    }
    (spot, perpetual)
}
```

```rust
// crates/apexes-market-data/src/provider.rs -- change subscribe's body:
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let (spot, perpetual) = filter_binance_instruments(instruments);

        if spot.is_empty() && perpetual.is_empty() {
            return Err(ApexesError::SubscriptionFailed {
                symbol: instruments
                    .first()
                    .map(|i| i.symbol.clone())
                    .unwrap_or_default(),
                venue: Venue::Binance,
                reason: "no instruments matched Venue::Binance + (AssetClass::Spot or AssetClass::Perpetual) after filtering"
                    .to_string(),
                source_detail: None,
            });
        }

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        if !spot.is_empty() {
            let symbols: Vec<String> = spot.iter().map(|i| i.symbol.clone()).collect();
            spawn_task_thread("trade_task_spot", {
                let symbols = symbols.clone();
                let tx = tx.clone();
                move || crate::trade_task::run(symbols, BinanceExchangeKind::Spot, tx)
            });
            spawn_task_thread("ticker_task_spot", {
                let symbols = symbols.clone();
                let tx = tx.clone();
                move || crate::ticker_task::run(symbols, BinanceExchangeKind::Spot, tx)
            });
            spawn_task_thread("depth_task_spot", {
                let tx = tx.clone();
                move || crate::depth_task::run(symbols, BinanceExchangeKind::Spot, tx)
            });
        }

        if !perpetual.is_empty() {
            let symbols: Vec<String> = perpetual.iter().map(|i| i.symbol.clone()).collect();
            spawn_task_thread("trade_task_perpetual", {
                let symbols = symbols.clone();
                let tx = tx.clone();
                move || crate::trade_task::run(symbols, BinanceExchangeKind::Perpetual, tx)
            });
            spawn_task_thread("ticker_task_perpetual", {
                let symbols = symbols.clone();
                let tx = tx.clone();
                move || crate::ticker_task::run(symbols, BinanceExchangeKind::Perpetual, tx)
            });
            spawn_task_thread("depth_task_perpetual", {
                let symbols = symbols.clone();
                let tx = tx.clone();
                move || crate::depth_task::run(symbols, BinanceExchangeKind::Perpetual, tx)
            });
            spawn_task_thread("mark_price_task", {
                let tx = tx.clone();
                move || crate::mark_price_task::run(symbols, tx)
            });
        }

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }
```

- [ ] **Step 4: Update this crate's existing unit tests for the renamed/widened filter function**

`filters_out_non_binance_venue`/`filters_out_non_spot_asset_class`/`keeps_binance_spot_instruments` currently call `filter_binance_spot_instruments` and check a single `Vec<&Instrument>` return -- update them for `filter_binance_instruments`'s new `(Vec<&Instrument>, Vec<&Instrument>)` return shape (e.g. `filters_out_non_spot_asset_class` becomes `keeps_binance_perpetual_instruments_separately_from_spot`, asserting the perpetual instrument lands in the second tuple element, not that it's filtered out entirely -- it's no longer filtered out, since Perpetual is now supported). Add a new test:

```rust
    #[test]
    fn perpetual_instruments_are_kept_separately_from_spot() {
        let instruments = vec![
            binance_spot_instrument("BTCUSDT"),
            binance_perpetual_instrument(),
        ];
        let (spot, perpetual) = filter_binance_instruments(&instruments);
        assert_eq!(spot.len(), 1);
        assert_eq!(spot[0].symbol, "BTCUSDT");
        assert_eq!(perpetual.len(), 1);
        assert_eq!(perpetual[0].symbol, "BTCUSDT");
    }
```

- [ ] **Step 5: Run this crate's tests, clippy, and fmt**

Run: `cargo test -p apexes-market-data`
Expected: PASS, including all of `mark_price_task`'s new unit tests and this task's updated `provider.rs` tests.

Run: `cargo clippy -p apexes-market-data --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 6: Add a live-network integration test for `mark_price_task`**

```rust
// crates/apexes-market-data/tests/live_binance.rs -- add:
#[tokio::test]
#[ignore]
async fn subscribing_to_a_perpetual_instrument_produces_mark_price_events() {
    let provider = BinanceMarketDataProvider::new();
    let instrument = Instrument::new(
        "BTCUSDT",
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Perpetual,
        Some("BTC".to_string()),
        Some("USDT".to_string()),
        dec!(0.01),
        dec!(0.001),
        dec!(1),
    )
    .unwrap();

    let mut stream = provider
        .subscribe(&[instrument])
        .await
        .expect("subscribe should succeed for a valid Binance Perpetual instrument");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut saw_mark_price = false;

    while tokio::time::Instant::now() < deadline && !saw_mark_price {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(MarketEvent::MarkPrice(_))) => saw_mark_price = true,
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_timeout) => break,
        }
    }

    assert!(saw_mark_price, "expected at least one MarketEvent::MarkPrice within 30s");
}
```

Run: `cargo test -p apexes-market-data -- --ignored subscribing_to_a_perpetual_instrument_produces_mark_price_events`
Expected: PASS (requires live network access to Binance).

- [ ] **Step 7: Commit**

```bash
git add crates/apexes-market-data/src/mark_price_task.rs crates/apexes-market-data/src/lib.rs crates/apexes-market-data/src/provider.rs crates/apexes-market-data/tests/live_binance.rs
git commit -S -s -m "feat(apexes-market-data): mark_price_task + BinanceMarketDataProvider Perpetual support"
```

---

## Phase 3: `apexes-storage` -- `mark_prices` table

### Task 5: `MarkPriceRow` + `mark_prices` table, `StorageWriter`/`StorageReader` support

**Files:**
- Modify: `crates/apexes-storage/src/schema.rs`
- Modify: `crates/apexes-storage/src/writer.rs`
- Modify: `crates/apexes-storage/src/reader.rs`
- Modify: `crates/apexes-storage/tests/live_clickhouse.rs`

**Interfaces:**
- Consumes: `apexes_core::MarketEvent::MarkPrice`/`MarkPriceUpdate` (Task 1); this task's own placeholder `BatchVariant::MarkPrice`/`EventType::MarkPrice` arms added in Task 2 Steps 5-6 (replaced here with real implementations).
- Produces: `apexes_storage::schema::{MarK_PRICES_TABLE_DDL, MarkPriceRow}` (following the exact naming convention of the other DDL constants -- `MARK_PRICES_TABLE_DDL`), `StorageWriter::new` creates the `mark_prices` table, `StorageWriter::write_batch` accepts homogeneous `MarketEvent::MarkPrice` batches, `StorageReader::read_batch(..., EventType::MarkPrice)` reads them back.

- [ ] **Step 1: Add `MARK_PRICES_TABLE_DDL` and `MarkPriceRow` to `schema.rs`**

```rust
// crates/apexes-storage/src/schema.rs -- add imports:
use apexes_core::{Bar, BarInterval, DepthUpdate, MarkPriceUpdate, PriceLevel, Side, Tick, TradeEvent, Venue};
```

```rust
// crates/apexes-storage/src/schema.rs -- add DDL constant, following
// the exact existing Trade/Tick/Depth/Bar pattern:
pub const MARK_PRICES_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS mark_prices
(
    symbol      String,
    venue       String,
    mark_price  Decimal(38, 10),
    timestamp   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (venue, symbol, timestamp)
";

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MarkPriceRow {
    pub symbol: String,
    pub venue: String,
    pub mark_price: CkDecimal,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl TryFrom<&MarkPriceUpdate> for MarkPriceRow {
    type Error = StorageError;

    fn try_from(update: &MarkPriceUpdate) -> Result<Self, StorageError> {
        Ok(MarkPriceRow {
            symbol: update.symbol.clone(),
            venue: format!("{:?}", update.venue),
            mark_price: decimal_to_fixed(update.mark_price)?,
            timestamp: update.timestamp,
        })
    }
}

impl TryFrom<MarkPriceRow> for MarkPriceUpdate {
    type Error = StorageError;

    fn try_from(row: MarkPriceRow) -> Result<Self, StorageError> {
        Ok(MarkPriceUpdate {
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            mark_price: fixed_to_decimal(row.mark_price),
            timestamp: row.timestamp,
        })
    }
}
```

- [ ] **Step 2: Write unit tests for `MarkPriceRow`'s conversions**

Add to `schema.rs`'s existing `#[cfg(test)] mod tests` (check its exact location and existing test style via `file_read` before adding -- follow the same round-trip-assertion pattern used for `TradeRow`/`TickRow`):

```rust
    #[test]
    fn mark_price_row_round_trips_through_try_from() {
        let update = MarkPriceUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            mark_price: dec!(65012.34),
            timestamp: Utc::now(),
        };
        let row = MarkPriceRow::try_from(&update).unwrap();
        let round_tripped = MarkPriceUpdate::try_from(row).unwrap();
        assert_eq!(update, round_tripped);
    }
```

- [ ] **Step 3: Wire `MARK_PRICES_TABLE_DDL` into `StorageWriter::new`**

```rust
// crates/apexes-storage/src/writer.rs -- add to the imports:
use crate::schema::{
    BARS_TABLE_DDL, BarRow, DEPTH_UPDATES_TABLE_DDL, DepthRow, MARK_PRICES_TABLE_DDL, MarkPriceRow,
    TICKS_TABLE_DDL, TRADES_TABLE_DDL, TickRow, TradeRow,
};
```

```rust
// crates/apexes-storage/src/writer.rs -- add to StorageWriter::new, after the existing BARS_TABLE_DDL call:
        client
            .query(MARK_PRICES_TABLE_DDL)
            .execute()
            .await
            .map_err(|error| StorageError::ConnectionFailed {
                reason: format!("failed to create mark_prices table: {error}"),
            })?;
```

- [ ] **Step 4: Implement real `write_mark_prices`, replacing Task 2's placeholder error arm**

```rust
// crates/apexes-storage/src/writer.rs -- add a new method, following write_bars exactly:
    async fn write_mark_prices(&self, events: &[MarketEvent]) -> Result<(), StorageError> {
        let mut insert = self.client.insert::<MarkPriceRow>("mark_prices").await?;
        for event in events {
            let MarketEvent::MarkPrice(update) = event else {
                return Err(StorageError::MixedBatch);
            };
            insert.write(&MarkPriceRow::try_from(update)?).await?;
        }
        insert.end().await?;
        Ok(())
    }
```

```rust
// crates/apexes-storage/src/writer.rs -- replace write_batch's placeholder MarkPrice arm:
    pub async fn write_batch(&self, events: &[MarketEvent]) -> Result<(), StorageError> {
        let variant = classify_batch(events)?;

        match variant {
            BatchVariant::Trade => self.write_trades(events).await,
            BatchVariant::Tick => self.write_ticks(events).await,
            BatchVariant::Depth => self.write_depth(events).await,
            BatchVariant::Bar => self.write_bars(events).await,
            BatchVariant::MarkPrice => self.write_mark_prices(events).await,
        }
    }
```

Add a `sample_mark_price()` helper and a `classify_batch_accepts_homogeneous_mark_prices` test to this file's existing `#[cfg(test)] mod tests`, mirroring `sample_trade`/`sample_tick`'s exact pattern.

- [ ] **Step 5: Implement real `read_mark_prices`, replacing Task 2's placeholder error arm**

```rust
// crates/apexes-storage/src/reader.rs -- add to imports:
use crate::schema::{BarRow, DepthRow, MarkPriceRow, TickRow, TradeRow};
```

```rust
// crates/apexes-storage/src/reader.rs -- add a new method, following read_bars's pattern (timestamp column, not open_time):
    async fn read_mark_prices(
        &self,
        symbol: &str,
        venue: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM mark_prices WHERE venue = ? AND symbol = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp",
            )
            .bind(venue)
            .bind(symbol)
            .bind(start.format(CLICKHOUSE_DATETIME_FORMAT).to_string())
            .bind(end.format(CLICKHOUSE_DATETIME_FORMAT).to_string())
            .fetch_all::<MarkPriceRow>()
            .await?;

        rows.into_iter()
            .map(|row| Ok(MarketEvent::MarkPrice(row.try_into()?)))
            .collect()
    }
```

```rust
// crates/apexes-storage/src/reader.rs -- replace read_batch's placeholder MarkPrice arm:
    pub async fn read_batch(
        &self,
        symbol: &str,
        venue: Venue,
        date_range: Range<NaiveDate>,
        event_type: EventType,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let venue_str = format!("{venue:?}");
        let start = day_boundary(date_range.start)?;
        let end = day_boundary(date_range.end)?;

        match event_type {
            EventType::Trade => self.read_trades(symbol, &venue_str, start, end).await,
            EventType::Tick => self.read_ticks(symbol, &venue_str, start, end).await,
            EventType::Depth => self.read_depth(symbol, &venue_str, start, end).await,
            EventType::Bar => self.read_bars(symbol, &venue_str, start, end).await,
            EventType::MarkPrice => self.read_mark_prices(symbol, &venue_str, start, end).await,
        }
    }
```

- [ ] **Step 6: Run this crate's tests, clippy, and fmt**

Run: `cargo test -p apexes-storage`
Expected: PASS, including the new `MarkPriceRow` round-trip test and `write_batch`'s new `MarkPrice` classification test.

Run: `cargo clippy -p apexes-storage --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 7: Add a live-ClickHouse integration test**

```rust
// crates/apexes-storage/tests/live_clickhouse.rs -- add imports:
use apexes_core::MarkPriceUpdate;
```

```rust
// crates/apexes-storage/tests/live_clickhouse.rs -- add a new test,
// mirroring write_and_read_back_trades exactly:
#[tokio::test]
#[ignore]
async fn write_and_read_back_mark_prices() {
    let writer = StorageWriter::new(CLICKHOUSE_URL)
        .await
        .expect("StorageWriter::new should succeed with ClickHouse running locally");
    let reader = StorageReader::new(CLICKHOUSE_URL)
        .await
        .expect("StorageReader::new should succeed");

    let now = Utc::now();
    let symbol = unique_symbol("TESTMARKPRICE", now);
    let events = vec![MarketEvent::MarkPrice(MarkPriceUpdate {
        symbol: symbol.clone(),
        venue: Venue::Binance,
        mark_price: dec!(65012.34567),
        timestamp: now,
    })];

    writer
        .write_batch(&events)
        .await
        .expect("write_batch should succeed");

    let date = now.date_naive();
    let read_back = reader
        .read_batch(
            &symbol,
            Venue::Binance,
            date..(date.succ_opt().unwrap()),
            EventType::MarkPrice,
        )
        .await
        .expect("read_batch should succeed");

    assert_eq!(read_back.len(), 1);
    let MarketEvent::MarkPrice(update) = &read_back[0] else {
        panic!("expected MarketEvent::MarkPrice");
    };
    assert_eq!(update.symbol, symbol);
    assert_eq!(update.mark_price, dec!(65012.34567));
}
```

Run: `cargo test -p apexes-storage -- --ignored write_and_read_back_mark_prices`
Expected: PASS (requires `docker compose up -d` for a local ClickHouse instance).

- [ ] **Step 8: Commit**

```bash
git add crates/apexes-storage/src/schema.rs crates/apexes-storage/src/writer.rs crates/apexes-storage/src/reader.rs crates/apexes-storage/tests/live_clickhouse.rs Cargo.lock
git commit -S -s -m "feat(apexes-storage): MarkPriceRow + mark_prices table, StorageWriter/StorageReader support"
```

---

## Phase 4: General ingestion loop + `apexes-backtest` replay verification

### Task 6: `apexes_market_data::ingestion::run_ingestion_loop`

**Files:**
- Create: `crates/apexes-market-data/src/ingestion.rs`
- Modify: `crates/apexes-market-data/src/lib.rs`
- Modify: `crates/apexes-market-data/Cargo.toml`

**Interfaces:**
- Consumes: `apexes_core::{MarketDataProvider, MarketEvent, Instrument, ApexesError}`, `apexes_storage::StorageWriter` (Task 5).
- Produces: `apexes_market_data::ingestion::run_ingestion_loop(provider: &dyn MarketDataProvider, writer: &StorageWriter, instruments: &[Instrument], stop_signal: tokio::sync::oneshot::Receiver<()>) -> Result<(), ApexesError>` -- the project's first general-purpose live-market-data-to-ClickHouse ingestion path, usable for ANY `MarketEvent` variant any `MarketDataProvider` produces, not specific to mark price.

**Design decision, settled here:** per-variant buffering (5 buffers: Tick/Depth/Bar/Trade/MarkPrice), each flushed to `writer.write_batch` whenever it reaches `MAX_BATCH_SIZE` events OR `FLUSH_INTERVAL` has elapsed since that variant's own last flush, whichever comes first. A `tokio::time::interval` ticks independently of the event stream (checked via `tokio::select!`) so a low-volume variant's buffer doesn't wait indefinitely for enough events to reach `MAX_BATCH_SIZE`. Errors from `writer.write_batch` are logged (`tracing::error!`) and the buffer is dropped (not retried indefinitely, not propagated as a fatal error) -- one bad batch must not silently wedge ingestion for every other event type forever.

- [ ] **Step 1: Add the `apexes-storage` dependency**

```toml
# crates/apexes-market-data/Cargo.toml -- add to [dependencies]:
apexes-storage = { path = "../apexes-storage" }
```

- [ ] **Step 2: Write `ingestion.rs`**

```rust
// crates/apexes-market-data/src/ingestion.rs (new file)
//! [`run_ingestion_loop`]: the project's first general-purpose
//! live-market-data-to-ClickHouse ingestion path. Subscribes to
//! `provider` for `instruments`, buffers incoming `MarketEvent`s by
//! variant (`StorageWriter::write_batch` requires a homogeneous
//! batch), and flushes each variant's buffer to `writer` whenever it
//! reaches `MAX_BATCH_SIZE` events OR `FLUSH_INTERVAL` has elapsed
//! since that variant's last flush, whichever comes first -- balances
//! write latency (a low-volume symbol's data shouldn't wait
//! indefinitely) against ClickHouse insert efficiency (one-row-at-a-time
//! inserts are wasteful). Runs until `provider`'s stream ends or
//! `stop_signal` fires (graceful-shutdown convention matching
//! `run_live_trading`/`run_paper_trading`'s existing
//! `oneshot::Receiver<()>` pattern).

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent};
use apexes_storage::StorageWriter;
use futures::StreamExt;
use std::time::Duration;
use tokio::sync::oneshot;

const MAX_BATCH_SIZE: usize = 500;
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Buffers {
    tick: Vec<MarketEvent>,
    depth: Vec<MarketEvent>,
    bar: Vec<MarketEvent>,
    trade: Vec<MarketEvent>,
    mark_price: Vec<MarketEvent>,
}

impl Buffers {
    fn push(&mut self, event: MarketEvent) {
        match &event {
            MarketEvent::Tick(_) => self.tick.push(event),
            MarketEvent::Depth(_) => self.depth.push(event),
            MarketEvent::Bar(_) => self.bar.push(event),
            MarketEvent::Trade(_) => self.trade.push(event),
            MarketEvent::MarkPrice(_) => self.mark_price.push(event),
        }
    }

    fn any_over_size_threshold(&self) -> bool {
        [&self.tick, &self.depth, &self.bar, &self.trade, &self.mark_price]
            .iter()
            .any(|buf| buf.len() >= MAX_BATCH_SIZE)
    }
}

async fn flush_if_nonempty(writer: &StorageWriter, buffer: &mut Vec<MarketEvent>) {
    if buffer.is_empty() {
        return;
    }
    if let Err(error) = writer.write_batch(buffer).await {
        tracing::error!(?error, count = buffer.len(), "run_ingestion_loop: write_batch failed, dropping buffered batch");
    }
    buffer.clear();
}

async fn flush_all(writer: &StorageWriter, buffers: &mut Buffers) {
    flush_if_nonempty(writer, &mut buffers.tick).await;
    flush_if_nonempty(writer, &mut buffers.depth).await;
    flush_if_nonempty(writer, &mut buffers.bar).await;
    flush_if_nonempty(writer, &mut buffers.trade).await;
    flush_if_nonempty(writer, &mut buffers.mark_price).await;
}

pub async fn run_ingestion_loop(
    provider: &dyn MarketDataProvider,
    writer: &StorageWriter,
    instruments: &[Instrument],
    mut stop_signal: oneshot::Receiver<()>,
) -> Result<(), ApexesError> {
    let mut stream = provider.subscribe(instruments).await?;
    let mut buffers = Buffers::default();
    let mut flush_timer = tokio::time::interval(FLUSH_INTERVAL);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = &mut stop_signal => {
                tracing::info!("run_ingestion_loop: stop signal received, flushing remaining buffers");
                flush_all(writer, &mut buffers).await;
                return Ok(());
            }
            event = stream.next() => {
                let Some(event) = event else {
                    tracing::warn!("run_ingestion_loop: provider stream ended, flushing remaining buffers");
                    flush_all(writer, &mut buffers).await;
                    return Ok(());
                };
                buffers.push(event);
                if buffers.any_over_size_threshold() {
                    flush_all(writer, &mut buffers).await;
                }
            }
            _ = flush_timer.tick() => {
                flush_all(writer, &mut buffers).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, MarketType, TradeEvent, Venue};
    use async_trait::async_trait;
    use chrono::Utc;
    use futures::stream::BoxStream;
    use rust_decimal_macros::dec;
    use std::sync::Arc;
    use tokio::sync::Mutex as TokioMutex;

    /// A `MarketDataProvider` test double that yields a fixed sequence
    /// of events then ends its stream -- lets `run_ingestion_loop`'s
    /// "stream ended -> flush and return" path be tested without any
    /// live network dependency.
    struct FixedEventsProvider {
        events: TokioMutex<Vec<MarketEvent>>,
    }

    #[async_trait]
    impl MarketDataProvider for FixedEventsProvider {
        async fn subscribe(
            &self,
            _instruments: &[Instrument],
        ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
            let events = std::mem::take(&mut *self.events.lock().await);
            Ok(Box::pin(futures::stream::iter(events)))
        }
    }

    fn sample_instrument() -> Instrument {
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

    fn sample_trade() -> MarketEvent {
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

    #[test]
    fn buffers_push_routes_events_to_the_correct_variant_buffer() {
        let mut buffers = Buffers::default();
        buffers.push(sample_trade());
        assert_eq!(buffers.trade.len(), 1);
        assert_eq!(buffers.tick.len(), 0);
    }

    #[test]
    fn any_over_size_threshold_detects_a_full_buffer() {
        let mut buffers = Buffers::default();
        for _ in 0..MAX_BATCH_SIZE {
            buffers.push(sample_trade());
        }
        assert!(buffers.any_over_size_threshold());
    }

    #[test]
    fn any_over_size_threshold_is_false_when_under_the_limit() {
        let mut buffers = Buffers::default();
        buffers.push(sample_trade());
        assert!(!buffers.any_over_size_threshold());
    }
}
```

**Note for the implementer:** the module-level test suite above deliberately covers only `Buffers`' pure logic (no live ClickHouse/network dependency needed for THIS crate's unit tests). A full end-to-end test of `run_ingestion_loop` itself (provider -> buffer -> real `StorageWriter::write_batch` -> ClickHouse) belongs in a `#[ignore]`d live integration test (Step 4 below), since it requires both a running ClickHouse instance for `StorageWriter` and produces real inserted rows.

- [ ] **Step 3: Register the new module**

```rust
// crates/apexes-market-data/src/lib.rs -- add:
pub mod ingestion;
```

- [ ] **Step 4: Add a live integration test**

```rust
// crates/apexes-market-data/tests/live_ingestion.rs (new file)
//! Live integration test: subscribes to real Binance market data and
//! ingests it into a real local ClickHouse instance via
//! `run_ingestion_loop`. Excluded from default `cargo test` -- run
//! explicitly:
//!   docker compose up -d clickhouse
//!   cargo test -p apexes-market-data --test live_ingestion -- --ignored

use apexes_core::{AssetClass, Instrument, MarketType, Venue};
use apexes_market_data::BinanceMarketDataProvider;
use apexes_market_data::ingestion::run_ingestion_loop;
use apexes_storage::{EventType, StorageReader, StorageWriter};
use rust_decimal_macros::dec;
use std::time::Duration;

const CLICKHOUSE_URL: &str = "http://localhost:8123";

#[tokio::test]
#[ignore]
async fn ingests_real_binance_trades_into_clickhouse() {
    let provider = BinanceMarketDataProvider::new();
    let writer = StorageWriter::new(CLICKHOUSE_URL)
        .await
        .expect("StorageWriter::new should succeed with ClickHouse running locally");
    let reader = StorageReader::new(CLICKHOUSE_URL)
        .await
        .expect("StorageReader::new should succeed");

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

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let ingestion_handle = tokio::spawn(async move {
        run_ingestion_loop(&provider, &writer, &[instrument], stop_rx).await
    });

    // Let ingestion run long enough to accumulate at least one trade
    // and cross the 5-second flush interval.
    tokio::time::sleep(Duration::from_secs(20)).await;
    let _ = stop_tx.send(());
    ingestion_handle
        .await
        .expect("ingestion task should not panic")
        .expect("run_ingestion_loop should return Ok on graceful stop");

    let today = chrono::Utc::now().date_naive();
    let read_back = reader
        .read_batch("BTCUSDT", Venue::Binance, today..today.succ_opt().unwrap(), EventType::Trade)
        .await
        .expect("read_batch should succeed");

    assert!(!read_back.is_empty(), "expected at least one ingested trade to be readable back from ClickHouse");
}
```

Run: `cargo test -p apexes-market-data --test live_ingestion -- --ignored`
Expected: PASS (requires live network access to Binance AND a local ClickHouse instance via `docker compose up -d clickhouse`).

- [ ] **Step 5: Run this crate's non-ignored tests, clippy, and fmt**

Run: `cargo test -p apexes-market-data`
Expected: PASS, including the new `Buffers`-logic unit tests.

Run: `cargo clippy -p apexes-market-data --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-market-data/src/ingestion.rs crates/apexes-market-data/src/lib.rs crates/apexes-market-data/Cargo.toml crates/apexes-market-data/tests/live_ingestion.rs Cargo.lock
git commit -S -s -m "feat(apexes-market-data): general-purpose run_ingestion_loop (provider -> StorageWriter)"
```

---

### Task 7: `apexes-backtest` replay verification + final workspace checkpoint

**Files:**
- Modify: `crates/apexes-backtest/src/market_data_source.rs`
- Create: `crates/apexes-backtest/tests/mark_price_replay.rs`

**Interfaces:**
- Consumes: `apexes_core::MarketEvent::MarkPrice` (Task 1), `apexes_storage::EventType::MarkPrice` (Task 5).
- Produces: no new public interfaces -- `StorageMarketDataSource` already replays whatever `EventType` variants it's told to read; this task adds `EventType::MarkPrice` to its existing hardcoded list and proves it end-to-end.

- [ ] **Step 1: Add `EventType::MarkPrice` to `StorageMarketDataSource::events()`'s read loop**

```rust
// crates/apexes-backtest/src/market_data_source.rs -- change events()'s event_type array:
impl HistoricalMarketDataSource for StorageMarketDataSource {
    async fn events(&self) -> Result<Vec<MarketEvent>, ApexesError> {
        let mut all_events = Vec::new();

        for event_type in [
            EventType::Trade,
            EventType::Tick,
            EventType::Depth,
            EventType::Bar,
            EventType::MarkPrice,
        ] {
            let events = self
                .reader
                .read_batch(
                    &self.symbol,
                    self.venue,
                    self.date_range.clone(),
                    event_type,
                )
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
```

- [ ] **Step 2: Write a test proving `run_backtest` replays a `MarkPrice` event without error**

```rust
// crates/apexes-backtest/tests/mark_price_replay.rs (new file)
//! Proves `MarketEvent::MarkPrice` events flow through `run_backtest`
//! without error -- this sub-project's own scope stops at "mark price
//! is real, replayable data"; it does NOT yet make any strategy/ledger
//! logic react to it (that is a later, separate margin/leverage
//! design's job).

use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, InMemoryMarketDataSource, OrderIntent, Strategy,
    StrategyContext, run_backtest,
};
use apexes_core::{
    AssetClass, Instrument, MarketEvent, MarketType, MarkPriceUpdate, Venue,
};
use chrono::{NaiveDate, Utc};
use rust_decimal_macros::dec;
use std::collections::HashMap;

/// A strategy that never submits any order -- this test only cares
/// whether replay itself succeeds without error, not about any
/// order/fill behavior.
struct NoOpStrategy;

impl Strategy for NoOpStrategy {
    fn on_market_event(&mut self, _event: &MarketEvent, _ctx: &StrategyContext) -> Vec<OrderIntent> {
        vec![]
    }
}

fn btc_usdt_perpetual_instrument() -> Instrument {
    Instrument::new(
        "BTCUSDT",
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Perpetual,
        Some("BTC".to_string()),
        Some("USDT".to_string()),
        dec!(0.01),
        dec!(0.001),
        dec!(1),
    )
    .unwrap()
}

#[tokio::test]
async fn backtest_replays_a_mark_price_event_without_error() {
    let events = vec![MarketEvent::MarkPrice(MarkPriceUpdate {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        mark_price: dec!(65012.34),
        timestamp: Utc::now(),
    })];
    let market_data = InMemoryMarketDataSource::new(events);

    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(100000));

    let config = BacktestConfig {
        instruments: vec![btc_usdt_perpetual_instrument()],
        date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
            ..NaiveDate::from_ymd_opt(2027, 1, 1).unwrap(),
        initial_balances,
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        },
        speed: apexes_backtest::ReplaySpeed::AsFastAsPossible,
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
    };

    let registry = apexes_core::registry::Registry::new();
    let report = run_backtest(config, &registry, market_data, NoOpStrategy)
        .await
        .expect("run_backtest must not error on a MarkPrice event");

    assert!(report.trades.is_empty(), "NoOpStrategy submits no orders");
}
```

(Check `apexes_backtest`'s actual re-exported item names -- `FlatFeeSchedule`, `ReplaySpeed`, `run_backtest`'s exact signature -- via `file_read` on `crates/apexes-backtest/src/lib.rs` and an existing test like `crates/apexes-backtest/tests/end_to_end.rs` before finalizing this test's imports, since this plan's authors verified these names against the codebase as of this plan's drafting but a fresh implementer should confirm nothing has drifted.)

- [ ] **Step 3: Run this crate's tests, clippy, and fmt**

Run: `cargo test -p apexes-backtest`
Expected: PASS, including the new `backtest_replays_a_mark_price_event_without_error` test.

Run: `cargo clippy -p apexes-backtest --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 4: Full workspace final checkpoint**

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo test --workspace -- --test-threads=1`
Expected: 0 failures (matching the same pre-existing environmental-limitation `#[ignore]`d-test documentation in `.superpowers/sdd/progress.md` -- confirm these are the SAME pre-existing failures, nothing new introduced by this plan).

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-backtest/src/market_data_source.rs crates/apexes-backtest/tests/mark_price_replay.rs
git commit -S -s -m "feat(apexes-backtest): replay MarketEvent::MarkPrice via StorageMarketDataSource"
```

---

### Task 8: Final whole-branch code review and `docs/FEATURES.md` update

**Files:**
- Modify: `docs/FEATURES.md`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:** none (documentation/review-only task).

- [ ] **Step 1: Run the complete verification suite one final time**

Run: `cargo build --workspace`, `cargo test --workspace -- --test-threads=1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`.
Expected: all clean, matching Task 7's Step 4 exactly (this is a final re-confirmation before declaring this plan done, per this project's established multi-sub-project-completion precedent).

- [ ] **Step 2: Update `docs/FEATURES.md`**

This plan does NOT itself complete any `docs/FEATURES.md` row -- row `6.4` (margin & leverage monitoring) is the FOLLOW-UP work this pipeline enables, not delivered by this plan. Add a brief note to row `6.4`'s existing Notes column (currently empty/`🔲 Not Started`) recording that its prerequisite data pipeline now exists:

Find row `6.4` (`grep -n "| 6.4 |" docs/FEATURES.md`) and update its Notes column to: `Prerequisite mark-price data pipeline (apexes-core::MarketEvent::MarkPrice, apexes-market-data ingestion, apexes-storage mark_prices table) now complete -- see docs/superpowers/plans/2026-08-02-apexes-mark-price-pipeline.md. Row 6.4's own margin/leverage/liquidation math is still 🔲 Not Started.`

Do NOT change row `6.4`'s Status column away from `🔲 Not Started` -- this plan does not implement margin/leverage monitoring itself, only its data prerequisite. Do NOT recompute the Summary counts table (no Status column values changed).

- [ ] **Step 3: Append a `.superpowers/sdd/progress.md` ledger entry**

Summarize this plan's completion: the new `MarketEvent::MarkPrice` variant, Perpetual support added to `apexes-market-data`'s ingestion tasks, the new `mark_price_task`, the new general-purpose `run_ingestion_loop`, and the new `mark_prices` ClickHouse table -- framed explicitly as a prerequisite for sub-project #5's row 6.4, not a completion of it.

- [ ] **Step 4: Commit**

```bash
git add docs/FEATURES.md .superpowers/sdd/progress.md
git commit -S -s -m "docs: record mark-price data pipeline completion (prerequisite for #5 row 6.4)"
```
