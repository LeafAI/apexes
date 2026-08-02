# Apexes: In-Process Market Event Bus Implementation Plan (`docs/FEATURES.md` row `1.7`)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the design in `docs/superpowers/specs/2026-08-03-apexes-message-bus-design.md`: a new `apexes-message-bus` crate providing `MarketEventBus`, an in-process multi-subscriber broadcast layer over `apexes_core::MarketDataProvider` with lazy connection, fixed-instrument-set enforcement, and time-window downsampling -- completing roadmap sub-project `#12`'s in-process scope for `docs/FEATURES.md` row `1.7`.

**Architecture:** Six sequential tasks. Task 1 scaffolds the crate plus a hand-written mock `MarketDataProvider` test double and the `BusState`/`MarketEventBus::new`/`new_with_capacity` skeleton. Task 2 implements `subscribe()`'s lazy-connect/reuse/mismatch-error logic. Task 3 implements `subscribe_downsampled()`. Task 4 adds dedicated lagged-receiver-handling coverage (the underlying behavior already exists after Tasks 2-3; this task's job is proving it, and hardening if a gap is found). Task 5 is a full-workspace verification checkpoint. Task 6 is the final whole-branch review plus the `docs/FEATURES.md` update.

**Tech Stack:** Rust 2024, existing workspace deps only (`apexes-core`, `async-trait`, `futures`, `tokio`, `tracing`). `tokio`'s `sync` feature (for `broadcast`) and, for tests only, `test-util`/`macros`/`rt-multi-thread`/`time` (for `#[tokio::test(start_paused = true)]` + `tokio::time::advance`, confirmed working against a spawned-task-plus-`broadcast`-channel setup during design research). No new third-party dependencies.

## Global Constraints

- Rust edition 2024, workspace-wide `cargo check`/`cargo clippy --all-targets -- -D warnings`/`cargo fmt --check` must stay clean after every task.
- No `unsafe` blocks. Never bare `.unwrap()`/`.expect()` in production code (test code may use them freely).
- Every commit signed: `git commit -S -s`.
- **No panics, no silent event drops without a log line.** A lagged/slow subscriber's `broadcast::Receiver::recv()` returning `Err(RecvError::Lagged(n))` must be caught and logged via `tracing::warn!`, then the read loop continues -- never panic, never silently stop delivering events to that subscriber.
- **`MarketEventBus` does NOT implement `apexes_core::MarketDataProvider`.** It is a distinct type with its own `subscribe`/`subscribe_downsampled` methods returning `tokio::sync::broadcast::Receiver<MarketEvent>` directly (not `BoxStream`) -- callers must explicitly construct and use a `MarketEventBus` to opt into broadcast semantics; this project's existing `MarketDataProvider` consumers are not modified by this plan.
- **The instrument set is fixed by the first `subscribe()` call.** A later `subscribe()`/`subscribe_downsampled()` call with a different instrument set (order-independent equality) must return `Err(ApexesError::SubscriptionFailed { .. })`, never silently resubscribe or silently ignore the mismatch. Dynamically growing/shrinking a live bus's instrument set is explicitly out of scope for this plan.
- **No new `ApexesError` variant.** Reuse the existing `SubscriptionFailed { symbol, venue, reason, source_detail }` for both real connection failures (propagated from the wrapped provider) and the fixed-instrument-set-mismatch case.
- **This plan implements in-process broadcast only.** No NATS JetStream, no inter-process transport of any kind -- that remains explicitly deferred per the design spec §1.
- Full design rationale (why `apexes-backtest::run_backtest` is out of scope, why `subscribe_downsampled` samples rather than aggregates, why `BarAggregator` remains the tool for true OHLCV aggregation) is in the design spec -- read it before starting if anything below is unclear on *why*, not just *what*.

---

### Task 1: `apexes-message-bus` crate scaffold — `BusState`, `MarketEventBus::new`/`new_with_capacity`, mock provider test double

**Files:**
- Create: `crates/apexes-message-bus/Cargo.toml`
- Create: `crates/apexes-message-bus/src/lib.rs`
- Create: `crates/apexes-message-bus/src/bus.rs`
- Create: `crates/apexes-message-bus/src/test_support.rs`
- Modify: `Cargo.toml` (workspace members)

**Interfaces:**
- Consumes: `apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent}` (existing).
- Produces: `apexes_message_bus::{MarketEventBus, test_support::MockMarketDataProvider}`.

- [ ] **Step 1: Add the crate to the workspace**

```toml
# Cargo.toml -- add "crates/apexes-message-bus" to the [workspace] members list
```

- [ ] **Step 2: Write `Cargo.toml` for the new crate**

```toml
# crates/apexes-message-bus/Cargo.toml (new file)
[package]
name = "apexes-message-bus"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
async-trait = { workspace = true }
futures = { workspace = true }
tokio = { workspace = true, features = ["sync"] }
tracing = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread", "time"] }
rust_decimal_macros = "1.40"
chrono = { workspace = true }
```

- [ ] **Step 3: Write the mock `MarketDataProvider` test double**

```rust
// crates/apexes-message-bus/src/test_support.rs (new file)
//! Test-only support code for this crate's own tests. `pub` (not
//! `#[cfg(test)]`-gated) so downstream crates that later want to test
//! against `MarketEventBus` can reuse `MockMarketDataProvider` too,
//! matching this project's existing `apexes-oms::MockExecutionGateway`
//! pattern (a real, reusable in-memory test double, not a throwaway
//! per-test mock).

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent};
use async_trait::async_trait;
use futures::stream::BoxStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// An in-memory `MarketDataProvider` a test can push synthetic
/// `MarketEvent`s into via `push_event` -- `subscribe()` returns a
/// stream reading from an internal unbounded channel. Tracks how many
/// times `subscribe()` was actually called (`subscribe_call_count`),
/// which `MarketEventBus`'s lazy-connection/connection-reuse tests
/// assert on directly.
pub struct MockMarketDataProvider {
    subscribe_call_count: AtomicUsize,
    event_sender: Mutex<Option<mpsc::UnboundedSender<MarketEvent>>>,
}

impl Default for MockMarketDataProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl MockMarketDataProvider {
    pub fn new() -> Self {
        Self {
            subscribe_call_count: AtomicUsize::new(0),
            event_sender: Mutex::new(None),
        }
    }

    /// How many times `subscribe()` has actually been called on this
    /// provider -- `MarketEventBus`'s connection-reuse tests assert
    /// this stays at `1` across multiple `MarketEventBus::subscribe()`
    /// calls with the same instrument set.
    pub fn subscribe_call_count(&self) -> usize {
        self.subscribe_call_count.load(Ordering::SeqCst)
    }

    /// Pushes `event` into the stream returned by the most recent
    /// `subscribe()` call. Panics if `subscribe()` was never called
    /// (a test setup error) -- this is test-support code, not
    /// production code, so a panic here is an acceptable "fail fast
    /// on misuse" signal, not a violation of this project's
    /// no-bare-unwrap-in-production-code rule.
    pub fn push_event(&self, event: MarketEvent) {
        let guard = self.event_sender.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let sender = guard
            .as_ref()
            .expect("push_event called before subscribe() was ever called on this mock");
        sender
            .send(event)
            .expect("push_event: receiver side of the mock's channel was dropped");
    }
}

#[async_trait]
impl MarketDataProvider for MockMarketDataProvider {
    async fn subscribe(
        &self,
        _instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        use futures::StreamExt;
        self.subscribe_call_count.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::unbounded_channel();
        *self.event_sender.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(tx);
        Ok(UnboundedReceiverStream::new(rx).boxed())
    }
}
```

- [ ] **Step 4: Add `tokio-stream` as a dependency (needed by the mock)**

```toml
# crates/apexes-message-bus/Cargo.toml -- add to [dependencies]:
tokio-stream = "0.1"
```

- [ ] **Step 5: Write the failing tests for `MarketEventBus::new`**

```rust
// crates/apexes-message-bus/src/bus.rs (new file)
//! [`MarketEventBus`]: an in-process, multi-subscriber broadcast layer
//! over one real `apexes_core::MarketDataProvider`. Deliberately does
//! NOT implement `MarketDataProvider` itself -- see this crate's
//! `lib.rs` doc comment for the full rationale. See
//! `docs/superpowers/specs/2026-08-03-apexes-message-bus-design.md`
//! for the complete design.

use std::sync::Arc;

use apexes_core::{Instrument, MarketDataProvider};
use tokio::sync::{broadcast, Mutex};

/// Default `tokio::sync::broadcast` channel capacity used by
/// `MarketEventBus::new` -- callers needing a different capacity (e.g.
/// a very bursty feed with slow consumers) should use
/// `MarketEventBus::new_with_capacity` instead.
const DEFAULT_BROADCAST_CAPACITY: usize = 1024;

enum BusState {
    NotConnected,
    #[allow(dead_code)] // fields read in Task 2
    Connected {
        instruments: Vec<Instrument>,
    },
}

pub struct MarketEventBus {
    provider: Box<dyn MarketDataProvider>,
    capacity: usize,
    state: Mutex<BusState>,
}

impl MarketEventBus {
    pub fn new(provider: Box<dyn MarketDataProvider>) -> Self {
        Self::new_with_capacity(provider, DEFAULT_BROADCAST_CAPACITY)
    }

    pub fn new_with_capacity(provider: Box<dyn MarketDataProvider>, capacity: usize) -> Self {
        Self {
            provider,
            capacity,
            state: Mutex::new(BusState::NotConnected),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::MockMarketDataProvider;
    use apexes_core::{AssetClass, Instrument, MarketType, Venue};
    use rust_decimal_macros::dec;
    use std::sync::Arc;

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

    #[tokio::test]
    async fn new_does_not_call_subscribe_on_the_wrapped_provider() {
        let mock = Arc::new(MockMarketDataProvider::new());
        let _bus = MarketEventBus::new(Box::new(Arc::clone(&mock)));
        assert_eq!(mock.subscribe_call_count(), 0);
    }

    #[tokio::test]
    async fn new_with_capacity_also_does_not_call_subscribe() {
        let mock = Arc::new(MockMarketDataProvider::new());
        let _bus = MarketEventBus::new_with_capacity(Box::new(Arc::clone(&mock)), 4);
        assert_eq!(mock.subscribe_call_count(), 0);
    }
}
```

Note: `MockMarketDataProvider` must be usable both directly (owned) and
wrapped in `Arc` so a test can hold its own handle to assert on
`subscribe_call_count()` while also handing a `Box<dyn
MarketDataProvider>` to `MarketEventBus`. Implement `MarketDataProvider`
for `Arc<MockMarketDataProvider>` too (a blanket delegating impl) in
Step 3's file so `Box::new(Arc::clone(&mock))` type-checks:

```rust
// crates/apexes-message-bus/src/test_support.rs -- add after the
// existing `impl MarketDataProvider for MockMarketDataProvider` block:

#[async_trait]
impl MarketDataProvider for std::sync::Arc<MockMarketDataProvider> {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        self.as_ref().subscribe(instruments).await
    }
}
```

Run: `cargo test -p apexes-message-bus bus`
Expected: FAIL with "cannot find type `MarketEventBus`"/"cannot find struct field" -- expected; Step 6 registers the module so this compiles, and the struct itself is already written above (this task's TDD "RED" step is really about the mock/blanket-impl compiling and the assertion being meaningful, since `MarketEventBus`'s own code is written directly in Step 5, not iteratively -- run the test now to confirm it fails only because `lib.rs` doesn't exist/register the module yet, not because of a logic error).

- [ ] **Step 6: Write `lib.rs`**

```rust
// crates/apexes-message-bus/src/lib.rs (new file)
//! Apexes in-process market event bus. Covers `docs/FEATURES.md` row
//! `1.7`'s in-process-broadcast-and-downsampling scope (genuine
//! inter-process/multi-machine message-bus support, e.g. NATS
//! JetStream, remains unimplemented and explicitly deferred -- see
//! `docs/superpowers/specs/2026-08-03-apexes-message-bus-design.md`
//! §1 for the full scope rationale).
//!
//! [`MarketEventBus`] deliberately does NOT implement
//! `apexes_core::MarketDataProvider` -- it is a distinct type with its
//! own `subscribe`/`subscribe_downsampled` methods returning real
//! `tokio::sync::broadcast::Receiver<MarketEvent>` values directly
//! (not `BoxStream`), so callers must explicitly construct and use a
//! `MarketEventBus` to opt into multi-subscriber broadcast semantics
//! rather than accidentally getting them through the existing trait.

pub mod bus;
pub mod test_support;

pub use bus::MarketEventBus;
```

- [ ] **Step 7: Run the new tests**

Run: `cargo test -p apexes-message-bus bus`
Expected: both pass.

- [ ] **Step 8: Run this crate's full test suite, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-message-bus`
Expected: all pass (2 tests).

Run: `cargo clippy -p apexes-message-bus --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 9: Commit**

```bash
git add Cargo.toml crates/apexes-message-bus
git commit -S -s -m "feat(apexes-message-bus): crate scaffold -- BusState, MarketEventBus::new/new_with_capacity, MockMarketDataProvider test double"
```

---

### Task 2: `MarketEventBus::subscribe` — lazy connection, connection reuse, fixed-instrument-set mismatch

**Files:**
- Modify: `crates/apexes-message-bus/src/bus.rs`

**Interfaces:**
- Consumes: `apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent}` (existing); `BusState`/`MarketEventBus` (Task 1).
- Produces: `MarketEventBus::subscribe(&self, instruments: &[Instrument]) -> Result<broadcast::Receiver<MarketEvent>, ApexesError>`.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/apexes-message-bus/src/bus.rs -- add to the existing
// #[cfg(test)] mod tests block (after the two Task 1 tests):

use apexes_core::{ApexesError, MarketEvent, TradeEvent};
use chrono::Utc;

fn sample_instrument_2() -> Instrument {
    Instrument::new(
        "ETHUSDT",
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("ETH".to_string()),
        Some("USDT".to_string()),
        dec!(0.01),
        dec!(0.0001),
        dec!(1),
    )
    .unwrap()
}

fn sample_trade_event(symbol: &str) -> MarketEvent {
    MarketEvent::Trade(TradeEvent {
        symbol: symbol.to_string(),
        venue: Venue::Binance,
        price: dec!(65000),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: Utc::now(),
    })
}

#[tokio::test]
async fn first_subscribe_calls_the_wrapped_provider_exactly_once() {
    let mock = Arc::new(MockMarketDataProvider::new());
    let bus = MarketEventBus::new(Box::new(Arc::clone(&mock)));
    let instruments = vec![sample_instrument()];

    let _receiver = bus.subscribe(&instruments).await.unwrap();
    assert_eq!(mock.subscribe_call_count(), 1);
}

#[tokio::test]
async fn second_subscribe_with_the_same_instruments_reuses_the_connection() {
    let mock = Arc::new(MockMarketDataProvider::new());
    let bus = MarketEventBus::new(Box::new(Arc::clone(&mock)));
    let instruments = vec![sample_instrument()];

    let _receiver_1 = bus.subscribe(&instruments).await.unwrap();
    let _receiver_2 = bus.subscribe(&instruments).await.unwrap();
    assert_eq!(mock.subscribe_call_count(), 1);
}

#[tokio::test]
async fn second_subscribe_with_a_reordered_same_set_still_reuses_the_connection() {
    let mock = Arc::new(MockMarketDataProvider::new());
    let bus = MarketEventBus::new(Box::new(Arc::clone(&mock)));
    let forward_order = vec![sample_instrument(), sample_instrument_2()];
    let reverse_order = vec![sample_instrument_2(), sample_instrument()];

    let _receiver_1 = bus.subscribe(&forward_order).await.unwrap();
    let _receiver_2 = bus.subscribe(&reverse_order).await.unwrap();
    assert_eq!(mock.subscribe_call_count(), 1);
}

#[tokio::test]
async fn subscribe_with_a_different_instrument_set_after_connection_returns_err() {
    let mock = Arc::new(MockMarketDataProvider::new());
    let bus = MarketEventBus::new(Box::new(Arc::clone(&mock)));
    let first_set = vec![sample_instrument()];
    let second_set = vec![sample_instrument_2()];

    let _receiver = bus.subscribe(&first_set).await.unwrap();
    let result = bus.subscribe(&second_set).await;
    assert!(matches!(result, Err(ApexesError::SubscriptionFailed { .. })));
}

#[tokio::test]
async fn two_receivers_both_get_the_same_broadcast_event() {
    let mock = Arc::new(MockMarketDataProvider::new());
    let bus = MarketEventBus::new(Box::new(Arc::clone(&mock)));
    let instruments = vec![sample_instrument()];

    let mut receiver_1 = bus.subscribe(&instruments).await.unwrap();
    let mut receiver_2 = bus.subscribe(&instruments).await.unwrap();

    mock.push_event(sample_trade_event("BTCUSDT"));

    let event_1 = receiver_1.recv().await.unwrap();
    let event_2 = receiver_2.recv().await.unwrap();
    assert_eq!(event_1, event_2);
}
```

Run: `cargo test -p apexes-message-bus bus`
Expected: FAIL with "no method named `subscribe` found for struct `MarketEventBus`" -- expected; Step 2 implements it.

- [ ] **Step 2: Implement `subscribe`**

```rust
// crates/apexes-message-bus/src/bus.rs -- modify the `BusState` enum
// (remove the #[allow(dead_code)], the field is used now) and add the
// `subscribe` method to `impl MarketEventBus` (above the #[cfg(test)]
// block):

enum BusState {
    NotConnected,
    Connected {
        instruments: Vec<Instrument>,
        sender: broadcast::Sender<MarketEvent>,
    },
}
```

```rust
// crates/apexes-message-bus/src/bus.rs -- add to `impl MarketEventBus`:

use apexes_core::{ApexesError, MarketEvent};
use futures::StreamExt;

impl MarketEventBus {
    /// Compares two instrument slices for order-independent set
    /// equality. `Instrument` derives `PartialEq`/`Eq`/`Hash`, but a
    /// `HashSet` comparison would silently ignore duplicate entries in
    /// either slice (e.g. `[A, A]` vs `[A]` would incorrectly compare
    /// equal) -- sorting a stable, comparable projection instead
    /// preserves duplicate-count sensitivity. `Instrument` itself does
    /// not derive `Ord`, so this sorts by the `(symbol, venue)` pair
    /// (a unique key for any realistic instrument set this project
    /// constructs) paired with the original `Instrument` for the
    /// final equality check.
    fn same_instrument_set(a: &[Instrument], b: &[Instrument]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        let mut a_sorted: Vec<&Instrument> = a.iter().collect();
        let mut b_sorted: Vec<&Instrument> = b.iter().collect();
        let sort_key = |i: &&Instrument| (i.symbol.clone(), format!("{:?}", i.venue));
        a_sorted.sort_by_key(sort_key);
        b_sorted.sort_by_key(sort_key);
        a_sorted == b_sorted
    }

    pub async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<broadcast::Receiver<MarketEvent>, ApexesError> {
        let mut state = self.state.lock().await;
        match &*state {
            BusState::Connected {
                instruments: connected_instruments,
                sender,
            } => {
                if Self::same_instrument_set(connected_instruments, instruments) {
                    Ok(sender.subscribe())
                } else {
                    Err(ApexesError::SubscriptionFailed {
                        symbol: instruments
                            .first()
                            .map(|i| i.symbol.clone())
                            .unwrap_or_else(|| "*".to_string()),
                        venue: instruments
                            .first()
                            .map(|i| i.venue)
                            .unwrap_or(apexes_core::Venue::Binance),
                        reason: "MarketEventBus is already connected with a different, fixed \
                                 instrument set -- dynamically changing a live bus's instrument \
                                 set is not supported"
                            .to_string(),
                        source_detail: None,
                    })
                }
            }
            BusState::NotConnected => {
                let mut stream = self.provider.subscribe(instruments).await?;
                let (sender, receiver) = broadcast::channel(self.capacity);
                let forward_sender = sender.clone();
                tokio::spawn(async move {
                    while let Some(event) = stream.next().await {
                        // A `send` error here means every receiver has
                        // been dropped -- nothing left to forward to,
                        // not a failure worth logging.
                        let _ = forward_sender.send(event);
                    }
                    tracing::info!(
                        "MarketEventBus: upstream provider stream ended, forwarding task exiting"
                    );
                });
                *state = BusState::Connected {
                    instruments: instruments.to_vec(),
                    sender,
                };
                Ok(receiver)
            }
        }
    }
}
```

- [ ] **Step 3: Run the new tests**

Run: `cargo test -p apexes-message-bus bus`
Expected: all 7 pass (2 from Task 1 + 5 from this task).

- [ ] **Step 4: Run this crate's full test suite, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-message-bus`
Expected: all pass.

Run: `cargo clippy -p apexes-message-bus --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-message-bus/src/bus.rs
git commit -S -s -m "feat(apexes-message-bus): MarketEventBus::subscribe -- lazy connection, connection reuse, fixed-instrument-set mismatch"
```

---

### Task 3: `MarketEventBus::subscribe_downsampled` — per-window last-event-wins sampling

**Files:**
- Modify: `crates/apexes-message-bus/src/bus.rs`

**Interfaces:**
- Consumes: `MarketEventBus::subscribe` (Task 2); `apexes_core::MarketEvent` (existing).
- Produces: `MarketEventBus::subscribe_downsampled(&self, instruments: &[Instrument], interval: Duration) -> Result<broadcast::Receiver<MarketEvent>, ApexesError>`.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/apexes-message-bus/src/bus.rs -- add to the #[cfg(test)] mod
// tests block:

use std::time::Duration;

fn sample_tick_event(symbol: &str, bid_price: rust_decimal::Decimal) -> MarketEvent {
    apexes_core::MarketEvent::Tick(apexes_core::Tick {
        symbol: symbol.to_string(),
        venue: Venue::Binance,
        bid_price,
        bid_size: dec!(1),
        ask_price: bid_price + dec!(1),
        ask_size: dec!(1),
        timestamp: Utc::now(),
    })
}

#[tokio::test(start_paused = true)]
async fn subscribe_downsampled_forwards_only_the_last_event_per_window() {
    let mock = Arc::new(MockMarketDataProvider::new());
    let bus = MarketEventBus::new(Box::new(Arc::clone(&mock)));
    let instruments = vec![sample_instrument()];

    let mut downsampled = bus
        .subscribe_downsampled(&instruments, Duration::from_secs(1))
        .await
        .unwrap();

    // Three ticks arrive within the same window; only the last
    // (bid_price = dec!(3)) should be forwarded once the window elapses.
    mock.push_event(sample_tick_event("BTCUSDT", dec!(1)));
    tokio::task::yield_now().await;
    mock.push_event(sample_tick_event("BTCUSDT", dec!(2)));
    tokio::task::yield_now().await;
    mock.push_event(sample_tick_event("BTCUSDT", dec!(3)));
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;

    let forwarded = downsampled.recv().await.unwrap();
    match forwarded {
        MarketEvent::Tick(tick) => assert_eq!(tick.bid_price, dec!(3)),
        other => panic!("expected a Tick event, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn subscribe_downsampled_keeps_separate_windows_per_symbol() {
    let mock = Arc::new(MockMarketDataProvider::new());
    let bus = MarketEventBus::new(Box::new(Arc::clone(&mock)));
    let instruments = vec![sample_instrument(), sample_instrument_2()];

    let mut downsampled = bus
        .subscribe_downsampled(&instruments, Duration::from_secs(1))
        .await
        .unwrap();

    mock.push_event(sample_tick_event("BTCUSDT", dec!(100)));
    tokio::task::yield_now().await;
    mock.push_event(sample_tick_event("ETHUSDT", dec!(200)));
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;

    let mut seen_symbols = std::collections::HashSet::new();
    for _ in 0..2 {
        let forwarded = downsampled.recv().await.unwrap();
        if let MarketEvent::Tick(tick) = forwarded {
            seen_symbols.insert(tick.symbol);
        }
    }
    assert!(seen_symbols.contains("BTCUSDT"));
    assert!(seen_symbols.contains("ETHUSDT"));
}
```

Run: `cargo test -p apexes-message-bus bus`
Expected: FAIL with "no method named `subscribe_downsampled` found for struct `MarketEventBus`" -- expected; Step 2 implements it.

- [ ] **Step 2: Implement `subscribe_downsampled`**

```rust
// crates/apexes-message-bus/src/bus.rs -- add to `impl MarketEventBus`:

use std::collections::HashMap;
use std::time::Duration;

impl MarketEventBus {
    /// A bucket key identifying one independently-downsampled stream:
    /// symbol, venue, and the `MarketEvent` variant discriminant (so a
    /// `Tick` and a `Trade` for the same symbol/venue are sampled
    /// independently, never overwriting each other's "last event").
    fn downsample_bucket_key(event: &MarketEvent) -> (String, apexes_core::Venue, std::mem::Discriminant<MarketEvent>) {
        let (symbol, venue) = match event {
            MarketEvent::Tick(t) => (t.symbol.clone(), t.venue),
            MarketEvent::Depth(d) => (d.symbol.clone(), d.venue),
            MarketEvent::Bar(b) => (b.symbol.clone(), b.venue),
            MarketEvent::Trade(t) => (t.symbol.clone(), t.venue),
            MarketEvent::MarkPrice(m) => (m.symbol.clone(), m.venue),
        };
        (symbol, venue, std::mem::discriminant(event))
    }

    pub async fn subscribe_downsampled(
        &self,
        instruments: &[Instrument],
        interval: Duration,
    ) -> Result<broadcast::Receiver<MarketEvent>, ApexesError> {
        let mut raw_receiver = self.subscribe(instruments).await?;
        let (downsampled_sender, downsampled_receiver) = broadcast::channel(self.capacity);

        tokio::spawn(async move {
            let mut latest_per_bucket: HashMap<
                (String, apexes_core::Venue, std::mem::Discriminant<MarketEvent>),
                MarketEvent,
            > = HashMap::new();
            let mut window_timer = tokio::time::interval(interval);
            window_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    event = raw_receiver.recv() => {
                        match event {
                            Ok(event) => {
                                let key = Self::downsample_bucket_key(&event);
                                latest_per_bucket.insert(key, event);
                            }
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                tracing::warn!(
                                    skipped,
                                    "MarketEventBus::subscribe_downsampled: raw receiver lagged, some events skipped"
                                );
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                tracing::info!(
                                    "MarketEventBus::subscribe_downsampled: upstream broadcast closed, forwarding task exiting"
                                );
                                return;
                            }
                        }
                    }
                    _ = window_timer.tick() => {
                        for (_, event) in latest_per_bucket.drain() {
                            let _ = downsampled_sender.send(event);
                        }
                    }
                }
            }
        });

        Ok(downsampled_receiver)
    }
}
```

- [ ] **Step 3: Run the new tests**

Run: `cargo test -p apexes-message-bus bus`
Expected: all 9 pass (7 from Tasks 1-2 + 2 from this task).

- [ ] **Step 4: Run this crate's full test suite, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-message-bus`
Expected: all pass.

Run: `cargo clippy -p apexes-message-bus --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-message-bus/src/bus.rs
git commit -S -s -m "feat(apexes-message-bus): MarketEventBus::subscribe_downsampled -- per-window last-event-wins sampling"
```

---

### Task 4: Lagged-receiver-handling test coverage

**Files:**
- Modify: `crates/apexes-message-bus/src/bus.rs`

**Interfaces:**
- Consumes: `MarketEventBus::subscribe` (Task 2).
- Produces: nothing new (test coverage only).

- [ ] **Step 1: Write the failing test**

```rust
// crates/apexes-message-bus/src/bus.rs -- add to the #[cfg(test)] mod
// tests block:

#[tokio::test]
async fn a_slow_receiver_falling_behind_does_not_panic_the_consumer() {
    // A small capacity so it's easy to make a receiver "lag" (fall far
    // enough behind that the broadcast channel drops old messages for
    // it) without needing to send an enormous number of events.
    let mock = Arc::new(MockMarketDataProvider::new());
    let bus = MarketEventBus::new_with_capacity(Box::new(Arc::clone(&mock)), 2);
    let instruments = vec![sample_instrument()];

    let mut slow_receiver = bus.subscribe(&instruments).await.unwrap();

    // Send more events than the channel's capacity while `slow_receiver`
    // never polls -- this guarantees it falls behind.
    for i in 0..10u32 {
        mock.push_event(sample_trade_event(&format!("SYMBOL{i}")));
        tokio::task::yield_now().await;
    }

    // The receiver's next `recv()` must surface as `Err(Lagged(_))`,
    // not a panic and not a silently-stuck future.
    let result = slow_receiver.recv().await;
    assert!(matches!(
        result,
        Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
    ));

    // After the Lagged error, the receiver can keep reading -- it is
    // not permanently broken.
    let next = slow_receiver.recv().await;
    assert!(next.is_ok() || matches!(next, Err(tokio::sync::broadcast::error::RecvError::Lagged(_))));
}
```

Run: `cargo test -p apexes-message-bus a_slow_receiver_falling_behind`
Expected: PASS immediately -- this test exercises `tokio::sync::broadcast::Receiver`'s own built-in `Lagged` behavior directly (no new production code is needed for a caller reading a receiver directly to observe `Lagged`; Task 3's `subscribe_downsampled` forwarding task already handles `Lagged` explicitly as shown in Task 3 Step 2). If this test fails, it indicates a genuine gap in either this test's own setup (e.g. capacity too large, not enough events sent to actually lag, missing `yield_now` calls preventing the sender from actually racing ahead of the receiver) -- fix the test, not production code, unless investigation reveals `MarketEventBus::subscribe` itself is doing something that prevents a receiver from ever lagging (it should not be).

- [ ] **Step 2: Run the crate's full test suite, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-message-bus`
Expected: all pass (10 tests: 2 + 5 + 2 + 1).

Run: `cargo clippy -p apexes-message-bus --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 3: Commit**

```bash
git add crates/apexes-message-bus/src/bus.rs
git commit -S -s -m "test(apexes-message-bus): lagged-receiver handling coverage -- a slow subscriber falling behind surfaces Lagged, never panics"
```

---

### Task 5: Full-workspace verification checkpoint

**Files:** none created/modified -- verification only.

- [ ] **Step 1: Full workspace test run**

Run: `cargo test --workspace`
Expected: 0 failed across every crate (some `#[ignore]`d live-integration tests are expected in unrelated crates; this plan's new crate adds no `#[ignore]`d tests).

- [ ] **Step 2: Full workspace clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings across all crates including the new one.

- [ ] **Step 3: Full workspace fmt check**

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 4: Placeholder-safety grep**

Run: `grep -rn "unimplemented!\|todo!\|panic!(\"not" crates/apexes-message-bus/src`
Expected: no matches.

- [ ] **Step 5: No-bare-unwrap-in-production-code spot check**

Run: `grep -n "\.unwrap()\|\.expect(" crates/apexes-message-bus/src/bus.rs | grep -v "^.*#\[cfg(test)\]" `
Expected: any matches found must be inside the `#[cfg(test)] mod tests` block (verify by inspecting line numbers against where that block starts) -- `test_support.rs`'s `push_event`/mock-setup unwraps are intentional test-support-code panics-on-misuse, documented as such in Task 1, not production code under this rule.

- [ ] **Step 6: No commit for this task** (verification-only; if any check fails, fix the underlying task's code and re-run that task's own commit).

---

### Task 6: Final whole-branch review + `docs/FEATURES.md` update

**Files:**
- Modify: `docs/FEATURES.md`

- [ ] **Step 1: Generate the whole-branch review package**

Use the base commit recorded right before Task 1 started (the tip of
`master` immediately after this plan document itself was committed) and
HEAD (the tip after Task 4). Dispatch a final reviewer (per this
project's established subagent-tiering convention, a top-tier profile
for final whole-branch reviews) covering:
- Scope match against the design spec: does `MarketEventBus` genuinely
  implement lazy connection, connection reuse, fixed-instrument-set
  enforcement, and per-window-last-event-wins downsampling exactly as
  specified -- no more (no accidental dynamic-instrument-set support),
  no less?
- The safety-critical invariant: does a lagging/slow subscriber ever
  cause a panic or a silent, unlogged stop in delivery anywhere in this
  crate?
- Confirm `MarketEventBus` genuinely does NOT implement
  `apexes_core::MarketDataProvider` (grep for `impl MarketDataProvider
  for MarketEventBus` -- must find zero matches in production code).
- Code quality: no unsafe, no bare unwrap/expect in production code,
  idiomatic Rust, no blocking calls in async context.
- Whether `docs/FEATURES.md` row `1.7` should be marked `Done` or `In
  Progress` -- per the design spec §5, this row's Notes must honestly
  state that only in-process broadcast is implemented and that genuine
  inter-process message-bus support (NATS JetStream or similar) remains
  unimplemented and deferred; the reviewer should judge whether the
  in-process subset alone meets this project's "Done = genuinely
  usable" bar for the row AS SCOPED (not the original full roadmap
  scope, which this plan never claimed to fully deliver).

- [ ] **Step 2: Process findings**

Fix any Critical/Important findings directly (following this project's
established pattern: the controller applies straightforward fixes
itself rather than re-dispatching for trivial issues, re-verifying
independently afterward). Accept Minor/advisory findings as documented
scope decisions unless the user directs otherwise.

- [ ] **Step 3: Update `docs/FEATURES.md`**

Update row `1.7` with accurate status (per Step 1's final
determination) and Notes describing exactly what was and wasn't
implemented, mirroring the honesty and detail level of this project's
existing rows (e.g. `5.3.2`/`5.3.3`, `1.1.2`-`1.4.2`) Notes. Recompute
the Summary table's status counts via `grep -c` (never estimated) for
each status symbol, subtracting for the legend row and the Summary
table's own self-referencing row exactly as done for every prior
sub-project's `FEATURES.md` update in this project. Update the
"Current active work" narrative paragraph at the bottom of the file to
describe this sub-project's completion, avoiding any raw status-emoji
string inside prose (use a plain-English description like "marked
in-progress" instead of literally writing the emoji in running text --
this exact mistake was caught and fixed during a prior sub-project's
`FEATURES.md` update).

Run: `cargo build --workspace` and `cargo test --workspace` one more
time after any Step 2 fixes, to reconfirm a clean state before
committing.

- [ ] **Step 4: Commit**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: update FEATURES.md row 1.7 (in-process market event bus, sub-project #12)"
```

- [ ] **Step 5: Append a `.superpowers/sdd/progress.md` ledger entry**

Follow this project's established ledger format (one entry per task,
plus a final summary entry), then declare this plan's sub-project work
complete.
