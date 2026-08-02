# Sub-project #12 (continued): Wiring `MarketEventBus` into the Three Real Consumers

**Date:** 2026-08-03
**Sub-project:** #12 (Message bus), follow-up to `2026-08-03-apexes-message-bus-design.md`
**Status:** Draft for review

## 1. Scope

The prior spec (`docs/superpowers/specs/2026-08-03-apexes-message-bus-design.md`)
delivered `apexes-message-bus::MarketEventBus` -- a fully working,
tested, in-process multi-subscriber broadcast layer over one real
`apexes_core::MarketDataProvider`. Its final whole-branch review found
that none of this project's three real `MarketDataProvider` consumers
had actually been wired to use it, so the connection-sharing benefit
it exists to provide was not yet realized anywhere. This spec closes
that gap: wiring `MarketEventBus` into
`apexes-market-data::run_ingestion_loop`,
`apexes-paper::run_paper_trading`, and
`apexes-live-trading::run_live_trading`'s `subscribe_all_venues`.

### 1.1 A note on current real-world impact

No production entrypoint in this workspace today calls two or more of
these three functions in the same process at the same time --
`apexes-cli` is a skeleton with no real command wiring yet, and each
function is currently only exercised by its own crate's tests. The
"two independent WebSocket connections to the same venue" scenario
this bus exists to prevent therefore does not concretely happen
anywhere in the codebase as of this spec. Per explicit user direction,
this integration proceeds anyway, as forward-looking infrastructure
for when a real multi-consumer entrypoint exists (e.g. a future
`apexes-cli` command that starts ingestion and live trading together) --
this is a documented, deliberate choice, not an oversight.

## 2. Architecture

### 2.1 Signature changes (direct parameter type swap)

All three functions change their market-data-source parameter's type
from `&dyn MarketDataProvider` to `&MarketEventBus`. Callers are
responsible for constructing the `MarketEventBus` (wrapping whatever
real `MarketDataProvider` they would previously have passed directly)
and passing a reference to it in. This is a breaking signature change
to all three functions -- every existing call site must be updated to
construct a `MarketEventBus::new(Box::new(provider))` and pass `&bus`
instead of `&provider`, specifically:

- `apexes-market-data/src/ingestion.rs`'s own `#[cfg(test)]` tests and
  `apexes-market-data/tests/live_ingestion.rs`.
- `apexes-paper/src/runner.rs`'s own `#[cfg(test)]` tests and
  `apexes-paper/tests/live_binance.rs`.
- `apexes-live-trading/src/runner.rs`'s own `#[cfg(test)]` tests and
  `apexes-live-trading/tests/live_trading_integration.rs` (this file's
  `register::<Arc<dyn MarketDataProvider>, Venue>(Venue::Binance,
  provider)` call site, confirmed via inspection to be the only
  provider-registration call site in this crate today, becomes
  `register::<Arc<MarketEventBus>, Venue>(Venue::Binance,
  Arc::new(MarketEventBus::new(Box::new(provider))))`).

- **`apexes-market-data::run_ingestion_loop`**: `provider: &dyn
  MarketDataProvider` becomes `bus: &MarketEventBus`. Internally,
  `provider.subscribe(instruments).await?` becomes
  `bus.subscribe(instruments).await?`, and the resulting
  `broadcast::Receiver<MarketEvent>` is adapted into a
  `Stream<Item = MarketEvent>` (see §2.2) before being read the same
  way the function's existing `tokio::select!` loop already reads
  a stream via `.next()`.
- **`apexes-paper::run_paper_trading`**: `provider: &dyn
  MarketDataProvider` becomes `bus: &MarketEventBus`. The resulting
  adapted stream is passed to `apexes_backtest::event_loop::
  run_event_loop` exactly where the raw `BoxStream` from
  `provider.subscribe()` is passed today -- `run_event_loop`'s own
  signature (`events: impl Stream<Item = MarketEvent> + Unpin`) does
  not need to change, since the adapter in §2.2 produces exactly that
  bound.
- **`apexes-live-trading`'s `subscribe_all_venues`**: the
  `apexes_core::registry::Registry` lookup changes from
  `registry.get::<Arc<dyn MarketDataProvider>, Venue>()` to
  `registry.get::<Arc<MarketEventBus>, Venue>()`. Each venue's real
  provider must now be registered wrapped in its own `MarketEventBus`
  (one bus instance per venue, matching the existing one-provider-per-venue
  registration granularity) rather than registered directly. The
  function's internal loop changes from `provider.subscribe(&venue_instruments)`
  to `bus.subscribe(&venue_instruments).await?`, each result adapted
  (§2.2) before being pushed into the same `futures::stream::select_all`
  the function already builds.

### 2.2 The `broadcast::Receiver` -> `Stream<Item = MarketEvent>` adapter

A single shared helper function, placed in `apexes-message-bus` itself
(so all three consumer crates depend on one canonical implementation
rather than three near-duplicate copies):

```rust
pub fn receiver_into_stream(
    receiver: broadcast::Receiver<MarketEvent>,
) -> impl Stream<Item = MarketEvent> + Unpin
```

Internally: wraps `receiver` in `tokio_stream::wrappers::BroadcastStream`
(confirmed during design research to correctly adapt a `broadcast::Receiver`
into a `Stream`, ending cleanly once the sender side is dropped) and
applies `.filter_map(..)`, unwrapping `Ok(event) -> Some(event)` and,
on `Err(BroadcastStreamRecvError::Lagged(n))`, logging
`tracing::warn!` with the lag count and yielding `None` (skip this
item, do not end the stream) -- matching `MarketEventBus`'s own
already-established lagged-receiver handling convention (log and
continue, never panic, never silently stop). This is not a new
policy decision; it is applying the existing one consistently at
every consumption point.

## 3. Testing strategy

For each of the three consumer functions: update every existing test
that previously constructed a raw mock `MarketDataProvider` and passed
it directly to instead wrap that same mock in
`MarketEventBus::new(Box::new(mock))` and pass a reference to the bus.
The tests' actual assertions (on `BacktestReport`, on written
ClickHouse batches, etc.) do not need to change -- only the
construction/wiring at the top of each test. Add one new test per
consumer function verifying the `MarketEventBus`-mediated path
specifically: constructing a bus, calling the consumer function twice
concurrently (or as close to concurrently as a deterministic test
allows) against the same bus and instrument set, and asserting the
underlying mock provider's `subscribe_call_count()` (from
`apexes-message-bus::test_support::MockMarketDataProvider`, already
built and reusable per its own crate's doc comment) stays at `1` --
i.e. genuinely proving connection sharing occurs through each real
consumer's own code path, not just through `MarketEventBus` in
isolation.

## 4. Error handling

No new error variants. `MarketEventBus::subscribe`'s existing
`Result<_, ApexesError>` (already `SubscriptionFailed` on both a real
connection failure and the fixed-instrument-set-mismatch case)
propagates through each consumer function's existing `?`-based error
handling exactly as `provider.subscribe()`'s `Result` does today --
no consumer function's own error-handling logic changes, only the
type of the value being unwrapped/propagated.

## 5. Documentation honesty

Once this integration lands, `docs/FEATURES.md` row `1.7`'s Notes will
be updated to record that all three real `MarketDataProvider`
consumers are now wired to `MarketEventBus`, removing the "not yet
realized in production" caveat from the prior update -- but explicitly
retaining the honest caveat from §1.1 above: no current production
entrypoint actually exercises two consumers concurrently against the
same bus, so the connection-sharing benefit remains only
structurally/testably proven, not yet observed in a real running
system. Whether row `1.7` should move from `In Progress` to `Done`
after this integration is a judgment call for this work's own final
review, following the same honesty standard as every prior
`docs/FEATURES.md` update in this project.
