# Sub-project #12: In-Process Market Event Bus

**Date:** 2026-08-03
**Sub-project:** #12 (Message bus)
**Status:** Draft for review

## 1. Scope

Per the roadmap (`docs/superpowers/specs/2026-07-28-apexes-vision-and-roadmap.md`
§2.3/§3 row 12), sub-project #12 originally covered introducing NATS
JetStream or a self-built lock-free ring buffer for multi-subscriber
market data broadcast, once storage-layer scale bottlenecks were hit.
The storage layer itself was already pulled forward into sub-project
#2 (ClickHouse), so #12's remaining scope is purely the pub/sub layer:
letting multiple independent consumers (storage ingestion, live
trading, paper trading, backtesting) share one upstream exchange
connection instead of each independently calling
`MarketDataProvider::subscribe()` and opening a separate connection.

This spec covers `docs/FEATURES.md` row `1.7` ("Market data
distribution bus (multi-subscriber broadcast, on-demand
downsampling)"). Per explicit user direction, this first pass
implements **in-process broadcast only** (a single Rust process with
multiple async consumers); a genuine inter-process message-bus
(NATS JetStream or similar) is an explicitly deferred follow-up, not
in this spec's scope.

## 2. Current state

Three existing `MarketDataProvider` consumers call
`provider.subscribe(instruments)` independently against a *live* feed:
`apexes-market-data::run_ingestion_loop` (ClickHouse storage),
`apexes-paper::run_paper_trading`, and
`apexes-live-trading::run_live_trading`'s `subscribe_all_venues`.
(`apexes-backtest::run_backtest` is not one of these -- it replays a
pre-loaded historical `Vec<MarketEvent>` from ClickHouse storage, not
a live `MarketDataProvider` stream, so it is out of scope for a
live-connection-sharing bus; `run_paper_trading` and
`run_ingestion_loop`/`run_live_trading` share the underlying
`apexes_backtest::event_loop::run_event_loop` core for their matching
logic, but each still independently calls `subscribe()` for its own
live market data.) Running two of the three live-feed consumers
against the same venue/instrument set today means two separate
upstream connections (e.g. two WebSocket connections to the same
exchange for the same symbols). There is no shared layer.

## 3. Architecture

### 3.1 New crate: `apexes-message-bus`

- **`MarketEventBus`**: wraps one real `Box<dyn MarketDataProvider>`.
  Does **not** implement `MarketDataProvider` itself -- it is a
  distinct type with its own `subscribe`/`subscribe_downsampled`
  methods, so callers must explicitly opt in by constructing and using
  a `MarketEventBus` rather than accidentally getting broadcast
  semantics through the existing trait.
- **Lazy connection**: `MarketEventBus::new(provider)` does no network
  I/O. Internally holds `tokio::sync::Mutex<BusState>`, where
  `BusState` is either `NotConnected` or `Connected { instruments:
  Vec<Instrument>, sender: broadcast::Sender<MarketEvent> }`. The
  first call to `subscribe(instruments)` transitions
  `NotConnected -> Connected`: it calls the wrapped provider's real
  `subscribe(instruments)`, creates a `tokio::sync::broadcast::channel`
  (bounded capacity, a `MarketEventBus::new_with_capacity` constructor
  lets callers override the default), and spawns a background task
  that reads the real provider's stream and forwards every event into
  the broadcast sender. Every subsequent `subscribe()` call (from any
  caller, with the *same* instrument set) simply calls
  `sender.subscribe()` to get a fresh `broadcast::Receiver` -- no new
  upstream connection.
- **Fixed subscription scope**: the instrument set is fixed by the
  first `subscribe()` call. A later `subscribe()` call with a
  *different* instrument set (checked via `Vec<Instrument>` equality,
  order-independent) returns an explicit
  `Err(ApexesError::SubscriptionFailed { reason: "MarketEventBus is
  already connected with a different, fixed instrument set", .. })`
  rather than silently subscribing to a different symbol set than the
  caller asked for, or silently ignoring the mismatch. Dynamically
  growing/shrinking a live bus's instrument set is explicitly out of
  scope for this pass.
- **`subscribe_downsampled(interval: Duration)`**: calls `subscribe()`
  internally to get a raw `broadcast::Receiver<MarketEvent>`, then
  spawns a background task that buckets incoming events by `(symbol,
  venue, discriminant of the MarketEvent variant)` and, once per
  `interval` per bucket, forwards only the *last* event received in
  that window to a second, freshly created `broadcast::Sender` (whose
  `Receiver` is returned to the caller). This is sampling, not
  aggregation -- unlike `apexes-market-data::BarAggregator` (which
  aggregates `Trade` events into synthetic OHLCV `Bar`s), this simply
  thins out the event rate for any `MarketEvent` variant by keeping
  only the most recent update per window. `BarAggregator` remains the
  right tool when true OHLCV aggregation is needed; it is unaffected
  by this change and callers needing bars should keep using it
  directly (optionally on top of an un-downsampled `MarketEventBus`
  subscription).
- **Lagged-receiver handling**: `broadcast::Receiver::recv()` returning
  `Err(RecvError::Lagged(n))` (a slow consumer that fell behind and had
  events dropped) is caught, logged via `tracing::warn!` with the lag
  count, and treated as non-fatal -- the consumer's stream continues
  from the next available event, it does not panic or silently stop.
  `Err(RecvError::Closed)` (the sender/upstream task ended) ends the
  consumer's stream cleanly.

### 3.2 Interfaces

```rust
pub struct MarketEventBus { /* .. */ }

impl MarketEventBus {
    pub fn new(provider: Box<dyn MarketDataProvider>) -> Self;
    pub fn new_with_capacity(provider: Box<dyn MarketDataProvider>, capacity: usize) -> Self;

    /// First call with a given `instruments` set establishes the real
    /// upstream connection; later calls with the SAME set (order-independent
    /// equality) return a fresh receiver over the same connection. A call
    /// with a DIFFERENT set after the bus is already connected returns
    /// `Err(ApexesError::SubscriptionFailed { .. })`.
    pub async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<broadcast::Receiver<MarketEvent>, ApexesError>;

    /// Downsamples an underlying `subscribe()` stream: for each
    /// (symbol, venue, MarketEvent variant) bucket, forwards only the
    /// most recent event received within each `interval`-long window.
    pub async fn subscribe_downsampled(
        &self,
        instruments: &[Instrument],
        interval: Duration,
    ) -> Result<broadcast::Receiver<MarketEvent>, ApexesError>;
}
```

No new `ApexesError` variant is needed -- the existing
`SubscriptionFailed { symbol, venue, reason, source_detail }` is
already generic enough to describe both a real connection failure
(propagated from the wrapped provider) and the fixed-instrument-set
mismatch case (using a sentinel `symbol`/`venue` describing the
mismatch in `reason`, since the mismatch isn't about one specific
symbol/venue).

## 4. Testing strategy

- Unit tests using a hand-written mock `MarketDataProvider`
  implementation (an in-memory stream of synthetic `MarketEvent`s,
  following the same test-double pattern already used elsewhere in
  this workspace, e.g. `apexes-oms`'s `MockExecutionGateway`) to
  verify: lazy connection (no calls to the mock before the first
  `subscribe()`), connection reuse (multiple `subscribe()` calls with
  the same instrument set result in exactly one call to the mock's
  `subscribe()`), the fixed-instrument-set-mismatch error path, basic
  broadcast fan-out (two receivers both get the same events), and
  `subscribe_downsampled`'s per-window last-event-wins semantics using
  a virtual-time-controlled test (`tokio::time::pause()` /
  `tokio::time::advance()`, matching this workspace's existing
  event-loop test conventions).
- Lagged-receiver handling is tested by constructing a
  `MarketEventBus` with a small capacity, having one receiver
  deliberately fall behind (not polling while many events are sent),
  and asserting the slow receiver's next `recv()` surfaces as a
  logged-and-continued condition rather than a panic or a silently
  stuck stream.

## 5. Documentation honesty

`docs/FEATURES.md` row `1.7`'s Notes will describe this as
in-process-only broadcast with downsampling, explicitly noting that
genuine inter-process/multi-machine message-bus support (NATS
JetStream or similar) remains unimplemented and deferred -- this spec
does not claim to deliver the full original roadmap scope for row
`1.7`, only the in-process subset explicitly agreed with the user.
