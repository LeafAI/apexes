//! [`Gateway`]: holds ONE real upstream `MarketDataProvider` connection
//! and broadcasts every event to N connected WebSocket clients over
//! `/market`. Deliberately NOT built on `apexes_message_bus::MarketEventBus`
//! (an in-process-only broadcast wrapper) -- see this crate's own
//! `lib.rs` docs and the design spec for the full rationale.

use crate::book_cache::BookCache;
use crate::persistence::PersistenceBuffer;
use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent};
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};

/// Default `tokio::sync::broadcast` channel capacity used by
/// `Gateway::new` when a caller doesn't have a specific capacity in
/// mind -- matches `apexes_message_bus::MarketEventBus`'s own default
/// for consistency across this project's broadcast-based components.
const DEFAULT_BROADCAST_CAPACITY: usize = 1024;

/// Whether `connect` has been called yet on this `Gateway` -- guards
/// the "exactly once, at process startup" constraint documented on
/// `connect` itself.
enum ConnectState {
    NotConnected,
    Connected,
}

/// Holds one real upstream `MarketDataProvider` connection and
/// broadcasts every event it produces to N subscribers via a
/// `tokio::sync::broadcast::Sender`. A later task's `axum` `/market`
/// WS handler calls `subscribe()` per incoming client connection and
/// forwards each received event as a WS frame (see `protocol::Frame`).
pub struct Gateway {
    provider: Box<dyn MarketDataProvider>,
    sender: broadcast::Sender<MarketEvent>,
    state: Mutex<ConnectState>,
    books: Arc<Mutex<BookCache>>,
    /// `None` until/unless `attach_persistence` is called. Shared via
    /// `Arc` (not just `Mutex`) because `connect`'s forwarding loop
    /// runs in its own spawned task that outlives the `&self` borrow
    /// of this method, so it needs an owned, independently-lockable
    /// handle onto this same state -- and `Arc<Mutex<..>>` also lets a
    /// separate periodic task (`main.rs`, Task 5) call
    /// `flush_persistence_aged` concurrently with the forwarding
    /// loop's per-event `record` calls without racing.
    persistence: Arc<Mutex<Option<PersistenceBuffer>>>,
}

impl Gateway {
    /// Constructs a `Gateway` wrapping `provider`, with a
    /// `DEFAULT_BROADCAST_CAPACITY`-sized broadcast channel and no
    /// persistence attached (see `attach_persistence`).
    pub fn new(provider: Box<dyn MarketDataProvider>, capacity: usize) -> Self {
        let capacity = if capacity == 0 {
            DEFAULT_BROADCAST_CAPACITY
        } else {
            capacity
        };
        let (sender, _receiver) = broadcast::channel(capacity);
        Self {
            provider,
            sender,
            state: Mutex::new(ConnectState::NotConnected),
            books: Arc::new(Mutex::new(BookCache::default())),
            persistence: Arc::new(Mutex::new(None)),
        }
    }

    /// Attaches `buffer` as this `Gateway`'s persistence sink. Can be
    /// called before or after `connect` -- the forwarding loop reads
    /// this shared, lockable slot fresh on every event rather than
    /// snapshotting it once at `connect` time, so persistence starts
    /// applying to subsequent events as soon as this call completes,
    /// regardless of ordering relative to `connect`. Calling this a
    /// second time replaces the previous buffer (its own unflushed
    /// contents are dropped, matching this crate's established
    /// "monitoring availability over historical completeness"
    /// convention -- see `persistence.rs`'s docs).
    pub async fn attach_persistence(&self, buffer: PersistenceBuffer) {
        *self.persistence.lock().await = Some(buffer);
    }

    /// Runs the age-based flush pass (see
    /// `PersistenceBuffer::flush_aged`) on the currently attached
    /// persistence buffer, if any. A no-op if `attach_persistence` was
    /// never called. Intended to be called periodically (e.g. from a
    /// `tokio::time::interval` loop in `main.rs`, Task 5).
    pub async fn flush_persistence_aged(&self) {
        if let Some(buffer) = self.persistence.lock().await.as_mut() {
            buffer.flush_aged().await;
        }
    }

    /// Subscribes to `instruments` from the real upstream provider ONCE
    /// (called exactly once, at process startup by `main.rs`, a later
    /// task) -- this crate does not support subscribing to a second,
    /// different instrument set later in the same process lifetime
    /// (matching `MarketEventBus`'s own fixed-instrument-set-per-
    /// connection constraint, for the same underlying reason: one
    /// upstream connection, one fixed subscription).
    ///
    /// Calling `connect` a second time (on an already-connected
    /// `Gateway`, regardless of whether `instruments` matches the
    /// first call) returns `Err(ApexesError::SubscriptionFailed { .. })`
    /// rather than silently reusing or replacing the existing upstream
    /// connection -- unlike `MarketEventBus::subscribe`, which lazily
    /// connects on first call and transparently reuses the connection
    /// for a matching instrument set on every later call, `connect`
    /// here is an explicit, one-shot startup action with no lazy path,
    /// so any second call is treated as caller error.
    ///
    /// The forwarding loop below (upstream provider stream ->
    /// `self.sender.send(event)`) is structurally a *sender*-side
    /// operation on the broadcast channel and therefore can never
    /// observe `broadcast::error::RecvError::Lagged` -- that error is
    /// only produced on the *receiver* side, when a slow consumer
    /// falls behind and the ring buffer wraps underneath it. That
    /// receiver-side handling is the responsibility of whatever holds
    /// a `broadcast::Receiver` returned by `subscribe()` below (a
    /// later task's per-client `/market` WS forwarding loop) -- not
    /// this method. A `send` error here means every receiver has been
    /// dropped (no clients currently connected); that is not a failure
    /// worth logging, matching `MarketEventBus::subscribe`'s own
    /// established convention for the identical situation.
    pub async fn connect(&self, instruments: &[Instrument]) -> Result<(), ApexesError> {
        let mut state = self.state.lock().await;
        if matches!(*state, ConnectState::Connected) {
            return Err(ApexesError::SubscriptionFailed {
                symbol: instruments
                    .first()
                    .map(|i| i.symbol.clone())
                    .unwrap_or_else(|| "*".to_string()),
                venue: instruments
                    .first()
                    .map(|i| i.venue)
                    .unwrap_or(apexes_core::Venue::Binance),
                reason: "Gateway::connect was already called once on this Gateway -- a single \
                         Gateway instance supports exactly one upstream connection for the \
                         duration of the process, established exactly once at startup"
                    .to_string(),
                source_detail: None,
            });
        }

        let mut stream = self.provider.subscribe(instruments).await?;
        let forward_sender = self.sender.clone();
        let persistence = Arc::clone(&self.persistence);
        let books = Arc::clone(&self.books);
        tokio::spawn(async move {
            while let Some(event) = stream.next().await {
                // Buffering into `persistence` only appends to an
                // in-memory `Vec` and, at most, `tokio::spawn`s a
                // flush task -- it never itself awaits a ClickHouse
                // network round-trip, so this never slows down the
                // broadcast send below.
                if let Some(buffer) = persistence.lock().await.as_mut() {
                    buffer.record(&event).await;
                }
                // A `send` error here means every receiver has been
                // dropped -- nothing left to forward to, not a
                // failure worth logging.
                // The cache update and broadcast share the same critical
                // section as subscribing with a snapshot. No network I/O
                // takes place under this lock.
                let mut cache = books.lock().await;
                if let MarketEvent::Depth(update) = &event
                    && !cache.apply(update)
                {
                    tracing::warn!(symbol = %update.symbol, "discarding depth delta without a valid base or with an old sequence");
                    continue;
                }
                let _ = forward_sender.send(event);
            }
            books.lock().await.clear();
            tracing::info!("Gateway: upstream provider stream ended, forwarding task exiting");
        });
        *state = ConnectState::Connected;
        Ok(())
    }

    /// Returns a fresh `broadcast::Receiver` for a newly-connecting WS
    /// client (a later task's `axum` handler calls this per
    /// connection). Can be called before `connect` -- the returned
    /// receiver simply sees no events until `connect` starts
    /// forwarding.
    /// Atomic snapshot/subscribe boundary: the receiver begins strictly
    /// after the cached book state. Preserve original exchange timestamps.
    pub(crate) async fn subscribe_with_snapshots(
        &self,
    ) -> (Vec<MarketEvent>, broadcast::Receiver<MarketEvent>) {
        let books = self.books.lock().await;
        (books.snapshots(), self.sender.subscribe())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MarketEvent> {
        self.sender.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, Instrument, MarketType, Venue};
    use apexes_message_bus::test_support::MockMarketDataProvider;
    use rust_decimal_macros::dec;

    /// Yields repeatedly to let the spawned forwarding task (mock's
    /// internal channel -> `connect`'s task -> broadcast) fully drain
    /// before a test asserts on what subscribers received. Mirrors
    /// `apexes_message_bus::bus`'s own test helper of the same shape
    /// and for the same reason (a single `yield_now` only guarantees
    /// one hop's worth of scheduling progress).
    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
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

    fn sample_tick_event() -> MarketEvent {
        MarketEvent::Tick(apexes_core::Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(64999.5),
            bid_size: dec!(0.8),
            ask_price: dec!(65000.5),
            ask_size: dec!(1.2),
            timestamp: chrono::Utc::now(),
        })
    }

    #[tokio::test]
    async fn subscribe_returns_a_receiver_that_sees_broadcast_events() {
        let mock = MockMarketDataProvider::new();
        let handle = mock.handle();
        let gateway = Gateway::new(Box::new(mock), 16);

        let mut receiver = gateway.subscribe();

        gateway.connect(&[sample_instrument()]).await.unwrap();
        settle().await;

        let event = sample_tick_event();
        handle.push_event(event.clone());
        settle().await;

        let received = receiver.recv().await.unwrap();
        assert_eq!(received, event);
    }

    #[tokio::test]
    async fn multiple_subscribers_all_see_the_same_events() {
        // Proves the core "one upstream connection, many downstream
        // consumers" property this crate exists to provide: a single
        // `push_event` on the mock's ONE upstream stream must be
        // observed, in full and identically, by every independent
        // subscriber -- not just that `subscribe()` hands back
        // distinct receiver handles.
        let mock = MockMarketDataProvider::new();
        let handle = mock.handle();
        let gateway = Gateway::new(Box::new(mock), 16);

        let mut subscriber_a = gateway.subscribe();
        let mut subscriber_b = gateway.subscribe();
        let mut subscriber_c = gateway.subscribe();

        gateway.connect(&[sample_instrument()]).await.unwrap();
        settle().await;

        assert_eq!(handle.subscribe_call_count(), 1);

        let event = sample_tick_event();
        handle.push_event(event.clone());
        settle().await;

        for receiver in [&mut subscriber_a, &mut subscriber_b, &mut subscriber_c] {
            let received = receiver.recv().await.unwrap();
            assert_eq!(received, event);
        }
    }

    #[tokio::test]
    async fn connect_can_only_be_called_once_per_instrument_set() {
        let mock = MockMarketDataProvider::new();
        let handle = mock.handle();
        let gateway = Gateway::new(Box::new(mock), 16);

        gateway.connect(&[sample_instrument()]).await.unwrap();
        settle().await;
        assert_eq!(handle.subscribe_call_count(), 1);

        // A second `connect` call, even with the identical instrument
        // set, is rejected -- `Gateway::connect` is a one-shot startup
        // action with no lazy-reconnect/reuse path (unlike
        // `MarketEventBus::subscribe`).
        let second_call = gateway.connect(&[sample_instrument()]).await;
        assert!(second_call.is_err());
        assert_eq!(
            handle.subscribe_call_count(),
            1,
            "a rejected second connect() must not touch the upstream provider again"
        );
    }
    #[tokio::test]
    async fn snapshot_subscription_has_no_gap_or_duplicate_delta() {
        use apexes_core::{DepthUpdate, PriceLevel};
        let mock = MockMarketDataProvider::new();
        let handle = mock.handle();
        let gateway = Gateway::new(Box::new(mock), 16);
        gateway.connect(&[sample_instrument()]).await.unwrap();
        let mut witness = gateway.subscribe();
        let depth = |sequence, is_snapshot, quantity| {
            MarketEvent::Depth(DepthUpdate {
                symbol: "BTCUSDT".into(),
                venue: Venue::Binance,
                sequence: Some(sequence),
                is_snapshot,
                bids: vec![PriceLevel {
                    price: dec!(100),
                    quantity,
                }],
                asks: vec![PriceLevel {
                    price: dec!(101),
                    quantity: dec!(1),
                }],
                timestamp: chrono::Utc::now(),
            })
        };
        for event in [depth(10, true, dec!(1)), depth(15, false, dec!(2))] {
            handle.push_event(event.clone());
            assert_eq!(witness.recv().await.unwrap(), event);
        }
        let (snapshots, mut subscriber) = gateway.subscribe_with_snapshots().await;
        assert_eq!(snapshots.len(), 1);
        let MarketEvent::Depth(snapshot) = &snapshots[0] else {
            panic!("expected snapshot")
        };
        assert!(snapshot.is_snapshot);
        assert_eq!(snapshot.sequence, Some(15));
        assert_eq!(snapshot.bids[0].quantity, dec!(2));
        let next = depth(20, false, dec!(3));
        handle.push_event(next.clone());
        assert_eq!(subscriber.recv().await.unwrap(), next);
    }
}
