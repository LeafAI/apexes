//! [`MarketEventBus`]: an in-process, multi-subscriber broadcast layer
//! over one real `apexes_core::MarketDataProvider`. Deliberately does
//! NOT implement `MarketDataProvider` itself -- see this crate's
//! `lib.rs` doc comment for the full rationale. See
//! `docs/superpowers/specs/2026-08-03-apexes-message-bus-design.md`
//! for the complete design.

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent};
use futures::StreamExt;
use tokio::sync::{Mutex, broadcast};

/// Default `tokio::sync::broadcast` channel capacity used by
/// `MarketEventBus::new` -- callers needing a different capacity (e.g.
/// a very bursty feed with slow consumers) should use
/// `MarketEventBus::new_with_capacity` instead.
const DEFAULT_BROADCAST_CAPACITY: usize = 1024;

enum BusState {
    NotConnected,
    Connected {
        instruments: Vec<Instrument>,
        sender: broadcast::Sender<MarketEvent>,
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

    /// A bucket key identifying one independently-downsampled stream:
    /// symbol, venue, and the `MarketEvent` variant discriminant (so a
    /// `Tick` and a `Trade` for the same symbol/venue are sampled
    /// independently, never overwriting each other's "last event").
    fn downsample_bucket_key(
        event: &MarketEvent,
    ) -> (
        String,
        apexes_core::Venue,
        std::mem::Discriminant<MarketEvent>,
    ) {
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
        interval: std::time::Duration,
    ) -> Result<broadcast::Receiver<MarketEvent>, ApexesError> {
        let mut raw_receiver = self.subscribe(instruments).await?;
        let (downsampled_sender, downsampled_receiver) = broadcast::channel(self.capacity);

        tokio::spawn(async move {
            let mut latest_per_bucket: std::collections::HashMap<
                (
                    String,
                    apexes_core::Venue,
                    std::mem::Discriminant<MarketEvent>,
                ),
                MarketEvent,
            > = std::collections::HashMap::new();
            let mut window_timer = tokio::time::interval(interval);
            window_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // `tokio::time::interval`'s very first `tick()` resolves
            // immediately (by design -- it does not wait a full `interval`
            // before its first completion). Left unconsumed, that would let
            // the very first loop iteration race an immediate premature
            // "window elapsed" against the first incoming event, flushing
            // whatever (if anything) had already landed in
            // `latest_per_bucket` well before a real `interval` has passed.
            // Consuming that first immediate tick up front means the first
            // real window boundary is genuinely `interval` after this task
            // starts, matching the documented per-window semantics.
            window_timer.tick().await;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::MockMarketDataProvider;
    use apexes_core::{AssetClass, Instrument, MarketType, Venue};
    use rust_decimal_macros::dec;

    /// Yields repeatedly to let multi-hop spawned-task chains (e.g.
    /// mock -> `subscribe`'s raw-stream-forwarding task -> broadcast ->
    /// `subscribe_downsampled`'s bucketing task) fully drain before a
    /// test proceeds. A single `yield_now` only guarantees one hop's
    /// worth of scheduling progress, which made downsampling tests
    /// flaky depending on scheduling order (confirmed via repeated
    /// local runs) -- this settles every pending hop deterministically.
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

    #[tokio::test]
    async fn new_does_not_call_subscribe_on_the_wrapped_provider() {
        let mock = MockMarketDataProvider::new();
        let handle = mock.handle();
        let _bus = MarketEventBus::new(Box::new(mock));
        assert_eq!(handle.subscribe_call_count(), 0);
    }

    #[tokio::test]
    async fn new_with_capacity_also_does_not_call_subscribe() {
        let mock = MockMarketDataProvider::new();
        let handle = mock.handle();
        let _bus = MarketEventBus::new_with_capacity(Box::new(mock), 4);
        assert_eq!(handle.subscribe_call_count(), 0);
    }

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
        MarketEvent::Trade(apexes_core::TradeEvent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: chrono::Utc::now(),
        })
    }

    #[tokio::test]
    async fn first_subscribe_calls_the_wrapped_provider_exactly_once() {
        let mock = MockMarketDataProvider::new();
        let handle = mock.handle();
        let bus = MarketEventBus::new(Box::new(mock));
        let instruments = vec![sample_instrument()];

        let _receiver = bus.subscribe(&instruments).await.unwrap();
        assert_eq!(handle.subscribe_call_count(), 1);
    }

    #[tokio::test]
    async fn second_subscribe_with_the_same_instruments_reuses_the_connection() {
        let mock = MockMarketDataProvider::new();
        let handle = mock.handle();
        let bus = MarketEventBus::new(Box::new(mock));
        let instruments = vec![sample_instrument()];

        let _receiver_1 = bus.subscribe(&instruments).await.unwrap();
        let _receiver_2 = bus.subscribe(&instruments).await.unwrap();
        assert_eq!(handle.subscribe_call_count(), 1);
    }

    #[tokio::test]
    async fn second_subscribe_with_a_reordered_same_set_still_reuses_the_connection() {
        let mock = MockMarketDataProvider::new();
        let handle = mock.handle();
        let bus = MarketEventBus::new(Box::new(mock));
        let forward_order = vec![sample_instrument(), sample_instrument_2()];
        let reverse_order = vec![sample_instrument_2(), sample_instrument()];

        let _receiver_1 = bus.subscribe(&forward_order).await.unwrap();
        let _receiver_2 = bus.subscribe(&reverse_order).await.unwrap();
        assert_eq!(handle.subscribe_call_count(), 1);
    }

    #[tokio::test]
    async fn subscribe_with_a_different_instrument_set_after_connection_returns_err() {
        let mock = MockMarketDataProvider::new();
        let bus = MarketEventBus::new(Box::new(mock));
        let first_set = vec![sample_instrument()];
        let second_set = vec![sample_instrument_2()];

        let _receiver = bus.subscribe(&first_set).await.unwrap();
        let result = bus.subscribe(&second_set).await;
        assert!(matches!(
            result,
            Err(ApexesError::SubscriptionFailed { .. })
        ));
    }

    #[tokio::test]
    async fn two_receivers_both_get_the_same_broadcast_event() {
        let mock = MockMarketDataProvider::new();
        let handle = mock.handle();
        let bus = MarketEventBus::new(Box::new(mock));
        let instruments = vec![sample_instrument()];

        let mut receiver_1 = bus.subscribe(&instruments).await.unwrap();
        let mut receiver_2 = bus.subscribe(&instruments).await.unwrap();

        handle.push_event(sample_trade_event("BTCUSDT"));

        let event_1 = receiver_1.recv().await.unwrap();
        let event_2 = receiver_2.recv().await.unwrap();
        assert_eq!(event_1, event_2);
    }

    fn sample_tick_event(symbol: &str, bid_price: rust_decimal::Decimal) -> MarketEvent {
        apexes_core::MarketEvent::Tick(apexes_core::Tick {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            bid_price,
            bid_size: dec!(1),
            ask_price: bid_price + dec!(1),
            ask_size: dec!(1),
            timestamp: chrono::Utc::now(),
        })
    }

    #[tokio::test(start_paused = true)]
    async fn subscribe_downsampled_forwards_only_the_last_event_per_window() {
        let mock = MockMarketDataProvider::new();
        let handle = mock.handle();
        let bus = MarketEventBus::new(Box::new(mock));
        let instruments = vec![sample_instrument()];

        let mut downsampled = bus
            .subscribe_downsampled(&instruments, std::time::Duration::from_secs(1))
            .await
            .unwrap();

        // Three ticks arrive within the same window; only the last
        // (bid_price = dec!(3)) should be forwarded once the window elapses.
        handle.push_event(sample_tick_event("BTCUSDT", dec!(1)));
        settle().await;
        handle.push_event(sample_tick_event("BTCUSDT", dec!(2)));
        settle().await;
        handle.push_event(sample_tick_event("BTCUSDT", dec!(3)));
        settle().await;

        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        settle().await;

        let forwarded = downsampled.recv().await.unwrap();
        match forwarded {
            MarketEvent::Tick(tick) => assert_eq!(tick.bid_price, dec!(3)),
            other => panic!("expected a Tick event, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn subscribe_downsampled_keeps_separate_windows_per_symbol() {
        let mock = MockMarketDataProvider::new();
        let handle = mock.handle();
        let bus = MarketEventBus::new(Box::new(mock));
        let instruments = vec![sample_instrument(), sample_instrument_2()];

        let mut downsampled = bus
            .subscribe_downsampled(&instruments, std::time::Duration::from_secs(1))
            .await
            .unwrap();

        handle.push_event(sample_tick_event("BTCUSDT", dec!(100)));
        settle().await;
        handle.push_event(sample_tick_event("ETHUSDT", dec!(200)));
        settle().await;

        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        settle().await;

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

    #[tokio::test]
    async fn a_slow_receiver_falling_behind_does_not_panic_the_consumer() {
        // A small capacity so it's easy to make a receiver "lag" (fall far
        // enough behind that the broadcast channel drops old messages for
        // it) without needing to send an enormous number of events.
        let mock = MockMarketDataProvider::new();
        let handle = mock.handle();
        let bus = MarketEventBus::new_with_capacity(Box::new(mock), 2);
        let instruments = vec![sample_instrument()];

        let mut slow_receiver = bus.subscribe(&instruments).await.unwrap();

        // Send more events than the channel's capacity while `slow_receiver`
        // never polls -- this guarantees it falls behind.
        for i in 0..10u32 {
            handle.push_event(sample_trade_event(&format!("SYMBOL{i}")));
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
        assert!(
            next.is_ok()
                || matches!(
                    next,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
                )
        );
    }
}
