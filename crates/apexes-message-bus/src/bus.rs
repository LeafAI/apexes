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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::MockMarketDataProvider;
    use apexes_core::{AssetClass, Instrument, MarketType, Venue};
    use rust_decimal_macros::dec;

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
}
