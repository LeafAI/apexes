//! [`MarketEventBus`]: an in-process, multi-subscriber broadcast layer
//! over one real `apexes_core::MarketDataProvider`. Deliberately does
//! NOT implement `MarketDataProvider` itself -- see this crate's
//! `lib.rs` doc comment for the full rationale. See
//! `docs/superpowers/specs/2026-08-03-apexes-message-bus-design.md`
//! for the complete design.

use apexes_core::{Instrument, MarketDataProvider};
use tokio::sync::Mutex;

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

#[allow(dead_code)] // fields read starting in Task 2's `subscribe` impl
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

    #[allow(dead_code)] // used starting in Task 2's tests
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
}
