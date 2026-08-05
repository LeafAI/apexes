//! [`CombinedMarketDataSource`]: merges multiple
//! `apexes_backtest::market_data_source::HistoricalMarketDataSource`
//! instances (one per instrument, in `SessionMode::Backtest` mode --
//! see `main.rs`'s own doc comment) into the single source
//! `apexes_backtest::run_backtest` expects. No existing caller in the
//! workspace combines more than one `StorageMarketDataSource` today
//! (every existing test/caller constructs exactly one), so this is a
//! genuinely new, small combinator rather than a reuse of existing
//! precedent.

use apexes_backtest::market_data_source::HistoricalMarketDataSource;
use apexes_core::{ApexesError, MarketEvent};

/// Combines `sources`' events into one chronologically-sorted
/// sequence. Mirrors `apexes_backtest::market_data_source`'s own
/// private `event_timestamp` helper (duplicated here rather than
/// imported since that function is not `pub` in that crate).
pub struct CombinedMarketDataSource<S> {
    sources: Vec<S>,
}

impl<S> CombinedMarketDataSource<S> {
    pub fn new(sources: Vec<S>) -> Self {
        Self { sources }
    }
}

impl<S: HistoricalMarketDataSource + Send + Sync> HistoricalMarketDataSource
    for CombinedMarketDataSource<S>
{
    async fn events(&self) -> Result<Vec<MarketEvent>, ApexesError> {
        let mut all_events = Vec::new();
        for source in &self.sources {
            all_events.extend(source.events().await?);
        }
        all_events.sort_by_key(event_timestamp);
        Ok(all_events)
    }
}

/// Extracts the timestamp used for chronological ordering from any
/// `MarketEvent` variant (Bar uses `open_time`, all others use
/// `timestamp`) -- identical logic to
/// `apexes_backtest::market_data_source`'s private helper of the same
/// name/shape.
fn event_timestamp(event: &MarketEvent) -> chrono::DateTime<chrono::Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
        MarketEvent::MarkPrice(mark_price) => mark_price.timestamp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_backtest::market_data_source::InMemoryMarketDataSource;
    use apexes_core::{TradeEvent, Venue};
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
    async fn combines_and_sorts_events_from_multiple_sources_chronologically() {
        let source_a = InMemoryMarketDataSource::new(vec![trade_at("BTCUSDT", 3)]);
        let source_b =
            InMemoryMarketDataSource::new(vec![trade_at("ETHUSDT", 1), trade_at("ETHUSDT", 2)]);
        let combined = CombinedMarketDataSource::new(vec![source_a, source_b]);

        let events = combined.events().await.unwrap();

        assert_eq!(events.len(), 3);
        let timestamps: Vec<_> = events.iter().map(event_timestamp).collect();
        let mut sorted = timestamps.clone();
        sorted.sort();
        assert_eq!(timestamps, sorted);
    }

    #[tokio::test]
    async fn empty_sources_list_produces_an_empty_event_list() {
        let combined: CombinedMarketDataSource<InMemoryMarketDataSource> =
            CombinedMarketDataSource::new(vec![]);
        let events = combined.events().await.unwrap();
        assert!(events.is_empty());
    }
}
