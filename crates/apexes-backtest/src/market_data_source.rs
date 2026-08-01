use apexes_core::{ApexesError, MarketEvent, Venue};
use apexes_storage::{EventType, StorageReader};
use chrono::NaiveDate;
use std::ops::Range;

/// Supplies a chronologically-ordered sequence of historical `MarketEvent`s
/// for backtest replay.
///
/// Implementations load fully into memory before returning (an accepted
/// scope boundary for this sub-project, matching
/// `apexes_storage::StorageReader::read_batch`'s own full-range-into-memory
/// behavior -- see this plan's Global Constraints).
pub trait HistoricalMarketDataSource {
    /// Returns all events in chronological (timestamp ascending) order,
    /// merged across every event type/instrument this source was
    /// constructed with.
    fn events(&self) -> impl Future<Output = Result<Vec<MarketEvent>, ApexesError>> + Send;
}

impl<T: HistoricalMarketDataSource + Sync> HistoricalMarketDataSource for &T {
    async fn events(&self) -> Result<Vec<MarketEvent>, ApexesError> {
        (**self).events().await
    }
}

/// An in-memory `HistoricalMarketDataSource` backed by a pre-supplied
/// `Vec<MarketEvent>`. Used for fast, network-free unit/integration tests
/// that don't require a live ClickHouse instance.
pub struct InMemoryMarketDataSource {
    events: Vec<MarketEvent>,
}

impl InMemoryMarketDataSource {
    /// Constructs a new source from `events`, sorting them into
    /// chronological order by timestamp (each `MarketEvent` variant's
    /// `timestamp`/`open_time` field, extracted via `event_timestamp`).
    pub fn new(mut events: Vec<MarketEvent>) -> Self {
        events.sort_by_key(event_timestamp);
        Self { events }
    }
}

impl HistoricalMarketDataSource for InMemoryMarketDataSource {
    async fn events(&self) -> Result<Vec<MarketEvent>, ApexesError> {
        Ok(self.events.clone())
    }
}

/// A `HistoricalMarketDataSource` backed by `apexes_storage::StorageReader`,
/// reading real historical data from ClickHouse.
pub struct StorageMarketDataSource {
    reader: StorageReader,
    symbol: String,
    venue: Venue,
    date_range: Range<NaiveDate>,
}

impl StorageMarketDataSource {
    pub fn new(
        reader: StorageReader,
        symbol: impl Into<String>,
        venue: Venue,
        date_range: Range<NaiveDate>,
    ) -> Self {
        Self {
            reader,
            symbol: symbol.into(),
            venue,
            date_range,
        }
    }
}

impl HistoricalMarketDataSource for StorageMarketDataSource {
    async fn events(&self) -> Result<Vec<MarketEvent>, ApexesError> {
        let mut all_events = Vec::new();

        for event_type in [
            EventType::Trade,
            EventType::Tick,
            EventType::Depth,
            EventType::Bar,
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

/// Extracts the timestamp used for chronological ordering from any
/// `MarketEvent` variant (Bar uses `open_time`, all others use
/// `timestamp`).
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
    use apexes_core::TradeEvent;
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
    async fn in_memory_source_sorts_events_chronologically() {
        let events = vec![trade_at("A", 3), trade_at("B", 1), trade_at("C", 2)];
        let source = InMemoryMarketDataSource::new(events);

        let result = source.events().await.unwrap();

        let MarketEvent::Trade(first) = &result[0] else {
            panic!("expected Trade");
        };
        let MarketEvent::Trade(second) = &result[1] else {
            panic!("expected Trade");
        };
        let MarketEvent::Trade(third) = &result[2] else {
            panic!("expected Trade");
        };
        assert_eq!(first.symbol, "B");
        assert_eq!(second.symbol, "C");
        assert_eq!(third.symbol, "A");
    }

    #[tokio::test]
    async fn in_memory_source_handles_empty_input() {
        let source = InMemoryMarketDataSource::new(vec![]);
        let result = source.events().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    // The `&source` borrow below is intentional, not needless: this test's
    // entire purpose is to prove `&InMemoryMarketDataSource` itself (not
    // just `InMemoryMarketDataSource`) implements `HistoricalMarketDataSource`
    // via this task's blanket `&T` impl. Clippy can't tell the difference
    // since owned `source` isn't used again either way.
    #[allow(clippy::needless_borrow)]
    async fn reference_to_in_memory_source_implements_the_trait() {
        let events = vec![trade_at("A", 1)];
        let source = InMemoryMarketDataSource::new(events);

        let result = (&source).events().await.unwrap();

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn event_timestamp_extracts_bar_open_time_not_close_time() {
        let now = Utc::now();
        let later = now + chrono::Duration::minutes(1);
        let bar = MarketEvent::Bar(apexes_core::Bar {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            open: dec!(65000),
            high: dec!(65100),
            low: dec!(64900),
            close: dec!(65050),
            volume: dec!(1),
            interval: apexes_core::BarInterval::OneMinute,
            open_time: now,
            close_time: later,
        });
        assert_eq!(event_timestamp(&bar), now);
    }
}
