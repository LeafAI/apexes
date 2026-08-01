use crate::error::StorageError;
use crate::schema::{
    BARS_TABLE_DDL, BarRow, DEPTH_UPDATES_TABLE_DDL, DepthRow, MARK_PRICES_TABLE_DDL, MarkPriceRow,
    TICKS_TABLE_DDL, TRADES_TABLE_DDL, TickRow, TradeRow,
};
use apexes_core::MarketEvent;
use clickhouse::Client;

/// Writes batches of `MarketEvent`s to ClickHouse. Each `write_batch` call
/// requires a homogeneous batch (all elements the same `MarketEvent`
/// variant) and issues exactly one ClickHouse INSERT -- there is no
/// internal buffering, since ClickHouse inserts are cheap and repeatable
/// (unlike the append-once Parquet format originally considered for this
/// sub-project).
pub struct StorageWriter {
    client: Client,
}

impl StorageWriter {
    /// Connects to ClickHouse at `url` (e.g. "http://localhost:8123") and
    /// ensures all 5 tables exist (CREATE TABLE IF NOT EXISTS).
    pub async fn new(url: &str) -> Result<Self, StorageError> {
        let client = Client::default().with_url(url);

        client
            .query(TRADES_TABLE_DDL)
            .execute()
            .await
            .map_err(|error| StorageError::ConnectionFailed {
                reason: format!("failed to create trades table: {error}"),
            })?;
        client
            .query(TICKS_TABLE_DDL)
            .execute()
            .await
            .map_err(|error| StorageError::ConnectionFailed {
                reason: format!("failed to create ticks table: {error}"),
            })?;
        client
            .query(DEPTH_UPDATES_TABLE_DDL)
            .execute()
            .await
            .map_err(|error| StorageError::ConnectionFailed {
                reason: format!("failed to create depth_updates table: {error}"),
            })?;
        client
            .query(BARS_TABLE_DDL)
            .execute()
            .await
            .map_err(|error| StorageError::ConnectionFailed {
                reason: format!("failed to create bars table: {error}"),
            })?;
        client
            .query(MARK_PRICES_TABLE_DDL)
            .execute()
            .await
            .map_err(|error| StorageError::ConnectionFailed {
                reason: format!("failed to create mark_prices table: {error}"),
            })?;

        Ok(StorageWriter { client })
    }

    /// Writes a homogeneous batch of `MarketEvent`s. Returns
    /// `Err(StorageError::EmptyBatch)` for an empty slice, or
    /// `Err(StorageError::MixedBatch)` if the slice contains more than one
    /// `MarketEvent` variant.
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

    async fn write_trades(&self, events: &[MarketEvent]) -> Result<(), StorageError> {
        let mut insert = self.client.insert::<TradeRow>("trades").await?;
        for event in events {
            let MarketEvent::Trade(trade) = event else {
                // classify_batch already validated homogeneity before this
                // private helper is called; this branch should never be
                // reachable via the public write_batch API, but we still
                // return a proper error rather than panicking, per the
                // project's no-panics-in-production-code convention.
                return Err(StorageError::MixedBatch);
            };
            insert.write(&TradeRow::try_from(trade)?).await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn write_ticks(&self, events: &[MarketEvent]) -> Result<(), StorageError> {
        let mut insert = self.client.insert::<TickRow>("ticks").await?;
        for event in events {
            let MarketEvent::Tick(tick) = event else {
                return Err(StorageError::MixedBatch);
            };
            insert.write(&TickRow::try_from(tick)?).await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn write_depth(&self, events: &[MarketEvent]) -> Result<(), StorageError> {
        let mut insert = self.client.insert::<DepthRow>("depth_updates").await?;
        for event in events {
            let MarketEvent::Depth(update) = event else {
                return Err(StorageError::MixedBatch);
            };
            insert.write(&DepthRow::try_from(update)?).await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn write_bars(&self, events: &[MarketEvent]) -> Result<(), StorageError> {
        let mut insert = self.client.insert::<BarRow>("bars").await?;
        for event in events {
            let MarketEvent::Bar(bar) = event else {
                return Err(StorageError::MixedBatch);
            };
            insert.write(&BarRow::try_from(bar)?).await?;
        }
        insert.end().await?;
        Ok(())
    }

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
}

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

/// Validates that `events` is non-empty and every element is the same
/// `MarketEvent` variant, returning that variant.
fn classify_batch(events: &[MarketEvent]) -> Result<BatchVariant, StorageError> {
    let first_variant = events
        .first()
        .map(variant_of)
        .ok_or(StorageError::EmptyBatch)?;

    if events
        .iter()
        .any(|event| variant_of(event) != first_variant)
    {
        return Err(StorageError::MixedBatch);
    }

    Ok(first_variant)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{Bar, BarInterval, MarkPriceUpdate, Tick, TradeEvent, Venue};
    use chrono::Utc;
    use rust_decimal_macros::dec;

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

    fn sample_tick() -> MarketEvent {
        MarketEvent::Tick(Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(64999),
            bid_size: dec!(1),
            ask_price: dec!(65001),
            ask_size: dec!(1),
            timestamp: Utc::now(),
        })
    }

    fn sample_mark_price() -> MarketEvent {
        MarketEvent::MarkPrice(MarkPriceUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            mark_price: dec!(65012.34),
            timestamp: Utc::now(),
        })
    }

    #[test]
    fn classify_batch_rejects_empty_slice() {
        let result = classify_batch(&[]);
        assert!(matches!(result, Err(StorageError::EmptyBatch)));
    }

    #[test]
    fn classify_batch_rejects_mixed_variants() {
        let events = vec![sample_trade(), sample_tick()];
        let result = classify_batch(&events);
        assert!(matches!(result, Err(StorageError::MixedBatch)));
    }

    #[test]
    fn classify_batch_accepts_homogeneous_trades() {
        let events = vec![sample_trade(), sample_trade()];
        let result = classify_batch(&events);
        assert_eq!(result.unwrap(), BatchVariant::Trade);
    }

    #[test]
    fn classify_batch_accepts_homogeneous_ticks() {
        let events = vec![sample_tick(), sample_tick()];
        let result = classify_batch(&events);
        assert_eq!(result.unwrap(), BatchVariant::Tick);
    }

    #[test]
    fn classify_batch_accepts_homogeneous_mark_prices() {
        let events = vec![sample_mark_price(), sample_mark_price()];
        let result = classify_batch(&events);
        assert_eq!(result.unwrap(), BatchVariant::MarkPrice);
    }

    #[test]
    fn classify_batch_accepts_single_bar() {
        let now = Utc::now();
        let bar = MarketEvent::Bar(Bar {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            open: dec!(1),
            high: dec!(1),
            low: dec!(1),
            close: dec!(1),
            volume: dec!(1),
            interval: BarInterval::OneMinute,
            open_time: now,
            close_time: now,
        });
        let result = classify_batch(&[bar]);
        assert_eq!(result.unwrap(), BatchVariant::Bar);
    }
}
