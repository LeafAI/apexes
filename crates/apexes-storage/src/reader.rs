use crate::error::StorageError;
use crate::schema::{BarRow, DepthRow, TickRow, TradeRow};
use apexes_core::{MarketEvent, Venue};
use chrono::NaiveDate;
use clickhouse::Client;
use std::ops::Range;

/// Which `MarketEvent` variant (and therefore which ClickHouse table) to
/// query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Trade,
    Tick,
    Depth,
    Bar,
}

/// Reads `MarketEvent`s back from ClickHouse.
pub struct StorageReader {
    client: Client,
}

impl StorageReader {
    pub async fn new(url: &str) -> Result<Self, StorageError> {
        Ok(StorageReader {
            client: Client::default().with_url(url),
        })
    }

    /// Reads all events for `symbol`/`venue` within `date_range` (UTC day
    /// boundaries, exclusive end) from the table matching `event_type`,
    /// returning them as `MarketEvent`s. Uses the tables' `ORDER BY
    /// (venue, symbol, timestamp)` (or `open_time` for bars) sort key for
    /// an efficient range scan.
    pub async fn read_batch(
        &self,
        symbol: &str,
        venue: Venue,
        date_range: Range<NaiveDate>,
        event_type: EventType,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let venue_str = format!("{venue:?}");
        let start = date_range.start.and_hms_opt(0, 0, 0).ok_or_else(|| StorageError::QueryFailed {
            reason: "invalid range start date".to_string(),
        })?;
        let end = date_range.end.and_hms_opt(0, 0, 0).ok_or_else(|| StorageError::QueryFailed {
            reason: "invalid range end date".to_string(),
        })?;

        match event_type {
            EventType::Trade => self.read_trades(symbol, &venue_str, start, end).await,
            EventType::Tick => self.read_ticks(symbol, &venue_str, start, end).await,
            EventType::Depth => self.read_depth(symbol, &venue_str, start, end).await,
            EventType::Bar => self.read_bars(symbol, &venue_str, start, end).await,
        }
    }

    async fn read_trades(
        &self,
        symbol: &str,
        venue: &str,
        start: chrono::NaiveDateTime,
        end: chrono::NaiveDateTime,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM trades WHERE venue = ? AND symbol = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp",
            )
            .bind(venue)
            .bind(symbol)
            .bind(start.and_utc())
            .bind(end.and_utc())
            .fetch_all::<TradeRow>()
            .await?;

        rows.into_iter()
            .map(|row| Ok(MarketEvent::Trade(row.try_into()?)))
            .collect()
    }

    async fn read_ticks(
        &self,
        symbol: &str,
        venue: &str,
        start: chrono::NaiveDateTime,
        end: chrono::NaiveDateTime,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM ticks WHERE venue = ? AND symbol = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp",
            )
            .bind(venue)
            .bind(symbol)
            .bind(start.and_utc())
            .bind(end.and_utc())
            .fetch_all::<TickRow>()
            .await?;

        rows.into_iter()
            .map(|row| Ok(MarketEvent::Tick(row.try_into()?)))
            .collect()
    }

    async fn read_depth(
        &self,
        symbol: &str,
        venue: &str,
        start: chrono::NaiveDateTime,
        end: chrono::NaiveDateTime,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM depth_updates WHERE venue = ? AND symbol = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp",
            )
            .bind(venue)
            .bind(symbol)
            .bind(start.and_utc())
            .bind(end.and_utc())
            .fetch_all::<DepthRow>()
            .await?;

        rows.into_iter()
            .map(|row| Ok(MarketEvent::Depth(row.try_into()?)))
            .collect()
    }

    async fn read_bars(
        &self,
        symbol: &str,
        venue: &str,
        start: chrono::NaiveDateTime,
        end: chrono::NaiveDateTime,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM bars WHERE venue = ? AND symbol = ? AND open_time >= ? AND open_time < ? ORDER BY open_time",
            )
            .bind(venue)
            .bind(symbol)
            .bind(start.and_utc())
            .bind(end.and_utc())
            .fetch_all::<BarRow>()
            .await?;

        rows.into_iter()
            .map(|row| Ok(MarketEvent::Bar(row.try_into()?)))
            .collect()
    }
}
