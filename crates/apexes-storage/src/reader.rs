use crate::error::StorageError;
use crate::schema::{BarRow, DepthRow, TickRow, TradeRow};
use apexes_core::{MarketEvent, Venue};
use chrono::{DateTime, NaiveDate, Utc};
use clickhouse::Client;
use std::ops::Range;

/// ClickHouse "basic" DateTime output format; binding this string (rather
/// than a raw `DateTime<Utc>`, which the crate serializes as RFC3339 and
/// ClickHouse rejects with `TYPE_MISMATCH` against `DateTime64(3, 'UTC')`)
/// is the format ClickHouse accepts for DateTime64 comparisons.
const CLICKHOUSE_DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f";

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
        let start = day_boundary(date_range.start)?;
        let end = day_boundary(date_range.end)?;

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
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM trades WHERE venue = ? AND symbol = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp",
            )
            .bind(venue)
            .bind(symbol)
            .bind(start.format(CLICKHOUSE_DATETIME_FORMAT).to_string())
            .bind(end.format(CLICKHOUSE_DATETIME_FORMAT).to_string())
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
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM ticks WHERE venue = ? AND symbol = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp",
            )
            .bind(venue)
            .bind(symbol)
            .bind(start.format(CLICKHOUSE_DATETIME_FORMAT).to_string())
            .bind(end.format(CLICKHOUSE_DATETIME_FORMAT).to_string())
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
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM depth_updates WHERE venue = ? AND symbol = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp",
            )
            .bind(venue)
            .bind(symbol)
            .bind(start.format(CLICKHOUSE_DATETIME_FORMAT).to_string())
            .bind(end.format(CLICKHOUSE_DATETIME_FORMAT).to_string())
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
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM bars WHERE venue = ? AND symbol = ? AND open_time >= ? AND open_time < ? ORDER BY open_time",
            )
            .bind(venue)
            .bind(symbol)
            .bind(start.format(CLICKHOUSE_DATETIME_FORMAT).to_string())
            .bind(end.format(CLICKHOUSE_DATETIME_FORMAT).to_string())
            .fetch_all::<BarRow>()
            .await?;

        rows.into_iter()
            .map(|row| Ok(MarketEvent::Bar(row.try_into()?)))
            .collect()
    }
}

/// Converts a `NaiveDate` into midnight (00:00:00) UTC on that date. Used
/// by `read_batch` to turn its day-granularity `Range<NaiveDate>` into the
/// half-open `[start, end)` timestamp range bound into the queries (the
/// caller passes `date_range.end` as the exclusive upper bound, so the
/// returned midnight is the *start* of the end day, not its end).
fn day_boundary(date: NaiveDate) -> Result<DateTime<Utc>, StorageError> {
    date.and_hms_opt(0, 0, 0)
        .map(|midnight| midnight.and_utc())
        .ok_or_else(|| StorageError::QueryFailed {
            reason: format!("invalid date: {date}"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn day_boundary_returns_midnight_utc() {
        let date = NaiveDate::from_ymd_opt(2026, 7, 28)
            .unwrap_or_else(|| panic!("2026-07-28 is a valid date"));
        let boundary = day_boundary(date).unwrap_or_else(|err| panic!("valid date: {err}"));
        assert_eq!(boundary.to_rfc3339(), "2026-07-28T00:00:00+00:00");
    }

    #[test]
    fn day_boundary_handles_leap_day() {
        let date = NaiveDate::from_ymd_opt(2024, 2, 29)
            .unwrap_or_else(|| panic!("2024-02-29 is a valid date"));
        let boundary = day_boundary(date).unwrap_or_else(|err| panic!("valid date: {err}"));
        assert_eq!(boundary.to_rfc3339(), "2024-02-29T00:00:00+00:00");
    }

    #[test]
    fn day_boundary_handles_year_rollover() {
        let date = NaiveDate::from_ymd_opt(2026, 12, 31)
            .unwrap_or_else(|| panic!("2026-12-31 is a valid date"));
        let boundary = day_boundary(date).unwrap_or_else(|err| panic!("valid date: {err}"));
        // The exclusive-end caller passes end = start_of_next_day, so a
        // range ending 2026-12-31 must NOT include any of 2026-12-31
        // itself: the boundary is midnight *at the start* of that day.
        assert_eq!(boundary.to_rfc3339(), "2026-12-31T00:00:00+00:00");
    }
}
