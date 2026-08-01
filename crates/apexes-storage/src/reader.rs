use crate::conversion::fixed_to_decimal;
use crate::error::StorageError;
use crate::schema::{BarRow, DepthRow, TickRow, TradeRow};
use apexes_core::{MarketEvent, Venue};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use clickhouse::Client;
use rust_decimal::Decimal;
use std::collections::BTreeMap;
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
    MarkPrice,
}

/// A day-independent intraday volume distribution: the fraction of a
/// full trading day's total volume that has typically occurred by each
/// point in the day, built by averaging historical daily volume curves
/// across `lookback_days`. Used by the EMS VWAP execution algorithm
/// (`apexes-ems`, sub-project #9) to pace child-order submission
/// against real historical volume patterns rather than assuming
/// uniform intraday distribution.
#[derive(Debug, Clone, PartialEq)]
pub struct VolumeCurve {
    /// `(offset from the start of the trading day, cumulative fraction
    /// of that day's total volume, 0.0..=1.0)`, sorted by offset
    /// ascending. The last element's fraction is always exactly `1.0`.
    pub cumulative_fractions: Vec<(Duration, Decimal)>,
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
            EventType::MarkPrice => Err(StorageError::QueryFailed {
                reason: "MarkPrice reading not yet implemented (see Task 4)".to_string(),
            }),
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

    /// Builds a `VolumeCurve` for `symbol`/`venue` by averaging the
    /// last `lookback_days` calendar days' hourly bar volumes (queried
    /// from the `bars` table, `BarInterval::OneHour` rows only -- other
    /// intervals are ignored so the curve has a consistent granularity
    /// regardless of what other interval data happens to exist for
    /// this symbol). For each of the `lookback_days` most recent
    /// COMPLETE calendar days that have at least one hourly bar, computes
    /// that day's own cumulative-fraction-of-day-total curve, then
    /// averages the fraction at each hour offset across all
    /// contributing days.
    ///
    /// Returns `Err(StorageError::InsufficientHistory)` if fewer than
    /// `lookback_days` distinct days have any hourly bar data at all --
    /// no silent fallback to a shorter lookback or a uniform-distribution
    /// assumption (design spec §四: "not a silent degrade to uniform").
    pub async fn intraday_volume_curve(
        &self,
        symbol: &str,
        venue: Venue,
        lookback_days: u32,
    ) -> Result<VolumeCurve, StorageError> {
        let venue_str = format!("{venue:?}");
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM bars WHERE venue = ? AND symbol = ? AND interval = 'OneHour' ORDER BY open_time DESC LIMIT 10000",
            )
            .bind(&venue_str)
            .bind(symbol)
            .fetch_all::<BarRow>()
            .await?;

        // Group bars by calendar day (UTC), each day's bars by
        // hour-of-day offset from that day's midnight.
        let mut by_day: BTreeMap<chrono::NaiveDate, BTreeMap<i64, Decimal>> = BTreeMap::new();
        for row in &rows {
            let day = row.open_time.date_naive();
            let midnight = day
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| StorageError::QueryFailed {
                    reason: format!("invalid date: {day}"),
                })?
                .and_utc();
            let offset_hours = (row.open_time - midnight).num_hours();
            let volume = fixed_to_decimal(row.volume);
            *by_day
                .entry(day)
                .or_default()
                .entry(offset_hours)
                .or_insert(Decimal::ZERO) += volume;
        }

        let available_days = by_day.len() as u32;
        if available_days < lookback_days {
            return Err(StorageError::InsufficientHistory {
                available_days,
                requested_days: lookback_days,
            });
        }

        // Most recent `lookback_days` days (BTreeMap is sorted
        // ascending by NaiveDate, so take the last `lookback_days`
        // entries).
        let recent_days: Vec<&BTreeMap<i64, Decimal>> =
            by_day.values().rev().take(lookback_days as usize).collect();

        // Union of every hour offset that appears in ANY contributing
        // day, so a hole in one day's data doesn't silently shift
        // another day's offsets out of alignment.
        let mut all_offsets: Vec<i64> = recent_days
            .iter()
            .flat_map(|day| day.keys().copied())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        all_offsets.sort_unstable();

        let mut cumulative_fractions = Vec::with_capacity(all_offsets.len());
        let day_count = Decimal::from(recent_days.len() as u64);

        for &offset in &all_offsets {
            let mut fraction_sum_at_offset = Decimal::ZERO;
            for day in &recent_days {
                let day_total: Decimal = day.values().copied().sum();
                if day_total.is_zero() {
                    continue;
                }
                let cumulative_volume_at_offset: Decimal = day
                    .iter()
                    .filter(|(hour, _)| **hour <= offset)
                    .map(|(_, volume)| *volume)
                    .sum();
                fraction_sum_at_offset += cumulative_volume_at_offset / day_total;
            }
            cumulative_fractions
                .push((Duration::hours(offset), fraction_sum_at_offset / day_count));
        }

        // Force the last fraction to exactly 1.0 -- averaging across
        // days can leave tiny rounding residue below 1.0 at the final
        // offset even though every individual day's own curve ends
        // exactly at 1.0.
        if let Some(last) = cumulative_fractions.last_mut() {
            last.1 = Decimal::ONE;
        }

        Ok(VolumeCurve {
            cumulative_fractions,
        })
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
