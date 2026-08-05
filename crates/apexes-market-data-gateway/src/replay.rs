//! Converts a client-supplied `replay_from: DateTime<Utc>` into the
//! day-granularity `apexes_storage::StorageReader::read_batch` calls
//! needed to reconstruct history for a fixed instrument set, merged
//! by timestamp. `read_batch` is per-`(symbol, venue, event_type)` --
//! this module issues one call per combination and merges the results,
//! since `apexes-storage` has no single "everything since timestamp X"
//! query (confirmed against its actual current API before writing this
//! -- do not assume a more convenient query shape exists).
//!
//! Which `EventType`s to replay: [`Gateway::connect`]'s forwarding loop
//! (Task 3, `gateway.rs`) forwards every `MarketEvent` the upstream
//! `MarketDataProvider` stream produces unconditionally -- there is no
//! Trade/Tick-only filter on the real-time path. To keep replayed
//! history consistent with what a client would have seen in real time,
//! [`replay_since`] therefore queries every `EventType` variant, not
//! just Trade/Tick.

use apexes_core::{Instrument, MarketEvent};
use apexes_storage::StorageReader;
use apexes_storage::error::StorageError;
use apexes_storage::reader::EventType;
use chrono::{DateTime, NaiveDate, Utc};
use std::ops::Range;

/// Every `EventType` the gateway may need to replay, matching Task 3's
/// forwarding loop, which does not filter by variant. See this module's
/// doc comment above for the full rationale.
const REPLAY_EVENT_TYPES: [EventType; 5] = [
    EventType::Trade,
    EventType::Tick,
    EventType::Depth,
    EventType::Bar,
    EventType::MarkPrice,
];

/// Replays every persisted `MarketEvent` for `instruments` from
/// `replay_from` through "now", merged by timestamp ascending.
///
/// Issues one `StorageReader::read_batch` call per `(instrument,
/// EventType)` combination (`instruments.len() * 5` calls total) and
/// merges every result by event timestamp. Because `read_batch` is
/// day-granularity, the returned events may include a few events
/// earlier in `replay_from`'s own day than `replay_from` itself --
/// this is an accepted over-inclusion (see the design spec §3.2):
/// clients dedupe by `(symbol, timestamp)`, so a handful of duplicate
/// or slightly-early events are harmless.
pub async fn replay_since(
    reader: &StorageReader,
    instruments: &[Instrument],
    replay_from: DateTime<Utc>,
) -> Result<Vec<MarketEvent>, StorageError> {
    let date_range = replay_date_range(replay_from, Utc::now())?;

    let mut merged = Vec::new();
    for instrument in instruments {
        for event_type in REPLAY_EVENT_TYPES {
            let events = reader
                .read_batch(
                    &instrument.symbol,
                    instrument.venue,
                    date_range.clone(),
                    event_type,
                )
                .await?;
            merged.extend(events);
        }
    }

    merged.sort_by_key(event_timestamp);
    Ok(merged)
}

/// Converts `replay_from` into the `[start, end)` `NaiveDate` range
/// `read_batch` expects: `start` is `replay_from`'s own UTC calendar
/// day (so a `replay_from` timestamp of, say, `14:30` still pulls that
/// whole day starting from midnight -- `read_batch` has no sub-day
/// granularity), and `end` is the day *after* `now`'s UTC calendar day
/// (exclusive upper bound, so all of "today" is included).
fn replay_date_range(
    replay_from: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Range<NaiveDate>, StorageError> {
    let start = replay_from.date_naive();
    let end = now
        .date_naive()
        .succ_opt()
        .ok_or_else(|| StorageError::QueryFailed {
            reason: format!("no valid day after {}", now.date_naive()),
        })?;
    Ok(start..end)
}

/// Extracts the event's own timestamp field regardless of variant.
/// `apexes-backtest`'s `unified_loop.rs` has a private function of the
/// identical shape and purpose; this crate cannot depend on
/// `apexes-backtest`, so this is an intentional local copy rather than
/// a shared dependency.
fn event_timestamp(event: &MarketEvent) -> DateTime<Utc> {
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
    use apexes_core::{TradeEvent, Venue};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn trade_at(hour: u32, minute: u32) -> MarketEvent {
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(1),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc
                .with_ymd_and_hms(2026, 8, 5, hour, minute, 0)
                .single()
                .unwrap_or_else(|| panic!("valid timestamp")),
        })
    }

    #[test]
    fn replay_date_range_at_exact_midnight_utc() {
        let replay_from = Utc
            .with_ymd_and_hms(2026, 8, 1, 0, 0, 0)
            .single()
            .unwrap_or_else(|| panic!("valid timestamp"));
        let now = Utc
            .with_ymd_and_hms(2026, 8, 3, 12, 30, 0)
            .single()
            .unwrap_or_else(|| panic!("valid timestamp"));

        let range = replay_date_range(replay_from, now).unwrap_or_else(|err| panic!("{err}"));

        assert_eq!(range.start, NaiveDate::from_ymd_opt(2026, 8, 1).unwrap());
        // Exclusive end is the day AFTER "now"'s calendar day, so all
        // of 2026-08-03 is included.
        assert_eq!(range.end, NaiveDate::from_ymd_opt(2026, 8, 4).unwrap());
    }

    #[test]
    fn replay_date_range_at_a_time_later_in_the_day() {
        let replay_from = Utc
            .with_ymd_and_hms(2026, 8, 1, 14, 30, 45)
            .single()
            .unwrap_or_else(|| panic!("valid timestamp"));
        let now = Utc
            .with_ymd_and_hms(2026, 8, 1, 15, 0, 0)
            .single()
            .unwrap_or_else(|| panic!("valid timestamp"));

        let range = replay_date_range(replay_from, now).unwrap_or_else(|err| panic!("{err}"));

        // `replay_from`'s own calendar day is still the start, even
        // though replay_from itself is mid-day (day-granularity has no
        // sub-day start boundary).
        assert_eq!(range.start, NaiveDate::from_ymd_opt(2026, 8, 1).unwrap());
        assert_eq!(range.end, NaiveDate::from_ymd_opt(2026, 8, 2).unwrap());
    }

    #[test]
    fn replay_date_range_spanning_a_year_boundary() {
        let replay_from = Utc
            .with_ymd_and_hms(2025, 12, 30, 23, 59, 59)
            .single()
            .unwrap_or_else(|| panic!("valid timestamp"));
        let now = Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 1)
            .single()
            .unwrap_or_else(|| panic!("valid timestamp"));

        let range = replay_date_range(replay_from, now).unwrap_or_else(|err| panic!("{err}"));

        assert_eq!(range.start, NaiveDate::from_ymd_opt(2025, 12, 30).unwrap());
        assert_eq!(range.end, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    }

    #[test]
    fn events_are_merged_in_ascending_timestamp_order() {
        let mut events = [trade_at(10, 0), trade_at(8, 0), trade_at(9, 30)];
        events.sort_by_key(event_timestamp);

        let timestamps: Vec<_> = events.iter().map(event_timestamp).collect();
        assert!(timestamps.is_sorted());
    }
}
