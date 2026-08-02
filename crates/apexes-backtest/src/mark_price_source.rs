//! [`StorageReaderMarkPriceProvider`]: the backtest-only
//! `apexes_strategy::MarkPriceProvider` implementation, reading real
//! historical mark prices from `apexes_storage::StorageReader`'s
//! `mark_prices` table. Defined HERE (not in `apexes-storage`, which
//! doesn't depend on `apexes-strategy` where `MarkPriceProvider` is
//! defined) -- see this plan's Global Constraints.

use apexes_core::{ApexesError, Venue};
use apexes_storage::{EventType, StorageReader};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::sync::Mutex;

pub struct StorageReaderMarkPriceProvider {
    reader: StorageReader,
    /// Updated by the caller (`run_event_loop`) as it advances through
    /// replay -- "current" mark price means "as of this timestamp".
    current_timestamp: Mutex<DateTime<Utc>>,
}

impl StorageReaderMarkPriceProvider {
    pub fn new(reader: StorageReader, initial_timestamp: DateTime<Utc>) -> Self {
        Self {
            reader,
            current_timestamp: Mutex::new(initial_timestamp),
        }
    }

    pub fn advance_to(&self, timestamp: DateTime<Utc>) {
        *self
            .current_timestamp
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = timestamp;
    }
}

#[async_trait]
impl apexes_strategy::MarkPriceProvider for StorageReaderMarkPriceProvider {
    async fn mark_price(&self, symbol: &str, venue: Venue) -> Result<Decimal, ApexesError> {
        let now = *self
            .current_timestamp
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let date = now.date_naive();
        let events = self
            .reader
            .read_batch(
                symbol,
                venue,
                date..date.succ_opt().unwrap_or(date),
                EventType::MarkPrice,
            )
            .await
            .map_err(|error| ApexesError::InvalidBacktestConfig {
                reason: format!("failed to read mark_prices for {symbol}: {error}"),
            })?;

        events
            .into_iter()
            .filter_map(|event| match event {
                apexes_core::MarketEvent::MarkPrice(update) if update.timestamp <= now => {
                    Some((update.timestamp, update.mark_price))
                }
                _ => None,
            })
            .max_by_key(|(timestamp, _)| *timestamp)
            .map(|(_, price)| price)
            .ok_or_else(|| ApexesError::InvalidBacktestConfig {
                reason: format!("no mark price available for {symbol} at or before {now}"),
            })
    }
}
