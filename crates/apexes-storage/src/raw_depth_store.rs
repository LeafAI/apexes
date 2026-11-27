//! F03.03: isolated, lossless raw capture batches. This opt-in table
//! deliberately does not share the legacy symbol/venue depth namespace.
use crate::StorageError;
use apexes_core::MarketEvent;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

const DDL: &str = "
CREATE TABLE IF NOT EXISTS raw_depth_batches
(
    run_id String,
    batch_index UInt64,
    events_json String
)
ENGINE = MergeTree
ORDER BY (run_id, batch_index)
";

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct RawDepthBatch {
    pub run_id: String,
    pub batch_index: u64,
    pub events_json: String,
}

impl RawDepthBatch {
    pub fn new(
        run_id: &str,
        batch_index: u64,
        events: &[MarketEvent],
    ) -> Result<Self, StorageError> {
        if events.is_empty() {
            return Err(StorageError::EmptyBatch);
        }
        for event in events {
            let MarketEvent::Depth(depth) = event else {
                return Err(StorageError::MixedBatch);
            };
            depth
                .validate_source_range()
                .map_err(|reason| StorageError::InvalidDepthRange { reason })?;
        }
        Ok(Self {
            run_id: run_id.into(),
            batch_index,
            events_json: serde_json::to_string(events).map_err(|error| {
                StorageError::QueryFailed {
                    reason: error.to_string(),
                }
            })?,
        })
    }

    pub fn events(&self) -> Result<Vec<MarketEvent>, StorageError> {
        let events: Vec<MarketEvent> =
            serde_json::from_str(&self.events_json).map_err(|error| StorageError::QueryFailed {
                reason: error.to_string(),
            })?;
        Self::new(&self.run_id, self.batch_index, &events)?;
        Ok(events)
    }
}

/// The caller owns a unique run ID and must never retry ambiguous inserts.
pub struct RawDepthStore {
    client: Client,
}

impl RawDepthStore {
    pub async fn new(url: &str) -> Result<Self, StorageError> {
        let client = Client::default()
            .with_url(url)
            .with_setting("async_insert", "0");
        client.query(DDL).execute().await?;
        Ok(Self { client })
    }

    pub async fn write_batch(&self, batch: &RawDepthBatch) -> Result<(), StorageError> {
        batch.events()?;
        let mut insert = self
            .client
            .insert::<RawDepthBatch>("raw_depth_batches")
            .await?;
        insert.write(batch).await?;
        insert.end().await?;
        Ok(())
    }

    /// Bounded read; duplicated batch identity is an integrity error.
    pub async fn read_batch(
        &self,
        run_id: &str,
        batch_index: u64,
    ) -> Result<Option<RawDepthBatch>, StorageError> {
        let mut rows = self.client.query("SELECT run_id, batch_index, events_json FROM raw_depth_batches WHERE run_id = ? AND batch_index = ? LIMIT 2")
            .bind(run_id).bind(batch_index).fetch_all::<RawDepthBatch>().await?;
        if rows.len() > 1 {
            return Err(StorageError::QueryFailed {
                reason: "duplicate raw depth batch identity".into(),
            });
        }
        Ok(rows.pop())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{DepthSourceRange, DepthUpdate, PriceLevel, Venue};
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    fn depth() -> MarketEvent {
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: dec!(0.123456789012345678),
                quantity: dec!(1),
            }],
            asks: vec![],
            sequence: Some(103),
            source_range: Some(DepthSourceRange {
                first: 101,
                last: 103,
                previous_final: Some(100),
            }),
            is_snapshot: false,
            timestamp: Utc.timestamp_opt(1_700_000_000, 123_456_789).unwrap(),
        })
    }

    #[test]
    fn batch_preserves_exact_event_order_range_and_precision() {
        let mut later = depth();
        if let MarketEvent::Depth(d) = &mut later {
            d.sequence = Some(105);
            d.source_range = Some(DepthSourceRange {
                first: 104,
                last: 105,
                previous_final: Some(103),
            });
        }
        let events = vec![depth(), later];
        let batch = RawDepthBatch::new("run", 0, &events).unwrap();
        assert_eq!(batch.events().unwrap(), events);
        assert!(batch.events_json.contains("0.123456789012345678"));
        assert!(batch.events_json.contains("123456789"));
    }

    #[test]
    fn empty_or_inconsistent_range_is_rejected() {
        assert!(RawDepthBatch::new("run", 0, &[]).is_err());
        let mut event = depth();
        if let MarketEvent::Depth(d) = &mut event {
            d.sequence = Some(104);
        }
        assert!(RawDepthBatch::new("run", 0, &[event]).is_err());
    }
}
