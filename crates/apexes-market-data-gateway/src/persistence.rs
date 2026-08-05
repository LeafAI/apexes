//! Buffers incoming `MarketEvent`s GROUPED BY VARIANT (required --
//! `StorageWriter::write_batch` rejects a mixed-variant slice with
//! `StorageError::MixedBatch`) and flushes each variant's buffer to
//! ClickHouse when it reaches a size threshold OR a time threshold,
//! whichever comes first. A flush failure is logged via
//! `tracing::error!` and the buffer is dropped (not retried, not
//! blocking the real-time forwarding path) -- monitoring availability
//! takes priority over historical completeness, matching
//! `apexes-message-bus`'s own established "log and continue, never
//! block the hot path" convention.

use apexes_core::MarketEvent;
use apexes_storage::StorageWriter;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Which `MarketEvent` variant a buffered event belongs to -- the
/// grouping key that guarantees every `StorageWriter::write_batch` call
/// this module makes is homogeneous. Kept local to this crate (rather
/// than reusing `apexes-storage`'s own private, identically-shaped
/// `writer::BatchVariant`) since that type is not exported -- see this
/// crate's constraint against adding new `apexes-storage` surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

/// Returns true if a bucket holding `len` events has crossed
/// `max_batch_size` and should be flushed immediately. A free function
/// (rather than inlined into `record`) so it can be unit-tested without
/// needing a real `StorageWriter`/ClickHouse connection.
fn bucket_should_flush_on_size(len: usize, max_batch_size: usize) -> bool {
    len >= max_batch_size
}

/// Returns true if a bucket whose oldest buffered event was recorded at
/// `oldest_at` has aged past `max_batch_age` as of `now`. A free
/// function for the same unit-testability reason as
/// `bucket_should_flush_on_size` above.
fn bucket_should_flush_on_age(
    oldest_at: Option<Instant>,
    max_batch_age: Duration,
    now: Instant,
) -> bool {
    match oldest_at {
        Some(started) => now.duration_since(started) >= max_batch_age,
        None => false,
    }
}

/// One variant's pending events plus the time the first (oldest) of
/// them was buffered, used for age-based flush triggering.
#[derive(Default)]
struct Bucket {
    events: Vec<MarketEvent>,
    oldest_at: Option<Instant>,
}

impl Bucket {
    fn push(&mut self, event: MarketEvent) {
        if self.events.is_empty() {
            self.oldest_at = Some(Instant::now());
        }
        self.events.push(event);
    }

    /// Removes and returns every buffered event, resetting this bucket
    /// to empty. Returns `None` (no-op) if there was nothing to flush.
    fn take(&mut self) -> Option<Vec<MarketEvent>> {
        if self.events.is_empty() {
            return None;
        }
        self.oldest_at = None;
        Some(std::mem::take(&mut self.events))
    }
}

/// Buffers `MarketEvent`s by variant and flushes each variant's buffer
/// to ClickHouse (via `StorageWriter::write_batch`) once it crosses
/// `max_batch_size` events or `max_batch_age` wall-clock time, whichever
/// comes first. Every flush runs in its own spawned task so `record`
/// never blocks its caller (the gateway's real-time forwarding loop) on
/// a ClickHouse network round-trip.
pub struct PersistenceBuffer {
    writer: Arc<StorageWriter>,
    max_batch_size: usize,
    max_batch_age: Duration,
    buckets: HashMap<BatchVariant, Bucket>,
}

impl PersistenceBuffer {
    pub fn new(writer: StorageWriter, max_batch_size: usize, max_batch_age: Duration) -> Self {
        Self {
            writer: Arc::new(writer),
            max_batch_size,
            max_batch_age,
            buckets: HashMap::new(),
        }
    }

    /// Called for every event the gateway forwards. Buffers it under
    /// the correct per-variant bucket; flushes that bucket immediately
    /// (as its own spawned task, never awaited here) if it just
    /// crossed `max_batch_size`. Never issues a ClickHouse network
    /// call inline -- the real-time broadcast path this is wired
    /// alongside (`Gateway::connect`'s forwarding loop) must never
    /// wait on one.
    pub async fn record(&mut self, event: &MarketEvent) {
        let variant = variant_of(event);
        let bucket = self.buckets.entry(variant).or_default();
        bucket.push(event.clone());

        if bucket_should_flush_on_size(bucket.events.len(), self.max_batch_size) {
            self.flush_variant(variant);
        }
    }

    /// Time-based flush, called periodically (e.g. from a
    /// `tokio::time::interval` loop in `main.rs`, Task 5) for buckets
    /// that have events older than `max_batch_age` but haven't hit
    /// `max_batch_size` yet.
    pub async fn flush_aged(&mut self) {
        let now = Instant::now();
        let aged_variants: Vec<BatchVariant> = self
            .buckets
            .iter()
            .filter(|(_, bucket)| {
                bucket_should_flush_on_age(bucket.oldest_at, self.max_batch_age, now)
            })
            .map(|(variant, _)| *variant)
            .collect();

        for variant in aged_variants {
            self.flush_variant(variant);
        }
    }

    /// Drains `variant`'s bucket (if non-empty) and spawns a task that
    /// writes the drained, guaranteed-homogeneous batch to ClickHouse.
    /// A flush failure is logged and the batch is dropped -- never
    /// retried, never propagated back to the caller.
    fn flush_variant(&mut self, variant: BatchVariant) {
        let Some(bucket) = self.buckets.get_mut(&variant) else {
            return;
        };
        let Some(events) = bucket.take() else {
            return;
        };

        let writer = Arc::clone(&self.writer);
        tokio::spawn(async move {
            if let Err(error) = writer.write_batch(&events).await {
                tracing::error!(
                    ?variant,
                    batch_size = events.len(),
                    %error,
                    "PersistenceBuffer: flush failed, dropping batch"
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{Tick, TradeEvent, Venue};
    use rust_decimal_macros::dec;

    fn sample_trade() -> MarketEvent {
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(1),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: chrono::Utc::now(),
        })
    }

    fn sample_tick() -> MarketEvent {
        MarketEvent::Tick(Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(1),
            bid_size: dec!(1),
            ask_price: dec!(2),
            ask_size: dec!(1),
            timestamp: chrono::Utc::now(),
        })
    }

    #[test]
    fn variant_of_distinguishes_trade_and_tick() {
        assert_eq!(variant_of(&sample_trade()), BatchVariant::Trade);
        assert_eq!(variant_of(&sample_tick()), BatchVariant::Tick);
    }

    #[test]
    fn bucket_should_flush_on_size_triggers_at_threshold() {
        assert!(!bucket_should_flush_on_size(4, 5));
        assert!(bucket_should_flush_on_size(5, 5));
        assert!(bucket_should_flush_on_size(6, 5));
    }

    #[test]
    fn bucket_should_flush_on_age_triggers_after_max_age_elapses() {
        let max_batch_age = Duration::from_millis(10);
        let started = Instant::now();

        assert!(!bucket_should_flush_on_age(
            Some(started),
            max_batch_age,
            started
        ));
        assert!(!bucket_should_flush_on_age(None, max_batch_age, started));

        let later = started + Duration::from_millis(20);
        assert!(bucket_should_flush_on_age(
            Some(started),
            max_batch_age,
            later
        ));
    }

    #[test]
    fn bucket_take_resets_oldest_at_and_returns_none_when_empty() {
        let mut bucket = Bucket::default();
        assert!(bucket.take().is_none());

        bucket.push(sample_trade());
        assert!(bucket.oldest_at.is_some());

        let taken = bucket.take().unwrap_or_else(|| panic!("expected events"));
        assert_eq!(taken.len(), 1);
        assert!(bucket.oldest_at.is_none());
        assert!(bucket.events.is_empty());
    }

    #[test]
    fn bucket_push_only_sets_oldest_at_on_first_event() {
        let mut bucket = Bucket::default();
        bucket.push(sample_trade());
        let first_oldest_at = bucket.oldest_at;

        bucket.push(sample_trade());
        assert_eq!(bucket.oldest_at, first_oldest_at);
    }
}
