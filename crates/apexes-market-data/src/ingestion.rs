//! [`run_ingestion_loop`]: the project's first general-purpose
//! live-market-data-to-ClickHouse ingestion path. Subscribes to
//! `provider` for `instruments`, buffers incoming `MarketEvent`s by
//! variant (`StorageWriter::write_batch` requires a homogeneous
//! batch), and flushes each variant's buffer to `writer` whenever it
//! reaches `MAX_BATCH_SIZE` events OR `FLUSH_INTERVAL` has elapsed
//! since that variant's last flush, whichever comes first -- balances
//! write latency (a low-volume symbol's data shouldn't wait
//! indefinitely) against ClickHouse insert efficiency (one-row-at-a-time
//! inserts are wasteful). Runs until `provider`'s stream ends or
//! `stop_signal` fires (graceful-shutdown convention matching
//! `run_live_trading`/`run_paper_trading`'s existing
//! `oneshot::Receiver<()>` pattern).

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent};
use apexes_storage::StorageWriter;
use futures::StreamExt;
use std::time::Duration;
use tokio::sync::oneshot;

const MAX_BATCH_SIZE: usize = 500;
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Buffers {
    tick: Vec<MarketEvent>,
    depth: Vec<MarketEvent>,
    bar: Vec<MarketEvent>,
    trade: Vec<MarketEvent>,
    mark_price: Vec<MarketEvent>,
}

impl Buffers {
    fn push(&mut self, event: MarketEvent) {
        match &event {
            MarketEvent::Tick(_) => self.tick.push(event),
            MarketEvent::Depth(_) => self.depth.push(event),
            MarketEvent::Bar(_) => self.bar.push(event),
            MarketEvent::Trade(_) => self.trade.push(event),
            MarketEvent::MarkPrice(_) => self.mark_price.push(event),
        }
    }

    fn any_over_size_threshold(&self) -> bool {
        [
            &self.tick,
            &self.depth,
            &self.bar,
            &self.trade,
            &self.mark_price,
        ]
        .iter()
        .any(|buf| buf.len() >= MAX_BATCH_SIZE)
    }
}

async fn flush_if_nonempty(writer: &StorageWriter, buffer: &mut Vec<MarketEvent>) {
    if buffer.is_empty() {
        return;
    }
    if let Err(error) = writer.write_batch(buffer).await {
        tracing::error!(
            ?error,
            count = buffer.len(),
            "run_ingestion_loop: write_batch failed, dropping buffered batch"
        );
    }
    buffer.clear();
}

async fn flush_all(writer: &StorageWriter, buffers: &mut Buffers) {
    flush_if_nonempty(writer, &mut buffers.tick).await;
    flush_if_nonempty(writer, &mut buffers.depth).await;
    flush_if_nonempty(writer, &mut buffers.bar).await;
    flush_if_nonempty(writer, &mut buffers.trade).await;
    flush_if_nonempty(writer, &mut buffers.mark_price).await;
}

pub async fn run_ingestion_loop(
    provider: &dyn MarketDataProvider,
    writer: &StorageWriter,
    instruments: &[Instrument],
    mut stop_signal: oneshot::Receiver<()>,
) -> Result<(), ApexesError> {
    let mut stream = provider.subscribe(instruments).await?;
    let mut buffers = Buffers::default();
    let mut flush_timer = tokio::time::interval(FLUSH_INTERVAL);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = &mut stop_signal => {
                tracing::info!("run_ingestion_loop: stop signal received, flushing remaining buffers");
                flush_all(writer, &mut buffers).await;
                return Ok(());
            }
            event = stream.next() => {
                let Some(event) = event else {
                    tracing::warn!("run_ingestion_loop: provider stream ended, flushing remaining buffers");
                    flush_all(writer, &mut buffers).await;
                    return Ok(());
                };
                buffers.push(event);
                if buffers.any_over_size_threshold() {
                    flush_all(writer, &mut buffers).await;
                }
            }
            _ = flush_timer.tick() => {
                flush_all(writer, &mut buffers).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{TradeEvent, Venue};
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

    #[test]
    fn buffers_push_routes_events_to_the_correct_variant_buffer() {
        let mut buffers = Buffers::default();
        buffers.push(sample_trade());
        assert_eq!(buffers.trade.len(), 1);
        assert_eq!(buffers.tick.len(), 0);
    }

    #[test]
    fn any_over_size_threshold_detects_a_full_buffer() {
        let mut buffers = Buffers::default();
        for _ in 0..MAX_BATCH_SIZE {
            buffers.push(sample_trade());
        }
        assert!(buffers.any_over_size_threshold());
    }

    #[test]
    fn any_over_size_threshold_is_false_when_under_the_limit() {
        let mut buffers = Buffers::default();
        buffers.push(sample_trade());
        assert!(!buffers.any_over_size_threshold());
    }
}
