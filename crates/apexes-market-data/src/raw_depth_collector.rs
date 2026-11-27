//! F03.03: dedicated raw capture persistence with independent durable
//! intent/commit receipts. A stopped run is not a coverage attestation.
use crate::{provider::BinanceExchangeKind, raw_depth_capture};
use apexes_core::{MarketEvent, Venue};
use apexes_storage::raw_depth_store::{RawDepthBatch, RawDepthStore};
use chrono::Utc;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{future::Future, path::Path, time::Duration};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::{mpsc, oneshot},
};
use uuid::Uuid;

const CHANNEL_CAPACITY: usize = 8192;
const BATCH_SIZE: usize = 64;
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Receipt<'a> {
    Header {
        schema_version: u32,
        run_id: &'a str,
        symbol: &'a str,
        market: &'a str,
        started_at: chrono::DateTime<Utc>,
    },
    Intent {
        batch_index: u64,
        expected_count: usize,
        payload_sha256: String,
        invalidations: usize,
    },
    Commit {
        batch_index: u64,
    },
    Terminal {
        stopped: bool,
        reason: Option<&'a str>,
        committed_batches: u64,
    },
}

struct Journal {
    file: File,
    run_id: String,
    next: u64,
}

impl Journal {
    async fn create(
        directory: &Path,
        symbol: &str,
        exchange: BinanceExchangeKind,
    ) -> Result<Self, String> {
        let run_id = Uuid::new_v4().to_string();
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join(format!("{run_id}.jsonl")))
            .await
            .map_err(|e| e.to_string())?;
        let mut journal = Self {
            file,
            run_id: run_id.clone(),
            next: 0,
        };
        journal
            .append(Receipt::Header {
                schema_version: 1,
                run_id: &run_id,
                symbol,
                market: match exchange {
                    BinanceExchangeKind::Spot => "spot",
                    BinanceExchangeKind::Perpetual => "usd_m_perpetual",
                },
                started_at: Utc::now(),
            })
            .await?;
        File::open(directory)
            .await
            .map_err(|e| e.to_string())?
            .sync_all()
            .await
            .map_err(|e| e.to_string())?;
        Ok(journal)
    }

    async fn append(&mut self, record: Receipt<'_>) -> Result<(), String> {
        let mut bytes = serde_json::to_vec(&record).map_err(|e| e.to_string())?;
        bytes.push(b'\n');
        self.file
            .write_all(&bytes)
            .await
            .map_err(|e| e.to_string())?;
        self.file.sync_all().await.map_err(|e| e.to_string())
    }

    async fn persist<F, Fut>(&mut self, events: &[MarketEvent], write: &mut F) -> Result<(), String>
    where
        F: FnMut(RawDepthBatch) -> Fut,
        Fut: Future<Output = Result<(), String>>,
    {
        let index = self.next;
        let next = index.checked_add(1).ok_or("batch index exhausted")?;
        let batch = RawDepthBatch::new(&self.run_id, index, events).map_err(|e| e.to_string())?;
        self.append(Receipt::Intent {
            batch_index: index,
            expected_count: events.len(),
            payload_sha256: format!("{:x}", Sha256::digest(batch.events_json.as_bytes())),
            invalidations: events
                .iter()
                .filter(|e| matches!(e, MarketEvent::Depth(d) if d.is_invalidation()))
                .count(),
        })
        .await?;
        tokio::time::timeout(Duration::from_secs(30), write(batch))
            .await
            .map_err(|_| "raw depth insert timed out; outcome ambiguous")??;
        self.append(Receipt::Commit { batch_index: index }).await?;
        self.next = next;
        Ok(())
    }
}

async fn consume<F, Fut>(
    mut rx: mpsc::Receiver<MarketEvent>,
    mut stop: oneshot::Receiver<()>,
    journal: &mut Journal,
    symbol: &str,
    mut write: F,
) -> Result<(), String>
where
    F: FnMut(RawDepthBatch) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let mut buffer = Vec::with_capacity(BATCH_SIZE);
    let mut timer = tokio::time::interval(FLUSH_INTERVAL);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut stopping = None;
    loop {
        let flush = tokio::select! {
            signal = &mut stop, if stopping.is_none() => { stopping = Some(signal.is_ok()); rx.close(); false }
            event = rx.recv() => match event {
                Some(event) => {
                    if !matches!(&event, MarketEvent::Depth(d) if d.symbol == symbol && d.venue == Venue::Binance) {
                        return Err("unexpected event in dedicated raw capture".into());
                    }
                    buffer.push(event);
                    buffer.len() >= BATCH_SIZE
                }
                None => {
                    if !buffer.is_empty() { journal.persist(&buffer, &mut write).await?; }
                    return if stopping == Some(true) { Ok(()) } else { Err("raw capture ended without requested stop".into()) };
                }
            },
            _ = timer.tick() => !buffer.is_empty(),
        };
        if flush {
            journal.persist(&buffer, &mut write).await?;
            buffer.clear();
        }
    }
}

/// Collect one explicitly selected public market. The journal directory
/// must already exist. Returns the run ID only after an intentional
/// stop, drained channel, acknowledged inserts and synced terminal receipt.
/// An error leaves the journal available for diagnosis; never retry a run.
pub async fn collect(
    symbol: &str,
    exchange: BinanceExchangeKind,
    store: &RawDepthStore,
    receipt_directory: &Path,
    stop: oneshot::Receiver<()>,
) -> Result<String, String> {
    let mut journal = Journal::create(receipt_directory, symbol, exchange).await?;
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let capture = raw_depth_capture::capture(symbol, exchange, tx);
    let consumer = consume(rx, stop, &mut journal, symbol, |batch| async move {
        store
            .write_batch(&batch)
            .await
            .map_err(|_| "raw depth insert failed; outcome may be ambiguous".into())
    });
    let (capture_result, consumer_result) = tokio::join!(capture, consumer);
    let result = capture_result.and(consumer_result);
    let terminal = journal
        .append(Receipt::Terminal {
            stopped: result.is_ok(),
            reason: result.as_ref().err().map(String::as_str),
            committed_batches: journal.next,
        })
        .await;
    terminal?;
    result?;
    Ok(journal.run_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{DepthSourceRange, DepthUpdate};
    use serde_json::Value;

    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("apexes-receipts-{}", Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn delta() -> MarketEvent {
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![],
            sequence: Some(103),
            source_range: Some(DepthSourceRange {
                first: 101,
                last: 103,
                previous_final: None,
            }),
            is_snapshot: false,
            timestamp: Utc::now(),
        })
    }

    fn records(path: &Path) -> Vec<Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn intent_is_durable_before_insert_and_commit_follows_ack() {
        let dir = TempDir::new();
        let mut journal = Journal::create(&dir.0, "BTCUSDT", BinanceExchangeKind::Spot)
            .await
            .unwrap();
        let path = dir.0.join(format!("{}.jsonl", journal.run_id));
        let event = delta();
        journal
            .persist(std::slice::from_ref(&event), &mut |batch| {
                let observed = records(&path);
                assert_eq!(observed.len(), 2);
                assert_eq!(observed[0]["market"], "spot");
                assert_eq!(observed[1]["kind"], "intent");
                assert_eq!(observed[1]["expected_count"], 1);
                assert_eq!(
                    observed[1]["payload_sha256"],
                    format!("{:x}", Sha256::digest(batch.events_json.as_bytes()))
                );
                assert_eq!(batch.events().unwrap(), vec![event.clone()]);
                std::future::ready(Ok(()))
            })
            .await
            .unwrap();
        assert_eq!(records(&path)[2]["kind"], "commit");
        assert_eq!(journal.next, 1);
    }

    #[tokio::test]
    async fn failed_insert_is_not_committed_or_retried() {
        let dir = TempDir::new();
        let mut journal = Journal::create(&dir.0, "BTCUSDT", BinanceExchangeKind::Perpetual)
            .await
            .unwrap();
        let path = dir.0.join(format!("{}.jsonl", journal.run_id));
        let mut writes = 0;
        assert!(
            journal
                .persist(&[delta()], &mut |_| {
                    writes += 1;
                    std::future::ready(Err("ambiguous write".into()))
                })
                .await
                .is_err()
        );
        assert_eq!(writes, 1);
        assert_eq!(journal.next, 0);
        let observed = records(&path);
        assert_eq!(observed.len(), 2);
        assert_eq!(observed[0]["market"], "usd_m_perpetual");
        assert_eq!(observed[1]["kind"], "intent");
    }

    #[tokio::test]
    async fn explicit_stop_drains_queued_events_including_invalidation() {
        let dir = TempDir::new();
        let mut journal = Journal::create(&dir.0, "BTCUSDT", BinanceExchangeKind::Spot)
            .await
            .unwrap();
        let (tx, rx) = mpsc::channel(4);
        let (stop_tx, stop) = oneshot::channel();
        let mut invalidation = delta();
        if let MarketEvent::Depth(d) = &mut invalidation {
            d.is_snapshot = true;
            d.sequence = None;
            d.source_range = None;
        }
        let expected = vec![delta(), invalidation];
        for e in &expected {
            tx.send(e.clone()).await.unwrap();
        }
        stop_tx.send(()).unwrap();
        let mut persisted = vec![];
        consume(rx, stop, &mut journal, "BTCUSDT", |batch| {
            persisted.extend(batch.events().unwrap());
            std::future::ready(Ok(()))
        })
        .await
        .unwrap();
        assert_eq!(persisted, expected);
        let observed = records(&dir.0.join(format!("{}.jsonl", journal.run_id)));
        assert_eq!(
            observed
                .iter()
                .filter_map(|r| r["invalidations"].as_u64())
                .sum::<u64>(),
            1
        );
    }

    #[tokio::test]
    async fn unexpected_channel_end_cannot_report_success() {
        let dir = TempDir::new();
        let mut journal = Journal::create(&dir.0, "BTCUSDT", BinanceExchangeKind::Spot)
            .await
            .unwrap();
        let (tx, rx) = mpsc::channel(4);
        let (_stop_tx, stop) = oneshot::channel();
        tx.send(delta()).await.unwrap();
        drop(tx);
        let mut count = 0;
        assert!(
            consume(rx, stop, &mut journal, "BTCUSDT", |batch| {
                count += batch.events().unwrap().len();
                std::future::ready(Ok(()))
            })
            .await
            .is_err()
        );
        assert_eq!(count, 1);
    }
    #[tokio::test]
    async fn dropping_stop_sender_is_not_an_intentional_stop() {
        let dir = TempDir::new();
        let mut journal = Journal::create(&dir.0, "BTCUSDT", BinanceExchangeKind::Spot)
            .await
            .unwrap();
        let (tx, rx) = mpsc::channel(4);
        let (stop_tx, stop) = oneshot::channel();
        tx.send(delta()).await.unwrap();
        drop(stop_tx);
        let mut count = 0;
        assert!(
            consume(rx, stop, &mut journal, "BTCUSDT", |batch| {
                count += batch.events().unwrap().len();
                std::future::ready(Ok(()))
            })
            .await
            .is_err()
        );
        assert_eq!(count, 1);
    }
}
