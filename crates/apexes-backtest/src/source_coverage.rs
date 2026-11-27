//! F03.02/F03.03 — Reconcile caller-supplied collection receipts against
//! sealed replay data. A receipt is a claim, not an upstream attestation.

use crate::historical_dataset::{DatasetManifest, DatasetStream, StreamKind, VerifiedDataset};
use crate::market_data_source::event_timestamp;
use apexes_core::MarketEvent;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

const MAX_WINDOWS: usize = 100_000;

/// Only use unit-increment sequences when the source contract guarantees
/// one identifier per record, across collection windows and empty periods.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SequenceContract {
    Unavailable,
    ContiguousTradeId,
    ContiguousDepthUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SequenceRange {
    pub first: u64,
    pub last: u64,
}

/// A source receipt from collection, not inferred from replay contents.
/// The end timestamp is exclusive. Failed/incomplete windows cannot pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionWindow {
    pub receipt_id: String,
    pub start: DateTime<Utc>,
    pub end_exclusive: DateTime<Utc>,
    pub complete: bool,
    pub expected_count: u64,
    pub sequence_range: Option<SequenceRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamCoverage {
    pub stream: DatasetStream,
    pub sequence_contract: SequenceContract,
    pub windows: Vec<CollectionWindow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCoverage {
    pub schema_version: u32,
    pub dataset_id: String,
    pub dataset_version: String,
    pub source_id: String,
    pub replay_sha256: String,
    pub streams: Vec<StreamCoverage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageReport {
    pub evidence: SourceCoverage,
    pub windows_verified: usize,
    pub empty_windows: usize,
    pub sequence_checked_streams: usize,
    pub sequence_unavailable_streams: usize,
    /// Supplied receipts reconciled; not a claim of independently
    /// established exchange completeness or receipt authenticity.
    pub independently_attested: bool,
}

fn range_count(r: &SequenceRange) -> Option<u64> {
    r.last.checked_sub(r.first)?.checked_add(1)
}

fn validate_windows(
    stream: &StreamCoverage,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    receipts: &mut HashSet<String>,
) -> Result<(), String> {
    match (stream.sequence_contract, stream.stream.kind) {
        (SequenceContract::Unavailable, _)
        | (SequenceContract::ContiguousTradeId, StreamKind::Trade)
        | (SequenceContract::ContiguousDepthUpdate, StreamKind::Depth) => {}
        _ => return Err("sequence contract does not match event kind".into()),
    }
    let mut next_start = start;
    let mut last_sequence: Option<u64> = None;
    for w in &stream.windows {
        if w.receipt_id.trim().is_empty()
            || !receipts.insert(w.receipt_id.clone())
            || !w.complete
            || w.start != next_start
            || w.end_exclusive <= w.start
            || w.end_exclusive > end
        {
            return Err(
                "missing, incomplete, overlapping or invalid collection window/receipt".into(),
            );
        }
        next_start = w.end_exclusive;
        match (
            &w.sequence_range,
            stream.sequence_contract,
            w.expected_count,
        ) {
            (None, SequenceContract::Unavailable, _) | (None, _, 0) => {}
            (Some(r), contract, n)
                if contract != SequenceContract::Unavailable
                    && n > 0
                    && range_count(r) == Some(n) =>
            {
                if last_sequence.is_some_and(|last| last.checked_add(1) != Some(r.first)) {
                    return Err("source sequence gap or overlap between windows".into());
                }
                last_sequence = Some(r.last);
            }
            _ => return Err("missing or invalid expected source sequence range".into()),
        }
    }
    if next_start != end {
        return Err("collection windows do not cover entire requested range".into());
    }
    Ok(())
}

fn event_sequence(event: &MarketEvent, contract: SequenceContract) -> Result<u64, String> {
    match (event, contract) {
        (MarketEvent::Trade(t), SequenceContract::ContiguousTradeId) => t
            .trade_id
            .as_ref()
            .filter(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|id| id.parse().ok())
            .ok_or_else(|| "missing or invalid numeric trade ID".into()),
        (MarketEvent::Depth(d), SequenceContract::ContiguousDepthUpdate) => {
            if d.is_invalidation() {
                return Err("depth invalidation cannot establish contiguous collection".into());
            }
            d.sequence.ok_or_else(|| "missing depth sequence".into())
        }
        _ => Err("event does not match sequence contract".into()),
    }
}

struct Cursor<'a> {
    stream: &'a StreamCoverage,
    window: usize,
    count: u64,
}

impl Cursor<'_> {
    fn finish_window(&mut self) -> Result<(), String> {
        let w = &self.stream.windows[self.window];
        if self.count != w.expected_count {
            return Err(format!(
                "collection count mismatch for receipt {}",
                w.receipt_id
            ));
        }
        self.window += 1;
        self.count = 0;
        Ok(())
    }

    fn observe(&mut self, event: &MarketEvent) -> Result<(), String> {
        let at = event_timestamp(event);
        while self
            .stream
            .windows
            .get(self.window)
            .is_some_and(|w| at >= w.end_exclusive)
        {
            self.finish_window()?;
        }
        let w = self
            .stream
            .windows
            .get(self.window)
            .filter(|w| at >= w.start)
            .ok_or("event outside collection windows")?;
        if self.count >= w.expected_count {
            return Err(format!("collection count exceeds receipt {}", w.receipt_id));
        }
        if let Some(range) = &w.sequence_range {
            let expected = range
                .first
                .checked_add(self.count)
                .ok_or("sequence overflow")?;
            if event_sequence(event, self.stream.sequence_contract)? != expected {
                return Err(format!(
                    "source sequence mismatch for receipt {}",
                    w.receipt_id
                ));
            }
        }
        self.count += 1;
        Ok(())
    }

    fn finish(mut self) -> Result<(), String> {
        while self.window < self.stream.windows.len() {
            self.finish_window()?;
        }
        Ok(())
    }
}

impl SourceCoverage {
    /// Verify the dataset first, then reconcile every receipt with the exact
    /// ordered input. This does not authenticate receipts or infer gaps from
    /// timestamps of trades that may legitimately be sparse.
    pub fn verify(
        &self,
        manifest: &DatasetManifest,
        events: &[MarketEvent],
    ) -> Result<CoverageReport, String> {
        manifest.verify(events)?;
        self.verify_receipts(manifest, events)
    }

    /// Return verified dataset identity and reconciled source receipts together.
    pub fn verify_dataset(
        &self,
        manifest: &DatasetManifest,
        events: &[MarketEvent],
    ) -> Result<VerifiedDataset, String> {
        let mut verified = manifest.verify(events)?;
        verified.source_coverage = Some(self.verify_receipts(manifest, events)?);
        Ok(verified)
    }

    /// Used by the runner only after its manifest verification succeeds.
    pub(crate) fn verify_receipts(
        &self,
        manifest: &DatasetManifest,
        events: &[MarketEvent],
    ) -> Result<CoverageReport, String> {
        let d = &manifest.definition;
        if self.schema_version != 1
            || self.dataset_id != d.dataset_id
            || self.dataset_version != d.version
            || self.source_id != d.source_id
            || self.replay_sha256 != manifest.replay_sha256
        {
            return Err("coverage identity differs from sealed dataset".into());
        }
        let start = d
            .start_day
            .and_hms_opt(0, 0, 0)
            .ok_or("invalid start")?
            .and_utc();
        let end = d
            .end_day_exclusive
            .and_hms_opt(0, 0, 0)
            .ok_or("invalid end")?
            .and_utc();
        let declared: HashSet<_> = d.streams.iter().collect();
        let mut cursors = HashMap::new();
        let mut receipts = HashSet::new();
        let mut total = 0usize;
        let mut empty = 0usize;
        let mut checked = 0usize;
        for stream in &self.streams {
            total = total
                .checked_add(stream.windows.len())
                .filter(|n| *n <= MAX_WINDOWS)
                .ok_or("coverage exceeds window limit")?;
            if !declared.contains(&stream.stream)
                || cursors
                    .insert(
                        &stream.stream,
                        Cursor {
                            stream,
                            window: 0,
                            count: 0,
                        },
                    )
                    .is_some()
            {
                return Err("unknown or duplicate coverage stream".into());
            }
            validate_windows(stream, start, end, &mut receipts)?;
            empty += stream
                .windows
                .iter()
                .filter(|w| w.expected_count == 0)
                .count();
            checked += usize::from(
                stream.sequence_contract != SequenceContract::Unavailable
                    && stream.windows.iter().any(|w| w.expected_count > 0),
            );
        }
        if cursors.len() != declared.len() {
            return Err("missing coverage stream".into());
        }
        for event in events {
            let key = DatasetStream {
                venue: event.venue(),
                symbol: event.symbol().into(),
                kind: StreamKind::of(event),
            };
            cursors
                .get_mut(&key)
                .ok_or("event outside coverage streams")?
                .observe(event)?;
        }
        for cursor in cursors.into_values() {
            cursor.finish()?;
        }
        Ok(CoverageReport {
            evidence: self.clone(),
            windows_verified: total,
            empty_windows: empty,
            sequence_checked_streams: checked,
            sequence_unavailable_streams: self.streams.len() - checked,
            independently_attested: false,
        })
    }
}
