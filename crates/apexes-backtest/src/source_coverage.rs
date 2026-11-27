//! F03.02/F03.03 — Reconcile caller-supplied collection receipts against
//! sealed replay data. A receipt is a claim, not an upstream attestation.

use crate::historical_dataset::{
    DatasetManifest, DatasetStream, OBSERVATION_SCHEMA, StreamKind, VerifiedDataset,
};
use crate::market_data_source::event_timestamp;
use apexes_core::{Bar, BarInterval, MarketEvent, Venue};
use chrono::{DateTime, TimeDelta, Utc};
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
    /// Required for bars in receipt schema 2. Omitted in schema 1 and on non-bar streams.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "read_bar_interval"
    )]
    pub bar_interval: Option<BarInterval>,
    pub sequence_contract: SequenceContract,
    pub windows: Vec<CollectionWindow>,
}

fn read_bar_interval<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<BarInterval>, D::Error> {
    BarInterval::deserialize(deserializer).map(Some)
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

fn validate_windows<'a>(
    stream: &'a StreamCoverage,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    receipts: &mut HashSet<&'a str>,
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
            || !receipts.insert(w.receipt_id.as_str())
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

// The manifest retains its original stream/day partitions. Intervals refine
// those stream identities only for coverage reconciliation.
type StreamKey<'a> = (Venue, &'a str, StreamKind);
type ReceiptKey<'a> = (StreamKey<'a>, Option<BarInterval>);

fn stream_key(stream: &DatasetStream) -> StreamKey<'_> {
    (stream.venue, &stream.symbol, stream.kind)
}

fn validate_bar(bar: &Bar, previous_close: Option<DateTime<Utc>>) -> Result<(), String> {
    let seconds = match bar.interval {
        BarInterval::OneSecond => 1,
        BarInterval::OneMinute => 60,
        BarInterval::FiveMinutes => 300,
        BarInterval::FifteenMinutes => 900,
        BarInterval::OneHour => 3_600,
        BarInterval::OneDay => 86_400,
    };
    if bar
        .open_time
        .checked_add_signed(TimeDelta::seconds(seconds))
        != Some(bar.close_time)
    {
        return Err(format!(
            "invalid scheduled bar boundary for {:?}/{} {:?} at {}",
            bar.venue, bar.symbol, bar.interval, bar.open_time
        ));
    }
    if previous_close.is_some_and(|close| bar.open_time < close) {
        return Err(format!(
            "duplicate or overlapping bar for {:?}/{} {:?} at {}",
            bar.venue, bar.symbol, bar.interval, bar.open_time
        ));
    }
    Ok(())
}

struct Cursor<'a> {
    stream: &'a StreamCoverage,
    window: usize,
    count: u64,
    previous_bar_close: Option<DateTime<Utc>>,
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
        if self.stream.bar_interval.is_some()
            && let MarketEvent::Bar(bar) = event
        {
            validate_bar(bar, self.previous_bar_close)?;
            self.previous_bar_close = Some(bar.close_time);
        }
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
        self.count = self.count.checked_add(1).ok_or("event count overflow")?;
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
        let interval_specific = match self.schema_version {
            1 => false,
            2 if manifest.schema_version == OBSERVATION_SCHEMA => true,
            _ => return Err("unsupported coverage schema for dataset manifest".into()),
        };
        let d = &manifest.definition;
        if !interval_specific
            && manifest.schema_version == OBSERVATION_SCHEMA
            && d.streams.iter().any(|s| s.kind == StreamKind::Bar)
        {
            return Err(
                "schema 2 bar coverage requires an interval-specific receipt contract".into(),
            );
        }
        if self.dataset_id != d.dataset_id
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
        let mut declared: HashMap<_, _> =
            d.streams.iter().map(|s| (stream_key(s), false)).collect();
        if self.streams.len() > MAX_WINDOWS {
            return Err("coverage exceeds window limit".into());
        }
        let mut keys: HashMap<ReceiptKey<'_>, usize> = HashMap::new();
        let mut cursors = Vec::new();
        let mut receipts = HashSet::new();
        let mut total = 0usize;
        let mut empty = 0usize;
        let mut checked = 0usize;
        for stream in &self.streams {
            total = total
                .checked_add(stream.windows.len())
                .filter(|n| *n <= MAX_WINDOWS)
                .ok_or("coverage exceeds window limit")?;
            let base = stream_key(&stream.stream);
            if stream.bar_interval.is_some() != (interval_specific && base.2 == StreamKind::Bar) {
                return Err("bar interval required only on schema 2 bar coverage streams".into());
            }
            let covered = declared.get_mut(&base).ok_or("unknown coverage stream")?;
            if keys
                .insert((base, stream.bar_interval), cursors.len())
                .is_some()
            {
                return Err("duplicate coverage stream".into());
            }
            *covered = true;
            validate_windows(stream, start, end, &mut receipts)?;
            cursors.push(Cursor {
                stream,
                window: 0,
                count: 0,
                previous_bar_close: None,
            });
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
        if declared.values().any(|covered| !covered) {
            return Err("missing coverage stream".into());
        }
        for event in events {
            let interval = match event {
                MarketEvent::Bar(bar) if interval_specific => Some(bar.interval),
                _ => None,
            };
            let base = (event.venue(), event.symbol(), StreamKind::of(event));
            let index = keys
                .get(&(base, interval))
                .ok_or_else(|| format!("event outside coverage streams: {base:?}, {interval:?}"))?;
            cursors[*index].observe(event)?;
        }
        for cursor in cursors {
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
