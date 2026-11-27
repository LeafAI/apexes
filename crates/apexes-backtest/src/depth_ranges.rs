//! Offline range-aware depth continuity evidence. Normalized DepthUpdate
//! exposes only the final sequence; never infer missing source endpoints
//! or previous-final links from that number.

use crate::historical_dataset::{DatasetManifest, DatasetStream, StreamKind, VerifiedDataset};
use apexes_core::MarketEvent;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Source-specific contracts, not inferred from the venue. Both require
/// advancing increments and an initial snapshot. Resynchronization across
/// a known missing range requires a new independently qualified export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeContract {
    /// Absolute price-level updates: every advancing interval must
    /// include the next required ID; overlap is allowed.
    InclusiveNext,
    /// First increment bridges the snapshot ID; subsequent increments
    /// must link to the previous final ID. Numeric IDs may be sparse.
    PreviousFinal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthRange {
    /// Zero-based position in the manifest's ordered normalized events.
    pub event_index: usize,
    pub first: u64,
    pub last: u64,
    pub previous_final: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthStreamRanges {
    pub stream: DatasetStream,
    pub contract: RangeContract,
    /// One original source range for every incremental event in this
    /// stream, in replay order. Snapshots use their own sequence.
    pub increments: Vec<DepthRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthRangeEvidence {
    pub schema_version: u32,
    pub dataset_id: String,
    pub dataset_version: String,
    pub source_id: String,
    pub replay_sha256: String,
    pub streams: Vec<DepthStreamRanges>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepthRangeReport {
    pub evidence: DepthRangeEvidence,
    pub streams_verified: usize,
    pub snapshots_verified: usize,
    pub increments_verified: usize,
    /// Supplied source metadata reconciled; not independently attested.
    pub independently_attested: bool,
}

struct Cursor<'a> {
    ranges: &'a DepthStreamRanges,
    next: usize,
    last: Option<u64>,
    first_increment: bool,
    snapshots: usize,
}

impl Cursor<'_> {
    fn observe(&mut self, index: usize, d: &apexes_core::DepthUpdate) -> Result<(), String> {
        if d.is_invalidation() {
            return Err("explicit depth invalidation interrupts source continuity".into());
        }
        let final_id = d.sequence.ok_or("missing normalized depth sequence")?;
        if d.is_snapshot {
            // A snapshot can establish the initial anchor, but a later
            // snapshot must not conceal a missing interval or a reset.
            if self.last.is_some_and(|last| last != final_id) {
                return Err("snapshot jumps or resets an established source chain".into());
            }
            self.last = Some(final_id);
            // Same-ID snapshots do not reset an already linked chain.
            self.snapshots += 1;
            return Ok(());
        }
        let prior = self.last.ok_or("increment before initial depth snapshot")?;
        let r = self
            .ranges
            .increments
            .get(self.next)
            .ok_or("missing original depth range")?;
        if r.event_index != index || r.last != final_id || r.first > r.last || r.last <= prior {
            return Err("invalid depth range, event binding or nonadvancing final ID".into());
        }
        match self.ranges.contract {
            RangeContract::InclusiveNext => {
                let next = prior.checked_add(1).ok_or("depth sequence overflow")?;
                if r.previous_final.is_some() || r.first > next || r.last < next {
                    return Err(
                        "inclusive depth range gap or unexpected previous-final link".into(),
                    );
                }
            }
            RangeContract::PreviousFinal => {
                let previous = r.previous_final.ok_or("missing previous-final link")?;
                if previous >= r.last {
                    return Err("invalid previous-final link".into());
                }
                if self.first_increment {
                    if r.first > prior || previous > prior {
                        return Err("first depth range does not bridge snapshot".into());
                    }
                } else if previous != prior {
                    return Err("depth previous-final link mismatch".into());
                }
            }
        }
        self.next += 1;
        self.last = Some(r.last);
        self.first_increment = false;
        Ok(())
    }
}

impl DepthRangeEvidence {
    pub fn verify_dataset(
        &self,
        manifest: &DatasetManifest,
        events: &[MarketEvent],
    ) -> Result<VerifiedDataset, String> {
        let mut verified = manifest.verify(events)?;
        verified.depth_ranges = Some(self.verify_receipts(manifest, events)?);
        Ok(verified)
    }

    pub(crate) fn verify_receipts(
        &self,
        manifest: &DatasetManifest,
        events: &[MarketEvent],
    ) -> Result<DepthRangeReport, String> {
        let d = &manifest.definition;
        if self.schema_version != 1
            || self.dataset_id != d.dataset_id
            || self.dataset_version != d.version
            || self.source_id != d.source_id
            || self.replay_sha256 != manifest.replay_sha256
        {
            return Err("depth range evidence identity differs from sealed dataset".into());
        }
        let declared: HashSet<_> = d
            .streams
            .iter()
            .filter(|s| s.kind == StreamKind::Depth)
            .collect();
        if declared.is_empty() || self.streams.len() != declared.len() {
            return Err("depth range evidence requires all declared depth streams".into());
        }
        let mut cursors = HashMap::new();
        for stream in &self.streams {
            if !declared.contains(&stream.stream)
                || cursors
                    .insert(
                        &stream.stream,
                        Cursor {
                            ranges: stream,
                            next: 0,
                            last: None,
                            first_increment: true,
                            snapshots: 0,
                        },
                    )
                    .is_some()
            {
                return Err("unknown or duplicate depth range stream".into());
            }
        }
        for (index, event) in events.iter().enumerate() {
            if let MarketEvent::Depth(d) = event {
                let key = DatasetStream {
                    venue: d.venue,
                    symbol: d.symbol.clone(),
                    kind: StreamKind::Depth,
                };
                cursors
                    .get_mut(&key)
                    .ok_or("depth event outside range evidence")?
                    .observe(index, d)?;
            }
        }
        let mut snapshots = 0;
        let mut increments = 0;
        for cursor in cursors.values() {
            if cursor.next != cursor.ranges.increments.len()
                || cursor.snapshots == 0
                || cursor.next == 0
            {
                return Err(
                    "unused depth ranges or stream lacks snapshot/increment evidence".into(),
                );
            }
            snapshots += cursor.snapshots;
            increments += cursor.next;
        }
        Ok(DepthRangeReport {
            evidence: self.clone(),
            streams_verified: cursors.len(),
            snapshots_verified: snapshots,
            increments_verified: increments,
            independently_attested: false,
        })
    }
}
