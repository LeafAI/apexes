//! Explicit identity contracts for offline historical export normalization.
//! Never infer identity from prices, sizes, or timestamp alone unless the
//! source contract guarantees that identity. Conflicts fail closed.

use crate::historical_dataset::{
    DatasetDefinition, DatasetManifest, DatasetStream, EvidenceBuilder, StreamKind, declared_keys,
    seal_dataset,
};
use crate::market_data_source::event_timestamp;
use apexes_core::{BarInterval, MarketEvent};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityContract {
    /// No reliable identity. Preserve every record.
    Unavailable,
    /// Nonempty exchange trade IDs, unique within this source/stream.
    OpaqueTradeId,
    /// Decimal u64 trade IDs; spelling aliases identify the same trade.
    NumericTradeId,
    /// Incremental depth IDs unique across the entire export. Snapshots
    /// and invalidations are always retained, never keyed as increments.
    DepthIncrementSequence,
    /// Immutable finalized candles keyed by interval and open time.
    FinalBar,
    /// Source guarantees one quote/mark update per event timestamp.
    UniqueTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamIdentity {
    pub stream: DatasetStream,
    pub contract: IdentityContract,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityPolicy {
    pub schema_version: u32,
    pub source_id: String,
    pub streams: Vec<StreamIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDeduplication {
    pub stream: DatasetStream,
    pub input_count: u64,
    pub retained_count: u64,
    pub identical_duplicates_removed: u64,
    pub unidentified_retained: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeduplicationReport {
    pub policy: IdentityPolicy,
    pub dataset_id: String,
    pub dataset_version: String,
    pub input_sha256: String,
    pub output_sha256: String,
    pub streams: Vec<StreamDeduplication>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedDataset {
    pub manifest: DatasetManifest,
    pub events: Vec<MarketEvent>,
    pub deduplication: DeduplicationReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Identity<'a> {
    Opaque(&'a str),
    Number(u64),
    Bar(BarInterval, DateTime<Utc>),
    Time(DateTime<Utc>),
}

fn identity(
    event: &MarketEvent,
    contract: IdentityContract,
) -> Result<Option<Identity<'_>>, String> {
    match (contract, event) {
        (IdentityContract::Unavailable, _) => Ok(None),
        (IdentityContract::OpaqueTradeId, MarketEvent::Trade(t)) => {
            let id = t
                .trade_id
                .as_deref()
                .filter(|id| !id.trim().is_empty())
                .ok_or("missing opaque trade identity")?;
            Ok(Some(Identity::Opaque(id)))
        }
        (IdentityContract::NumericTradeId, MarketEvent::Trade(t)) => {
            let id = t
                .trade_id
                .as_deref()
                .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or("invalid numeric trade identity")?;
            Ok(Some(Identity::Number(id)))
        }
        (IdentityContract::DepthIncrementSequence, MarketEvent::Depth(d)) => {
            if d.is_snapshot || d.is_invalidation() {
                return Ok(None);
            }
            Ok(Some(Identity::Number(
                d.sequence.ok_or("missing depth increment identity")?,
            )))
        }
        (IdentityContract::FinalBar, MarketEvent::Bar(b)) => {
            if b.close_time <= b.open_time {
                return Err("invalid final bar time range".into());
            }
            Ok(Some(Identity::Bar(b.interval, b.open_time)))
        }
        (IdentityContract::UniqueTimestamp, MarketEvent::Tick(_) | MarketEvent::MarkPrice(_)) => {
            Ok(Some(Identity::Time(event_timestamp(event))))
        }
        _ => Err("identity contract does not match event kind".into()),
    }
}

fn compatible(kind: StreamKind, contract: IdentityContract) -> bool {
    matches!(
        (kind, contract),
        (_, IdentityContract::Unavailable)
            | (
                StreamKind::Trade,
                IdentityContract::OpaqueTradeId | IdentityContract::NumericTradeId
            )
            | (StreamKind::Depth, IdentityContract::DepthIncrementSequence)
            | (StreamKind::Bar, IdentityContract::FinalBar)
            | (
                StreamKind::Tick | StreamKind::MarkPrice,
                IdentityContract::UniqueTimestamp
            )
    )
}

struct Scan<'a> {
    retained: Vec<&'a MarketEvent>,
    streams: Vec<StreamDeduplication>,
    input_sha256: String,
}

impl IdentityPolicy {
    fn scan<'a>(
        &self,
        definition: &DatasetDefinition,
        events: &'a [MarketEvent],
    ) -> Result<Scan<'a>, String> {
        declared_keys(definition)?;
        if self.schema_version != 1
            || self.source_id != definition.source_id
            || self.streams.len() != definition.streams.len()
        {
            return Err("identity policy does not match dataset source or streams".into());
        }
        let mut policies = HashMap::new();
        let mut stats = Vec::new();
        for item in &self.streams {
            if !definition.streams.contains(&item.stream)
                || !compatible(item.stream.kind, item.contract)
                || policies
                    .insert(item.stream.clone(), (item.contract, stats.len()))
                    .is_some()
            {
                return Err("unknown, duplicate or incompatible identity contract".into());
            }
            stats.push(StreamDeduplication {
                stream: item.stream.clone(),
                input_count: 0,
                retained_count: 0,
                identical_duplicates_removed: 0,
                unidentified_retained: 0,
            });
        }
        let mut seen = HashMap::new();
        let mut retained = Vec::with_capacity(events.len());
        let mut input = EvidenceBuilder::default();
        let mut previous = None;
        for event in events {
            let at = event_timestamp(event);
            if previous.is_some_and(|t| at < t)
                || at.date_naive() < definition.start_day
                || at.date_naive() >= definition.end_day_exclusive
            {
                return Err("raw events regress in time or exceed dataset range".into());
            }
            previous = Some(at);
            let stream = DatasetStream {
                venue: event.venue(),
                symbol: event.symbol().into(),
                kind: StreamKind::of(event),
            };
            let &(contract, index) = policies
                .get(&stream)
                .ok_or("event outside identity policy")?;
            let bytes = serde_json::to_vec(event).map_err(|e| e.to_string())?;
            input.add(&bytes)?;
            let stat = &mut stats[index];
            stat.input_count += 1;
            if let Some(key) = identity(event, contract)? {
                // Bind the complete normalized MarketEvent encoding, not
                // merely its price/quantity. Conflicting corrections must
                // be explicitly reconciled by the source, not overwritten.
                let digest: [u8; 32] = Sha256::digest(&bytes).into();
                match seen.entry((index, key)) {
                    std::collections::hash_map::Entry::Occupied(prior) => {
                        if prior.get() != &digest {
                            return Err(format!(
                                "conflicting payload for source identity in {stream:?}"
                            ));
                        }
                        stat.identical_duplicates_removed += 1;
                        continue;
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(digest);
                    }
                }
            } else {
                stat.unidentified_retained += 1;
            }
            stat.retained_count += 1;
            retained.push(event);
        }
        Ok(Scan {
            retained,
            streams: stats,
            input_sha256: input.finish().1,
        })
    }

    /// Replay verification never silently removes records.
    pub fn verify_unique(
        &self,
        definition: &DatasetDefinition,
        events: &[MarketEvent],
    ) -> Result<(), String> {
        let scan = self.scan(definition, events)?;
        if scan.retained.len() != events.len() {
            return Err("normalized dataset contains duplicate source identities".into());
        }
        Ok(())
    }
}

/// Prepare a new export, preserving first occurrence and tie order.
/// Raw input remains untouched. The manifest binds the retained sequence;
/// the accompanying report records the original sequence digest and counts.
pub fn normalize_dataset(
    definition: DatasetDefinition,
    policy: IdentityPolicy,
    events: &[MarketEvent],
) -> Result<NormalizedDataset, String> {
    let scan = policy.scan(&definition, events)?;
    let retained: Vec<_> = scan.retained.into_iter().cloned().collect();
    let mut manifest = seal_dataset(definition, &retained)?;
    manifest.identity_policy = Some(policy.clone());
    let deduplication = DeduplicationReport {
        policy,
        dataset_id: manifest.definition.dataset_id.clone(),
        dataset_version: manifest.definition.version.clone(),
        input_sha256: scan.input_sha256,
        output_sha256: manifest.replay_sha256.clone(),
        streams: scan.streams,
    };
    Ok(NormalizedDataset {
        manifest,
        events: retained,
        deduplication,
    })
}
