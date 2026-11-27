//! F03.02/F03.03 — Versioned replay input identity and explicit UTC-day
//! coverage. Sealing an export fingerprints its contents; it does not
//! certify completeness of the upstream exchange feed.

use crate::market_data_source::{event_timestamp, observation_timestamp};
use apexes_core::{Instrument, MarketEvent, Venue};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::ops::Range;

const SCHEMA: u32 = 1;
pub const OBSERVATION_SCHEMA: u32 = 2;
const MAX_PARTITIONS: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Trade,
    Tick,
    Depth,
    Bar,
    MarkPrice,
}

impl StreamKind {
    pub(crate) fn of(event: &MarketEvent) -> Self {
        match event {
            MarketEvent::Trade(_) => Self::Trade,
            MarketEvent::Tick(_) => Self::Tick,
            MarketEvent::Depth(_) => Self::Depth,
            MarketEvent::Bar(_) => Self::Bar,
            MarketEvent::MarkPrice(_) => Self::MarkPrice,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetStream {
    pub venue: Venue,
    pub symbol: String,
    pub kind: StreamKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetDefinition {
    pub dataset_id: String,
    pub version: String,
    /// Non-secret source/export identifier; caller-declared, not attested.
    pub source_id: String,
    pub start_day: NaiveDate,
    pub end_day_exclusive: NaiveDate,
    pub streams: Vec<DatasetStream>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionEvidence {
    pub stream: DatasetStream,
    pub day: NaiveDate,
    pub event_count: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetManifest {
    pub schema_version: u32,
    pub definition: DatasetDefinition,
    /// One entry for every declared stream/day, including empty days.
    pub partitions: Vec<PartitionEvidence>,
    /// Hash over the entire ordered input, including inter-stream tie order.
    pub replay_sha256: String,
    /// Explicit identity contract for normalized datasets; legacy manifests omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_policy: Option<crate::source_identity::IdentityPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedDataset {
    pub manifest: DatasetManifest,
    pub event_count: u64,
    pub empty_partitions: usize,
    /// Integrity against supplied manifest only; no independent upstream
    /// attestation is implemented in this version.
    pub upstream_completeness_verified: bool,
    #[serde(default)]
    pub source_coverage: Option<crate::source_coverage::CoverageReport>,
    #[serde(default)]
    pub depth_ranges: Option<crate::depth_ranges::DepthRangeReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collector_receipts: Option<crate::collector_receipts::CollectorReport>,
}

#[derive(Default)]
pub(crate) struct EvidenceBuilder {
    count: u64,
    digest: Sha256,
}

impl EvidenceBuilder {
    pub(crate) fn add(&mut self, bytes: &[u8]) -> Result<(), String> {
        let len = u64::try_from(bytes.len()).map_err(|_| "event size overflow")?;
        self.count = self.count.checked_add(1).ok_or("event count overflow")?;
        self.digest.update(len.to_be_bytes());
        self.digest.update(bytes);
        Ok(())
    }

    pub(crate) fn finish(self) -> (u64, String) {
        (self.count, format!("{:x}", self.digest.finalize()))
    }
}

type PartitionKey = (DatasetStream, NaiveDate);

pub(crate) fn declared_keys(def: &DatasetDefinition) -> Result<Vec<PartitionKey>, String> {
    if [&def.dataset_id, &def.version, &def.source_id]
        .iter()
        .any(|s| s.trim().is_empty())
        || def.streams.is_empty()
        || def.start_day >= def.end_day_exclusive
    {
        return Err("nonempty identity, streams and positive date range required".into());
    }
    let mut seen = HashSet::new();
    for stream in &def.streams {
        if stream.symbol.trim().is_empty() || !seen.insert(stream) {
            return Err("empty or duplicate dataset stream".into());
        }
    }
    let days = usize::try_from((def.end_day_exclusive - def.start_day).num_days())
        .map_err(|_| "invalid dataset day range")?;
    let size = days
        .checked_mul(def.streams.len())
        .filter(|n| *n <= MAX_PARTITIONS)
        .ok_or("dataset exceeds partition limit")?;
    let mut keys = Vec::with_capacity(size);
    for stream in &def.streams {
        let mut day = def.start_day;
        while day < def.end_day_exclusive {
            keys.push((stream.clone(), day));
            day = day.succ_opt().ok_or("date overflow")?;
        }
    }
    Ok(keys)
}

/// Fingerprint the exact serialized event sequence, with a big-endian
/// u64 byte length before each event to unambiguously frame the digest.
/// Schema v1 uses serde_json's MarketEvent serialization; changing that
/// representation requires resealing under a new schema.
pub fn seal_dataset(
    definition: DatasetDefinition,
    events: &[MarketEvent],
) -> Result<DatasetManifest, String> {
    seal_ordered(definition, events, SCHEMA)
}

/// Seal an explicitly observation-ordered sequence; never sort or deduplicate.
pub fn seal_observation_dataset(
    definition: DatasetDefinition,
    events: &[MarketEvent],
) -> Result<DatasetManifest, String> {
    seal_ordered(definition, events, OBSERVATION_SCHEMA)
}

fn seal_ordered(
    definition: DatasetDefinition,
    events: &[MarketEvent],
    schema: u32,
) -> Result<DatasetManifest, String> {
    let keys = declared_keys(&definition)?;
    if schema == OBSERVATION_SCHEMA {
        if definition
            .source_id
            .starts_with(crate::collector_receipts::SOURCE_PREFIX)
        {
            return Err("raw collector manifests retain schema 1".into());
        }
        crate::replay_identity::validate_observation_order(events, true)?;
    }
    let mut partitions: HashMap<_, EvidenceBuilder> = keys
        .iter()
        .cloned()
        .map(|key| (key, EvidenceBuilder::default()))
        .collect();
    let mut replay = EvidenceBuilder::default();
    if schema == OBSERVATION_SCHEMA {
        replay.digest.update(b"apexes-observation-replay-v2\0");
    }
    let end = definition
        .end_day_exclusive
        .and_hms_opt(0, 0, 0)
        .ok_or("invalid end date")?
        .and_utc();
    let mut previous = None;
    let mut identified_trades = HashSet::new();
    for event in events {
        if let MarketEvent::Depth(depth) = event {
            depth.validate_source_range()?;
        }
        let timestamp = event_timestamp(event);
        let order_time = if schema == OBSERVATION_SCHEMA {
            let observed = observation_timestamp(event);
            if observed > end {
                return Err("observation outside declared replay horizon".into());
            }
            observed
        } else {
            timestamp
        };
        if previous.is_some_and(|at| order_time < at) {
            return Err("historical events regress in time".into());
        }
        previous = Some(order_time);
        let stream = DatasetStream {
            venue: event.venue(),
            symbol: event.symbol().into(),
            kind: StreamKind::of(event),
        };
        let key = (stream, timestamp.date_naive());
        let partition = partitions
            .get_mut(&key)
            .ok_or_else(|| format!("event outside declared stream/day: {key:?}"))?;
        // A known trade ID is unique within the declared source's venue
        // and symbol. Do not silently discard duplicates or guess IDs
        // for trades without one.
        if let MarketEvent::Trade(t) = event
            && let Some(id) = &t.trade_id
            && (id.trim().is_empty()
                || !identified_trades.insert((t.venue, t.symbol.as_str(), id.as_str())))
        {
            return Err("empty or duplicate identified trade".into());
        }
        let bytes = serde_json::to_vec(event).map_err(|e| format!("serialize event: {e}"))?;
        partition.add(&bytes)?;
        replay.add(&bytes)?;
    }
    let partitions = keys
        .into_iter()
        .map(|key| {
            let builder = partitions
                .remove(&key)
                .ok_or("internal missing partition")?;
            let (event_count, sha256) = builder.finish();
            Ok(PartitionEvidence {
                stream: key.0,
                day: key.1,
                event_count,
                sha256,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(DatasetManifest {
        schema_version: schema,
        definition,
        partitions,
        replay_sha256: replay.finish().1,
        identity_policy: None,
    })
}

fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl DatasetManifest {
    /// Require the declared market universe and date range to match the
    /// actual backtest configuration. Asset class cannot be inferred from
    /// MarketEvent; ambiguous same-venue/symbol instruments are refused.
    pub fn verify_scope(
        &self,
        instruments: &[Instrument],
        range: &Range<NaiveDate>,
    ) -> Result<(), String> {
        let declared: HashSet<_> = self
            .definition
            .streams
            .iter()
            .map(|s| (s.venue, s.symbol.as_str()))
            .collect();
        let configured: HashSet<_> = instruments
            .iter()
            .map(|i| (i.venue, i.symbol.as_str()))
            .collect();
        if configured.len() != instruments.len()
            || declared != configured
            || self.definition.start_day != range.start
            || self.definition.end_day_exclusive != range.end
        {
            return Err("dataset scope differs from backtest instruments or date range".into());
        }
        Ok(())
    }

    pub fn verify(&self, events: &[MarketEvent]) -> Result<VerifiedDataset, String> {
        if !matches!(self.schema_version, SCHEMA | OBSERVATION_SCHEMA)
            || !valid_hash(&self.replay_sha256)
        {
            return Err("unsupported manifest schema or invalid replay digest".into());
        }
        let keys = declared_keys(&self.definition)?;
        if self.partitions.len() != keys.len() {
            return Err("missing or excess stream/day partition".into());
        }
        let mut expected = HashMap::new();
        for partition in &self.partitions {
            if !valid_hash(&partition.sha256)
                || expected
                    .insert((partition.stream.clone(), partition.day), partition)
                    .is_some()
            {
                return Err("invalid digest or duplicate partition".into());
            }
        }
        let actual = seal_ordered(self.definition.clone(), events, self.schema_version)?;
        if let Some(policy) = &self.identity_policy {
            policy.verify_unique_ordered(
                &self.definition,
                events,
                self.schema_version == OBSERVATION_SCHEMA,
            )?;
        }
        for partition in &actual.partitions {
            let key = (partition.stream.clone(), partition.day);
            let Some(prior) = expected.get(&key) else {
                return Err(format!("missing stream/day partition: {key:?}"));
            };
            if *prior != partition {
                return Err(format!(
                    "historical partition count or digest mismatch: {key:?}"
                ));
            }
        }
        if actual.replay_sha256 != self.replay_sha256 {
            return Err("ordered replay digest mismatch".into());
        }
        Ok(VerifiedDataset {
            manifest: self.clone(),
            event_count: u64::try_from(events.len()).map_err(|_| "event count overflow")?,
            empty_partitions: actual
                .partitions
                .iter()
                .filter(|p| p.event_count == 0)
                .count(),
            upstream_completeness_verified: false,
            source_coverage: None,
            depth_ranges: None,
            collector_receipts: None,
        })
    }
}

/// Explicit migration result; provenance fields are caller-visible evidence
/// of this operation, not independently attested source metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationExport {
    pub source_version: String,
    pub source_replay_sha256: String,
    pub manifest: DatasetManifest,
    pub events: Vec<MarketEvent>,
}

/// Verify before deriving the new order. Equal observation-time ties keep
/// their original occurrence order; no event is added or removed.
pub fn migrate_observation_dataset(
    source: &DatasetManifest,
    events: &[MarketEvent],
    new_version: &str,
) -> Result<ObservationExport, String> {
    if source.schema_version != SCHEMA
        || new_version.trim().is_empty()
        || new_version == source.definition.version
    {
        return Err("migration requires schema 1 and a distinct nonempty dataset version".into());
    }
    source.verify(events)?;
    let mut ordered = events.to_vec();
    ordered.sort_by_key(observation_timestamp);
    let mut definition = source.definition.clone();
    definition.version = new_version.to_owned();
    let mut manifest = seal_observation_dataset(definition, &ordered)?;
    manifest.identity_policy = source.identity_policy.clone();
    manifest.verify(&ordered)?;
    Ok(ObservationExport {
        source_version: source.definition.version.clone(),
        source_replay_sha256: source.replay_sha256.clone(),
        manifest,
        events: ordered,
    })
}
