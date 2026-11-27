//! F03.03: independently reconcile durable capture receipts and bind
//! the exact captured sequence to replay. Not an exchange attestation.
use crate::collector_segments::{CaptureSegmentation, Segmenter};
use crate::historical_dataset::{
    DatasetDefinition, DatasetManifest, DatasetStream, StreamKind, VerifiedDataset, seal_dataset,
};
use crate::market_data_source::event_timestamp;
use apexes_core::{AssetClass, Instrument, MarketEvent, Venue};
use apexes_storage::raw_depth_store::{RawDepthBatch, RawDepthReader};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const SOURCE_PREFIX: &str = "binance-raw:";
const MAX_BATCHES: usize = 100_000;
pub const MAX_JOURNAL_BYTES: usize = 64 * 1024 * 1024;
const MAX_BATCH_EVENTS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureMarket {
    Spot,
    UsdMPerpetual,
}

impl CaptureMarket {
    fn name(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::UsdMPerpetual => "usd_m_perpetual",
        }
    }

    fn asset_class(self) -> AssetClass {
        match self {
            Self::Spot => AssetClass::Spot,
            Self::UsdMPerpetual => AssetClass::Perpetual,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Header {
        schema_version: u32,
        run_id: String,
        symbol: String,
        market: CaptureMarket,
        started_at: DateTime<Utc>,
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
        reason: Option<String>,
        committed_batches: u64,
    },
}

#[derive(Debug)]
struct Intent {
    count: usize,
    hash: String,
    invalidations: usize,
}

#[derive(Debug)]
struct Journal {
    run_id: String,
    symbol: String,
    market: CaptureMarket,
    started_at: DateTime<Utc>,
    intents: Vec<Intent>,
}

fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn hash_valid(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn parse_record(line: Option<&str>) -> Result<Record, String> {
    serde_json::from_str(line.ok_or("missing receipt record")?)
        .map_err(|e| format!("invalid receipt record: {e}"))
}

impl Journal {
    fn parse(raw: &str) -> Result<Self, String> {
        if raw.len() > MAX_JOURNAL_BYTES || !raw.ends_with('\n') {
            return Err("oversized or unterminated receipt journal".into());
        }
        let mut lines = raw.lines();
        let Record::Header {
            schema_version: 1,
            run_id,
            symbol,
            market,
            started_at,
        } = parse_record(lines.next())?
        else {
            return Err("expected schema-v1 collector header".into());
        };
        if Uuid::parse_str(&run_id).is_err()
            || Uuid::parse_str(&run_id).is_ok_and(|id| id.to_string() != run_id)
            || symbol.is_empty()
            || !symbol
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        {
            return Err("invalid collector run identity or symbol".into());
        }
        let mut journal = Self {
            run_id,
            symbol,
            market,
            started_at,
            intents: vec![],
        };
        loop {
            let index = journal.intents.len() as u64;
            match parse_record(lines.next())? {
                Record::Intent {
                    batch_index,
                    expected_count,
                    payload_sha256,
                    invalidations,
                } if batch_index == index
                    && journal.intents.len() < MAX_BATCHES
                    && (1..=MAX_BATCH_EVENTS).contains(&expected_count)
                    && invalidations <= expected_count
                    && hash_valid(&payload_sha256) =>
                {
                    if !matches!(parse_record(lines.next())?, Record::Commit { batch_index } if batch_index == index)
                    {
                        return Err("missing or mismatched batch commit".into());
                    }
                    journal.intents.push(Intent {
                        count: expected_count,
                        hash: payload_sha256,
                        invalidations,
                    });
                }
                Record::Terminal {
                    stopped: true,
                    reason: None,
                    committed_batches,
                } if committed_batches == index && lines.next().is_none() => return Ok(journal),
                _ => return Err("invalid intent sequence or unsuccessful terminal receipt".into()),
            }
        }
    }

    fn source_id(&self) -> String {
        format!("{SOURCE_PREFIX}{}:{}", self.market.name(), self.run_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorReport {
    pub source_id: String,
    pub run_id: String,
    pub symbol: String,
    pub market: CaptureMarket,
    pub started_at: DateTime<Utc>,
    pub journal_sha256: String,
    pub committed_batches: usize,
    pub event_count: usize,
    pub invalidations: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segmentation: Option<CaptureSegmentation>,
    pub independently_attested: bool,
    pub upstream_completeness_verified: bool,
}

#[derive(Debug, Serialize)]
pub struct ReconciledCapture {
    pub report: CollectorReport,
    pub events: Vec<MarketEvent>,
}

/// Preserve the original JSONL journal, rather than synthesizing receipts
/// from the replay input. Registry evidence is optional for legacy sources.
#[derive(Debug, Clone)]
pub struct CollectorEvidence {
    pub journal: String,
}

struct Verifier<'a> {
    journal: &'a Journal,
    previous: Option<DateTime<Utc>>,
    count: usize,
    invalidations: usize,
    segmenter: Segmenter,
}

impl<'a> Verifier<'a> {
    fn new(journal: &'a Journal) -> Self {
        Self {
            journal,
            previous: None,
            count: 0,
            invalidations: 0,
            segmenter: Segmenter::new(journal.market),
        }
    }

    fn verify_batch(
        &mut self,
        index: usize,
        row: &RawDepthBatch,
    ) -> Result<Vec<MarketEvent>, String> {
        let intent = self
            .journal
            .intents
            .get(index)
            .ok_or("unexpected extra batch")?;
        if row.run_id != self.journal.run_id
            || row.batch_index != index as u64
            || sha(row.events_json.as_bytes()) != intent.hash
        {
            return Err(format!(
                "batch {index}: identity or payload digest mismatch"
            ));
        }
        let events = row.events().map_err(|e| e.to_string())?;
        // Schema-v1 collector payloads are the canonical serialization.
        // Reject a matching receipt for a representation that replay
        // cannot reproduce from decoded events (including unknown fields).
        if serde_json::to_string(&events).map_err(|e| e.to_string())? != row.events_json {
            return Err(format!("batch {index}: noncanonical collector payload"));
        }
        if events.len() != intent.count {
            return Err(format!("batch {index}: event count mismatch"));
        }
        let mut invalidations = 0;
        for (offset, event) in events.iter().enumerate() {
            let MarketEvent::Depth(d) = event else {
                return Err("non-depth event in capture".into());
            };
            if d.symbol != self.journal.symbol
                || d.venue != Venue::Binance
                || self.previous.is_some_and(|at| d.timestamp < at)
            {
                return Err(format!("batch {index}: stream mismatch or time regression"));
            }
            self.segmenter.observe(self.count + offset, d)?;
            self.previous = Some(d.timestamp);
            invalidations += usize::from(d.is_invalidation());
        }
        if invalidations != intent.invalidations {
            return Err(format!("batch {index}: invalidation count mismatch"));
        }
        self.count += events.len();
        self.invalidations += invalidations;
        Ok(events)
    }

    fn finish(self, raw: &str) -> Result<CollectorReport, String> {
        Ok(CollectorReport {
            source_id: self.journal.source_id(),
            run_id: self.journal.run_id.clone(),
            symbol: self.journal.symbol.clone(),
            market: self.journal.market,
            started_at: self.journal.started_at,
            journal_sha256: sha(raw.as_bytes()),
            committed_batches: self.journal.intents.len(),
            event_count: self.count,
            invalidations: self.invalidations,
            segmentation: Some(self.segmenter.finish()?),
            independently_attested: false,
            upstream_completeness_verified: false,
        })
    }
}

pub fn reconcile(raw: &str, batches: &[RawDepthBatch]) -> Result<ReconciledCapture, String> {
    let journal = Journal::parse(raw)?;
    if batches.len() != journal.intents.len() {
        return Err("missing or unexpected persisted batch count".into());
    }
    let mut verifier = Verifier::new(&journal);
    let mut events = vec![];
    for (index, row) in batches.iter().enumerate() {
        events.extend(verifier.verify_batch(index, row)?);
    }
    Ok(ReconciledCapture {
        report: verifier.finish(raw)?,
        events,
    })
}

/// An export containing the original journal, exactly ordered events and
/// a manifest sealed only after receipt reconciliation. It is not
/// an upstream exchange attestation or a full-day coverage claim.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualifiedCapture {
    /// Where the rows were obtained, not an attestation of exchange completeness.
    pub qualification_method: QualificationMethod,
    pub journal: String,
    pub dataset: VerifiedDataset,
    pub events: Vec<MarketEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationMethod {
    CompleteExportSupplied,
    DirectClickHouseRead,
}

pub async fn qualify_recorded_run(
    raw: &str,
    reader: &RawDepthReader,
) -> Result<QualifiedCapture, String> {
    let journal = Journal::parse(raw)?;
    let rows = tokio::time::timeout(
        std::time::Duration::from_secs(90),
        reader.read_run(&journal.run_id, journal.intents.len()),
    )
    .await
    .map_err(|_| "raw capture qualification query timed out")?
    .map_err(|e| e.to_string())?;
    let mut qualified = qualify_export(raw, &rows)?;
    qualified.qualification_method = QualificationMethod::DirectClickHouseRead;
    Ok(qualified)
}

/// Qualify a supplied complete export against the original journal and
/// derive the dataset scope from its observations, never caller labels.
pub fn qualify_export(raw: &str, rows: &[RawDepthBatch]) -> Result<QualifiedCapture, String> {
    let capture = reconcile(raw, rows)?;
    if capture
        .report
        .segmentation
        .as_ref()
        .is_none_or(|s| s.segments.is_empty())
    {
        return Err("no synchronized capture segment for qualification".into());
    }
    let first = capture.events.first().ok_or("empty capture")?;
    let last = capture.events.last().ok_or("empty capture")?;
    let definition = DatasetDefinition {
        dataset_id: format!("binance-raw-{}", capture.report.run_id),
        version: "v1".into(),
        source_id: capture.report.source_id.clone(),
        start_day: event_timestamp(first).date_naive(),
        end_day_exclusive: event_timestamp(last)
            .date_naive()
            .succ_opt()
            .ok_or("capture date overflow")?,
        streams: vec![DatasetStream {
            venue: Venue::Binance,
            symbol: capture.report.symbol.clone(),
            kind: StreamKind::Depth,
        }],
    };
    let manifest = seal_dataset(definition, &capture.events)?;
    let mut dataset = manifest.verify(&capture.events)?;
    dataset.collector_receipts = Some(capture.report);
    Ok(QualifiedCapture {
        qualification_method: QualificationMethod::CompleteExportSupplied,
        journal: raw.into(),
        dataset,
        events: capture.events,
    })
}

impl CollectorEvidence {
    pub fn verify_replay(
        &self,
        manifest: &DatasetManifest,
        events: &[MarketEvent],
        instruments: &[Instrument],
    ) -> Result<CollectorReport, String> {
        let journal = Journal::parse(&self.journal)?;
        let expected_stream = DatasetStream {
            venue: Venue::Binance,
            symbol: journal.symbol.clone(),
            kind: StreamKind::Depth,
        };
        if journal.intents.is_empty()
            || manifest.definition.source_id != journal.source_id()
            || manifest.definition.streams.as_slice() != [expected_stream]
            || instruments.len() != 1
            || instruments[0].venue != Venue::Binance
            || instruments[0].symbol != journal.symbol
            || instruments[0].asset_class != journal.market.asset_class()
        {
            return Err(
                "collector identity, market or dataset scope mismatch (or empty capture)".into(),
            );
        }
        let mut verifier = Verifier::new(&journal);
        let mut remaining = events;
        for (index, intent) in journal.intents.iter().enumerate() {
            let (batch, rest) = remaining
                .split_at_checked(intent.count)
                .ok_or("missing replay events")?;
            let row = RawDepthBatch::new(&journal.run_id, index as u64, batch)
                .map_err(|e| e.to_string())?;
            verifier.verify_batch(index, &row)?;
            remaining = rest;
        }
        if !remaining.is_empty() {
            return Err("unexpected extra replay events".into());
        }
        let report = verifier.finish(&self.journal)?;
        if report
            .segmentation
            .as_ref()
            .is_none_or(|s| s.segments.is_empty())
        {
            return Err("no synchronized capture segment for replay".into());
        }
        Ok(report)
    }
}
