//! F04.04 — Durable submission-boundary evidence for historical replay.
//! A recorded attempt or error is uncertain until independently reconciled.
//! This module never resubmits orders or restores an active trading session.
use apexes_core::{ApexesError, AssetClass, Order, OrderStatus, OrderType, Side, Venue};
use apexes_strategy::{IntentId, OrderIntent, ParentOrderId};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

const MAX_BYTES: u64 = 64 * 1024 * 1024;
const MAX_LINE: usize = 64 * 1024;
const SCHEMA: u32 = 1;

#[derive(Debug, Clone)]
pub struct SubmissionJournalPolicy {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionRoute {
    Direct,
    EmsParent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attempt {
    pub intent_id: u64,
    pub at: DateTime<Utc>,
    pub route: SubmissionRoute,
    pub symbol: String,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubmissionResult {
    OrderReturned {
        order_id: Uuid,
        status: OrderStatus,
    },
    ParentReturned {
        parent_id: Uuid,
    },
    /// Error or missing response cannot prove absence of remote side effects.
    UncertainError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredAttempt {
    pub attempt: Attempt,
    pub result: Option<SubmissionResult>,
}

#[derive(Debug)]
pub struct RecoveredJournal {
    pub run_id: Uuid,
    pub namespace: Uuid,
    pub replay_sha256: String,
    pub attempts: Vec<RecoveredAttempt>,
    /// Present only after successful finalization of the historical report.
    pub report_sha256: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Event {
    Header {
        schema: u32,
        run_id: Uuid,
        namespace: Uuid,
        replay_sha256: String,
    },
    Begin {
        attempt: Attempt,
    },
    End {
        intent_id: u64,
        result: SubmissionResult,
    },
    Finished {
        report_sha256: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    seq: u64,
    previous: String,
    event: Event,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    record: Record,
    sha256: String,
}

fn invalid(reason: impl std::fmt::Display) -> ApexesError {
    ApexesError::InvalidBacktestConfig {
        reason: format!("submission journal: {reason}"),
    }
}
fn digest(record: &Record) -> Result<String, ApexesError> {
    let bytes = serde_json::to_vec(record).map_err(invalid)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
fn hash_valid(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn route_matches(route: SubmissionRoute, result: &SubmissionResult) -> bool {
    matches!(
        (route, result),
        (_, SubmissionResult::UncertainError)
            | (
                SubmissionRoute::Direct,
                SubmissionResult::OrderReturned { .. }
            )
            | (
                SubmissionRoute::EmsParent,
                SubmissionResult::ParentReturned { .. }
            )
    )
}

/// Single writer. No reopen-for-append or retry after I/O failure.
pub(crate) struct Journal {
    file: File,
    seq: u64,
    previous: String,
    bytes: u64,
    pending: Option<(u64, SubmissionRoute)>,
    seen: HashSet<u64>,
    failed: bool,
}

impl Journal {
    pub(crate) fn create(
        policy: &SubmissionJournalPolicy,
        namespace: Uuid,
        replay_sha256: String,
    ) -> Result<Self, ApexesError> {
        if !hash_valid(&replay_sha256) {
            return Err(invalid("invalid dataset digest"));
        }
        let directory = policy
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&policy.path).map_err(invalid)?;
        let mut journal = Self {
            file,
            seq: 0,
            previous: String::new(),
            bytes: 0,
            pending: None,
            seen: HashSet::new(),
            failed: false,
        };
        journal.append(Event::Header {
            schema: SCHEMA,
            run_id: Uuid::new_v4(),
            namespace,
            replay_sha256,
        })?;
        File::open(directory)
            .and_then(|f| f.sync_all())
            .map_err(invalid)?;
        Ok(journal)
    }

    fn append(&mut self, event: Event) -> Result<(), ApexesError> {
        if self.failed {
            return Err(invalid("writer poisoned"));
        }
        // Poison before any potentially partial write. Only a fully synced
        // record advances the chain.
        self.failed = true;
        let record = Record {
            seq: self.seq,
            previous: self.previous.clone(),
            event,
        };
        let sha256 = digest(&record)?;
        let mut line = serde_json::to_vec(&Envelope {
            record,
            sha256: sha256.clone(),
        })
        .map_err(invalid)?;
        line.push(b'\n');
        let bytes = self
            .bytes
            .checked_add(line.len() as u64)
            .ok_or_else(|| invalid("size overflow"))?;
        if bytes > MAX_BYTES || line.len() > MAX_LINE {
            return Err(invalid("size limit exceeded"));
        }
        let next = self
            .seq
            .checked_add(1)
            .ok_or_else(|| invalid("sequence exhausted"))?;
        self.file
            .write_all(&line)
            .and_then(|_| self.file.sync_all())
            .map_err(invalid)?;
        self.seq = next;
        self.bytes = bytes;
        self.previous = sha256;
        self.failed = false;
        Ok(())
    }

    pub(crate) fn begin(
        &mut self,
        id: Option<IntentId>,
        at: DateTime<Utc>,
        intent: &OrderIntent,
    ) -> Result<(), ApexesError> {
        if self.pending.is_some() {
            return Err(invalid("unresolved attempt"));
        }
        let id = id
            .filter(|id| id.0 != 0)
            .ok_or_else(|| invalid("missing intent identity"))?
            .0;
        if self.seen.contains(&id) {
            return Err(invalid("duplicate intent identity"));
        }
        let route = if intent.execution_algo.is_some() {
            SubmissionRoute::EmsParent
        } else {
            SubmissionRoute::Direct
        };
        let attempt = Attempt {
            intent_id: id,
            at,
            route,
            symbol: intent.symbol.clone(),
            venue: intent.venue,
            asset_class: intent.asset_class,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: intent.price,
            stop_price: intent.stop_price,
        };
        self.append(Event::Begin { attempt })?;
        self.seen.insert(id);
        self.pending = Some((id, route));
        Ok(())
    }

    pub(crate) fn end(&mut self, result: SubmissionResult) -> Result<(), ApexesError> {
        let (intent_id, route) = self
            .pending
            .ok_or_else(|| invalid("result without attempt"))?;
        if !route_matches(route, &result) {
            return Err(invalid("result route mismatch"));
        }
        self.append(Event::End { intent_id, result })?;
        self.pending = None;
        Ok(())
    }

    pub(crate) fn order_returned(&mut self, order: &Order) -> Result<(), ApexesError> {
        self.end(SubmissionResult::OrderReturned {
            order_id: order.id.0,
            status: order.status,
        })
    }
    pub(crate) fn parent_returned(&mut self, parent: ParentOrderId) -> Result<(), ApexesError> {
        self.end(SubmissionResult::ParentReturned {
            parent_id: parent.0,
        })
    }
    pub(crate) fn finish(mut self, report: &crate::BacktestReport) -> Result<(), ApexesError> {
        if self.pending.is_some() {
            return Err(invalid("unresolved attempt at finish"));
        }
        let report_sha256 =
            crate::replay_identity::canonical_report_sha256(report).map_err(invalid)?;
        self.append(Event::Finished { report_sha256 })
    }
}

impl RecoveredJournal {
    /// Recover a structurally valid journal at a complete record boundary.
    /// A torn/truncated line fails closed; no automatic repair or append.
    /// The caller must not read concurrently with an active writer.
    pub fn load(path: &Path) -> Result<Self, ApexesError> {
        if !fs::symlink_metadata(path).map_err(invalid)?.is_file() {
            return Err(invalid("not a regular file"));
        }
        let file = File::open(path).map_err(invalid)?;
        if !file.metadata().map_err(invalid)?.is_file() {
            return Err(invalid("not a regular file"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(invalid)?;
        if bytes.len() as u64 > MAX_BYTES || bytes.last() != Some(&b'\n') {
            return Err(invalid("oversized or incomplete journal"));
        }
        let mut previous = String::new();
        let mut recovered: Option<Self> = None;
        let mut ids = HashSet::new();
        let mut pending = false;
        for (seq, line) in bytes[..bytes.len() - 1].split(|b| *b == b'\n').enumerate() {
            if line.len() >= MAX_LINE {
                return Err(invalid("record exceeds limit"));
            }
            let envelope: Envelope = serde_json::from_slice(line).map_err(invalid)?;
            let r = envelope.record;
            if r.seq != seq as u64 || r.previous != previous || digest(&r)? != envelope.sha256 {
                return Err(invalid("record chain mismatch"));
            }
            previous = envelope.sha256;
            match r.event {
                Event::Header {
                    schema,
                    run_id,
                    namespace,
                    replay_sha256,
                } if seq == 0
                    && schema == SCHEMA
                    && !run_id.is_nil()
                    && hash_valid(&replay_sha256) =>
                {
                    recovered = Some(Self {
                        run_id,
                        namespace,
                        replay_sha256,
                        attempts: vec![],
                        report_sha256: None,
                    });
                }
                event => {
                    let state = recovered
                        .as_mut()
                        .ok_or_else(|| invalid("invalid header"))?;
                    if state.report_sha256.is_some() {
                        return Err(invalid("record after finish"));
                    }
                    match event {
                        Event::Begin { attempt }
                            if !pending
                                && attempt.intent_id != 0
                                && ids.insert(attempt.intent_id) =>
                        {
                            state.attempts.push(RecoveredAttempt {
                                attempt,
                                result: None,
                            });
                            pending = true;
                        }
                        Event::End { intent_id, result } if pending => {
                            let last = state
                                .attempts
                                .last_mut()
                                .ok_or_else(|| invalid("missing attempt"))?;
                            if last.attempt.intent_id != intent_id
                                || !route_matches(last.attempt.route, &result)
                            {
                                return Err(invalid("result identity or route mismatch"));
                            }
                            last.result = Some(result);
                            pending = false;
                        }
                        Event::Finished { report_sha256 }
                            if !pending && hash_valid(&report_sha256) =>
                        {
                            state.report_sha256 = Some(report_sha256)
                        }
                        _ => return Err(invalid("invalid record transition")),
                    }
                }
            }
        }
        recovered.ok_or_else(|| invalid("missing header"))
    }

    /// Includes attempts with no returned result and explicit submission errors.
    pub fn uncertain_attempts(&self) -> impl Iterator<Item = &Attempt> {
        self.attempts
            .iter()
            .filter(|a| matches!(a.result, None | Some(SubmissionResult::UncertainError)))
            .map(|a| &a.attempt)
    }

    pub fn matches_report(&self, report: &crate::BacktestReport) -> Result<bool, ApexesError> {
        Ok(report
            .replay_identity
            .is_some_and(|id| id.namespace == self.namespace)
            && report
                .historical_dataset
                .as_ref()
                .is_some_and(|d| d.manifest.replay_sha256 == self.replay_sha256)
            && self.report_sha256.as_ref()
                == Some(&crate::replay_identity::canonical_report_sha256(report).map_err(invalid)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_strategy::OrderRouting;

    struct Temp(PathBuf);
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn directory() -> Temp {
        let path = std::env::temp_dir().join(format!("apexes-journal-test-{}", Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Temp(path)
    }
    fn intent() -> OrderIntent {
        OrderIntent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: Decimal::ONE,
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }
    fn create(path: &Path) -> Journal {
        Journal::create(
            &SubmissionJournalPolicy {
                path: path.to_owned(),
            },
            Uuid::from_u128(1),
            "a".repeat(64),
        )
        .unwrap()
    }
    #[test]
    fn interrupted_attempt_and_uncertain_error_are_never_confirmed_rejections() {
        let dir = directory();
        let path = dir.0.join("journal.jsonl");
        let mut journal = create(&path);
        journal
            .begin(Some(IntentId(1)), DateTime::UNIX_EPOCH, &intent())
            .unwrap();
        journal.end(SubmissionResult::UncertainError).unwrap();
        journal
            .begin(Some(IntentId(2)), DateTime::UNIX_EPOCH, &intent())
            .unwrap();
        // Simulate interruption before the response is recorded. No finish.
        drop(journal);
        let recovered = RecoveredJournal::load(&path).unwrap();
        assert_eq!(recovered.report_sha256, None);
        assert_eq!(
            recovered.attempts[0].result,
            Some(SubmissionResult::UncertainError)
        );
        assert_eq!(recovered.attempts[1].result, None);
        assert_eq!(
            recovered
                .uncertain_attempts()
                .map(|a| a.intent_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let bytes = fs::read(&path).unwrap();
        assert!(
            Journal::create(
                &SubmissionJournalPolicy { path: path.clone() },
                Uuid::from_u128(1),
                "a".repeat(64)
            )
            .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let torn = dir.0.join("torn.jsonl");
        fs::write(&torn, &bytes[..bytes.len() - 1]).unwrap();
        assert!(RecoveredJournal::load(&torn).is_err());
        let mut changed = bytes.clone();
        let offset = changed.iter().position(|b| *b == b'a').unwrap();
        changed[offset] = b'b';
        fs::write(&torn, changed).unwrap();
        assert!(RecoveredJournal::load(&torn).is_err());
    }

    #[test]
    fn writer_refuses_invalid_transition_and_poisoned_writer_does_not_append() {
        let dir = directory();
        let path = dir.0.join("journal.jsonl");
        let mut journal = create(&path);
        assert!(journal.end(SubmissionResult::UncertainError).is_err());
        assert!(
            journal
                .begin(None, DateTime::UNIX_EPOCH, &intent())
                .is_err()
        );
        journal
            .begin(Some(IntentId(1)), DateTime::UNIX_EPOCH, &intent())
            .unwrap();
        assert!(
            journal
                .begin(Some(IntentId(2)), DateTime::UNIX_EPOCH, &intent())
                .is_err()
        );
        assert!(
            journal
                .parent_returned(ParentOrderId(Uuid::from_u128(3)))
                .is_err()
        );
        journal.end(SubmissionResult::UncertainError).unwrap();
        assert!(
            journal
                .begin(Some(IntentId(1)), DateTime::UNIX_EPOCH, &intent())
                .is_err()
        );
        // Force the limit before append, then ensure no retry even after
        // restoring the artificial counter.
        let actual = journal.bytes;
        journal.bytes = MAX_BYTES;
        assert!(
            journal
                .begin(Some(IntentId(2)), DateTime::UNIX_EPOCH, &intent())
                .is_err()
        );
        journal.bytes = actual;
        assert!(
            journal
                .begin(Some(IntentId(2)), DateTime::UNIX_EPOCH, &intent())
                .is_err()
        );
        drop(journal);
        assert_eq!(fs::metadata(&path).unwrap().len(), actual);
        assert_eq!(RecoveredJournal::load(&path).unwrap().attempts.len(), 1);
    }

    #[test]
    fn reader_checks_transitions_even_with_recomputed_chain() {
        let dir = directory();
        let path = dir.0.join("journal.jsonl");
        let header = || Event::Header {
            schema: SCHEMA,
            run_id: Uuid::from_u128(1),
            namespace: Uuid::from_u128(2),
            replay_sha256: "a".repeat(64),
        };
        let begin = || Event::Begin {
            attempt: Attempt {
                intent_id: 1,
                at: DateTime::UNIX_EPOCH,
                route: SubmissionRoute::Direct,
                symbol: "BTCUSDT".into(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: Decimal::ONE,
                price: None,
                stop_price: None,
            },
        };
        let end = || Event::End {
            intent_id: 1,
            result: SubmissionResult::UncertainError,
        };
        let finish = || Event::Finished {
            report_sha256: "b".repeat(64),
        };
        let write = |events: Vec<Event>| {
            let mut bytes = Vec::new();
            let mut previous = String::new();
            for (seq, event) in events.into_iter().enumerate() {
                let record = Record {
                    seq: seq as u64,
                    previous,
                    event,
                };
                previous = digest(&record).unwrap();
                bytes.extend(
                    serde_json::to_vec(&Envelope {
                        record,
                        sha256: previous.clone(),
                    })
                    .unwrap(),
                );
                bytes.push(b'\n');
            }
            fs::write(&path, bytes).unwrap();
        };
        for events in [
            vec![header(), end()],
            vec![header(), begin(), begin()],
            vec![
                header(),
                begin(),
                Event::End {
                    intent_id: 2,
                    result: SubmissionResult::UncertainError,
                },
            ],
            vec![
                header(),
                begin(),
                Event::End {
                    intent_id: 1,
                    result: SubmissionResult::ParentReturned {
                        parent_id: Uuid::from_u128(3),
                    },
                },
            ],
            vec![header(), begin(), finish()],
            vec![header(), begin(), end(), begin()],
            vec![header(), finish(), begin()],
            vec![header(), header()],
        ] {
            write(events);
            assert!(RecoveredJournal::load(&path).is_err());
        }
        write(vec![header(), begin(), end(), finish()]);
        let recovered = RecoveredJournal::load(&path).unwrap();
        assert_eq!(recovered.report_sha256, Some("b".repeat(64)));
        assert_eq!(recovered.uncertain_attempts().count(), 1);
        write(vec![header()]);
        let recovered = RecoveredJournal::load(&path).unwrap();
        assert!(recovered.attempts.is_empty());
        assert!(recovered.report_sha256.is_none());
    }
}
