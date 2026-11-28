//! F04.04 — Opt-in, report-bound lifecycle evidence for completed replays.
//! This is not a trading-state checkpoint or a crash recovery journal.
use crate::{BacktestReport, replay_identity::canonical_report_sha256};
use apexes_core::{ApexesError, FillId, OrderId};
use apexes_strategy::{IntentId, ParentOrderId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

const SCHEMA: u32 = 1;
const MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Registry opt-in for a verified, deterministic historical replay.
/// The destination must not already exist.
#[derive(Debug, Clone)]
pub struct LifecycleArchivePolicy {
    pub path: PathBuf,
}

/// Final local correlation state, not a complete event history.
/// Sorted arrays avoid nondeterministic map order and duplicate JSON keys.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Evidence {
    pub(crate) last_intent: u64,
    pub(crate) orders: Vec<(Uuid, Option<u64>)>,
    pub(crate) parents: Vec<(Uuid, Option<u64>)>,
    pub(crate) children: Vec<(Uuid, Option<Uuid>)>,
    pub(crate) ambiguous_parents: Vec<Uuid>,
    pub(crate) seen_fills: Vec<Uuid>,
    pub(crate) parent_executed: Vec<(Uuid, Option<Decimal>)>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    schema_version: u32,
    namespace: Uuid,
    report_sha256: String,
    evidence: Evidence,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    payload: Payload,
    sha256: String,
}

/// Validated inspection-only snapshot. Construct via load_verified.
#[derive(Debug)]
pub struct LifecycleArchive {
    payload: Payload,
}

fn invalid(reason: impl Into<String>) -> ApexesError {
    ApexesError::InvalidBacktestConfig {
        reason: format!("lifecycle archive: {}", reason.into()),
    }
}

fn digest(payload: &Payload) -> Result<String, ApexesError> {
    let bytes = serde_json::to_vec(payload).map_err(|e| invalid(e.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn sorted_unique<T: Ord>(items: impl Iterator<Item = T>) -> bool {
    let mut previous = None;
    for item in items {
        if previous.as_ref().is_some_and(|prior| prior >= &item) {
            return false;
        }
        previous = Some(item);
    }
    true
}

impl Evidence {
    fn validate(&self, report: &BacktestReport) -> Result<(), ApexesError> {
        let valid_intent = |id: &Option<u64>| id.is_none_or(|i| i > 0 && i <= self.last_intent);
        if !sorted_unique(self.orders.iter().map(|x| x.0))
            || !sorted_unique(self.parents.iter().map(|x| x.0))
            || !sorted_unique(self.children.iter().map(|x| x.0))
            || !sorted_unique(self.ambiguous_parents.iter().copied())
            || !sorted_unique(self.seen_fills.iter().copied())
            || !sorted_unique(self.parent_executed.iter().map(|x| x.0))
            || !self
                .orders
                .iter()
                .chain(&self.parents)
                .all(|(_, id)| valid_intent(id))
        {
            return Err(invalid(
                "invalid ordering, duplicate identity or intent ordinal",
            ));
        }
        let parents: BTreeMap<_, _> = self.parents.iter().copied().collect();
        let orders: BTreeMap<_, _> = self.orders.iter().copied().collect();
        let children: BTreeMap<_, _> = self.children.iter().copied().collect();
        if self.children.iter().any(|(child, parent)| {
            parent.is_some_and(|p| !parents.contains_key(&p) || orders.contains_key(child))
                || (parent.is_none() && orders.get(child).is_some_and(Option::is_some))
        }) || self
            .ambiguous_parents
            .iter()
            .any(|id| !parents.contains_key(id))
        {
            return Err(invalid("invalid child or ambiguous parent reference"));
        }
        let mut fills: Vec<_> = report.trades.iter().map(|f| f.id.0).collect();
        fills.sort_unstable();
        if fills != self.seen_fills {
            return Err(invalid(
                "notification IDs do not match unique report executions",
            ));
        }
        // Reconcile valid progress against the actual report executions.
        // Ambiguous parents are explicitly unavailable, not recomputed.
        let mut expected = BTreeMap::new();
        for fill in &report.trades {
            if let Some(Some(parent)) = children.get(&fill.order_id.0) {
                let total = expected.entry(*parent).or_insert(Some(Decimal::ZERO));
                *total = total
                    .filter(|_| fill.quantity > Decimal::ZERO)
                    .and_then(|q| q.checked_add(fill.quantity));
            }
        }
        for (id, total) in &self.parent_executed {
            if !parents.contains_key(id)
                || total.is_some_and(|v| v < Decimal::ZERO)
                || (self.ambiguous_parents.binary_search(id).is_ok() && total.is_some())
            {
                return Err(invalid("invalid parent progress"));
            }
            if self.ambiguous_parents.binary_search(id).is_err()
                && expected.remove(id) != Some(*total)
            {
                return Err(invalid("parent progress differs from report"));
            }
        }
        if expected
            .keys()
            .any(|id| self.ambiguous_parents.binary_search(id).is_err())
        {
            return Err(invalid("missing parent progress"));
        }
        Ok(())
    }
}

impl LifecycleArchivePolicy {
    pub(crate) fn validate_destination(&self) -> Result<(), ApexesError> {
        match fs::symlink_metadata(&self.path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(invalid(e.to_string())),
            Ok(_) => return Err(invalid("destination already exists")),
        }
        let directory = self
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if self.path.file_name().is_none() || !directory.is_dir() {
            return Err(invalid(
                "destination requires an existing directory and filename",
            ));
        }
        Ok(())
    }
}

impl LifecycleArchive {
    pub(crate) fn publish(
        path: &Path,
        report: &BacktestReport,
        evidence: Evidence,
    ) -> Result<(), ApexesError> {
        evidence.validate(report)?;
        let namespace = report
            .replay_identity
            .ok_or_else(|| invalid("missing deterministic replay identity"))?
            .namespace;
        let payload = Payload {
            schema_version: SCHEMA,
            namespace,
            report_sha256: canonical_report_sha256(report).map_err(|e| invalid(e.to_string()))?,
            evidence,
        };
        let archive = Envelope {
            sha256: digest(&payload)?,
            payload,
        };
        let bytes = serde_json::to_vec(&archive).map_err(|e| invalid(e.to_string()))?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(invalid("archive exceeds size limit"));
        }
        publish_new(path, &bytes).map_err(|e| invalid(e.to_string()))
    }

    /// Recover inspection-only evidence against the exact finalized report.
    /// Never install this data into an active executor or ledger.
    pub fn load_verified(path: &Path, report: &BacktestReport) -> Result<Self, ApexesError> {
        if !fs::symlink_metadata(path)
            .map_err(|e| invalid(e.to_string()))?
            .is_file()
        {
            return Err(invalid("not a regular file"));
        }
        let file = File::open(path).map_err(|e| invalid(e.to_string()))?;
        if !file
            .metadata()
            .map_err(|e| invalid(e.to_string()))?
            .is_file()
        {
            return Err(invalid("not a regular file"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| invalid(e.to_string()))?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(invalid("archive exceeds size limit"));
        }
        let archive: Envelope =
            serde_json::from_slice(&bytes).map_err(|e| invalid(e.to_string()))?;
        let p = &archive.payload;
        if p.schema_version != SCHEMA
            || digest(p)? != archive.sha256
            || report
                .replay_identity
                .is_none_or(|id| id.namespace != p.namespace)
            || canonical_report_sha256(report).map_err(|e| invalid(e.to_string()))?
                != p.report_sha256
        {
            return Err(invalid("version, checksum or report identity mismatch"));
        }
        p.evidence.validate(report)?;
        Ok(Self {
            payload: archive.payload,
        })
    }

    pub fn last_intent_ordinal(&self) -> u64 {
        self.payload.evidence.last_intent
    }
    pub fn parent_intent(&self, parent: ParentOrderId) -> Option<IntentId> {
        let parents = &self.payload.evidence.parents;
        parents
            .binary_search_by_key(&parent.0, |x| x.0)
            .ok()
            .and_then(|i| parents[i].1)
            .map(IntentId)
    }
    pub fn child_parent(&self, order: OrderId) -> Option<ParentOrderId> {
        let children = &self.payload.evidence.children;
        children
            .binary_search_by_key(&order.0, |x| x.0)
            .ok()
            .and_then(|i| children[i].1)
            .map(ParentOrderId)
    }
    pub fn order_intent(&self, order: OrderId) -> Option<IntentId> {
        if let Some(parent) = self.child_parent(order) {
            return self.parent_intent(parent);
        }
        let orders = &self.payload.evidence.orders;
        orders
            .binary_search_by_key(&order.0, |x| x.0)
            .ok()
            .and_then(|i| orders[i].1)
            .map(IntentId)
    }
    pub fn was_fill_notified(&self, fill: FillId) -> bool {
        self.payload
            .evidence
            .seen_fills
            .binary_search(&fill.0)
            .is_ok()
    }
    pub fn parent_executed_quantity(&self, parent: ParentOrderId) -> Option<Decimal> {
        let rows = &self.payload.evidence.parent_executed;
        rows.binary_search_by_key(&parent.0, |x| x.0)
            .ok()
            .and_then(|i| rows[i].1)
    }
}

/// Write and sync an exclusive temporary file on the destination filesystem,
/// then publish without replacement. Sync the directory entry as well.
fn publish_new(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let directory = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let temporary = directory.join(format!(".apexes-lifecycle-{}.tmp", Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::hard_link(&temporary, path)?;
        File::open(directory)?.sync_all()
    })();
    drop(file);
    let cleanup = fs::remove_file(&temporary);
    result.and(cleanup)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{performance::PerformanceTracker, replay_identity::ReplayIdentityPolicy};
    use apexes_core::{Fill, Side, Venue};

    struct Temp(PathBuf);
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn directory() -> Temp {
        let path = std::env::temp_dir().join(format!("apexes-archive-test-{}", Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Temp(path)
    }
    fn fixture() -> (BacktestReport, Evidence) {
        let id = Uuid::from_u128;
        let fill = Fill {
            id: FillId(id(4)),
            order_id: OrderId(id(3)),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            side: Side::Buy,
            price: Decimal::ONE,
            quantity: Decimal::ONE,
            fee: Decimal::ZERO,
            fee_currency: None,
            filled_at: chrono::DateTime::UNIX_EPOCH,
            leverage: None,
            margin_mode: None,
        };
        let report = BacktestReport {
            overall: PerformanceTracker::new(Decimal::ZERO).summary().into(),
            per_symbol: Default::default(),
            per_venue: Default::default(),
            equity_curve: vec![],
            trades: vec![fill],
            spot_pnl: Default::default(),
            simulated_fees: None,
            simulated_execution: None,
            simulated_depth: None,
            event_latency: None,
            historical_dataset: None,
            replay_identity: Some(ReplayIdentityPolicy { namespace: id(9) }),
            execution_performance: None,
            drawdown_duration: None,
            benchmark: None,
            portfolio_benchmark: None,
            funding: None,
        };
        let evidence = Evidence {
            last_intent: 2,
            orders: vec![(id(1), Some(1))],
            parents: vec![(id(2), Some(2))],
            children: vec![(id(3), Some(id(2)))],
            ambiguous_parents: vec![],
            seen_fills: vec![id(4)],
            parent_executed: vec![(id(2), Some(Decimal::ONE))],
        };
        (report, evidence)
    }

    #[test]
    fn archive_atomic_publish_and_read_only_lookup() {
        let tmp = directory();
        let path = tmp.0.join("archive.json");
        let (report, evidence) = fixture();
        LifecycleArchive::publish(&path, &report, evidence.clone()).unwrap();
        let archive = LifecycleArchive::load_verified(&path, &report).unwrap();
        assert_eq!(
            archive.order_intent(OrderId(Uuid::from_u128(1))),
            Some(IntentId(1))
        );
        assert_eq!(
            archive.order_intent(OrderId(Uuid::from_u128(3))),
            Some(IntentId(2))
        );
        assert_eq!(
            archive.parent_executed_quantity(ParentOrderId(Uuid::from_u128(2))),
            Some(Decimal::ONE)
        );
        assert_eq!(archive.child_parent(OrderId(Uuid::from_u128(1))), None);
        let bytes = fs::read(&path).unwrap();
        assert!(LifecycleArchive::publish(&path, &report, evidence).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(fs::read_dir(&tmp.0).unwrap().count(), 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn archive_rejects_rehashed_invalid_structure_and_identity() {
        let tmp = directory();
        let path = tmp.0.join("invalid.json");
        let (report, evidence) = fixture();
        let payload = || Payload {
            schema_version: SCHEMA,
            namespace: report.replay_identity.unwrap().namespace,
            report_sha256: canonical_report_sha256(&report).unwrap(),
            evidence: evidence.clone(),
        };
        let verify_bad = |payload: Payload| {
            let envelope = Envelope {
                sha256: digest(&payload).unwrap(),
                payload,
            };
            fs::write(&path, serde_json::to_vec(&envelope).unwrap()).unwrap();
            assert!(LifecycleArchive::load_verified(&path, &report).is_err());
        };
        let mut p = payload();
        p.schema_version = 2;
        verify_bad(p);
        let mut p = payload();
        p.namespace = Uuid::nil();
        verify_bad(p);
        let mut p = payload();
        p.evidence.orders.push(p.evidence.orders[0]);
        verify_bad(p);
        let mut p = payload();
        p.evidence.orders[0].1 = Some(0);
        verify_bad(p);
        let mut p = payload();
        p.evidence.orders[0].1 = Some(3);
        verify_bad(p);
        let mut p = payload();
        p.evidence.children[0].1 = Some(Uuid::nil());
        verify_bad(p);
        let mut p = payload();
        p.evidence.orders.push((Uuid::from_u128(3), Some(1)));
        verify_bad(p);
        let mut p = payload();
        p.evidence.seen_fills.clear();
        verify_bad(p);
        let mut p = payload();
        p.evidence.parent_executed[0].1 = Some(Decimal::TWO);
        verify_bad(p);
        let mut p = payload();
        p.evidence.parent_executed.clear();
        verify_bad(p);
        let mut p = payload();
        p.evidence.ambiguous_parents.push(Uuid::from_u128(2));
        verify_bad(p);
        let mut p = payload();
        p.evidence.children[0].1 = None;
        p.evidence.orders.push((Uuid::from_u128(3), Some(1)));
        verify_bad(p);
    }
}
