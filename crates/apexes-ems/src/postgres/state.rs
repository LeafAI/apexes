//! F01.04: durable-backed scheduling index and latched fault state.
//! The short synchronous lock never spans database or exchange I/O.
use super::{AlgoSpecJson, ParentOrderRow, parse_venue};
use crate::error::EmsError;
use apexes_core::Venue;
use apexes_oms::KillSwitch;
use apexes_strategy::{ExecutionAlgoSpec, ParentOrderId, ParentOrderStatus};
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, Ordering},
};

#[derive(Default)]
struct Index {
    status: HashMap<ParentOrderId, ParentOrderStatus>,
    pov: HashMap<(Venue, String), HashSet<ParentOrderId>>,
    keys: HashMap<ParentOrderId, (Venue, String)>,
}

pub(super) struct SchedulingState {
    index: Mutex<Index>,
    faulted: AtomicBool,
    kill_switch: Option<Arc<KillSwitch>>,
}

impl SchedulingState {
    pub(super) fn new(kill_switch: Option<Arc<KillSwitch>>) -> Self {
        Self {
            index: Mutex::new(Index::default()),
            faulted: AtomicBool::new(false),
            kill_switch,
        }
    }
    fn index(&self) -> MutexGuard<'_, Index> {
        self.index.lock().unwrap_or_else(|poisoned| {
            self.trip("EMS scheduling index poisoned");
            poisoned.into_inner()
        })
    }
    pub(super) fn healthy(&self) -> bool {
        !self.faulted.load(Ordering::Acquire)
    }
    pub(super) fn allowed(&self) -> bool {
        if !self.healthy() {
            self.trip("EMS reconciliation required");
            return false;
        }
        !self.kill_switch.as_ref().is_some_and(|ks| ks.is_engaged())
    }
    pub(super) fn trip(&self, reason: &'static str) {
        if !self.faulted.swap(true, Ordering::AcqRel) {
            tracing::error!(reason, "EMS scheduling suspended; reconciliation required");
        }
        if let Some(ks) = &self.kill_switch {
            ks.engage(reason);
        }
    }
    pub(super) fn register(&self, row: &ParentOrderRow) -> Result<(), EmsError> {
        let id = row.parent_id();
        let status = row.parse_status()?;
        let mut index = self.index();
        if status == ParentOrderStatus::Scheduling {
            let spec: ExecutionAlgoSpec =
                serde_json::from_value::<AlgoSpecJson>(row.algo_spec.clone())
                    .map_err(|_| EmsError::SchedulingSuspended)?
                    .into();
            if matches!(spec, ExecutionAlgoSpec::Pov { .. }) {
                let key = (parse_venue(&row.venue)?, row.symbol.clone());
                index.pov.entry(key.clone()).or_default().insert(id);
                index.keys.insert(id, key);
            }
        }
        index.status.insert(id, status);
        Ok(())
    }
    pub(super) fn status(&self, id: ParentOrderId) -> Option<ParentOrderStatus> {
        self.index().status.get(&id).cloned()
    }
    pub(super) fn set_status(&self, id: ParentOrderId, status: ParentOrderStatus) {
        let mut index = self.index();
        if status != ParentOrderStatus::Scheduling
            && let Some(key) = index.keys.remove(&id)
            && let Some(ids) = index.pov.get_mut(&key)
        {
            ids.remove(&id);
            if ids.is_empty() {
                index.pov.remove(&key);
            }
        }
        index.status.insert(id, status);
    }
    pub(super) fn active_for(&self, venue: Venue, symbol: &str) -> Vec<ParentOrderId> {
        self.index()
            .pov
            .get(&(venue, symbol.to_owned()))
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
    }
    pub(super) fn is_active(&self, id: ParentOrderId) -> bool {
        self.index().keys.contains_key(&id)
    }
}
