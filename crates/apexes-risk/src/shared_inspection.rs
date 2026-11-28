//! Ephemeral shared-monitor references and simultaneous inspection guards.
//! Pointer identities are never serialized or exposed in retained evidence.
use std::sync::{Mutex, MutexGuard, TryLockError};

use crate::{
    CircuitBreaker, MarginMonitor, MarketConditionMonitor, RegisteredRiskState, RiskStateError,
};

/// Opaque reference to an actual built-in shared monitor. Custom checks
/// default to no shared reference; this does not grant restore authority.
pub struct SharedMonitorRef<'a>(pub(crate) SharedMonitor<'a>);

pub(crate) enum SharedMonitor<'a> {
    Circuit(&'a Mutex<CircuitBreaker>),
    Margin(&'a Mutex<MarginMonitor>),
    Market(&'a Mutex<MarketConditionMonitor>),
}

pub(crate) enum SharedGuard<'a> {
    Circuit(MutexGuard<'a, CircuitBreaker>),
    Margin(MutexGuard<'a, MarginMonitor>),
    Market(MutexGuard<'a, MarketConditionMonitor>),
}

fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, RiskStateError> {
    mutex.try_lock().map_err(|error| match error {
        TryLockError::Poisoned(_) => RiskStateError::Poisoned,
        TryLockError::WouldBlock => RiskStateError::Busy,
    })
}

impl<'a> SharedMonitorRef<'a> {
    pub(crate) fn key(&self) -> (u8, usize) {
        match self.0 {
            SharedMonitor::Circuit(m) => (0, std::ptr::from_ref(m) as usize),
            SharedMonitor::Margin(m) => (1, std::ptr::from_ref(m) as usize),
            SharedMonitor::Market(m) => (2, std::ptr::from_ref(m) as usize),
        }
    }

    pub(crate) fn try_lock(&self) -> Result<SharedGuard<'a>, RiskStateError> {
        match self.0 {
            SharedMonitor::Circuit(m) => lock(m).map(SharedGuard::Circuit),
            SharedMonitor::Margin(m) => lock(m).map(SharedGuard::Margin),
            SharedMonitor::Market(m) => lock(m).map(SharedGuard::Market),
        }
    }
}

impl SharedGuard<'_> {
    pub(crate) fn inspect(&self) -> Result<RegisteredRiskState, RiskStateError> {
        match self {
            Self::Circuit(m) => m.inspect_checkpoint().map(RegisteredRiskState::Circuit),
            Self::Margin(m) => m.inspect_checkpoint().map(RegisteredRiskState::Margin),
            Self::Market(m) => m
                .inspect_checkpoint()
                .map(RegisteredRiskState::MarketCondition),
        }
    }
}

/// Resolve actual handle identity before taking locks; inspect nonshared
/// slots while only their composite registration locks are held.
#[derive(Default)]
pub(crate) struct InspectionPlan<'a> {
    monitors: Vec<SharedMonitorRef<'a>>,
}

pub(crate) enum Slot {
    Shared(u16),
    Own(RegisteredRiskState),
}

impl<'a> InspectionPlan<'a> {
    pub(crate) fn add(
        &mut self,
        shared: Option<SharedMonitorRef<'a>>,
        inspect: impl FnOnce() -> Result<RegisteredRiskState, RiskStateError>,
    ) -> Result<Slot, RiskStateError> {
        let Some(shared) = shared else {
            return inspect().map(Slot::Own);
        };
        let index = if let Some(index) = self.monitors.iter().position(|m| m.key() == shared.key())
        {
            index
        } else {
            let index = self.monitors.len();
            self.monitors.push(shared);
            index
        };
        Ok(Slot::Shared(
            u16::try_from(index).map_err(|_| RiskStateError::Invalid)?,
        ))
    }

    /// All distinct actual monitors are locked simultaneously, and each
    /// is inspected once. Try-lock avoids waiting on external handles.
    pub(crate) fn inspect(&self) -> Result<Vec<RegisteredRiskState>, RiskStateError> {
        let guards = self
            .monitors
            .iter()
            .map(SharedMonitorRef::try_lock)
            .collect::<Result<Vec<_>, _>>()?;
        guards.iter().map(SharedGuard::inspect).collect()
    }
}

pub(crate) fn resolve(
    slots: Vec<Slot>,
    shared: &[RegisteredRiskState],
) -> Result<(Vec<RegisteredRiskState>, Vec<Option<u16>>), RiskStateError> {
    slots
        .into_iter()
        .map(|slot| match slot {
            Slot::Own(state) => Ok((state, None)),
            Slot::Shared(id) => Ok((
                shared
                    .get(usize::from(id))
                    .ok_or(RiskStateError::Invalid)?
                    .clone(),
                Some(id),
            )),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn contended_shared_monitor_refuses_and_releases_other_guards() {
        let first = Mutex::new(MarginMonitor::new(dec!(1.5)));
        let second = Mutex::new(MarginMonitor::new(dec!(1.5)));
        let mut plan = InspectionPlan::default();
        plan.add(
            Some(SharedMonitorRef(SharedMonitor::Margin(&first))),
            || unreachable!(),
        )
        .unwrap();
        plan.add(
            Some(SharedMonitorRef(SharedMonitor::Margin(&second))),
            || unreachable!(),
        )
        .unwrap();
        let blocked = second.lock().unwrap();
        assert_eq!(plan.inspect().unwrap_err(), RiskStateError::Busy);
        assert!(first.try_lock().is_ok());
        drop(blocked);
        let states = plan.inspect().unwrap();
        assert_eq!(states.len(), 2);
        assert_eq!(states[0], states[1]);
    }
}
