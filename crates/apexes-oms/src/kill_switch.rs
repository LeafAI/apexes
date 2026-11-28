//! [`KillSwitch`]: a process-wide trading halt shared between the order
//! manager (which rejects new orders while engaged), the live loop's
//! cancel-all task, a staleness watchdog, and the engine's control
//! endpoint. Deliberately independent of the market-event loop: a
//! halt must work precisely when no market events are arriving.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// F01.05: session readiness is independent of the emergency cancellation
/// signal. A fault is latched until a new session completes recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPhase {
    Initializing,
    Reconciling,
    Running,
    Degraded,
    Halted,
}

impl SessionPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Initializing => "Initializing",
            Self::Reconciling => "Reconciling",
            Self::Running => "Running",
            Self::Degraded => "Degraded",
            Self::Halted => "Halted",
        }
    }
}

#[derive(Debug, Clone)]
struct Health {
    phase: SessionPhase,
    reason: String,
}

#[derive(Debug, Default)]
pub struct KillSwitch {
    engaged: AtomicBool,
    flatten_requested: AtomicBool,
    reason: Mutex<Option<String>>,
    notify: Notify,
    health: Mutex<Option<Health>>,
}

impl KillSwitch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Live engines opt in; library callers retain legacy behavior.
    pub fn managed() -> Self {
        Self {
            health: Mutex::new(Some(Health {
                phase: SessionPhase::Initializing,
                reason: "initializing live dependencies".into(),
            })),
            ..Self::default()
        }
    }

    fn health_lock(&self) -> std::sync::MutexGuard<'_, Option<Health>> {
        self.health.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn phase(&self) -> Option<SessionPhase> {
        self.health_lock().as_ref().map(|h| h.phase)
    }

    pub fn health_reason(&self) -> Option<String> {
        self.health_lock().as_ref().map(|h| h.reason.clone())
    }

    pub fn blocks_submissions(&self) -> bool {
        self.is_engaged() || self.phase().is_some_and(|p| p != SessionPhase::Running)
    }

    /// Coherent read of the submission gate. All gate transitions hold
    /// the health lock; no free-form fault message is returned to scripts.
    pub fn health_observation(&self) -> apexes_strategy::HealthObservation {
        use apexes_strategy::{HealthObservation, HealthPhase};
        let health = self.health_lock();
        let phase = health.as_ref().map(|h| match h.phase {
            SessionPhase::Initializing => HealthPhase::Initializing,
            SessionPhase::Reconciling => HealthPhase::Reconciling,
            SessionPhase::Running => HealthPhase::Running,
            SessionPhase::Degraded => HealthPhase::Degraded,
            SessionPhase::Halted => HealthPhase::Halted,
        });
        let kill_switch_engaged = self.is_engaged();
        HealthObservation {
            phase,
            kill_switch_engaged,
            blocks_submissions: kill_switch_engaged
                || phase.is_some_and(|p| p != HealthPhase::Running),
        }
    }

    pub fn begin_reconciliation(&self) {
        let mut health = self.health_lock();
        if let Some(h) = health.as_mut()
            && h.phase == SessionPhase::Initializing
            && !self.is_engaged()
        {
            h.phase = SessionPhase::Reconciling;
            h.reason = "recovering orders, balances and market data".into();
        }
        self.notify.notify_waiters();
    }

    /// Only the startup path may advance to Running. Never clear a fault.
    pub fn startup_ready(&self) {
        let mut health = self.health_lock();
        if let Some(h) = health.as_mut()
            && h.phase == SessionPhase::Reconciling
            && !self.is_engaged()
        {
            h.phase = SessionPhase::Running;
            h.reason.clear();
            tracing::info!("live session health: Running");
        }
        self.notify.notify_waiters();
    }

    pub fn cancel_sweep_complete(&self) {
        let mut health = self.health_lock();
        if let Some(h) = health.as_mut()
            && h.phase == SessionPhase::Degraded
        {
            h.phase = SessionPhase::Halted;
        }
    }

    /// Faults are opt-in for managed live engines. Preserve compatibility
    /// for standalone OMS and simulator callers.
    pub fn fault(&self, reason: &'static str) {
        if self.phase().is_some() {
            self.engage(reason);
        }
    }

    /// Recovered scheduling tasks wait here until the runner has subscribed
    /// and observed usable market data. A halt never becomes readiness.
    pub async fn wait_startup_ready(&self) -> bool {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_engaged() {
                return false;
            }
            if !self.blocks_submissions() {
                return true;
            }
            notified.await;
        }
    }

    /// Engages the switch (idempotent; the first reason is kept).
    pub fn engage(&self, reason: impl Into<String>) {
        let reason = reason.into();
        let mut health = self.health_lock();
        if !self.engaged.swap(true, Ordering::SeqCst) {
            tracing::error!(%reason, "KILL SWITCH ENGAGED: new orders blocked");
            *self.reason.lock().unwrap_or_else(|p| p.into_inner()) = Some(reason.clone());
            if let Some(h) = health.as_mut() {
                h.phase = SessionPhase::Degraded;
                h.reason = reason;
            }
            apexes_telemetry::metrics().kill_switch_engaged.set(1);
        }
        self.notify.notify_waiters();
    }

    /// Asks the live runner to also close open positions once the
    /// cancel-all sweep has finished. Call BEFORE [`Self::engage`] so the
    /// runner sees the request when it wakes. Cleared on release.
    pub fn request_flatten(&self) {
        self.flatten_requested.store(true, Ordering::SeqCst);
    }

    pub fn flatten_requested(&self) -> bool {
        self.flatten_requested.load(Ordering::SeqCst)
    }

    /// Releases the switch; trading may resume.
    pub fn release(&self) {
        let _ = self.try_release();
    }

    /// Explicit policy: managed sessions cannot resume after a fault or
    /// halt in-process; a new startup must reconcile durable state.
    pub fn try_release(&self) -> Result<(), &'static str> {
        let health = self.health_lock();
        if health.is_some() {
            return Err("live session requires reconciliation and restart before resuming");
        }
        self.flatten_requested.store(false, Ordering::SeqCst);
        if self.engaged.swap(false, Ordering::SeqCst) {
            tracing::warn!("kill switch released: new orders allowed again");
            *self.reason.lock().unwrap_or_else(|p| p.into_inner()) = None;
            apexes_telemetry::metrics().kill_switch_engaged.set(0);
        }
        Ok(())
    }

    pub fn is_engaged(&self) -> bool {
        self.engaged.load(Ordering::SeqCst)
    }

    pub fn reason(&self) -> Option<String> {
        self.reason
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Resolves once the switch is engaged (immediately if it already is).
    pub async fn wait_engaged(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Register interest before checking state so an `engage`
            // between the check and the await is not missed.
            notified.as_mut().enable();
            if self.is_engaged() {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn engage_keeps_first_reason_and_release_clears() {
        let ks = KillSwitch::new();
        assert!(!ks.is_engaged());
        ks.engage("first");
        ks.engage("second");
        assert!(ks.is_engaged());
        assert_eq!(ks.reason().as_deref(), Some("first"));
        ks.release();
        assert!(!ks.is_engaged());
        assert_eq!(ks.reason(), None);
    }

    #[test]
    fn flatten_request_is_cleared_by_release() {
        let ks = KillSwitch::new();
        assert!(!ks.flatten_requested());
        ks.request_flatten();
        ks.engage("x");
        assert!(ks.flatten_requested());
        ks.release();
        assert!(!ks.flatten_requested());
    }

    #[tokio::test]
    async fn wait_engaged_resolves_on_later_engage_and_when_already_engaged() {
        let ks = Arc::new(KillSwitch::new());
        let waiter = {
            let ks = Arc::clone(&ks);
            tokio::spawn(async move { ks.wait_engaged().await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        ks.engage("x");
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), ks.wait_engaged())
            .await
            .unwrap();
    }
    #[test]
    fn health_observation_matches_gate_through_managed_and_legacy_transitions() {
        use apexes_strategy::{HealthObservation, HealthPhase};
        let gate = KillSwitch::managed();
        let assert_state = |phase, engaged, blocked| {
            assert_eq!(
                gate.health_observation(),
                HealthObservation {
                    phase: Some(phase),
                    kill_switch_engaged: engaged,
                    blocks_submissions: blocked
                }
            );
            assert_eq!(gate.blocks_submissions(), blocked);
        };
        assert_state(HealthPhase::Initializing, false, true);
        gate.begin_reconciliation();
        assert_state(HealthPhase::Reconciling, false, true);
        gate.startup_ready();
        assert_state(HealthPhase::Running, false, false);
        let saved = gate.health_observation();
        gate.fault("test internal detail");
        assert_state(HealthPhase::Degraded, true, true);
        gate.cancel_sweep_complete();
        assert_state(HealthPhase::Halted, true, true);
        assert!(gate.try_release().is_err());
        assert_state(HealthPhase::Halted, true, true);
        assert!(!saved.blocks_submissions);
        let legacy = KillSwitch::new();
        assert_eq!(
            legacy.health_observation(),
            HealthObservation {
                phase: None,
                kill_switch_engaged: false,
                blocks_submissions: false
            }
        );
        legacy.engage("legacy halt");
        assert_eq!(
            legacy.health_observation(),
            HealthObservation {
                phase: None,
                kill_switch_engaged: true,
                blocks_submissions: true
            }
        );
        legacy.release();
        assert!(!legacy.health_observation().blocks_submissions);
    }

    #[test]
    fn managed_health_is_fail_closed_and_cannot_be_released_by_operator() {
        let ks = KillSwitch::managed();
        assert_eq!(ks.phase(), Some(SessionPhase::Initializing));
        assert!(ks.blocks_submissions());
        assert!(!ks.is_engaged());
        assert!(ks.try_release().is_err());
        ks.begin_reconciliation();
        assert_eq!(ks.phase(), Some(SessionPhase::Reconciling));
        assert!(ks.blocks_submissions());
        ks.startup_ready();
        assert_eq!(ks.phase(), Some(SessionPhase::Running));
        assert!(!ks.blocks_submissions());
        ks.fault("exchange disconnected");
        assert_eq!(ks.phase(), Some(SessionPhase::Degraded));
        assert!(ks.blocks_submissions());
        ks.cancel_sweep_complete();
        assert_eq!(ks.phase(), Some(SessionPhase::Halted));
        ks.release();
        ks.begin_reconciliation();
        ks.startup_ready();
        assert!(ks.is_engaged());
        assert_eq!(ks.health_reason().as_deref(), Some("exchange disconnected"));
        assert_eq!(ks.phase(), Some(SessionPhase::Halted));
    }

    #[tokio::test]
    async fn recovered_tasks_wait_for_readiness_and_fault_unblocks_waiter() {
        let ks = Arc::new(KillSwitch::managed());
        let task = tokio::spawn({
            let ks = ks.clone();
            async move { ks.wait_startup_ready().await }
        });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        ks.fault("startup failure");
        assert!(!task.await.unwrap());
    }
}
