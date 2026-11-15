//! [`KillSwitch`]: a process-wide trading halt shared between the order
//! manager (which rejects new orders while engaged), the live loop's
//! cancel-all task, a staleness watchdog, and the engine's control
//! endpoint. Deliberately independent of the market-event loop: a
//! halt must work precisely when no market events are arriving.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

#[derive(Debug, Default)]
pub struct KillSwitch {
    engaged: AtomicBool,
    flatten_requested: AtomicBool,
    reason: Mutex<Option<String>>,
    notify: Notify,
}

impl KillSwitch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Engages the switch (idempotent; the first reason is kept).
    pub fn engage(&self, reason: impl Into<String>) {
        let reason = reason.into();
        if !self.engaged.swap(true, Ordering::SeqCst) {
            tracing::error!(%reason, "KILL SWITCH ENGAGED: new orders blocked");
            *self.reason.lock().unwrap() = Some(reason);
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
        self.flatten_requested.store(false, Ordering::SeqCst);
        if self.engaged.swap(false, Ordering::SeqCst) {
            tracing::warn!("kill switch released: new orders allowed again");
            *self.reason.lock().unwrap() = None;
            apexes_telemetry::metrics().kill_switch_engaged.set(0);
        }
    }

    pub fn is_engaged(&self) -> bool {
        self.engaged.load(Ordering::SeqCst)
    }

    pub fn reason(&self) -> Option<String> {
        self.reason.lock().unwrap().clone()
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
}
