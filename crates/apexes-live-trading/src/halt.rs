//! Background tasks that give the [`KillSwitch`] teeth in live trading.
//!
//! Both run independently of the market-event loop (which only drains
//! control commands on market ticks): a halt has to work precisely when
//! the feed is dead.
//! - [`spawn_cancel_all_task`]: once the switch engages, cancels every
//!   open order, retrying until none are left or the switch is released.
//! - [`spawn_staleness_watchdog`]: engages the switch when no market data
//!   has arrived within a configurable window.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use apexes_oms::{KillSwitch, OrderManager};
use tokio::task::JoinHandle;
use tokio::time::Instant;

const CANCEL_RETRY_INTERVAL: Duration = Duration::from_secs(2);
const RELEASE_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Records the last time market data was seen.
#[derive(Debug)]
pub struct ActivityClock {
    base: Instant,
    last_ms: AtomicU64,
}

impl ActivityClock {
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
            last_ms: AtomicU64::new(0),
        }
    }

    pub fn touch(&self) {
        self.last_ms
            .store(self.base.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    pub fn idle(&self) -> Duration {
        let now = self.base.elapsed().as_millis() as u64;
        Duration::from_millis(now.saturating_sub(self.last_ms.load(Ordering::Relaxed)))
    }
}

impl Default for ActivityClock {
    fn default() -> Self {
        Self::new()
    }
}

pub fn spawn_cancel_all_task(
    kill_switch: Arc<KillSwitch>,
    order_manager: Arc<tokio::sync::Mutex<OrderManager>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            kill_switch.wait_engaged().await;
            while kill_switch.is_engaged() {
                let report = order_manager.lock().await.cancel_all_open().await;
                tracing::warn!(
                    cancelled = report.cancelled.len(),
                    failed = report.failed.len(),
                    unresolved = report.unresolved.len(),
                    "kill switch: cancel-all pass"
                );
                if report.is_complete() {
                    break;
                }
                tokio::time::sleep(CANCEL_RETRY_INTERVAL).await;
            }
            // Re-arm only after release so one engagement = one cancel sweep.
            while kill_switch.is_engaged() {
                tokio::time::sleep(RELEASE_POLL_INTERVAL).await;
            }
        }
    })
}

pub fn spawn_staleness_watchdog(
    kill_switch: Arc<KillSwitch>,
    clock: Arc<ActivityClock>,
    stale_after: Duration,
) -> JoinHandle<()> {
    let check_every = (stale_after / 4).max(Duration::from_millis(10));
    tokio::spawn(async move {
        clock.touch();
        loop {
            tokio::time::sleep(check_every).await;
            if kill_switch.is_engaged() {
                // Operator must release; give the feed a fresh grace window after.
                while kill_switch.is_engaged() {
                    tokio::time::sleep(RELEASE_POLL_INTERVAL).await;
                }
                clock.touch();
                continue;
            }
            let idle = clock.idle();
            if idle >= stale_after {
                kill_switch.engage(format!(
                    "market data stale: no events for {:.1}s (limit {:.1}s)",
                    idle.as_secs_f64(),
                    stale_after.as_secs_f64()
                ));
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn watchdog_engages_only_after_the_feed_goes_quiet() {
        let ks = Arc::new(KillSwitch::new());
        let clock = Arc::new(ActivityClock::new());
        let _wd =
            spawn_staleness_watchdog(Arc::clone(&ks), Arc::clone(&clock), Duration::from_secs(10));

        // Feed alive for 30s: touched every 2s.
        for _ in 0..15 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            clock.touch();
        }
        assert!(!ks.is_engaged());

        // Feed goes quiet.
        tokio::time::sleep(Duration::from_secs(15)).await;
        assert!(ks.is_engaged());
        assert!(ks.reason().unwrap().contains("market data stale"));
    }

    #[tokio::test(start_paused = true)]
    async fn after_release_the_watchdog_grants_a_fresh_grace_window() {
        let ks = Arc::new(KillSwitch::new());
        let clock = Arc::new(ActivityClock::new());
        let _wd =
            spawn_staleness_watchdog(Arc::clone(&ks), Arc::clone(&clock), Duration::from_secs(10));

        tokio::time::sleep(Duration::from_secs(15)).await;
        assert!(ks.is_engaged());
        ks.release();
        // Within the grace window the feed is still "stale" by the clock
        // but must not immediately re-engage.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!ks.is_engaged());
        tokio::time::sleep(Duration::from_secs(15)).await;
        assert!(ks.is_engaged());
    }

    #[test]
    fn idle_is_zero_right_after_touch() {
        let clock = ActivityClock::new();
        clock.touch();
        assert!(clock.idle() < Duration::from_millis(50));
    }
}
