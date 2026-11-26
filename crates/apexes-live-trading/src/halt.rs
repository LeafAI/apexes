//! Background tasks that give the [`KillSwitch`] teeth in live trading.
//!
//! Both run independently of the market-event loop (which only drains
//! control commands on market ticks): a halt has to work precisely when
//! the feed is dead.
//! - [`spawn_cancel_all_task`]: once the switch engages, cancels every
//!   open order, retrying until none are left or the switch is released.
//! - [`spawn_staleness_watchdog`]: engages the switch when no market data
//!   has arrived within a configurable window.

use apexes_core::{Instrument, MarketEvent, Venue};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apexes_oms::{KillSwitch, OrderManager};

use crate::flatten::{FlattenSetup, flatten_positions};
use tokio::task::JoinHandle;
use tokio::time::Instant;

const CANCEL_RETRY_INTERVAL: Duration = Duration::from_secs(2);
const RELEASE_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Records the last time market data was seen.
#[derive(Debug)]
pub struct ActivityClock {
    base: Instant,
    last_ms: AtomicU64,
    instruments: Mutex<HashMap<(Venue, String), Instant>>,
    seen: Mutex<std::collections::HashSet<(Venue, String)>>,
}

impl ActivityClock {
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
            last_ms: AtomicU64::new(0),
            instruments: Mutex::new(HashMap::new()),
            seen: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Register each instrument before starting the stream. An active
    /// neighbour must never reset another instrument's freshness.
    pub fn for_instruments(instruments: &[Instrument]) -> Self {
        let clock = Self::new();
        if let Ok(mut entries) = clock.instruments.lock() {
            let now = Instant::now();
            entries.extend(
                instruments
                    .iter()
                    .map(|i| ((i.venue, i.symbol.clone()), now)),
            );
        }
        clock
    }

    pub fn observe(&self, event: &MarketEvent) {
        // Mark prices alone do not establish a usable executable market.
        if matches!(event, MarketEvent::MarkPrice(_))
            || matches!(event, MarketEvent::Depth(d) if d.is_invalidation())
        {
            return;
        }
        if let Ok(mut entries) = self.instruments.lock()
            && let Some(last) = entries.get_mut(&(event.venue(), event.symbol().to_string()))
        {
            *last = Instant::now();
            if let Ok(mut seen) = self.seen.lock() {
                seen.insert((event.venue(), event.symbol().to_string()));
            }
        }
        self.touch();
    }

    pub fn all_observed(&self) -> bool {
        let Ok(instruments) = self.instruments.lock() else {
            return false;
        };
        let Ok(seen) = self.seen.lock() else {
            return false;
        };
        !instruments.is_empty() && instruments.keys().all(|key| seen.contains(key))
    }

    fn reset_grace(&self) {
        self.touch();
        if let Ok(mut entries) = self.instruments.lock() {
            let now = Instant::now();
            entries.values_mut().for_each(|last| *last = now);
        }
    }

    /// Return the stalest configured instrument (or aggregate clock for
    /// legacy callers with no instrument set).
    pub fn stalest(&self) -> (Option<(Venue, String)>, Duration) {
        match self.instruments.lock() {
            Ok(entries) => entries
                .iter()
                .map(|(key, last)| (Some(key.clone()), last.elapsed()))
                .max_by_key(|(_, idle)| *idle)
                .unwrap_or_else(|| (None, self.idle())),
            Err(_) => (None, Duration::MAX),
        }
    }

    pub fn enforce(&self, kill_switch: &KillSwitch, stale_after: Duration) {
        let (instrument, idle) = self.stalest();
        apexes_telemetry::metrics()
            .market_data_idle_seconds
            .set(idle.as_secs_f64());
        if idle >= stale_after && !kill_switch.is_engaged() {
            let label = instrument
                .map(|(venue, symbol)| format!("{venue:?}:{symbol}"))
                .unwrap_or_else(|| "session".into());
            kill_switch.engage(format!(
                "market data stale: {label}, no usable events for {:.1}s (limit {:.1}s)",
                idle.as_secs_f64(),
                stale_after.as_secs_f64()
            ));
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

/// Once the switch engages: cancel every open order (retrying until none
/// remain), then -- if the engagement asked for it and `flatten` is
/// configured -- close open positions. One engagement = one sweep and at
/// most one flatten; re-arms after release.
pub fn spawn_cancel_all_task(
    kill_switch: Arc<KillSwitch>,
    order_manager: Arc<tokio::sync::Mutex<OrderManager>>,
    flatten: Option<FlattenSetup>,
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
                    kill_switch.cancel_sweep_complete();
                    break;
                }
                tokio::time::sleep(CANCEL_RETRY_INTERVAL).await;
            }
            // Until release: honour a flatten request (it may arrive on a
            // re-sent Halt after the sweep already finished), once.
            let mut flattened = false;
            while kill_switch.is_engaged() {
                if kill_switch.flatten_requested() && !flattened {
                    flattened = true;
                    match &flatten {
                        Some(setup) => {
                            let report = flatten_positions(setup, &order_manager).await;
                            if !report.is_flat() {
                                tracing::error!(
                                    remaining = ?report.remaining,
                                    "kill switch: flatten INCOMPLETE -- positions still open, close them manually"
                                );
                            }
                        }
                        None => tracing::error!(
                            "kill switch: flatten requested but no position source is configured; positions stay open"
                        ),
                    }
                }
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
        clock.reset_grace();
        loop {
            tokio::time::sleep(check_every).await;
            if kill_switch.is_engaged() {
                // Operator must release; give the feed a fresh grace window after.
                while kill_switch.is_engaged() {
                    tokio::time::sleep(RELEASE_POLL_INTERVAL).await;
                }
                clock.reset_grace();
                continue;
            }
            clock.enforce(&kill_switch, stale_after);
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
    #[tokio::test(start_paused = true)]
    async fn active_symbol_cannot_mask_another_stale_symbol() {
        use apexes_core::{AssetClass, MarketType, TradeEvent};
        use rust_decimal_macros::dec;
        let instrument = |symbol: &str| {
            Instrument::new(
                symbol,
                MarketType::Crypto,
                Venue::Binance,
                AssetClass::Spot,
                None,
                Some("USDT".into()),
                dec!(0.01),
                dec!(0.001),
                dec!(1),
            )
            .unwrap()
        };
        let clock = Arc::new(ActivityClock::for_instruments(&[
            instrument("BTCUSDT"),
            instrument("ETHUSDT"),
        ]));
        let ks = Arc::new(KillSwitch::new());
        let _wd = spawn_staleness_watchdog(ks.clone(), clock.clone(), Duration::from_secs(10));
        for _ in 0..8 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            clock.observe(&MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".into(),
                venue: Venue::Binance,
                price: dec!(1),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: chrono::Utc::now(),
            }));
        }
        assert!(ks.is_engaged());
        assert!(ks.reason().unwrap().contains("ETHUSDT"));
    }
    #[test]
    fn startup_requires_a_usable_event_for_every_configured_symbol() {
        use apexes_core::{AssetClass, MarketType, TradeEvent};
        use rust_decimal_macros::dec;
        let instrument = |symbol: &str| {
            Instrument::new(
                symbol,
                MarketType::Crypto,
                Venue::Binance,
                AssetClass::Spot,
                None,
                Some("USDT".into()),
                dec!(0.01),
                dec!(0.001),
                dec!(1),
            )
            .unwrap()
        };
        let clock = ActivityClock::for_instruments(&[instrument("BTCUSDT"), instrument("ETHUSDT")]);
        let trade = |symbol: &str| {
            MarketEvent::Trade(TradeEvent {
                symbol: symbol.into(),
                venue: Venue::Binance,
                price: dec!(100),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: chrono::Utc::now(),
            })
        };
        assert!(!clock.all_observed());
        clock.observe(&trade("BTCUSDT"));
        clock.observe(&trade("OTHER"));
        assert!(!clock.all_observed());
        clock.observe(&trade("ETHUSDT"));
        assert!(clock.all_observed());
    }
}
