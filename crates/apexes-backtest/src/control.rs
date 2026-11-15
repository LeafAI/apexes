//! [`ControlHandle`]: the live-tuning/monitoring control surface built
//! directly into `crate::unified_loop::run_unified_event_loop` (NOT a
//! fourth pluggable `apexes_strategy` trait -- see design spec
//! `2026-08-05-apexes-13c-strategy-monitoring-tuning-design.md` §5.1
//! for why). `None` on `UnifiedLoopConfig.control` is a silent no-op,
//! matching every other optional feature this loop supports.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use apexes_strategy::{Position, PositionMarginStatus};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

/// A command sent by a caller holding the other half of a
/// [`ControlHandle`] (e.g. `apexes-engine`'s WS handler) to steer a
/// running session.
///
/// Derives `Serialize`/`Deserialize` (added by `apexes-engine`'s Task
/// 8, design spec §6.2) so this type can be embedded directly in
/// `apexes_engine::protocol::SessionMessage::Command` and sent over
/// `/session/ws` -- Task 2 (this crate) had no serialization consumer
/// of its own, so these derives were not required until now.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SessionCommand {
    /// Hot-updates one tunable strategy parameter. Applied via
    /// `Strategy::set_param`; an unknown parameter name logs a warning
    /// but never aborts the loop.
    SetParam { name: String, value: Decimal },
    /// Suspends `Strategy::on_market_event` dispatch starting the next
    /// tick. Funding settlement, margin/liquidation checks, and
    /// de-risking keep running unaffected.
    Pause,
    /// Resumes `Strategy::on_market_event` dispatch.
    Resume,
    /// Requests the unified loop exit after this tick, equivalent to
    /// `stop_signal` firing.
    Stop,
    /// Engages the live kill switch (blocks new orders, cancels all open
    /// orders) and pauses strategy dispatch. The switch itself is engaged
    /// by the engine's control endpoint directly -- this loop only drains
    /// commands on market ticks, which is exactly when a halt cannot be
    /// relied upon -- so the loop-side effect is just the pause.
    /// With `flatten`, the live runner also closes open positions after
    /// cancelling orders. Absent on the wire = `false`.
    Halt {
        reason: String,
        #[serde(default)]
        flatten: bool,
    },
    /// Releases the kill switch and resumes strategy dispatch.
    ResumeTrading,
}

/// A point-in-time view of a running session's state, pushed by the
/// unified loop via a `watch` channel (throttled to `min_interval`) for
/// monitoring/UI display.
///
/// Derives `Serialize`/`Deserialize` for the same reason
/// [`SessionCommand`] does -- see that type's doc comment.
/// One open order as shown to an operator: plain strings/decimals so a UI
/// needs no knowledge of the core order types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenOrderView {
    pub id: String,
    pub symbol: String,
    /// `"Buy"` / `"Sell"`.
    pub side: String,
    /// `"Market"`, `"Limit"`, ...
    pub order_type: String,
    pub quantity: Decimal,
    pub filled_quantity: Decimal,
    pub price: Option<Decimal>,
    /// `"New"` / `"PartiallyFilled"` / ...
    pub status: String,
    pub created_at: DateTime<Utc>,
}

impl SessionSnapshot {
    /// True for the initial value a `ControlHandle` is created with (stamped
    /// with the UNIX epoch) before the trading loop has pushed a real
    /// snapshot. Consumers must treat it as "no data yet", not as a flat,
    /// zero-equity account.
    pub fn is_placeholder(&self) -> bool {
        self.timestamp.timestamp() == 0
    }
}

impl From<&apexes_core::Order> for OpenOrderView {
    fn from(order: &apexes_core::Order) -> Self {
        Self {
            id: order.id.0.to_string(),
            symbol: order.symbol.clone(),
            side: format!("{:?}", order.side),
            order_type: format!("{:?}", order.order_type),
            quantity: order.quantity,
            filled_quantity: order.filled_quantity,
            price: order.price,
            status: format!("{:?}", order.status),
            created_at: order.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub positions: HashMap<String, Position>,
    pub balances: HashMap<String, Decimal>,
    pub equity: Decimal,
    pub realized_pnl: HashMap<String, Decimal>,
    pub margin_status: HashMap<String, PositionMarginStatus>,
    pub params: HashMap<String, Decimal>,
    pub paused: bool,
    /// Orders currently working (live: the order manager's open orders;
    /// backtest/paper: the simulated book's). Absent in older engines.
    #[serde(default)]
    pub open_orders: Vec<OpenOrderView>,
    /// Who this session is (network, id, strategy). Set once from the
    /// session's launch config; `None` from older engines and for sessions
    /// that were not given an identity.
    #[serde(default)]
    pub identity: Option<SessionIdentity>,
    /// Running evaluation (PnL, drawdown, exposure, fees, limit usage).
    /// `None` until the loop has computed one.
    #[serde(default)]
    pub evaluation: Option<crate::evaluation::EvaluationSummary>,
    /// Thinned equity history ending at the latest point, for the console's
    /// timeline. Bounded (see `evaluation::SNAPSHOT_HISTORY_POINTS`).
    #[serde(default)]
    pub equity_history: Vec<(DateTime<Utc>, Decimal)>,
    pub timestamp: DateTime<Utc>,
}

/// Identity of a running session, shown in the console so an operator can
/// always see which network and strategy they are looking at. Describes
/// what the *engine* is actually using, not what a launcher intended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionIdentity {
    pub session_id: String,
    /// `"mainnet"` / `"testnet"` for live sessions; `None` for paper and
    /// backtest, which touch no exchange.
    pub network: Option<String>,
    pub mode: String,
    pub strategy: String,
}

/// Constructed by the caller (`apexes-engine`, a later task) BEFORE
/// calling `run_unified_event_loop` -- the caller keeps the
/// `mpsc::UnboundedSender<SessionCommand>` and
/// `watch::Receiver<SessionSnapshot>` halves for itself (e.g. to wire
/// into its own `axum` WS handler), passing this struct (which holds
/// the OTHER halves) into `UnifiedLoopConfig.control`.
pub struct ControlHandle {
    identity: Option<SessionIdentity>,
    limits: crate::evaluation::LimitSpec,
    snapshot_tx: watch::Sender<SessionSnapshot>,
    command_rx: mpsc::UnboundedReceiver<SessionCommand>,
    min_interval: Duration,
    last_pushed: Option<Instant>,
    paused: bool,
}

/// The outcome of one [`ControlHandle::drain_commands`] call.
pub(crate) struct DrainResult {
    pub stop_requested: bool,
}

impl ControlHandle {
    /// Builds a new control channel pair, returning the handle to be
    /// embedded in `UnifiedLoopConfig.control` alongside the sender/
    /// receiver halves the external caller keeps.
    pub fn new(
        min_interval: Duration,
        initial_snapshot: SessionSnapshot,
    ) -> (
        Self,
        mpsc::UnboundedSender<SessionCommand>,
        watch::Receiver<SessionSnapshot>,
    ) {
        let (snapshot_tx, snapshot_rx) = watch::channel(initial_snapshot);
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let handle = Self {
            identity: None,
            limits: crate::evaluation::LimitSpec {
                max_position_per_symbol: None,
                max_daily_fills: None,
            },
            snapshot_tx,
            command_rx,
            min_interval,
            last_pushed: None,
            paused: false,
        };
        (handle, command_tx, snapshot_rx)
    }

    /// Identity stamped onto every snapshot this handle pushes.
    pub fn with_identity(mut self, identity: SessionIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Limits the evaluation view reports utilisation against.
    pub fn with_limits(mut self, limits: crate::evaluation::LimitSpec) -> Self {
        self.limits = limits;
        self
    }

    pub(crate) fn identity(&self) -> Option<SessionIdentity> {
        self.identity.clone()
    }

    pub(crate) fn limit_spec(&self) -> crate::evaluation::LimitSpec {
        self.limits.clone()
    }

    /// Called once per tick by the unified loop. Drains ALL pending
    /// commands (not just one -- a tick may have several queued),
    /// applying `SetParam` to `strategy` immediately (logging
    /// `tracing::warn!` on an `Err` from `Strategy::set_param`, never
    /// propagating it as a loop error -- an unknown param name is an
    /// operator mistake, not a system failure), and returns whether a
    /// `Stop` command was seen (the loop's caller checks this
    /// alongside its existing `stop_signal` check).
    pub(crate) fn drain_commands(
        &mut self,
        strategy: &mut dyn apexes_strategy::Strategy,
    ) -> DrainResult {
        let mut stop_requested = false;
        loop {
            match self.command_rx.try_recv() {
                Ok(command) => match command {
                    SessionCommand::SetParam { name, value } => {
                        if let Err(error) = strategy.set_param(&name, value) {
                            tracing::warn!(
                                ?error,
                                param = %name,
                                %value,
                                "SetParam command referenced an unknown strategy parameter"
                            );
                        }
                    }
                    SessionCommand::Pause => self.paused = true,
                    SessionCommand::Resume => self.paused = false,
                    SessionCommand::Stop => stop_requested = true,
                    SessionCommand::Halt { .. } => self.paused = true,
                    SessionCommand::ResumeTrading => self.paused = false,
                },
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        DrainResult { stop_requested }
    }

    pub(crate) fn is_paused(&self) -> bool {
        self.paused
    }

    /// Peeks whether `min_interval` has elapsed since the last push,
    /// WITHOUT consuming/pushing anything -- lets a caller decide
    /// whether it's worth constructing (and cloning ledger state into)
    /// a `SessionSnapshot` at all before doing that work, rather than
    /// building one only to have `maybe_push_snapshot` throw it away.
    pub(crate) fn is_snapshot_due(&self) -> bool {
        match self.last_pushed {
            None => true,
            Some(last) => Instant::now().duration_since(last) >= self.min_interval,
        }
    }

    /// Pushes `snapshot` via the `watch` channel IF `min_interval` has
    /// elapsed since the last push (silently skips otherwise -- no
    /// queuing, `watch`'s own "only the latest value matters" semantics
    /// do the throttling naturally once this elapsed-time gate is
    /// applied at the call site).
    pub(crate) fn maybe_push_snapshot(&mut self, snapshot: SessionSnapshot) {
        let now = Instant::now();
        let should_push = match self.last_pushed {
            None => true,
            Some(last) => now.duration_since(last) >= self.min_interval,
        };
        if !should_push {
            return;
        }
        // `watch::Sender::send` only fails when every `Receiver` has
        // been dropped -- not a loop error, just means nobody is
        // listening for snapshots anymore.
        let _ = self.snapshot_tx.send(snapshot);
        self.last_pushed = Some(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::MarketEvent;
    use apexes_strategy::{OrderIntent, Strategy, StrategyContext};
    use rust_decimal_macros::dec;
    use std::sync::{Arc, Mutex};

    fn empty_snapshot() -> SessionSnapshot {
        SessionSnapshot {
            identity: None,
            evaluation: None,
            equity_history: Vec::new(),
            positions: HashMap::new(),
            balances: HashMap::new(),
            equity: Decimal::ZERO,
            realized_pnl: HashMap::new(),
            margin_status: HashMap::new(),
            params: HashMap::new(),
            paused: false,
            open_orders: Vec::new(),
            timestamp: Utc::now(),
        }
    }

    /// A no-op `Strategy` that records every `set_param` call it
    /// receives, for asserting `drain_commands` really applies
    /// `SetParam` commands.
    struct RecordingStrategy {
        received: Arc<Mutex<Vec<(String, Decimal)>>>,
    }

    impl Strategy for RecordingStrategy {
        fn on_market_event(
            &mut self,
            _event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            Vec::new()
        }

        fn set_param(
            &mut self,
            name: &str,
            value: Decimal,
        ) -> Result<(), apexes_strategy::StrategyError> {
            self.received
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((name.to_string(), value));
            Ok(())
        }
    }

    #[test]
    fn min_interval_throttles_snapshot_pushes() {
        let (mut handle, _command_tx, snapshot_rx) =
            ControlHandle::new(Duration::from_secs(3600), empty_snapshot());

        let mut first = empty_snapshot();
        first.equity = dec!(100);
        handle.maybe_push_snapshot(first);

        let mut second = empty_snapshot();
        second.equity = dec!(200);
        handle.maybe_push_snapshot(second);

        // The second push happened well within `min_interval`, so it
        // must have been throttled away -- the receiver should only
        // ever observe the first value.
        assert_eq!(snapshot_rx.borrow().equity, dec!(100));
    }

    #[test]
    fn drain_commands_applies_set_param_to_strategy() {
        let (mut handle, command_tx, _snapshot_rx) =
            ControlHandle::new(Duration::from_secs(3600), empty_snapshot());
        let received = Arc::new(Mutex::new(Vec::new()));
        let mut strategy = RecordingStrategy {
            received: Arc::clone(&received),
        };

        command_tx
            .send(SessionCommand::SetParam {
                name: "threshold".to_string(),
                value: dec!(42),
            })
            .expect("receiver still alive");

        let result = handle.drain_commands(&mut strategy);

        assert!(!result.stop_requested);
        assert_eq!(
            *received.lock().unwrap(),
            vec![("threshold".to_string(), dec!(42))]
        );
    }

    #[test]
    fn drain_commands_drains_multiple_queued_commands_in_one_call() {
        let (mut handle, command_tx, _snapshot_rx) =
            ControlHandle::new(Duration::from_secs(3600), empty_snapshot());
        let received = Arc::new(Mutex::new(Vec::new()));
        let mut strategy = RecordingStrategy {
            received: Arc::clone(&received),
        };

        command_tx
            .send(SessionCommand::SetParam {
                name: "a".to_string(),
                value: dec!(1),
            })
            .unwrap();
        command_tx
            .send(SessionCommand::SetParam {
                name: "b".to_string(),
                value: dec!(2),
            })
            .unwrap();
        command_tx
            .send(SessionCommand::SetParam {
                name: "c".to_string(),
                value: dec!(3),
            })
            .unwrap();

        handle.drain_commands(&mut strategy);

        assert_eq!(
            *received.lock().unwrap(),
            vec![
                ("a".to_string(), dec!(1)),
                ("b".to_string(), dec!(2)),
                ("c".to_string(), dec!(3)),
            ]
        );
    }

    #[test]
    fn pause_then_resume_toggles_is_paused() {
        let (mut handle, command_tx, _snapshot_rx) =
            ControlHandle::new(Duration::from_secs(3600), empty_snapshot());
        let mut strategy = RecordingStrategy {
            received: Arc::new(Mutex::new(Vec::new())),
        };

        assert!(!handle.is_paused());

        command_tx.send(SessionCommand::Pause).unwrap();
        handle.drain_commands(&mut strategy);
        assert!(handle.is_paused());

        command_tx.send(SessionCommand::Resume).unwrap();
        handle.drain_commands(&mut strategy);
        assert!(!handle.is_paused());
    }

    #[test]
    fn stop_command_sets_stop_requested() {
        let (mut handle, command_tx, _snapshot_rx) =
            ControlHandle::new(Duration::from_secs(3600), empty_snapshot());
        let mut strategy = RecordingStrategy {
            received: Arc::new(Mutex::new(Vec::new())),
        };

        command_tx.send(SessionCommand::Stop).unwrap();
        let result = handle.drain_commands(&mut strategy);

        assert!(result.stop_requested);
    }
}
