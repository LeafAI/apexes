// crates/apexes-strategy/src/order_executor.rs (new file)
//! Three pluggable traits letting a single unified event-loop core
//! (see `apexes-backtest`'s unified loop, added by a later plan task)
//! drive either simulated (backtest/paper) or real (live-trading)
//! order execution without knowing which. See design spec
//! `docs/superpowers/specs/2026-08-06-apexes-trading-loop-unification-design.md`
//! §二 for full rationale.

use std::collections::HashMap;

use apexes_core::{ApexesError, Fill, MarketEvent, Order, OrderId};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use rust_decimal::Decimal;

use crate::{OrderIntent, Position};

/// Submits non-algo-routed `OrderIntent`s (Market/Limit/Stop/StopLimit)
/// and answers "what's currently open" — the execution backend a
/// unified event loop drives, abstracting over
/// `apexes-backtest::MatchingEngine` (synchronous simulated fills) and
/// `apexes-oms::OrderManager` (asynchronous, fills-arrive-later real
/// submission).
/// Pre-commit guard for simulated resting fills. The last flag indicates
/// terminal execution (including a triggered market stop's IOC remainder).
pub type SimulatedRestingCheck<'a> =
    dyn FnMut(OrderId, &[Fill], bool) -> Result<(), String> + Send + 'a;

/// Simulation-only direct execution guard; quantity is the working remainder.
pub type SimulatedDirectCheck<'a> = dyn FnMut(&[Fill], Decimal) -> Result<(), String> + Send + 'a;

#[async_trait]
pub trait OrderExecutor: Send + Sync {
    /// Submits `intent`, returning the accepted `Order` plus any fills
    /// that happened immediately as a direct result of this call.
    /// `Vec::new()` is a completely normal, expected return (a resting
    /// Limit/Stop order in backtest that didn't cross the book yet;
    /// EVERY live-trading submission, since real fills always arrive
    /// later via `SupplementalFillSource` or `on_market_event`).
    ///
    /// `positions`/`balances`: required by `OrderManager::submit_order`
    /// (live trading) for its own `risk_checker` pre-trade check — a
    /// SECOND, independent check from the one the unified loop already
    /// performs directly, matching today's exact behavior (not new).
    /// `MatchingEngineOrderExecutor` (backtest) ignores both parameters
    /// entirely — `MatchingEngine` performs no risk checking of its
    /// own.
    async fn submit(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        now: DateTime<Utc>,
    ) -> Result<(Order, Vec<Fill>), ApexesError>;

    /// Optional strategy correlation at the actual submission boundary.
    /// Legacy/simulated executors retain the original submission path.
    fn intent_run_scope(&self) -> Option<uuid::Uuid> {
        None
    }

    async fn submit_correlated(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        now: DateTime<Utc>,
        _intent_id: Option<crate::IntentId>,
    ) -> Result<(Order, Vec<Fill>), ApexesError> {
        self.submit(intent, positions, balances, now).await
    }

    async fn submit_simulated_checked(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        now: DateTime<Utc>,
        _check: &mut SimulatedDirectCheck<'_>,
    ) -> Result<(Order, Vec<Fill>), ApexesError> {
        self.submit(intent, positions, balances, now).await
    }

    fn simulated_margin_rates(
        &self,
        _intent: &OrderIntent,
        _now: DateTime<Utc>,
    ) -> Result<(Decimal, Decimal), ApexesError> {
        Err(ApexesError::OrderRejected {
            reason: "simulated margin budget unavailable".into(),
        })
    }

    /// Read-only simulated Spot buy budget, including modeled execution fees.
    /// Live executors do not participate in event-time capital reservation.
    fn simulated_buy_budget(
        &self,
        _intent: &OrderIntent,
        _now: DateTime<Utc>,
    ) -> Result<Decimal, ApexesError> {
        Err(ApexesError::OrderRejected {
            reason: "simulated buy budget unavailable".into(),
        })
    }

    /// Read-only validated book observation. No assumption of live
    /// exchange depth or of integrity in legacy simulation.
    fn book_observation(
        &self,
        _event: &MarketEvent,
        _now: DateTime<Utc>,
    ) -> Option<crate::BookObservation> {
        None
    }

    /// Read-only modeled fee assumptions. Unfrozen schedules, unknown
    /// routes and live execution do not imply any known fee rate.
    fn cost_observation(
        &self,
        _instrument: &apexes_core::Instrument,
    ) -> Option<crate::CostObservation> {
        None
    }

    /// Optional snapshot of a real OMS submission gate. Simulation and
    /// unsupported executors do not imply a healthy live session.
    fn health_observation(&self) -> Option<crate::HealthObservation> {
        None
    }

    /// Cancel all local simulated working orders before emergency execution.
    /// Never dispatches a live cancellation by default.
    fn cancel_simulated_working(&self) -> Result<Vec<OrderId>, ApexesError> {
        Err(ApexesError::OrderRejected {
            reason: "simulated cancellation unavailable".into(),
        })
    }

    fn on_market_event_checked(
        &self,
        event: &MarketEvent,
        _check: &mut SimulatedRestingCheck<'_>,
    ) -> Vec<(OrderId, Vec<Fill>)> {
        self.on_market_event(event)
    }

    /// Gives this executor's local resting-order book a chance to react
    /// to a new `MarketEvent` on the SAME primary market-data stream the
    /// unified loop is already iterating (not an independent stream —
    /// see `SupplementalFillSource` for that). Returns any fills this
    /// produced, grouped by the original `OrderId`.
    ///
    /// Backtest: forwards to `MatchingEngine::on_trade_event`/
    /// `on_depth_update` depending on `event`'s variant.
    /// Live trading: always returns `Vec::new()` — live trading holds
    /// no local resting-order simulation to react to market data with.
    fn on_market_event(&self, event: &MarketEvent) -> Vec<(OrderId, Vec<Fill>)>;

    /// Consume explicit completion evidence from the last market-event
    /// processing pass, after its fills have been applied. Unsupported
    /// backends never infer completion from open-order disappearance.
    fn take_simulated_completions(&self) -> Vec<Order> {
        Vec::new()
    }

    /// Consume explicit failed-conversion/IOC-remainder evidence from the
    /// last simulated event. No evidence is inferred from absent orders.
    fn take_simulated_trigger_terminals(&self) -> Vec<crate::SimulatedTriggerTerminal> {
        Vec::new()
    }

    /// Every currently open (unfilled or partially-filled) order,
    /// keyed by `OrderId` — feeds `StrategyContext::open_orders`.
    /// Returns an owned `HashMap` (a clone each call); see this
    /// plan's Global Constraints for why that's a deliberate tradeoff,
    /// not a gap. `async fn` (not synchronous): `OrderManager::open_orders()`
    /// requires `.await`ing a `tokio::sync::Mutex` guard; the backtest
    /// implementation (a `std::sync::Mutex`) simply locks/clones/returns
    /// synchronously inside the `async fn` body without ever actually
    /// crossing an `.await` point.
    async fn open_orders(&self) -> HashMap<OrderId, Order>;

    /// Acknowledge a supplemental batch only after the ledger processed it.
    /// Simulated executors need no acknowledgement.
    async fn acknowledge_supplemental_fills(&self, _fills: &[Fill], _applied: bool) {}

    /// Managed live executors return durable OMS linkage aligned to the
    /// actual applied fill batch. None preserves the legacy/simulated path.
    async fn persisted_supplemental_fill_links(
        &self,
        _fills: &[Fill],
    ) -> Option<Vec<crate::PersistedFillLink>> {
        None
    }

    /// A contradictory or unavailable managed attribution is not evidence
    /// of non-execution. The ledger still receives the authoritative fill.
    fn report_live_fill_attribution_fault(&self) {}

    /// Only managed live executors supply positively reconciled terminal
    /// snapshots. Empty means no evidence ready, not a terminal inference.
    async fn ready_live_terminals(&self) -> Vec<crate::LiveTerminalEvidence> {
        Vec::new()
    }

    /// Advance the current-process receipt only after callbacks return.
    async fn acknowledge_live_terminals(&self, _ids: &[OrderId]) {}

    /// Explicit, opt-in publication at the completed unified-loop stop
    /// boundary. Legacy/simulated backends never request a checkpoint.
    fn checkpoint_predecessor(&self) -> Option<crate::checkpoint::CheckpointReceipt> {
        None
    }

    /// The loop calls this only after all synchronous callbacks for its
    /// completed observations and supplemental batches returned. The
    /// backend must independently verify the durable execution prefix.
    async fn publish_completed_checkpoint(
        &self,
        _previous: &crate::checkpoint::CheckpointReceipt,
        _cursor: crate::checkpoint::StateCursor,
        _last_intent: u64,
        _envelope: &[u8],
    ) -> Result<(), ApexesError> {
        Err(ApexesError::InvalidBacktestConfig {
            reason: "checkpoint publication backend is unsupported".into(),
        })
    }

    /// Optional durable inspection attachment. Backends must explicitly
    /// support this, rather than silently dropping requested evidence.
    async fn publish_completed_checkpoint_with_capture(
        &self,
        _previous: &crate::checkpoint::CheckpointReceipt,
        _cursor: crate::checkpoint::StateCursor,
        _last_intent: u64,
        _envelope: &[u8],
        _capture: &[u8],
    ) -> Result<(), ApexesError> {
        Err(ApexesError::InvalidBacktestConfig {
            reason: "durable checkpoint capture backend is unsupported".into(),
        })
    }

    /// Observe returns from the callbacks for a fully applied supplemental batch.
    /// This does not persist strategy state or replay callbacks after restart.
    async fn acknowledge_supplemental_notifications(&self, _fills: &[Fill], _returned: bool) {}
}

/// Distinguishes why a position is being force/partially-closed, since
/// live trading's auto-execution policy differs by reason (see
/// `LiquidationPolicy::should_auto_execute`'s doc comment).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiquidationReason {
    /// A single leveraged position's margin ratio breached its
    /// maintenance-margin threshold (`docs/FEATURES.md` row `6.4`).
    MarginBreach,
    /// Extreme market conditions triggered a proportional de-risking
    /// partial close (`docs/FEATURES.md` row `6.5`).
    ExtremeMarketCondition,
}

/// Decides whether a computed liquidation/de-risking order should
/// actually be submitted automatically, or merely logged
/// (monitor-only) for an operator to act on.
pub trait LiquidationPolicy: Send + Sync {
    fn should_auto_execute(&self, reason: LiquidationReason) -> bool;
}

/// A source of fills that is genuinely independent of the primary
/// market-event stream the unified loop iterates — e.g. live trading's
/// exchange push-update channel plus its periodic `reconcile()` poll,
/// merged into one stream. Backtest/paper have no such independent
/// source (all their fills come from `OrderExecutor::submit`'s return
/// value or `OrderExecutor::on_market_event`), so their implementation
/// is a stream that never yields.
pub trait SupplementalFillSource: Send {
    fn stream(&mut self) -> impl Stream<Item = Vec<Fill>> + Unpin + '_;
}
