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
