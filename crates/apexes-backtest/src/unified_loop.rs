//! The single unified event-loop core (`run_unified_event_loop`) driving
//! `run_backtest` (historical replay, `Vec<MarketEvent>` wrapped as a
//! `Stream`), `apexes-paper`'s `run_paper_trading` (live
//! `MarketDataProvider::subscribe()` stream), and
//! `apexes-live-trading`'s `run_live_trading` (live exchange
//! push-updates + reconcile poll).
//!
//! This module replaces a formerly per-crate-duplicated hand-written
//! event loop (see design spec
//! `2026-08-06-apexes-trading-loop-unification-design.md`). The core
//! per-tick logic is unchanged from what each caller used to do
//! directly; only the MECHANISM by which orders reach an execution
//! backend changed, from direct backend-specific method calls to the
//! three `apexes_strategy` traits:
//!
//! - [`OrderExecutor`] -- submits intents, reacts to market events with
//!   its own resting-order book (if it has one), and owns the
//!   `OrderId -> Order` bookkeeping this loop used to keep itself.
//! - [`LiquidationPolicy`] -- decides whether a computed liquidation /
//!   de-risking order is actually submitted or merely logged.
//! - [`SupplementalFillSource`] -- an asynchronous fill source
//!   independent of the primary market-event stream (live trading's
//!   exchange push updates + `reconcile()` poll); backtest/paper pass a
//!   never-yielding implementation.
//!
//! This module is `pub` (cross-crate visible so `apexes-paper` and
//! `apexes-live-trading` can call it) but deliberately NOT re-exported
//! from `apexes-backtest`'s top-level `lib.rs` -- callers use the full
//! path `apexes_backtest::unified_loop::run_unified_event_loop`.
//! `run_backtest` remains `apexes-backtest`'s primary public entry
//! point.

use crate::clock::BacktestClock;
use crate::control::{ControlHandle, SessionSnapshot};
use crate::cost_model::{CostModel, FlatFeeSchedule};
use crate::diagnostics::{Diagnostics, error_code, rule_code};
use crate::exit_attribution::ExitReason;
use crate::latency::LatencyModel;
use crate::ledger::PortfolioLedger;
use crate::performance::PerformanceTracker;
use crate::report::BacktestReport;
use apexes_core::{ApexesError, Fill, FillId, Instrument, MarketEvent, OrderType};
use apexes_ems::Ems;
use apexes_risk::{CompositeRiskChecker, MarketConditionEventMonitor, PreTradeCheck};
use apexes_strategy::{
    LiquidationPolicy, LiquidationReason, OrderExecutor, ParentOrderId, ParentOrderStatus,
    Position, Strategy, StrategyContext, SupplementalFillSource,
};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Configuration shared by every caller of [`run_unified_event_loop`]:
/// `run_backtest` (whose public config type is `BacktestConfig`),
/// `run_paper_trading` (`PaperTradingConfig`), and -- from a later plan
/// task -- `run_live_trading` (`LiveTradingConfig`). Each caller
/// constructs this shared internal struct before calling the loop.
///
/// `risk_checker` is `Option<Arc<CompositeRiskChecker>>` (not an owned
/// `CompositeRiskChecker`) because every caller resolves it via
/// `registry.try_one::<Arc<CompositeRiskChecker>>()` (sub-project #9's
/// registry retrofit) before constructing this struct --
/// `run_unified_event_loop` itself stays `Registry`-agnostic.
///
/// Deliberately ABSENT: `cost_model`/`queue_position_factor` (the
/// backtest/paper matching-simulation parameters). Those stay on the
/// caller's own public config type and are baked into the
/// `MatchingEngine` the caller constructs before building its
/// `Arc<dyn OrderExecutor>` -- live trading has no equivalent concept
/// (real fills have no "queue position model"). See
/// [`UnifiedLoopConfig::funding_rate_bps`] for the one funding-related
/// exception.
pub struct UnifiedLoopConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    /// Pre-existing positions to seed the ledger with (live startup
    /// recovery from the exchange). Empty for backtest/paper.
    pub initial_positions: HashMap<String, Position>,
    /// Funding rate (in basis points) for open leveraged positions;
    /// `None` disables funding settlement entirely.
    ///
    /// This is the ONE piece of `CostModel` the unified loop itself
    /// needs, and it is genuinely shared by all three callers (today's
    /// `run_live_trading` performs the same funding settlement against
    /// its own `LiveTradingConfig::cost_model`). The loop's periodic
    /// `PortfolioLedger::apply_funding_payments` pass requires a
    /// `&CostModel`, but the full `CostModel` (which also carries the
    /// fee schedule) belongs to the caller's `MatchingEngine`, now
    /// hidden behind `Arc<dyn OrderExecutor>` and unreachable from
    /// here. Passing just the rate is provably equivalent:
    /// `apply_funding_payments` consults ONLY
    /// `CostModel::funding_payment`, which reads `funding_rate_bps` and
    /// never touches `fee_schedule`, so the loop reconstructs a minimal
    /// zero-fee `CostModel` carrying this rate purely for that call.
    /// Callers must pass the same rate their own `CostModel` uses.
    pub funding_rate_bps: Option<Decimal>,
    pub risk_free_return: Decimal,
    pub risk_checker: Option<Arc<CompositeRiskChecker>>,
    /// Tiered maintenance-margin tables, keyed by symbol. Empty
    /// `HashMap` disables all margin/liquidation monitoring (every
    /// leveraged position's `margin_status` lookup simply finds no
    /// table and produces no margin_ratio -- an intentional silent
    /// no-op for callers that never use `OrderIntent.leverage`).
    pub margin_tiers: HashMap<String, apexes_strategy::MarginTierTable>,
    /// Extreme-market circuit breaker configuration (design spec
    /// `2026-08-04-apexes-extreme-market-circuit-breaker-design.md`).
    /// `None` (the default for every pre-existing caller) is a silent
    /// no-op: no volatility/jump detection, no de-risking.
    pub market_condition_config: Option<apexes_risk::MarketConditionConfig>,
    /// Live-tuning/monitoring control channel (design spec
    /// `2026-08-05-apexes-13c-strategy-monitoring-tuning-design.md`
    /// §5). `None` (the default for every pre-existing caller) is a
    /// silent no-op: no command draining, no pause gating, no
    /// snapshot pushes.
    pub control: Option<ControlHandle>,
    /// A last price per symbol known before the first event (live startup
    /// asks the exchange). Without it an asset the account holds is valued at
    /// the ledger's 1.0 placeholder until its first trade arrives, which on a
    /// quiet market can be minutes. Empty for backtest/paper.
    pub initial_prices: HashMap<String, Decimal>,
    /// The exchange's trading rules per symbol (quantity step, minimums, tick).
    /// Every order is conformed to its symbol's rules before the risk check,
    /// and one that cannot be made valid is refused with the reason. A symbol
    /// without an entry is sent as the strategy asked, and the exchange
    /// decides. Empty for backtest/paper.
    pub symbol_rules: HashMap<String, apexes_core::SymbolRules>,
}

/// Runs the unified event loop: consumes `market_events` in order
/// through `strategy`, executing orders through `executor`, applying
/// `apexes-risk` checks if configured, tracking portfolio state via a
/// `PortfolioLedger`, and producing a `BacktestReport`.
///
/// `clock` paces dispatch (an already-configured `BacktestClock` --
/// historical-replay callers pass one built from their `ReplaySpeed`;
/// live/paper callers pass `BacktestClock::new(ReplaySpeed::AsFastAsPossible)`
/// since live events already arrive at real-time pace and need no
/// additional artificial pacing).
///
/// `ems` is supplied by the caller rather than constructed here,
/// because the `ChildOrderSink` an `Ems` submits through is
/// backend-specific (backtest/paper wrap the very same
/// `Arc<Mutex<MatchingEngine>>` they built `executor` from, so EMS
/// child-order fills and direct-strategy fills interact through one
/// single shared order book; live trading wraps its `OrderManager` and
/// uses `PostgresEms` for crash-recoverable algo state).
///
/// `stop_signal`, if `Some`, causes the loop to exit early (returning
/// whatever `BacktestReport` has accumulated so far, not an error) once
/// it fires or its sender is dropped. `run_backtest` always passes
/// `None` (historical replay runs to completion by construction, since
/// `market_events` is a finite pre-loaded stream).
///
/// `latency_model`, if `Some`, is applied (via `LatencyModel::apply`,
/// a real `tokio::time::sleep`) after a submitted `OrderIntent` passes
/// its `PreTradeCheck` and before it reaches `executor`. `run_backtest`
/// always passes `None` (historical replay has no "order submission
/// delay" concept -- recorded timestamps are already fixed).
// The parameter count is inherent to this being the one shared core
// every caller drives; the design spec specifies this shape explicitly
// (minus `venue_scorer`/`volume_curve_provider`, which moved into the
// caller-constructed `ems` -- see that parameter's docs).
#[allow(clippy::too_many_arguments)]
pub async fn run_unified_event_loop(
    config: UnifiedLoopConfig,
    market_events: impl Stream<Item = MarketEvent> + Unpin,
    clock: BacktestClock,
    executor: Arc<dyn OrderExecutor>,
    liquidation_policy: Arc<dyn LiquidationPolicy>,
    supplemental_fills: impl SupplementalFillSource,
    ems: Arc<dyn Ems>,
    strategy: impl Strategy,
    stop_signal: Option<tokio::sync::oneshot::Receiver<()>>,
    latency_model: Option<LatencyModel>,
) -> Result<BacktestReport, ApexesError> {
    run_unified_event_loop_with_attribution(
        config,
        market_events,
        clock,
        executor,
        liquidation_policy,
        supplemental_fills,
        ems,
        strategy,
        stop_signal,
        latency_model,
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_unified_event_loop_with_attribution(
    config: UnifiedLoopConfig,
    mut market_events: impl Stream<Item = MarketEvent> + Unpin,
    mut clock: BacktestClock,
    executor: Arc<dyn OrderExecutor>,
    liquidation_policy: Arc<dyn LiquidationPolicy>,
    mut supplemental_fills: impl SupplementalFillSource,
    ems: Arc<dyn Ems>,
    mut strategy: impl Strategy,
    mut stop_signal: Option<tokio::sync::oneshot::Receiver<()>>,
    latency_model: Option<LatencyModel>,
    mut funding_report: Option<crate::funding_attribution::FundingReport>,
    mut fill_origins: Option<&mut Vec<ExitReason>>,
) -> Result<BacktestReport, ApexesError> {
    let instruments_by_symbol: HashMap<String, Instrument> = config
        .instruments
        .iter()
        .map(|instrument| (instrument.symbol.clone(), instrument.clone()))
        .collect();

    // What the account held before the session traded: the baseline the
    // strategy's PnL is separated from. Taken here, before any fill moves a
    // balance (the start equity is fixed after the first event's fills).
    let starting_balances = config.initial_balances.clone();
    let mut ledger = PortfolioLedger::new(instruments_by_symbol.clone(), config.initial_balances)
        .with_positions(config.initial_positions);
    // A minimal, zero-fee `CostModel` existing solely to carry
    // `funding_rate_bps` into `PortfolioLedger::apply_funding_payments`
    // -- see `UnifiedLoopConfig::funding_rate_bps` for why the real
    // `CostModel` (owned by the caller's `MatchingEngine`, behind
    // `executor`) cannot be reached from here, and why this is
    // equivalent.
    let funding_cost_model = CostModel {
        fee_schedule: Box::new(FlatFeeSchedule {
            maker_bps: Decimal::ZERO,
            taker_bps: Decimal::ZERO,
        }),
        funding_rate_bps: config.funding_rate_bps,
    };
    let mut open_parent_orders: HashMap<ParentOrderId, ParentOrderStatus> = HashMap::new();
    let mut overall_tracker = PerformanceTracker::new(config.risk_free_return);
    let mut per_symbol_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut per_venue_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut last_prices: HashMap<String, Decimal> =
        seeded_last_prices(&config.initial_prices, &instruments_by_symbol);
    let mut price_history = crate::history::PriceHistory::new();
    let mut mark_prices: HashMap<String, Decimal> = HashMap::new();
    let mut last_funding_settlement: Option<chrono::DateTime<chrono::Utc>> = None;
    let mut all_fills: Vec<Fill> = Vec::new();
    let mut supplemental_seen = HashSet::new();
    let mut notifications = crate::fill_notifications::FillNotifications::default();
    use apexes_strategy::{FillSource, TerminalKind, TerminalOutcome};
    let mut recorded_initial_equity = false;
    let mut diagnostics = Diagnostics::new();
    let mut risk_checker = config.risk_checker;
    let mut control = config.control;
    // Only built for sessions with a control handle: `control: None` stays
    // a true no-op (no extra state, no extra work per tick).
    // What the operator sees under "Recent orders". Only kept when a console
    // can be watching (a `control` handle), like the evaluation tracker.
    let mut order_log = control
        .as_ref()
        .map(|_| crate::order_log::OrderLog::default());
    let mut evaluation = control
        .as_ref()
        .map(|handle| crate::evaluation::EvaluationTracker::new(handle.limit_spec()));

    // Extreme-market circuit breaker (design spec
    // `2026-08-04-apexes-extreme-market-circuit-breaker-design.md`):
    // this loop constructs its OWN local `MarketConditionMonitor` from
    // `config.market_condition_config` and splits it locally --
    // deliberately NOT routed through the caller's
    // `CompositeRiskChecker` registration lists, because this loop also
    // needs the `MarketConditionReadHandle` (for
    // `on_equity_update`/`reduce_fractions`), which is not one of
    // `CompositeRiskChecker`'s registered trait objects, and the
    // checker arrives as an already-constructed `Arc` whose `with_*`
    // builders consume `self` by value (not something this loop can
    // extend at runtime). `include_spot` is captured BEFORE the config
    // is moved out into `MarketConditionMonitor::new` (a partial move
    // would otherwise make `config.market_condition_config` unusable
    // for the `as_ref()` read at capture time).
    let market_condition_include_spot = config
        .market_condition_config
        .as_ref()
        .map(|c| c.include_spot)
        .unwrap_or(false);
    let mut market_condition_monitor = config
        .market_condition_config
        .map(apexes_risk::MarketConditionMonitor::new)
        .map(apexes_risk::MarketConditionMonitor::split);

    // Constructed ONCE, outside the loop: an implementation may hold
    // internal state across yields (live trading's merges an exchange
    // push stream with a `reconcile()` interval timer), which
    // re-creating the stream every iteration would disturb.
    let mut supplemental_fill_stream = supplemental_fills.stream();
    let mut arrivals = match latency_model {
        Some(LatencyModel::EventTime(policy)) => {
            policy
                .validate()
                .map_err(|reason| ApexesError::InvalidBacktestConfig { reason })?;
            Some(crate::event_latency::ArrivalQueue::new(policy))
        }
        _ => None,
    };

    loop {
        // Polled at the top of each iteration -- NOT as a
        // `tokio::select!` arm. Both `run_paper_trading` and
        // `run_live_trading` depend on this precise shape: each gates
        // its own event stream with `take_until(stop_signal)` because
        // this loop only observes the signal here, and moving the check
        // into the `select!` below would change that documented
        // interaction.
        if let Some(signal) = stop_signal.as_mut() {
            match signal.try_recv() {
                Ok(()) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => break,
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
            }
        }

        tokio::select! {
            event = market_events.next() => {
                let Some(event) = event else { break };

                let timestamp=clock.logical_timestamp(&event, arrivals.is_some())?;
                if arrivals.as_mut().is_some_and(|q|!q.observe(timestamp)){continue;}
                if !recorded_initial_equity {
                    let initial_equity = ledger.equity(&last_prices)?;
                    overall_tracker.record_equity_point(timestamp, initial_equity);
                    recorded_initial_equity = true;
                    // The exchange's prices from before the first event are
                    // real observations: the charts start from them.
                    for (symbol, price) in &config.initial_prices {
                        price_history.record(symbol, timestamp, *price);
                    }
                }

                diagnostics.next_event();
                clock.wait_for_next_event(timestamp).await;

                // Feed every event to the local `MarketConditionMonitor` (a
                // no-op for events that don't match its configured
                // `PriceSource`) so its per-symbol volatility/jump state -- and
                // hence its de-risking fractions -- stay current. This must run
                // BEFORE the de-risking block later in the same tick reads
                // `reduce_fractions`.
                if let Some((_, event_monitor, _)) = &mut market_condition_monitor
                    && let Err(violations) = event_monitor.on_market_event(&event)
                {
                    tracing::warn!(?violations, "market condition monitor flagged a violation");
                }

                // Price-map maintenance, split by event type exactly as
                // before: `last_prices` (used to value non-quote-currency
                // balances) is keyed by BASE ASSET (e.g. "BTC") from Trade
                // prints; `mark_prices` (used by every `PortfolioLedger`
                // margin method) is keyed the same way from MarkPrice
                // updates. Tick/Bar events only advance the clock/context.
                // This runs BEFORE `executor.on_market_event` below,
                // preserving the former per-arm ordering (`last_prices`
                // was inserted before `MatchingEngine::on_trade_event`).
                match &event {
                    MarketEvent::Trade(trade) => {
                        price_history.record(&trade.symbol, trade.timestamp, trade.price);
                        ledger.observe_spot_price(&trade.symbol, trade.timestamp, trade.price);
                        if let Some(instrument) = instruments_by_symbol.get(&trade.symbol)
                            && let Some(base) = &instrument.base_currency
                        {
                            last_prices.insert(base.clone(), trade.price);
                        }
                    }
                    MarketEvent::MarkPrice(update) => {
                        if let Some(instrument) = instruments_by_symbol.get(&update.symbol)
                            && let Some(base) = &instrument.base_currency
                        {
                            mark_prices.insert(base.clone(), update.mark_price);
                        }
                    }
                    _ => {}
                }

                let mut emergency_event = false;
                if let Some(queue) = &mut arrivals {
                    // Settle the boundary against positions held entering
                    // this observed event, not orders that will arrive on it.
                    let boundary = last_funding_boundary(timestamp);
                    if last_funding_settlement != Some(boundary) {
                        crate::funding_attribution::settle(&mut ledger, &mark_prices, &funding_cost_model, boundary, timestamp, funding_report.as_mut())?;
                        last_funding_settlement = Some(boundary);
                    }
                    // Sample current marked equity once before matching.
                    // Execution-induced changes become visible next event.
                    if let Some((_, _, monitor)) = &market_condition_monitor {
                        let equity = ledger.equity(&mark_prices).unwrap_or(Decimal::ZERO);
                        if let Err(violations) = monitor.on_equity_update(equity) {
                            tracing::warn!(?violations, "account-wide market condition violation");
                        }
                    }
                    let margin_breach = liquidation_policy.should_auto_execute(LiquidationReason::MarginBreach)
                        && !ledger.check_liquidations(&mark_prices, &config.margin_tiers).is_empty();
                    let extreme = liquidation_policy.should_auto_execute(LiquidationReason::ExtremeMarketCondition)
                        && market_condition_monitor.as_ref().is_some_and(|(_, _, monitor)| {
                            let symbols: Vec<_> = ledger.positions().keys().cloned().collect();
                            !ledger.check_de_risking(&monitor.reduce_fractions(&symbols), market_condition_include_spot).is_empty()
                        });
                    if margin_breach || extreme {
                        preempt_simulated_orders(queue, executor.as_ref(), ems.as_ref(), &mut open_parent_orders, &mut strategy, timestamp).await;
                        emergency_event = true;
                    }
                }

                // Let the execution backend's own resting-order book react
                // to this event (backtest/paper: the `MatchingEngine`'s
                // depth-snapshot ingestion and resting-order matching; live
                // trading: nothing, it holds no local simulation). The
                // executor also owns the `OrderId -> Order`
                // filled_quantity/status bookkeeping this loop used to do
                // inline here.
                let resting_fills = if let Some(queue) = &mut arrivals {
                    let mut projected = ledger.execution_snapshot();
                    let mut check = |id, fills: &[Fill], terminal| {
                        let mut candidate = projected.execution_snapshot();
                        let result = (|| {
                            if emergency_event {
                                return Err("emergency preempts resting execution".into());
                            }
                            queue.validate_resting_claim(id, fills, projected.positions())?;
                            for fill in fills {
                                candidate.apply_fill(fill).map_err(|error| error.to_string())?;
                            }
                            queue.accept_resting(id, fills, terminal, projected.balances(), candidate.balances())
                        })();
                        if result.is_ok() {
                            projected = candidate;
                        } else {
                            queue.report.resting_capital_refused += 1;
                        }
                        result
                    };
                    executor.on_market_event_checked(&event, &mut check)
                } else {
                    executor.on_market_event(&event)
                };
                if let Some(queue) = &mut arrivals {
                    queue.reconcile_resting(&executor.open_orders().await);
                }
                for (_order_id, fills) in resting_fills {
                    apply_fills(
                        diagnostics, ExitReason::Resting, fill_origins.as_deref_mut(),
                        &mut ledger,
                        &mut overall_tracker,
                        &mut per_symbol_trackers,
                        &mut per_venue_trackers,
                        &mut all_fills,
                        &fills,
                        config.risk_free_return,
                    )?;
                    if let Some(checker) = &mut risk_checker {
                        for fill in &fills {
                            if let Err(violations) =
                                checker.on_fill(fill, ledger.positions(), ledger.balances())
                            {
                                tracing::warn!(
                                    ?violations,
                                    symbol = %fill.symbol,
                                    "in-trade risk monitor flagged a violation"
                                );
                            }
                        }
                    }
                    notifications.deliver(&mut strategy, timestamp, FillSource::Resting, &fills);
                }

                for order in executor.take_simulated_completions() {
                    strategy.on_order_terminal(&TerminalOutcome { at: timestamp, kind: TerminalKind::Filled { order: &order } });
                }

                let mut checked_child_fills = HashSet::new();
                let ems_result = if let Some(queue) = &mut arrivals {
                    let open_orders = executor.open_orders().await;
                    let mut projected = ledger.execution_snapshot();
                    let mut check = |parent: Option<ParentOrderId>, intent: &apexes_strategy::OrderIntent, fills: &[Fill]| {
                        if emergency_event {
                            return Err("emergency preempts child execution".into());
                        }
                        let priced = apexes_strategy::OrderIntent {
                            // Conservative notional valuation across the proposed
                            // execution, not a stale last-trade reference.
                            price: fills.iter().map(|fill| fill.price).max(),
                            ..intent.clone()
                        };
                        if let Some(checker) = &risk_checker {
                            checker.check_child(&priced, projected.positions(), projected.balances(), &open_orders, timestamp)
                                .map_err(|violations| join_violations(&violations))?;
                        }
                        if let Some((pre_trade, _, _)) = &market_condition_monitor {
                            pre_trade.check_child(&priced, projected.positions(), projected.balances(), &open_orders, timestamp)
                                .map_err(|violations| join_violations(&violations))?;
                        }
                        // Validate the entire child's proposed execution before
                        // committing any depth, including all fees and margin.
                        queue.validate_child_claim(parent, intent, projected.positions())?;
                        let mut candidate = projected.execution_snapshot();
                        for fill in fills {
                            candidate.apply_fill(fill).map_err(|error| error.to_string())?;
                        }
                        queue.accept_child(parent, intent, projected.balances(), candidate.balances())?;
                        // Each accepted child advances the projection before
                        // the next competing child checks the same account.
                        if let Some(checker) = &risk_checker {
                            for fill in fills {
                                if let Err(violations) = checker.on_fill(fill, candidate.positions(), candidate.balances()) {
                                    tracing::warn!(?violations, symbol = %fill.symbol, "in-trade risk monitor flagged a child violation");
                                }
                                checked_child_fills.insert(fill.id);
                            }
                        }
                        projected = candidate;
                        Ok(())
                    };
                    ems.on_market_event_checked(&event, &mut check).await
                } else {
                    ems.on_market_event(&event).await
                };
                let ems_fills = ems_result.map_err(|error| {
                    ApexesError::InvalidBacktestConfig {
                        reason: format!("ems.on_market_event failed: {error}"),
                    }
                })?;
                if !ems_fills.is_empty() {
                    if let Some(log) = &mut order_log {
                        log.filled(&ems_fills);
                    }
                    apply_fills(
                        diagnostics, ExitReason::Ems, fill_origins.as_deref_mut(),
                        &mut ledger,
                        &mut overall_tracker,
                        &mut per_symbol_trackers,
                        &mut per_venue_trackers,
                        &mut all_fills,
                        &ems_fills,
                        config.risk_free_return,
                    )?;
                    if let Some(checker) = &mut risk_checker {
                        for fill in &ems_fills {
                            if checked_child_fills.contains(&fill.id) {
                                continue;
                            }
                            if let Err(violations) =
                                checker.on_fill(fill, ledger.positions(), ledger.balances())
                            {
                                tracing::warn!(
                                    ?violations,
                                    symbol = %fill.symbol,
                                    "in-trade risk monitor flagged a violation"
                                );
                            }
                        }
                    }
                    notifications.deliver(&mut strategy, timestamp, FillSource::Ems, &ems_fills);
                }
                refresh_open_parent_orders(ems.as_ref(), &mut open_parent_orders).await;
                if let Some(queue) = &mut arrivals {
                    queue.reconcile_parents(&open_parent_orders);
                }

                // Funding settlement: once per crossed Binance-aligned 8-hour
                // UTC boundary (00:00/08:00/16:00) -- not per-boundary-precise
                // if several boundaries fall within one tick's gap; a single
                // settlement pass is applied per tick that crosses at least
                // one new boundary.
                let current_funding_boundary = last_funding_boundary(timestamp);
                if last_funding_settlement != Some(current_funding_boundary) {
                    crate::funding_attribution::settle(&mut ledger, &mark_prices, &funding_cost_model, current_funding_boundary, timestamp, funding_report.as_mut())?;
                    last_funding_settlement = Some(current_funding_boundary);
                }

                let margin_status = ledger.margin_status(&mark_prices, &config.margin_tiers);
                if let Some(checker) = &risk_checker
                    && let Err(violations) = checker.on_tick(&margin_status)
                {
                    tracing::warn!(?violations, "position monitor flagged a margin violation");
                }

                // Auto-liquidation: every position whose margin ratio has
                // breached 1.0 is force-closed with a market order through
                // `executor` -- but ONLY if `liquidation_policy` allows it.
                // Backtest/paper always do (`BacktestLiquidationPolicy`);
                // live trading never does for this reason, logging loudly
                // instead, since real liquidation is the exchange's own
                // responsibility.
                let symbols_to_liquidate = ledger.check_liquidations(&mark_prices, &config.margin_tiers);
                for symbol in symbols_to_liquidate {
                    let Some(position) = ledger.positions().get(&symbol).cloned() else {
                        continue;
                    };
                    if !liquidation_policy.should_auto_execute(LiquidationReason::MarginBreach) {
                        tracing::error!(
                            symbol = %symbol,
                            "position margin ratio has breached the liquidation threshold -- \
                             the configured LiquidationPolicy forbids auto-liquidation, so no \
                             order was submitted; new orders for this symbol are blocked via \
                             MarginMonitor."
                        );
                        continue;
                    }
                    let liquidation_intent = apexes_strategy::OrderIntent {
                        symbol: symbol.clone(),
                        venue: instruments_by_symbol
                            .get(&symbol)
                            .map(|i| i.venue)
                            .unwrap_or(apexes_core::Venue::Binance),
                        asset_class: instruments_by_symbol
                            .get(&symbol)
                            .map(|i| i.asset_class)
                            .unwrap_or(apexes_core::AssetClass::Perpetual),
                        side: if position.quantity.is_sign_positive() {
                            apexes_core::Side::Sell
                        } else {
                            apexes_core::Side::Buy
                        },
                        order_type: OrderType::Market,
                        quantity: position.quantity.abs(),
                        price: None,
                        stop_price: None,
                        execution_algo: None,
                        routing: apexes_strategy::OrderRouting::Fixed,
                        // The closing fill must carry the position's OWN
                        // leverage/margin_mode: `PortfolioLedger::apply_fill`
                        // dispatches on `fill.leverage.is_some()`, so a
                        // `leverage: None` close would take the SPOT path and
                        // fail with `InsufficientBalance` (a leveraged perpetual
                        // account holds no base-asset balance to debit) instead
                        // of closing the position via `close_leveraged`.
                        leverage: position.leverage,
                        margin_mode: position.margin_mode,
                    };
                    if !emergency_event && let Some(queue) = &mut arrivals {
                        preempt_simulated_orders(queue, executor.as_ref(), ems.as_ref(), &mut open_parent_orders, &mut strategy, timestamp).await;
                        emergency_event = true;
                    }
                    let trace = diagnostics.intent(&liquidation_intent, "liquidation", timestamp);
                    match executor
                        .submit(&liquidation_intent, ledger.positions(), ledger.balances(), timestamp)
                        .await
                    {
                        Ok((order, fills)) => {
                            trace.accepted(&order);
                            tracing::error!(symbol = %symbol, "position force-liquidated (margin ratio breached)");
                            apply_fills(
                                diagnostics, ExitReason::Liquidation, fill_origins.as_deref_mut(),
                                &mut ledger,
                                &mut overall_tracker,
                                &mut per_symbol_trackers,
                                &mut per_venue_trackers,
                                &mut all_fills,
                                &fills,
                                config.risk_free_return,
                            )?;
                            notifications.deliver(&mut strategy, timestamp, FillSource::Liquidation, &fills);
                        }
                        Err(error) => {
                            trace.outcome("submission_failed", error_code(&error));
                            tracing::error!(reason = error_code(&error), symbol = %symbol, "liquidation order submission failed");
                        }
                    }
                }

                // Extreme-market de-risking: every symbol
                // `MarketConditionMonitor` has flagged gets a partial close
                // proportional to its computed `reduce_fraction`, submitted
                // as a market order through `executor` -- again only if
                // `liquidation_policy` allows it for this reason (live
                // trading defaults to monitor-only, gated on its own
                // `auto_derisk_live` flag).
                if let Some((_, _, read_handle)) = &market_condition_monitor {
                    if arrivals.is_none() {
                        let equity_for_volatility = ledger.equity(&mark_prices).unwrap_or(Decimal::ZERO);
                        if let Err(violations) = read_handle.on_equity_update(equity_for_volatility) {
                            tracing::warn!(?violations, "account-wide market condition violation");
                        }
                    }

                    let open_symbols: Vec<String> = ledger.positions().keys().cloned().collect();
                    let reduce_fractions = read_handle.reduce_fractions(&open_symbols);
                    let de_risking_orders =
                        ledger.check_de_risking(&reduce_fractions, market_condition_include_spot);

                    for (symbol, side, quantity) in de_risking_orders {
                        let Some(position) = ledger.positions().get(&symbol).cloned() else {
                            continue;
                        };
                        if !liquidation_policy
                            .should_auto_execute(LiquidationReason::ExtremeMarketCondition)
                        {
                            tracing::error!(
                                symbol = %symbol,
                                %quantity,
                                "extreme market conditions detected -- the configured \
                                 LiquidationPolicy forbids automatic de-risking, so no order \
                                 was submitted; new orders for this symbol are blocked via \
                                 MarketConditionMonitor."
                            );
                            continue;
                        }
                        let de_risk_intent = apexes_strategy::OrderIntent {
                            symbol: symbol.clone(),
                            venue: instruments_by_symbol
                                .get(&symbol)
                                .map(|i| i.venue)
                                .unwrap_or(apexes_core::Venue::Binance),
                            asset_class: instruments_by_symbol
                                .get(&symbol)
                                .map(|i| i.asset_class)
                                .unwrap_or(apexes_core::AssetClass::Perpetual),
                            side,
                            order_type: OrderType::Market,
                            quantity,
                            price: None,
                            stop_price: None,
                            execution_algo: None,
                            routing: apexes_strategy::OrderRouting::Fixed,
                            // The closing fill must carry the position's OWN
                            // leverage/margin_mode (same convention as the
                            // auto-liquidation block above): `apply_fill`
                            // dispatches on `fill.leverage.is_some()`, so a
                            // spot position closes via the spot path and a
                            // leveraged one via `close_leveraged`.
                            leverage: position.leverage,
                            margin_mode: position.margin_mode,
                        };
                        if !emergency_event && let Some(queue) = &mut arrivals {
                            preempt_simulated_orders(queue, executor.as_ref(), ems.as_ref(), &mut open_parent_orders, &mut strategy, timestamp).await;
                            emergency_event = true;
                        }
                        let trace = diagnostics.intent(&de_risk_intent, "derisk", timestamp);
                        match executor
                            .submit(&de_risk_intent, ledger.positions(), ledger.balances(), timestamp)
                            .await
                        {
                            Ok((order, fills)) => {
                                trace.accepted(&order);
                                tracing::warn!(symbol = %symbol, %quantity, "position partially closed (extreme market conditions)");
                                apply_fills(
                                    diagnostics, ExitReason::Derisk, fill_origins.as_deref_mut(),
                                    &mut ledger,
                                    &mut overall_tracker,
                                    &mut per_symbol_trackers,
                                    &mut per_venue_trackers,
                                    &mut all_fills,
                                    &fills,
                                    config.risk_free_return,
                                )?;
                                notifications.deliver(&mut strategy, timestamp, FillSource::Derisk, &fills);
                            }
                            Err(error) => {
                                trace.outcome("submission_failed", error_code(&error));
                                tracing::error!(reason = error_code(&error), symbol = %symbol, "de-risking order submission failed");
                            }
                        }
                    }
                }

                // Process due arrivals before making a new decision. In legacy
                // mode the first pass is empty.
                for arriving in [true,false] {
                let intents = if arriving {
                    arrivals.as_mut().map_or_else(Vec::new,|q|q.due(timestamp))
                } else {
                // `open_orders` comes from the executor, fetched fresh --
                // this loop no longer keeps its own map.
                let open_orders = executor.open_orders().await;
                let arrived_margin_status = arrivals.as_ref().map(|_| ledger.margin_status(&mark_prices, &config.margin_tiers));
                let market = apexes_strategy::InstrumentContext::for_event(&event, &config.instruments, &config.symbol_rules);
                let ctx = StrategyContext {
                    health: executor.health_observation(),
                    capital: arrivals.as_ref().and_then(|q| q.capital_snapshot(&config.instruments, ledger.positions(), ledger.balances())),
                    market,
                    costs: market.and_then(|m| executor.cost_observation(m.instrument())).filter(|costs| market.is_some_and(|m| costs.matches(&event, m.instrument()))),
                    book: market.and_then(|_| executor.book_observation(&event, timestamp)).filter(|book| book.matches_at(&event, timestamp)),
                    positions: ledger.positions(),
                    balances: ledger.balances(),
                    open_orders: &open_orders,
                    open_parent_orders: &open_parent_orders,
                    margin_status: arrived_margin_status.as_ref().unwrap_or(&margin_status),
                    now: timestamp,
                };
                let paused = control.as_ref().is_some_and(ControlHandle::is_paused);
                let intents = if paused {
                    // Paused: no new decisions this tick. Everything
                    // BEFORE this point (funding settlement, margin
                    // status, auto-liquidation, de-risking) already ran
                    // unconditionally above and is unaffected by pause
                    // state.
                    Vec::new()
                } else {
                    strategy.on_market_event(&event, &ctx)
                };

                diagnostics.decision(&event, timestamp, paused, intents.len());
                intents
                };
                use apexes_strategy::{AttemptStage, OrderOutcomeKind, order_outcome::notify_direct};
                let stage = if arriving { AttemptStage::Arrival } else { AttemptStage::Decision };
                for intent in intents {
                    if arriving && let Some(queue) = &mut arrivals {
                        queue.release(&intent);
                    }
                    let trace = diagnostics.intent(&intent, if arriving {"strategy_arrival"} else {"strategy"}, timestamp);
                    if emergency_event && let Some(queue) = &mut arrivals {
                        queue.report.emergency_intents_refused += 1;
                        trace.outcome("rules_rejected", "emergency_preemption");
                        notify_direct(&mut strategy, &intent, timestamp, stage, OrderOutcomeKind::Rejected { code: "emergency_preemption" });
                        if let Some(log) = &mut order_log {
                            record_refusal(log, &intent, timestamp, "emergency exit preempts strategy orders");
                        }
                        continue;
                    }
                    // Make the order valid for its symbol (round the quantity
                    // down to the exchange's step, the price to its tick) before
                    // anything judges it: the risk checks must see the order
                    // that will really be sent, and one that cannot be made
                    // valid is refused here, with the reason, not sent to be
                    // refused by the exchange.
                    let intent = match conform_to_rules(
                        &intent,
                        &config.symbol_rules,
                        &instruments_by_symbol,
                        &last_prices,
                    ) {
                        Ok(conformed) => conformed,
                        Err(violation) => {
                            trace.outcome("rules_rejected", rule_code(&violation));
                        notify_direct(&mut strategy, &intent, timestamp, stage, OrderOutcomeKind::Rejected { code: rule_code(&violation) });
                            let reason = violation.to_string();
                            tracing::warn!(
                                symbol = %intent.symbol,
                                quantity = %intent.quantity,
                                %reason,
                                "order refused: it cannot satisfy the exchange's trading rules"
                            );
                            if let Some(log) = &mut order_log {
                                record_refusal(log, &intent, timestamp, &reason);
                            }
                            continue;
                        }
                    };
                    trace.conformed(&intent);
                    // Re-fetched per intent (not reusing `open_orders`
                    // above): the former loop's own map was updated as
                    // each intent was submitted, so a later intent's risk
                    // check saw earlier submissions from the SAME tick.
                    // Querying the executor per intent reproduces that
                    // exactly.
                    let open_orders_for_check = executor.open_orders().await;
                    // A protective classification cannot substitute for
                    // instrument identity, available inventory, or outstanding
                    // parent-order reconciliation. Do not grant entry-budget
                    // exemptions against unrelated wallet holdings.
                    if apexes_risk::is_protective_spot_exit(&intent, ledger.positions(), &open_orders_for_check) {
                        let available = instruments_by_symbol.get(&intent.symbol)
                            .filter(|i| i.venue == intent.venue && i.asset_class == intent.asset_class)
                            .and_then(|i| i.base_currency.as_ref())
                            .and_then(|base| ledger.balances().get(base))
                            .copied().unwrap_or(Decimal::ZERO);
                        if available < intent.quantity || open_parent_orders.values().any(|status|
                            matches!(status, apexes_strategy::ParentOrderStatus::Scheduling)) {
                            trace.outcome("rules_rejected", "protective_inventory_unverified");
                        notify_direct(&mut strategy, &intent, timestamp, stage, OrderOutcomeKind::Rejected { code: "protective_inventory_unverified" });
                            if let Some(log) = &mut order_log {
                                record_refusal(log, &intent, timestamp, "protective inventory or parent orders not reconciled");
                            }
                            continue;
                        }
                    }
                    // A market order has no price, so a notional limit could never
                    // apply to it. For the check only, value it at the last trade
                    // price (the order itself is sent unchanged).
                    let checked_intent =
                        priced_for_risk_check(&intent, &instruments_by_symbol, &last_prices);
                    if let Some(checker) = &risk_checker
                        && let Err(violations) = checker.check(
                            &checked_intent,
                            ledger.positions(),
                            ledger.balances(),
                            &open_orders_for_check,
                            timestamp,
                        )
                    {
                        trace.risk_rejected("risk_rejected", &violations);
                        notify_direct(&mut strategy, &intent, timestamp, stage, OrderOutcomeKind::Rejected { code: "risk_rejected" });
                        tracing::warn!(?violations, symbol = %intent.symbol, "order rejected by risk checker");
                        if let Some(log) = &mut order_log {
                            record_refusal(log, &intent, timestamp, &join_violations(&violations));
                        }
                        continue;
                    }
                    if let Some((pre_trade, _, _)) = &market_condition_monitor
                        && let Err(violations) = pre_trade.check(
                            &intent,
                            ledger.positions(),
                            ledger.balances(),
                            &open_orders_for_check,
                            timestamp,
                        )
                    {
                        trace.risk_rejected("market_condition_rejected", &violations);
                        notify_direct(&mut strategy, &intent, timestamp, stage, OrderOutcomeKind::Rejected { code: "market_condition_rejected" });
                        tracing::warn!(?violations, symbol = %intent.symbol, "order rejected by market condition monitor");
                        if let Some(log) = &mut order_log {
                            record_refusal(log, &intent, timestamp, &join_violations(&violations));
                        }
                        continue;
                    }

                    trace.outcome("pretrade_passed", if risk_checker.is_none() && market_condition_monitor.is_none() { "checks_not_configured" } else { "none" });
                    let mut admitted_hold = None;
                    if let Some(queue)=&mut arrivals {
                        let hold = match intent_capital_hold(&intent, &instruments_by_symbol, executor.as_ref(), timestamp, queue, ledger.positions()) {
                            Ok(hold) if hold.as_ref().is_none_or(|hold| queue.check_capital(hold, ledger.balances())) => hold,
                            result => {
                                queue.capital_refused(!arriving);
                                let reason = result.err().unwrap_or_else(|| "insufficient unreserved capital".into());
                                trace.outcome("rules_rejected", "capital_reservation");
                        notify_direct(&mut strategy, &intent, timestamp, stage, OrderOutcomeKind::Rejected { code: "capital_reservation" });
                                if let Some(log) = &mut order_log { record_refusal(log, &intent, timestamp, &reason); }
                                continue;
                            }
                        };
                        if !arriving {
                            match queue.schedule(timestamp,intent.clone(),hold) {
                                Ok(())=>{
                                    trace.outcome("scheduled","event_latency");
                                    notify_direct(&mut strategy, &intent, timestamp, stage, OrderOutcomeKind::Scheduled);
                                },
                                Err(reason)=>{
                                    trace.outcome("rules_rejected",reason);
                        notify_direct(&mut strategy, &intent, timestamp, stage, OrderOutcomeKind::Rejected { code: reason });
                                    if let Some(log)=&mut order_log {record_refusal(log,&intent,timestamp,reason);}
                                }
                            }
                            continue;
                        }
                        admitted_hold = hold;
                    } else if let Some(latency_model) = &latency_model {
                        latency_model.apply().await;
                    }

                    if let Some(execution_algo) = intent.execution_algo.clone() {
                        let request = apexes_ems::AlgoOrderRequest {
                            symbol: intent.symbol.clone(),
                            venue: intent.venue,
                            side: intent.side,
                            quantity: intent.quantity,
                            execution_algo,
                            routing: intent.routing.clone(),
                            leverage: intent.leverage,
                            margin_mode: intent.margin_mode,
                        };
                        match ems.submit_algo_order(request).await {
                            Ok(parent_id) => {
                                trace.parent_accepted(parent_id);
                                if let (Some(queue), Some(hold)) = (&mut arrivals, admitted_hold) {
                                    queue.reserve_parent(parent_id, &intent, hold);
                                }
                                open_parent_orders
                                    .insert(parent_id, apexes_strategy::ParentOrderStatus::Scheduling);
                                // The parent order is what the strategy asked for and
                                // what counts against the daily allowance; its child
                                // orders are the executor's business.
                                if let Some(checker) = &risk_checker {
                                    checker.on_order_accepted(&intent, timestamp);
                                }
                            }
                            Err(_) => {
                                trace.outcome("parent_submission_failed", "ems_error");
                                tracing::warn!(reason = "ems_error", symbol = %intent.symbol, "algo order submission failed");
                            }
                        }
                        continue;
                    }

                    // Order-type dispatch (Market vs. Limit vs.
                    // Stop/StopLimit) now lives inside the executor,
                    // which also decides whether any fills happened
                    // immediately -- an empty fill vec is normal (a
                    // resting limit order in backtest; every live
                    // submission).
                    let submission = if let Some(queue) = &arrivals {
                        let mut check = |fills: &[Fill], remaining| {
                            let mut candidate = ledger.execution_snapshot();
                            for fill in fills {
                                candidate.apply_fill(fill).map_err(|error| error.to_string())?;
                            }
                            queue.check_direct(admitted_hold.as_ref(), intent.quantity, remaining, ledger.balances(), candidate.balances())
                        };
                        executor.submit_simulated_checked(&intent, ledger.positions(), ledger.balances(), timestamp, &mut check).await
                    } else {
                        executor.submit(&intent, ledger.positions(), ledger.balances(), timestamp).await
                    };
                    match submission {
                        Ok((order, fills)) => {
                            trace.accepted(&order);
                            if let (Some(queue), Some(hold)) = (&mut arrivals, admitted_hold)
                                && matches!(order.status, apexes_core::OrderStatus::New | apexes_core::OrderStatus::PartiallyFilled) {
                                queue.reserve_resting(&order, hold).map_err(|reason| ApexesError::InvalidBacktestConfig { reason })?;
                            }
                            if let Some(log) = &mut order_log {
                                log.submitted(&order);
                                log.filled(&fills);
                                log.terminal_submission(&order);
                            }
                            // Accepted for execution: only now does the order use up
                            // daily allowance. An order the exchange rejects never does,
                            // so a strategy sending rejected orders cannot exhaust the
                            // daily cap and lock out valid ones. The loop is the one
                            // layer that always checks first (live also re-checks inside
                            // `OrderManager`), so it is the single owner of this signal.
                            if let Some(checker) = &risk_checker {
                                checker.on_order_accepted(&intent, timestamp);
                            }
                            if !fills.is_empty() {
                                apply_fills(
                                    diagnostics, ExitReason::Strategy, fill_origins.as_deref_mut(),
                                    &mut ledger,
                                    &mut overall_tracker,
                                    &mut per_symbol_trackers,
                                    &mut per_venue_trackers,
                                    &mut all_fills,
                                    &fills,
                                    config.risk_free_return,
                                )?;
                                if let Some(checker) = &mut risk_checker {
                                    for fill in &fills {
                                        if let Err(violations) =
                                            checker.on_fill(fill, ledger.positions(), ledger.balances())
                                        {
                                            tracing::warn!(
                                                ?violations,
                                                symbol = %fill.symbol,
                                                "in-trade risk monitor flagged a violation"
                                            );
                                        }
                                    }
                                }
                            }
                            notify_direct(&mut strategy, &intent, timestamp, stage, OrderOutcomeKind::Returned { order: &order });
                            notifications.deliver(&mut strategy, timestamp, FillSource::Direct, &fills);
                        }
                        Err(error) => {
                            trace.outcome("submission_failed", error_code(&error));
                            notify_direct(&mut strategy, &intent, timestamp, stage, OrderOutcomeKind::SubmissionFailed { code: error_code(&error) });
                            tracing::warn!(
                                reason = error_code(&error),
                                symbol = %intent.symbol,
                                side = ?intent.side,
                                quantity = %intent.quantity,
                                "order submission rejected"
                            );
                            if let Some(log) = &mut order_log {
                                record_refusal(log, &intent, timestamp, error_code(&error));
                            }
                        }
                    }
                }

                }
                let equity = ledger.equity(&last_prices)?;
                overall_tracker.record_equity_point(timestamp, equity);
                // Bars carry no new price (equity only moves on trades) and are
                // stamped with their OPEN time, which for a daily bar can be
                // many hours ago: recording them would drag the timeline back.
                if let Some(tracker) = &mut evaluation
                    && !matches!(event, MarketEvent::Bar(_))
                {
                    tracker.observe_fills(&all_fills);
                    let started = tracker.record_equity_at(
                        timestamp,
                        equity,
                        ledger.positions().values().any(|p| !p.quantity.is_zero()),
                        holdings_priced(ledger.balances(), &last_prices, &instruments_by_symbol),
                        &last_prices,
                    );
                    if started {
                        tracker.set_baseline(starting_holdings(
                            &starting_balances,
                            &last_prices,
                            &instruments_by_symbol,
                        ));
                    }
                }

                if let Some(handle) = &mut control {
                    let drain_result = handle.drain_commands(&mut strategy);
                    if drain_result.stop_requested {
                        break;
                    }

                    // Throttle check happens BEFORE constructing/cloning
                    // the `SessionSnapshot` -- avoids a wasted
                    // allocation on a snapshot that would just be
                    // thrown away by `maybe_push_snapshot`'s own
                    // (redundant, but harmless) throttle check.
                    if handle.is_snapshot_due() {
                        let mut open_orders: Vec<crate::control::OpenOrderView> = executor
                            .open_orders()
                            .await
                            .values()
                            .map(crate::control::OpenOrderView::from)
                            .collect();
                        open_orders.sort_by_key(|o| o.created_at);
                        let (evaluation_summary, equity_history) = match &evaluation {
                            Some(tracker) => {
                                let (position_exposure, max_abs_position) = exposure_inputs(
                                    ledger.positions(),
                                    &last_prices,
                                    &instruments_by_symbol,
                                );
                                // Exposure is what the account has at risk to
                                // price moves: its non-cash holdings, which
                                // include a pre-existing balance the strategy
                                // never traded (the positions alone miss it).
                                let gross_exposure = position_exposure.max(holdings_value(
                                    ledger.balances(),
                                    &last_prices,
                                    &instruments_by_symbol,
                                ));
                                let realized: Decimal = crate::stable_order::entries(ledger.realized_pnl()).into_iter().map(|(_, value)| *value).sum();
                                (
                                    Some({
                                        let mut summary = tracker.summary_at(
                                            equity, realized, gross_exposure, max_abs_position, &last_prices,
                                        );
                                        summary.spot_pnl = ledger.spot_pnl();
                                        summary
                                    }),
                                    tracker.equity_history(),
                                )
                            }
                            None => (None, Vec::new()),
                        };
                        let recent_orders = match &mut order_log {
                            Some(log) => {
                                let open_ids: std::collections::HashSet<String> =
                                    open_orders.iter().map(|o| o.id.clone()).collect();
                                log.sync_open(timestamp, &open_ids);
                                log.view(crate::order_log::ORDER_LOG_CAP)
                            }
                            None => Vec::new(),
                        };
                        let snapshot = SessionSnapshot {
                            identity: handle.identity(),
                            evaluation: evaluation_summary,
                            equity_history,
                            positions: ledger.positions().clone(),
                            balances: ledger.balances().clone(),
                            equity,
                            realized_pnl: ledger.realized_pnl().clone(),
                            margin_status: margin_status.clone(),
                            params: strategy.current_params(),
                            paused: handle.is_paused(),
                            open_orders,
                            recent_orders,
                            price_history: price_history
                                .view(crate::evaluation::SNAPSHOT_HISTORY_POINTS),
                            equity_priced: holdings_priced(
                                ledger.balances(),
                                &last_prices,
                                &instruments_by_symbol,
                            ),
                            timestamp,
                        };
                        handle.maybe_push_snapshot(snapshot);
                    }
                }
            }

            Some(fills) = supplemental_fill_stream.next() => {
                // Fills from a source independent of the primary market-event
                // stream (live trading's exchange push updates and
                // `reconcile()` poll). Backtest/paper's
                // `NeverFillSupplementalFillSource` never yields, so this arm
                // is unreachable for them.
                //
                // An empty `Vec<Fill>` must NOT record an equity point --
                // matching today's (pre-unification) `run_live_trading`,
                // which only recorded equity when its exchange-update/
                // reconcile arms actually produced at least one fill
                // (`if !new_fills.is_empty()` / `any_fills`). This arm was
                // unreachable until `apexes-live-trading`'s
                // `LiveSupplementalFillSource` became this trait's first
                // real implementation (Task 5), so this guard is a defensive
                // fix for a dormant bug rather than a change in observed
                // behavior for any existing caller: every
                // `SupplementalFillSource` implementation to date already
                // never yields an empty `Vec` in the first place (`stream`'s
                // own internal loop `continue`s on empty results instead of
                // yielding them) -- this is defense-in-depth against a
                // future implementation that doesn't uphold that contract.
                if fills.is_empty() {
                    continue;
                }
                let before = all_fills.len();
                let applied = apply_supplemental_fills(
                    diagnostics,
                    &mut ledger,
                    &mut overall_tracker,
                    &mut per_symbol_trackers,
                    &mut per_venue_trackers,
                    &mut risk_checker,
                    &mut all_fills,
                    &mut supplemental_seen,
                    &fills,
                    config.risk_free_return,
                );
                // A managed executor closes the submission gate until
                // this acknowledgement succeeds, or latches a fault.
                executor.acknowledge_supplemental_fills(&fills, applied).await;
                if let Some(log) = &mut order_log {
                    log.filled(&all_fills[before..]);
                }
                let equity = ledger.equity(&last_prices)?;
                let now = chrono::Utc::now();
                notifications.deliver(&mut strategy, now, FillSource::Supplemental, &all_fills[before..]);
                overall_tracker.record_equity_point(now, equity);
                if let Some(tracker) = &mut evaluation {
                    tracker.observe_fills(&all_fills);
                    let started = tracker.record_equity_at(
                        now,
                        equity,
                        ledger.positions().values().any(|p| !p.quantity.is_zero()),
                        holdings_priced(ledger.balances(), &last_prices, &instruments_by_symbol),
                        &last_prices,
                    );
                    if started {
                        tracker.set_baseline(starting_holdings(
                            &starting_balances,
                            &last_prices,
                            &instruments_by_symbol,
                        ));
                    }
                }
            }
        }
    }

    if let Some(queue) = &arrivals {
        queue.notify_expiry(&mut strategy);
    }

    Ok(BacktestReport {
        overall: overall_tracker.summary().into(),
        per_symbol: per_symbol_trackers
            .into_iter()
            .map(|(k, v)| (k, v.summary().into()))
            .collect(),
        per_venue: per_venue_trackers
            .into_iter()
            .map(|(k, v)| (k, v.summary().into()))
            .collect(),
        equity_curve: overall_tracker.equity_curve().to_vec(),
        trades: all_fills,
        spot_pnl: ledger.spot_pnl(),
        simulated_fees: None,
        simulated_execution: None,
        simulated_depth: None,
        event_latency: arrivals.map(|q| q.finish()),
        historical_dataset: None,
        replay_identity: None,
        execution_performance: None,
        drawdown_duration: None,
        benchmark: None,
        portfolio_benchmark: None,
        funding: funding_report,
    })
}

/// Reserve spend assets for Spot or linear perpetual intents in transit.
fn intent_capital_hold(
    intent: &apexes_strategy::OrderIntent,
    instruments: &HashMap<String, Instrument>,
    executor: &dyn OrderExecutor,
    now: chrono::DateTime<chrono::Utc>,
    queue: &crate::event_latency::ArrivalQueue,
    positions: &HashMap<String, Position>,
) -> Result<Option<crate::event_latency::CapitalHold>, String> {
    use apexes_core::{AssetClass, Side};
    let leveraged = intent.asset_class == AssetClass::Perpetual;
    if queue.has_closing_owner(&intent.symbol) {
        return Err("another simulated order owns closing quantity".into());
    }
    if !matches!(intent.asset_class, AssetClass::Spot | AssetClass::Perpetual) {
        if intent.leverage.is_some() || intent.margin_mode.is_some() {
            return Err("unsupported leveraged reservation asset class".into());
        }
        return Ok(None);
    }
    if leveraged {
        if intent.leverage.is_none_or(|l| l < Decimal::ONE) {
            return Err("perpetual reservation requires leverage >= 1".into());
        }
    } else if intent.leverage.is_some() || intent.margin_mode.is_some() {
        return Err("Spot reservation does not support leverage or margin mode".into());
    }
    if intent.execution_algo.is_some()
        && (intent.order_type != OrderType::Market
            || intent.routing != apexes_strategy::OrderRouting::Fixed)
    {
        return Err("simulated parent requires fixed-route market intent".into());
    }
    let instrument = instruments
        .get(&intent.symbol)
        .filter(|i| i.venue == intent.venue && i.asset_class == intent.asset_class)
        .ok_or("unknown reservation instrument")?;
    if leveraged && instrument.multiplier != Decimal::ONE {
        return Err("leveraged reservation requires unit contract multiplier".into());
    }
    use crate::event_latency::{CapitalHold, MarginAllocation, closing_quantity};
    let asset = |value: Option<&String>| {
        value
            .filter(|a| !a.is_empty())
            .cloned()
            .ok_or("unknown reservation spend asset")
    };
    if leveraged {
        let closing = if queue.has_symbol_owner(&intent.symbol) {
            Decimal::ZERO
        } else {
            closing_quantity(intent, positions)
        };
        let (unit_margin, unit_fee) = executor
            .simulated_margin_rates(intent, now)
            .map_err(|e| e.to_string())?;
        return CapitalHold::leveraged(
            asset(instrument.quote_currency.as_ref())?,
            intent.quantity,
            MarginAllocation {
                closing,
                unit_margin,
                unit_fee,
            },
        )
        .map(Some);
    }
    let (asset, amount) = match intent.side {
        Side::Buy => (
            asset(instrument.quote_currency.as_ref())?,
            executor
                .simulated_buy_budget(intent, now)
                .map_err(|e| e.to_string())?,
        ),
        Side::Sell => (asset(instrument.base_currency.as_ref())?, intent.quantity),
    };
    if amount <= Decimal::ZERO {
        return Err("nonpositive reservation".into());
    }
    Ok(Some(CapitalHold::plain(asset, amount)))
}

/// `intent` as the risk checks should see it: a priceless order (market) gets
/// the last trade price of its instrument so notional limits can apply. Used
/// for the check only; the order is submitted unchanged. With no price known
/// yet the intent is returned as is.
fn priced_for_risk_check(
    intent: &apexes_strategy::OrderIntent,
    instruments_by_symbol: &HashMap<String, Instrument>,
    last_prices: &HashMap<String, Decimal>,
) -> apexes_strategy::OrderIntent {
    if intent.price.is_some() {
        return intent.clone();
    }
    let reference = instruments_by_symbol
        .get(&intent.symbol)
        .and_then(|instrument| instrument.base_currency.as_ref())
        .and_then(|base| last_prices.get(base).copied());
    apexes_strategy::OrderIntent {
        price: reference,
        ..intent.clone()
    }
}

/// `intent` with its quantity (and limit price) made valid for its symbol's
/// exchange rules. A symbol with no rules comes back unchanged. `Err` is the
/// reason the order cannot be made valid, for the order log.
fn conform_to_rules(
    intent: &apexes_strategy::OrderIntent,
    rules: &HashMap<String, apexes_core::SymbolRules>,
    instruments_by_symbol: &HashMap<String, Instrument>,
    last_prices: &HashMap<String, Decimal>,
) -> Result<apexes_strategy::OrderIntent, apexes_core::RuleViolation> {
    let Some(rule) = rules.get(&intent.symbol) else {
        return Ok(intent.clone());
    };
    let reference = instruments_by_symbol
        .get(&intent.symbol)
        .and_then(|instrument| instrument.base_currency.as_ref())
        .and_then(|base| last_prices.get(base).copied());
    rule.conform(intent.side, intent.quantity, intent.price, reference)
        .map(|conformed| apexes_strategy::OrderIntent {
            quantity: conformed.quantity,
            price: conformed.price,
            ..intent.clone()
        })
}

/// The seeded prices keyed the way the loop keeps them (by base asset), for
/// the symbols the session actually trades. A non-positive price is ignored.
fn seeded_last_prices(
    initial_prices: &HashMap<String, Decimal>,
    instruments_by_symbol: &HashMap<String, Instrument>,
) -> HashMap<String, Decimal> {
    initial_prices
        .iter()
        .filter(|(_, price)| **price > Decimal::ZERO)
        .filter_map(|(symbol, price)| {
            let base = instruments_by_symbol.get(symbol)?.base_currency.clone()?;
            Some((base, *price))
        })
        .collect()
}

/// True when every non-zero balance is either a quote currency of a traded
/// instrument or has a last trade price. The ledger values an asset with no
/// price at 1.0, so until this holds `ledger.equity` is not a real number.
fn holdings_priced(
    balances: &HashMap<String, Decimal>,
    last_prices: &HashMap<String, Decimal>,
    instruments_by_symbol: &HashMap<String, Instrument>,
) -> bool {
    balances.iter().all(|(asset, balance)| {
        balance.is_zero()
            || last_prices.contains_key(asset)
            || instruments_by_symbol
                .values()
                .any(|i| i.quote_currency.as_deref() == Some(asset.as_str()))
    })
}

/// Records an order that was refused, with its reason. The refused intent never
/// became an `Order`, so it gets an id of its own to be listed by.
fn record_refusal(
    log: &mut crate::order_log::OrderLog,
    intent: &apexes_strategy::OrderIntent,
    at: chrono::DateTime<chrono::Utc>,
    reason: &str,
) {
    log.rejected(
        uuid::Uuid::new_v4().to_string(),
        at,
        &intent.symbol,
        &format!("{:?}", intent.side),
        &format!("{:?}", intent.order_type),
        intent.quantity,
        intent.price,
        reason.to_string(),
    );
}

fn join_violations(violations: &[apexes_risk::RiskViolation]) -> String {
    violations
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

/// The non-cash assets the account holds, with the price each has now.
/// A quote currency of a traded instrument is cash; so is anything without a
/// price (its value is unknown, so it cannot be attributed a PnL).
fn starting_holdings(
    balances: &HashMap<String, Decimal>,
    last_prices: &HashMap<String, Decimal>,
    instruments_by_symbol: &HashMap<String, Instrument>,
) -> Vec<crate::evaluation::BaselineHolding> {
    let is_cash = |asset: &str| {
        instruments_by_symbol
            .values()
            .any(|i| i.quote_currency.as_deref() == Some(asset))
    };
    let mut holdings: Vec<_> = balances
        .iter()
        .filter(|(asset, balance)| !balance.is_zero() && !is_cash(asset))
        .filter_map(|(asset, balance)| {
            last_prices
                .get(asset)
                .map(|price| crate::evaluation::BaselineHolding {
                    asset: asset.clone(),
                    quantity: *balance,
                    price: *price,
                })
        })
        .collect();
    holdings.sort_by(|a, b| a.asset.cmp(&b.asset));
    holdings
}

/// Current value, in quote currency, of the non-cash assets held.
fn holdings_value(
    balances: &HashMap<String, Decimal>,
    last_prices: &HashMap<String, Decimal>,
    instruments_by_symbol: &HashMap<String, Instrument>,
) -> Decimal {
    starting_holdings(balances, last_prices, instruments_by_symbol)
        .iter()
        .map(|h| h.quantity * h.price)
        .sum()
}

/// Gross exposure (sum of |position value| in quote currency) and the
/// largest |net position| in base units, for the evaluation view. Prices
/// use the same base-asset-keyed `last_prices` map equity uses, falling
/// back to a position's entry price before the first trade print.
fn exposure_inputs(
    positions: &HashMap<String, Position>,
    last_prices: &HashMap<String, Decimal>,
    instruments_by_symbol: &HashMap<String, Instrument>,
) -> (Decimal, Decimal) {
    let mut gross = Decimal::ZERO;
    let mut largest = Decimal::ZERO;
    for (_, position) in crate::stable_order::entries(positions) {
        let quantity = position.quantity.abs();
        if quantity.is_zero() {
            continue;
        }
        let price = instruments_by_symbol
            .get(&position.symbol)
            .and_then(|instrument| instrument.base_currency.as_ref())
            .and_then(|base| last_prices.get(base).copied())
            .unwrap_or(position.average_entry_price);
        gross += quantity * price;
        if quantity > largest {
            largest = quantity;
        }
    }
    (gross, largest)
}

/// Applies a batch of `Fill`s produced by the executor (or the EMS):
/// updates the ledger, records each fill into the
/// overall/per-symbol/per-venue `PerformanceTracker`s, and accumulates
/// every fill into `all_fills` (the `BacktestReport.trades` source) in
/// occurrence order.
#[allow(clippy::too_many_arguments)]
fn apply_fills(
    diagnostics: Diagnostics,
    source: ExitReason,
    fill_origins: Option<&mut Vec<ExitReason>>,
    ledger: &mut PortfolioLedger,
    overall_tracker: &mut PerformanceTracker,
    per_symbol_trackers: &mut HashMap<String, PerformanceTracker>,
    per_venue_trackers: &mut HashMap<String, PerformanceTracker>,
    all_fills: &mut Vec<Fill>,
    fills: &[Fill],
    risk_free_return: Decimal,
) -> Result<(), ApexesError> {
    for fill in fills {
        let result = ledger.apply_fill(fill);
        diagnostics.fill(fill, source.label(), result.as_ref().err());
        result?;
        overall_tracker.record_fill(fill);
        record_fill_in_split_trackers(
            per_symbol_trackers,
            per_venue_trackers,
            fill,
            risk_free_return,
        );
    }
    if let Some(origins) = fill_origins {
        origins.extend(std::iter::repeat_n(source, fills.len()));
    }
    all_fills.extend_from_slice(fills);
    Ok(())
}

/// Applies a batch of `Fill`s that arrived from a
/// [`SupplementalFillSource`] -- ported from `run_live_trading`'s
/// `apply_new_fills`. A rejected execution is never counted in the
/// local performance history. The caller acknowledges the batch outcome
/// to the executor so managed live sessions latch the discrepancy and
/// retain the durable history for restart recovery.
#[allow(clippy::too_many_arguments)]
fn apply_supplemental_fills(
    diagnostics: Diagnostics,
    ledger: &mut PortfolioLedger,
    overall_tracker: &mut PerformanceTracker,
    per_symbol_trackers: &mut HashMap<String, PerformanceTracker>,
    per_venue_trackers: &mut HashMap<String, PerformanceTracker>,
    risk_checker: &mut Option<Arc<CompositeRiskChecker>>,
    all_fills: &mut Vec<Fill>,
    seen: &mut HashSet<FillId>,
    new_fills: &[Fill],
    risk_free_return: Decimal,
) -> bool {
    let mut applied = true;
    for fill in new_fills {
        if seen.contains(&fill.id) {
            continue;
        }
        let result = ledger.apply_fill(fill);
        diagnostics.fill(fill, "supplemental", result.as_ref().err());
        if let Err(error) = result {
            tracing::warn!(reason = error_code(&error), order_id = ?fill.order_id, "failed to apply fill to ledger");
            applied = false;
            continue;
        }
        seen.insert(fill.id);
        all_fills.push(fill.clone());
        overall_tracker.record_fill(fill);
        record_fill_in_split_trackers(
            per_symbol_trackers,
            per_venue_trackers,
            fill,
            risk_free_return,
        );
        if let Some(checker) = risk_checker
            && let Err(violations) = checker.on_fill(fill, ledger.positions(), ledger.balances())
        {
            tracing::warn!(?violations, symbol = %fill.symbol, "in-trade risk monitor flagged a violation");
        }
    }
    applied
}

/// Records `fill` into its per-symbol and per-venue `PerformanceTracker`,
/// creating either on first use. Shared by [`apply_fills`] and
/// [`apply_supplemental_fills`] so the two paths cannot drift.
fn record_fill_in_split_trackers(
    per_symbol_trackers: &mut HashMap<String, PerformanceTracker>,
    per_venue_trackers: &mut HashMap<String, PerformanceTracker>,
    fill: &Fill,
    risk_free_return: Decimal,
) {
    per_symbol_trackers
        .entry(fill.symbol.clone())
        .or_insert_with(|| PerformanceTracker::new(risk_free_return))
        .record_fill(fill);
    per_venue_trackers
        .entry(format!("{:?}", fill.venue))
        .or_insert_with(|| PerformanceTracker::new(risk_free_return))
        .record_fill(fill);
}

/// Returns the most recent Binance-aligned 8-hour UTC boundary
/// (00:00/08:00/16:00) at or before `timestamp`.
fn last_funding_boundary(
    timestamp: chrono::DateTime<chrono::Utc>,
) -> chrono::DateTime<chrono::Utc> {
    use chrono::Timelike;
    let hour_bucket = (timestamp.hour() / 8) * 8;
    match timestamp.date_naive().and_hms_opt(hour_bucket, 0, 0) {
        Some(boundary) => boundary.and_utc(),
        // Unreachable for hour_bucket in {0, 8, 16}; fall back to the
        // timestamp itself (still monotone) rather than panic.
        None => timestamp,
    }
}

/// Refreshes `open_parent_orders` by querying `ems.parent_order_status`
/// for every `ParentOrderId` already tracked. `Ems` has no "list all
/// parent order IDs" method (deliberately out of scope, per the design
/// spec) -- new parent orders are added to the map at the point
/// `submit_algo_order` succeeds (see the call site above), not
/// discovered here.
/// Event-time simulation only: emergency reduction takes precedence over
/// all ordinary order reservations, including orders on other symbols
/// sharing the same wallet. Release only confirmed cancellations.
async fn preempt_simulated_orders(
    queue: &mut crate::event_latency::ArrivalQueue,
    executor: &dyn OrderExecutor,
    ems: &dyn Ems,
    parents: &mut HashMap<ParentOrderId, ParentOrderStatus>,
    strategy: &mut impl Strategy,
    at: chrono::DateTime<chrono::Utc>,
) {
    use apexes_strategy::{TerminalKind, TerminalOutcome};
    queue.report.emergency_events += 1;
    let cancelled_transit = queue.cancel_transit_for_emergency();
    let mut cancelled_ids = Vec::new();
    match executor.cancel_simulated_working() {
        Ok(ids) => {
            queue.report.emergency_resting_cancelled += ids.len() as u64;
            queue.reconcile_resting(&executor.open_orders().await);
            cancelled_ids = ids;
        }
        Err(error) => {
            queue.report.emergency_cancel_failures += 1;
            tracing::error!(%error, "simulated emergency working-order cancellation failed");
        }
    }
    let ids: Vec<_> = parents
        .iter()
        .filter(|(_, status)| **status == ParentOrderStatus::Scheduling)
        .map(|(id, _)| *id)
        .collect();
    for id in ids {
        match ems.cancel_parent_order(id).await {
            Ok(()) => {
                parents.insert(id, ParentOrderStatus::Cancelled);
                queue.report.emergency_parents_cancelled += 1;
            }
            Err(error) => {
                queue.report.emergency_cancel_failures += 1;
                tracing::error!(parent_id = %id.0, %error, "simulated emergency parent cancellation failed");
            }
        }
    }
    queue.reconcile_parents(parents);
    for intent in &cancelled_transit {
        if intent.execution_algo.is_none() {
            strategy.on_order_terminal(&TerminalOutcome {
                at,
                kind: TerminalKind::TransitCancelled { intent },
            });
        }
    }
    for order_id in cancelled_ids {
        strategy.on_order_terminal(&TerminalOutcome {
            at,
            kind: TerminalKind::Cancelled { order_id },
        });
    }
}

async fn refresh_open_parent_orders(
    ems: &dyn Ems,
    open_parent_orders: &mut HashMap<ParentOrderId, ParentOrderStatus>,
) {
    let mut ids: Vec<ParentOrderId> = open_parent_orders.keys().copied().collect();
    ids.sort_unstable_by_key(|id| id.0);
    for id in ids {
        match ems.parent_order_status(id).await {
            Ok(Some(status)) => {
                open_parent_orders.insert(id, status);
            }
            Ok(None) => {
                open_parent_orders.remove(&id);
            }
            Err(_) => {
                tracing::warn!(reason = "ems_error", parent_id = ?id, "failed to refresh parent order status");
            }
        }
    }
}

/// Type alias kept for readability at call sites that think of this
/// loop's output as "the unified loop's report" (the design spec's
/// `UnifiedLoopReport`). It IS `BacktestReport` -- the report shape both
/// re-pointed callers already returned, unchanged by this refactor.
pub type UnifiedLoopReport = BacktestReport;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ReplaySpeed;
    use crate::control::SessionCommand;
    use crate::cost_model::FlatFeeSchedule;
    use crate::matching_engine::MatchingEngine;
    use crate::order_executor_adapter::{
        BacktestLiquidationPolicy, MatchingEngineOrderExecutor, NeverFillSupplementalFillSource,
    };
    use apexes_core::{
        AssetClass, DepthUpdate, MarginMode, MarketType, PriceLevel, Side, TradeEvent,
    };
    use apexes_strategy::{MarginTier, MarginTierTable, OrderIntent, OrderRouting};
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal_macros::dec;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    fn spot_btcusdt() -> Instrument {
        Instrument {
            asset_class: AssetClass::Spot,
            ..btcusdt_perpetual_instrument()
        }
    }

    fn live_fill(side: Side, quantity: Decimal, fee: Decimal, fee_currency: &str) -> Fill {
        Fill {
            id: apexes_core::FillId(uuid::Uuid::new_v4()),
            order_id: apexes_core::OrderId(uuid::Uuid::new_v4()),
            symbol: "BTCUSDT".to_string(),
            venue: apexes_core::Venue::Binance,
            side,
            price: dec!(60000),
            quantity,
            fee,
            fee_currency: Some(fee_currency.to_string()),
            filled_at: Utc::now(),
            leverage: None,
            margin_mode: None,
        }
    }

    /// Applies exchange-reported fills the way the live loop does and returns
    /// the ledger and the overall tracker.
    fn apply_live_fills(
        balances: &[(&str, Decimal)],
        fills: Vec<Fill>,
    ) -> (PortfolioLedger, Vec<Fill>) {
        let mut ledger = PortfolioLedger::new(
            HashMap::from([("BTCUSDT".to_string(), spot_btcusdt())]),
            balances.iter().map(|(a, v)| (a.to_string(), *v)).collect(),
        );
        let mut tracker = PerformanceTracker::new(Decimal::ZERO);
        let mut all_fills = Vec::new();
        apply_supplemental_fills(
            Diagnostics::new(),
            &mut ledger,
            &mut tracker,
            &mut HashMap::new(),
            &mut HashMap::new(),
            &mut None,
            &mut all_fills,
            &mut HashSet::new(),
            &fills,
            Decimal::ZERO,
        );
        (ledger, all_fills)
    }

    #[test]
    fn supplemental_duplicate_and_refused_execution_are_not_counted_twice_or_as_applied() {
        let accepted = live_fill(Side::Buy, dec!(0.5), Decimal::ZERO, "USDT");
        let refused = live_fill(Side::Buy, dec!(10), Decimal::ZERO, "USDT");
        let (ledger, fills) = apply_live_fills(
            &[("USDT", dec!(100000))],
            vec![accepted.clone(), accepted, refused],
        );
        assert_eq!(fills.len(), 1);
        assert_eq!(ledger.positions()["BTCUSDT"].quantity, dec!(0.5));
        assert_eq!(ledger.balances()["USDT"], dec!(70000));
    }

    #[test]
    fn supplemental_notifications_only_deliver_applied_fills_and_suppress_repeats() {
        use apexes_strategy::{FillOutcome, FillSource};
        #[derive(Default)]
        struct Probe(Vec<(FillSource, Fill)>);
        impl Strategy for Probe {
            fn on_market_event(
                &mut self,
                _: &MarketEvent,
                _: &StrategyContext,
            ) -> Vec<OrderIntent> {
                vec![]
            }
            fn on_fill(&mut self, outcome: &FillOutcome<'_>) {
                self.0.push((outcome.source, outcome.fill.clone()));
            }
        }
        let accepted = live_fill(Side::Buy, dec!(0.5), Decimal::ZERO, "USDT");
        let refused = live_fill(Side::Buy, dec!(10), Decimal::ZERO, "USDT");
        let (_, applied) = apply_live_fills(
            &[("USDT", dec!(100000))],
            vec![accepted.clone(), accepted.clone(), refused],
        );
        assert_eq!(applied, vec![accepted.clone()]);
        let mut notifications = crate::fill_notifications::FillNotifications::default();
        let mut probe = Probe::default();
        notifications.deliver(&mut probe, Utc::now(), FillSource::Supplemental, &applied);
        // Re-delivery through another path must not notify twice.
        notifications.deliver(&mut probe, Utc::now(), FillSource::Direct, &applied);
        assert_eq!(probe.0, vec![(FillSource::Supplemental, accepted)]);
    }

    #[test]
    fn supplemental_fill_diagnostics_distinguish_applied_and_failed_ledger_updates() {
        let applied = live_fill(Side::Buy, dec!(0.5), Decimal::ZERO, "USDT");
        let refused = live_fill(Side::Buy, dec!(10), Decimal::ZERO, "USDT");
        let applied_id = applied.id.0.to_string();
        let refused_id = refused.id.0.to_string();
        let records = crate::diagnostics::tests::capture(|| {
            let (ledger, _) = apply_live_fills(&[("USDT", dec!(100000))], vec![applied, refused]);
            assert_eq!(ledger.positions()["BTCUSDT"].quantity, dec!(0.5));
        });
        let fills: Vec<_> = records
            .iter()
            .filter(|r| r["target"] == "apexes::decision" && r["fields"]["stage"] == "fill")
            .map(|r| &r["fields"])
            .collect();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0]["fill_id"], applied_id);
        assert_eq!(fills[0]["ledger_result"], "applied");
        assert_eq!(fills[1]["fill_id"], refused_id);
        assert_eq!(fills[1]["ledger_result"], "failed");
        assert_eq!(fills[1]["reason"], "insufficient_balance");
        assert!(fills.iter().all(|f| f["source"] == "supplemental"));
    }

    /// The reported risk: a mainnet account that pays fees in BNB. The ledger
    /// used to refuse the fill ("required 0.0004 BNB, available 0"), and this
    /// loop logged and skipped it, so the session's position, balances and PnL
    /// stopped matching the exchange.
    #[test]
    fn a_fill_whose_fee_is_paid_in_bnb_still_reaches_the_ledger_and_the_fee_total() {
        let (ledger, fills) = apply_live_fills(
            &[("USDT", dec!(100000))],
            vec![live_fill(Side::Buy, dec!(0.5), dec!(0.0004), "BNB")],
        );
        assert_eq!(fills.len(), 1, "the fill was not skipped");
        assert_eq!(ledger.positions()["BTCUSDT"].quantity, dec!(0.5));
        assert_eq!(ledger.balances()["USDT"], dec!(70000));
        // The evaluation's fee total reads the same fill list, so the BNB
        // fee is reported in its own currency.
        let mut evaluation =
            crate::evaluation::EvaluationTracker::new(crate::evaluation::LimitSpec {
                max_position_per_symbol: None,
                max_daily_fills: None,
            });
        evaluation.observe_fills(&fills);
        let summary = evaluation.summary(dec!(0), dec!(0), dec!(0), dec!(0));
        assert_eq!(summary.fees_paid["BNB"], dec!(0.0004));
    }

    /// Binance's default fee is a slice of what was just bought.
    #[test]
    fn a_fill_whose_fee_is_taken_from_the_asset_bought_reaches_the_ledger() {
        let (ledger, fills) = apply_live_fills(
            &[("USDT", dec!(100000))],
            vec![live_fill(Side::Buy, dec!(0.5), dec!(0.0005), "BTC")],
        );
        assert_eq!(fills.len(), 1);
        assert_eq!(ledger.positions()["BTCUSDT"].quantity, dec!(0.4995));
    }

    /// The safety net stays: a fill the ledger truly cannot afford is still
    /// skipped (logged), and the ones before and after it are unaffected.
    #[test]
    fn a_genuinely_unaffordable_fill_is_still_skipped_without_blocking_the_rest() {
        let (ledger, fills) = apply_live_fills(
            &[("USDT", dec!(100000))],
            vec![
                live_fill(Side::Buy, dec!(0.5), dec!(0), "BTC"),
                live_fill(Side::Sell, dec!(5), dec!(0), "USDT"), // sells 5 BTC, holds 0.5
                live_fill(Side::Buy, dec!(0.1), dec!(0), "BTC"),
            ],
        );
        // The durable exchange journal retains all three, but the local
        // applied-fill history must contain only the two accepted fills.
        assert_eq!(fills.len(), 2);
        assert!(fills.iter().all(|fill| fill.side == Side::Buy));
        assert_eq!(ledger.positions()["BTCUSDT"].quantity, dec!(0.6));
        assert_eq!(ledger.balances()["BTC"], dec!(0.6));
        assert_eq!(ledger.balances()["USDT"], dec!(100000) - dec!(36000));
    }

    fn btcusdt_perpetual_instrument() -> Instrument {
        Instrument {
            symbol: "BTCUSDT".to_string(),
            market_type: MarketType::Crypto,
            venue: apexes_core::Venue::Binance,
            asset_class: AssetClass::Perpetual,
            base_currency: Some("BTC".to_string()),
            quote_currency: Some("USDT".to_string()),
            tick_size: dec!(0.01),
            lot_size: dec!(0.001),
            multiplier: dec!(1),
        }
    }

    fn zero_cost_model(funding_rate_bps: Option<Decimal>) -> CostModel {
        CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: Decimal::ZERO,
                taker_bps: Decimal::ZERO,
            }),
            funding_rate_bps,
        }
    }

    fn depth_snapshot(timestamp: chrono::DateTime<Utc>) -> MarketEvent {
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: apexes_core::Venue::Binance,
            bids: vec![PriceLevel {
                price: dec!(99),
                quantity: dec!(10),
            }],
            asks: vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(10),
            }],
            sequence: None,
            source_range: None,
            is_snapshot: true,
            timestamp,
        })
    }

    fn trade_event(price: Decimal, timestamp: chrono::DateTime<Utc>) -> MarketEvent {
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: apexes_core::Venue::Binance,
            price,
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp,
        })
    }

    fn mark_price_event(price: Decimal, timestamp: chrono::DateTime<Utc>) -> MarketEvent {
        MarketEvent::MarkPrice(apexes_core::MarkPriceUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: apexes_core::Venue::Binance,
            mark_price: price,
            timestamp,
        })
    }

    fn single_tier_table() -> MarginTierTable {
        MarginTierTable {
            tiers: vec![MarginTier {
                notional_floor: dec!(0),
                max_leverage: dec!(20),
                maintenance_margin_rate: dec!(0.005),
                maintenance_amount: dec!(0),
            }],
        }
    }

    /// Opens a 10x leveraged long on the first Trade event it sees, then
    /// goes quiet. Records the margin-status entry it observes for
    /// BTCUSDT on every subsequent tick (to verify `StrategyContext.
    /// margin_status` is really wired).
    struct LeveragedBuyOnce {
        bought: bool,
        observed_ratios: Arc<Mutex<Vec<Option<Decimal>>>>,
    }

    impl LeveragedBuyOnce {
        fn new(observed_ratios: Arc<Mutex<Vec<Option<Decimal>>>>) -> Self {
            Self {
                bought: false,
                observed_ratios,
            }
        }
    }

    impl Strategy for LeveragedBuyOnce {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            if self.bought {
                self.observed_ratios
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(
                        ctx.margin_status
                            .get("BTCUSDT")
                            .and_then(|status| status.margin_ratio),
                    );
                return Vec::new();
            }
            if matches!(event, MarketEvent::Trade(_)) {
                self.bought = true;
                return vec![OrderIntent {
                    symbol: "BTCUSDT".to_string(),
                    venue: apexes_core::Venue::Binance,
                    asset_class: AssetClass::Perpetual,
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    quantity: dec!(1),
                    price: None,
                    stop_price: None,
                    execution_algo: None,
                    routing: OrderRouting::Fixed,
                    leverage: Some(dec!(10)),
                    margin_mode: Some(MarginMode::Isolated),
                }];
            }
            Vec::new()
        }
    }

    /// Opens a 1-BTC spot long on the first Trade event it sees, then
    /// goes quiet -- the minimal strategy the market-condition wiring
    /// tests drive (its position is what the monitor de-risks).
    struct BuyOnceStrategy {
        bought: bool,
    }

    impl Strategy for BuyOnceStrategy {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            if !self.bought && matches!(event, MarketEvent::Trade(_)) {
                self.bought = true;
                return vec![OrderIntent {
                    symbol: "BTCUSDT".to_string(),
                    venue: apexes_core::Venue::Binance,
                    asset_class: AssetClass::Perpetual,
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    quantity: dec!(1),
                    price: None,
                    stop_price: None,
                    execution_algo: None,
                    routing: OrderRouting::Fixed,
                    leverage: None,
                    margin_mode: None,
                }];
            }
            Vec::new()
        }
    }

    fn unified_loop_config(
        initial_usdt: Decimal,
        funding_rate_bps: Option<Decimal>,
        margin_tiers: HashMap<String, MarginTierTable>,
    ) -> UnifiedLoopConfig {
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), initial_usdt);
        UnifiedLoopConfig {
            instruments: vec![btcusdt_perpetual_instrument()],
            initial_balances,
            initial_positions: HashMap::new(),
            funding_rate_bps,
            risk_free_return: Decimal::ZERO,
            risk_checker: None,
            margin_tiers,
            market_condition_config: None,
            control: None,
            initial_prices: HashMap::new(),
            symbol_rules: HashMap::new(),
        }
    }

    /// Builds the backtest-flavored executor/policy/EMS trio around one
    /// shared `MatchingEngine` and runs the unified loop -- the same
    /// wiring `run_backtest` performs, in miniature. `cost_model` must
    /// carry the same `funding_rate_bps` as `config`, exactly as
    /// `run_backtest` guarantees for its own callers.
    async fn run(
        config: UnifiedLoopConfig,
        cost_model: CostModel,
        events: Vec<MarketEvent>,
        strategy: impl Strategy,
    ) -> BacktestReport {
        run_with_clock(
            config,
            cost_model,
            events,
            strategy,
            BacktestClock::new(ReplaySpeed::AsFastAsPossible),
        )
        .await
    }

    async fn run_with_clock(
        config: UnifiedLoopConfig,
        cost_model: CostModel,
        events: Vec<MarketEvent>,
        strategy: impl Strategy,
        clock: BacktestClock,
    ) -> BacktestReport {
        let matching_engine = Arc::new(Mutex::new(MatchingEngine::new(cost_model, dec!(0.5))));
        let executor: Arc<dyn OrderExecutor> = Arc::new(MatchingEngineOrderExecutor::new(
            Arc::clone(&matching_engine),
        ));
        let child_order_sink: Arc<dyn apexes_ems::ChildOrderSink> = Arc::new(
            crate::ems_adapter::MatchingEngineChildOrderSink::new(Arc::clone(&matching_engine)),
        );
        let ems: Arc<dyn Ems> = Arc::new(apexes_ems::InMemoryEms::new(
            child_order_sink,
            Arc::new(apexes_strategy::PriceAndFeeScorer),
            HashMap::new(),
            None,
        ));

        run_unified_event_loop(
            config,
            futures::stream::iter(events),
            clock,
            executor,
            Arc::new(BacktestLiquidationPolicy),
            NeverFillSupplementalFillSource,
            ems,
            strategy,
            None,
            None,
        )
        .await
        .expect("event loop must not error")
    }

    #[tokio::test]
    async fn strategy_market_context_sees_session_rules_and_post_fill_ledger() {
        type ObservedBalances = Vec<(Option<Decimal>, Option<Decimal>)>;
        struct ObserveContext(Arc<Mutex<ObservedBalances>>);
        impl Strategy for ObserveContext {
            fn on_market_event(
                &mut self,
                event: &MarketEvent,
                ctx: &StrategyContext,
            ) -> Vec<OrderIntent> {
                let market = ctx.market.unwrap();
                assert!(market.matches(event));
                assert_eq!(market.instrument().lot_size, dec!(0.001));
                let rule = market.rules().unwrap();
                assert_eq!(rule.quantity_step, dec!(0.01));
                let mut seen = self.0.lock().unwrap();
                seen.push((ctx.ledger_balance("BTC"), ctx.ledger_balance("USDT")));
                if seen.len() != 1 {
                    return vec![];
                }
                vec![OrderIntent {
                    symbol: "BTCUSDT".into(),
                    venue: apexes_core::Venue::Binance,
                    asset_class: AssetClass::Spot,
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    quantity: dec!(0.027),
                    price: None,
                    stop_price: None,
                    execution_algo: None,
                    routing: OrderRouting::Fixed,
                    leverage: None,
                    margin_mode: None,
                }]
            }
        }
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mut config = unified_loop_config(dec!(1000), None, HashMap::new());
        config.instruments[0].asset_class = AssetClass::Spot;
        config.symbol_rules.insert(
            "BTCUSDT".into(),
            apexes_core::SymbolRules {
                quantity_step: dec!(0.01),
                min_quantity: dec!(0.01),
                max_quantity: dec!(10),
                price_tick: dec!(0.05),
                min_notional: Decimal::ZERO,
            },
        );
        let observed = Arc::new(Mutex::new(Vec::new()));
        let report = run(
            config,
            zero_cost_model(None),
            vec![
                depth_snapshot(t0),
                trade_event(dec!(100), t0 + Duration::seconds(1)),
            ],
            ObserveContext(observed.clone()),
        )
        .await;
        assert_eq!(report.trades.len(), 1);
        assert_eq!(report.trades[0].quantity, dec!(0.02));
        assert_eq!(
            *observed.lock().unwrap(),
            vec![
                (None, Some(dec!(1000))),
                (Some(dec!(0.02)), Some(dec!(998)))
            ]
        );
    }

    #[tokio::test]
    async fn delayed_closed_bar_uses_consumer_time_for_gate_and_order_submission() {
        use apexes_strategy::event_time::{TimeObservation, TimeState};
        struct ClosedBarBuyer(TimeState);
        impl Strategy for ClosedBarBuyer {
            fn on_market_event(
                &mut self,
                event: &MarketEvent,
                ctx: &StrategyContext,
            ) -> Vec<OrderIntent> {
                let observation = TimeObservation::new(event, ctx.now).unwrap();
                if !self.0.accept_closed_bar(observation).unwrap() {
                    return vec![];
                }
                vec![OrderIntent {
                    symbol: "BTCUSDT".into(),
                    venue: apexes_core::Venue::Binance,
                    asset_class: AssetClass::Perpetual,
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    quantity: dec!(1),
                    price: None,
                    stop_price: None,
                    execution_algo: None,
                    routing: OrderRouting::Fixed,
                    leverage: Some(dec!(10)),
                    margin_mode: Some(MarginMode::Isolated),
                }]
            }
        }
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let close = t0 + Duration::seconds(60);
        let delivered = close + Duration::seconds(5);
        let bar = MarketEvent::Bar(apexes_core::Bar {
            symbol: "BTCUSDT".into(),
            venue: apexes_core::Venue::Binance,
            open: dec!(100),
            high: dec!(100),
            low: dec!(100),
            close: dec!(100),
            volume: dec!(1),
            interval: apexes_core::BarInterval::OneMinute,
            open_time: t0,
            close_time: close,
        });
        // A newer trade arrives before the delayed finalized bar, as in
        // the actual trade task's trade-then-closed-bars delivery order.
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), delivered - Duration::milliseconds(1)),
            bar.clone(),
            bar,
        ];
        let mut samples = [
            t0,
            delivered,
            delivered,
            delivered + Duration::milliseconds(1),
        ]
        .into_iter();
        let clock = BacktestClock::with_observation_source(move || samples.next().unwrap());
        let report = run_with_clock(
            unified_loop_config(dec!(1000), None, HashMap::new()),
            zero_cost_model(None),
            events,
            ClosedBarBuyer(TimeState::default()),
            clock,
        )
        .await;
        assert_eq!(report.trades.len(), 1);
        assert_eq!(report.trades[0].filled_at, delivered);
        assert!(report.equity_curve.windows(2).all(|w| w[0].0 <= w[1].0));
    }

    /// End-to-end auto-liquidation: a 10x long opened at 100 collapses
    /// to a mark price of 89, breaching the maintenance-margin ratio --
    /// the event loop must force-close the position through the
    /// `MatchingEngine` (selling into the resting 99 bid) without any
    /// strategy action.
    #[tokio::test]
    async fn breached_margin_ratio_force_liquidates_position_through_matching_engine() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), t0 + Duration::seconds(1)),
            mark_price_event(dec!(100), t0 + Duration::seconds(2)),
            mark_price_event(dec!(89), t0 + Duration::seconds(3)),
            // One more event so the loop processes a tick AFTER the
            // liquidation (the position must stay closed -- no second
            // liquidation order).
            mark_price_event(dec!(88), t0 + Duration::seconds(4)),
        ];

        let mut margin_tiers = HashMap::new();
        margin_tiers.insert("BTCUSDT".to_string(), single_tier_table());

        let report = run(
            unified_loop_config(dec!(1000), None, margin_tiers),
            zero_cost_model(None),
            events,
            LeveragedBuyOnce::new(Arc::new(Mutex::new(Vec::new()))),
        )
        .await;

        // Exactly two fills: the strategy's leveraged buy at 100 (walked
        // the 100 ask), then the liquidation sell at 99 (walked the 99
        // bid) -- a `leverage: Some` closing fill, or it would never
        // have closed the leveraged position.
        assert_eq!(report.trades.len(), 2, "expected open + liquidation fills");
        let open_fill = &report.trades[0];
        assert_eq!(open_fill.side, Side::Buy);
        assert_eq!(open_fill.price, dec!(100));
        assert_eq!(open_fill.leverage, Some(dec!(10)));
        let liquidation_fill = &report.trades[1];
        assert_eq!(liquidation_fill.side, Side::Sell);
        assert_eq!(liquidation_fill.price, dec!(99));
        assert_eq!(liquidation_fill.quantity, dec!(1));
        assert_eq!(
            liquidation_fill.leverage,
            Some(dec!(10)),
            "liquidation fill must carry the position's leverage to take the close_leveraged path"
        );
        assert_eq!(liquidation_fill.margin_mode, Some(MarginMode::Isolated));

        // Accounting: 1000 USDT - 10 reserved margin - 1 realized loss
        // + 10 released margin = 999 USDT, no residual BTC balance.
        let final_equity = report
            .equity_curve
            .last()
            .map(|(_, equity)| *equity)
            .expect("equity curve must be non-empty");
        assert_eq!(final_equity, dec!(999));
    }

    /// The margin state computed by the ledger must be visible to the
    /// strategy through `StrategyContext.margin_status` -- and must
    /// disappear once the position has been liquidated.
    #[tokio::test]
    async fn strategy_context_exposes_live_margin_status() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), t0 + Duration::seconds(1)),
            mark_price_event(dec!(100), t0 + Duration::seconds(2)),
            mark_price_event(dec!(100), t0 + Duration::seconds(3)),
        ];

        let mut margin_tiers = HashMap::new();
        margin_tiers.insert("BTCUSDT".to_string(), single_tier_table());

        let observed_ratios = Arc::new(Mutex::new(Vec::new()));
        let report = run(
            unified_loop_config(dec!(1000), None, margin_tiers),
            zero_cost_model(None),
            events,
            LeveragedBuyOnce::new(Arc::clone(&observed_ratios)),
        )
        .await;

        assert_eq!(
            report.trades.len(),
            1,
            "no liquidation at an unchanged mark"
        );
        // Observed on the two post-open ticks: mark 100, entry 100,
        // allocated margin 10 -> effective margin 10, maintenance
        // 100 * 0.005 = 0.5 -> ratio 20.
        let observed = observed_ratios.lock().unwrap_or_else(|p| p.into_inner());
        assert_eq!(observed.len(), 2, "two ticks after the opening trade");
        assert_eq!(observed[0], Some(dec!(20)));
        assert_eq!(observed[1], Some(dec!(20)));
    }

    /// Funding settles exactly once per crossed 8-hour UTC boundary:
    /// events at 07:59 and 08:01 cross the 08:00 boundary (one
    /// settlement of notional * 10bps = 0.1 USDT); a further event at
    /// 08:02 must NOT settle again.
    #[tokio::test]
    async fn funding_settles_once_per_crossed_eight_hour_boundary() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 7, 59, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), t0 + Duration::seconds(30)),
            // Mark price known before the boundary so notional is
            // computed against it (100 either way).
            mark_price_event(dec!(100), t0 + Duration::seconds(40)),
            // 08:01 -- first tick past the 08:00 boundary: settle.
            mark_price_event(dec!(100), t0 + Duration::minutes(2)),
            // 08:02 -- same 8-hour bucket: must NOT settle again.
            mark_price_event(dec!(100), t0 + Duration::minutes(3)),
        ];

        let report = run(
            unified_loop_config(dec!(1000), Some(dec!(10)), HashMap::new()),
            zero_cost_model(Some(dec!(10))),
            events,
            LeveragedBuyOnce::new(Arc::new(Mutex::new(Vec::new()))),
        )
        .await;

        assert_eq!(report.trades.len(), 1, "no margin tiers -> no liquidation");
        // 1000 - 10 margin reserved - 0.1 funding (notional 100 * 10bps)
        // = 989.9 exactly. A second settlement would give 989.8.
        let final_equity = report
            .equity_curve
            .last()
            .map(|(_, equity)| *equity)
            .expect("equity curve must be non-empty");
        assert_eq!(final_equity, dec!(989.9));
    }

    /// A spot-only backtest with no margin tiers configured must behave
    /// exactly as before this task: no liquidation, no funding, no
    /// margin status -- the empty-`margin_tiers` default is a silent
    /// no-op.
    #[tokio::test]
    async fn empty_margin_tiers_is_a_silent_noop_for_spot_backtests() {
        struct BuySpotOnce {
            bought: bool,
        }
        impl Strategy for BuySpotOnce {
            fn on_market_event(
                &mut self,
                event: &MarketEvent,
                _ctx: &StrategyContext,
            ) -> Vec<OrderIntent> {
                if !self.bought && matches!(event, MarketEvent::Trade(_)) {
                    self.bought = true;
                    return vec![OrderIntent {
                        symbol: "BTCUSDT".to_string(),
                        venue: apexes_core::Venue::Binance,
                        asset_class: AssetClass::Perpetual,
                        side: Side::Buy,
                        order_type: OrderType::Market,
                        quantity: dec!(1),
                        price: None,
                        stop_price: None,
                        execution_algo: None,
                        routing: OrderRouting::Fixed,
                        leverage: None,
                        margin_mode: None,
                    }];
                }
                Vec::new()
            }
        }

        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), t0 + Duration::seconds(1)),
            mark_price_event(dec!(50), t0 + Duration::seconds(2)),
        ];

        let report = run(
            unified_loop_config(dec!(1000), Some(dec!(10)), HashMap::new()),
            zero_cost_model(Some(dec!(10))),
            events,
            BuySpotOnce { bought: false },
        )
        .await;

        // Spot fill only; the mark-price collapse liquidates nothing.
        assert_eq!(report.trades.len(), 1);
        assert_eq!(report.trades[0].side, Side::Buy);
    }

    /// Regression guard: `market_condition_config: None` must run
    /// byte-for-byte identically to the pre-this-plan behavior. Runs a
    /// simple buy-once scenario (the same shape this file's other
    /// pre-existing tests use) with `market_condition_config`
    /// explicitly set to `None`, and asserts exactly the same fill
    /// count a pre-this-plan run would have produced -- if the unified
    /// loop ever starts unconditionally touching ledger state when
    /// `market_condition_monitor` is `None`, this test's exact-fill
    /// assertion will catch it.
    #[tokio::test]
    async fn market_condition_config_none_does_not_change_existing_behavior() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), t0 + Duration::seconds(1)),
            trade_event(dec!(130), t0 + Duration::seconds(2)),
        ];

        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));

        let config = UnifiedLoopConfig {
            instruments: vec![btcusdt_perpetual_instrument()],
            initial_balances,
            initial_positions: HashMap::new(),
            funding_rate_bps: None,
            risk_free_return: Decimal::ZERO,
            risk_checker: None,
            margin_tiers: HashMap::new(),
            market_condition_config: None,
            control: None,
            initial_prices: HashMap::new(),
            symbol_rules: HashMap::new(),
        };

        let report = run(
            config,
            zero_cost_model(None),
            events,
            BuyOnceStrategy { bought: false },
        )
        .await;

        // Exactly one fill (the strategy's opening buy) -- no
        // de-risking activity, since market_condition_config is None
        // (the 30% jump would trip the monitor if it were wired).
        assert_eq!(report.trades.len(), 1);
        assert_eq!(report.trades[0].side, Side::Buy);
    }

    /// With `market_condition_config: Some(...)`, a price jump past
    /// `jump_threshold_pct` must drive an automatic partial-close
    /// market order through the `MatchingEngine` -- the strategy's
    /// opening buy, then a de-risking sell on the jumped tick.
    #[tokio::test]
    async fn extreme_price_jump_triggers_a_partial_close_through_the_matching_engine() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), t0 + Duration::seconds(1)),
            // A 30% jump -- well past the 10% threshold.
            trade_event(dec!(130), t0 + Duration::seconds(2)),
        ];

        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));

        let config = UnifiedLoopConfig {
            instruments: vec![btcusdt_perpetual_instrument()],
            initial_balances,
            initial_positions: HashMap::new(),
            funding_rate_bps: None,
            risk_free_return: Decimal::ZERO,
            risk_checker: None,
            margin_tiers: HashMap::new(),
            market_condition_config: Some(apexes_risk::MarketConditionConfig {
                volatility_threshold: dec!(1000000), // effectively disabled, isolate the jump signal
                jump_threshold_pct: dec!(0.10),
                sensitivity: dec!(1),
                volatility_window: 20,
                price_source: apexes_risk::PriceSource::Trade,
                account_equity_volatility_threshold: None,
                include_spot: true, // this test uses a SPOT buy-once strategy
            }),
            control: None,
            initial_prices: HashMap::new(),
            symbol_rules: HashMap::new(),
        };

        let report = run(
            config,
            zero_cost_model(None),
            events,
            BuyOnceStrategy { bought: false },
        )
        .await;

        // Expect 2 fills: the strategy's opening buy, then a de-risking
        // sell triggered by the 30% jump (fraction 1.0, full close).
        assert_eq!(report.trades.len(), 2);
        assert_eq!(report.trades[0].side, Side::Buy);
        assert_eq!(report.trades[1].side, Side::Sell);
    }

    /// An executor the exchange always rejects (every order fails, as a real
    /// exchange does for a notional below its minimum).
    struct RejectingExecutor {
        attempts: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl OrderExecutor for RejectingExecutor {
        async fn submit(
            &self,
            _intent: &OrderIntent,
            _positions: &HashMap<String, Position>,
            _balances: &HashMap<String, Decimal>,
            _now: chrono::DateTime<Utc>,
        ) -> Result<(apexes_core::Order, Vec<Fill>), ApexesError> {
            self.attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(ApexesError::OrderRejected {
                reason: "Filter failure: NOTIONAL".to_string(),
            })
        }

        fn on_market_event(&self, _event: &MarketEvent) -> Vec<(apexes_core::OrderId, Vec<Fill>)> {
            Vec::new()
        }

        async fn open_orders(&self) -> HashMap<apexes_core::OrderId, apexes_core::Order> {
            HashMap::new()
        }
    }

    /// Regression: a strategy whose orders the exchange keeps rejecting must
    /// not use up the daily trade allowance. Found on the real testnet: a
    /// strategy sized below the exchange minimum was rejected 25 times and the
    /// cap (50) was then hit, because the check counted every ATTEMPT, twice.
    #[tokio::test]
    async fn orders_the_exchange_rejects_do_not_use_up_the_daily_trade_cap() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events: Vec<MarketEvent> = (0..40)
            .map(|i| trade_event(dec!(100), t0 + Duration::seconds(i)))
            .collect();
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let executor: Arc<dyn OrderExecutor> = Arc::new(RejectingExecutor {
            attempts: Arc::clone(&attempts),
        });
        let checker = Arc::new(
            apexes_risk::CompositeRiskChecker::new().with_pre_trade_check(Box::new(
                apexes_risk::LimitChecker::new(apexes_risk::RiskLimits {
                    max_daily_trade_count: Some(5),
                    ..Default::default()
                }),
            )),
        );
        let config = UnifiedLoopConfig {
            risk_checker: Some(checker),
            ..unified_loop_config(dec!(10000), None, HashMap::new())
        };
        let matching_engine = Arc::new(Mutex::new(MatchingEngine::new(
            zero_cost_model(None),
            dec!(0.5),
        )));
        let child_order_sink: Arc<dyn apexes_ems::ChildOrderSink> = Arc::new(
            crate::ems_adapter::MatchingEngineChildOrderSink::new(Arc::clone(&matching_engine)),
        );
        let ems: Arc<dyn Ems> = Arc::new(apexes_ems::InMemoryEms::new(
            child_order_sink,
            Arc::new(apexes_strategy::PriceAndFeeScorer),
            HashMap::new(),
            None,
        ));
        run_unified_event_loop(
            config,
            futures::stream::iter(events),
            BacktestClock::new(ReplaySpeed::AsFastAsPossible),
            executor,
            Arc::new(BacktestLiquidationPolicy),
            NeverFillSupplementalFillSource,
            ems,
            AlwaysBuyStrategy,
            None,
            None,
        )
        .await
        .expect("loop must not error");

        // 40 events, a cap of 5: with the old counting the 6th order onward was
        // blocked by the RISK CHECK and never reached the executor.
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            40,
            "every order must still reach the exchange: rejected orders use no allowance"
        );
    }

    /// A market order must be held to `max_order_notional` like any other:
    /// it has no price of its own, so the loop values it at the last trade.
    /// Before this, the limit silently never applied to market orders, which is
    /// all the demo strategy sends.
    #[tokio::test]
    async fn the_notional_limit_applies_to_market_orders_via_the_last_trade_price() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        // AlwaysBuyStrategy buys 0.01 BTC at market; at 100 that is 1 USDT.
        let run_with_limit = |max_notional: Decimal, price: Decimal| async move {
            let events = vec![
                depth_snapshot(t0),
                trade_event(price, t0 + Duration::seconds(1)),
                trade_event(price, t0 + Duration::seconds(2)),
            ];
            let checker = Arc::new(
                apexes_risk::CompositeRiskChecker::new().with_pre_trade_check(Box::new(
                    apexes_risk::LimitChecker::new(apexes_risk::RiskLimits {
                        max_order_notional: Some(max_notional),
                        ..Default::default()
                    }),
                )),
            );
            let config = UnifiedLoopConfig {
                risk_checker: Some(checker),
                ..unified_loop_config(dec!(100000), None, HashMap::new())
            };
            run(config, zero_cost_model(None), events, AlwaysBuyStrategy)
                .await
                .trades
                .len()
        };
        // The strategy also fires on the opening depth event, before any trade
        // has printed: that order has no reference price yet, so the limit
        // cannot apply to it (documented in `priced_for_risk_check`). Every
        // order AFTER the first trade is priced and must be held to the cap.
        let within = run_with_limit(dec!(5), dec!(100)).await;
        let blocked = run_with_limit(dec!(0.5), dec!(100)).await;
        assert!(
            within >= 2,
            "under the cap every order trades, got {within}"
        );
        assert_eq!(
            within - blocked,
            2,
            "both orders placed after a trade printed (1 USDT each) are blocked by a 0.5 cap"
        );
    }

    /// The console's order list: every order the loop sends shows up in the
    /// snapshot with what became of it, newest first. (The order manager only
    /// holds WORKING orders, and a market order works for under a second, so
    /// before this the console could never show what the strategy had done.)
    #[tokio::test]
    async fn the_snapshot_lists_the_orders_the_strategy_sent_and_how_they_ended() {
        let snapshot = snapshot_after(
            &[("USDT", dec!(10000))],
            &[dec!(100), dec!(101), dec!(102)],
            AlwaysBuyStrategy,
        )
        .await;
        let orders = &snapshot.recent_orders;
        assert!(orders.len() >= 3, "one order per event: {}", orders.len());
        assert!(
            orders.windows(2).all(|w| w[0].at >= w[1].at),
            "newest first"
        );
        let first = orders.last().expect("an order");
        assert_eq!(first.side, "Buy");
        assert_eq!(first.order_type, "Market");
        assert_eq!(first.quantity, dec!(0.01));
        let filled = orders
            .iter()
            .find(|o| o.status == crate::order_log::FILLED)
            .expect("a market order fills at once in the simulator");
        assert_eq!(filled.filled_quantity, dec!(0.01));
        assert!(filled.avg_fill_price.is_some(), "the fill price is shown");
    }

    /// An order refused by a risk limit never reaches the exchange, so there is
    /// nothing to look up afterwards: the console must be able to say why.
    #[tokio::test]
    async fn an_order_refused_by_a_risk_limit_is_listed_with_the_reason() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), t0 + Duration::seconds(1)),
            trade_event(dec!(100), t0 + Duration::seconds(2)),
        ];
        let (control_handle, _command_tx, snapshot_rx) =
            ControlHandle::new(std::time::Duration::ZERO, empty_session_snapshot());
        let checker = Arc::new(
            apexes_risk::CompositeRiskChecker::new().with_pre_trade_check(Box::new(
                apexes_risk::LimitChecker::new(apexes_risk::RiskLimits {
                    max_order_notional: Some(dec!(0.5)),
                    ..Default::default()
                }),
            )),
        );
        let config = UnifiedLoopConfig {
            control: Some(control_handle),
            initial_prices: HashMap::new(),
            symbol_rules: HashMap::new(),
            risk_checker: Some(checker),
            ..unified_loop_config(dec!(100000), None, HashMap::new())
        };
        run(config, zero_cost_model(None), events, AlwaysBuyStrategy).await;
        let snapshot = snapshot_rx.borrow().clone();
        let refused: Vec<_> = snapshot
            .recent_orders
            .iter()
            .filter(|o| o.status == crate::order_log::REJECTED)
            .collect();
        assert!(!refused.is_empty(), "{:?}", snapshot.recent_orders);
        let reason = refused[0].detail.as_deref().expect("a reason");
        assert!(reason.contains("notional"), "{reason}");
        assert_eq!(refused[0].filled_quantity, dec!(0));
    }

    /// Never trades.
    struct NoopStrategy;

    impl Strategy for NoopStrategy {
        fn on_market_event(
            &mut self,
            _event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            Vec::new()
        }
    }

    /// Buys 1 BTC spot on EVERY market event (not just once) -- used to
    /// distinguish "strategy dispatch happened" from "strategy dispatch
    /// was skipped because the session was paused".
    struct AlwaysBuyStrategy;

    impl Strategy for AlwaysBuyStrategy {
        fn on_market_event(
            &mut self,
            _event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            vec![OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: apexes_core::Venue::Binance,
                asset_class: AssetClass::Perpetual,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(0.01),
                price: None,
                stop_price: None,
                execution_algo: None,
                routing: OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            }]
        }
    }

    fn empty_session_snapshot() -> crate::control::SessionSnapshot {
        crate::control::SessionSnapshot {
            identity: None,
            evaluation: None,
            equity_history: Vec::new(),
            recent_orders: Vec::new(),
            price_history: HashMap::new(),
            equity_priced: true,
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

    /// A `Pause` command sent before the loop starts must take effect
    /// starting the tick AFTER it is drained (per `ControlHandle`'s
    /// documented drain point at the END of each market-event tick):
    /// the first tick still dispatches to the strategy normally, every
    /// tick after the pause is drained produces no new intents/fills,
    /// but `record_equity_point` keeps being called every tick
    /// regardless (funding/margin/liquidation/de-risking are
    /// unaffected by pause state).
    #[tokio::test]
    async fn paused_session_stops_producing_new_intents_but_keeps_updating_equity() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), t0 + Duration::seconds(1)),
            trade_event(dec!(100), t0 + Duration::seconds(2)),
            trade_event(dec!(100), t0 + Duration::seconds(3)),
            trade_event(dec!(100), t0 + Duration::seconds(4)),
        ];
        let num_events = events.len();

        let (control_handle, command_tx, _snapshot_rx) = ControlHandle::new(
            std::time::Duration::from_secs(3600),
            empty_session_snapshot(),
        );
        // Queued before the loop starts -- drained at the end of the
        // FIRST tick, so the first Trade event still dispatches
        // normally and every Trade event after it is paused.
        command_tx.send(SessionCommand::Pause).unwrap();

        let config = unified_loop_config(dec!(10000), None, HashMap::new());
        let config = UnifiedLoopConfig {
            control: Some(control_handle),
            initial_prices: HashMap::new(),
            symbol_rules: HashMap::new(),
            ..config
        };

        let report = run(config, zero_cost_model(None), events, AlwaysBuyStrategy).await;

        // Only the first (depth-snapshot) tick's buy went through --
        // the Pause command drains at the end of that tick, so every
        // subsequent Trade tick is paused and produces no new intents.
        assert_eq!(report.trades.len(), 1);
        // One equity point per event plus the initial pre-first-tick
        // point -- proving equity kept recording every tick, paused or
        // not.
        assert_eq!(report.equity_curve.len(), num_events + 1);
    }

    /// With a control handle, pushed snapshots carry the session identity,
    /// a running evaluation (equity history, PnL, fills, limit usage), and
    /// the data the console's timeline and banner need.
    #[tokio::test]
    async fn snapshots_carry_identity_evaluation_and_equity_history() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), t0 + Duration::seconds(1)),
            trade_event(dec!(110), t0 + Duration::seconds(2)),
            trade_event(dec!(120), t0 + Duration::seconds(3)),
        ];

        let (control_handle, _command_tx, snapshot_rx) =
            ControlHandle::new(std::time::Duration::ZERO, empty_session_snapshot());
        let control_handle = control_handle
            .with_identity(crate::control::SessionIdentity {
                session_id: "s-1".to_string(),
                network: Some("testnet".to_string()),
                mode: "live".to_string(),
                strategy: "demo.drift".to_string(),
                fee_label: None,
            })
            .with_limits(crate::evaluation::LimitSpec {
                max_position_per_symbol: Some(dec!(1)),
                max_daily_fills: Some(10),
            });

        let config = unified_loop_config(dec!(10000), None, HashMap::new());
        let config = UnifiedLoopConfig {
            control: Some(control_handle),
            initial_prices: HashMap::new(),
            symbol_rules: HashMap::new(),
            ..config
        };
        let report = run(config, zero_cost_model(None), events, AlwaysBuyStrategy).await;
        assert!(!report.trades.is_empty(), "the strategy must have traded");

        let snapshot = snapshot_rx.borrow().clone();
        let identity = snapshot
            .identity
            .expect("identity stamped on every snapshot");
        assert_eq!(identity.network.as_deref(), Some("testnet"));
        assert_eq!(identity.strategy, "demo.drift");

        let evaluation = snapshot.evaluation.expect("evaluation present");
        assert!(evaluation.trade_count >= 1);
        assert!(evaluation.turnover > Decimal::ZERO);
        let names: Vec<_> = evaluation
            .limit_usage
            .iter()
            .map(|u| u.name.as_str())
            .collect();
        assert_eq!(names, ["position per symbol", "fills today"]);

        assert!(
            snapshot.equity_history.len() >= 2,
            "a timeline needs points"
        );
        assert!(
            snapshot.equity_history.windows(2).all(|w| w[0].0 <= w[1].0),
            "history is chronological"
        );
    }

    /// A live account already holds the base asset (here 1 BTC) before the
    /// first price arrives. The ledger values unpriced BTC at 1.0, so equity
    /// starts at ~10001 and would jump to ~110k on the first trade. The
    /// evaluation must not treat that as a 100k gain: it starts only once
    /// the holding is priced.
    #[tokio::test]
    async fn a_pre_existing_holding_does_not_create_phantom_pnl_in_the_evaluation() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), t0 + Duration::seconds(1)),
            trade_event(dec!(101), t0 + Duration::seconds(2)),
            trade_event(dec!(102), t0 + Duration::seconds(3)),
        ];
        let (control_handle, _command_tx, snapshot_rx) =
            ControlHandle::new(std::time::Duration::ZERO, empty_session_snapshot());

        let mut balances = HashMap::new();
        balances.insert("USDT".to_string(), dec!(10000));
        balances.insert("BTC".to_string(), dec!(1));
        let config = UnifiedLoopConfig {
            control: Some(control_handle),
            initial_prices: HashMap::new(),
            symbol_rules: HashMap::new(),
            ..unified_loop_config(dec!(10000), None, HashMap::new())
        };
        let config = UnifiedLoopConfig {
            initial_balances: balances,
            ..config
        };
        run(config, zero_cost_model(None), events, NoopStrategy).await;

        let snapshot = snapshot_rx.borrow().clone();
        let evaluation = snapshot.evaluation.expect("evaluation present");
        // 10 000 USDT + 1 BTC at the first printed price (100). Valued at the
        // ledger's 1.0 placeholder it would have been 10 001.
        assert_eq!(
            evaluation.start_equity,
            dec!(10100),
            "start equity must use BTC's real price, not the 1.0 placeholder"
        );
        assert!(
            evaluation.total_pnl.abs() < dec!(10),
            "PnL over 100 -> 102 on 1 BTC is about 2, not tens of thousands: {}",
            evaluation.total_pnl
        );
        assert_eq!(evaluation.max_drawdown, dec!(0));
    }

    /// Runs the loop with a control handle and returns the last snapshot.
    async fn snapshot_after(
        balances: &[(&str, Decimal)],
        prices: &[Decimal],
        strategy: impl Strategy,
    ) -> crate::control::SessionSnapshot {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mut events = vec![depth_snapshot(t0)];
        for (i, price) in prices.iter().enumerate() {
            events.push(trade_event(*price, t0 + Duration::seconds(1 + i as i64)));
        }
        let (control_handle, _command_tx, snapshot_rx) =
            ControlHandle::new(std::time::Duration::ZERO, empty_session_snapshot());
        let config = UnifiedLoopConfig {
            control: Some(control_handle),
            initial_prices: HashMap::new(),
            symbol_rules: HashMap::new(),
            initial_balances: balances.iter().map(|(a, v)| (a.to_string(), *v)).collect(),
            ..unified_loop_config(dec!(0), None, HashMap::new())
        };
        run(config, zero_cost_model(None), events, strategy).await;
        let snapshot = snapshot_rx.borrow();
        snapshot.clone()
    }

    /// The screenshot's case end to end: 1 BTC already in the account, a
    /// strategy that never trades, and BTC falling. The whole loss is the
    /// market's, and exposure must show the holding even though the strategy
    /// is flat.
    #[tokio::test]
    async fn a_session_that_never_trades_has_no_strategy_pnl_when_a_held_asset_moves() {
        let snapshot = snapshot_after(
            &[("USDT", dec!(10000)), ("BTC", dec!(1))],
            &[dec!(100), dec!(95), dec!(90)],
            NoopStrategy,
        )
        .await;
        let e = snapshot.evaluation.expect("evaluation");
        assert_eq!(e.start_equity, dec!(10100));
        assert_eq!(e.total_pnl, dec!(-10));
        assert_eq!(e.holdings_pnl, dec!(-10), "1 BTC fell by 10");
        assert_eq!(e.strategy_pnl, dec!(0));
        assert_eq!(e.unrealized_pnl, dec!(0));
        assert_eq!(e.holdings_value_at_start, dec!(100));
        assert!(
            e.gross_exposure_fraction > dec!(0.008),
            "a 1 BTC holding is exposure even while the strategy is flat: {}",
            e.gross_exposure_fraction
        );
    }

    /// A strategy's own trades land in the strategy line and not the holdings one.
    #[tokio::test]
    async fn the_strategys_trades_are_attributed_to_the_strategy_not_the_holdings() {
        let snapshot = snapshot_after(
            &[("USDT", dec!(10000)), ("BTC", dec!(1))],
            &[dec!(100), dec!(110)],
            AlwaysBuyStrategy,
        )
        .await;
        let e = snapshot.evaluation.expect("evaluation");
        // The 1 BTC held from the start gained +10 whatever the strategy did.
        assert_eq!(e.holdings_pnl, dec!(10));
        assert_eq!(e.strategy_pnl + e.holdings_pnl, e.total_pnl);
    }

    /// A flat account has nothing to separate: the two views agree.
    #[tokio::test]
    async fn a_session_that_starts_flat_reports_no_holdings_pnl() {
        let snapshot = snapshot_after(
            &[("USDT", dec!(10000))],
            &[dec!(100), dec!(110)],
            AlwaysBuyStrategy,
        )
        .await;
        let e = snapshot.evaluation.expect("evaluation");
        assert_eq!(e.holdings_pnl, dec!(0));
        assert_eq!(e.holdings_value_at_start, dec!(0));
        assert_eq!(e.strategy_pnl, e.total_pnl);
    }

    /// A quiet market: the account holds 1 BTC and no trade has printed yet.
    /// With the exchange's last price seeded, the session is valued at once;
    /// without it, the headline equity is flagged as not yet real.
    #[tokio::test]
    async fn a_seeded_price_values_a_held_asset_before_its_first_trade() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let (control_handle, _tx, snapshot_rx) =
            ControlHandle::new(std::time::Duration::ZERO, empty_session_snapshot());
        let config = UnifiedLoopConfig {
            control: Some(control_handle),
            initial_balances: [
                ("USDT".to_string(), dec!(10000)),
                ("BTC".to_string(), dec!(1)),
            ]
            .into_iter()
            .collect(),
            initial_prices: [("BTCUSDT".to_string(), dec!(100))].into_iter().collect(),
            ..unified_loop_config(dec!(0), None, HashMap::new())
        };
        run(
            config,
            zero_cost_model(None),
            vec![depth_snapshot(t0)],
            NoopStrategy,
        )
        .await;

        let snapshot = snapshot_rx.borrow().clone();
        assert!(snapshot.equity_priced);
        assert_eq!(snapshot.equity, dec!(10100), "1 BTC at 100, not at 1.0");
        let e = snapshot
            .evaluation
            .expect("evaluation starts without waiting for a trade");
        assert_eq!(e.start_equity, dec!(10100));
        assert_eq!(e.holdings_value_at_start, dec!(100));
        assert_eq!(e.total_pnl, dec!(0));
    }

    #[tokio::test]
    async fn without_a_price_the_held_asset_is_unpriced_and_equity_says_so() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let (control_handle, _tx, snapshot_rx) =
            ControlHandle::new(std::time::Duration::ZERO, empty_session_snapshot());
        let config = UnifiedLoopConfig {
            control: Some(control_handle),
            initial_balances: [
                ("USDT".to_string(), dec!(10000)),
                ("BTC".to_string(), dec!(1)),
            ]
            .into_iter()
            .collect(),
            ..unified_loop_config(dec!(0), None, HashMap::new())
        };
        run(
            config,
            zero_cost_model(None),
            vec![depth_snapshot(t0)],
            NoopStrategy,
        )
        .await;
        let snapshot = snapshot_rx.borrow().clone();
        assert!(
            !snapshot.equity_priced,
            "BTC has no price yet: equity is a placeholder"
        );
        assert!(
            snapshot.equity_history.is_empty(),
            "the placeholder equity must not be charted"
        );
    }

    /// A flat account has nothing to price: equity is real from the start.
    #[tokio::test]
    async fn a_cash_only_account_is_always_priced() {
        let snapshot = snapshot_after(&[("USDT", dec!(10000))], &[], NoopStrategy).await;
        assert!(snapshot.equity_priced);
    }

    /// An unpriced holding is flagged, and the flag clears with its first trade.
    #[tokio::test]
    async fn the_first_trade_prices_an_unseeded_holding() {
        let snapshot = snapshot_after(
            &[("USDT", dec!(10000)), ("BTC", dec!(1))],
            &[dec!(100)],
            NoopStrategy,
        )
        .await;
        assert!(snapshot.equity_priced);
        assert_eq!(snapshot.equity, dec!(10100));
    }

    #[tokio::test]
    async fn the_snapshot_carries_each_symbols_trade_prices_in_order() {
        let snapshot = snapshot_after(
            &[("USDT", dec!(10000))],
            &[dec!(100), dec!(101), dec!(99)],
            NoopStrategy,
        )
        .await;
        let prices: Vec<Decimal> = snapshot.price_history["BTCUSDT"]
            .iter()
            .map(|(_, price)| *price)
            .collect();
        assert_eq!(prices, vec![dec!(100), dec!(101), dec!(99)]);
        assert!(
            snapshot.price_history["BTCUSDT"]
                .windows(2)
                .all(|w| w[0].0 <= w[1].0)
        );
    }

    /// A seeded price is a real observation: the chart starts with it instead
    /// of staying empty until a quiet symbol's first trade.
    #[tokio::test]
    async fn a_seeded_price_starts_the_symbols_price_history() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let (control_handle, _tx, snapshot_rx) =
            ControlHandle::new(std::time::Duration::ZERO, empty_session_snapshot());
        let config = UnifiedLoopConfig {
            control: Some(control_handle),
            initial_balances: [("USDT".to_string(), dec!(10000))].into_iter().collect(),
            initial_prices: [("BTCUSDT".to_string(), dec!(100))].into_iter().collect(),
            ..unified_loop_config(dec!(0), None, HashMap::new())
        };
        run(
            config,
            zero_cost_model(None),
            vec![depth_snapshot(t0)],
            NoopStrategy,
        )
        .await;
        let snapshot = snapshot_rx.borrow().clone();
        assert_eq!(snapshot.price_history["BTCUSDT"], vec![(t0, dec!(100))]);
    }

    /// Asks for one fixed quantity on every tick, as `trend_guard` does for ETH
    /// (a step of 0.0001) on a symbol whose own step is coarser.
    struct FixedQuantity(Decimal, Side, OrderType, Option<Decimal>);

    impl Strategy for FixedQuantity {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            if !matches!(event, MarketEvent::Trade(_)) {
                return Vec::new();
            }
            vec![OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: apexes_core::Venue::Binance,
                asset_class: AssetClass::Perpetual,
                side: self.1,
                order_type: self.2,
                quantity: self.0,
                price: self.3,
                stop_price: None,
                execution_algo: None,
                routing: OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            }]
        }
    }

    /// Remembers every intent it is asked to submit, and accepts nothing.
    #[derive(Default)]
    struct Recorder(Mutex<Vec<(Decimal, Option<Decimal>)>>);

    #[async_trait::async_trait]
    impl OrderExecutor for Recorder {
        async fn submit(
            &self,
            intent: &OrderIntent,
            _positions: &HashMap<String, apexes_strategy::Position>,
            _balances: &HashMap<String, Decimal>,
            _now: chrono::DateTime<Utc>,
        ) -> Result<(apexes_core::Order, Vec<Fill>), apexes_core::ApexesError> {
            if let Ok(mut sent) = self.0.lock() {
                sent.push((intent.quantity, intent.price));
            }
            Err(apexes_core::ApexesError::OrderRejected {
                reason: "recorder accepts nothing".to_string(),
            })
        }
        fn on_market_event(&self, _event: &MarketEvent) -> Vec<(apexes_core::OrderId, Vec<Fill>)> {
            Vec::new()
        }
        async fn open_orders(&self) -> HashMap<apexes_core::OrderId, apexes_core::Order> {
            HashMap::new()
        }
    }

    fn bnb_like_rules() -> HashMap<String, apexes_core::SymbolRules> {
        // The 0.001 step and 5 USDT minimum BNBUSDT really has, applied to the
        // test symbol so the rejected order from the log can be replayed.
        [(
            "BTCUSDT".to_string(),
            apexes_core::SymbolRules {
                quantity_step: dec!(0.001),
                min_quantity: dec!(0.001),
                max_quantity: dec!(900000),
                price_tick: dec!(0.01),
                min_notional: dec!(5),
            },
        )]
        .into_iter()
        .collect()
    }

    /// Runs `strategy` for two trades at `price` and returns what reached the
    /// executor and the order log's last snapshot.
    async fn through_rules(
        strategy: FixedQuantity,
        rules: HashMap<String, apexes_core::SymbolRules>,
        price: Decimal,
    ) -> (
        Vec<(Decimal, Option<Decimal>)>,
        crate::control::SessionSnapshot,
    ) {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(price, t0 + Duration::seconds(1)),
            trade_event(price, t0 + Duration::seconds(2)),
        ];
        let (control_handle, _tx, snapshot_rx) =
            ControlHandle::new(std::time::Duration::ZERO, empty_session_snapshot());
        let config = UnifiedLoopConfig {
            control: Some(control_handle),
            initial_balances: [("USDT".to_string(), dec!(100000))].into_iter().collect(),
            symbol_rules: rules,
            ..unified_loop_config(dec!(0), None, HashMap::new())
        };
        let recorder = Arc::new(Recorder::default());
        let executor: Arc<dyn OrderExecutor> = recorder.clone();
        let ems: Arc<dyn Ems> = Arc::new(apexes_ems::InMemoryEms::new(
            Arc::new(crate::ems_adapter::MatchingEngineChildOrderSink::new(
                Arc::new(Mutex::new(MatchingEngine::new(
                    zero_cost_model(None),
                    dec!(0.5),
                ))),
            )),
            Arc::new(apexes_strategy::PriceAndFeeScorer),
            HashMap::new(),
            None,
        ));
        run_unified_event_loop(
            config,
            futures::stream::iter(events),
            BacktestClock::new(ReplaySpeed::AsFastAsPossible),
            executor,
            Arc::new(BacktestLiquidationPolicy),
            NeverFillSupplementalFillSource,
            ems,
            strategy,
            None,
            None,
        )
        .await
        .expect("loop");
        let sent = recorder.0.lock().map(|s| s.clone()).unwrap_or_default();
        let snapshot = snapshot_rx.borrow().clone();
        (sent, snapshot)
    }

    /// The reported order: Buy 0.0846 against a 0.001 step.
    #[tokio::test]
    async fn an_off_step_quantity_is_rounded_down_before_it_is_sent() {
        let strategy = FixedQuantity(dec!(0.0846), Side::Buy, OrderType::Market, None);
        let (sent, _) = through_rules(strategy, bnb_like_rules(), dec!(590)).await;
        assert_eq!(sent.len(), 2);
        assert!(sent.iter().all(|(q, _)| *q == dec!(0.084)), "{sent:?}");
    }

    #[tokio::test]
    async fn without_rules_the_order_is_sent_exactly_as_the_strategy_asked() {
        let strategy = FixedQuantity(dec!(0.0846), Side::Buy, OrderType::Market, None);
        let (sent, _) = through_rules(strategy, HashMap::new(), dec!(590)).await;
        assert!(sent.iter().all(|(q, _)| *q == dec!(0.0846)), "{sent:?}");
    }

    /// Too small to be valid: never sent, and the operator is told why.
    #[tokio::test]
    async fn an_order_that_cannot_be_made_valid_is_refused_with_the_reason_and_never_sent() {
        let strategy = FixedQuantity(dec!(0.0004), Side::Buy, OrderType::Market, None);
        let (sent, snapshot) = through_rules(strategy, bnb_like_rules(), dec!(590)).await;
        assert!(sent.is_empty(), "nothing reaches the exchange: {sent:?}");
        let refused = snapshot
            .recent_orders
            .iter()
            .find(|o| o.status == crate::order_log::REJECTED)
            .expect("the refusal is listed under Recent orders");
        let detail = refused.detail.clone().unwrap_or_default();
        assert!(detail.contains("0.001"), "names the step: {detail}");
    }

    #[tokio::test]
    async fn an_order_under_the_minimum_value_is_refused_not_sent() {
        // 0.008 at 590 is 4.72 USDT, under the 5 minimum.
        let strategy = FixedQuantity(dec!(0.008), Side::Buy, OrderType::Market, None);
        let (sent, snapshot) = through_rules(strategy, bnb_like_rules(), dec!(590)).await;
        assert!(sent.is_empty());
        let detail = snapshot
            .recent_orders
            .iter()
            .find_map(|o| o.detail.clone())
            .unwrap_or_default();
        assert!(detail.contains("minimum"), "{detail}");
    }

    #[tokio::test]
    async fn a_limit_price_is_moved_to_the_tick_and_the_quantity_to_the_step() {
        let strategy = FixedQuantity(
            dec!(0.0846),
            Side::Buy,
            OrderType::Limit,
            Some(dec!(590.019)),
        );
        let (sent, _) = through_rules(strategy, bnb_like_rules(), dec!(590)).await;
        assert_eq!(sent[0], (dec!(0.084), Some(dec!(590.01))));
    }

    /// The order the strategy asked for is what the risk checker sees only
    /// after conforming: a rounded-down order is smaller, never larger.
    #[tokio::test]
    async fn conforming_never_makes_an_order_bigger_than_asked() {
        for asked in [dec!(0.0846), dec!(0.0999), dec!(1.2345)] {
            let strategy = FixedQuantity(asked, Side::Buy, OrderType::Market, None);
            let (sent, _) = through_rules(strategy, bnb_like_rules(), dec!(590)).await;
            assert!(sent.iter().all(|(q, _)| *q <= asked), "{asked}: {sent:?}");
        }
    }

    /// `control: None` must stay free of evaluation work: no handle, no
    /// tracker, and the report is unchanged (the existing guard
    /// `control_none_is_a_true_no_op` covers the config; this covers the
    /// loop's behaviour).
    #[tokio::test]
    async fn without_a_control_handle_the_loop_still_reports_normally() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), t0 + Duration::seconds(1)),
        ];
        let config = unified_loop_config(dec!(10000), None, HashMap::new());
        let report = run(config, zero_cost_model(None), events, AlwaysBuyStrategy).await;
        assert!(!report.equity_curve.is_empty());
    }

    /// A strategy that reads a hot-updatable `threshold` param (shared
    /// via `Arc<Mutex<Decimal>>`, exposed through `current_params`/
    /// `set_param`) and only buys when the observed trade price exceeds
    /// it. On the FIRST Trade event it sees, it also sends a
    /// `SetParam` command through `command_tx` (simulating an external
    /// operator sending a live-tuning command mid-run) -- since
    /// `ControlHandle::drain_commands` runs AFTER this dispatch each
    /// tick, the command sent during THIS tick's dispatch is only
    /// visible starting the NEXT tick's dispatch.
    struct ThresholdBuyStrategy {
        threshold: Arc<Mutex<Decimal>>,
        command_tx: mpsc::UnboundedSender<SessionCommand>,
        sent_command: bool,
    }

    impl Strategy for ThresholdBuyStrategy {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            let MarketEvent::Trade(trade) = event else {
                return Vec::new();
            };
            if !self.sent_command {
                self.sent_command = true;
                self.command_tx
                    .send(SessionCommand::SetParam {
                        name: "threshold".to_string(),
                        value: dec!(50),
                    })
                    .expect("receiver still alive");
            }
            let threshold = *self.threshold.lock().unwrap_or_else(|p| p.into_inner());
            if trade.price > threshold {
                return vec![OrderIntent {
                    symbol: "BTCUSDT".to_string(),
                    venue: apexes_core::Venue::Binance,
                    asset_class: AssetClass::Perpetual,
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    quantity: dec!(0.01),
                    price: None,
                    stop_price: None,
                    execution_algo: None,
                    routing: OrderRouting::Fixed,
                    leverage: None,
                    margin_mode: None,
                }];
            }
            Vec::new()
        }

        fn current_params(&self) -> HashMap<String, Decimal> {
            let mut params = HashMap::new();
            params.insert(
                "threshold".to_string(),
                *self.threshold.lock().unwrap_or_else(|p| p.into_inner()),
            );
            params
        }

        fn set_param(
            &mut self,
            name: &str,
            value: Decimal,
        ) -> Result<(), apexes_strategy::StrategyError> {
            if name != "threshold" {
                return Err(apexes_strategy::StrategyError::UnknownParam {
                    name: name.to_string(),
                });
            }
            *self.threshold.lock().unwrap_or_else(|p| p.into_inner()) = value;
            Ok(())
        }
    }

    /// A `SetParam` command sent DURING the first Trade tick's
    /// dispatch is only drained AFTER that tick's dispatch, so that
    /// same tick still observes the OLD threshold (no buy); the SECOND
    /// Trade tick's dispatch observes the NEW threshold (buys).
    #[tokio::test]
    async fn set_param_command_changes_strategy_behavior_next_tick() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), t0 + Duration::seconds(1)),
            trade_event(dec!(100), t0 + Duration::seconds(2)),
        ];

        let (control_handle, command_tx, _snapshot_rx) = ControlHandle::new(
            std::time::Duration::from_secs(3600),
            empty_session_snapshot(),
        );

        let config = unified_loop_config(dec!(10000), None, HashMap::new());
        let config = UnifiedLoopConfig {
            control: Some(control_handle),
            initial_prices: HashMap::new(),
            symbol_rules: HashMap::new(),
            ..config
        };

        let report = run(
            config,
            zero_cost_model(None),
            events,
            ThresholdBuyStrategy {
                threshold: Arc::new(Mutex::new(dec!(1000000))),
                command_tx,
                sent_command: false,
            },
        )
        .await;

        // The first Trade tick (threshold still 1_000_000) produced no
        // buy; only the second Trade tick (threshold now 50) did.
        assert_eq!(report.trades.len(), 1);
        assert_eq!(report.trades[0].side, Side::Buy);
    }

    /// Regression guard for this task itself: `control: None` (the
    /// default at every pre-existing call site) must behave exactly
    /// like the pre-this-task code -- a fixed, known-good numeric
    /// outcome for the same buy-once scenario `unified_loop_config`'s
    /// other consumers already exercise.
    #[tokio::test]
    async fn control_none_behaves_identically_to_before_this_task() {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let events = vec![
            depth_snapshot(t0),
            trade_event(dec!(100), t0 + Duration::seconds(1)),
        ];

        let config = unified_loop_config(dec!(10000), None, HashMap::new());
        assert!(config.control.is_none());

        let report = run(
            config,
            zero_cost_model(None),
            events,
            BuyOnceStrategy { bought: false },
        )
        .await;

        assert_eq!(report.trades.len(), 1);
        assert_eq!(report.trades[0].side, Side::Buy);
        assert_eq!(report.trades[0].quantity, dec!(1));
    }
}
