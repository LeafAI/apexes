//! Shared event-loop core driving both `run_backtest` (historical
//! replay, `Vec<MarketEvent>` wrapped as a `Stream`) and `apexes-paper`'s
//! `run_paper_trading` (live `MarketDataProvider::subscribe()` stream).
//! See design spec `2026-07-31-apexes-paper-design.md` §二 for why this
//! extraction was necessary: the two callers' event sources are
//! fundamentally different (pre-loaded `Vec` vs. live async `Stream`),
//! and the project roadmap explicitly requires `apexes-paper` to "reuse
//! the backtest matching-engine kernel".
//!
//! This module is `pub` (cross-crate visible so `apexes-paper` can call
//! it) but deliberately NOT re-exported from `apexes-backtest`'s
//! top-level `lib.rs` -- callers use the full path
//! `apexes_backtest::event_loop::run_event_loop`. `run_backtest` remains
//! `apexes-backtest`'s primary public entry point.

use crate::clock::BacktestClock;
use crate::cost_model::CostModel;
use crate::latency::LatencyModel;
use crate::ledger::PortfolioLedger;
use crate::matching_engine::MatchingEngine;
use crate::performance::PerformanceTracker;
use crate::report::BacktestReport;
use apexes_core::{ApexesError, Fill, Instrument, MarketEvent, Order, OrderId, OrderType};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{Strategy, StrategyContext};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Configuration shared by `run_backtest` and `run_paper_trading`'s event
/// loop core. `BacktestConfig` (in `runner.rs`) is the historical-replay
/// caller's public config type; `apexes-paper`'s `PaperTradingConfig` is
/// the live-trading caller's public config type. Both construct this
/// shared internal struct before calling `run_event_loop`.
pub struct EventLoopConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub queue_position_factor: Decimal,
    pub risk_free_return: Decimal,
    pub risk_checker: Option<CompositeRiskChecker>,
}

/// Runs the shared event loop: consumes `events` in order through
/// `strategy`, simulating fills via a `MatchingEngine`, applying
/// `apexes-risk` checks if configured, tracking portfolio state via a
/// `PortfolioLedger`, and producing a `BacktestReport`.
///
/// `clock` paces dispatch (an already-configured `BacktestClock` --
/// historical-replay callers pass one built from their `ReplaySpeed`;
/// live-trading callers pass `BacktestClock::new(ReplaySpeed::AsFastAsPossible)`
/// since live events already arrive at real-time pace and need no
/// additional artificial pacing).
///
/// `stop_signal`, if `Some`, causes the loop to exit early (returning
/// whatever `BacktestReport` has accumulated so far, not an error) once
/// it fires or its sender is dropped. `run_backtest` always passes
/// `None` (historical replay runs to completion by construction, since
/// `events` is a finite pre-loaded stream).
///
/// `latency_model`, if `Some`, is applied (via `LatencyModel::apply`,
/// a real `tokio::time::sleep`) after a submitted `OrderIntent` passes
/// its `PreTradeCheck` and before it reaches the matching engine.
/// `run_backtest` always passes `None` (historical replay has no
/// "order submission delay" concept -- recorded timestamps are already
/// fixed).
pub async fn run_event_loop(
    config: EventLoopConfig,
    mut events: impl Stream<Item = MarketEvent> + Unpin,
    mut clock: BacktestClock,
    mut strategy: impl Strategy,
    mut stop_signal: Option<tokio::sync::oneshot::Receiver<()>>,
    latency_model: Option<LatencyModel>,
) -> Result<BacktestReport, ApexesError> {
    let instruments_by_symbol: HashMap<String, Instrument> = config
        .instruments
        .iter()
        .map(|instrument| (instrument.symbol.clone(), instrument.clone()))
        .collect();

    let mut ledger = PortfolioLedger::new(instruments_by_symbol.clone(), config.initial_balances);
    let mut matching_engine = MatchingEngine::new(config.cost_model, config.queue_position_factor);
    let mut overall_tracker = PerformanceTracker::new(config.risk_free_return);
    let mut per_symbol_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut per_venue_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut open_orders: HashMap<OrderId, Order> = HashMap::new();
    let mut last_prices: HashMap<String, Decimal> = HashMap::new();
    let mut all_fills: Vec<Fill> = Vec::new();
    let mut recorded_initial_equity = false;
    let mut risk_checker = config.risk_checker;

    loop {
        if let Some(signal) = stop_signal.as_mut() {
            match signal.try_recv() {
                Ok(()) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => break,
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
            }
        }

        let event = match events.next().await {
            Some(event) => event,
            None => break,
        };

        if !recorded_initial_equity {
            let initial_equity = ledger.equity(&last_prices)?;
            overall_tracker.record_equity_point(event_timestamp(&event), initial_equity);
            recorded_initial_equity = true;
        }

        let timestamp = event_timestamp(&event);
        clock.wait_for_next_event(timestamp).await;

        match &event {
            MarketEvent::Depth(update) => {
                matching_engine.on_depth_update(update);
            }
            MarketEvent::Trade(trade) => {
                if let Some(instrument) = instruments_by_symbol.get(&trade.symbol)
                    && let Some(base) = &instrument.base_currency
                {
                    last_prices.insert(base.clone(), trade.price);
                }
                let fill_results = matching_engine.on_trade_event(trade, timestamp);
                for (order_id, fills) in fill_results {
                    apply_fills(
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
                    if let Some(order) = open_orders.get_mut(&order_id)
                        && let Some(last_fill) = fills.last()
                    {
                        order.filled_quantity += fills.iter().map(|f| f.quantity).sum::<Decimal>();
                        order.updated_at = last_fill.filled_at;
                        if order.filled_quantity >= order.quantity {
                            order.status = apexes_core::OrderStatus::Filled;
                        } else {
                            order.status = apexes_core::OrderStatus::PartiallyFilled;
                        }
                    }
                }
            }
            _ => {} // Tick/Bar events currently only advance the clock/context; no direct matching-engine action.
        }

        let ctx = StrategyContext {
            positions: ledger.positions(),
            balances: ledger.balances(),
            open_orders: &open_orders,
            now: timestamp,
        };
        let intents = strategy.on_market_event(&event, &ctx);

        for intent in intents {
            if let Some(checker) = &risk_checker
                && let Err(violations) = checker.check(
                    &intent,
                    ledger.positions(),
                    ledger.balances(),
                    &open_orders,
                    timestamp,
                )
            {
                tracing::warn!(?violations, symbol = %intent.symbol, "order rejected by risk checker");
                continue;
            }

            if let Some(latency_model) = &latency_model {
                latency_model.apply().await;
            }

            let submit_result = match intent.order_type {
                OrderType::Market => matching_engine
                    .submit_market_order(&intent, timestamp)
                    .map(|(order, fills)| (order, Some(fills))),
                OrderType::Limit => matching_engine
                    .submit_limit_order(&intent, timestamp)
                    .map(|order| (order, None)),
                OrderType::Stop | OrderType::StopLimit => matching_engine
                    .submit_stop_order(&intent, timestamp)
                    .map(|order| (order, None)),
            };

            match submit_result {
                Ok((order, immediate_fills)) => {
                    open_orders.insert(order.id, order);
                    if let Some(fills) = immediate_fills {
                        apply_fills(
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
                }
                Err(error) => {
                    tracing::warn!(?error, symbol = %intent.symbol, "order submission rejected");
                }
            }
        }

        let equity = ledger.equity(&last_prices)?;
        overall_tracker.record_equity_point(timestamp, equity);
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
    })
}

/// Applies a batch of `Fill`s produced by the matching engine: updates
/// the ledger, records each fill into the overall/per-symbol/per-venue
/// `PerformanceTracker`s, and accumulates every fill into `all_fills`
/// (the `BacktestReport.trades` source) in occurrence order.
fn apply_fills(
    ledger: &mut PortfolioLedger,
    overall_tracker: &mut PerformanceTracker,
    per_symbol_trackers: &mut HashMap<String, PerformanceTracker>,
    per_venue_trackers: &mut HashMap<String, PerformanceTracker>,
    all_fills: &mut Vec<Fill>,
    fills: &[Fill],
    risk_free_return: Decimal,
) -> Result<(), ApexesError> {
    for fill in fills {
        ledger.apply_fill(fill)?;
        overall_tracker.record_fill(fill);
        per_symbol_trackers
            .entry(fill.symbol.clone())
            .or_insert_with(|| PerformanceTracker::new(risk_free_return))
            .record_fill(fill);
        per_venue_trackers
            .entry(format!("{:?}", fill.venue))
            .or_insert_with(|| PerformanceTracker::new(risk_free_return))
            .record_fill(fill);
    }
    all_fills.extend_from_slice(fills);
    Ok(())
}

fn event_timestamp(event: &MarketEvent) -> chrono::DateTime<chrono::Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
    }
}
