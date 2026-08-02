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
use apexes_ems::Ems;
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{ParentOrderId, ParentOrderStatus, Strategy, StrategyContext, VenueScorer};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Configuration shared by `run_backtest` and `run_paper_trading`'s event
/// loop core. `BacktestConfig` (in `runner.rs`) is the historical-replay
/// caller's public config type; `apexes-paper`'s `PaperTradingConfig` is
/// the live-trading caller's public config type. Both construct this
/// shared internal struct before calling `run_event_loop`. `risk_checker`
/// is `Option<Arc<CompositeRiskChecker>>` (not owned `CompositeRiskChecker`)
/// because both callers resolve it via
/// `registry.try_one::<Arc<CompositeRiskChecker>>()` (sub-project #9's
/// registry retrofit) before constructing this struct -- `run_event_loop`
/// itself stays `Registry`-agnostic.
pub struct EventLoopConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub queue_position_factor: Decimal,
    pub risk_free_return: Decimal,
    pub risk_checker: Option<std::sync::Arc<CompositeRiskChecker>>,
    /// Tiered maintenance-margin tables, keyed by symbol. Empty
    /// `HashMap` disables all margin/liquidation monitoring (every
    /// leveraged position's `margin_status` lookup simply finds no
    /// table and produces no margin_ratio -- an intentional silent
    /// no-op for callers that never use `OrderIntent.leverage`).
    pub margin_tiers: HashMap<String, apexes_strategy::MarginTierTable>,
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
// The 8-parameter shape is plan-mandated (Task 17 added `venue_scorer`/
// `volume_curve_provider` alongside the pre-existing 6); packing them
// into `EventLoopConfig` was considered but rejected to keep the plan's
// explicitly-specified signature.
#[allow(clippy::too_many_arguments)]
pub async fn run_event_loop(
    config: EventLoopConfig,
    mut events: impl Stream<Item = MarketEvent> + Unpin,
    mut clock: BacktestClock,
    venue_scorer: Arc<dyn VenueScorer>,
    volume_curve_provider: Option<Arc<dyn apexes_ems::VolumeCurveProvider>>,
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
    // `std::sync::Mutex` (not `tokio::sync::Mutex`) because the Task-15
    // `MatchingEngineChildOrderSink` adapter takes that exact type, and
    // every access below is a short synchronous critical section that
    // never holds the guard across an `.await`.
    let matching_engine = Arc::new(Mutex::new(MatchingEngine::new(
        config.cost_model,
        config.queue_position_factor,
    )));
    let child_order_sink: Arc<dyn apexes_ems::ChildOrderSink> = Arc::new(
        crate::ems_adapter::MatchingEngineChildOrderSink::new(Arc::clone(&matching_engine)),
    );
    let ems: Arc<dyn Ems> = Arc::new(apexes_ems::InMemoryEms::new(
        child_order_sink,
        venue_scorer,
        HashMap::new(),
        volume_curve_provider,
    ));
    let mut open_parent_orders: HashMap<ParentOrderId, ParentOrderStatus> = HashMap::new();
    let mut overall_tracker = PerformanceTracker::new(config.risk_free_return);
    let mut per_symbol_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut per_venue_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut open_orders: HashMap<OrderId, Order> = HashMap::new();
    let mut last_prices: HashMap<String, Decimal> = HashMap::new();
    let mut mark_prices: HashMap<String, Decimal> = HashMap::new();
    let mut last_funding_settlement: Option<chrono::DateTime<chrono::Utc>> = None;
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

        let ems_fills = ems.on_market_event(&event).await.map_err(|error| {
            ApexesError::InvalidBacktestConfig {
                reason: format!("ems.on_market_event failed: {error}"),
            }
        })?;
        if !ems_fills.is_empty() {
            apply_fills(
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
        refresh_open_parent_orders(ems.as_ref(), &mut open_parent_orders).await;

        match &event {
            MarketEvent::Depth(update) => {
                lock_engine(&matching_engine).on_depth_update(update);
            }
            MarketEvent::Trade(trade) => {
                if let Some(instrument) = instruments_by_symbol.get(&trade.symbol)
                    && let Some(base) = &instrument.base_currency
                {
                    last_prices.insert(base.clone(), trade.price);
                }
                let fill_results = lock_engine(&matching_engine).on_trade_event(trade, timestamp);
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
            MarketEvent::MarkPrice(update) => {
                // Keyed by BASE ASSET (e.g. "BTC"), mirroring the Trade
                // arm's `last_prices` convention (Task 14) -- every
                // `PortfolioLedger` margin method looks mark prices up by
                // the instrument's base currency, not the trading pair.
                if let Some(instrument) = instruments_by_symbol.get(&update.symbol)
                    && let Some(base) = &instrument.base_currency
                {
                    mark_prices.insert(base.clone(), update.mark_price);
                }
            }
            _ => {} // Tick/Bar events currently only advance the clock/context; no direct matching-engine action.
        }

        // Funding settlement: once per crossed Binance-aligned 8-hour
        // UTC boundary (00:00/08:00/16:00) -- not per-boundary-precise
        // if several boundaries fall within one tick's gap; a single
        // settlement pass is applied per tick that crosses at least
        // one new boundary.
        let current_funding_boundary = last_funding_boundary(timestamp);
        if last_funding_settlement != Some(current_funding_boundary) {
            ledger
                .apply_funding_payments(&mark_prices, lock_engine(&matching_engine).cost_model())?;
            last_funding_settlement = Some(current_funding_boundary);
        }

        let margin_status = ledger.margin_status(&mark_prices, &config.margin_tiers);
        if let Some(checker) = &risk_checker {
            // TEMPORARY until Task 6 adds CompositeRiskChecker::on_tick --
            // this shim always reports no violation; Task 6 replaces this
            // call with `checker.on_tick(&margin_status)` and deletes the
            // `on_tick_shim` helper below.
            if let Err(violations) = on_tick_shim(checker, &margin_status) {
                tracing::warn!(?violations, "position monitor flagged a margin violation");
            }
        }

        // Auto-liquidation (backtest/paper only -- live trading NEVER
        // auto-liquidates; that policy lives in `run_live_trading`,
        // wired by Task 7): every position whose margin ratio has
        // breached 1.0 is force-closed with a market order through the
        // `MatchingEngine`, applied as a normal fill.
        let symbols_to_liquidate = ledger.check_liquidations(&mark_prices, &config.margin_tiers);
        for symbol in symbols_to_liquidate {
            let Some(position) = ledger.positions().get(&symbol).cloned() else {
                continue;
            };
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
            match lock_engine(&matching_engine).submit_market_order(&liquidation_intent, timestamp)
            {
                Ok((order, fills)) => {
                    tracing::error!(symbol = %symbol, "position force-liquidated (margin ratio breached)");
                    open_orders.insert(order.id, order);
                    apply_fills(
                        &mut ledger,
                        &mut overall_tracker,
                        &mut per_symbol_trackers,
                        &mut per_venue_trackers,
                        &mut all_fills,
                        &fills,
                        config.risk_free_return,
                    )?;
                }
                Err(error) => {
                    tracing::error!(?error, symbol = %symbol, "liquidation order submission failed");
                }
            }
        }

        let ctx = StrategyContext {
            positions: ledger.positions(),
            balances: ledger.balances(),
            open_orders: &open_orders,
            open_parent_orders: &open_parent_orders,
            margin_status: &margin_status,
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

            if let Some(execution_algo) = intent.execution_algo {
                let request = apexes_ems::AlgoOrderRequest {
                    symbol: intent.symbol.clone(),
                    venue: intent.venue,
                    side: intent.side,
                    quantity: intent.quantity,
                    execution_algo,
                    routing: intent.routing,
                };
                match ems.submit_algo_order(request).await {
                    Ok(parent_id) => {
                        open_parent_orders
                            .insert(parent_id, apexes_strategy::ParentOrderStatus::Scheduling);
                    }
                    Err(error) => {
                        tracing::warn!(?error, symbol = %intent.symbol, "algo order submission failed");
                    }
                }
                continue;
            }

            let submit_result = match intent.order_type {
                OrderType::Market => lock_engine(&matching_engine)
                    .submit_market_order(&intent, timestamp)
                    .map(|(order, fills)| (order, Some(fills))),
                OrderType::Limit => lock_engine(&matching_engine)
                    .submit_limit_order(&intent, timestamp)
                    .map(|order| (order, None)),
                OrderType::Stop | OrderType::StopLimit => lock_engine(&matching_engine)
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

fn event_timestamp(event: &MarketEvent) -> chrono::DateTime<chrono::Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
        MarketEvent::MarkPrice(mark_price) => mark_price.timestamp,
    }
}

// TEMPORARY until Task 6 adds CompositeRiskChecker::on_tick -- delete
// this shim and use `checker.on_tick(...)` directly once Task 6 lands.
fn on_tick_shim(
    _checker: &CompositeRiskChecker,
    _margin_status: &HashMap<String, apexes_strategy::PositionMarginStatus>,
) -> Result<(), Vec<apexes_risk::RiskViolation>> {
    Ok(())
}

/// Locks the shared `MatchingEngine`, recovering from poisoning by
/// taking the inner value (the codebase's standard poison-recovery
/// pattern -- a panicking holder must not permanently wedge the
/// backtest). Never held across an `.await` (all `MatchingEngine`
/// methods are synchronous).
fn lock_engine(engine: &Arc<Mutex<MatchingEngine>>) -> std::sync::MutexGuard<'_, MatchingEngine> {
    engine.lock().unwrap_or_else(|p| p.into_inner())
}

/// Refreshes `open_parent_orders` by querying `ems.parent_order_status`
/// for every `ParentOrderId` already tracked. `Ems` has no "list all
/// parent order IDs" method (deliberately out of scope, per the design
/// spec) -- new parent orders are added to the map at the point
/// `submit_algo_order` succeeds (see the call site above), not
/// discovered here.
async fn refresh_open_parent_orders(
    ems: &dyn Ems,
    open_parent_orders: &mut HashMap<ParentOrderId, ParentOrderStatus>,
) {
    let ids: Vec<ParentOrderId> = open_parent_orders.keys().copied().collect();
    for id in ids {
        match ems.parent_order_status(id).await {
            Ok(Some(status)) => {
                open_parent_orders.insert(id, status);
            }
            Ok(None) => {
                open_parent_orders.remove(&id);
            }
            Err(error) => {
                tracing::warn!(?error, parent_id = ?id, "failed to refresh parent order status");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ReplaySpeed;
    use crate::cost_model::FlatFeeSchedule;
    use apexes_core::{
        AssetClass, DepthUpdate, MarginMode, MarketType, PriceLevel, Side, TradeEvent,
    };
    use apexes_strategy::{MarginTier, MarginTierTable, OrderIntent, OrderRouting};
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal_macros::dec;

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

    fn event_loop_config(
        initial_usdt: Decimal,
        funding_rate_bps: Option<Decimal>,
        margin_tiers: HashMap<String, MarginTierTable>,
    ) -> EventLoopConfig {
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), initial_usdt);
        EventLoopConfig {
            instruments: vec![btcusdt_perpetual_instrument()],
            initial_balances,
            cost_model: zero_cost_model(funding_rate_bps),
            queue_position_factor: dec!(0.5),
            risk_free_return: Decimal::ZERO,
            risk_checker: None,
            margin_tiers,
        }
    }

    async fn run(
        config: EventLoopConfig,
        events: Vec<MarketEvent>,
        strategy: impl Strategy,
    ) -> BacktestReport {
        run_event_loop(
            config,
            futures::stream::iter(events),
            BacktestClock::new(ReplaySpeed::AsFastAsPossible),
            Arc::new(apexes_strategy::PriceAndFeeScorer),
            None,
            strategy,
            None,
            None,
        )
        .await
        .expect("event loop must not error")
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
            event_loop_config(dec!(1000), None, margin_tiers),
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
            event_loop_config(dec!(1000), None, margin_tiers),
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
            event_loop_config(dec!(1000), Some(dec!(10)), HashMap::new()),
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
            event_loop_config(dec!(1000), Some(dec!(10)), HashMap::new()),
            events,
            BuySpotOnce { bought: false },
        )
        .await;

        // Spot fill only; the mark-price collapse liquidates nothing.
        assert_eq!(report.trades.len(), 1);
        assert_eq!(report.trades[0].side, Side::Buy);
    }
}
