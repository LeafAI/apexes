//! [`run_live_trading`]: drives `Strategy` -> `OrderManager` against
//! live market data, exchange push updates, and a periodic
//! `reconcile()` poll -- see this crate's `lib.rs` docs for the overall
//! design and its relationship to `apexes-paper::run_paper_trading`.

use std::collections::HashMap;
use std::time::Duration;

use apexes_backtest::{PerformanceSummary, PerformanceTracker, PortfolioLedger};
use apexes_core::{ApexesError, Fill, Instrument, MarketDataProvider, MarketEvent};
use apexes_oms::{ExchangeUpdate, OrderManager};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{Strategy, StrategyContext};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use tokio::sync::oneshot;

pub struct LiveTradingConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub risk_free_return: Decimal,
    /// Fallback polling interval for `OrderManager::reconcile()`,
    /// running alongside (not instead of) the push-based
    /// `exchange_updates` stream -- catches anything a WS disconnect
    /// window might have missed.
    pub reconcile_interval: Duration,
}

pub struct LiveTradingReport {
    pub overall: PerformanceSummary,
    pub trades: Vec<Fill>,
}

/// Runs the live-trading event loop until `stop_signal` fires or its
/// sender is dropped, then returns whatever `LiveTradingReport` has
/// accumulated so far (not an error -- graceful shutdown, matching
/// `run_backtest`/`run_paper_trading`'s stop-signal semantics).
///
/// Three concurrent event sources, selected via `tokio::select!`:
/// 1. `market_data_provider`'s live `MarketEvent` stream ->
///    `strategy.on_market_event` -> `risk_checker` (if configured) ->
///    `order_manager.submit_order` for each resulting `OrderIntent`.
/// 2. `exchange_updates` (a real gateway's push stream, e.g. Binance
///    User Data Stream via `apexes-gateway-binance`) ->
///    `order_manager.apply_exchange_update` -> any returned `Fill`s
///    applied to `ledger`.
/// 3. A `config.reconcile_interval` timer -> `order_manager.reconcile()`
///    -> any returned `Fill`s applied to `ledger` the same way.
///
/// Mirrors `apexes_backtest::event_loop::run_event_loop`'s equity-curve
/// and in-trade-risk-monitoring behavior (see this crate's design
/// rationale in `lib.rs`): an equity point is recorded on the very
/// first event received (of any kind) and after every event that could
/// change equity (market events that may submit orders, and any batch
/// of new fills from the push/reconcile paths), and every fill is
/// passed to `risk_checker`'s `on_fill` in addition to `check`.
pub async fn run_live_trading(
    config: LiveTradingConfig,
    market_data_provider: &dyn MarketDataProvider,
    mut strategy: impl Strategy,
    mut order_manager: OrderManager,
    mut risk_checker: Option<CompositeRiskChecker>,
    mut exchange_updates: impl Stream<Item = ExchangeUpdate> + Unpin,
    mut stop_signal: oneshot::Receiver<()>,
) -> Result<LiveTradingReport, ApexesError> {
    let mut market_events = market_data_provider.subscribe(&config.instruments).await?;

    let instruments_by_symbol: HashMap<String, Instrument> = config
        .instruments
        .iter()
        .map(|instrument| (instrument.symbol.clone(), instrument.clone()))
        .collect();

    let mut ledger = PortfolioLedger::new(instruments_by_symbol.clone(), config.initial_balances);
    let mut tracker = PerformanceTracker::new(config.risk_free_return);
    let mut all_fills: Vec<Fill> = Vec::new();
    let mut last_prices: HashMap<String, Decimal> = HashMap::new();
    let mut reconcile_timer = tokio::time::interval(config.reconcile_interval);
    let mut recorded_initial_equity = false;

    loop {
        tokio::select! {
            _ = &mut stop_signal => break,

            market_event = market_events.next() => {
                let Some(event) = market_event else { break };

                if !recorded_initial_equity {
                    let initial_equity = ledger.equity(&last_prices)?;
                    tracker.record_equity_point(event_timestamp(&event), initial_equity);
                    recorded_initial_equity = true;
                }

                update_last_price(&event, &instruments_by_symbol, &mut last_prices);

                handle_market_event(
                    &event,
                    &mut strategy,
                    &risk_checker,
                    &mut order_manager,
                    &ledger,
                ).await;

                let equity = ledger.equity(&last_prices)?;
                tracker.record_equity_point(event_timestamp(&event), equity);
            }

            update = exchange_updates.next() => {
                let Some(update) = update else { continue };
                match order_manager
                    .apply_exchange_update(update.client_order_id, update.exchange_order_id, update.status)
                    .await
                {
                    Ok(new_fills) if !new_fills.is_empty() => {
                        apply_new_fills(&mut ledger, &mut tracker, &mut risk_checker, &mut all_fills, new_fills);
                        let equity = ledger.equity(&last_prices)?;
                        tracker.record_equity_point(chrono::Utc::now(), equity);
                    }
                    Ok(_) => {}
                    Err(error) => tracing::warn!(?error, "apply_exchange_update failed"),
                }
            }

            _ = reconcile_timer.tick() => {
                match order_manager.reconcile().await {
                    Ok(changes) => {
                        let mut any_fills = false;
                        for (order_id, new_fills) in changes {
                            if new_fills.is_empty() {
                                continue;
                            }
                            any_fills = true;
                            tracing::info!(order_id = ?order_id, fill_count = new_fills.len(), "reconcile applied a status change");
                            apply_new_fills(&mut ledger, &mut tracker, &mut risk_checker, &mut all_fills, new_fills);
                        }
                        if any_fills {
                            let equity = ledger.equity(&last_prices)?;
                            tracker.record_equity_point(chrono::Utc::now(), equity);
                        }
                    }
                    Err(error) => tracing::warn!(?error, "reconcile() failed"),
                }
            }
        }
    }

    Ok(LiveTradingReport {
        overall: tracker.summary(),
        trades: all_fills,
    })
}

async fn handle_market_event(
    event: &MarketEvent,
    strategy: &mut impl Strategy,
    risk_checker: &Option<CompositeRiskChecker>,
    order_manager: &mut OrderManager,
    ledger: &PortfolioLedger,
) {
    let ctx = StrategyContext {
        positions: ledger.positions(),
        balances: ledger.balances(),
        open_orders: order_manager.open_orders(),
        now: event_timestamp(event),
    };
    let intents = strategy.on_market_event(event, &ctx);

    for intent in intents {
        if let Some(checker) = risk_checker
            && let Err(violations) = checker.check(
                &intent,
                ledger.positions(),
                ledger.balances(),
                order_manager.open_orders(),
                event_timestamp(event),
            )
        {
            tracing::warn!(?violations, symbol = %intent.symbol, "order rejected by risk checker");
            continue;
        }

        let symbol = intent.symbol.clone();
        match order_manager
            .submit_order(intent, ledger.positions(), ledger.balances())
            .await
        {
            Ok(order_id) => tracing::info!(order_id = ?order_id, symbol, "order submitted"),
            Err(error) => tracing::warn!(?error, symbol, "order submission failed"),
        }
    }
}

/// Applies a batch of `Fill`s to the ledger, the overall
/// `PerformanceTracker`, and (if configured) every registered
/// `InTradeMonitor` via `risk_checker`'s `on_fill` -- mirroring
/// `apexes_backtest::event_loop::run_event_loop`'s `apply_fills`, which
/// this function is the live-trading counterpart of. A violation from
/// `on_fill` is logged, not propagated as an error -- an in-trade
/// monitor observes fills that have already happened; it cannot undo
/// them, only flag them for the caller/operator to act on (matching the
/// reference implementation's behavior exactly).
fn apply_new_fills(
    ledger: &mut PortfolioLedger,
    tracker: &mut PerformanceTracker,
    risk_checker: &mut Option<CompositeRiskChecker>,
    all_fills: &mut Vec<Fill>,
    new_fills: Vec<Fill>,
) {
    for fill in &new_fills {
        if let Err(error) = ledger.apply_fill(fill) {
            tracing::warn!(?error, order_id = ?fill.order_id, "failed to apply fill to ledger");
            continue;
        }
        tracker.record_fill(fill);
        if let Some(checker) = risk_checker
            && let Err(violations) = checker.on_fill(fill, ledger.positions(), ledger.balances())
        {
            tracing::warn!(?violations, symbol = %fill.symbol, "in-trade risk monitor flagged a violation");
        }
    }
    all_fills.extend(new_fills);
}

/// Updates `last_prices` (the mark-price map `PortfolioLedger::equity`
/// needs to value non-quote-currency balances) from a market event,
/// mirroring `apexes_backtest::event_loop::run_event_loop`'s handling
/// of `MarketEvent::Trade` -- extended here to also use `Tick`'s
/// midpoint, since a live venue's own price feed is the source of
/// truth for mark prices (not just trade prints) and Tick events are a
/// primary data source for many venues.
fn update_last_price(
    event: &MarketEvent,
    instruments_by_symbol: &HashMap<String, Instrument>,
    last_prices: &mut HashMap<String, Decimal>,
) {
    let (symbol, price) = match event {
        MarketEvent::Trade(trade) => (&trade.symbol, trade.price),
        MarketEvent::Tick(tick) => (
            &tick.symbol,
            (tick.bid_price + tick.ask_price) / Decimal::TWO,
        ),
        _ => return,
    };
    if let Some(instrument) = instruments_by_symbol.get(symbol)
        && let Some(base) = &instrument.base_currency
    {
        last_prices.insert(base.clone(), price);
    }
}

fn event_timestamp(event: &MarketEvent) -> chrono::DateTime<chrono::Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
    }
}
