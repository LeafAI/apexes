//! [`run_live_trading`]: drives `Strategy` -> `OrderManager` against
//! live market data, exchange push updates, and a periodic
//! `reconcile()` poll -- see this crate's `lib.rs` docs for the overall
//! design and its relationship to `apexes-paper::run_paper_trading`.

use std::collections::HashMap;
use std::time::Duration;

use std::sync::Arc;

use apexes_backtest::{CostModel, PerformanceSummary, PerformanceTracker, PortfolioLedger};
use apexes_core::registry::Registry;
use apexes_core::{ApexesError, Fill, Instrument, MarketDataProvider, MarketEvent, Venue};
use apexes_ems::Ems;
use apexes_ems::postgres::{OrderManagerChildOrderSink, PostgresEms};
use apexes_oms::{ExchangeUpdate, OrderManager};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{
    MarginTierTable, ParentOrderId, ParentOrderStatus, PositionMarginStatus, Strategy,
    StrategyContext, VenueScorer,
};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use tokio::sync::oneshot;

pub struct LiveTradingConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    /// Live trading does not simulate fills/fees itself (real fills
    /// arrive via the exchange push/reconcile paths); this field exists
    /// purely for its `funding_rate_bps`, which the local ledger's
    /// periodic `apply_funding_payments` pass needs to keep its equity
    /// curve consistent with the funding the exchange actually charged
    /// (matching backtest/paper's accounting convention). Set
    /// `funding_rate_bps: None` to disable funding settlement.
    pub cost_model: CostModel,
    /// Per-symbol tiered maintenance-margin tables, used by the local
    /// ledger's `margin_status`/`check_liquidations` monitor-only
    /// passes (live trading NEVER auto-liquidates -- see the margin
    /// block in `run_live_trading`'s market-event branch).
    pub margin_tiers: HashMap<String, MarginTierTable>,
    pub risk_free_return: Decimal,
    /// Fallback polling interval for `OrderManager::reconcile()`,
    /// running alongside (not instead of) the push-based
    /// `exchange_updates` stream -- catches anything a WS disconnect
    /// window might have missed.
    pub reconcile_interval: Duration,
    /// PostgreSQL connection URL for `PostgresEms`'s own `parent_orders`/
    /// `parent_order_children` tables -- typically the SAME database
    /// `OrderManager`'s own `OrderStore` uses, but passed independently
    /// since `run_live_trading` only receives an already-constructed
    /// `OrderManager`, not a reusable connection pool.
    pub ems_postgres_url: String,
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
    registry: &Registry,
    mut strategy: impl Strategy,
    order_manager: OrderManager,
    exchange_updates: impl Stream<Item = ExchangeUpdate> + Unpin,
    mut stop_signal: oneshot::Receiver<()>,
) -> Result<LiveTradingReport, ApexesError> {
    if config.reconcile_interval.is_zero() {
        return Err(ApexesError::InvalidBacktestConfig {
            reason: "reconcile_interval must be non-zero".to_string(),
        });
    }

    let mut risk_checker: Option<Arc<CompositeRiskChecker>> = registry
        .try_one::<Arc<CompositeRiskChecker>>()
        .map_err(|error| ApexesError::InvalidBacktestConfig {
            reason: format!("registry lookup for CompositeRiskChecker failed: {error}"),
        })?;
    let venue_scorer = registry
        .try_one::<Arc<dyn VenueScorer>>()
        .map_err(|error| ApexesError::InvalidBacktestConfig {
            reason: format!("registry lookup for VenueScorer failed: {error}"),
        })?
        .unwrap_or_else(|| Arc::new(apexes_strategy::PriceAndFeeScorer));
    let volume_curve_provider = registry
        .try_one::<Arc<dyn apexes_ems::VolumeCurveProvider>>()
        .map_err(|error| ApexesError::InvalidBacktestConfig {
            reason: format!("registry lookup for VolumeCurveProvider failed: {error}"),
        })?;
    // `tokio::sync::Mutex` (matching `OrderManagerChildOrderSink`'s
    // expected type): every critical section below awaits
    // `OrderManager`'s async methods while holding the guard, so a
    // `std::sync::Mutex` would be wrong here.
    let order_manager = Arc::new(tokio::sync::Mutex::new(order_manager));
    let child_order_sink: Arc<dyn apexes_ems::ChildOrderSink> =
        Arc::new(OrderManagerChildOrderSink::new(Arc::clone(&order_manager)));
    let ems: Arc<dyn Ems> = PostgresEms::new(
        &config.ems_postgres_url,
        child_order_sink,
        venue_scorer,
        HashMap::new(),
        volume_curve_provider,
    )
    .await
    .map_err(|error| ApexesError::InvalidBacktestConfig {
        reason: format!("failed to construct PostgresEms: {error}"),
    })?;
    let mut open_parent_orders: HashMap<ParentOrderId, ParentOrderStatus> = HashMap::new();

    let mut market_events = subscribe_all_venues(registry, &config.instruments).await?;

    // Once `exchange_updates` ends (returns `None`), chain in a
    // `pending()` stream that never resolves, so this branch stops
    // being re-polled every loop iteration -- treating "the stream
    // permanently ended" as "this branch is now permanently inert"
    // rather than "nothing ready yet, try again immediately", which
    // would otherwise busy-loop (verified empirically to spin tens of
    // millions of iterations per second) for as long as
    // `market_events` keeps the outer loop alive afterward. The real
    // Binance User Data Stream never ends on its own (its socket task
    // retries forever), so this only matters for a finite/synthetic
    // stream (e.g. a test's `futures::stream::empty()`), but the fix
    // costs nothing in the common case.
    let mut exchange_updates = exchange_updates.chain(futures::stream::pending());

    let instruments_by_symbol: HashMap<String, Instrument> = config
        .instruments
        .iter()
        .map(|instrument| (instrument.symbol.clone(), instrument.clone()))
        .collect();

    let mut ledger = PortfolioLedger::new(instruments_by_symbol.clone(), config.initial_balances);
    let mut tracker = PerformanceTracker::new(config.risk_free_return);
    let mut all_fills: Vec<Fill> = Vec::new();
    let mut last_prices: HashMap<String, Decimal> = HashMap::new();
    // Mark prices keyed by BASE CURRENCY (e.g. "BTC"), mirroring
    // `last_prices`'s convention: every `PortfolioLedger` margin method
    // looks prices up by the instrument's base currency, not the
    // trading-pair symbol.
    let mut mark_prices: HashMap<String, Decimal> = HashMap::new();
    let mut last_funding_settlement: Option<chrono::DateTime<chrono::Utc>> = None;
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

                update_prices(&event, &instruments_by_symbol, &mut last_prices, &mut mark_prices);

                match ems.on_market_event(&event).await {
                    Ok(ems_fills) if !ems_fills.is_empty() => {
                        apply_new_fills(&mut ledger, &mut tracker, &mut risk_checker, &mut all_fills, ems_fills);
                        let equity = ledger.equity(&last_prices)?;
                        tracker.record_equity_point(chrono::Utc::now(), equity);
                    }
                    Ok(_) => {}
                    Err(error) => tracing::warn!(?error, "ems.on_market_event failed"),
                }
                refresh_open_parent_orders(ems.as_ref(), &mut open_parent_orders).await;

                // Funding settlement: once per crossed Binance-aligned
                // 8-hour UTC boundary (00:00/08:00/16:00), mirroring
                // `run_event_loop`'s convention. Live trading's own
                // funding settlement is informational/local-ledger-only
                // -- the REAL funding charge already happened on the
                // exchange itself; this keeps our local ledger's equity
                // curve consistent with what the exchange actually
                // charged, matching backtest/paper's accounting
                // convention for comparability, not because this ledger
                // is authoritative for real funds.
                let current_funding_boundary = last_funding_boundary(event_timestamp(&event));
                if last_funding_settlement != Some(current_funding_boundary) {
                    ledger.apply_funding_payments(&mark_prices, &config.cost_model)?;
                    last_funding_settlement = Some(current_funding_boundary);
                }

                let margin_status = ledger.margin_status(&mark_prices, &config.margin_tiers);
                if let Some(checker) = &risk_checker
                    && let Err(violations) = checker.on_tick(&margin_status)
                {
                    tracing::warn!(?violations, "position monitor flagged a margin violation (live)");
                }

                // MONITOR-ONLY: never submit a liquidation order in
                // live trading -- only log loudly and rely on
                // MarginMonitor's PreTradeCheck (wired via
                // CompositeRiskChecker above) to block new orders for
                // the affected symbol. Real liquidation is the
                // exchange's own responsibility; this local ledger has
                // no authority to force-close a real position.
                for symbol in ledger.check_liquidations(&mark_prices, &config.margin_tiers) {
                    tracing::error!(
                        symbol = %symbol,
                        "LIVE TRADING: position margin ratio has breached the liquidation threshold -- \
                         this system will NOT auto-liquidate; the exchange's own liquidation engine may \
                         act independently. New orders for this symbol are blocked via MarginMonitor."
                    );
                }

                handle_market_event(
                    &event,
                    &mut strategy,
                    &risk_checker,
                    &order_manager,
                    &ledger,
                    &ems,
                    &mut open_parent_orders,
                    &margin_status,
                ).await;

                let equity = ledger.equity(&last_prices)?;
                tracker.record_equity_point(event_timestamp(&event), equity);
            }

            update = exchange_updates.next() => {
                let Some(update) = update else { continue };
                match order_manager
                    .lock()
                    .await
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
                match order_manager.lock().await.reconcile().await {
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

#[allow(clippy::too_many_arguments)]
async fn handle_market_event(
    event: &MarketEvent,
    strategy: &mut impl Strategy,
    risk_checker: &Option<Arc<CompositeRiskChecker>>,
    order_manager: &Arc<tokio::sync::Mutex<OrderManager>>,
    ledger: &PortfolioLedger,
    ems: &Arc<dyn Ems>,
    open_parent_orders: &mut HashMap<ParentOrderId, ParentOrderStatus>,
    margin_status: &HashMap<String, PositionMarginStatus>,
) {
    // The guard must be bound to a named local (not a temporary
    // expression inside the `StrategyContext` literal) so the
    // `&HashMap<OrderId, Order>` `open_orders()` returns can borrow
    // from it for the context's whole lifetime.
    let open_orders_guard = order_manager.lock().await;
    let ctx = StrategyContext {
        positions: ledger.positions(),
        balances: ledger.balances(),
        open_orders: open_orders_guard.open_orders(),
        open_parent_orders,
        margin_status,
        now: event_timestamp(event),
    };
    let intents = strategy.on_market_event(event, &ctx);
    drop(open_orders_guard);

    for intent in intents {
        if let Some(checker) = risk_checker
            && let Err(violations) = checker.check(
                &intent,
                ledger.positions(),
                ledger.balances(),
                order_manager.lock().await.open_orders(),
                event_timestamp(event),
            )
        {
            tracing::warn!(?violations, symbol = %intent.symbol, "order rejected by risk checker");
            continue;
        }

        if let Some(execution_algo) = intent.execution_algo {
            let request = apexes_ems::AlgoOrderRequest {
                symbol: intent.symbol.clone(),
                venue: intent.venue,
                side: intent.side,
                quantity: intent.quantity,
                execution_algo,
                routing: intent.routing,
                leverage: intent.leverage,
                margin_mode: intent.margin_mode,
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

        let symbol = intent.symbol.clone();
        let mut order_manager = order_manager.lock().await;
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
    risk_checker: &mut Option<Arc<CompositeRiskChecker>>,
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

/// Updates `last_prices` (the price map `PortfolioLedger::equity`
/// needs to value non-quote-currency balances) and `mark_prices` (the
/// map every `PortfolioLedger` margin method -- `margin_status`,
/// `check_liquidations`, `apply_funding_payments` -- looks prices up
/// in) from a market event. Both maps are keyed by BASE CURRENCY (e.g.
/// "BTC"), resolved through `instruments_by_symbol`, NOT by the
/// trading-pair symbol. Mirrors
/// `apexes_backtest::event_loop::run_event_loop`'s handling of
/// `MarketEvent::Trade`/`MarkPrice` -- extended here to also use
/// `Tick`'s midpoint for `last_prices`, since a live venue's own price
/// feed is the source of truth for mark prices (not just trade prints)
/// and Tick events are a primary data source for many venues.
fn update_prices(
    event: &MarketEvent,
    instruments_by_symbol: &HashMap<String, Instrument>,
    last_prices: &mut HashMap<String, Decimal>,
    mark_prices: &mut HashMap<String, Decimal>,
) {
    let (symbol, price, target) = match event {
        MarketEvent::Trade(trade) => (&trade.symbol, trade.price, last_prices),
        MarketEvent::Tick(tick) => (
            &tick.symbol,
            (tick.bid_price + tick.ask_price) / Decimal::TWO,
            last_prices,
        ),
        MarketEvent::MarkPrice(mark_price) => {
            (&mark_price.symbol, mark_price.mark_price, mark_prices)
        }
        _ => return,
    };
    if let Some(instrument) = instruments_by_symbol.get(symbol)
        && let Some(base) = &instrument.base_currency
    {
        target.insert(base.clone(), price);
    }
}

/// Returns the most recent Binance-aligned 8-hour UTC boundary
/// (00:00/08:00/16:00) at or before `timestamp`. Identical logic to
/// `apexes_backtest::event_loop`'s private copy, duplicated here since
/// this crate cannot import a private function from another crate.
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

/// Refreshes `open_parent_orders` by querying `ems.parent_order_status`
/// for every `ParentOrderId` already tracked. Identical logic to
/// `apexes_backtest::event_loop`'s private copy, duplicated here since
/// this crate cannot import a private function from another crate.
/// `Ems` has no "list all parent order IDs" method (deliberately out of
/// scope, per the design spec) -- new parent orders are added to the
/// map at the point `submit_algo_order` succeeds, not discovered here.
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

/// Groups `instruments` by `Venue`, looks up the one registered
/// `Arc<dyn MarketDataProvider>` for each distinct venue involved
/// (`registry.get::<Arc<dyn MarketDataProvider>, Venue>()`), subscribes
/// each provider to its own subset of `instruments`, and merges all the
/// resulting streams into one via `futures::stream::select_all` -- so
/// the rest of `run_live_trading`'s event loop sees a single unified
/// `MarketEvent` stream regardless of how many distinct venues
/// `config.instruments` actually spans. Returns
/// `Err(ApexesError::SubscriptionFailed)` if any involved venue has no
/// registered provider.
async fn subscribe_all_venues(
    registry: &Registry,
    instruments: &[Instrument],
) -> Result<futures::stream::SelectAll<futures::stream::BoxStream<'static, MarketEvent>>, ApexesError>
{
    let providers = registry.get::<Arc<dyn MarketDataProvider>, Venue>();

    let mut instruments_by_venue: HashMap<Venue, Vec<Instrument>> = HashMap::new();
    for instrument in instruments {
        instruments_by_venue
            .entry(instrument.venue)
            .or_default()
            .push(instrument.clone());
    }

    let mut streams = Vec::new();
    for (venue, venue_instruments) in instruments_by_venue {
        let provider = providers
            .get(&venue)
            .ok_or_else(|| ApexesError::SubscriptionFailed {
                symbol: venue_instruments
                    .first()
                    .map(|i| i.symbol.clone())
                    .unwrap_or_default(),
                venue,
                reason: "no MarketDataProvider registered for this venue".to_string(),
                source_detail: None,
            })?;
        streams.push(provider.subscribe(&venue_instruments).await?);
    }

    Ok(futures::stream::select_all(streams))
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, MarkPriceUpdate, MarketType, Tick, TradeEvent};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn btc_usdt_instrument(asset_class: AssetClass) -> Instrument {
        Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            asset_class,
            Some("BTC".to_string()),
            Some("USDT".to_string()),
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap()
    }

    fn instruments_by_symbol(asset_class: AssetClass) -> HashMap<String, Instrument> {
        HashMap::from([("BTCUSDT".to_string(), btc_usdt_instrument(asset_class))])
    }

    #[test]
    fn last_funding_boundary_buckets_into_8_hour_windows() {
        // 00:00/08:00/16:00 UTC boundaries; anything in between rounds
        // DOWN to the most recent boundary.
        let cases = [
            ((2026, 1, 1, 0, 0, 0), (2026, 1, 1, 0, 0, 0)),
            ((2026, 1, 1, 7, 59, 59), (2026, 1, 1, 0, 0, 0)),
            ((2026, 1, 1, 8, 0, 0), (2026, 1, 1, 8, 0, 0)),
            ((2026, 1, 1, 9, 30, 0), (2026, 1, 1, 8, 0, 0)),
            ((2026, 1, 1, 16, 0, 0), (2026, 1, 1, 16, 0, 0)),
            ((2026, 1, 1, 23, 59, 59), (2026, 1, 1, 16, 0, 0)),
        ];
        for ((iy, im, id, ih, imin, is), (ey, em, ed, eh, emin, es)) in cases {
            let input = chrono::Utc
                .with_ymd_and_hms(iy, im, id, ih, imin, is)
                .unwrap();
            let expected = chrono::Utc
                .with_ymd_and_hms(ey, em, ed, eh, emin, es)
                .unwrap();
            assert_eq!(last_funding_boundary(input), expected, "input {input}");
        }
    }

    #[test]
    fn update_prices_keys_mark_price_by_base_currency_not_symbol() {
        let instruments = instruments_by_symbol(AssetClass::Perpetual);
        let mut last_prices = HashMap::new();
        let mut mark_prices = HashMap::new();
        let event = MarketEvent::MarkPrice(MarkPriceUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            mark_price: dec!(50000),
            timestamp: chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        });

        update_prices(&event, &instruments, &mut last_prices, &mut mark_prices);

        // Every PortfolioLedger margin method looks prices up by BASE
        // currency -- a "BTCUSDT" key would be silently never-found.
        assert_eq!(mark_prices.get("BTC"), Some(&dec!(50000)));
        assert!(!mark_prices.contains_key("BTCUSDT"));
        // MarkPrice events feed mark_prices only, not last_prices
        // (mirroring run_event_loop's Trade/MarkPrice split).
        assert!(last_prices.is_empty());
    }

    #[test]
    fn update_prices_trade_and_tick_feed_last_prices_by_base_currency() {
        let instruments = instruments_by_symbol(AssetClass::Spot);
        let mut last_prices = HashMap::new();
        let mut mark_prices = HashMap::new();
        let timestamp = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();

        update_prices(
            &MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price: dec!(100),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp,
            }),
            &instruments,
            &mut last_prices,
            &mut mark_prices,
        );
        update_prices(
            &MarketEvent::Tick(Tick {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                bid_price: dec!(98),
                ask_price: dec!(104),
                bid_size: dec!(1),
                ask_size: dec!(1),
                timestamp,
            }),
            &instruments,
            &mut last_prices,
            &mut mark_prices,
        );

        assert_eq!(last_prices.get("BTC"), Some(&dec!(101))); // tick midpoint
        assert!(mark_prices.is_empty());
    }

    #[test]
    fn update_prices_ignores_unknown_symbols() {
        let instruments = instruments_by_symbol(AssetClass::Perpetual);
        let mut last_prices = HashMap::new();
        let mut mark_prices = HashMap::new();
        let event = MarketEvent::MarkPrice(MarkPriceUpdate {
            symbol: "ETHUSDT".to_string(),
            venue: Venue::Binance,
            mark_price: dec!(3000),
            timestamp: chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        });

        update_prices(&event, &instruments, &mut last_prices, &mut mark_prices);

        assert!(mark_prices.is_empty());
    }
}
