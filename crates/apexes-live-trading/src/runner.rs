//! [`run_live_trading`]: drives `Strategy` -> `OrderManager` against
//! live market data, exchange push updates, and a periodic
//! `reconcile()` poll -- see this crate's `lib.rs` docs for the overall
//! design and its relationship to `apexes-paper::run_paper_trading`.
//!
//! Delegates to the shared `apexes_backtest::unified_loop::run_unified_event_loop`
//! core (see that module's docs) -- this function's job is to resolve
//! this crate's own registry lookups, construct the live-flavored
//! execution backend (`OrderManagerExecutor`/`LiveLiquidationPolicy`/
//! `LiveSupplementalFillSource`, `crate::order_executor_adapter`) plus a
//! `PostgresEms` sharing the same `OrderManager`, and adapt
//! `UnifiedLoopReport` (a `BacktestReport` alias) back to this crate's
//! own `LiveTradingReport` shape.

use std::collections::HashMap;
use std::time::Duration;

use std::sync::Arc;

use apexes_backtest::clock::BacktestClock;
use apexes_backtest::unified_loop::{UnifiedLoopConfig, run_unified_event_loop};
use apexes_backtest::{CostModel, PerformanceSummary, ReplaySpeed};
use apexes_core::registry::Registry;
use apexes_core::{ApexesError, Fill, Instrument, MarketEvent, Venue};
use apexes_ems::Ems;
use apexes_ems::postgres::{OrderManagerChildOrderSink, PostgresEms};
use apexes_message_bus::MarketEventBus;
use apexes_oms::{ExchangeUpdate, OrderManager};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{MarginTierTable, OrderExecutor, Strategy, VenueScorer};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use tokio::sync::oneshot;

use crate::order_executor_adapter::{
    LiveLiquidationPolicy, LiveSupplementalFillSource, OrderManagerExecutor,
};

pub struct LiveTradingConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    /// Pre-existing exchange positions recovered at startup (see
    /// `crate::startup_recovery`). Empty if the account is flat.
    pub initial_positions: HashMap<String, apexes_strategy::Position>,
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
    /// passes (live trading NEVER auto-liquidates -- see
    /// `LiveLiquidationPolicy::should_auto_execute`'s
    /// `LiquidationReason::MarginBreach` arm).
    pub margin_tiers: HashMap<String, MarginTierTable>,
    /// Extreme-market circuit breaker configuration. `None` (default)
    /// disables it entirely.
    pub market_condition_config: Option<apexes_risk::MarketConditionConfig>,
    /// Whether `run_live_trading` is allowed to automatically submit
    /// de-risking (partial-close) orders when `market_condition_config`
    /// detects extreme conditions. Defaults to `false` at every
    /// existing call site (added by this task): monitor-only, matching
    /// row 6.4's live-trading-never-auto-liquidates precedent. Set to
    /// `true` to explicitly opt into live automatic partial closes --
    /// this is a deliberate choice this plan's design spec calls out
    /// as a real behavioral difference from margin-based liquidation
    /// (row 6.4), which NEVER auto-executes in live trading regardless
    /// of configuration.
    pub auto_derisk_live: bool,
    /// A last price per symbol fetched from the exchange before the first
    /// event, so held assets are valued from the start. See
    /// `UnifiedLoopConfig::initial_prices`.
    pub initial_prices: HashMap<String, Decimal>,
    /// The exchange's rules per symbol, applied to every order before it is
    /// sent. See `UnifiedLoopConfig::symbol_rules`.
    pub symbol_rules: HashMap<String, apexes_core::SymbolRules>,
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
    /// Live-tuning/monitoring control channel, threaded straight
    /// through to `UnifiedLoopConfig.control`. `None` (the default for
    /// every pre-existing caller) is a silent no-op -- identical to
    /// this crate's behavior before this field existed.
    pub control: Option<apexes_backtest::ControlHandle>,
    /// Kill switch shared with the engine's control endpoint. When
    /// engaged it blocks new orders (attach the SAME `Arc` to the
    /// `OrderManager` via `with_kill_switch`) and this runner cancels
    /// every open order. `None` disables the halt machinery.
    pub kill_switch: Option<Arc<apexes_oms::KillSwitch>>,
    /// Engage `kill_switch` automatically if no market event arrives
    /// for this long. Requires `kill_switch`; `None` disables.
    pub stale_data_timeout: Option<Duration>,
    /// Notified when the exchange push stream (re)connects; triggers an
    /// immediate `reconcile()`. `None` relies on `reconcile_interval` only.
    pub resync: Option<Arc<tokio::sync::Notify>>,
    /// Where open exchange positions are read from when a halt asks to
    /// flatten. `None`: a flatten request is logged as unfulfillable.
    pub position_source: Option<Arc<dyn crate::flatten::PositionSource>>,
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
/// Delegates to `apexes_backtest::unified_loop::run_unified_event_loop`
/// (the same core `run_backtest`/`run_paper_trading` drive) with:
/// - `executor`: `OrderManagerExecutor` wrapping the SAME
///   `Arc<tokio::sync::Mutex<OrderManager>>` shared with
///   `OrderManagerChildOrderSink`'s `PostgresEms` -- direct-strategy and
///   EMS-driven child orders both go through the one real order
///   manager.
/// - `liquidation_policy`: `LiveLiquidationPolicy` -- margin-breach
///   liquidations NEVER auto-execute regardless of configuration (real
///   liquidation is the exchange's own responsibility); extreme-market
///   de-risking follows `config.auto_derisk_live`.
/// - `supplemental_fills`: `LiveSupplementalFillSource`, merging the
///   push-based `exchange_updates` stream with a
///   `config.reconcile_interval` polling timer into the one fill
///   source independent of the primary market-event stream that the
///   unified loop's second `tokio::select!` arm consumes.
/// - `clock`: `BacktestClock::new(ReplaySpeed::AsFastAsPossible)` --
///   live events already arrive at real-time pace and need no
///   additional artificial pacing.
/// - `latency_model`: `None` -- live trading has no artificial
///   submission-delay concept (unlike paper trading's simulated
///   network latency).
/// - `stop_signal`: `None` -- `stop_signal` is instead used to gate
///   `market_events` itself via `take_until`, exactly the same
///   workaround `run_paper_trading` uses and documents at its own
///   call site: `run_unified_event_loop` only observes its own
///   `stop_signal` parameter at the top of each loop iteration
///   (BEFORE entering that iteration's `tokio::select!`), so a signal
///   arriving while both `select!` arms are still pending would
///   otherwise never be observed until one of them happened to
///   resolve on its own -- a real hang risk here, since a live
///   `MarketEventBus` never ends its stream on its own and
///   `LiveSupplementalFillSource`'s merged stream may also sit
///   pending for a long `reconcile_interval`.
pub async fn run_live_trading(
    config: LiveTradingConfig,
    registry: &Registry,
    strategy: impl Strategy,
    order_manager: OrderManager,
    exchange_updates: impl Stream<Item = ExchangeUpdate> + Unpin + Send,
    stop_signal: oneshot::Receiver<()>,
) -> Result<LiveTradingReport, ApexesError> {
    if config.reconcile_interval.is_zero() {
        return Err(ApexesError::InvalidBacktestConfig {
            reason: "reconcile_interval must be non-zero".to_string(),
        });
    }

    let risk_checker = registry
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
    // expected type): every critical section the executor/sink runs
    // awaits `OrderManager`'s async methods while holding the guard, so
    // a `std::sync::Mutex` would be wrong here.
    let order_manager = Arc::new(tokio::sync::Mutex::new(order_manager));
    let child_order_sink: Arc<dyn apexes_ems::ChildOrderSink> =
        Arc::new(OrderManagerChildOrderSink::new(Arc::clone(&order_manager)));
    let ems: Arc<dyn Ems> = PostgresEms::new_with_kill_switch(
        &config.ems_postgres_url,
        child_order_sink,
        venue_scorer,
        HashMap::new(),
        volume_curve_provider,
        config.kill_switch.clone(),
    )
    .await
    .map_err(|error| ApexesError::InvalidBacktestConfig {
        reason: format!("failed to construct PostgresEms: {error}"),
    })?;

    // `run_unified_event_loop` polls its own `stop_signal` parameter
    // only at the very top of each iteration -- BEFORE entering that
    // iteration's `tokio::select!` -- so a signal arriving while the
    // loop is already blocked inside `select!` (both the market-event
    // and supplemental-fill arms genuinely never-ending in live
    // trading: a live `MarketEventBus` holds its broadcast sender open
    // for its whole connected lifetime, and `LiveSupplementalFillSource`
    // merges a real exchange push stream with a `reconcile_interval`
    // timer, neither of which resolves on a fixed short cadence) would
    // never be observed until one of those arms happens to resolve on
    // its own. `run_paper_trading` hit this same hazard and works
    // around it by gating its own event stream with `take_until`
    // instead of passing `stop_signal` through to the loop directly --
    // mirrored here exactly: `market_events` (this loop's `select!`
    // arm most likely to be the one actually driving forward progress)
    // is gated with `take_until(stop_signal)`, and `None` is passed as
    // `run_unified_event_loop`'s own `stop_signal` parameter.
    let activity_clock = Arc::new(crate::halt::ActivityClock::for_instruments(
        &config.instruments,
    ));
    let mut halt_tasks = Vec::new();
    if let Some(kill_switch) = &config.kill_switch {
        let flatten = config
            .position_source
            .as_ref()
            .map(|source| crate::flatten::FlattenSetup {
                source: Arc::clone(source),
                instruments: Arc::new(
                    config
                        .instruments
                        .iter()
                        .map(|i| (i.symbol.clone(), i.clone()))
                        .collect(),
                ),
                config: crate::flatten::FlattenConfig::default(),
            });
        halt_tasks.push(crate::halt::spawn_cancel_all_task(
            Arc::clone(kill_switch),
            Arc::clone(&order_manager),
            flatten,
        ));
        if let Some(stale_after) = config.stale_data_timeout {
            halt_tasks.push(crate::halt::spawn_staleness_watchdog(
                Arc::clone(kill_switch),
                Arc::clone(&activity_clock),
                stale_after,
            ));
        }
    } else if config.stale_data_timeout.is_some() {
        return Err(ApexesError::InvalidBacktestConfig {
            reason: "stale_data_timeout requires a kill_switch".to_string(),
        });
    }

    let market_events =
        subscribe_all_venues(registry, &config.instruments, config.kill_switch.clone())
            .await?
            .inspect({
                let activity_clock = Arc::clone(&activity_clock);
                let kill_switch = config.kill_switch.clone();
                let stale_after = config.stale_data_timeout;
                move |event| {
                    // Check before refreshing, so the first event after a long
                    // stall cannot conceal the outage before the strategy runs.
                    if let (Some(ks), Some(limit)) = (&kill_switch, stale_after) {
                        activity_clock.enforce(ks, limit);
                    }
                    if let Some(ks) = &kill_switch
                        && matches!(event, MarketEvent::Depth(depth) if depth.is_invalidation())
                    {
                        ks.engage("market data order book invalidated");
                    }
                    activity_clock.observe(event);
                    if let Some(ks) = &kill_switch
                        && ks.phase() == Some(apexes_oms::SessionPhase::Reconciling)
                        && activity_clock.all_observed()
                    {
                        ks.startup_ready();
                    }
                }
            })
            .take_until(Box::pin(async move {
                let _ = stop_signal.await;
            }));

    // Once `exchange_updates` ends (returns `None`), chain in a
    // `pending()` stream that never resolves, so `LiveSupplementalFillSource`'s
    // merged-stream loop stops being re-polled every iteration --
    // treating "the stream permanently ended" as "this branch is now
    // permanently inert" rather than "nothing ready yet, try again
    // immediately", which would otherwise busy-loop for as long as
    // `market_events` keeps the outer loop alive afterward. The real
    // Binance User Data Stream never ends on its own (its socket task
    // retries forever), so this only matters for a finite/synthetic
    // stream (e.g. a test's `futures::stream::empty()`), but the fix
    // costs nothing in the common case.
    let exchange_updates = exchange_updates.chain(futures::stream::pending());

    let executor: Arc<dyn OrderExecutor> =
        Arc::new(OrderManagerExecutor::new(Arc::clone(&order_manager)));
    let liquidation_policy = Arc::new(LiveLiquidationPolicy::new(config.auto_derisk_live));
    let mut supplemental_fills = LiveSupplementalFillSource::new(
        Arc::clone(&order_manager),
        exchange_updates,
        config.reconcile_interval,
    );
    if let Some(resync) = config.resync.clone() {
        supplemental_fills = supplemental_fills.with_resync(resync);
    }

    // `funding_rate_bps` is read off the `CostModel` BEFORE the config
    // struct is otherwise consumed: the unified loop needs the rate for
    // its own funding-settlement pass (see
    // `UnifiedLoopConfig::funding_rate_bps`'s doc comment), while the
    // full `CostModel` -- fee schedule included -- has no live-trading
    // use (live trading never simulates fees itself; real fees arrive
    // embedded in the exchange's own fill data).
    let funding_rate_bps = config.cost_model.funding_rate_bps;
    let clock = match registry
        .try_one::<apexes_backtest::clock::LiveObservationPolicy>()
        .map_err(|error| ApexesError::InvalidBacktestConfig {
            reason: format!("registry lookup for LiveObservationPolicy failed: {error}"),
        })? {
        Some(policy) => BacktestClock::live_observations(policy),
        None => BacktestClock::new(ReplaySpeed::AsFastAsPossible),
    };

    let unified_loop_config = UnifiedLoopConfig {
        instruments: config.instruments,
        initial_balances: config.initial_balances,
        initial_positions: config.initial_positions,
        funding_rate_bps,
        risk_free_return: config.risk_free_return,
        risk_checker,
        margin_tiers: config.margin_tiers,
        market_condition_config: config.market_condition_config,
        control: config.control,
        initial_prices: config.initial_prices,
        symbol_rules: config.symbol_rules,
    };

    let report = run_unified_event_loop(
        unified_loop_config,
        market_events,
        clock,
        executor,
        liquidation_policy,
        supplemental_fills,
        ems,
        strategy,
        None,
        None,
    )
    .await?;

    // `UnifiedLoopReport` (a `BacktestReport` alias) carries
    // `overall: SerializablePerformanceSummary` (a serde-derived
    // mirror of `PerformanceSummary`, per that type's own doc comment
    // in `apexes-backtest`'s `report.rs`) plus `per_symbol`/`per_venue`/
    // `equity_curve` breakdowns `LiveTradingReport` has never exposed.
    // Mapped back to this crate's own pre-existing `LiveTradingReport`
    // shape field-by-field (no `From` impl exists in either direction,
    // and adding one to `apexes-backtest` would be out of this task's
    // file scope) -- `LiveTradingReport`'s public shape is UNCHANGED by
    // this refactor.
    Ok(LiveTradingReport {
        overall: PerformanceSummary {
            total_pnl: report.overall.total_pnl,
            sharpe: report.overall.sharpe,
            sortino: report.overall.sortino,
            calmar: report.overall.calmar,
            max_drawdown: report.overall.max_drawdown,
            win_rate: report.overall.win_rate,
            profit_factor: report.overall.profit_factor,
            turnover: report.overall.turnover,
            trade_count: report.overall.trade_count,
        },
        trades: report.trades,
    })
}

/// Groups `instruments` by `Venue`, looks up the one registered
/// `Arc<MarketEventBus>` for each distinct venue involved
/// (`registry.get::<Arc<MarketEventBus>, Venue>()`), subscribes each
/// bus to its own subset of `instruments`, adapts each resulting
/// `broadcast::Receiver` back into a `Stream<Item = MarketEvent>`
/// via `receiver_into_stream`, and merges all the resulting streams
/// into one via `futures::stream::select_all` -- so the rest of
/// `run_live_trading` sees a single unified `MarketEvent` stream
/// regardless of how many distinct venues `config.instruments`
/// actually spans. Returns `Err(ApexesError::SubscriptionFailed)` if
/// any involved venue has no registered bus.
async fn subscribe_all_venues(
    registry: &Registry,
    instruments: &[Instrument],
    kill_switch: Option<Arc<apexes_oms::KillSwitch>>,
) -> Result<futures::stream::SelectAll<futures::stream::BoxStream<'static, MarketEvent>>, ApexesError>
{
    let buses = registry.get::<Arc<MarketEventBus>, Venue>();

    let mut instruments_by_venue: HashMap<Venue, Vec<Instrument>> = HashMap::new();
    for instrument in instruments {
        instruments_by_venue
            .entry(instrument.venue)
            .or_default()
            .push(instrument.clone());
    }

    let mut streams = Vec::new();
    for (venue, venue_instruments) in instruments_by_venue {
        let bus = buses
            .get(&venue)
            .ok_or_else(|| ApexesError::SubscriptionFailed {
                symbol: venue_instruments
                    .first()
                    .map(|i| i.symbol.clone())
                    .unwrap_or_default(),
                venue,
                reason: "no MarketEventBus registered for this venue".to_string(),
                source_detail: None,
            })?;
        let receiver = bus.subscribe(&venue_instruments).await?;
        let kill_switch = kill_switch.clone();
        let stream = futures::stream::unfold(
            (receiver, kill_switch),
            move |(mut receiver, kill_switch)| async move {
                loop {
                    match receiver.recv().await {
                        Ok(event) => return Some((event, (receiver, kill_switch))),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::error!(
                                ?venue,
                                skipped,
                                "live market data consumer lost events"
                            );
                            if let Some(ks) = &kill_switch {
                                ks.engage(format!(
                                    "market data gap: {venue:?}, {skipped} events lost"
                                ));
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            if let Some(ks) = &kill_switch {
                                ks.engage(format!("market data stream closed: {venue:?}"));
                            }
                            return None;
                        }
                    }
                }
            },
        );
        streams.push(Box::pin(stream) as futures::stream::BoxStream<'static, MarketEvent>);
    }

    Ok(futures::stream::select_all(streams))
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, MarketType};
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

    #[test]
    fn btc_usdt_instrument_helper_builds_a_valid_instrument() {
        // A minimal smoke test that this module's own test-support
        // helper (reused by `run_live_trading`'s config construction
        // in doc examples/integration tests) still produces a valid
        // `Instrument` -- the substantive per-tick logic this file used
        // to own directly (funding boundaries, price-map maintenance,
        // fill application) now lives in and is tested by
        // `apexes_backtest::unified_loop`, which every caller
        // (including this one) shares.
        let instrument = btc_usdt_instrument(AssetClass::Perpetual);
        assert_eq!(instrument.symbol, "BTCUSDT");
        assert_eq!(instrument.venue, Venue::Binance);
    }
}
