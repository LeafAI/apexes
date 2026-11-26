//! [`run_paper_trading`]: the `apexes-paper` counterpart to
//! `apexes_backtest::run_backtest` -- drives the same shared event-loop
//! core (`apexes_backtest::unified_loop::run_unified_event_loop`)
//! against a live `MarketDataProvider::subscribe()` stream instead of
//! pre-loaded historical data.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use apexes_backtest::clock::BacktestClock;
use apexes_backtest::ems_adapter::MatchingEngineChildOrderSink;
use apexes_backtest::order_executor_adapter::{
    BacktestLiquidationPolicy, MatchingEngineOrderExecutor, NeverFillSupplementalFillSource,
};
use apexes_backtest::unified_loop::{UnifiedLoopConfig, run_unified_event_loop};
use apexes_backtest::{BacktestReport, CostModel, LatencyModel, MatchingEngine, ReplaySpeed};
use apexes_core::registry::Registry;
use apexes_core::{ApexesError, Instrument};
use apexes_ems::Ems;
use apexes_message_bus::{MarketEventBus, receiver_into_stream};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{MarginTierTable, OrderExecutor, Strategy, VenueScorer};
use futures::StreamExt;
use rust_decimal::Decimal;
use tokio::sync::oneshot;

pub struct PaperTradingConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub queue_position_factor: Decimal,
    pub risk_free_return: Decimal,
    pub latency_model: LatencyModel,
    /// Per-symbol tiered maintenance-margin tables driving margin
    /// monitoring and auto-liquidation. Empty map (the default for
    /// every pre-existing spot-only caller) is a silent no-op.
    pub margin_tiers: HashMap<String, MarginTierTable>,
    /// Extreme-market circuit breaker configuration. `None` (default)
    /// disables it entirely.
    pub market_condition_config: Option<apexes_risk::MarketConditionConfig>,
    /// Live-tuning/monitoring control channel, threaded straight
    /// through to `UnifiedLoopConfig.control`. `None` (the default for
    /// every pre-existing caller) is a silent no-op -- identical to
    /// this crate's behavior before this field existed.
    pub control: Option<apexes_backtest::ControlHandle>,
}

/// Runs paper trading: subscribes to `bus` for `config.instruments`,
/// drives the shared event-loop core against the resulting live stream,
/// applying `config.latency_model`'s delay before each `OrderIntent`
/// that passes its `PreTradeCheck` reaches the matching engine. Exits
/// gracefully (returning the `BacktestReport` accumulated so far, not an
/// error) once `stop_signal` fires or its sender is dropped -- the event
/// stream itself is explicitly gated on `stop_signal` via `take_until`
/// (see the comment at its construction site below) rather than relying
/// on the stream ending on its own, since `MarketEventBus` holds its
/// broadcast sender open for its whole connected lifetime and therefore
/// does not end its stream merely because the real upstream provider's
/// own connection dropped -- a provider-level disconnect during paper
/// trading is currently only visible via a log line from
/// `MarketEventBus`'s internal forwarding task, not by this function
/// returning on its own.
///
/// `registry` (sub-project #9's registry retrofit) is used to resolve
/// an optional `CompositeRiskChecker` via
/// `registry.try_one::<Arc<CompositeRiskChecker>>()` -- absent means no
/// risk checking is performed, preserving this crate's pre-existing
/// `None`-means-no-risk-checking behavior exactly. `bus` stays a
/// direct parameter (NOT a registry lookup) -- paper trading only ever
/// needs one directly-supplied event bus.
pub async fn run_paper_trading(
    mut config: PaperTradingConfig,
    registry: &Registry,
    bus: &MarketEventBus,
    strategy: impl Strategy,
    stop_signal: oneshot::Receiver<()>,
) -> Result<BacktestReport, ApexesError> {
    let execution =
        apexes_backtest::SimulatedExecution::from_registry(registry, &config.instruments)?;
    let simulated_fees = config.cost_model.freeze_fees(&config.instruments)?;
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

    // `run_unified_event_loop` polls its own stop signal only at the top
    // of each iteration, so a signal arriving while the loop is blocked on
    // the never-ending bus stream would never be observed (the loop would
    // hang after the mock provider's finite events are drained). Gating the
    // stream with `take_until` instead makes the stop signal end the stream
    // -- the loop's own exit path -- preserving the same "fires or its
    // sender is dropped" semantics `run_unified_event_loop` documents for
    // its own `stop_signal` parameter (a dropped oneshot sender resolves
    // the await).
    let event_stream = receiver_into_stream(bus.subscribe(&config.instruments).await?).take_until(
        Box::pin(async move {
            let _ = stop_signal.await;
        }),
    );
    let clock = BacktestClock::new(ReplaySpeed::AsFastAsPossible);

    // `funding_rate_bps` is read off the `CostModel` BEFORE it is moved
    // into the `MatchingEngine`: the unified loop needs the rate for its
    // own funding-settlement pass (see
    // `UnifiedLoopConfig::funding_rate_bps`), while the full `CostModel`
    // -- fee schedule included -- belongs to the matching engine that
    // computes per-fill fees.
    let funding_rate_bps = config.cost_model.funding_rate_bps;
    // ONE shared `MatchingEngine` instance backs both the executor
    // (direct strategy orders) and the EMS child-order sink (algo
    // orders), so both interact through a single simulated order book --
    // the same wiring `run_backtest` performs.
    let matching_engine = Arc::new(Mutex::new(
        MatchingEngine::new(config.cost_model, config.queue_position_factor)
            .with_execution(execution.clone()),
    ));
    let executor: Arc<dyn OrderExecutor> = Arc::new(MatchingEngineOrderExecutor::new(Arc::clone(
        &matching_engine,
    )));
    let child_order_sink: Arc<dyn apexes_ems::ChildOrderSink> = Arc::new(
        MatchingEngineChildOrderSink::new(Arc::clone(&matching_engine)),
    );
    let ems: Arc<dyn Ems> = Arc::new(apexes_ems::InMemoryEms::new(
        child_order_sink,
        venue_scorer,
        HashMap::new(),
        volume_curve_provider,
    ));

    let unified_loop_config = UnifiedLoopConfig {
        instruments: config.instruments,
        initial_balances: config.initial_balances,
        initial_positions: HashMap::new(),
        funding_rate_bps,
        risk_free_return: config.risk_free_return,
        risk_checker,
        margin_tiers: config.margin_tiers,
        market_condition_config: config.market_condition_config,
        control: config.control,
        initial_prices: HashMap::new(),
        symbol_rules: HashMap::new(),
    };

    let mut report = run_unified_event_loop(
        unified_loop_config,
        event_stream,
        clock,
        executor,
        Arc::new(BacktestLiquidationPolicy),
        NeverFillSupplementalFillSource,
        ems,
        strategy,
        None,
        Some(config.latency_model),
    )
    .await?;
    report.simulated_fees = Some(simulated_fees);
    report.simulated_execution = Some(execution);
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_backtest::cost_model::FlatFeeSchedule;
    use apexes_backtest::{OrderIntent, OrderRouting, Strategy, StrategyContext};
    use apexes_core::{
        AssetClass, DepthUpdate, MarketDataProvider, MarketEvent, MarketType, OrderType,
        PriceLevel, Side, TradeEvent, Venue,
    };
    use apexes_risk::{CompositeRiskChecker, ListMode, SymbolVenueList};
    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};
    use futures::stream::BoxStream;
    use rust_decimal_macros::dec;
    use std::collections::HashSet;
    use std::time::Duration;

    struct MockMarketDataProvider {
        events: Vec<MarketEvent>,
    }

    #[async_trait]
    impl MarketDataProvider for MockMarketDataProvider {
        async fn subscribe(
            &self,
            _instruments: &[Instrument],
        ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
            Ok(Box::pin(futures::stream::iter(self.events.clone())))
        }
    }

    fn btc_usdt_instrument() -> Instrument {
        Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            Some("BTC".to_string()),
            Some("USDT".to_string()),
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap()
    }

    fn zero_cost_model() -> CostModel {
        CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        }
    }

    fn sample_events() -> Vec<MarketEvent> {
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        vec![
            MarketEvent::Depth(DepthUpdate {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                bids: vec![],
                asks: vec![PriceLevel {
                    price: dec!(100),
                    quantity: dec!(1000),
                }],
                sequence: Some(1),
                is_snapshot: true,
                timestamp: t0,
            }),
            MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price: dec!(100),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: t0 + chrono::Duration::seconds(1),
            }),
        ]
    }

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
                    venue: Venue::Binance,
                    asset_class: AssetClass::Spot,
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
            vec![]
        }
    }

    fn base_config() -> PaperTradingConfig {
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(100000));
        PaperTradingConfig {
            instruments: vec![btc_usdt_instrument()],
            initial_balances,
            cost_model: zero_cost_model(),
            queue_position_factor: dec!(0.0),
            risk_free_return: dec!(0),
            latency_model: LatencyModel::None,
            margin_tiers: HashMap::new(),
            market_condition_config: None,
            control: None,
        }
    }

    #[tokio::test]
    async fn bounded_ioc_policy_applies_to_paper_and_is_reported() {
        let mut events = sample_events();
        if let MarketEvent::Depth(depth) = &mut events[0] {
            depth.asks[0].price = dec!(100.01);
        }
        let bus = MarketEventBus::new(Box::new(MockMarketDataProvider { events }));
        let registry = Registry::new();
        let policy = apexes_backtest::SimulatedExecution::BoundedSpotIoc {
            rules: std::collections::BTreeMap::from([(
                "BTCUSDT".into(),
                apexes_core::SymbolRules {
                    quantity_step: dec!(0.001),
                    min_quantity: dec!(0.001),
                    max_quantity: dec!(100),
                    price_tick: dec!(0.01),
                    min_notional: dec!(5),
                },
            )]),
            max_reference_age_ms: 1000,
        };
        registry
            .register::<apexes_backtest::SimulatedExecution, ()>((), policy.clone())
            .unwrap();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx.send(());
        });
        let report = run_paper_trading(
            base_config(),
            &registry,
            &bus,
            BuyOnceStrategy { bought: false },
            rx,
        )
        .await
        .unwrap();
        assert!(report.trades.is_empty());
        assert_eq!(report.simulated_execution, Some(policy));
        assert!(
            report
                .equity_curve
                .iter()
                .all(|(_, equity)| *equity == dec!(100000))
        );
    }

    #[tokio::test]
    async fn strategy_orders_are_filled_via_the_shared_matching_engine() {
        let provider = MockMarketDataProvider {
            events: sample_events(),
        };
        let bus = MarketEventBus::new(Box::new(provider));
        let registry = Registry::new();
        // MarketEventBus's broadcast stream does not end on its own even after the
        // mock provider's underlying events are exhausted, because the bus holds
        // its sender open for its whole lifetime -- so the loop needs the stop
        // signal to exit.
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx.send(());
        });

        let report = run_paper_trading(
            base_config(),
            &registry,
            &bus,
            BuyOnceStrategy { bought: false },
            rx,
        )
        .await
        .unwrap();

        assert_eq!(report.trades.len(), 1);
    }

    #[tokio::test]
    async fn configured_taker_fee_changes_paper_equity_and_report() {
        let bus = MarketEventBus::new(Box::new(MockMarketDataProvider {
            events: sample_events(),
        }));
        let mut config = base_config();
        config.cost_model = apexes_backtest::SimulatedFees::Flat {
            maker_bps: dec!(5),
            taker_bps: dec!(20),
        }
        .cost_model();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx.send(());
        });
        let report = run_paper_trading(
            config,
            &Registry::new(),
            &bus,
            BuyOnceStrategy { bought: false },
            rx,
        )
        .await
        .unwrap();
        assert_eq!(report.trades.len(), 1);
        assert_eq!(report.trades[0].fee, dec!(0.2));
        assert_eq!(report.equity_curve.last().unwrap().1, dec!(99999.8));
        let fees = report.simulated_fees.unwrap();
        assert_eq!(fees.rates_by_venue["Binance"].taker_bps, dec!(20));
        assert_eq!(fees.label(), "Modeled fees");
    }

    #[tokio::test]
    async fn latency_model_fixed_delays_order_processing() {
        let provider = MockMarketDataProvider {
            events: sample_events(),
        };
        let bus = MarketEventBus::new(Box::new(provider));
        let mut config = base_config();
        config.latency_model = LatencyModel::Fixed(Duration::from_millis(100));
        let registry = Registry::new();
        // MarketEventBus's broadcast stream does not end on its own even after the
        // mock provider's underlying events are exhausted, because the bus holds
        // its sender open for its whole lifetime -- so the loop needs the stop
        // signal to exit. 300ms keeps the signal well past the 100ms fill latency.
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = tx.send(());
        });

        let start = tokio::time::Instant::now();
        let report = run_paper_trading(
            config,
            &registry,
            &bus,
            BuyOnceStrategy { bought: false },
            rx,
        )
        .await
        .unwrap();

        assert_eq!(report.trades.len(), 1);
        assert!(start.elapsed() >= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn risk_checker_blacklist_prevents_the_order_from_being_filled() {
        let provider = MockMarketDataProvider {
            events: sample_events(),
        };
        let bus = MarketEventBus::new(Box::new(provider));
        let mut blacklist = HashSet::new();
        blacklist.insert("BTCUSDT".to_string());
        let risk_checker = CompositeRiskChecker::new().with_pre_trade_check(Box::new(
            SymbolVenueList::new(ListMode::Blacklist(blacklist), ListMode::Disabled),
        ));
        let registry = Registry::new();
        registry
            .register::<Arc<CompositeRiskChecker>, ()>((), Arc::new(risk_checker))
            .unwrap();
        let config = base_config();
        // MarketEventBus's broadcast stream does not end on its own even after the
        // mock provider's underlying events are exhausted, because the bus holds
        // its sender open for its whole lifetime -- so the loop needs the stop
        // signal to exit.
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx.send(());
        });

        let report = run_paper_trading(
            config,
            &registry,
            &bus,
            BuyOnceStrategy { bought: false },
            rx,
        )
        .await
        .unwrap();

        assert!(report.trades.is_empty());
    }

    #[tokio::test]
    async fn stop_signal_causes_graceful_early_exit_with_partial_results() {
        // An event stream long enough that firing the stop signal
        // immediately (before any events are consumed) reliably beats
        // the mock provider's synchronous stream delivery, proving the
        // loop checks the signal rather than always draining the stream.
        let mut many_events = Vec::new();
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        for i in 0..1000 {
            many_events.push(MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price: dec!(100),
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: t0 + chrono::Duration::milliseconds(i),
            }));
        }
        let provider = MockMarketDataProvider {
            events: many_events,
        };
        let bus = MarketEventBus::new(Box::new(provider));
        let (tx, rx) = oneshot::channel();
        tx.send(()).unwrap();
        let registry = Registry::new();

        let report = run_paper_trading(
            base_config(),
            &registry,
            &bus,
            BuyOnceStrategy { bought: false },
            rx,
        )
        .await
        .unwrap();

        // The loop's very first iteration checks the stop signal before
        // consuming any event, so the run produces no trades regardless
        // of how many events the mock provider could have delivered.
        assert!(report.trades.is_empty());
    }

    #[tokio::test]
    async fn subscribe_through_a_shared_bus_calls_the_upstream_provider_once() {
        // Mirrors apexes-market-data's equivalent test: proves the
        // integration point this task changed (bus.subscribe(...) inside
        // run_paper_trading, from Step 2) shares one upstream connection
        // across multiple subscribe() calls with the same instrument set.
        let mock = apexes_message_bus::test_support::MockMarketDataProvider::new();
        let handle = mock.handle();
        let bus = MarketEventBus::new(Box::new(mock));
        let instruments = vec![sample_instrument_for_bus_test()];

        let _receiver_1 = bus.subscribe(&instruments).await.unwrap();
        let _receiver_2 = bus.subscribe(&instruments).await.unwrap();

        assert_eq!(
            handle.subscribe_call_count(),
            1,
            "two subscribe() calls with the same instrument set must share one upstream connection"
        );
    }

    #[tokio::test]
    async fn two_concurrent_run_paper_trading_calls_share_one_upstream_connection() {
        // Stronger than `subscribe_through_a_shared_bus_calls_the_upstream_provider_once`
        // above: that test calls `bus.subscribe()` directly rather than
        // going through `run_paper_trading` itself. This test spawns TWO
        // real `run_paper_trading` invocations concurrently against the
        // same `&bus` and the same instrument set, and asserts the
        // upstream mock's `subscribe()` was still only called once --
        // genuinely exercising this consumer's own integration point
        // end to end, not just the bus in isolation. Unlike the
        // market-data crate's equivalent test, no real external service
        // (ClickHouse) blocks doing this here, since `run_paper_trading`
        // only needs an in-memory `Registry` and a mock provider.
        let mock = apexes_message_bus::test_support::MockMarketDataProvider::new();
        let handle = mock.handle();
        let bus = MarketEventBus::new(Box::new(mock));
        let registry = Registry::new();

        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx1.send(());
        });
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx2.send(());
        });

        let (report1, report2) = tokio::join!(
            run_paper_trading(
                base_config(),
                &registry,
                &bus,
                BuyOnceStrategy { bought: false },
                rx1,
            ),
            run_paper_trading(
                base_config(),
                &registry,
                &bus,
                BuyOnceStrategy { bought: false },
                rx2,
            ),
        );

        report1.unwrap();
        report2.unwrap();

        assert_eq!(
            handle.subscribe_call_count(),
            1,
            "two concurrent run_paper_trading calls against the same bus and instrument set \
             must share one upstream connection"
        );
    }

    fn sample_instrument_for_bus_test() -> Instrument {
        Instrument::new(
            "BTCUSDT",
            apexes_core::MarketType::Crypto,
            apexes_core::Venue::Binance,
            apexes_core::AssetClass::Spot,
            Some("BTC".to_string()),
            Some("USDT".to_string()),
            rust_decimal_macros::dec!(0.01),
            rust_decimal_macros::dec!(0.0001),
            rust_decimal_macros::dec!(1),
        )
        .unwrap()
    }
}
