//! [`run_paper_trading`]: the `apexes-paper` counterpart to
//! `apexes_backtest::run_backtest` -- drives the same shared event-loop
//! core (`apexes_backtest::event_loop::run_event_loop`) against a live
//! `MarketDataProvider::subscribe()` stream instead of pre-loaded
//! historical data.

use std::collections::HashMap;
use std::sync::Arc;

use apexes_backtest::clock::BacktestClock;
use apexes_backtest::event_loop::{EventLoopConfig, run_event_loop};
use apexes_backtest::{BacktestReport, CostModel, LatencyModel, ReplaySpeed};
use apexes_core::registry::Registry;
use apexes_core::{ApexesError, Instrument, MarketDataProvider};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{Strategy, VenueScorer};
use rust_decimal::Decimal;
use tokio::sync::oneshot;

pub struct PaperTradingConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub queue_position_factor: Decimal,
    pub risk_free_return: Decimal,
    pub latency_model: LatencyModel,
}

/// Runs paper trading: subscribes to `provider` for `config.instruments`,
/// drives the shared event-loop core against the resulting live stream,
/// applying `config.latency_model`'s delay before each `OrderIntent`
/// that passes its `PreTradeCheck` reaches the matching engine. Exits
/// gracefully (returning the `BacktestReport` accumulated so far, not an
/// error) once `stop_signal` fires or its sender is dropped.
///
/// `registry` (sub-project #9's registry retrofit) is used to resolve
/// an optional `CompositeRiskChecker` via
/// `registry.try_one::<Arc<CompositeRiskChecker>>()` -- absent means no
/// risk checking is performed, preserving this crate's pre-existing
/// `None`-means-no-risk-checking behavior exactly. `provider` stays a
/// direct parameter (NOT a registry lookup) -- paper trading only ever
/// needs one directly-supplied provider.
pub async fn run_paper_trading(
    config: PaperTradingConfig,
    registry: &Registry,
    provider: &dyn MarketDataProvider,
    strategy: impl Strategy,
    stop_signal: oneshot::Receiver<()>,
) -> Result<BacktestReport, ApexesError> {
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

    let event_stream = provider.subscribe(&config.instruments).await?;
    let clock = BacktestClock::new(ReplaySpeed::AsFastAsPossible);

    let event_loop_config = EventLoopConfig {
        instruments: config.instruments,
        initial_balances: config.initial_balances,
        cost_model: config.cost_model,
        queue_position_factor: config.queue_position_factor,
        risk_free_return: config.risk_free_return,
        risk_checker,
    };

    run_event_loop(
        event_loop_config,
        event_stream,
        clock,
        venue_scorer,
        volume_curve_provider,
        strategy,
        Some(stop_signal),
        Some(config.latency_model),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_backtest::cost_model::FlatFeeSchedule;
    use apexes_backtest::{OrderIntent, OrderRouting, Strategy, StrategyContext};
    use apexes_core::{
        AssetClass, DepthUpdate, MarketEvent, MarketType, OrderType, PriceLevel, Side, TradeEvent,
        Venue,
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
        }
    }

    #[tokio::test]
    async fn strategy_orders_are_filled_via_the_shared_matching_engine() {
        let provider = MockMarketDataProvider {
            events: sample_events(),
        };
        let registry = Registry::new();
        let (_tx, rx) = oneshot::channel();

        let report = run_paper_trading(
            base_config(),
            &registry,
            &provider,
            BuyOnceStrategy { bought: false },
            rx,
        )
        .await
        .unwrap();

        assert_eq!(report.trades.len(), 1);
    }

    #[tokio::test]
    async fn latency_model_fixed_delays_order_processing() {
        let provider = MockMarketDataProvider {
            events: sample_events(),
        };
        let mut config = base_config();
        config.latency_model = LatencyModel::Fixed(Duration::from_millis(100));
        let registry = Registry::new();
        let (_tx, rx) = oneshot::channel();

        let start = tokio::time::Instant::now();
        let report = run_paper_trading(
            config,
            &registry,
            &provider,
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
        let (_tx, rx) = oneshot::channel();

        let report = run_paper_trading(
            config,
            &registry,
            &provider,
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
        let (tx, rx) = oneshot::channel();
        tx.send(()).unwrap();
        let registry = Registry::new();

        let report = run_paper_trading(
            base_config(),
            &registry,
            &provider,
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
}
