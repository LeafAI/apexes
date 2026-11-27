//! F03.04 — repeatable, nontrivial mixed-event policy profiles.
use apexes_backtest::{
    BacktestConfig, BacktestReport, CostModel, DepthSequence, EventLatencyPolicy, FlatFeeSchedule,
    InMemoryMarketDataSource, ReplaySpeed, SimulatedDepthPolicy, SimulatedExecution,
    historical_dataset::{DatasetDefinition, DatasetStream, StreamKind, seal_dataset},
    replay_identity::{ReplayIdentityPolicy, canonical_report_sha256},
    run_backtest,
};
use apexes_core::{
    AssetClass, Bar, BarInterval, DepthUpdate, Instrument, MarginMode, MarkPriceUpdate,
    MarketEvent, MarketType, OrderType, PriceLevel, Side, SymbolRules, Tick, TradeEvent, Venue,
    registry::Registry,
};
use apexes_risk::{CompositeRiskChecker, LimitChecker, RiskLimits};
use apexes_strategy::{OrderIntent, OrderRouting, Strategy, StrategyContext};
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

const SYMBOL: &str = "BTCUSDT";

fn at(ms: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 7, 59, 50).unwrap() + TimeDelta::milliseconds(ms)
}

fn depth(ms: i64, sequence: u64, ask: Decimal) -> MarketEvent {
    MarketEvent::Depth(DepthUpdate {
        symbol: SYMBOL.into(),
        venue: Venue::Binance,
        bids: vec![PriceLevel {
            price: dec!(99),
            quantity: dec!(10),
        }],
        asks: vec![PriceLevel {
            price: ask,
            quantity: dec!(10),
        }],
        sequence: Some(sequence),
        source_range: None,
        is_snapshot: true,
        timestamp: at(ms),
    })
}

fn trade(ms: i64, price: Decimal, id: &str) -> MarketEvent {
    MarketEvent::Trade(TradeEvent {
        symbol: SYMBOL.into(),
        venue: Venue::Binance,
        price,
        quantity: dec!(2),
        side: Some(Side::Sell),
        trade_id: Some(id.into()),
        timestamp: at(ms),
    })
}

fn tick(ms: i64) -> MarketEvent {
    MarketEvent::Tick(Tick {
        symbol: SYMBOL.into(),
        venue: Venue::Binance,
        bid_price: dec!(99),
        bid_size: dec!(10),
        ask_price: dec!(100),
        ask_size: dec!(10),
        timestamp: at(ms),
    })
}

fn bar(ms: i64) -> MarketEvent {
    MarketEvent::Bar(Bar {
        symbol: SYMBOL.into(),
        venue: Venue::Binance,
        open: dec!(100),
        high: dec!(101),
        low: dec!(99),
        close: dec!(100),
        volume: dec!(10),
        interval: BarInterval::OneSecond,
        open_time: at(ms),
        close_time: at(ms + 1000),
    })
}

fn mark(ms: i64) -> MarketEvent {
    MarketEvent::MarkPrice(MarkPriceUpdate {
        symbol: SYMBOL.into(),
        venue: Venue::Binance,
        mark_price: dec!(100),
        timestamp: at(ms),
    })
}

fn configuration(asset: AssetClass, funding: Option<Decimal>) -> BacktestConfig {
    BacktestConfig {
        instruments: vec![
            Instrument::new(
                SYMBOL,
                MarketType::Crypto,
                Venue::Binance,
                asset,
                Some("BTC".into()),
                Some("USDT".into()),
                dec!(0.01),
                dec!(0.001),
                dec!(1),
            )
            .unwrap(),
        ],
        date_range: at(0).date_naive()..at(0).date_naive().succ_opt().unwrap(),
        initial_balances: HashMap::from([("USDT".into(), dec!(10000))]),
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(5),
                taker_bps: dec!(10),
            }),
            funding_rate_bps: funding,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: Decimal::ZERO,
        risk_free_return: Decimal::ZERO,
        margin_tiers: HashMap::new(),
        market_condition_config: None,
        control: None,
    }
}

fn registry(events: &[MarketEvent], profile: &str) -> Registry {
    let r = Registry::new();
    let manifest = seal_dataset(
        DatasetDefinition {
            dataset_id: profile.into(),
            version: "v1".into(),
            source_id: "synthetic mixed-event qualification".into(),
            start_day: at(0).date_naive(),
            end_day_exclusive: at(0).date_naive().succ_opt().unwrap(),
            streams: [
                StreamKind::Depth,
                StreamKind::Tick,
                StreamKind::Trade,
                StreamKind::Bar,
                StreamKind::MarkPrice,
            ]
            .into_iter()
            .map(|kind| DatasetStream {
                symbol: SYMBOL.into(),
                venue: Venue::Binance,
                kind,
            })
            .collect(),
        },
        events,
    )
    .unwrap();
    r.register::<_, ()>((), manifest).unwrap();
    r.register::<_, ()>(
        (),
        ReplayIdentityPolicy {
            namespace: uuid::Uuid::from_u128(64),
        },
    )
    .unwrap();
    r
}

fn intent(
    asset_class: AssetClass,
    side: Side,
    quantity: Decimal,
    price: Option<Decimal>,
) -> OrderIntent {
    OrderIntent {
        symbol: SYMBOL.into(),
        venue: Venue::Binance,
        asset_class,
        side,
        order_type: if price.is_some() {
            OrderType::Limit
        } else {
            OrderType::Market
        },
        quantity,
        price,
        stop_price: None,
        execution_algo: None,
        routing: OrderRouting::Fixed,
        leverage: None,
        margin_mode: None,
    }
}

struct Spot;
impl Strategy for Spot {
    fn on_market_event(&mut self, event: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
        match event {
            MarketEvent::Trade(t) if t.trade_id.as_deref() == Some("open") => vec![
                intent(AssetClass::Spot, Side::Buy, dec!(1), None),
                intent(AssetClass::Spot, Side::Buy, dec!(9), None), // refused by max quantity
            ],
            MarketEvent::Tick(t) if t.timestamp == at(2000) => {
                vec![intent(AssetClass::Spot, Side::Buy, dec!(1), Some(dec!(99)))]
            }
            _ => vec![],
        }
    }
}

async fn spot_run() -> BacktestReport {
    let events = vec![
        depth(0, 1, dec!(100)),
        trade(0, dec!(100), "open"),
        depth(500, 2, dec!(100)),
        tick(2000),
        depth(2500, 3, dec!(100)),
        trade(3000, dec!(99), "maker"),
        bar(4000),
        mark(5000),
    ];
    let r = registry(&events, "spot-qualification");
    r.register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
        .unwrap();
    r.register::<_, ()>(
        (),
        SimulatedDepthPolicy {
            sequence: DepthSequence::SnapshotOnly,
            max_age_ms: 3000,
        },
    )
    .unwrap();
    r.register::<_, ()>(
        (),
        SimulatedExecution::BoundedSpotIoc {
            rules: BTreeMap::from([(
                SYMBOL.into(),
                SymbolRules {
                    quantity_step: dec!(0.001),
                    min_quantity: dec!(0.001),
                    max_quantity: dec!(100),
                    price_tick: dec!(0.01),
                    min_notional: dec!(5),
                },
            )]),
            max_reference_age_ms: 1000,
        },
    )
    .unwrap();
    r.register::<Arc<CompositeRiskChecker>, ()>(
        (),
        Arc::new(
            CompositeRiskChecker::new().with_pre_trade_check(Box::new(LimitChecker::new(
                RiskLimits {
                    max_order_quantity: Some(dec!(2)),
                    ..Default::default()
                },
            ))),
        ),
    )
    .unwrap();
    run_backtest(
        configuration(AssetClass::Spot, None),
        &r,
        InMemoryMarketDataSource::new(events),
        Spot,
    )
    .await
    .unwrap()
}

struct Perpetual;
impl Strategy for Perpetual {
    fn on_market_event(&mut self, event: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
        let MarketEvent::Trade(t) = event else {
            return vec![];
        };
        let side = match t.trade_id.as_deref() {
            Some("open") => Side::Buy,
            Some("close") => Side::Sell,
            _ => return vec![],
        };
        let mut i = intent(AssetClass::Perpetual, side, dec!(1), None);
        i.leverage = Some(dec!(10));
        i.margin_mode = Some(MarginMode::Isolated);
        vec![i]
    }
}

async fn perpetual_run(funding: Option<Decimal>) -> BacktestReport {
    let events = vec![
        depth(0, 1, dec!(100)),
        trade(0, dec!(100), "open"),
        depth(500, 2, dec!(100)),
        tick(1000),
        bar(2000),
        mark(10000),
        depth(11000, 3, dec!(100)),
        trade(11000, dec!(99), "close"),
        tick(11500),
    ];
    let r = registry(&events, "perpetual-qualification");
    r.register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
        .unwrap();
    run_backtest(
        configuration(AssetClass::Perpetual, funding),
        &r,
        InMemoryMarketDataSource::new(events),
        Perpetual,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn mixed_spot_replay_repeats_latency_maker_taker_and_risk_refusal() {
    let mut expected = None;
    for _ in 0..10 {
        let report = spot_run().await;
        assert_eq!(report.trades.len(), 2);
        assert_eq!(report.trades[0].filled_at, at(500));
        assert_eq!(report.trades[0].fee, dec!(0.1));
        assert_eq!(report.trades[1].filled_at, at(3000));
        assert_eq!(report.trades[1].fee, dec!(0.0495));
        assert_eq!(report.trades[1].price, dec!(99));
        let latency = report.event_latency.as_ref().unwrap();
        assert_eq!(latency.regressing_events, 0);
        assert_eq!(
            (latency.scheduled, latency.arrived, latency.expired_at_end),
            (2, 2, 0)
        );
        let hash = canonical_report_sha256(&report).unwrap();
        if let Some(previous) = &expected {
            assert_eq!(&hash, previous);
        }
        expected = Some(hash);
    }
}

#[tokio::test]
async fn mixed_perpetual_replay_repeats_funding_and_fees() {
    let mut expected = None;
    for _ in 0..10 {
        let report = perpetual_run(Some(dec!(10))).await;
        assert_eq!(report.trades.len(), 2);
        assert_eq!(report.trades[0].filled_at, at(500));
        assert_eq!(report.trades[1].filled_at, at(11500));
        assert_eq!(report.trades[0].fee, dec!(0.1));
        assert_eq!(report.trades[1].fee, dec!(0.099));
        assert_eq!(report.equity_curve.last().unwrap().1, dec!(9998.701));
        use apexes_backtest::execution_performance::SymbolExecutionPerformance;
        let execution = report.execution_performance.as_ref().unwrap();
        let SymbolExecutionPerformance::Available(summary) = &execution.per_symbol[SYMBOL] else {
            panic!("expected realized metrics");
        };
        assert_eq!(summary.gross_realized, dec!(-1));
        assert_eq!(summary.net_realized, dec!(-1.199));
        assert_eq!(summary.fees_paid, dec!(0.199));
        assert_eq!(summary.turnover, dec!(199));
        assert_eq!(summary.expectancy_per_close, Some(dec!(-1.199)));
        assert_eq!(summary.losses, 1);
        assert_eq!(summary.profit_factor, Some(Decimal::ZERO));
        assert_eq!(summary.open_quantity, Decimal::ZERO);
        let mut legacy = serde_json::to_value(&report).unwrap();
        legacy
            .as_object_mut()
            .unwrap()
            .remove("execution_performance");
        let legacy: BacktestReport = serde_json::from_value(legacy).unwrap();
        assert!(legacy.execution_performance.is_none());

        let hash = canonical_report_sha256(&report).unwrap();
        if let Some(previous) = &expected {
            assert_eq!(&hash, previous);
        }
        expected = Some(hash);
    }
    let no_funding = perpetual_run(None).await;
    assert_eq!(no_funding.equity_curve.last().unwrap().1, dec!(9998.801));
    assert_ne!(
        Some(canonical_report_sha256(&no_funding).unwrap()),
        expected
    );
}

#[tokio::test]
async fn deterministic_bar_replay_rejects_future_data_and_regressing_observation_time() {
    struct NeverCalled;
    impl Strategy for NeverCalled {
        fn on_market_event(&mut self, _: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
            panic!("invalid replay must fail before invoking the strategy");
        }
    }
    let events = vec![
        depth(0, 1, dec!(100)),
        trade(0, dec!(100), "open"),
        bar(1000),
        tick(1500),
        mark(2000),
    ];
    let r = registry(&events, "overlapping-bar");
    r.register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
        .unwrap();
    let result = run_backtest(
        configuration(AssetClass::Spot, None),
        &r,
        InMemoryMarketDataSource::new(events.clone()),
        NeverCalled,
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("causal observation time regressed")
    );
    let r = registry(&events, "no-latency-bar");
    let result = run_backtest(
        configuration(AssetClass::Spot, None),
        &r,
        InMemoryMarketDataSource::new(events),
        NeverCalled,
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("bars requires explicit event-time latency")
    );
}
