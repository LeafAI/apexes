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
        use apexes_backtest::exit_attribution::ExitReason;
        let exits = summary.exit_attribution.as_ref().unwrap();
        assert_eq!(exits.closes.len(), 1);
        assert_eq!(exits.closes[0].reason, ExitReason::Strategy);
        assert_eq!(exits.closes[0].fill_id, report.trades[1].id);
        assert_eq!(exits.closes[0].allocated_entry_fee, dec!(0.1));
        assert_eq!(exits.closes[0].allocated_exit_fee, dec!(0.099));
        assert_eq!(
            exits.by_reason[&ExitReason::Strategy].net_realized,
            summary.net_realized
        );

        use apexes_backtest::funding_attribution::{FundingPriceSource, SymbolFunding};
        let funding = report.funding.as_ref().unwrap();
        assert_eq!(funding.rate_bps, Some(dec!(10)));
        assert_eq!(funding.observed_settlement_passes, 2);
        assert_eq!(funding.settlements.len(), 1);
        let settlement = &funding.settlements[0];
        assert_eq!(settlement.boundary, at(10000));
        assert_eq!(settlement.observed_at, at(10000));
        assert_eq!(settlement.movement.symbol, SYMBOL);
        assert_eq!(settlement.movement.position_quantity, dec!(1));
        assert_eq!(settlement.movement.price_source, FundingPriceSource::Mark);
        assert_eq!(settlement.movement.notional, dec!(100));
        assert_eq!(settlement.movement.balance_delta, dec!(-0.1));
        let SymbolFunding::Available(totals) = &funding.per_symbol[SYMBOL] else {
            panic!("missing funding totals")
        };
        assert_eq!(totals.quote_currency, "USDT");
        assert_eq!(totals.income, Decimal::ZERO);
        assert_eq!(totals.expenses, dec!(0.1));
        assert_eq!(totals.net_funding, dec!(-0.1));
        assert_eq!(totals.net_realized_plus_funding, Some(dec!(-1.299)));
        assert_eq!(
            report.overall.total_pnl,
            totals.net_realized_plus_funding.unwrap()
        );

        let mut legacy = serde_json::to_value(&report).unwrap();
        legacy
            .as_object_mut()
            .unwrap()
            .remove("execution_performance");
        legacy.as_object_mut().unwrap().remove("funding");
        let legacy: BacktestReport = serde_json::from_value(legacy).unwrap();
        assert!(legacy.execution_performance.is_none() && legacy.funding.is_none());

        let hash = canonical_report_sha256(&report).unwrap();
        if let Some(previous) = &expected {
            assert_eq!(&hash, previous);
        }
        expected = Some(hash);
    }
    let no_funding = perpetual_run(None).await;
    let disabled = no_funding.funding.as_ref().unwrap();
    assert_eq!(disabled.rate_bps, None);
    assert_eq!(disabled.observed_settlement_passes, 0);
    assert!(disabled.settlements.is_empty() && disabled.per_symbol.is_empty());
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

#[tokio::test]
async fn benchmark_and_drawdown_use_observed_horizon_and_reject_stale_references() {
    use apexes_backtest::{
        benchmark::{BenchmarkEvidence, BenchmarkPolicy},
        drawdown_duration::DrawdownEvidence,
    };
    struct OneBuy;
    impl Strategy for OneBuy {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            _: &StrategyContext,
        ) -> Vec<OrderIntent> {
            if matches!(event, MarketEvent::Trade(t) if t.trade_id.as_deref()==Some("open")) {
                vec![intent(AssetClass::Spot, Side::Buy, dec!(1), None)]
            } else {
                vec![]
            }
        }
    }
    let events = vec![
        trade(0, dec!(100), "baseline"),
        depth(100, 1, dec!(100)),
        trade(100, dec!(100), "open"),
        depth(600, 2, dec!(100)),
        trade(1000, dec!(80), "low"),
        trade(2000, dec!(120), "high"),
        trade(3000, dec!(90), "last"),
        bar(3000),
        mark(4000),
        tick(4500),
    ];
    let mut digest = None;
    for _ in 0..10 {
        let r = registry(&events, "benchmark");
        r.register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
            .unwrap();
        r.register::<_, ()>(
            (),
            BenchmarkPolicy {
                symbol: SYMBOL.into(),
                max_end_age_ms: 1500,
            },
        )
        .unwrap();
        let report = run_backtest(
            configuration(AssetClass::Spot, None),
            &r,
            InMemoryMarketDataSource::new(events.clone()),
            OneBuy,
        )
        .await
        .unwrap();
        assert_eq!(report.trades.len(), 1);
        let Some(BenchmarkEvidence::Available { comparison, .. }) = &report.benchmark else {
            panic!("{:?}", report.benchmark)
        };
        assert_eq!(comparison.quote_currency, "USDT");
        assert_eq!(comparison.start_at, at(0));
        assert_eq!(comparison.end_at, at(4500));
        assert_eq!(comparison.last_reference_at, at(3000));
        assert_eq!(comparison.final_equity, dec!(9989.9));
        assert_eq!(comparison.strategy_return, dec!(-0.00101));
        assert_eq!(comparison.cash_return, Decimal::ZERO);
        assert_eq!(comparison.buy_hold_return, dec!(-0.1));
        assert_eq!(comparison.excess_vs_buy_hold, dec!(0.09899));
        let Some(DrawdownEvidence::Available(d)) = &report.drawdown_duration else {
            panic!("missing drawdown")
        };
        assert_eq!(d.completed_episodes, 1);
        assert_eq!(d.longest_completed_ms, Some(1900));
        assert_eq!(d.longest_observed_ms, 2500);
        assert_eq!(d.current.as_ref().unwrap().observed_duration_ms, 2500);
        assert_eq!(d.current.as_ref().unwrap().trough_equity, dec!(9989.9));
        let hash = canonical_report_sha256(&report).unwrap();
        if let Some(previous) = &digest {
            assert_eq!(&hash, previous);
        }
        digest = Some(hash);
        let mut legacy = serde_json::to_value(&report).unwrap();
        legacy.as_object_mut().unwrap().remove("benchmark");
        legacy.as_object_mut().unwrap().remove("drawdown_duration");
        let old: BacktestReport = serde_json::from_value(legacy).unwrap();
        assert!(old.benchmark.is_none() && old.drawdown_duration.is_none());
    }
    let r = registry(&events, "benchmark-stale");
    r.register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
        .unwrap();
    r.register::<_, ()>(
        (),
        BenchmarkPolicy {
            symbol: SYMBOL.into(),
            max_end_age_ms: 1499,
        },
    )
    .unwrap();
    let report = run_backtest(
        configuration(AssetClass::Spot, None),
        &r,
        InMemoryMarketDataSource::new(events.clone()),
        OneBuy,
    )
    .await
    .unwrap();
    assert!(
        matches!(report.benchmark,Some(BenchmarkEvidence::Unavailable{reason,..}) if reason.contains("stale"))
    );
    assert_eq!(report.trades.len(), 1);
    let r = registry(&events, "benchmark-inventory");
    r.register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
        .unwrap();
    r.register::<_, ()>(
        (),
        BenchmarkPolicy {
            symbol: SYMBOL.into(),
            max_end_age_ms: 1500,
        },
    )
    .unwrap();
    let mut config = configuration(AssetClass::Spot, None);
    config.initial_balances.insert("BTC".into(), dec!(1));
    let report = run_backtest(config, &r, InMemoryMarketDataSource::new(events), OneBuy)
        .await
        .unwrap();
    assert!(
        matches!(report.benchmark,Some(BenchmarkEvidence::Unavailable{reason,..}) if reason.contains("cash-only"))
    );
}

#[tokio::test]
async fn benchmark_requires_sealed_input_and_first_observed_trade() {
    use apexes_backtest::benchmark::{BenchmarkEvidence, BenchmarkPolicy};
    struct Idle;
    impl Strategy for Idle {
        fn on_market_event(&mut self, _: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
            vec![]
        }
    }
    let events = vec![
        depth(0, 1, dec!(100)),
        trade(100, dec!(100), "first"),
        tick(200),
        bar(300),
        mark(1300),
    ];
    let policy = BenchmarkPolicy {
        symbol: SYMBOL.into(),
        max_end_age_ms: 2000,
    };
    let r = registry(&events, "benchmark-no-first-trade");
    r.register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
        .unwrap();
    r.register::<_, ()>((), policy.clone()).unwrap();
    let report = run_backtest(
        configuration(AssetClass::Spot, None),
        &r,
        InMemoryMarketDataSource::new(events.clone()),
        Idle,
    )
    .await
    .unwrap();
    assert!(
        matches!(report.benchmark,Some(BenchmarkEvidence::Unavailable{reason,..}) if reason.contains("first-event"))
    );
    let r = Registry::new();
    r.register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
        .unwrap();
    r.register::<_, ()>((), policy).unwrap();
    let report = run_backtest(
        configuration(AssetClass::Spot, None),
        &r,
        InMemoryMarketDataSource::new(events),
        Idle,
    )
    .await
    .unwrap();
    assert!(
        matches!(report.benchmark,Some(BenchmarkEvidence::Unavailable{reason,..}) if reason.contains("verified"))
    );
}

#[tokio::test]
async fn funding_attribution_preserves_gap_and_boundary_order_in_both_modes() {
    use apexes_backtest::funding_attribution::{FundingPriceSource, SymbolFunding};
    let gap = 28_820_000; // First observation after both 08:00 and 16:00.
    let events = vec![
        depth(0, 1, dec!(100)),
        trade(0, dec!(100), "open"),
        depth(500, 2, dec!(100)),
        tick(1000),
        mark(gap),
        depth(gap + 1000, 3, dec!(100)),
        trade(gap + 1000, dec!(99), "close"),
        tick(gap + 1500),
    ];
    // Use a manifest whose streams actually exist; no bars in this fixture.
    for causal in [false, true] {
        let r = Registry::new();
        let manifest = seal_dataset(
            DatasetDefinition {
                dataset_id: "funding-gap".into(),
                version: "v1".into(),
                source_id: "synthetic".into(),
                start_day: at(0).date_naive(),
                end_day_exclusive: at(0).date_naive().succ_opt().unwrap(),
                streams: [
                    StreamKind::Trade,
                    StreamKind::Depth,
                    StreamKind::Tick,
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
            &events,
        )
        .unwrap();
        r.register::<_, ()>((), manifest).unwrap();
        if causal {
            r.register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
                .unwrap();
        }
        let report = run_backtest(
            configuration(AssetClass::Perpetual, Some(dec!(-10))),
            &r,
            InMemoryMarketDataSource::new(events.clone()),
            Perpetual,
        )
        .await
        .unwrap();
        let funding = report.funding.as_ref().unwrap();
        assert_eq!(funding.observed_settlement_passes, 2);
        assert_eq!(
            funding.settlements.len(),
            1,
            "do not invent missed boundary settlements"
        );
        let settlement = &funding.settlements[0];
        assert_eq!(settlement.boundary, at(28_810_000));
        assert_eq!(settlement.observed_at, at(gap));
        assert_eq!(settlement.movement.price_source, FundingPriceSource::Mark);
        assert_eq!(settlement.movement.balance_delta, dec!(0.1));
        let SymbolFunding::Available(totals) = &funding.per_symbol[SYMBOL] else {
            panic!("expected funding totals")
        };
        assert_eq!(totals.income, dec!(0.1));
        assert_eq!(totals.expenses, Decimal::ZERO);
        assert_eq!(totals.net_realized_plus_funding, Some(dec!(-1.099)));
        assert_eq!(report.overall.total_pnl, dec!(-1.099));
    }
}

#[tokio::test]
async fn extreme_market_exit_is_attributed_to_derisk_not_strategy() {
    use apexes_backtest::{
        execution_performance::SymbolExecutionPerformance, exit_attribution::ExitReason,
    };
    use apexes_strategy::{FillOutcome, FillSource};
    struct BuyOnce(Arc<std::sync::Mutex<Vec<(FillSource, apexes_core::FillId)>>>);
    impl Strategy for BuyOnce {
        fn on_fill(&mut self, outcome: &FillOutcome<'_>) {
            self.0
                .lock()
                .unwrap()
                .push((outcome.source, outcome.fill.id));
        }
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            _: &StrategyContext,
        ) -> Vec<OrderIntent> {
            if matches!(event,MarketEvent::Trade(t) if t.trade_id.as_deref()==Some("open")) {
                vec![intent(AssetClass::Spot, Side::Buy, dec!(1), None)]
            } else {
                vec![]
            }
        }
    }
    let events = vec![
        depth(0, 1, dec!(100)),
        trade(100, dec!(100), "open"),
        trade(200, dec!(130), "jump"),
    ];
    let mut config = configuration(AssetClass::Spot, None);
    config.market_condition_config = Some(apexes_risk::MarketConditionConfig {
        volatility_threshold: dec!(1000000),
        jump_threshold_pct: dec!(0.1),
        sensitivity: dec!(1),
        volatility_window: 20,
        price_source: apexes_risk::PriceSource::Trade,
        account_equity_volatility_threshold: None,
        include_spot: true,
    });
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let report = run_backtest(
        config,
        &Registry::new(),
        InMemoryMarketDataSource::new(events),
        BuyOnce(seen.clone()),
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 2);
    assert_eq!(
        *seen.lock().unwrap(),
        [FillSource::Direct, FillSource::Derisk]
            .into_iter()
            .zip(report.trades.iter().map(|f| f.id))
            .collect::<Vec<_>>()
    );
    assert_eq!(seen.lock().unwrap().len(), report.trades.len());
    let SymbolExecutionPerformance::Available(metrics) =
        &report.execution_performance.as_ref().unwrap().per_symbol[SYMBOL]
    else {
        panic!("missing metrics")
    };
    let exits = metrics.exit_attribution.as_ref().unwrap();
    assert_eq!(exits.closes.len(), 1);
    assert_eq!(exits.closes[0].reason, ExitReason::Derisk);
    assert_eq!(exits.closes[0].fill_id, report.trades[1].id);
    assert_eq!(
        exits.by_reason[&ExitReason::Derisk].net_realized,
        dec!(-1.199)
    );
}

#[tokio::test]
async fn resting_limit_and_ems_closing_fills_keep_distinct_origins() {
    use apexes_backtest::{
        execution_performance::SymbolExecutionPerformance, exit_attribution::ExitReason,
    };
    use apexes_strategy::{FillOutcome, FillSource};
    struct ExitStrategy(Arc<std::sync::Mutex<Vec<(FillSource, apexes_core::FillId)>>>);
    impl Strategy for ExitStrategy {
        fn on_fill(&mut self, outcome: &FillOutcome<'_>) {
            self.0
                .lock()
                .unwrap()
                .push((outcome.source, outcome.fill.id));
        }
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            _: &StrategyContext,
        ) -> Vec<OrderIntent> {
            match event {
                MarketEvent::Trade(t) if t.trade_id.as_deref() == Some("open") => {
                    vec![intent(AssetClass::Spot, Side::Buy, dec!(3), None)]
                }
                MarketEvent::Trade(t) if t.trade_id.as_deref() == Some("limit") => vec![intent(
                    AssetClass::Spot,
                    Side::Sell,
                    dec!(1),
                    Some(dec!(101)),
                )],
                MarketEvent::Trade(t) if t.trade_id.as_deref() == Some("ems") => {
                    let mut i = intent(AssetClass::Spot, Side::Sell, dec!(2), None);
                    i.execution_algo = Some(apexes_strategy::ExecutionAlgoSpec::Twap {
                        duration: std::time::Duration::from_secs(2),
                        slices: 2,
                    });
                    vec![i]
                }
                _ => vec![],
            }
        }
    }
    let mut maker = trade(2000, dec!(101), "maker");
    if let MarketEvent::Trade(t) = &mut maker {
        t.side = Some(Side::Buy);
    }
    let events = vec![
        depth(0, 1, dec!(100)),
        trade(0, dec!(100), "open"),
        tick(500),
        trade(1000, dec!(100), "limit"),
        tick(1500),
        maker,
        trade(3000, dec!(100), "ems"),
        depth(3500, 2, dec!(100)),
        tick(4000),
        depth(4500, 3, dec!(100)),
        tick(5000),
        tick(5500),
        tick(6000),
    ];
    let registry = Registry::new();
    registry
        .register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
        .unwrap();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let report = run_backtest(
        configuration(AssetClass::Spot, None),
        &registry,
        InMemoryMarketDataSource::new(events),
        ExitStrategy(seen.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        [
            FillSource::Direct,
            FillSource::Resting,
            FillSource::Ems,
            FillSource::Ems
        ]
        .into_iter()
        .zip(report.trades.iter().map(|f| f.id))
        .collect::<Vec<_>>()
    );
    assert_eq!(seen.lock().unwrap().len(), report.trades.len());
    let SymbolExecutionPerformance::Available(metrics) =
        &report.execution_performance.as_ref().unwrap().per_symbol[SYMBOL]
    else {
        panic!("missing metrics")
    };
    let exits = metrics.exit_attribution.as_ref().unwrap();
    assert_eq!(report.trades.len(), 4, "{:?}", report.trades);
    assert_eq!(exits.closes.len(), 3);
    assert_eq!(exits.closes[0].reason, ExitReason::Resting);
    assert_eq!(
        exits.by_reason[&ExitReason::Resting].closed_quantity,
        dec!(1)
    );
    assert_eq!(exits.by_reason[&ExitReason::Ems].closed_quantity, dec!(2));
    assert_eq!(exits.by_reason[&ExitReason::Ems].closing_count, 2);
    assert_eq!(
        exits.closes.iter().map(|c| c.net_realized).sum::<Decimal>(),
        metrics.net_realized
    );
    assert_eq!(
        exits
            .closes
            .iter()
            .map(|c| c.allocated_entry_fee + c.allocated_exit_fee)
            .sum::<Decimal>(),
        metrics.fees_paid
    );
    assert_eq!(metrics.open_quantity, Decimal::ZERO);
}

#[test]
fn drift_closed_bar_decisions_are_causal_and_trade_rate_independent() {
    use apexes_strategy::drift_dsl::DriftScriptStrategy;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    let script =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/causal_time.drift");
    for causal in [false, true] {
        for dense in [false, true] {
            let mut events = vec![
                depth(0, 1, dec!(100)),
                trade(0, dec!(100), "open"),
                bar(1000),
                bar(1000),
                tick(2500),
                bar(3000),
                tick(4500),
                bar(5000),
                tick(6500),
            ];
            if dense {
                for ms in [2600, 2700, 2800, 4600, 4700, 4800] {
                    events.push(trade(ms, dec!(100), "extra"));
                }
            }
            let registry = Registry::new();
            if causal {
                registry
                    .register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
                    .unwrap();
            }
            let strategy = DriftScriptStrategy::from_file(&script).unwrap();
            let report = futures::executor::block_on(run_backtest(
                configuration(AssetClass::Spot, None),
                &registry,
                InMemoryMarketDataSource::new(events),
                strategy,
            ))
            .unwrap();
            if causal {
                assert_eq!(report.trades.len(), 2);
                assert_eq!(
                    report
                        .trades
                        .iter()
                        .map(|f| f.filled_at)
                        .collect::<Vec<_>>(),
                    vec![at(2500), at(6500)]
                );
            } else {
                assert!(
                    report.trades.is_empty(),
                    "legacy open-time bars must not authorize decisions"
                );
            }
        }
    }
}

#[test]
fn observation_manifest_replays_overlapping_bars_at_close_without_reordering_sealed_input() {
    use apexes_backtest::historical_dataset::{OBSERVATION_SCHEMA, migrate_observation_dataset};
    use apexes_strategy::drift_dsl::DriftScriptStrategy;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    let script =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/causal_time.drift");
    let mut wide = bar(10000);
    if let MarketEvent::Bar(b) = &mut wide {
        b.interval = BarInterval::OneMinute;
        b.close_time = at(70000);
    }
    // Source order is bar-open order, with interleaved trade and depth.
    let original = vec![
        depth(0, 1, dec!(100)),
        trade(0, dec!(100), "start"),
        wide.clone(),
        bar(11000),
        trade(12000, dec!(100), "tie1"),
        tick(12000),
        tick(12500),
        bar(13000),
        tick(14500),
        depth(70000, 2, dec!(100)),
        tick(70500),
    ];
    let definition = DatasetDefinition {
        dataset_id: "overlapping-bars".into(),
        version: "v1".into(),
        source_id: "synthetic".into(),
        start_day: at(0).date_naive(),
        end_day_exclusive: at(0).date_naive().succ_opt().unwrap(),
        streams: [
            StreamKind::Depth,
            StreamKind::Trade,
            StreamKind::Bar,
            StreamKind::Tick,
        ]
        .into_iter()
        .map(|kind| DatasetStream {
            symbol: SYMBOL.into(),
            venue: Venue::Binance,
            kind,
        })
        .collect(),
    };
    let v1 = seal_dataset(definition, &original).unwrap();
    let export = migrate_observation_dataset(&v1, &original, "v2").unwrap();
    assert_eq!(export.manifest.schema_version, OBSERVATION_SCHEMA);
    assert_eq!(export.source_replay_sha256, v1.replay_sha256);
    assert_eq!(export.events.len(), original.len());
    assert_eq!(export.events[2..5], original[3..6]);
    assert_eq!(export.events[8], wide);
    assert!(v1.verify(&export.events).is_err());
    assert!(v1.verify(&original).is_ok());
    let mut expected = None;
    for _ in 0..10 {
        let registry = Registry::new();
        registry
            .register::<_, ()>((), export.manifest.clone())
            .unwrap();
        registry
            .register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
            .unwrap();
        registry
            .register::<_, ()>(
                (),
                ReplayIdentityPolicy {
                    namespace: uuid::Uuid::from_u128(72),
                },
            )
            .unwrap();
        let report = futures::executor::block_on(run_backtest(
            configuration(AssetClass::Spot, None),
            &registry,
            InMemoryMarketDataSource::from_observation_ordered(export.events.clone()).unwrap(),
            DriftScriptStrategy::from_file(&script).unwrap(),
        ))
        .unwrap();
        assert_eq!(
            report
                .trades
                .iter()
                .map(|f| f.filled_at)
                .collect::<Vec<_>>(),
            vec![at(12500), at(70500)]
        );
        assert_eq!(report.event_latency.as_ref().unwrap().regressing_events, 0);
        assert_eq!(
            report.historical_dataset.as_ref().unwrap().manifest,
            export.manifest
        );
        let hash = canonical_report_sha256(&report).unwrap();
        if let Some(prior) = &expected {
            assert_eq!(&hash, prior);
        }
        expected = Some(hash);
    }
    let registry = Registry::new();
    registry
        .register::<_, ()>((), export.manifest.clone())
        .unwrap();
    let error = futures::executor::block_on(run_backtest(
        configuration(AssetClass::Spot, None),
        &registry,
        InMemoryMarketDataSource::from_observation_ordered(export.events.clone()).unwrap(),
        DriftScriptStrategy::from_file(&script).unwrap(),
    ))
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("requires explicit event-time latency")
    );
    registry
        .register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
        .unwrap();
    let error = futures::executor::block_on(run_backtest(
        configuration(AssetClass::Spot, None),
        &registry,
        InMemoryMarketDataSource::new(export.events),
        DriftScriptStrategy::from_file(&script).unwrap(),
    ))
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("historical dataset verification failed")
    );
}

#[test]
fn causal_book_context_uses_remaining_execution_depth_and_rejects_stale_or_gapped_books() {
    use apexes_strategy::{BookIntegrity, drift_dsl::DriftScriptStrategy};

    // Native assertions observe the same callback that invokes Drift.
    struct Probe {
        inner: DriftScriptStrategy,
        strict: bool,
    }
    impl Strategy for Probe {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            let ms = (ctx.now - at(0)).num_milliseconds();
            if !self.strict || matches!(ms, 1001 | 1300) {
                assert!(ctx.book.is_none(), "unexpected book at {ms}");
            } else {
                let book = ctx.book.as_ref().expect("validated book");
                assert!(book.matches_at(event, ctx.now));
                assert_eq!(book.integrity, BookIntegrity::Contiguous);
                assert_eq!(book.max_age_ms, 1000);
                assert_eq!(book.best_bid.unwrap().quantity, dec!(10));
                match ms {
                    0 | 500 | 1000 => {
                        assert_eq!(book.sequence, Some(9007199254740993));
                        assert_eq!(book.observed_at, at(0));
                        assert_eq!(book.age_ms(), ms);
                        assert_eq!(book.spread(), Some(dec!(1)));
                        assert_eq!(
                            book.best_ask.unwrap().quantity,
                            match ms {
                                0 => dec!(10),
                                500 => dec!(9),
                                _ => dec!(7),
                            }
                        );
                    }
                    1200 | 1400 => {
                        assert_eq!(book.observed_at, ctx.now);
                        assert_eq!(book.best_ask.unwrap().quantity, dec!(10));
                        assert_eq!(book.sequence, Some(if ms == 1200 { 20 } else { 30 }));
                    }
                    _ => panic!("unexpected event {ms}"),
                }
            }
            self.inner.on_market_event(event, ctx)
        }
    }

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    let script =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/book_context.drift");
    let mut gap = depth(1300, 22, dec!(111));
    if let MarketEvent::Depth(update) = &mut gap {
        update.is_snapshot = false;
    }
    let events = vec![
        depth(0, 9007199254740993, dec!(100)),
        tick(500),
        tick(1000),
        tick(1001),
        depth(1200, 20, dec!(110)),
        gap,
        depth(1400, 30, dec!(120)),
    ];
    let mut fingerprint = None;
    for strict in [true, true, false] {
        let registry = registry(&events, "causal-book-context");
        registry
            .register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
            .unwrap();
        if strict {
            registry
                .register::<_, ()>(
                    (),
                    SimulatedDepthPolicy {
                        max_age_ms: 1000,
                        sequence: DepthSequence::Contiguous,
                    },
                )
                .unwrap();
        }
        let strategy = Probe {
            inner: DriftScriptStrategy::from_file(&script).unwrap(),
            strict,
        };
        let report = futures::executor::block_on(run_backtest(
            configuration(AssetClass::Spot, None),
            &registry,
            InMemoryMarketDataSource::new(events.clone()),
            strategy,
        ))
        .unwrap();
        if strict {
            assert_eq!(
                report
                    .trades
                    .iter()
                    .map(|f| (f.quantity, f.price, f.filled_at))
                    .collect::<Vec<_>>(),
                vec![
                    (dec!(1), dec!(100), at(500)),
                    (dec!(2), dec!(100), at(1000))
                ]
            );
            let digest = canonical_report_sha256(&report).unwrap();
            if let Some(prior) = &fingerprint {
                assert_eq!(&digest, prior);
            }
            fingerprint = Some(digest);
        } else {
            assert!(report.trades.is_empty());
        }
    }
}

#[test]
fn direct_order_outcomes_are_ordered_and_available_to_drift_before_next_decision() {
    use apexes_core::OrderStatus;
    use apexes_strategy::{
        AttemptStage, OrderOutcome, OrderOutcomeKind, drift_dsl::DriftScriptStrategy,
    };
    type Evidence = Vec<(
        i64,
        AttemptStage,
        String,
        Decimal,
        Option<apexes_core::OrderId>,
        Option<apexes_strategy::IntentId>,
    )>;
    struct Probe {
        script: DriftScriptStrategy,
        outcomes: Arc<std::sync::Mutex<Evidence>>,
    }
    impl Strategy for Probe {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            self.script.on_market_event(event, ctx)
        }
        fn on_order_outcome(&mut self, outcome: &OrderOutcome<'_>) {
            let (kind, id) = match outcome.kind {
                OrderOutcomeKind::Rejected { code } => (code.to_owned(), None),
                OrderOutcomeKind::Scheduled => ("scheduled".into(), None),
                OrderOutcomeKind::Returned { order } => {
                    assert!(matches!(
                        order.status,
                        OrderStatus::New | OrderStatus::Filled
                    ));
                    (format!("returned:{:?}", order.status), Some(order.id))
                }
                OrderOutcomeKind::SubmissionFailed { code } => {
                    panic!("unexpected submission failure {code}")
                }
            };
            self.outcomes.lock().unwrap().push((
                (outcome.at - at(0)).num_milliseconds(),
                outcome.stage,
                kind,
                outcome.intent.quantity,
                id,
                outcome.intent_id,
            ));
            self.script.on_order_outcome(outcome);
        }
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/direct_order_outcomes.drift");
    let events = vec![
        depth(0, 1, dec!(100)),
        trade(0, dec!(100), "open"),
        tick(500),
        tick(1000),
        trade(1500, dec!(99), "maker"),
    ];
    use apexes_backtest::submission_journal::{
        RecoveredJournal, SubmissionJournalPolicy, SubmissionResult, SubmissionRoute,
    };
    struct Temp(std::path::PathBuf);
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let directory =
        Temp(std::env::temp_dir().join(format!("apexes-submit-{}", uuid::Uuid::new_v4())));
    std::fs::create_dir(&directory.0).unwrap();
    let path = directory.0.join("journal.jsonl");
    let mut previous = None;
    for index in 0..2 {
        let r = registry(&events, "direct-order-outcomes");
        if index == 1 {
            r.register::<_, ()>((), SubmissionJournalPolicy { path: path.clone() })
                .unwrap();
        }
        r.register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
            .unwrap();
        r.register::<Arc<CompositeRiskChecker>, ()>(
            (),
            Arc::new(CompositeRiskChecker::new().with_pre_trade_check(Box::new(
                LimitChecker::new(RiskLimits {
                    max_order_quantity: Some(dec!(2)),
                    ..Default::default()
                }),
            ))),
        )
        .unwrap();
        let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let strategy: Box<dyn Strategy> = Box::new(Probe {
            script: DriftScriptStrategy::from_file(&script).unwrap(),
            outcomes: outcomes.clone(),
        });
        let report = futures::executor::block_on(run_backtest(
            configuration(AssetClass::Spot, None),
            &r,
            InMemoryMarketDataSource::new(events.clone()),
            strategy,
        ))
        .unwrap();
        let seen = outcomes.lock().unwrap();
        assert_eq!(
            seen.iter()
                .map(|(ms, stage, kind, qty, _, _)| (*ms, *stage, kind.as_str(), *qty))
                .collect::<Vec<_>>(),
            vec![
                (0, AttemptStage::Decision, "scheduled", dec!(1)),
                (0, AttemptStage::Decision, "risk_rejected", dec!(9)),
                (
                    0,
                    AttemptStage::Decision,
                    "event_latency_symbol_pending",
                    dec!(1)
                ),
                (500, AttemptStage::Arrival, "returned:Filled", dec!(1)),
                (500, AttemptStage::Decision, "scheduled", dec!(1)),
                (1000, AttemptStage::Arrival, "returned:New", dec!(1)),
            ]
        );
        assert_eq!(
            seen.iter().map(|x| x.5.unwrap().0).collect::<Vec<_>>(),
            vec![1, 2, 3, 1, 4, 4]
        );
        assert_eq!(report.trades.len(), 2);
        assert_eq!(seen[3].4, Some(report.trades[0].order_id));
        assert_eq!(seen[5].4, Some(report.trades[1].order_id));
        if index == 1 {
            let recovered = RecoveredJournal::load(&path).unwrap();
            assert!(recovered.matches_report(&report).unwrap());
            assert_eq!(recovered.uncertain_attempts().count(), 0);
            assert_eq!(recovered.attempts.len(), 2);
            for (attempt, expected_id, ms, status, trade) in [
                (
                    &recovered.attempts[0],
                    1,
                    500,
                    OrderStatus::Filled,
                    &report.trades[0],
                ),
                (
                    &recovered.attempts[1],
                    4,
                    1000,
                    OrderStatus::New,
                    &report.trades[1],
                ),
            ] {
                assert_eq!(attempt.attempt.intent_id, expected_id);
                assert_eq!(attempt.attempt.at, at(ms));
                assert_eq!(attempt.attempt.route, SubmissionRoute::Direct);
                assert_eq!(
                    attempt.result,
                    Some(SubmissionResult::OrderReturned {
                        order_id: trade.order_id.0,
                        status
                    })
                );
            }
        } else {
            assert!(!path.exists());
        }
        let digest = canonical_report_sha256(&report).unwrap();
        if let Some((previous_seen, previous_digest)) = &previous {
            assert_eq!(&*seen, previous_seen);
            assert_eq!(&digest, previous_digest);
        }
        previous = Some((seen.clone(), digest));
    }
}

#[tokio::test]
async fn direct_outcomes_distinguish_cancelled_order_from_executor_error() {
    use apexes_core::OrderStatus;
    use apexes_strategy::{AttemptStage, OrderOutcome, OrderOutcomeKind};
    type Evidence = Vec<(String, Option<apexes_core::OrderId>)>;
    struct Probe {
        seen: Arc<std::sync::Mutex<Evidence>>,
    }
    impl Strategy for Probe {
        fn on_market_event(&mut self, _: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
            vec![intent(AssetClass::Spot, Side::Buy, dec!(1), None)]
        }
        fn on_order_outcome(&mut self, outcome: &OrderOutcome<'_>) {
            assert_eq!(outcome.stage, AttemptStage::Decision);
            let result = match outcome.kind {
                OrderOutcomeKind::Returned { order } => {
                    assert_eq!(order.status, OrderStatus::Cancelled);
                    assert_eq!(order.filled_quantity, Decimal::ZERO);
                    ("cancelled".to_owned(), Some(order.id))
                }
                OrderOutcomeKind::SubmissionFailed { code } => (code.to_owned(), None),
                other => panic!("unexpected outcome {other:?}"),
            };
            self.seen.lock().unwrap().push(result);
        }
    }
    use apexes_backtest::submission_journal::{
        RecoveredJournal, SubmissionJournalPolicy, SubmissionResult,
    };
    struct Temp(std::path::PathBuf);
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let directory =
        Temp(std::env::temp_dir().join(format!("apexes-submit-errors-{}", uuid::Uuid::new_v4())));
    std::fs::create_dir(&directory.0).unwrap();
    let events = vec![trade(0, dec!(100), "no-book")];
    for strict in [false, true] {
        let r = registry(&events, "direct-submission-evidence");
        let path = directory.0.join(format!("{strict}.jsonl"));
        r.register::<_, ()>((), SubmissionJournalPolicy { path: path.clone() })
            .unwrap();
        if strict {
            r.register::<_, ()>(
                (),
                SimulatedDepthPolicy {
                    sequence: DepthSequence::SnapshotOnly,
                    max_age_ms: 1000,
                },
            )
            .unwrap();
        }
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let report = run_backtest(
            configuration(AssetClass::Spot, None),
            &r,
            InMemoryMarketDataSource::new(events.clone()),
            Probe { seen: seen.clone() },
        )
        .await
        .unwrap();
        assert!(report.trades.is_empty());
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let recovered = RecoveredJournal::load(&path).unwrap();
        assert!(recovered.matches_report(&report).unwrap());
        assert_eq!(recovered.attempts.len(), 1);
        assert_eq!(recovered.uncertain_attempts().count(), usize::from(strict));
        assert_eq!(
            recovered.attempts[0].result,
            Some(if strict {
                SubmissionResult::UncertainError
            } else {
                SubmissionResult::OrderReturned {
                    order_id: seen[0].1.unwrap().0,
                    status: OrderStatus::Cancelled,
                }
            })
        );
        if strict {
            assert_eq!(seen[0], ("order_rejected".into(), None));
        } else {
            assert_eq!(seen[0].0, "cancelled");
            assert!(seen[0].1.is_some());
        }
    }
}

#[test]
fn committed_fill_callbacks_drive_next_decision_and_repeat_with_partial_resting_fills() {
    use apexes_strategy::{FillOutcome, FillSource, OrderOutcome, drift_dsl::DriftScriptStrategy};
    use apexes_strategy::{TerminalKind, TerminalOutcome};
    type Evidence = Vec<(DateTime<Utc>, FillSource, apexes_core::Fill)>;
    type Terminals = Vec<(DateTime<Utc>, apexes_core::OrderId)>;
    struct Probe {
        script: DriftScriptStrategy,
        seen: Arc<std::sync::Mutex<Evidence>>,
        terminal: Arc<std::sync::Mutex<Terminals>>,
    }
    impl Strategy for Probe {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            self.script.on_market_event(event, ctx)
        }
        fn on_order_outcome(&mut self, outcome: &OrderOutcome<'_>) {
            self.script.on_order_outcome(outcome);
        }
        fn on_order_terminal(&mut self, outcome: &TerminalOutcome<'_>) {
            let TerminalKind::Filled { order } = outcome.kind else {
                panic!("unexpected terminal");
            };
            assert_eq!(outcome.intent_id, Some(apexes_strategy::IntentId(2)));
            assert_eq!(order.filled_quantity, dec!(3));
            assert_eq!(order.quantity, dec!(3));
            assert_eq!(order.status, apexes_core::OrderStatus::Filled);
            assert_eq!(
                self.seen.lock().unwrap().last().unwrap().2.order_id,
                order.id
            );
            self.terminal.lock().unwrap().push((outcome.at, order.id));
            self.script.on_order_terminal(outcome);
        }
        fn on_fill(&mut self, outcome: &FillOutcome<'_>) {
            let expected = match self.seen.lock().unwrap().len() {
                0 => 1,
                1 | 2 => 2,
                3 => 3,
                _ => panic!("unexpected fill"),
            };
            assert_eq!(outcome.intent_id, Some(apexes_strategy::IntentId(expected)));

            self.seen
                .lock()
                .unwrap()
                .push((outcome.at, outcome.source, outcome.fill.clone()));
            self.script.on_fill(outcome);
        }
    }
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/fill_callbacks.drift");
    let events = vec![
        depth(0, 1, dec!(100)),
        trade(0, dec!(100), "entry"),
        tick(500),
        tick(1000),
        trade(1500, dec!(99), "partial"),
        trade(2000, dec!(99), "complete"),
        tick(2500),
        tick(3000),
    ];
    let mut prior = None;
    for _ in 0..2 {
        let r = registry(&events, "fill-callbacks");
        r.register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
            .unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let terminal = Arc::new(std::sync::Mutex::new(Vec::new()));
        let strategy: Box<dyn Strategy> = Box::new(Probe {
            script: DriftScriptStrategy::from_file(&path).unwrap(),
            seen: seen.clone(),
            terminal: terminal.clone(),
        });
        let report = futures::executor::block_on(run_backtest(
            configuration(AssetClass::Spot, None),
            &r,
            InMemoryMarketDataSource::new(events.clone()),
            strategy,
        ))
        .unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(
            seen.iter()
                .map(|(at, source, f)| (*at, *source, f.quantity))
                .collect::<Vec<_>>(),
            vec![
                (at(500), FillSource::Direct, dec!(1)),
                (at(1500), FillSource::Resting, dec!(2)),
                (at(2000), FillSource::Resting, dec!(1)),
                (at(3000), FillSource::Direct, dec!(1)),
            ]
        );
        assert_eq!(seen[1].2.order_id, seen[2].2.order_id);
        assert_eq!(
            *terminal.lock().unwrap(),
            vec![(at(2000), seen[2].2.order_id)]
        );
        assert_ne!(seen[1].2.id, seen[2].2.id);
        assert_eq!(
            seen.iter().map(|(_, _, f)| f.clone()).collect::<Vec<_>>(),
            report.trades
        );
        let digest = canonical_report_sha256(&report).unwrap();
        if let Some((prior_seen, prior_digest)) = &prior {
            assert_eq!(&*seen, prior_seen);
            assert_eq!(&digest, prior_digest);
        }
        prior = Some((seen.clone(), digest));
    }
}

#[tokio::test]
async fn transit_expiry_notifies_only_unsubmitted_direct_intents_at_observed_end() {
    use apexes_strategy::{
        AttemptStage, OrderOutcome, OrderOutcomeKind, TerminalKind, TerminalOutcome,
    };
    #[derive(Default)]
    struct Evidence {
        scheduled: usize,
        expired: Vec<(DateTime<Utc>, Decimal)>,
    }
    struct Probe(Arc<std::sync::Mutex<Evidence>>);
    impl Strategy for Probe {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            _: &StrategyContext,
        ) -> Vec<OrderIntent> {
            if matches!(event, MarketEvent::Trade(_)) {
                vec![intent(AssetClass::Spot, Side::Buy, dec!(2), None)]
            } else {
                vec![]
            }
        }
        fn on_order_outcome(&mut self, outcome: &OrderOutcome<'_>) {
            assert_eq!(outcome.intent_id, Some(apexes_strategy::IntentId(1)));
            assert_eq!(outcome.stage, AttemptStage::Decision);
            assert!(matches!(outcome.kind, OrderOutcomeKind::Scheduled));
            self.0.lock().unwrap().scheduled += 1;
        }
        fn on_order_terminal(&mut self, outcome: &TerminalOutcome<'_>) {
            assert_eq!(outcome.intent_id, Some(apexes_strategy::IntentId(1)));
            let TerminalKind::TransitExpired { intent } = outcome.kind else {
                panic!("unexpected terminal");
            };
            assert_eq!(intent.symbol, SYMBOL);
            let mut evidence = self.0.lock().unwrap();
            assert_eq!(evidence.scheduled, 1);
            evidence.expired.push((outcome.at, intent.quantity));
        }
    }
    let events = vec![
        depth(0, 1, dec!(100)),
        trade(100, dec!(100), "entry"),
        tick(200),
    ];
    let r = registry(&events, "transit-expiry");
    r.register::<_, ()>((), EventLatencyPolicy { delay_ms: 500 })
        .unwrap();
    let evidence = Arc::new(std::sync::Mutex::new(Evidence::default()));
    let report = run_backtest(
        configuration(AssetClass::Spot, None),
        &r,
        InMemoryMarketDataSource::new(events),
        Probe(evidence.clone()),
    )
    .await
    .unwrap();
    assert!(report.trades.is_empty());
    assert_eq!(report.event_latency.unwrap().expired_at_end, 1);
    assert_eq!(evidence.lock().unwrap().expired, vec![(at(200), dec!(2))]);
}

#[test]
fn trigger_expiry_callback_follows_committed_fills_and_drives_repeatable_drift_decision() {
    use apexes_strategy::{
        FillOutcome, FillSource, OrderOutcome, TerminalKind, TerminalOutcome,
        TriggerTerminalReason, drift_dsl::DriftScriptStrategy,
    };
    type Evidence = Vec<(DateTime<Utc>, apexes_core::OrderId, Decimal)>;
    struct Probe {
        script: DriftScriptStrategy,
        fills: Arc<std::sync::Mutex<Vec<apexes_core::Fill>>>,
        terminals: Arc<std::sync::Mutex<Evidence>>,
    }
    impl Strategy for Probe {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            self.script.on_market_event(event, ctx)
        }
        fn on_order_outcome(&mut self, outcome: &OrderOutcome<'_>) {
            self.script.on_order_outcome(outcome);
        }
        fn on_fill(&mut self, outcome: &FillOutcome<'_>) {
            if outcome.intent_id == Some(apexes_strategy::IntentId(1)) {
                assert_eq!(outcome.source, FillSource::Resting);
            }
            self.fills.lock().unwrap().push(outcome.fill.clone());
            self.script.on_fill(outcome);
        }
        fn on_order_terminal(&mut self, outcome: &TerminalOutcome<'_>) {
            let TerminalKind::Triggered { evidence } = outcome.kind else {
                panic!("unexpected terminal");
            };
            assert_eq!(outcome.intent_id, Some(apexes_strategy::IntentId(1)));
            assert_eq!(evidence.reason, TriggerTerminalReason::IocRemainderExpired);
            assert_eq!(evidence.order.order_type, OrderType::Stop);
            assert_eq!(evidence.order.status, apexes_core::OrderStatus::Cancelled);
            assert_eq!(evidence.order.stop_price, Some(dec!(101)));
            assert_eq!(evidence.order.quantity, dec!(15));
            assert_eq!(evidence.order.filled_quantity, dec!(10));
            let fills = self.fills.lock().unwrap();
            assert_eq!(fills.iter().map(|f| f.quantity).sum::<Decimal>(), dec!(10));
            assert!(fills.iter().all(|f| f.order_id == evidence.order.id));
            self.terminals.lock().unwrap().push((
                outcome.at,
                evidence.order.id,
                evidence.order.filled_quantity,
            ));
            self.script.on_order_terminal(outcome);
        }
    }
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/trigger_callbacks.drift");
    let events = vec![
        depth(0, 1, dec!(100)),
        trade(100, dec!(100), "admit"),
        trade(200, dec!(101), "trigger"),
        tick(300),
        trade(400, dec!(102), "repeat"),
    ];
    let mut prior = None;
    for _ in 0..2 {
        let r = registry(&events, "trigger-callbacks");
        let fills = Arc::new(std::sync::Mutex::new(Vec::new()));
        let terminals = Arc::new(std::sync::Mutex::new(Vec::new()));
        let report = futures::executor::block_on(run_backtest(
            configuration(AssetClass::Spot, None),
            &r,
            InMemoryMarketDataSource::new(events.clone()),
            Probe {
                script: DriftScriptStrategy::from_file(&path).unwrap(),
                fills: fills.clone(),
                terminals: terminals.clone(),
            },
        ))
        .unwrap();
        assert_eq!(report.trades.len(), 2);
        assert_eq!(report.trades[0].quantity, dec!(10));
        assert_eq!(report.trades[1].side, Side::Sell);
        assert_eq!(report.trades[1].quantity, dec!(1));
        assert_eq!(*fills.lock().unwrap(), report.trades);
        let observed = terminals.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec![(at(200), report.trades[0].order_id, dec!(10))]
        );
        let digest = canonical_report_sha256(&report).unwrap();
        if let Some(previous) = &prior {
            assert_eq!(previous, &(observed.clone(), digest.clone()));
        }
        prior = Some((observed, digest));
    }
}
