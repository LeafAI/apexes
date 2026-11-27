//! F03.05 — actual runner qualification with sealed synthetic Spot trades.

use apexes_backtest::{
    BacktestConfig, BacktestReport, CostModel, EventLatencyPolicy, FlatFeeSchedule,
    InMemoryMarketDataSource, ReplaySpeed,
    benchmark::{BenchmarkEvidence, BenchmarkPolicy},
    historical_dataset::{DatasetDefinition, DatasetStream, StreamKind, seal_dataset},
    portfolio_benchmark::{
        PortfolioAllocation, PortfolioBenchmarkEvidence, PortfolioBenchmarkMethod,
        PortfolioBenchmarkPolicy, PortfolioComparison,
    },
    replay_identity::{ReplayIdentityPolicy, canonical_report_sha256},
    run_backtest,
};
use apexes_core::{
    ApexesError, AssetClass, Bar, BarInterval, DepthUpdate, Instrument, MarketEvent, MarketType,
    OrderType, PriceLevel, Side, TradeEvent, Venue, registry::Registry,
};
use apexes_strategy::{OrderIntent, OrderRouting, Strategy, StrategyContext};
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

const BTC: &str = "BTCUSDT";
const ETH: &str = "ETHUSDT";

fn at(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(1_767_225_600_000 + ms).unwrap()
}

fn instrument(base: &str, symbol: &str) -> Instrument {
    Instrument::new(
        symbol,
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some(base.into()),
        Some("USDT".into()),
        dec!(0.01),
        dec!(0.001),
        Decimal::ONE,
    )
    .unwrap()
}

fn config() -> BacktestConfig {
    BacktestConfig {
        instruments: vec![instrument("BTC", BTC), instrument("ETH", ETH)],
        date_range: at(0).date_naive()..at(0).date_naive().succ_opt().unwrap(),
        initial_balances: HashMap::from([("USDT".into(), dec!(1000))]),
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(10),
                taker_bps: dec!(10),
            }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: Decimal::ZERO,
        risk_free_return: Decimal::ZERO,
        margin_tiers: HashMap::new(),
        market_condition_config: None,
        control: None,
    }
}

fn policy(weights: &[(&str, Decimal)]) -> PortfolioBenchmarkPolicy {
    PortfolioBenchmarkPolicy {
        allocations: weights
            .iter()
            .map(|(symbol, weight)| PortfolioAllocation {
                symbol: (*symbol).into(),
                weight: *weight,
            })
            .collect(),
        quote_currency: "USDT".into(),
        max_end_age_ms: 1000,
    }
}

fn trade(symbol: &str, ms: i64, price: Decimal) -> MarketEvent {
    MarketEvent::Trade(TradeEvent {
        symbol: symbol.into(),
        venue: Venue::Binance,
        price,
        quantity: Decimal::ONE,
        side: None,
        trade_id: None,
        timestamp: at(ms),
    })
}

fn bar(open: i64) -> MarketEvent {
    MarketEvent::Bar(Bar {
        symbol: BTC.into(),
        venue: Venue::Binance,
        open: dec!(999),
        high: dec!(999),
        low: dec!(999),
        close: dec!(999),
        volume: Decimal::ONE,
        interval: BarInterval::OneSecond,
        open_time: at(open),
        close_time: at(open) + TimeDelta::seconds(1),
    })
}

fn events() -> Vec<MarketEvent> {
    vec![
        trade(BTC, 0, dec!(100)),
        trade(ETH, 0, dec!(100)),
        trade(BTC, 1000, dec!(110)),
        trade(ETH, 1000, dec!(95)),
    ]
}

fn registry(config: &BacktestConfig, events: &[MarketEvent]) -> Registry {
    let mut streams: Vec<_> = config
        .instruments
        .iter()
        .map(|i| DatasetStream {
            symbol: i.symbol.clone(),
            venue: i.venue,
            kind: StreamKind::Trade,
        })
        .collect();
    for event in events {
        let stream = DatasetStream {
            symbol: event.symbol().into(),
            venue: event.venue(),
            kind: match event {
                MarketEvent::Trade(_) => StreamKind::Trade,
                MarketEvent::Depth(_) => StreamKind::Depth,
                MarketEvent::Bar(_) => StreamKind::Bar,
                _ => unreachable!("unsupported fixture event"),
            },
        };
        if !streams.contains(&stream) {
            streams.push(stream);
        }
    }
    let manifest = seal_dataset(
        DatasetDefinition {
            dataset_id: "static-spot-fixture".into(),
            version: "v1".into(),
            source_id: "synthetic".into(),
            start_day: config.date_range.start,
            end_day_exclusive: config.date_range.end,
            streams,
        },
        events,
    )
    .unwrap();
    let registry = Registry::new();
    registry.register::<_, ()>((), manifest).unwrap();
    registry
}

struct Idle;

impl Strategy for Idle {
    fn on_market_event(&mut self, _: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
        Vec::new()
    }
}

struct BuyBtc;

impl Strategy for BuyBtc {
    fn on_market_event(&mut self, event: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
        if !matches!(event, MarketEvent::Trade(trade) if trade.symbol == BTC && trade.timestamp == at(0) && trade.price == dec!(100))
        {
            return Vec::new();
        }
        vec![OrderIntent {
            symbol: BTC.into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: Decimal::ONE,
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }]
    }
}

async fn run(
    config: BacktestConfig,
    registry: &Registry,
    events: Vec<MarketEvent>,
    strategy: impl Strategy,
) -> Result<BacktestReport, ApexesError> {
    run_backtest(
        config,
        registry,
        InMemoryMarketDataSource::new(events),
        strategy,
    )
    .await
}

async fn evaluate(
    config: BacktestConfig,
    events: Vec<MarketEvent>,
    policy: PortfolioBenchmarkPolicy,
) -> PortfolioBenchmarkEvidence {
    let registry = registry(&config, &events);
    registry.register::<_, ()>((), policy).unwrap();
    run(config, &registry, events, Idle)
        .await
        .unwrap()
        .portfolio_benchmark
        .unwrap()
}

fn available(evidence: &PortfolioBenchmarkEvidence) -> &PortfolioComparison {
    let PortfolioBenchmarkEvidence::Available { comparison, .. } = evidence else {
        panic!("expected available benchmark, got {evidence:?}");
    };
    comparison
}

fn unavailable(evidence: &PortfolioBenchmarkEvidence, expected: &str) {
    assert!(
        matches!(evidence, PortfolioBenchmarkEvidence::Unavailable { reason, .. } if reason.contains(expected)),
        "expected {expected}, got {evidence:?}"
    );
}

#[tokio::test]
async fn weighted_portfolio_reconciles_with_actual_strategy_and_repeats_fingerprint() {
    let mut events = events();
    events.insert(
        0,
        MarketEvent::Depth(DepthUpdate {
            symbol: BTC.into(),
            venue: Venue::Binance,
            timestamp: at(0),
            bids: vec![PriceLevel {
                price: dec!(99),
                quantity: dec!(5),
            }],
            asks: vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(5),
            }],
            sequence: Some(1),
            is_snapshot: true,
            source_range: None,
        }),
    );
    // Later same-time trades cannot change the selected first reference, and
    // the last same-time trade at the endpoint sets the final reference.
    events.insert(3, trade(BTC, 0, dec!(102)));
    events.insert(4, trade(BTC, 1000, dec!(120)));
    let registry = registry(&config(), &events);
    registry
        .register::<_, ()>((), policy(&[(ETH, dec!(0.4)), (BTC, dec!(0.6))]))
        .unwrap();
    registry
        .register::<_, ()>(
            (),
            ReplayIdentityPolicy {
                namespace: uuid::Uuid::from_u128(305),
            },
        )
        .unwrap();

    let mut first_digest = None;
    for _ in 0..5 {
        let report = run(config(), &registry, events.clone(), BuyBtc)
            .await
            .unwrap();
        assert!(report.historical_dataset.is_some());
        assert_eq!(report.trades.len(), 1);
        let comparison = available(report.portfolio_benchmark.as_ref().unwrap());
        assert_eq!(
            comparison.method,
            PortfolioBenchmarkMethod::FrictionlessBuyAndHold
        );
        assert_eq!((comparison.start_at, comparison.end_at), (at(0), at(1000)));
        assert_eq!(comparison.quote_currency, "USDT");
        assert_eq!(comparison.initial_strategy_equity, dec!(1000));
        assert_eq!(comparison.final_strategy_equity, dec!(1009.9));
        assert_eq!(comparison.strategy_return, dec!(0.0099));
        assert_eq!(comparison.cash_return, dec!(0));
        assert_eq!(comparison.excess_vs_cash, dec!(0.0099));
        assert_eq!(comparison.excess_vs_portfolio, dec!(-0.0301));
        assert_eq!(comparison.initial_benchmark_equity, dec!(1000));
        assert_eq!(comparison.final_benchmark_equity, dec!(1040));
        assert_eq!(comparison.portfolio_return, dec!(0.04));
        let [btc, eth] = comparison.instruments.as_slice() else {
            panic!("expected two instruments")
        };
        assert_eq!((&*btc.symbol, &*eth.symbol), (BTC, ETH));
        assert_eq!((btc.start_price, btc.end_price), (dec!(100), dec!(110)));
        assert_eq!(
            (btc.start_reference_at, btc.end_reference_at),
            (at(0), at(1000))
        );
        assert_eq!(
            (btc.weight, btc.initial_capital, btc.final_holding_value),
            (dec!(0.6), dec!(600), dec!(660))
        );
        assert_eq!(
            (eth.weight, eth.initial_capital, eth.final_holding_value),
            (dec!(0.4), dec!(400), dec!(380))
        );
        assert_eq!(
            (btc.instrument_return, eth.instrument_return),
            (dec!(0.10), dec!(-0.05))
        );
        assert_eq!(
            (btc.contribution, eth.contribution),
            (dec!(0.06), dec!(-0.02))
        );
        assert_eq!(
            comparison.portfolio_return,
            btc.contribution + eth.contribution
        );
        let digest = canonical_report_sha256(&report).unwrap();
        assert_eq!(&digest, first_digest.get_or_insert_with(|| digest.clone()));
    }
}

#[tokio::test]
async fn partial_cash_allocation_and_flat_asset() {
    let mut events = events();
    events[3] = trade(ETH, 1000, dec!(100));
    let evidence = evaluate(
        config(),
        events,
        policy(&[(BTC, dec!(0.5)), (ETH, dec!(0.25))]),
    )
    .await;
    let comparison = available(&evidence);
    assert_eq!(comparison.cash_weight, dec!(0.25));
    assert_eq!(comparison.cash_capital, dec!(250));
    assert_eq!(comparison.portfolio_return, dec!(0.05));
    assert_eq!(comparison.final_benchmark_equity, dec!(1050));
    assert_eq!(comparison.instruments[1].instrument_return, dec!(0));
    assert_eq!(comparison.instruments[1].contribution, dec!(0));
}

#[tokio::test]
async fn single_asset_equivalence_and_legacy_report_round_trip() {
    fn single_config() -> BacktestConfig {
        let mut config = config();
        config.instruments.truncate(1);
        config
    }
    let config = single_config();
    let events = vec![trade(BTC, 0, dec!(100)), trade(BTC, 1000, dec!(110))];
    let registry = registry(&config, &events);
    registry
        .register::<_, ()>(
            (),
            BenchmarkPolicy {
                symbol: BTC.into(),
                max_end_age_ms: 1000,
            },
        )
        .unwrap();
    let report = run(single_config(), &registry, events.clone(), Idle)
        .await
        .unwrap();
    let legacy = serde_json::to_value(&report).unwrap();
    assert!(legacy.get("portfolio_benchmark").is_none());
    let old: BacktestReport = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(legacy, serde_json::to_value(&old).unwrap());
    assert_eq!(
        canonical_report_sha256(&report).unwrap(),
        canonical_report_sha256(&old).unwrap()
    );

    let Some(BenchmarkEvidence::Available {
        comparison: single, ..
    }) = report.benchmark
    else {
        panic!("expected available legacy benchmark");
    };
    let evidence = evaluate(
        single_config(),
        events.clone(),
        policy(&[(BTC, Decimal::ONE)]),
    )
    .await;
    let portfolio = available(&evidence);
    assert_eq!(portfolio.portfolio_return, single.buy_hold_return);
    assert_eq!(portfolio.excess_vs_portfolio, single.excess_vs_buy_hold);
    assert_eq!(portfolio.strategy_return, single.strategy_return);
    assert_eq!(portfolio.instruments[0].end_price, single.end_price);
    assert_eq!(
        serde_json::from_value::<PortfolioBenchmarkEvidence>(
            serde_json::to_value(&evidence).unwrap()
        )
        .unwrap(),
        evidence
    );

    registry
        .register::<_, ()>((), policy(&[(BTC, Decimal::ONE)]))
        .unwrap();
    let error = run(config, &registry, events, Idle).await.unwrap_err();
    assert!(error.to_string().contains("mutually exclusive"));
}

#[tokio::test]
async fn requires_a_start_trade_for_each_asset_and_a_fresh_final_trade() {
    let mut missing = events();
    missing.remove(1);
    let allocations = policy(&[(BTC, dec!(0.6)), (ETH, dec!(0.4))]);
    unavailable(
        &evaluate(config(), missing, allocations.clone()).await,
        "missing starting trade for ETHUSDT",
    );

    let mut stale = events();
    stale.pop();
    stale.push(trade(BTC, 1001, dec!(110)));
    unavailable(
        &evaluate(config(), stale, allocations).await,
        "final trade for ETHUSDT is stale",
    );
}

#[tokio::test]
async fn bars_are_available_at_close_and_must_fit_the_verified_horizon() {
    let mut events = events();
    events.push(bar(1000));
    let registry = registry(&config(), &events);
    registry
        .register::<_, ()>((), EventLatencyPolicy { delay_ms: 1 })
        .unwrap();
    registry
        .register::<_, ()>((), policy(&[(BTC, dec!(0.6)), (ETH, dec!(0.4))]))
        .unwrap();
    let report = run(config(), &registry, events.clone(), Idle)
        .await
        .unwrap();
    let comparison = available(report.portfolio_benchmark.as_ref().unwrap());
    assert_eq!(comparison.end_at, at(2000));
    assert_eq!(comparison.instruments[0].end_price, dec!(110));

    // Manifest v1 orders by bar open. The benchmark refuses that order if
    // a following trade arrived before the bar finished.
    events.push(trade(BTC, 1500, dec!(120)));
    let regressing = self::registry(&config(), &events);
    regressing
        .register::<_, ()>((), EventLatencyPolicy { delay_ms: 1 })
        .unwrap();
    regressing
        .register::<_, ()>((), policy(&[(BTC, Decimal::ONE)]))
        .unwrap();
    let report = run(config(), &regressing, events.clone(), Idle)
        .await
        .unwrap();
    unavailable(
        report.portfolio_benchmark.as_ref().unwrap(),
        "observation time regressed",
    );

    events.truncate(4);
    events.push(bar(86_399_500));
    let future = self::registry(&config(), &events);
    future
        .register::<_, ()>((), EventLatencyPolicy { delay_ms: 1 })
        .unwrap();
    future
        .register::<_, ()>((), policy(&[(BTC, Decimal::ONE)]))
        .unwrap();
    let report = run(config(), &future, events, Idle).await.unwrap();
    unavailable(
        report.portfolio_benchmark.as_ref().unwrap(),
        "beyond the verified replay date range",
    );
}

#[tokio::test]
async fn rejects_invalid_weights_and_ambiguous_instruments() {
    for (allocations, reason) in [
        (vec![], "at least one"),
        (vec![(BTC, dec!(0))], "positive"),
        (vec![(BTC, dec!(-0.1))], "positive"),
        (vec![(BTC, dec!(1.1))], "at most one"),
        (
            vec![
                (BTC, dec!(0.6)),
                (ETH, dec!(0.4000000000000000000000000001)),
            ],
            "exceeds one",
        ),
        (vec![(BTC, dec!(0.5)), (BTC, dec!(0.5))], "duplicate"),
        (vec![("UNKNOWN", dec!(1))], "unknown"),
    ] {
        unavailable(
            &evaluate(config(), events(), policy(&allocations)).await,
            reason,
        );
    }

    let allocations = policy(&[(BTC, dec!(0.6)), (ETH, dec!(0.4))]);
    let mut ambiguous = config();
    let mut another_venue = instrument("ALT", BTC);
    another_venue.venue = Venue::Okx;
    ambiguous.instruments.push(another_venue);
    unavailable(
        &evaluate(ambiguous, events(), allocations.clone()).await,
        "ambiguous symbol",
    );

    let mut shared_base = config();
    shared_base.instruments[1].base_currency = Some("BTC".into());
    unavailable(
        &evaluate(shared_base, events(), allocations).await,
        "base-asset valuation",
    );
}

#[tokio::test]
async fn rejects_incompatible_economics_and_unverified_input() {
    let allocations = policy(&[(BTC, dec!(0.6)), (ETH, dec!(0.4))]);
    let mut mixed_quotes = config();
    mixed_quotes.instruments[1].quote_currency = Some("USDC".into());
    unavailable(
        &evaluate(mixed_quotes, events(), allocations.clone()).await,
        "quote USDT",
    );

    let mut leveraged = config();
    leveraged.instruments[1].asset_class = AssetClass::Perpetual;
    unavailable(
        &evaluate(leveraged, events(), allocations.clone()).await,
        "unit-multiplier Spot",
    );

    let mut multiplier = config();
    multiplier.instruments[1].multiplier = dec!(2);
    unavailable(
        &evaluate(multiplier, events(), allocations.clone()).await,
        "unit-multiplier Spot",
    );

    let mut inventory = config();
    inventory.initial_balances.insert("ETH".into(), dec!(1));
    unavailable(
        &evaluate(inventory, events(), allocations.clone()).await,
        "cash-only",
    );

    let registry = Registry::new();
    registry.register::<_, ()>((), allocations).unwrap();
    let report = run(config(), &registry, events(), Idle).await.unwrap();
    unavailable(
        report.portfolio_benchmark.as_ref().unwrap(),
        "requires verified replay",
    );
}
