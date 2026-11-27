//! Integration tests proving the registry-resolved `CompositeRiskChecker`
//! actually gates order submission and reacts to fills inside
//! `run_backtest`'s real event loop -- not just unit-testing
//! `apexes-risk`'s types in isolation.

use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, InMemoryMarketDataSource, ReplaySpeed, run_backtest,
};
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, PriceLevel, TradeEvent, Venue,
};
use apexes_risk::{CompositeRiskChecker, ListMode, SymbolVenueList};
use apexes_strategy::{OrderIntent, OrderRouting, Strategy, StrategyContext};
use chrono::{TimeZone, Utc};
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};

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

fn sample_events() -> Vec<MarketEvent> {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let depth_snapshot = MarketEvent::Depth(DepthUpdate {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        bids: vec![],
        asks: vec![PriceLevel {
            price: dec!(1),
            quantity: dec!(1000),
        }],
        sequence: Some(1),
        source_range: None,
        is_snapshot: true,
        timestamp: t0,
    });
    let trade = MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        price: dec!(100),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: t0 + chrono::Duration::seconds(1),
    });
    vec![depth_snapshot, trade]
}

fn base_config(events_range_end_day: chrono::NaiveDate) -> BacktestConfig {
    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(100000));
    BacktestConfig {
        instruments: vec![btc_usdt_instrument()],
        date_range: chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()..events_range_end_day,
        initial_balances,
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
        margin_tiers: HashMap::new(),
        market_condition_config: None,
        control: None,
    }
}

/// Buys 1 unit of BTCUSDT on the very first `MarketEvent` it sees, then
/// does nothing on subsequent events. Deterministic and side-effect-free
/// beyond that single order, so tests can assert exactly whether that
/// one order made it through.
struct BuyOnceStrategy {
    has_bought: bool,
}

impl Strategy for BuyOnceStrategy {
    fn on_market_event(
        &mut self,
        _event: &MarketEvent,
        _ctx: &StrategyContext,
    ) -> Vec<OrderIntent> {
        if self.has_bought {
            return vec![];
        }
        self.has_bought = true;
        vec![OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: apexes_core::Side::Buy,
            order_type: apexes_core::OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }]
    }
}

#[tokio::test]
async fn event_latency_executes_against_arrival_depth_and_counts_unarrived_orders() {
    let mut events = sample_events();
    let MarketEvent::Depth(mut replacement) = events[0].clone() else {
        panic!("fixture depth")
    };
    replacement.timestamp += chrono::Duration::milliseconds(500);
    replacement.asks[0].price = dec!(2);
    replacement.sequence = Some(2);
    events.insert(1, MarketEvent::Depth(replacement.clone()));
    let registry = apexes_core::registry::Registry::new();
    let policy = apexes_backtest::EventLatencyPolicy { delay_ms: 500 };
    registry
        .register::<apexes_backtest::EventLatencyPolicy, ()>((), policy)
        .unwrap();
    let report = run_backtest(
        base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()),
        &registry,
        InMemoryMarketDataSource::new(events.clone()),
        BuyOnceStrategy { has_bought: false },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].price, dec!(2));
    assert_eq!(report.trades[0].filled_at, replacement.timestamp);
    let evidence = report.event_latency.unwrap();
    assert_eq!(evidence.policy, policy);
    assert_eq!(
        (
            evidence.scheduled,
            evidence.arrived,
            evidence.expired_at_end
        ),
        (1, 1, 0)
    );
    let report = run_backtest(
        base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()),
        &registry,
        InMemoryMarketDataSource::new(vec![events.remove(0)]),
        BuyOnceStrategy { has_bought: false },
    )
    .await
    .unwrap();
    assert!(report.trades.is_empty());
    let evidence = report.event_latency.unwrap();
    assert_eq!(
        (
            evidence.scheduled,
            evidence.arrived,
            evidence.expired_at_end
        ),
        (1, 0, 1)
    );
}

#[tokio::test]
async fn event_latency_admits_twap_parent_then_dispatches_on_observed_events() {
    struct TwapOnce(BuyOnceStrategy);
    impl Strategy for TwapOnce {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            self.0
                .on_market_event(event, ctx)
                .into_iter()
                .map(|mut i| {
                    i.quantity = dec!(2);
                    i.execution_algo = Some(apexes_strategy::ExecutionAlgoSpec::Twap {
                        duration: std::time::Duration::from_secs(2),
                        slices: 2,
                    });
                    i
                })
                .collect()
        }
    }
    let mut events = sample_events();
    let MarketEvent::Depth(first) = events[0].clone() else {
        panic!("depth fixture")
    };
    for (offset, price, seq) in [
        (500, dec!(2), 2),
        (600, dec!(3), 3),
        (1100, dec!(4), 4),
        (1600, dec!(5), 5),
        (2100, dec!(6), 6),
    ] {
        let mut d = first.clone();
        d.timestamp += chrono::Duration::milliseconds(offset);
        d.sequence = Some(seq);
        d.asks[0].price = price;
        events.push(MarketEvent::Depth(d));
    }
    events.sort_by_key(|e| match e {
        MarketEvent::Depth(d) => d.timestamp,
        MarketEvent::Trade(t) => t.timestamp,
        _ => unreachable!(),
    });
    let registry = apexes_core::registry::Registry::new();
    registry
        .register::<apexes_backtest::EventLatencyPolicy, ()>(
            (),
            apexes_backtest::EventLatencyPolicy { delay_ms: 500 },
        )
        .unwrap();
    let report = run_backtest(
        base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()),
        &registry,
        InMemoryMarketDataSource::new(events.clone()),
        TwapOnce(BuyOnceStrategy { has_bought: false }),
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 2);
    assert_eq!(
        report.trades.iter().map(|f| f.price).collect::<Vec<_>>(),
        vec![dec!(4), dec!(6)]
    );
    assert_eq!(
        report.trades[0].filled_at,
        first.timestamp + chrono::Duration::milliseconds(1100)
    );
    assert_eq!(
        report.trades[1].filled_at,
        first.timestamp + chrono::Duration::milliseconds(2100)
    );
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (latency.scheduled, latency.arrived, latency.expired_at_end),
        (1, 1, 0)
    );

    let children = latency.child_orders.unwrap();
    assert_eq!(
        (
            children.scheduled,
            children.arrived,
            children.expired_at_end
        ),
        (2, 2, 0)
    );

    // End at the second child's submission, without inventing another event.
    events.pop();
    let report = run_backtest(
        base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()),
        &registry,
        InMemoryMarketDataSource::new(events),
        TwapOnce(BuyOnceStrategy { has_bought: false }),
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
    let children = report.event_latency.unwrap().child_orders.unwrap();
    assert_eq!(
        (
            children.scheduled,
            children.arrived,
            children.expired_at_end
        ),
        (2, 1, 1)
    );
}

#[tokio::test]
async fn event_latency_rechecks_risk_at_arrival() {
    use apexes_core::{Order, OrderId};
    use apexes_risk::{PreTradeCheck, RiskViolation};
    use apexes_strategy::Position;
    use rust_decimal::Decimal;
    struct Cutoff(chrono::DateTime<Utc>);
    impl PreTradeCheck for Cutoff {
        fn check(
            &self,
            _: &OrderIntent,
            _: &HashMap<String, Position>,
            _: &HashMap<String, Decimal>,
            _: &HashMap<OrderId, Order>,
            now: chrono::DateTime<Utc>,
        ) -> Result<(), Vec<RiskViolation>> {
            if now >= self.0 {
                Err(vec![RiskViolation::OrderQuantityExceeded {
                    actual: dec!(1),
                    limit: dec!(0),
                }])
            } else {
                Ok(())
            }
        }
    }
    let registry = apexes_core::registry::Registry::new();
    registry
        .register::<apexes_backtest::EventLatencyPolicy, ()>(
            (),
            apexes_backtest::EventLatencyPolicy { delay_ms: 500 },
        )
        .unwrap();
    let cutoff = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 1).unwrap();
    let checker = CompositeRiskChecker::new().with_pre_trade_check(Box::new(Cutoff(cutoff)));
    registry
        .register::<std::sync::Arc<CompositeRiskChecker>, ()>((), std::sync::Arc::new(checker))
        .unwrap();
    let report = run_backtest(
        base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()),
        &registry,
        InMemoryMarketDataSource::new(sample_events()),
        BuyOnceStrategy { has_bought: false },
    )
    .await
    .unwrap();
    assert!(report.trades.is_empty());
    let evidence = report.event_latency.unwrap();
    assert_eq!(
        (
            evidence.scheduled,
            evidence.arrived,
            evidence.expired_at_end
        ),
        (1, 1, 0)
    );
}

#[tokio::test]
async fn risk_checker_none_behaves_identically_to_no_risk_checking() {
    let config = base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    let registry = apexes_core::registry::Registry::new();
    let market_data = InMemoryMarketDataSource::new(sample_events());
    let report = run_backtest(
        config,
        &registry,
        market_data,
        BuyOnceStrategy { has_bought: false },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
}

#[tokio::test]
async fn blacklisted_symbol_is_rejected_before_reaching_the_matching_engine() {
    let mut blacklist = HashSet::new();
    blacklist.insert("BTCUSDT".to_string());
    let risk_checker = CompositeRiskChecker::new().with_pre_trade_check(Box::new(
        SymbolVenueList::new(ListMode::Blacklist(blacklist), ListMode::Disabled),
    ));
    let registry = apexes_core::registry::Registry::new();
    registry
        .register::<std::sync::Arc<CompositeRiskChecker>, ()>((), std::sync::Arc::new(risk_checker))
        .unwrap();

    let config = base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    let market_data = InMemoryMarketDataSource::new(sample_events());
    let report = run_backtest(
        config,
        &registry,
        market_data,
        BuyOnceStrategy { has_bought: false },
    )
    .await
    .unwrap();

    assert!(report.trades.is_empty());
}

#[tokio::test]
async fn non_blacklisted_symbol_still_trades_normally_through_a_configured_risk_checker() {
    let mut blacklist = HashSet::new();
    blacklist.insert("ETHUSDT".to_string()); // a different symbol -- BTCUSDT is unaffected.
    let risk_checker = CompositeRiskChecker::new().with_pre_trade_check(Box::new(
        SymbolVenueList::new(ListMode::Blacklist(blacklist), ListMode::Disabled),
    ));
    let registry = apexes_core::registry::Registry::new();
    registry
        .register::<std::sync::Arc<CompositeRiskChecker>, ()>((), std::sync::Arc::new(risk_checker))
        .unwrap();

    let config = base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    let market_data = InMemoryMarketDataSource::new(sample_events());
    let report = run_backtest(
        config,
        &registry,
        market_data,
        BuyOnceStrategy { has_bought: false },
    )
    .await
    .unwrap();

    assert_eq!(report.trades.len(), 1);
}

struct ShortTwapOnce {
    inner: BuyOnceStrategy,
    slices: u32,
}

impl Strategy for ShortTwapOnce {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent> {
        self.inner
            .on_market_event(event, ctx)
            .into_iter()
            .map(|mut intent| {
                intent.quantity = rust_decimal::Decimal::from(self.slices);
                intent.execution_algo = Some(apexes_strategy::ExecutionAlgoSpec::Twap {
                    duration: std::time::Duration::from_millis(100 * u64::from(self.slices)),
                    slices: self.slices,
                });
                intent
            })
            .collect()
    }
}

fn child_risk_events(price: rust_decimal::Decimal) -> Vec<MarketEvent> {
    let MarketEvent::Depth(first) = sample_events().remove(0) else {
        unreachable!()
    };
    let mut events = vec![MarketEvent::Depth(first.clone())];
    for (offset, price, seq) in [
        (500, dec!(1), 2),
        (600, dec!(1), 3),
        (700, dec!(1), 4),
        (1200, price, 5),
    ] {
        let mut next = first.clone();
        next.timestamp += chrono::Duration::milliseconds(offset);
        next.sequence = Some(seq);
        next.asks[0].price = price;
        events.push(MarketEvent::Depth(next));
    }
    events
}

fn child_risk_registry(limits: apexes_risk::RiskLimits) -> apexes_core::registry::Registry {
    let registry = apexes_core::registry::Registry::new();
    registry
        .register::<apexes_backtest::EventLatencyPolicy, ()>(
            (),
            apexes_backtest::EventLatencyPolicy { delay_ms: 500 },
        )
        .unwrap();
    registry
        .register::<std::sync::Arc<CompositeRiskChecker>, ()>(
            (),
            std::sync::Arc::new(
                CompositeRiskChecker::new()
                    .with_pre_trade_check(Box::new(apexes_risk::LimitChecker::new(limits))),
            ),
        )
        .unwrap();
    registry
}

#[tokio::test]
async fn ems_child_rechecks_notional_against_arrival_depth() {
    let registry = child_risk_registry(apexes_risk::RiskLimits {
        max_order_notional: Some(dec!(50)),
        ..Default::default()
    });
    let report = run_backtest(
        base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()),
        &registry,
        InMemoryMarketDataSource::new(child_risk_events(dec!(100))),
        ShortTwapOnce {
            inner: BuyOnceStrategy { has_bought: false },
            slices: 1,
        },
    )
    .await
    .unwrap();
    assert!(report.trades.is_empty());
    let children = report.event_latency.unwrap().child_orders.unwrap();
    assert_eq!(
        (
            children.scheduled,
            children.arrived,
            children.rejected_at_arrival
        ),
        (1, 1, 1)
    );
}

#[tokio::test]
async fn competing_ems_children_share_arrival_capital_without_recharging_parent_quota() {
    let registry = child_risk_registry(apexes_risk::RiskLimits {
        max_daily_trade_count: Some(1),
        max_daily_volume: Some(dec!(2)),
        ..Default::default()
    });
    let mut config = base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    config.initial_balances.insert("USDT".into(), dec!(15));
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(child_risk_events(dec!(10))),
        ShortTwapOnce {
            inner: BuyOnceStrategy { has_bought: false },
            slices: 2,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].price, dec!(10));
    let children = report.event_latency.unwrap().child_orders.unwrap();
    assert_eq!(
        (
            children.scheduled,
            children.arrived,
            children.rejected_at_arrival
        ),
        (2, 2, 1)
    );
}

struct SharedQuoteBuys(bool);

impl Strategy for SharedQuoteBuys {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent> {
        if self.0 || !matches!(event, MarketEvent::Depth(depth) if depth.symbol == "ETHUSDT") {
            return vec![];
        }
        self.0 = true;
        let mut first = BuyOnceStrategy { has_bought: false }
            .on_market_event(event, ctx)
            .remove(0);
        first.quantity = dec!(1);
        let mut second = first.clone();
        second.symbol = "ETHUSDT".into();
        vec![first, second]
    }
}

fn shared_quote_fixture(
    balance: rust_decimal::Decimal,
    btc_arrival_price: rust_decimal::Decimal,
) -> (BacktestConfig, Vec<MarketEvent>) {
    let mut config = base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    config.initial_balances.insert("USDT".into(), balance);
    config.instruments.push(
        Instrument::new(
            "ETHUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            Some("ETH".into()),
            Some("USDT".into()),
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap(),
    );
    let MarketEvent::Depth(mut btc) = sample_events().remove(0) else {
        unreachable!()
    };
    btc.asks[0].price = dec!(6);
    let mut eth = btc.clone();
    eth.symbol = "ETHUSDT".into();
    let mut later = btc.clone();
    later.timestamp += chrono::Duration::milliseconds(500);
    later.sequence = Some(2);
    later.asks[0].price = btc_arrival_price;
    (
        config,
        vec![
            MarketEvent::Depth(btc),
            MarketEvent::Depth(eth),
            MarketEvent::Depth(later),
        ],
    )
}

#[tokio::test]
async fn in_transit_orders_share_quote_capital_across_symbols() {
    let (config, events) = shared_quote_fixture(dec!(10), dec!(6));
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        SharedQuoteBuys(false),
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].symbol, "BTCUSDT");
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (
            latency.scheduled,
            latency.arrived,
            latency.refused,
            latency.capital_refused
        ),
        (1, 1, 1, 1)
    );
}

#[tokio::test]
async fn price_increase_cannot_consume_another_due_order_hold() {
    let (config, events) = shared_quote_fixture(dec!(12), dec!(7));
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        SharedQuoteBuys(false),
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].symbol, "ETHUSDT");
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (
            latency.scheduled,
            latency.arrived,
            latency.refused,
            latency.capital_refused
        ),
        (2, 2, 0, 1)
    );
}

#[tokio::test]
async fn unarrived_market_intents_release_holds_at_end() {
    let (config, mut events) = shared_quote_fixture(dec!(12), dec!(6));
    events.pop();
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        SharedQuoteBuys(false),
    )
    .await
    .unwrap();
    assert!(report.trades.is_empty());
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (
            latency.scheduled,
            latency.expired_at_end,
            latency.capital_released_at_end
        ),
        (2, 2, 2)
    );
}

struct ParentThenDirect {
    parent: bool,
    direct: bool,
}

impl Strategy for ParentThenDirect {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent> {
        if !self.parent {
            self.parent = true;
            return ShortTwapOnce {
                inner: BuyOnceStrategy { has_bought: false },
                slices: 1,
            }
            .on_market_event(event, ctx);
        }
        if !self.direct && matches!(event, MarketEvent::Depth(depth) if depth.symbol == "ETHUSDT") {
            self.direct = true;
            let mut order = BuyOnceStrategy { has_bought: false }
                .on_market_event(event, ctx)
                .remove(0);
            order.symbol = "ETHUSDT".into();
            return vec![order];
        }
        vec![]
    }
}

#[tokio::test]
async fn arriving_child_cannot_spend_pending_direct_order_capital() {
    let (config, _) = shared_quote_fixture(dec!(15), dec!(6));
    let MarketEvent::Depth(first) = sample_events().remove(0) else {
        unreachable!()
    };
    let mut events = vec![MarketEvent::Depth(first.clone())];
    for (offset, symbol, price, seq) in [
        (500, "BTCUSDT", dec!(1), 2),
        (600, "BTCUSDT", dec!(1), 3),
        (700, "ETHUSDT", dec!(6), 1),
        (1100, "BTCUSDT", dec!(10), 4),
        (1200, "ETHUSDT", dec!(6), 2),
    ] {
        let mut d = first.clone();
        d.symbol = symbol.into();
        d.timestamp += chrono::Duration::milliseconds(offset);
        d.sequence = Some(seq);
        d.asks[0].price = price;
        events.push(MarketEvent::Depth(d));
    }
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        ParentThenDirect {
            parent: false,
            direct: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].symbol, "ETHUSDT");
    let children = report.event_latency.unwrap().child_orders.unwrap();
    assert_eq!(
        (
            children.scheduled,
            children.arrived,
            children.rejected_at_arrival
        ),
        (1, 1, 1)
    );
}

#[tokio::test]
async fn transit_buy_reservation_includes_taker_fee() {
    let (mut config, events) = shared_quote_fixture(dec!(6), dec!(6));
    config.cost_model.fee_schedule = Box::new(FlatFeeSchedule {
        maker_bps: dec!(0),
        taker_bps: dec!(10),
    });
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        SharedQuoteBuys(false),
    )
    .await
    .unwrap();
    assert!(report.trades.is_empty());
    let latency = report.event_latency.unwrap();
    assert_eq!((latency.scheduled, latency.capital_refused), (0, 2));
}

#[tokio::test]
async fn admitted_parent_keeps_capital_reserved_against_new_direct_order() {
    let (config, _) = shared_quote_fixture(dec!(15), dec!(6));
    let MarketEvent::Depth(mut first) = sample_events().remove(0) else {
        unreachable!()
    };
    first.asks[0].price = dec!(12);
    let mut events = vec![MarketEvent::Depth(first.clone())];
    for (offset, symbol, price, seq) in [
        (500, "BTCUSDT", dec!(12), 2),
        (600, "BTCUSDT", dec!(12), 3),
        (700, "ETHUSDT", dec!(6), 1),
        (1100, "BTCUSDT", dec!(10), 4),
        (1200, "ETHUSDT", dec!(6), 2),
    ] {
        let mut d = first.clone();
        d.symbol = symbol.into();
        d.timestamp += chrono::Duration::milliseconds(offset);
        d.sequence = Some(seq);
        d.asks[0].price = price;
        events.push(MarketEvent::Depth(d));
    }
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        ParentThenDirect {
            parent: false,
            direct: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].symbol, "BTCUSDT");
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (
            latency.capital_refused,
            latency.parent_capital_reserved,
            latency.parent_capital_released,
            latency.parent_capital_released_at_end
        ),
        (1, 1, 1, 0)
    );
}

#[tokio::test]
async fn pending_parent_budget_is_released_at_replay_end() {
    let mut events = child_risk_events(dec!(10));
    events.truncate(3);
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()),
        &registry,
        InMemoryMarketDataSource::new(events),
        ShortTwapOnce {
            inner: BuyOnceStrategy { has_bought: false },
            slices: 2,
        },
    )
    .await
    .unwrap();
    assert!(report.trades.is_empty());
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (
            latency.parent_capital_reserved,
            latency.parent_capital_released,
            latency.parent_capital_released_at_end
        ),
        (1, 0, 1)
    );
    assert_eq!(latency.child_orders.unwrap().expired_at_end, 1);
}

struct RestingThenDirect {
    first: bool,
    second: bool,
    stop: bool,
}

impl Strategy for RestingThenDirect {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent> {
        if !self.first {
            self.first = true;
            let mut order = BuyOnceStrategy { has_bought: false }
                .on_market_event(event, ctx)
                .remove(0);
            if self.stop {
                order.order_type = apexes_core::OrderType::Stop;
                order.stop_price = Some(dec!(6));
            } else {
                order.order_type = apexes_core::OrderType::Limit;
                order.price = Some(dec!(6));
                order.quantity = dec!(2);
            }
            return vec![order];
        }
        if !self.second && matches!(event, MarketEvent::Depth(depth) if depth.symbol == "ETHUSDT") {
            self.second = true;
            let mut order = BuyOnceStrategy { has_bought: false }
                .on_market_event(event, ctx)
                .remove(0);
            order.symbol = "ETHUSDT".into();
            return vec![order];
        }
        vec![]
    }
}

fn resting_capital_events(stop: bool) -> Vec<MarketEvent> {
    let MarketEvent::Depth(mut first) = sample_events().remove(0) else {
        unreachable!()
    };
    first.asks[0].price = if stop { dec!(5) } else { dec!(10) };
    let mut events = vec![MarketEvent::Depth(first.clone())];
    for (offset, symbol, price, seq) in [
        (500, "BTCUSDT", first.asks[0].price, 2),
        (600, "ETHUSDT", dec!(6), 1),
        (700, "BTCUSDT", dec!(10), 3),
    ] {
        let mut d = first.clone();
        d.symbol = symbol.into();
        d.timestamp += chrono::Duration::milliseconds(offset);
        d.asks[0].price = price;
        d.sequence = Some(seq);
        events.push(MarketEvent::Depth(d));
    }
    for offset in [800, 1200] {
        events.push(MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: if stop { dec!(10) } else { dec!(6) },
            quantity: dec!(1),
            side: Some(apexes_core::Side::Sell),
            trade_id: None,
            timestamp: first.timestamp + chrono::Duration::milliseconds(offset),
        }));
    }
    events
}

#[tokio::test]
async fn resting_limit_reserves_quote_through_partial_fill_and_completion() {
    let (config, _) = shared_quote_fixture(dec!(15), dec!(6));
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(resting_capital_events(false)),
        RestingThenDirect {
            first: false,
            second: false,
            stop: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 2);
    assert!(
        report
            .trades
            .iter()
            .all(|f| f.symbol == "BTCUSDT" && f.price == dec!(6) && f.quantity == dec!(1))
    );
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (
            latency.capital_refused,
            latency.resting_capital_reserved,
            latency.resting_capital_released,
            latency.resting_capital_released_at_end,
            latency.resting_capital_refused
        ),
        (1, 1, 1, 0, 0)
    );
}

#[tokio::test]
async fn gapped_stop_cannot_spend_pending_direct_order_hold() {
    let (config, _) = shared_quote_fixture(dec!(12), dec!(6));
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(resting_capital_events(true)),
        RestingThenDirect {
            first: false,
            second: false,
            stop: true,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].symbol, "ETHUSDT");
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (
            latency.resting_capital_reserved,
            latency.resting_capital_released,
            latency.resting_capital_refused
        ),
        (1, 1, 1)
    );
}

#[tokio::test]
async fn resting_remainder_reservation_released_at_replay_end() {
    let (config, _) = shared_quote_fixture(dec!(15), dec!(6));
    let registry = child_risk_registry(Default::default());
    let mut events = resting_capital_events(false);
    events.pop();
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        RestingThenDirect {
            first: false,
            second: false,
            stop: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (
            latency.resting_capital_reserved,
            latency.resting_capital_released,
            latency.resting_capital_released_at_end
        ),
        (1, 0, 1)
    );
}

struct LeveragedOnce {
    sent: bool,
    side: apexes_core::Side,
    order_type: apexes_core::OrderType,
    leverage: rust_decimal::Decimal,
    algo: bool,
    compete: bool,
}

impl Strategy for LeveragedOnce {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent> {
        if self.sent
            || (self.compete && !matches!(event, MarketEvent::Depth(d) if d.symbol == "ETHUSDT"))
        {
            return vec![];
        }
        self.sent = true;
        let mut intent = BuyOnceStrategy { has_bought: false }
            .on_market_event(event, ctx)
            .remove(0);
        intent.asset_class = AssetClass::Perpetual;
        intent.side = self.side;
        intent.leverage = Some(self.leverage);
        intent.margin_mode = Some(apexes_core::MarginMode::Isolated);
        intent.order_type = self.order_type;
        if self.order_type == apexes_core::OrderType::Limit {
            intent.price = Some(dec!(10));
        }
        if self.algo {
            intent.execution_algo = Some(apexes_strategy::ExecutionAlgoSpec::Twap {
                duration: std::time::Duration::from_millis(100),
                slices: 1,
            });
        }
        let mut intents = vec![intent];
        if self.compete {
            let mut spot = BuyOnceStrategy { has_bought: false }
                .on_market_event(event, ctx)
                .remove(0);
            spot.symbol = "ETHUSDT".into();
            intents.push(spot);
        }
        intents
    }
}

fn leveraged_fixture(
    balance: rust_decimal::Decimal,
    arrival_bid: rust_decimal::Decimal,
) -> (BacktestConfig, Vec<MarketEvent>) {
    let (mut config, mut events) = shared_quote_fixture(balance, dec!(10));
    config.instruments[0].asset_class = AssetClass::Perpetual;
    for event in &mut events {
        if let MarketEvent::Depth(d) = event
            && d.symbol == "BTCUSDT"
        {
            d.asks[0].price = dec!(10);
            d.bids = vec![PriceLevel {
                price: if d.sequence == Some(2) {
                    arrival_bid
                } else {
                    dec!(9)
                },
                quantity: dec!(100),
            }];
        }
    }
    (config, events)
}

#[tokio::test]
async fn leveraged_buy_reserves_margin_instead_of_full_notional() {
    let (config, events) = leveraged_fixture(dec!(5), dec!(9));
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        LeveragedOnce {
            sent: false,
            side: apexes_core::Side::Buy,
            order_type: apexes_core::OrderType::Market,
            leverage: dec!(2),
            algo: false,
            compete: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].leverage, Some(dec!(2)));
    assert_eq!(report.trades[0].price, dec!(10));
}

#[tokio::test]
async fn leveraged_short_and_spot_buy_share_quote_reservations() {
    let (config, events) = leveraged_fixture(dec!(10), dec!(9));
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        LeveragedOnce {
            sent: false,
            side: apexes_core::Side::Sell,
            order_type: apexes_core::OrderType::Market,
            leverage: dec!(2),
            algo: false,
            compete: true,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].symbol, "BTCUSDT");
    assert_eq!(report.trades[0].side, apexes_core::Side::Sell);
    let latency = report.event_latency.unwrap();
    assert_eq!((latency.scheduled, latency.capital_refused), (1, 1));
}

#[tokio::test]
async fn crossing_leveraged_sell_is_rejected_if_actual_margin_exceeds_limit_estimate() {
    let (config, events) = leveraged_fixture(dec!(6), dec!(20));
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        LeveragedOnce {
            sent: false,
            side: apexes_core::Side::Sell,
            order_type: apexes_core::OrderType::Limit,
            leverage: dec!(2),
            algo: false,
            compete: false,
        },
    )
    .await
    .unwrap();
    assert!(report.trades.is_empty());
    assert_eq!(report.event_latency.unwrap().resting_capital_reserved, 0);
}

#[tokio::test]
async fn leveraged_parent_keeps_margin_until_child_arrival() {
    let (config, mut events) = leveraged_fixture(dec!(10), dec!(9));
    let MarketEvent::Depth(first) = events[0].clone() else {
        unreachable!()
    };
    for (offset, seq) in [(600, 3), (1100, 4)] {
        let mut d = first.clone();
        d.timestamp += chrono::Duration::milliseconds(offset);
        d.sequence = Some(seq);
        events.push(MarketEvent::Depth(d));
    }
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        LeveragedOnce {
            sent: false,
            side: apexes_core::Side::Sell,
            order_type: apexes_core::OrderType::Market,
            leverage: dec!(2),
            algo: true,
            compete: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].leverage, Some(dec!(2)));
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (
            latency.parent_capital_reserved,
            latency.parent_capital_released
        ),
        (1, 1)
    );
    assert_eq!(latency.child_orders.unwrap().arrived, 1);
}

#[tokio::test]
async fn leveraged_reservation_refuses_invalid_leverage_and_unfunded_fees() {
    for (leverage, balance) in [(dec!(0), dec!(100)), (dec!(2), dec!(5))] {
        let (mut config, events) = leveraged_fixture(balance, dec!(9));
        config.cost_model.fee_schedule = Box::new(FlatFeeSchedule {
            maker_bps: dec!(0),
            taker_bps: dec!(10),
        });
        let registry = child_risk_registry(Default::default());
        let report = run_backtest(
            config,
            &registry,
            InMemoryMarketDataSource::new(events),
            LeveragedOnce {
                sent: false,
                side: apexes_core::Side::Buy,
                order_type: apexes_core::OrderType::Market,
                leverage,
                algo: false,
                compete: false,
            },
        )
        .await
        .unwrap();
        assert!(report.trades.is_empty());
        assert_eq!(report.event_latency.unwrap().capital_refused, 1);
    }
}

#[tokio::test]
async fn leveraged_resting_sell_reserves_margin_until_terminal_fill() {
    let (mut config, mut events) = leveraged_fixture(dec!(6), dec!(9));
    // All observed prices must remain compatible with the passive sell.
    config.cost_model.fee_schedule = Box::new(FlatFeeSchedule {
        maker_bps: dec!(10),
        taker_bps: dec!(20),
    });
    let MarketEvent::Depth(first) = events[0].clone() else {
        unreachable!()
    };
    events.push(MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        price: dec!(10),
        quantity: dec!(0.5),
        side: Some(apexes_core::Side::Buy),
        trade_id: None,
        timestamp: first.timestamp + chrono::Duration::milliseconds(600),
    }));
    events.push(MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        price: dec!(10),
        quantity: dec!(0.5),
        side: Some(apexes_core::Side::Buy),
        trade_id: None,
        timestamp: first.timestamp + chrono::Duration::milliseconds(700),
    }));
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        LeveragedOnce {
            sent: false,
            side: apexes_core::Side::Sell,
            order_type: apexes_core::OrderType::Limit,
            leverage: dec!(2),
            algo: false,
            compete: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 2);
    assert!(
        report
            .trades
            .iter()
            .all(|f| f.quantity == dec!(0.5) && f.leverage == Some(dec!(2)))
    );
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (
            latency.resting_capital_reserved,
            latency.resting_capital_released,
            latency.resting_capital_refused
        ),
        (1, 1, 0)
    );
}

/// Admit a long, an EMS parent, a resting order, and a pending direct
/// order in successive events. The emergency arrives exactly when the
/// first EMS child would otherwise execute.
struct EmergencyCompetition {
    start: chrono::DateTime<Utc>,
}

impl Strategy for EmergencyCompetition {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent> {
        let offset = (ctx.now - self.start).num_milliseconds();
        if !matches!(offset, 0 | 500 | 1000 | 1500 | 1600) {
            return vec![];
        }
        let mut intent = BuyOnceStrategy { has_bought: false }
            .on_market_event(event, ctx)
            .remove(0);
        intent.asset_class = AssetClass::Perpetual;
        intent.leverage = Some(dec!(10));
        intent.margin_mode = Some(apexes_core::MarginMode::Isolated);
        match offset {
            500 => {
                intent.execution_algo = Some(apexes_strategy::ExecutionAlgoSpec::Twap {
                    duration: std::time::Duration::from_secs(1),
                    slices: 1,
                })
            }
            1000 => {
                intent.order_type = apexes_core::OrderType::Limit;
                intent.price = Some(dec!(95));
            }
            _ => {}
        }
        vec![intent]
    }
}

fn emergency_competition_fixture(
    jump: bool,
) -> (BacktestConfig, Vec<MarketEvent>, chrono::DateTime<Utc>) {
    let (mut config, _) = leveraged_fixture(dec!(1000), dec!(99));
    let MarketEvent::Depth(mut depth) = sample_events().remove(0) else {
        unreachable!()
    };
    let start = depth.timestamp;
    depth.asks[0].price = dec!(100);
    depth.bids = vec![PriceLevel {
        price: dec!(99),
        quantity: dec!(100),
    }];
    let mut events = Vec::new();
    for (seq, offset) in [0, 500, 1000, 1100, 1500].into_iter().enumerate() {
        let mut next = depth.clone();
        next.timestamp = start + chrono::Duration::milliseconds(offset);
        next.sequence = Some(seq as u64 + 1);
        events.push(MarketEvent::Depth(next));
        if jump && offset == 0 {
            // Establish the prior price before the extreme print.
            events.push(MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".into(),
                venue: Venue::Binance,
                price: dec!(100),
                quantity: dec!(1),
                side: Some(apexes_core::Side::Sell),
                trade_id: None,
                timestamp: start + chrono::Duration::milliseconds(1),
            }));
        }
    }
    if jump {
        config.market_condition_config = Some(apexes_risk::MarketConditionConfig {
            volatility_threshold: dec!(1000000),
            jump_threshold_pct: dec!(0.1),
            sensitivity: dec!(1),
            volatility_window: 20,
            price_source: apexes_risk::PriceSource::Trade,
            account_equity_volatility_threshold: None,
            include_spot: true,
        });
        // The same trade would have matched the resting buy at 95.
        events.push(MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: dec!(70),
            quantity: dec!(10),
            side: Some(apexes_core::Side::Sell),
            trade_id: None,
            timestamp: start + chrono::Duration::milliseconds(1600),
        }));
    } else {
        config.margin_tiers.insert(
            "BTCUSDT".into(),
            apexes_strategy::MarginTierTable {
                tiers: vec![apexes_strategy::MarginTier {
                    notional_floor: dec!(0),
                    max_leverage: dec!(20),
                    maintenance_margin_rate: dec!(0.005),
                    maintenance_amount: dec!(0),
                }],
            },
        );
        events.push(MarketEvent::MarkPrice(apexes_core::MarkPriceUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            mark_price: dec!(89),
            timestamp: start + chrono::Duration::milliseconds(1600),
        }));
    }
    events.push(MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".into(),
        venue: Venue::Binance,
        price: dec!(95),
        quantity: dec!(10),
        side: Some(apexes_core::Side::Sell),
        trade_id: None,
        timestamp: start + chrono::Duration::milliseconds(2100),
    }));
    (config, events, start)
}

async fn assert_emergency_preempts_competing_orders(jump: bool) {
    let (config, events, start) = emergency_competition_fixture(jump);
    let registry = child_risk_registry(Default::default());
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        EmergencyCompetition { start },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 2, "{:?}", report.trades);
    assert_eq!(report.trades[0].side, apexes_core::Side::Buy);
    assert_eq!(report.trades[1].side, apexes_core::Side::Sell);
    assert_eq!(report.trades[1].quantity, dec!(1));
    assert_eq!(
        report.trades[1].filled_at,
        start + chrono::Duration::milliseconds(1600)
    );
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (
            latency.emergency_events,
            latency.emergency_transit_cancelled,
            latency.emergency_resting_cancelled,
            latency.emergency_parents_cancelled,
            latency.emergency_intents_refused,
            latency.emergency_cancel_failures
        ),
        (1, 1, 1, 1, 1, 0)
    );
    assert_eq!(
        (
            latency.parent_capital_released,
            latency.resting_capital_released,
            latency.capital_released_at_end,
            latency.parent_capital_released_at_end,
            latency.resting_capital_released_at_end
        ),
        (1, 1, 0, 0, 0)
    );
    let children = latency.child_orders.unwrap();
    assert_eq!(
        (
            children.scheduled,
            children.arrived,
            children.cancelled_before_arrival
        ),
        (1, 0, 1)
    );
}

#[tokio::test]
async fn margin_liquidation_preempts_due_child_resting_and_transit_orders() {
    assert_emergency_preempts_competing_orders(false).await;
}

#[tokio::test]
async fn extreme_trade_preempts_maker_fill_and_due_child() {
    assert_emergency_preempts_competing_orders(true).await;
}

struct ReduceSequence {
    start: chrono::DateTime<Utc>,
    opening_side: apexes_core::Side,
    opening_quantity: rust_decimal::Decimal,
    closing_quantity: rust_decimal::Decimal,
    kind: apexes_core::OrderType,
    algo: bool,
    competing_close: bool,
}

impl Strategy for ReduceSequence {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent> {
        let offset = (ctx.now - self.start).num_milliseconds();
        if offset != 0 && offset != 500 && !(self.competing_close && offset == 1100) {
            return vec![];
        }
        let mut intent = BuyOnceStrategy { has_bought: false }
            .on_market_event(event, ctx)
            .remove(0);
        intent.asset_class = AssetClass::Perpetual;
        intent.leverage = Some(dec!(2));
        intent.margin_mode = Some(apexes_core::MarginMode::Isolated);
        if offset == 0 {
            intent.side = self.opening_side;
            intent.quantity = self.opening_quantity;
        } else {
            intent.side = match self.opening_side {
                apexes_core::Side::Buy => apexes_core::Side::Sell,
                apexes_core::Side::Sell => apexes_core::Side::Buy,
            };
            intent.quantity = self.closing_quantity;
            if offset == 500 {
                intent.order_type = self.kind;
                if self.kind == apexes_core::OrderType::Limit {
                    intent.price = Some(dec!(10));
                }
                if self.algo {
                    intent.execution_algo = Some(apexes_strategy::ExecutionAlgoSpec::Twap {
                        duration: std::time::Duration::from_millis(200),
                        slices: 2,
                    });
                }
            }
        }
        vec![intent]
    }
}

fn reducing_fixture(
    balance: rust_decimal::Decimal,
) -> (BacktestConfig, Vec<MarketEvent>, chrono::DateTime<Utc>) {
    let (config, events) = leveraged_fixture(balance, dec!(9));
    let MarketEvent::Depth(depth) = events[0].clone() else {
        unreachable!()
    };
    let start = depth.timestamp;
    let mut events = Vec::new();
    for (seq, offset) in [0, 500, 1000, 1100, 1200, 1600, 1700, 1800, 2100]
        .into_iter()
        .enumerate()
    {
        let mut next = depth.clone();
        next.timestamp = start + chrono::Duration::milliseconds(offset);
        next.sequence = Some(seq as u64 + 1);
        events.push(MarketEvent::Depth(next));
    }
    (config, events, start)
}

#[tokio::test]
async fn fully_allocated_long_can_reduce_without_new_opening_margin() {
    let (config, events, start) = reducing_fixture(dec!(5));
    let report = run_backtest(
        config,
        &child_risk_registry(Default::default()),
        InMemoryMarketDataSource::new(events),
        ReduceSequence {
            start,
            opening_side: apexes_core::Side::Buy,
            opening_quantity: dec!(1),
            closing_quantity: dec!(0.5),
            kind: apexes_core::OrderType::Market,
            algo: false,
            competing_close: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 2);
    assert_eq!(report.trades[1].side, apexes_core::Side::Sell);
    assert_eq!(report.trades[1].quantity, dec!(0.5));
    assert_eq!(report.event_latency.unwrap().capital_refused, 0);
}

#[tokio::test]
async fn fully_allocated_short_can_close_without_new_opening_margin() {
    let (config, events, start) = reducing_fixture(dec!(4.5));
    let report = run_backtest(
        config,
        &child_risk_registry(Default::default()),
        InMemoryMarketDataSource::new(events),
        ReduceSequence {
            start,
            opening_side: apexes_core::Side::Sell,
            opening_quantity: dec!(1),
            closing_quantity: dec!(1),
            kind: apexes_core::OrderType::Market,
            algo: false,
            competing_close: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 2);
    assert_eq!(report.trades[1].side, apexes_core::Side::Buy);
    assert_eq!(report.trades[1].quantity, dec!(1));
    assert_eq!(report.event_latency.unwrap().capital_refused, 0);
}

#[tokio::test]
async fn reversal_reserves_only_excess_opening_margin_not_closing_proceeds() {
    for (balance, expected) in [(dec!(9.5), 2), (dec!(9.4), 1)] {
        let (config, events, start) = reducing_fixture(balance);
        let report = run_backtest(
            config,
            &child_risk_registry(Default::default()),
            InMemoryMarketDataSource::new(events),
            ReduceSequence {
                start,
                opening_side: apexes_core::Side::Buy,
                opening_quantity: dec!(1),
                closing_quantity: dec!(2),
                kind: apexes_core::OrderType::Market,
                algo: false,
                competing_close: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(report.trades.len(), expected);
        if expected == 2 {
            assert_eq!(report.trades[1].quantity, dec!(2));
        } else {
            assert_eq!(report.event_latency.unwrap().capital_refused, 1);
        }
    }
}

#[tokio::test]
async fn resting_reversal_keeps_opening_margin_after_closing_partial_fill() {
    let (config, mut events, start) = reducing_fixture(dec!(10));
    events.truncate(4);
    for (offset, quantity) in [(1150, dec!(0.5)), (1200, dec!(0.5)), (1300, dec!(1))] {
        events.push(MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price: dec!(10),
            quantity,
            side: Some(apexes_core::Side::Buy),
            trade_id: None,
            timestamp: start + chrono::Duration::milliseconds(offset),
        }));
    }
    let report = run_backtest(
        config,
        &child_risk_registry(Default::default()),
        InMemoryMarketDataSource::new(events),
        ReduceSequence {
            start,
            opening_side: apexes_core::Side::Buy,
            opening_quantity: dec!(1),
            closing_quantity: dec!(2),
            kind: apexes_core::OrderType::Limit,
            algo: false,
            competing_close: true,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 4, "{:?}", report.trades);
    assert_eq!(
        report.trades[1..]
            .iter()
            .map(|f| f.quantity)
            .sum::<rust_decimal::Decimal>(),
        dec!(2)
    );
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (
            latency.capital_refused,
            latency.resting_capital_reserved,
            latency.resting_capital_released
        ),
        (1, 1, 1)
    );
}

#[tokio::test]
async fn ems_closing_parent_owns_quantity_through_child_transit() {
    let (config, events, start) = reducing_fixture(dec!(5));
    let report = run_backtest(
        config,
        &child_risk_registry(Default::default()),
        InMemoryMarketDataSource::new(events),
        ReduceSequence {
            start,
            opening_side: apexes_core::Side::Buy,
            opening_quantity: dec!(1),
            closing_quantity: dec!(1),
            kind: apexes_core::OrderType::Market,
            algo: true,
            competing_close: true,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.trades.len(), 3, "{:?}", report.trades);
    assert!(
        report.trades[1..]
            .iter()
            .all(|f| f.side == apexes_core::Side::Sell && f.quantity == dec!(0.5))
    );
    let latency = report.event_latency.unwrap();
    assert_eq!(
        (
            latency.capital_refused,
            latency.parent_capital_reserved,
            latency.parent_capital_released
        ),
        (1, 1, 1)
    );
    assert_eq!(latency.child_orders.unwrap().arrived, 2);
}
