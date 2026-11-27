//! End-to-end test of the full backtest pipeline, using
//! `InMemoryMarketDataSource` so it requires no live database (runs under
//! default `cargo test`, no `--ignored` needed).

use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, InMemoryMarketDataSource, OrderIntent,
    OrderRouting, ReplaySpeed, Strategy, StrategyContext, run_backtest,
};
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, OrderType, PriceLevel, Side,
    TradeEvent, Venue,
};
use chrono::{NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

/// A trivial deterministic test strategy: buys 1 unit the first time it
/// sees a Trade event, then does nothing else.
struct BuyOnceStrategy {
    has_bought: bool,
}

impl Strategy for BuyOnceStrategy {
    fn on_market_event(&mut self, event: &MarketEvent, _ctx: &StrategyContext) -> Vec<OrderIntent> {
        if self.has_bought {
            return vec![];
        }
        if let MarketEvent::Trade(trade) = event {
            self.has_bought = true;
            return vec![OrderIntent {
                symbol: trade.symbol.clone(),
                venue: trade.venue,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(0.1),
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

#[tokio::test]
async fn full_backtest_produces_a_report_with_the_expected_trade() {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let t1 = t0 + chrono::Duration::seconds(1);

    let events = vec![
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![PriceLevel {
                price: dec!(65000),
                quantity: dec!(10),
            }],
            sequence: Some(1),
            source_range: None,
            is_snapshot: true,
            timestamp: t0,
        }),
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: Some(Side::Buy),
            trade_id: Some("T1".to_string()),
            timestamp: t1,
        }),
    ];
    let market_data = InMemoryMarketDataSource::new(events);

    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(10000));

    let config = BacktestConfig {
        instruments: vec![btc_usdt_instrument()],
        date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
            ..NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
        initial_balances,
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(10),
            }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
        margin_tiers: HashMap::new(),
        market_condition_config: None,
        control: None,
    };
    let strategy = BuyOnceStrategy { has_bought: false };

    let registry = apexes_core::registry::Registry::new();
    let report = run_backtest(config, &registry, market_data, strategy)
        .await
        .unwrap();

    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].symbol, "BTCUSDT");
    assert_eq!(report.trades[0].quantity, dec!(0.1));
    assert_eq!(report.trades[0].price, dec!(65000));
    // fee = 65000 * 0.1 * 0.10% (10 bps) = 6.5
    assert_eq!(report.trades[0].fee, dec!(6.5));

    assert!(report.per_symbol.contains_key("BTCUSDT"));
    assert_eq!(report.per_symbol["BTCUSDT"].trade_count, 1);
    assert!(!report.equity_curve.is_empty());
}

#[tokio::test]
async fn backtest_with_no_events_produces_an_empty_report_without_erroring() {
    let market_data = InMemoryMarketDataSource::new(vec![]);
    let config = BacktestConfig {
        instruments: vec![btc_usdt_instrument()],
        date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
            ..NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
        initial_balances: HashMap::new(),
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
    };
    let strategy = BuyOnceStrategy { has_bought: false };

    let registry = apexes_core::registry::Registry::new();
    let report = run_backtest(config, &registry, market_data, strategy)
        .await
        .unwrap();

    assert!(report.trades.is_empty());
    assert!(report.equity_curve.is_empty());
}

/// Places a Buy Stop order on the first event it sees, then does nothing
/// else.
struct BuyStopOnceStrategy {
    has_placed: bool,
}

impl Strategy for BuyStopOnceStrategy {
    fn on_market_event(&mut self, event: &MarketEvent, _ctx: &StrategyContext) -> Vec<OrderIntent> {
        if self.has_placed {
            return vec![];
        }
        if let MarketEvent::Trade(trade) = event {
            self.has_placed = true;
            return vec![OrderIntent {
                symbol: trade.symbol.clone(),
                venue: trade.venue,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Stop,
                quantity: dec!(0.1),
                price: None,
                stop_price: Some(dec!(66000)),
                execution_algo: None,
                routing: OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            }];
        }
        vec![]
    }
}

/// Regression guard for the final-review CRITICAL finding (C1): triggered
/// Stop orders' fills were previously computed inside
/// `MatchingEngine::check_pending_triggers` and then DISCARDED
/// (`.is_ok()` threw away the returned `(Order, Vec<Fill>)`), so a
/// triggered Stop order had ZERO effect on the ledger, equity curve, or
/// `BacktestReport.trades`. This test places a Buy Stop via a real
/// `Strategy`, triggers it with a real trade, and asserts BOTH
/// `report.trades` contains the fill AND the ledger/equity curve reflects
/// it (non-zero effect) -- a "book looks consumed"-only assertion is NOT
/// sufficient, per the reviewer's finding that such checks are vacuous.
#[tokio::test]
async fn triggered_buy_stop_fill_reaches_report_trades_and_ledger() {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();

    let events = vec![
        // Seed the book: 10 units of ask liquidity at 66100.
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![PriceLevel {
                price: dec!(66100),
                quantity: dec!(10),
            }],
            sequence: Some(1),
            source_range: None,
            is_snapshot: true,
            timestamp: t0,
        }),
        // Trade 1 (price 65000, below the 66000 stop): the strategy
        // places its Buy Stop here; the stop does NOT trigger.
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: Some(Side::Buy),
            trade_id: Some("T1".to_string()),
            timestamp: t0 + chrono::Duration::seconds(1),
        }),
        // Trade 2 (price 66050, at/above the 66000 stop): triggers the
        // Buy Stop, which converts to a Market order and walks the 66100
        // ask level.
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(66050),
            quantity: dec!(1),
            side: Some(Side::Buy),
            trade_id: Some("T2".to_string()),
            timestamp: t0 + chrono::Duration::seconds(2),
        }),
    ];
    let market_data = InMemoryMarketDataSource::new(events);

    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(10000));

    let config = BacktestConfig {
        instruments: vec![btc_usdt_instrument()],
        date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
            ..NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
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
    };
    let strategy = BuyStopOnceStrategy { has_placed: false };

    let registry = apexes_core::registry::Registry::new();
    let report = run_backtest(config, &registry, market_data, strategy)
        .await
        .unwrap();

    // (1) report.trades MUST contain the triggered Stop's fill.
    assert_eq!(
        report.trades.len(),
        1,
        "triggered Buy Stop fill must appear in report.trades"
    );
    let fill = &report.trades[0];
    assert_eq!(fill.symbol, "BTCUSDT");
    assert_eq!(fill.side, Side::Buy);
    assert_eq!(fill.quantity, dec!(0.1));
    assert_eq!(fill.price, dec!(66100)); // walked the ask level
    assert_eq!(report.per_symbol["BTCUSDT"].trade_count, 1);

    // (2) The ledger/equity MUST reflect the fill: the run spent
    // 0.1 * 66100 = 6610 USDT buying 0.1 BTC, marked at the last trade
    // price (66050) for a final equity of 3390 + 0.1 * 66050 = 9995.
    // The discarded-fill bug would instead report a flat 10000 (zero
    // effect on the ledger), which is exactly what this assertion guards.
    let final_equity = report
        .equity_curve
        .last()
        .map(|(_, equity)| *equity)
        .unwrap();
    assert_eq!(final_equity, dec!(9995));
    assert_ne!(
        final_equity,
        dec!(10000),
        "equity must reflect the triggered Stop fill, not the untouched initial balance"
    );
    // The equity curve MUST move at the trigger event (not stay flat).
    assert!(
        report
            .equity_curve
            .iter()
            .any(|(_, equity)| *equity != dec!(10000)),
        "equity curve must record a non-initial equity point after the Stop triggers"
    );
}

/// Submits ONE TWAP parent order (single slice) the first time it sees
/// any event, then does nothing else.
struct TwapOnceStrategy {
    has_submitted: bool,
}

impl Strategy for TwapOnceStrategy {
    fn on_market_event(&mut self, event: &MarketEvent, _ctx: &StrategyContext) -> Vec<OrderIntent> {
        if self.has_submitted {
            return vec![];
        }
        if let MarketEvent::Depth(update) = event {
            self.has_submitted = true;
            return vec![OrderIntent {
                symbol: update.symbol.clone(),
                venue: update.venue,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
                execution_algo: Some(apexes_strategy::ExecutionAlgoSpec::Twap {
                    duration: std::time::Duration::from_secs(10),
                    slices: 1,
                }),
                routing: OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            }];
        }
        vec![]
    }
}

/// Regression guard for the Task-17 follow-up finding (fixed in Task
/// 17b): `MatchingEngineChildOrderSink::submit_child_order` used to
/// DISCARD the `Vec<Fill>` the matching engine returned for an EMS
/// child order, so a strategy using `execution_algo` saw correct
/// order-book price impact but the ledger/equity-curve/`report.trades`
/// silently missed every child-order fill. This test runs a full
/// backtest whose only order is a single-slice TWAP (its one child
/// order fires on the first `ems.on_market_event` tick after
/// submission, per `InMemoryEms`'s `next_slice_at: Utc::now()`
/// semantics) and asserts BOTH that `report.trades` contains the child
/// order's fill AND that the equity curve reflects it -- without the
/// fix, both assertions fail (zero trades, flat equity).
///
/// NOTE on timestamps: `InMemoryEms` schedules the first TWAP slice at
/// REAL `Utc::now()` (submission wall-clock time), and
/// `on_market_event` compares that against each event's timestamp --
/// so these events must be timestamped at/after the real current time,
/// not at a fixed historical date.
#[tokio::test]
async fn full_backtest_with_execution_algo_applies_child_order_fills_to_the_ledger_and_report() {
    let t0 = Utc::now();
    let t1 = t0 + chrono::Duration::seconds(1);

    let events = vec![
        // Seed the book: 10 units of ask liquidity at 65000. The
        // strategy submits its single-slice TWAP parent order while
        // processing this event.
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![PriceLevel {
                price: dec!(65000),
                quantity: dec!(10),
            }],
            sequence: Some(1),
            source_range: None,
            is_snapshot: true,
            timestamp: t0,
        }),
        // Second event: its `ems.on_market_event` call finds the TWAP
        // slice due and submits the child Market order, which fills
        // immediately against the seeded book.
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![PriceLevel {
                price: dec!(65000),
                quantity: dec!(10),
            }],
            sequence: Some(2),
            source_range: None,
            is_snapshot: false,
            timestamp: t1,
        }),
    ];
    let market_data = InMemoryMarketDataSource::new(events);

    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(100000));

    let config = BacktestConfig {
        instruments: vec![btc_usdt_instrument()],
        date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
            ..NaiveDate::from_ymd_opt(2027, 1, 1).unwrap(),
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
    };
    let strategy = TwapOnceStrategy {
        has_submitted: false,
    };

    let registry = apexes_core::registry::Registry::new();
    let report = run_backtest(config, &registry, market_data, strategy)
        .await
        .unwrap();

    // (1) report.trades MUST contain the EMS child order's fill (the
    // single TWAP slice's full 1-unit quantity, filled at the 65000 ask).
    assert_eq!(
        report.trades.len(),
        1,
        "EMS child-order fill must appear in report.trades"
    );
    let fill = &report.trades[0];
    assert_eq!(fill.symbol, "BTCUSDT");
    assert_eq!(fill.side, Side::Buy);
    assert_eq!(fill.quantity, dec!(1));
    assert_eq!(fill.price, dec!(65000));
    assert_eq!(report.per_symbol["BTCUSDT"].trade_count, 1);

    // (2) The ledger/equity MUST reflect the fill: the run spent
    // 1 * 65000 = 65000 USDT buying 1 BTC. The discarded-fill bug
    // would instead report a flat 100000 (zero ledger effect).
    assert!(
        report
            .equity_curve
            .iter()
            .any(|(_, equity)| *equity != dec!(100000)),
        "equity curve must reflect the EMS child-order fill, not the untouched initial balance"
    );
}

/// Submits ONE Iceberg parent order (quantity 3, visible_qty 1 -- three
/// slices) the first time it sees any event, then does nothing else.
struct IcebergOnceStrategy {
    has_submitted: bool,
}

impl Strategy for IcebergOnceStrategy {
    fn on_market_event(&mut self, event: &MarketEvent, _ctx: &StrategyContext) -> Vec<OrderIntent> {
        if self.has_submitted {
            return vec![];
        }
        if let MarketEvent::Depth(update) = event {
            self.has_submitted = true;
            return vec![OrderIntent {
                symbol: update.symbol.clone(),
                venue: update.venue,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(3),
                price: None,
                stop_price: None,
                execution_algo: Some(apexes_strategy::ExecutionAlgoSpec::Iceberg {
                    visible_qty: dec!(1),
                }),
                routing: OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            }];
        }
        vec![]
    }
}

/// Regression guard for a real Critical bug caught in sub-project #9's
/// Task 18 whole-branch review: `MatchingEngineChildOrderSink::order_status`
/// used to always return `Ok(None)`, which `InMemoryEms`'s Iceberg
/// scheduling (`refill_iceberg_allocation`) treats identically to "the
/// resting child has not yet reached a terminal state" -- so every real
/// Iceberg parent order submitted through the real (non-mocked) adapter
/// would submit exactly one visible-quantity slice and then stall in
/// `ParentOrderStatus::Scheduling` forever, silently under-executing
/// with no error. This test drives a 3-slice Iceberg order (quantity 3,
/// visible_qty 1) through a real `run_backtest` call and asserts it
/// fully completes: all 3 slices fill and the parent order's quantity
/// is fully reflected in `report.trades`/the equity curve. Without the
/// fix, `report.trades.len()` would be 1 (only the first slice), not 3.
#[tokio::test]
async fn full_backtest_with_iceberg_execution_algo_completes_all_slices() {
    let t0 = Utc::now();

    // One event per slice is enough: `on_market_event` calls
    // `process_iceberg_parents` on every tick, and since this adapter's
    // `order_status` now reports each submitted slice as immediately
    // terminal (Filled), one event advances exactly one slice.
    let events: Vec<MarketEvent> = (0i64..5)
        .map(|i| {
            MarketEvent::Depth(DepthUpdate {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                bids: vec![],
                asks: vec![PriceLevel {
                    price: dec!(65000),
                    quantity: dec!(10),
                }],
                sequence: Some(i as u64),
                source_range: None,
                is_snapshot: i == 0,
                timestamp: t0 + chrono::Duration::seconds(i),
            })
        })
        .collect();
    let market_data = InMemoryMarketDataSource::new(events);

    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(1000000));

    let config = BacktestConfig {
        instruments: vec![btc_usdt_instrument()],
        date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
            ..NaiveDate::from_ymd_opt(2027, 1, 1).unwrap(),
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
    };
    let strategy = IcebergOnceStrategy {
        has_submitted: false,
    };

    let registry = apexes_core::registry::Registry::new();
    let report = run_backtest(config, &registry, market_data, strategy)
        .await
        .unwrap();

    let total_filled: Decimal = report.trades.iter().map(|fill| fill.quantity).sum();
    assert_eq!(
        total_filled,
        dec!(3),
        "all 3 Iceberg slices must fill, not just the first one before it stalls"
    );
    assert_eq!(
        report.trades.len(),
        3,
        "Iceberg must submit exactly 3 slices (quantity 3 / visible_qty 1), not stall after 1"
    );
}
