//! End-to-end test of the full backtest pipeline, using
//! `InMemoryMarketDataSource` so it requires no live database (runs under
//! default `cargo test`, no `--ignored` needed).

use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, InMemoryMarketDataSource, OrderIntent, ReplaySpeed,
    Strategy, StrategyContext, run_backtest,
};
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, OrderType, PriceLevel, Side,
    TradeEvent, Venue,
};
use chrono::{NaiveDate, TimeZone, Utc};
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
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(0.1),
                price: None,
                stop_price: None,
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
    };
    let strategy = BuyOnceStrategy { has_bought: false };

    let report = run_backtest(config, market_data, strategy).await.unwrap();

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
    };
    let strategy = BuyOnceStrategy { has_bought: false };

    let report = run_backtest(config, market_data, strategy).await.unwrap();

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
                side: Side::Buy,
                order_type: OrderType::Stop,
                quantity: dec!(0.1),
                price: None,
                stop_price: Some(dec!(66000)),
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
    };
    let strategy = BuyStopOnceStrategy { has_placed: false };

    let report = run_backtest(config, market_data, strategy).await.unwrap();

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
