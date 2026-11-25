//! F00.01: deterministic, offline replay of the original deployed Drift
//! strategy through the real matching engine and shared trading loop.
//! Synthetic data tests execution correctness, not trading profitability.

use apexes_backtest::{
    BacktestConfig, BacktestReport, CostModel, FlatFeeSchedule, InMemoryMarketDataSource,
    ReplaySpeed, run_backtest,
};
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, PriceLevel, Side, TradeEvent,
    Venue,
};
use apexes_strategy::drift_dsl::DriftScriptStrategy;
use chrono::{Duration, NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{collections::HashMap, path::Path};

const SYMBOL: &str = "ETHUSDT";

fn fixture(prices: &[Decimal]) -> Vec<MarketEvent> {
    let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let mut events = Vec::with_capacity(prices.len() * 2);
    for (i, &price) in prices.iter().enumerate() {
        let timestamp = start + Duration::seconds(i as i64 * 2);
        events.push(MarketEvent::Depth(DepthUpdate {
            symbol: SYMBOL.into(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: price - dec!(0.01),
                quantity: dec!(10),
            }],
            asks: vec![PriceLevel {
                price: price + dec!(0.01),
                quantity: dec!(10),
            }],
            sequence: Some(i as u64 + 1),
            is_snapshot: true,
            timestamp,
        }));
        events.push(MarketEvent::Trade(TradeEvent {
            symbol: SYMBOL.into(),
            venue: Venue::Binance,
            price,
            quantity: dec!(1),
            side: Some(Side::Buy),
            trade_id: Some(format!("fixture-{i}")),
            timestamp: timestamp + Duration::seconds(1),
        }));
    }
    events
}

fn replay(prices: &[Decimal], taker_bps: Decimal) -> BacktestReport {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/strategy.trend_guard.drift");
    let strategy = DriftScriptStrategy::from_file(&script).unwrap();
    let config = BacktestConfig {
        instruments: vec![
            Instrument::new(
                SYMBOL,
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
        ],
        date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
            ..NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
        initial_balances: HashMap::from([("USDT".into(), dec!(1000))]),
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: Decimal::ZERO,
                taker_bps,
            }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: Decimal::ZERO,
        risk_free_return: Decimal::ZERO,
        margin_tiers: HashMap::new(),
        market_condition_config: None,
        control: None,
    };
    // Drift internally uses Handle::block_on: enter the runtime but drive
    // the loop outside Tokio's task context, just as apexes-engine does.
    futures::executor::block_on(run_backtest(
        config,
        &apexes_core::registry::Registry::new(),
        InMemoryMarketDataSource::new(fixture(prices)),
        strategy,
    ))
    .unwrap()
}

fn trending_prices() -> Vec<Decimal> {
    let mut prices = vec![dec!(100); 120];
    prices.extend((1..=60).map(|i| dec!(100) + Decimal::from(i) * dec!(0.05)));
    prices.extend([dec!(98); 40]);
    prices
}

fn economic_report(report: &BacktestReport) -> serde_json::Value {
    // Execution IDs are deliberately random UUIDs. Keep all other report
    // fields, including timestamps, quantity, fees and equity, comparable.
    let mut value = serde_json::to_value(report).unwrap();
    for fill in value["trades"].as_array_mut().unwrap() {
        let fields = fill.as_object_mut().unwrap();
        fields.remove("id");
        fields.remove("order_id");
    }
    value
}

#[test]
fn flat_market_does_not_create_orders() {
    let report = replay(&[dec!(100); 250], dec!(10));
    assert!(report.trades.is_empty());
    assert!(!report.equity_curve.is_empty());
    assert!(
        report
            .equity_curve
            .iter()
            .all(|(_, value)| *value == dec!(1000))
    );
}

#[test]
fn trend_guard_round_trip_is_reproducible_and_cost_sensitive() {
    let prices = trending_prices();
    let paid = replay(&prices, dec!(10));
    let repeated = replay(&prices, dec!(10));
    let free = replay(&prices, Decimal::ZERO);
    assert_eq!(economic_report(&paid), economic_report(&repeated));
    assert_eq!(paid.trades.len(), 2, "{:?}", paid.trades);
    let (buy, sell) = (&paid.trades[0], &paid.trades[1]);
    assert_eq!(buy.side, Side::Buy);
    assert_eq!(sell.side, Side::Sell);
    assert_eq!(buy.quantity, sell.quantity);
    assert_eq!(buy.quantity, dec!(0.4977));
    assert_eq!(buy.price, dec!(100.46));
    assert_eq!(sell.price, dec!(101.99));
    assert!(buy.filled_at < sell.filled_at);
    for fill in &paid.trades {
        assert_eq!(fill.fee, fill.price * fill.quantity * dec!(0.001));
    }
    assert_eq!(free.trades.len(), 2);
    for (paid_fill, free_fill) in paid.trades.iter().zip(&free.trades) {
        assert_eq!(paid_fill.price, free_fill.price);
        assert_eq!(paid_fill.quantity, free_fill.quantity);
        assert_eq!(paid_fill.filled_at, free_fill.filled_at);
        assert_eq!(free_fill.fee, Decimal::ZERO);
    }
    let fees = buy.fee + sell.fee;
    let expected = dec!(1000) + (sell.price - buy.price) * buy.quantity - fees;
    assert_eq!(paid.equity_curve.last().unwrap().1, expected);
    assert_eq!(free.equity_curve.last().unwrap().1 - expected, fees);
}

#[test]
fn trend_guard_can_stop_out_after_a_filled_entry() {
    let mut prices = vec![dec!(100); 120];
    prices.extend((1..=9).map(|i| dec!(100) + Decimal::from(i) * dec!(0.05)));
    prices.extend([dec!(100.45); 25]);
    prices.extend([dec!(98); 40]);

    let report = replay(&prices, dec!(10));
    assert_eq!(report.trades.len(), 2, "{:?}", report.trades);
    let (buy, sell) = (&report.trades[0], &report.trades[1]);
    assert_eq!(buy.side, Side::Buy);
    assert_eq!(sell.side, Side::Sell);
    assert_eq!(buy.quantity, sell.quantity);
    assert_eq!(sell.price, dec!(97.99));
    assert!(sell.price < buy.price * dec!(0.994));
    assert!(report.equity_curve.last().unwrap().1 < dec!(1000));
}
