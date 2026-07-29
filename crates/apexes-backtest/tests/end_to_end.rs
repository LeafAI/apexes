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
