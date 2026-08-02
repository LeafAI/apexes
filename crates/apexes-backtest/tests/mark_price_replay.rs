//! Proves `MarketEvent::MarkPrice` events flow through `run_backtest`
//! without error -- this sub-project's own scope stops at "mark price
//! is real, replayable data"; it does NOT yet make any strategy/ledger
//! logic react to it (that is a later, separate margin/leverage
//! design's job).

use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, InMemoryMarketDataSource, OrderIntent, ReplaySpeed,
    Strategy, StrategyContext, run_backtest,
};
use apexes_core::{AssetClass, Instrument, MarkPriceUpdate, MarketEvent, MarketType, Venue};
use chrono::{NaiveDate, Utc};
use rust_decimal_macros::dec;
use std::collections::HashMap;

/// A strategy that never submits any order -- this test only cares
/// whether replay itself succeeds without error, not about any
/// order/fill behavior.
struct NoOpStrategy;

impl Strategy for NoOpStrategy {
    fn on_market_event(
        &mut self,
        _event: &MarketEvent,
        _ctx: &StrategyContext,
    ) -> Vec<OrderIntent> {
        vec![]
    }
}

fn btc_usdt_perpetual_instrument() -> Instrument {
    Instrument::new(
        "BTCUSDT",
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Perpetual,
        Some("BTC".to_string()),
        Some("USDT".to_string()),
        dec!(0.01),
        dec!(0.001),
        dec!(1),
    )
    .unwrap()
}

#[tokio::test]
async fn backtest_replays_a_mark_price_event_without_error() {
    let events = vec![MarketEvent::MarkPrice(MarkPriceUpdate {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        mark_price: dec!(65012.34),
        timestamp: Utc::now(),
    })];
    let market_data = InMemoryMarketDataSource::new(events);

    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(100000));

    let config = BacktestConfig {
        instruments: vec![btc_usdt_perpetual_instrument()],
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
    };

    let registry = apexes_core::registry::Registry::new();
    let report = run_backtest(config, &registry, market_data, NoOpStrategy)
        .await
        .expect("run_backtest must not error on a MarkPrice event");

    assert!(report.trades.is_empty(), "NoOpStrategy submits no orders");
}
