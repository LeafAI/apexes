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
