//! Regression guard for the final-review fix: `BacktestConfig` must
//! expose `margin_tiers` and `run_backtest` must thread it into the
//! shared event loop, so a leveraged position opened through the REAL
//! public entry point (not `run_event_loop` directly, which the
//! event-loop unit tests already cover) is auto-liquidated when the
//! mark price breaches the maintenance-margin ratio.

use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, InMemoryMarketDataSource, OrderIntent,
    OrderRouting, ReplaySpeed, Strategy, StrategyContext, run_backtest,
};
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarginMode, MarkPriceUpdate, MarketEvent, MarketType,
    OrderType, PriceLevel, Side, TradeEvent, Venue,
};
use apexes_strategy::{MarginTier, MarginTierTable};
use chrono::{NaiveDate, TimeZone, Utc};
use rust_decimal_macros::dec;
use std::collections::HashMap;

struct LeveragedBuyOnce {
    bought: bool,
}

impl Strategy for LeveragedBuyOnce {
    fn on_market_event(&mut self, event: &MarketEvent, _ctx: &StrategyContext) -> Vec<OrderIntent> {
        if !self.bought && matches!(event, MarketEvent::Trade(_)) {
            self.bought = true;
            return vec![OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Perpetual,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
                execution_algo: None,
                routing: OrderRouting::Fixed,
                leverage: Some(dec!(10)),
                margin_mode: Some(MarginMode::Isolated),
            }];
        }
        vec![]
    }
}

fn btcusdt_perpetual_instrument() -> Instrument {
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
    .expect("instrument must be valid")
}

fn single_tier_table() -> MarginTierTable {
    MarginTierTable {
        tiers: vec![MarginTier {
            notional_floor: dec!(0),
            max_leverage: dec!(20),
            maintenance_margin_rate: dec!(0.005),
            maintenance_amount: dec!(0),
        }],
    }
}

/// End-to-end through `run_backtest`: a 10x isolated long opened at 100
/// collapses to a mark price of 89, breaching the maintenance-margin
/// ratio -- the loop must force-close the position through the
/// `MatchingEngine` (selling into the resting 99 bid) without any
/// strategy action. With the pre-fix hardcoded empty `margin_tiers`,
/// this test would see only the opening fill and no liquidation.
#[tokio::test]
async fn run_backtest_auto_liquidates_leveraged_position_when_margin_tiers_supplied() {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let events = vec![
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: dec!(99),
                quantity: dec!(10),
            }],
            asks: vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(10),
            }],
            sequence: None,
            is_snapshot: true,
            timestamp: t0,
        }),
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(100),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: t0 + chrono::Duration::seconds(1),
        }),
        MarketEvent::MarkPrice(MarkPriceUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            mark_price: dec!(100),
            timestamp: t0 + chrono::Duration::seconds(2),
        }),
        MarketEvent::MarkPrice(MarkPriceUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            mark_price: dec!(89),
            timestamp: t0 + chrono::Duration::seconds(3),
        }),
        // One more event so the loop processes a tick AFTER the
        // liquidation (the position must stay closed -- no second
        // liquidation order).
        MarketEvent::MarkPrice(MarkPriceUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            mark_price: dec!(88),
            timestamp: t0 + chrono::Duration::seconds(4),
        }),
    ];

    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(1000));

    let mut margin_tiers = HashMap::new();
    margin_tiers.insert("BTCUSDT".to_string(), single_tier_table());

    let config = BacktestConfig {
        instruments: vec![btcusdt_perpetual_instrument()],
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
        queue_position_factor: dec!(0.5),
        risk_free_return: dec!(0),
        margin_tiers,
        market_condition_config: None,
    };

    let registry = apexes_core::registry::Registry::new();
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        LeveragedBuyOnce { bought: false },
    )
    .await
    .expect("run_backtest must not error");

    // Exactly two fills: the strategy's leveraged buy at 100 (walked
    // the 100 ask), then the liquidation sell at 99 (walked the 99
    // bid).
    assert_eq!(report.trades.len(), 2, "expected open + liquidation fills");
    let open_fill = &report.trades[0];
    assert_eq!(open_fill.side, Side::Buy);
    assert_eq!(open_fill.price, dec!(100));
    assert_eq!(open_fill.leverage, Some(dec!(10)));
    let liquidation_fill = &report.trades[1];
    assert_eq!(liquidation_fill.side, Side::Sell);
    assert_eq!(liquidation_fill.price, dec!(99));
    assert_eq!(liquidation_fill.quantity, dec!(1));
    assert_eq!(
        liquidation_fill.leverage,
        Some(dec!(10)),
        "liquidation fill must carry the position's leverage to take the close_leveraged path"
    );
    assert_eq!(liquidation_fill.margin_mode, Some(MarginMode::Isolated));

    // Accounting: 1000 USDT - 10 reserved margin - 1 realized loss
    // + 10 released margin = 999 USDT.
    let final_equity = report
        .equity_curve
        .last()
        .map(|(_, equity)| *equity)
        .expect("equity curve must be non-empty");
    assert_eq!(final_equity, dec!(999));
}

/// Companion no-op guard: the SAME setup with an empty `margin_tiers`
/// map (today's default for every existing caller) must behave exactly
/// as before the fix -- no liquidation, the position stays open.
#[tokio::test]
async fn run_backtest_with_empty_margin_tiers_never_liquidates() {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let events = vec![
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: dec!(99),
                quantity: dec!(10),
            }],
            asks: vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(10),
            }],
            sequence: None,
            is_snapshot: true,
            timestamp: t0,
        }),
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(100),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: t0 + chrono::Duration::seconds(1),
        }),
        MarketEvent::MarkPrice(MarkPriceUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            mark_price: dec!(89),
            timestamp: t0 + chrono::Duration::seconds(2),
        }),
    ];

    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(1000));

    let config = BacktestConfig {
        instruments: vec![btcusdt_perpetual_instrument()],
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
        queue_position_factor: dec!(0.5),
        risk_free_return: dec!(0),
        margin_tiers: HashMap::new(),
        market_condition_config: None,
    };

    let registry = apexes_core::registry::Registry::new();
    let report = run_backtest(
        config,
        &registry,
        InMemoryMarketDataSource::new(events),
        LeveragedBuyOnce { bought: false },
    )
    .await
    .expect("run_backtest must not error");

    assert_eq!(
        report.trades.len(),
        1,
        "empty margin_tiers must be a silent no-op: only the opening fill"
    );
}
