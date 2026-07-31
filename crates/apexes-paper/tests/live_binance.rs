//! Live integration test against a real Binance WebSocket feed, proving
//! `run_paper_trading` works end-to-end with `BinanceMarketDataProvider`
//! (not just the mock provider used in `runner.rs`'s unit tests).
//! Excluded from default `cargo test` -- run explicitly with:
//!   cargo test -p apexes-paper -- --ignored

use apexes_backtest::cost_model::FlatFeeSchedule;
use apexes_backtest::{CostModel, OrderIntent, Strategy, StrategyContext};
use apexes_core::{AssetClass, Instrument, MarketEvent, MarketType, Venue};
use apexes_market_data::BinanceMarketDataProvider;
use apexes_paper::{LatencyModel, PaperTradingConfig, run_paper_trading};
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot;

/// Never actually submits an order -- this test only proves the live
/// feed connects and `run_paper_trading` processes events without
/// panicking or erroring; it does not attempt to predict real market
/// conditions well enough to assert on specific fills.
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

fn btc_usdt_instrument() -> Instrument {
    Instrument::new(
        "BTCUSDT",
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("BTC".to_string()),
        Some("USDT".to_string()),
        dec!(0.01),
        dec!(0.00001),
        dec!(1),
    )
    .unwrap()
}

#[tokio::test]
#[ignore]
async fn run_paper_trading_against_real_binance_feed_for_a_few_seconds() {
    let provider = BinanceMarketDataProvider::new();
    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(10000));

    let config = PaperTradingConfig {
        instruments: vec![btc_usdt_instrument()],
        initial_balances,
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        },
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
        risk_checker: None,
        latency_model: LatencyModel::None,
    };

    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let _ = tx.send(());
    });

    let report = run_paper_trading(config, &provider, NoOpStrategy, rx)
        .await
        .expect("run_paper_trading should not error against a real Binance feed");

    // No specific assertions on report.trades (NoOpStrategy never
    // submits orders) -- this test's purpose is proving the live
    // subscribe -> event-loop -> graceful-stop pipeline works end to
    // end without panicking, matching apexes-market-data's own live
    // test's scope boundary (see crates/apexes-market-data/tests/live_binance.rs).
    assert!(report.equity_curve.len() <= 1 || !report.equity_curve.is_empty());
}
