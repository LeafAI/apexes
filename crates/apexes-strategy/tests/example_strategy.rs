//! The shipped `deploy/strategy.example.drift` must load and behave as its
//! header says: warm up, go long above its average, flatten below it, and
//! honour live `quantity` / `enabled` changes.

use apexes_core::{MarketEvent, Side, TradeEvent, Venue};
use apexes_strategy::drift_dsl::DriftScriptStrategy;
use apexes_strategy::{OrderIntent, Position, Strategy, StrategyContext};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::path::PathBuf;

fn example_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/strategy.example.drift")
}

fn trade(price: Decimal) -> MarketEvent {
    MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        price,
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: Utc::now(),
    })
}

/// See `drift_script_strategy.rs`: the strategy bridges to an async
/// interpreter with `block_on`, so it must run on a thread that has merely
/// entered a runtime, not inside an async task.
fn with_runtime<T>(f: impl FnOnce() -> T) -> T {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    f()
}

fn step(
    strategy: &mut DriftScriptStrategy,
    event: &MarketEvent,
    held: Decimal,
) -> Vec<OrderIntent> {
    let mut positions = HashMap::new();
    if !held.is_zero() {
        positions.insert(
            "BTCUSDT".to_string(),
            Position {
                symbol: "BTCUSDT".to_string(),
                quantity: held,
                average_entry_price: dec!(100),
                leverage: None,
                margin_mode: None,
                allocated_margin: Decimal::ZERO,
            },
        );
    }
    let ctx = StrategyContext {
        positions: &positions,
        balances: &HashMap::new(),
        open_orders: &HashMap::new(),
        open_parent_orders: &HashMap::new(),
        margin_status: &HashMap::new(),
        now: Utc::now(),
    };
    strategy.on_market_event(event, &ctx)
}

#[test]
fn warms_up_then_buys_in_an_uptrend_and_sells_in_a_downtrend() {
    with_runtime(|| {
        let mut strategy = DriftScriptStrategy::from_file(&example_path())
            .expect("the shipped example strategy must load");

        // SMA(20) needs 20 trades: nothing happens while it warms up.
        for i in 0..19 {
            assert!(step(&mut strategy, &trade(dec!(100) + Decimal::from(i)), dec!(0)).is_empty());
        }

        // The 20th trade is well above the average of a rising series.
        let buys = step(&mut strategy, &trade(dec!(200)), dec!(0));
        assert_eq!(buys.len(), 1, "should go long above the average");
        assert_eq!(buys[0].side, Side::Buy);
        assert_eq!(buys[0].quantity, dec!(0.001), "default quantity");
        assert_eq!(buys[0].symbol, "BTCUSDT");

        // Already holding the target quantity: no stacking.
        assert!(step(&mut strategy, &trade(dec!(201)), dec!(0.001)).is_empty());

        // A collapse below the average while holding: sell what is held.
        let sells = step(&mut strategy, &trade(dec!(50)), dec!(0.001));
        assert_eq!(sells.len(), 1);
        assert_eq!(sells[0].side, Side::Sell);
        assert_eq!(sells[0].quantity, dec!(0.001));
    });
}

#[test]
fn quantity_and_enabled_are_hot_tunable() {
    with_runtime(|| {
        let mut strategy = DriftScriptStrategy::from_file(&example_path()).unwrap();
        for i in 0..19 {
            step(&mut strategy, &trade(dec!(100) + Decimal::from(i)), dec!(0));
        }

        strategy.set_param("quantity", dec!(0.005)).unwrap();
        let buys = step(&mut strategy, &trade(dec!(200)), dec!(0));
        assert_eq!(
            buys[0].quantity,
            dec!(0.005),
            "new quantity applies on the next event"
        );

        // Disabled: with a position it must unwind, and it must not buy.
        strategy.set_param("enabled", dec!(0)).unwrap();
        assert!(step(&mut strategy, &trade(dec!(300)), dec!(0)).is_empty());
        let sells = step(&mut strategy, &trade(dec!(300)), dec!(0.005));
        assert_eq!(sells.len(), 1);
        assert_eq!(sells[0].side, Side::Sell);
    });
}

#[test]
fn non_trade_events_are_ignored() {
    with_runtime(|| {
        let mut strategy = DriftScriptStrategy::from_file(&example_path()).unwrap();
        let tick = MarketEvent::Tick(apexes_core::Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(1),
            ask_price: dec!(2),
            bid_size: dec!(1),
            ask_size: dec!(1),
            timestamp: Utc::now(),
        });
        assert!(step(&mut strategy, &tick, dec!(0)).is_empty());
    });
}

#[test]
fn both_params_are_listed_for_the_console() {
    with_runtime(|| {
        let mut strategy = DriftScriptStrategy::from_file(&example_path()).unwrap();
        step(&mut strategy, &trade(dec!(100)), dec!(0));
        let params = strategy.current_params();
        assert!(params.contains_key("quantity"), "{params:?}");
        assert!(params.contains_key("enabled"), "{params:?}");
    });
}
