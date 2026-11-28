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
        capital: None,
        costs: None,
        book: None,
        market: None,
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
        // 20 USDT at a price of 200 is 0.1, a whole multiple of the 0.0001 step.
        assert_eq!(buys[0].quantity, dec!(0.1), "default notional / price");
        assert_eq!(buys[0].symbol, "BTCUSDT");

        // Already holding the target quantity: no stacking.
        assert!(step(&mut strategy, &trade(dec!(201)), dec!(0.1)).is_empty());
        // Holding it up to floating-point residue (the position comes back
        // through a float): still nothing to do. This sent a 1.7e-18 order on
        // the real testnet and the exchange rejected it.
        assert!(step(&mut strategy, &trade(dec!(201)), dec!(0.0999999999999999)).is_empty());
        assert!(step(&mut strategy, &trade(dec!(201)), dec!(0.1000000000000001)).is_empty());

        // A collapse below the average while holding: sell what is held.
        let sells = step(&mut strategy, &trade(dec!(50)), dec!(0.1));
        assert_eq!(sells.len(), 1);
        assert_eq!(sells[0].side, Side::Sell);
        assert_eq!(sells[0].quantity, dec!(0.1));
    });
}

#[test]
fn notional_and_enabled_are_hot_tunable() {
    with_runtime(|| {
        let mut strategy = DriftScriptStrategy::from_file(&example_path()).unwrap();
        for i in 0..19 {
            step(&mut strategy, &trade(dec!(100) + Decimal::from(i)), dec!(0));
        }

        strategy.set_param("notional", dec!(40)).unwrap();
        let buys = step(&mut strategy, &trade(dec!(200)), dec!(0));
        assert_eq!(
            buys[0].quantity,
            dec!(0.2),
            "new notional applies on the next event"
        );

        // Disabled: with a position it must unwind, and it must not buy.
        strategy.set_param("enabled", dec!(0)).unwrap();
        assert!(step(&mut strategy, &trade(dec!(300)), dec!(0)).is_empty());
        let sells = step(&mut strategy, &trade(dec!(300)), dec!(0.2));
        assert_eq!(sells.len(), 1);
        assert_eq!(sells[0].side, Side::Sell);
    });
}

/// The same script must produce valid orders for different symbols: whole
/// multiples of the step, worth at least the exchange minimum. This is what
/// went wrong on the real testnet (a fixed 0.001 base quantity is 65 USDT of
/// BTC but only about 3 USDT of ETH, below the 5 USDT minimum).
#[test]
fn orders_are_sized_by_value_so_they_work_across_symbols() {
    with_runtime(|| {
        for (price, label) in [
            (dec!(65000), "BTC"),
            (dec!(3000), "ETH"),
            (dec!(150), "SOL"),
        ] {
            let mut strategy = DriftScriptStrategy::from_file(&example_path()).unwrap();
            // Warm up on a flat-then-rising series ending at `price`.
            for i in 0..19 {
                step(
                    &mut strategy,
                    &trade(price - Decimal::from(20 - i)),
                    dec!(0),
                );
            }
            let buys = step(&mut strategy, &trade(price), dec!(0));
            assert_eq!(buys.len(), 1, "{label}: should buy");
            let quantity = buys[0].quantity;
            assert!(quantity > Decimal::ZERO, "{label}");
            assert_eq!(
                quantity % dec!(0.0001),
                Decimal::ZERO,
                "{label}: {quantity} must be a whole multiple of the lot step"
            );
            let value = quantity * price;
            assert!(
                value >= dec!(5),
                "{label}: order value {value} is below the 5 USDT minimum"
            );
            assert!(
                value <= dec!(20),
                "{label}: order value {value} must not exceed the notional"
            );
        }
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
        assert!(params.contains_key("notional"), "{params:?}");
        assert!(params.contains_key("step"), "{params:?}");
        assert!(params.contains_key("enabled"), "{params:?}");
    });
}

#[test]
fn quantity_strings_are_clean_decimals_for_binance() {
    with_runtime(|| {
        for price in [dec!(65447.01), dec!(3012.37), dec!(150.5), dec!(0.5123)] {
            let mut strategy = DriftScriptStrategy::from_file(&example_path()).unwrap();
            for i in 0..19 {
                step(
                    &mut strategy,
                    &trade(price - Decimal::from(20 - i) / dec!(100)),
                    dec!(0),
                );
            }
            let buys = step(&mut strategy, &trade(price), dec!(0));
            if let Some(buy) = buys.first() {
                let text = buy.quantity.to_string();
                println!("price {price}: quantity {text}");
                let decimals = text.split('.').nth(1).map_or(0, str::len);
                assert!(
                    decimals <= 4,
                    "price {price}: {text} has more than 4 decimals (LOT_SIZE step is 0.0001)"
                );
                assert!(!text.contains('e') && !text.contains('E'), "{text}");
            }
        }
    });
}

/// The live feed's prices are long decimals (a float converted to Decimal, such
/// as 3012.3699999999998908606357872486114501953125 trimmed to 28 digits), not
/// the tidy values the other tests use. The order quantity must still be a
/// short, valid decimal.
#[test]
fn quantities_stay_short_for_the_long_prices_the_live_feed_produces() {
    use std::str::FromStr;
    with_runtime(|| {
        for text in [
            "3012.369999999999890860635787",
            "65446.98999999999796273186801",
            "3012.370000000000118234",
        ] {
            let price = Decimal::from_str(text).unwrap();
            let mut strategy = DriftScriptStrategy::from_file(&example_path()).unwrap();
            for i in 0..19 {
                step(
                    &mut strategy,
                    &trade(price - Decimal::from(20 - i)),
                    dec!(0),
                );
            }
            let buys = step(&mut strategy, &trade(price), dec!(0));
            let buy = buys.first().expect("should buy");
            let quantity = buy.quantity.to_string();
            println!("price {text}: quantity {quantity}");
            let decimals = quantity.split('.').nth(1).map_or(0, str::len);
            assert!(
                decimals <= 4,
                "price {text}: quantity {quantity} has {decimals} decimals"
            );
            assert!(buy.quantity > Decimal::ZERO, "{quantity}");
        }
    });
}

#[test]
fn a_tiny_leftover_is_never_ordered() {
    with_runtime(|| {
        let mut strategy = DriftScriptStrategy::from_file(&example_path()).unwrap();
        for i in 0..19 {
            step(&mut strategy, &trade(dec!(100) + Decimal::from(i)), dec!(0));
        }
        // Long and already holding (almost exactly) the target: no order at all.
        for held in [
            dec!(0.0109),
            dec!(0.01090000000000001),
            dec!(0.01089999999999999),
        ] {
            let out = step(&mut strategy, &trade(dec!(1819.87)), held);
            assert!(
                out.iter().all(|i| i.quantity >= dec!(0.0001)),
                "held {held}: {out:?}"
            );
        }
        // Dust below one step is not worth selling and is not sold.
        let down = step(&mut strategy, &trade(dec!(1)), dec!(0.00001));
        assert!(down.is_empty(), "{down:?}");
    });
}
