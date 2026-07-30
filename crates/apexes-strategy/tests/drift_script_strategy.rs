use apexes_core::{MarketEvent, Side, TradeEvent, Venue};
use apexes_strategy::drift_dsl::DriftScriptStrategy;
use apexes_strategy::{Position, Strategy, StrategyContext};
use chrono::Utc;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::path::PathBuf;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn sample_trade_event() -> MarketEvent {
    MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        price: dec!(65000),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: Utc::now(),
    })
}

type EmptyCtxState = (
    HashMap<String, Position>,
    HashMap<String, rust_decimal::Decimal>,
    HashMap<apexes_core::OrderId, apexes_core::Order>,
    chrono::DateTime<Utc>,
);

fn empty_ctx(now: chrono::DateTime<Utc>) -> EmptyCtxState {
    (HashMap::new(), HashMap::new(), HashMap::new(), now)
}

/// `DriftScriptStrategy` bridges its sync `Strategy` trait methods to the
/// async `Interpreter` via `tokio::runtime::Handle::current().block_on(..)`
/// internally. That call panics if invoked from a thread that is already
/// driving an async task (e.g. inside `#[tokio::test] async fn`), on any
/// runtime flavor -- `block_on` cannot nest inside an async execution
/// context. So these tests run as plain sync `#[test]`s on a thread that
/// has merely `enter()`ed a runtime (giving `Handle::current()` something
/// to find) without itself being polled as a task.
fn with_runtime<T>(f: impl FnOnce() -> T) -> T {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    f()
}

#[test]
fn always_buy_returns_one_buy_intent() {
    with_runtime(|| {
        let mut strategy =
            DriftScriptStrategy::from_file(&fixture_path("always_buy.drift")).unwrap();
        let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };
        let event = sample_trade_event();

        let intents = strategy.on_market_event(&event, &ctx);

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].symbol, "BTCUSDT");
        assert_eq!(intents[0].venue, Venue::Binance);
        assert_eq!(intents[0].side, Side::Buy);
    });
}

#[test]
fn uses_math_binding_computes_average_and_buys() {
    with_runtime(|| {
        let mut strategy =
            DriftScriptStrategy::from_file(&fixture_path("uses_math.drift")).unwrap();
        let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };
        let event = sample_trade_event();

        let intents = strategy.on_market_event(&event, &ctx);

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].quantity, dec!(1));
    });
}

#[test]
fn uses_ctx_position_skips_when_already_long() {
    with_runtime(|| {
        let mut strategy =
            DriftScriptStrategy::from_file(&fixture_path("uses_ctx_position.drift")).unwrap();
        let mut positions = HashMap::new();
        positions.insert(
            "BTCUSDT".to_string(),
            Position {
                symbol: "BTCUSDT".to_string(),
                quantity: dec!(1),
                average_entry_price: dec!(64000),
            },
        );
        let balances = HashMap::new();
        let open_orders = HashMap::new();
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now: Utc::now(),
        };
        let event = sample_trade_event();

        let intents = strategy.on_market_event(&event, &ctx);

        assert_eq!(
            intents.len(),
            0,
            "should not buy again when already holding a position"
        );
    });
}

#[test]
fn uses_ctx_position_buys_when_flat_and_no_open_orders() {
    with_runtime(|| {
        let mut strategy =
            DriftScriptStrategy::from_file(&fixture_path("uses_ctx_position.drift")).unwrap();
        let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };
        let event = sample_trade_event();

        let intents = strategy.on_market_event(&event, &ctx);

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].side, Side::Buy);
    });
}

#[test]
fn syntax_error_script_fails_construction() {
    let result = DriftScriptStrategy::from_file(&fixture_path("syntax_error.drift"));
    assert!(result.is_err());
}

#[test]
fn missing_entry_point_fails_construction() {
    with_runtime(|| {
        let path = fixture_path("no_entry_point_temp.drift");
        std::fs::write(&path, "let x = 1;").unwrap();
        let result = DriftScriptStrategy::from_file(&path);
        std::fs::remove_file(&path).ok();
        assert!(matches!(
            result,
            Err(apexes_strategy::StrategyError::MissingEntryPoint)
        ));
    });
}

#[test]
fn runtime_error_script_returns_empty_vec_not_panic() {
    with_runtime(|| {
        let mut strategy =
            DriftScriptStrategy::from_file(&fixture_path("runtime_panic.drift")).unwrap();
        let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };
        let event = sample_trade_event();

        let intents = strategy.on_market_event(&event, &ctx);

        assert!(
            intents.is_empty(),
            "runtime script error must yield an empty Vec, not panic"
        );
    });
}

#[test]
fn uses_indicators_sma_stays_flat_during_warmup_then_buys() {
    with_runtime(|| {
        let mut strategy =
            DriftScriptStrategy::from_file(&fixture_path("uses_indicators.drift")).unwrap();
        let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };

        // SMA(3) warm-up: first two calls, regardless of price, must
        // return no orders (sma.update returns null on the script side).
        let first_event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(10),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        assert_eq!(strategy.on_market_event(&first_event, &ctx).len(), 0);

        let second_event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(11),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        assert_eq!(strategy.on_market_event(&second_event, &ctx).len(), 0);

        // Third call: SMA(3) over [10, 11, 12] = 11; price 12 > 11 -> buy.
        let third_event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(12),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        let intents = strategy.on_market_event(&third_event, &ctx);
        assert_eq!(
            intents.len(),
            1,
            "SMA should be warm by the 3rd tick and trigger a buy"
        );
        assert_eq!(intents[0].side, Side::Buy);
    });
}

#[test]
fn uses_indicators_sma_state_is_independent_per_strategy_instance() {
    with_runtime(|| {
        // A second, freshly-constructed DriftScriptStrategy must start
        // its own SMA warm-up from scratch -- proving indicator state is
        // NOT accidentally shared across separate script instances (e.g.
        // via a process-global registry), only within one instance's
        // persistent top-level environment.
        let mut strategy =
            DriftScriptStrategy::from_file(&fixture_path("uses_indicators.drift")).unwrap();
        let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };
        let event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(999),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        assert_eq!(
            strategy.on_market_event(&event, &ctx).len(),
            0,
            "a brand-new strategy instance must start its own warm-up, not inherit state"
        );
    });
}

#[test]
fn uses_signals_produces_no_order_during_indicator_warmup() {
    with_runtime(|| {
        let mut strategy =
            DriftScriptStrategy::from_file(&fixture_path("uses_signals.drift")).unwrap();
        let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };
        let event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(10),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });

        // SMA(3) warm-up: first call must produce no orders.
        let intents = strategy.on_market_event(&event, &ctx);
        assert_eq!(
            intents.len(),
            0,
            "should produce no orders while the SMA is still warming up"
        );
    });
}

#[test]
fn uses_signals_produces_rebalance_order_once_warm() {
    with_runtime(|| {
        let mut strategy =
            DriftScriptStrategy::from_file(&fixture_path("uses_signals.drift")).unwrap();
        let (positions, balances, open_orders, now) = empty_ctx(Utc::now());
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };

        // Prices [10, 11, 12]: SMA(3) becomes warm on the 3rd call
        // (average of [10,11,12] = 11), and price (12) > sma (11), so
        // raw_score = 0.8 -> composite = 0.8 -> target_position(0.8, 10,
        // 0.2) = 10*(0.8-0.2)/(1-0.2) = 7.5 -> rebalance_order(7.5, 0,
        // ..., 0.01) -> a single Buy order for quantity 7.5.
        for price in [dec!(10), dec!(11)] {
            let event = MarketEvent::Trade(TradeEvent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                price,
                quantity: dec!(1),
                side: None,
                trade_id: None,
                timestamp: Utc::now(),
            });
            let intents = strategy.on_market_event(&event, &ctx);
            assert_eq!(intents.len(), 0, "still warming up at price {price}");
        }

        let third_event = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(12),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        let intents = strategy.on_market_event(&third_event, &ctx);
        assert_eq!(
            intents.len(),
            1,
            "SMA should be warm by the 3rd tick and trigger a rebalance"
        );
        assert_eq!(intents[0].side, Side::Buy);
        assert!(
            (intents[0].quantity - dec!(7.5)).abs() < dec!(0.0001),
            "expected quantity ~7.5, got {}",
            intents[0].quantity
        );
    });
}
