//! The shipped `deploy/strategy.mean_reversion.drift`, in the real interpreter:
//! it must ignore the testnet's garbage prints, buy a real dip, sell on the
//! reversion, and keep its brakes (entry cap, enabled switch, stop).

use apexes_core::{MarketEvent, Side, TradeEvent, Venue};
use apexes_strategy::drift_dsl::DriftScriptStrategy;
use apexes_strategy::{OrderIntent, Position, Strategy, StrategyContext};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::path::PathBuf;

fn script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/strategy.mean_reversion.drift")
}

fn with_runtime<T>(f: impl FnOnce() -> T) -> T {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    f()
}

struct Run {
    strategy: DriftScriptStrategy,
    held: Decimal,
    entry: Decimal,
}

impl Run {
    fn new() -> Self {
        Self {
            strategy: DriftScriptStrategy::from_file(&script()).expect("the script loads"),
            held: Decimal::ZERO,
            entry: Decimal::ZERO,
        }
    }

    /// Feeds one trade; an order it returns is filled at that price at once.
    fn tick(&mut self, price: Decimal) -> Vec<OrderIntent> {
        let event = MarketEvent::Trade(TradeEvent {
            symbol: "ETHUSDT".to_string(),
            venue: Venue::Binance,
            price,
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        let mut positions = HashMap::new();
        if !self.held.is_zero() {
            positions.insert(
                "ETHUSDT".to_string(),
                Position {
                    symbol: "ETHUSDT".to_string(),
                    quantity: self.held,
                    average_entry_price: self.entry,
                    leverage: None,
                    margin_mode: None,
                    allocated_margin: Decimal::ZERO,
                },
            );
        }
        let (balances, open, parents, margin) = (
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open,
            open_parent_orders: &parents,
            margin_status: &margin,
            now: Utc::now(),
        };
        let intents = self.strategy.on_market_event(&event, &ctx);
        for intent in &intents {
            match intent.side {
                Side::Buy => {
                    self.held = intent.quantity;
                    self.entry = price;
                }
                Side::Sell => self.held = Decimal::ZERO,
            }
        }
        intents
    }

    /// 700 trades wandering within +-0.5 of 1800: warm, with a real spread.
    fn warm_up(&mut self) {
        for i in 0..700 {
            let wobble = Decimal::from((i * 7) % 11) / dec!(5) - dec!(1);
            assert!(
                self.tick(dec!(1800) + wobble).is_empty(),
                "no order while flat"
            );
        }
    }
}

#[test]
fn it_loads_and_stays_quiet_in_a_flat_market() {
    with_runtime(|| {
        let mut run = Run::new();
        run.warm_up();
        for _ in 0..200 {
            assert!(run.tick(dec!(1800)).is_empty());
        }
    });
}

#[test]
fn a_real_dip_is_bought_and_the_reversion_is_sold() {
    with_runtime(|| {
        let mut run = Run::new();
        run.warm_up();
        // 5 below the average: 0.28%, many standard deviations, and well inside
        // the 1% guard, so it is a real dip, not a glitch.
        let buys = run.tick(dec!(1795));
        assert_eq!(buys.len(), 1, "{buys:?}");
        assert_eq!(buys[0].side, Side::Buy);
        assert_eq!(
            buys[0].quantity,
            dec!(0.0278),
            "50 USDT at 1795, whole steps"
        );
        for _ in 0..5 {
            assert!(
                run.tick(dec!(1796)).is_empty(),
                "holds while below the average"
            );
        }
        let sells = run.tick(dec!(1801));
        assert_eq!(sells.len(), 1, "{sells:?}");
        assert_eq!(sells[0].side, Side::Sell);
        assert_eq!(
            sells[0].quantity,
            dec!(0.0278),
            "sells exactly what it holds"
        );
    });
}

/// The reported hazard: bursts of prints hundreds of dollars off the market.
#[test]
fn a_glitch_print_is_ignored_and_never_buys_a_phantom_crash() {
    with_runtime(|| {
        let mut run = Run::new();
        run.warm_up();
        for glitch in [dec!(1500), dec!(1490), dec!(1510), dec!(2100)] {
            assert!(
                run.tick(glitch).is_empty(),
                "garbage print {glitch} must not trade"
            );
        }
        // The market was never disturbed: a real dip afterwards still buys.
        assert_eq!(run.tick(dec!(1795)).len(), 1);
    });
}

/// A real move (not a glitch) lasts: after `guard_resync` rejected prints in a
/// row the guard re-anchors, so the strategy is never blind to a crash.
#[test]
fn a_sustained_move_re_anchors_the_guard_instead_of_blinding_the_strategy() {
    with_runtime(|| {
        let mut run = Run::new();
        run.warm_up();
        let mut traded = false;
        for i in 0..40 {
            traded |= !run.tick(dec!(1700) + Decimal::from(i % 3)).is_empty();
        }
        assert!(
            traded,
            "after 30 rejected prints the new level is accepted and the dip is bought"
        );
    });
}

#[test]
fn a_stop_ends_a_trade_that_keeps_falling() {
    with_runtime(|| {
        let mut run = Run::new();
        run.warm_up();
        assert_eq!(run.tick(dec!(1795)).len(), 1);
        for _ in 0..5 {
            run.tick(dec!(1794));
        }
        // 0.5% under 1795 is 1786.0: a falling knife is sold, not averaged down.
        let sells = run.tick(dec!(1785));
        assert_eq!(sells.len(), 1, "{sells:?}");
        assert_eq!(sells[0].side, Side::Sell);
    });
}

/// Orders count against the launch config's daily limit, buys and sells alike:
/// an entry cap leaves room for every exit.
#[test]
fn the_entry_cap_stops_new_buys_but_never_the_exit() {
    with_runtime(|| {
        let mut run = Run::new();
        run.strategy.set_param("max_entries", dec!(1)).unwrap();
        run.warm_up();
        assert_eq!(run.tick(dec!(1795)).len(), 1, "the one allowed entry");
        for _ in 0..5 {
            run.tick(dec!(1796));
        }
        assert_eq!(run.tick(dec!(1801)).len(), 1, "and its exit");
        for _ in 0..40 {
            run.tick(dec!(1800));
        }
        assert!(run.tick(dec!(1795)).is_empty(), "no second entry");
    });
}

#[test]
fn switching_it_off_sells_what_is_held_and_buys_nothing_more() {
    with_runtime(|| {
        let mut run = Run::new();
        run.warm_up();
        assert_eq!(run.tick(dec!(1795)).len(), 1);
        run.strategy.set_param("enabled", dec!(0)).unwrap();
        for _ in 0..5 {
            run.tick(dec!(1795));
        }
        let out = run.tick(dec!(1795));
        assert!(
            out.iter().all(|o| o.side == Side::Sell),
            "only exits once disabled: {out:?}"
        );
        for _ in 0..60 {
            assert!(run.tick(dec!(1790)).iter().all(|o| o.side == Side::Sell));
        }
    });
}

#[test]
fn every_parameter_the_header_documents_is_exposed_to_the_console() {
    with_runtime(|| {
        let mut run = Run::new();
        run.warm_up();
        run.tick(dec!(1800));
        let params = run.strategy.current_params();
        for name in [
            "notional",
            "step",
            "enabled",
            "entry_z",
            "min_edge_pct",
            "stop_pct",
            "max_hold",
            "cooldown",
            "order_gap",
            "max_entries",
            "guard_pct",
            "guard_resync",
            "min_notional",
        ] {
            assert!(params.contains_key(name), "{name} missing from {params:?}");
        }
    });
}
