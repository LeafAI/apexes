use apexes_core::{AssetClass, Order, OrderId, OrderType, Side, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// The shared strategy interface implemented by both the backtest engine
/// (`apexes-backtest`) and all future live-trading sub-projects (#6 Paper
/// Trading, #7 OMS, #8 live crypto gateway), per the project's
/// non-negotiable backtest/live logic-parity requirement. A `Strategy`
/// implementation never needs to know whether it is running in backtest
/// or live mode.
pub trait Strategy: Send {
    /// Called once per incoming `MarketEvent`, in timestamp order.
    fn on_market_event(
        &mut self,
        event: &apexes_core::MarketEvent,
        ctx: &StrategyContext,
    ) -> Vec<OrderIntent>;
}

/// Read-only view of current backtest/live state, passed to
/// `Strategy::on_market_event` so it can make informed order decisions.
pub struct StrategyContext<'a> {
    /// Current net positions, keyed by symbol.
    pub positions: &'a HashMap<String, Position>,
    /// Current balances, keyed by asset symbol (e.g. "USDT", "BTC").
    pub balances: &'a HashMap<String, Decimal>,
    /// Currently-open (unfilled or partially-filled) orders, keyed by
    /// `OrderId`.
    ///
    /// Triggered Stop orders: a Stop order that triggers converts
    /// internally to a Market order, and its fills are reported under the
    /// ORIGINAL `OrderId` this map already holds -- the runner's
    /// `open_orders.get_mut(&order_id)` lookup finds the same entry, so
    /// the strategy observes the fill on the order it placed.
    ///
    /// Known limitation (StopLimit only): a triggered StopLimit converts
    /// to a NEW resting Limit order tracked under a NEW `OrderId`; the
    /// original OrderId's entry in this map will not reflect fills of
    /// the converted order. See
    /// docs/superpowers/plans/2026-07-28-apexes-backtest.md Task 14 for
    /// full rationale.
    pub open_orders: &'a HashMap<OrderId, Order>,
    /// Current simulated (backtest) or real (live) time.
    pub now: DateTime<Utc>,
}

/// An order a `Strategy` wants to place, returned from
/// `on_market_event`. Distinct from `apexes_core::Order` (which represents
/// an order already accepted into the system with an assigned `OrderId`
/// and lifecycle state) -- `OrderIntent` is the strategy's *request*,
/// before the matching engine (backtest) or OMS (live) assigns it an
/// identity and begins tracking its status.
#[derive(Debug, Clone)]
pub struct OrderIntent {
    pub symbol: String,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    /// Limit price (`Limit`, `StopLimit` only).
    pub price: Option<Decimal>,
    /// Trigger price (`Stop`, `StopLimit` only).
    pub stop_price: Option<Decimal>,
}

/// A net position in a single symbol.
#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    pub symbol: String,
    /// Signed: positive = long, negative = short.
    pub quantity: Decimal,
    pub average_entry_price: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::MarketEvent;
    use rust_decimal_macros::dec;

    /// A trivial test `Strategy` that emits a fixed `OrderIntent` on every
    /// event, used to verify the trait's call shape compiles and works as
    /// expected. Real strategy logic will be added by future sub-projects
    /// consuming this trait, not by this plan.
    struct AlwaysBuyOneUnit {
        symbol: String,
        venue: Venue,
    }

    impl Strategy for AlwaysBuyOneUnit {
        fn on_market_event(
            &mut self,
            _event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            vec![OrderIntent {
                symbol: self.symbol.clone(),
                venue: self.venue,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
            }]
        }
    }

    #[test]
    fn strategy_trait_is_callable_and_returns_intents() {
        let mut strategy = AlwaysBuyOneUnit {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
        };
        let positions = HashMap::new();
        let balances = HashMap::new();
        let open_orders = HashMap::new();
        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now: Utc::now(),
        };
        let event = MarketEvent::Trade(apexes_core::TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });

        let intents = strategy.on_market_event(&event, &ctx);

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].symbol, "BTCUSDT");
        assert_eq!(intents[0].side, Side::Buy);
        assert_eq!(intents[0].quantity, dec!(1));
    }

    #[test]
    fn strategy_context_exposes_provided_state() {
        let mut positions = HashMap::new();
        positions.insert(
            "BTCUSDT".to_string(),
            Position {
                symbol: "BTCUSDT".to_string(),
                quantity: dec!(2),
                average_entry_price: dec!(64000),
            },
        );
        let balances = HashMap::new();
        let open_orders = HashMap::new();
        let now = Utc::now();

        let ctx = StrategyContext {
            positions: &positions,
            balances: &balances,
            open_orders: &open_orders,
            now,
        };

        assert_eq!(ctx.positions.get("BTCUSDT").unwrap().quantity, dec!(2));
        assert_eq!(ctx.now, now);
    }
}
