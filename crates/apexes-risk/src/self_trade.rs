//! [`SelfTradePreventer`]: rejects an `OrderIntent` that would risk
//! trading against the caller's own resting opposite-side orders.
//! Limit/StopLimit orders are checked for actual price-range overlap;
//! Market orders are rejected outright against any opposite-side open
//! order for the same symbol, since a Market order's execution price
//! isn't known in advance and this check errs conservative.

use std::collections::HashMap;

use apexes_core::{Order, OrderId, OrderType, Side};
use apexes_strategy::{OrderIntent, Position};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::error::RiskViolation;
use crate::pre_trade::PreTradeCheck;

pub struct SelfTradePreventer;

impl PreTradeCheck for SelfTradePreventer {
    fn check(
        &self,
        intent: &OrderIntent,
        _positions: &HashMap<String, Position>,
        _balances: &HashMap<String, Decimal>,
        open_orders: &HashMap<OrderId, Order>,
        _now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let opposite_side = match intent.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let risky = open_orders.values().any(|order| {
            if order.symbol != intent.symbol || order.side != opposite_side {
                return false;
            }

            match (intent.order_type, intent.price, order.price) {
                // Market intent against any resting opposite-side order
                // for this symbol: reject outright (no price to compare).
                (OrderType::Market, _, _) => true,
                // Limit/StopLimit intent against a resting order that
                // also has a price: overlap check. A Buy overlaps a
                // resting Sell if buy_price >= sell_price (the buy would
                // be willing to pay at least as much as the resting sell
                // wants); a Sell overlaps a resting Buy if
                // sell_price <= buy_price (symmetric case).
                (_, Some(intent_price), Some(order_price)) => match intent.side {
                    Side::Buy => intent_price >= order_price,
                    Side::Sell => intent_price <= order_price,
                },
                // Intent has a price but the resting order doesn't (the
                // resting order is itself a Market order, which
                // shouldn't normally still be "open" since Markets fill
                // immediately -- but if it somehow is, treat as risky
                // conservatively rather than assuming safe).
                (_, Some(_), None) => true,
                // Intent has no price (shouldn't happen for non-Market
                // order_type per apexes_strategy::OrderIntent's field
                // semantics, but handled defensively): treat as risky.
                (_, None, _) => true,
            }
        });

        if risky {
            Err(vec![RiskViolation::SelfTradeRisk {
                symbol: intent.symbol.clone(),
            }])
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, OrderStatus, Venue};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap()
    }

    fn empty_positions() -> HashMap<String, Position> {
        HashMap::new()
    }

    fn empty_balances() -> HashMap<String, Decimal> {
        HashMap::new()
    }

    fn limit_intent(symbol: &str, side: Side, price: Decimal) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(price),
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    fn market_intent(symbol: &str, side: Side) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    fn resting_limit_order(symbol: &str, side: Side, price: Decimal) -> (OrderId, Order) {
        let id = OrderId(Uuid::new_v4());
        let order = Order {
            id,
            exchange_order_id: None,
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(price),
            stop_price: None,
            leverage: None,
            margin_mode: None,
            filled_quantity: dec!(0),
            status: OrderStatus::New,
            created_at: now(),
            updated_at: now(),
        };
        (id, order)
    }

    fn open_orders_with(order: (OrderId, Order)) -> HashMap<OrderId, Order> {
        let mut map = HashMap::new();
        map.insert(order.0, order.1);
        map
    }

    #[test]
    fn buy_overlapping_resting_sell_price_is_rejected() {
        let preventer = SelfTradePreventer;
        let open_orders = open_orders_with(resting_limit_order("BTCUSDT", Side::Sell, dec!(100)));
        let intent = limit_intent("BTCUSDT", Side::Buy, dec!(100));
        let result = preventer.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &open_orders,
            now(),
        );
        assert_eq!(
            result,
            Err(vec![RiskViolation::SelfTradeRisk {
                symbol: "BTCUSDT".to_string(),
            }])
        );
    }

    #[test]
    fn buy_below_resting_sell_price_does_not_overlap() {
        let preventer = SelfTradePreventer;
        let open_orders = open_orders_with(resting_limit_order("BTCUSDT", Side::Sell, dec!(100)));
        let intent = limit_intent("BTCUSDT", Side::Buy, dec!(90));
        let result = preventer.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &open_orders,
            now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn sell_overlapping_resting_buy_price_is_rejected() {
        let preventer = SelfTradePreventer;
        let open_orders = open_orders_with(resting_limit_order("BTCUSDT", Side::Buy, dec!(100)));
        let intent = limit_intent("BTCUSDT", Side::Sell, dec!(100));
        let result = preventer.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &open_orders,
            now(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn sell_above_resting_buy_price_does_not_overlap() {
        let preventer = SelfTradePreventer;
        let open_orders = open_orders_with(resting_limit_order("BTCUSDT", Side::Buy, dec!(100)));
        let intent = limit_intent("BTCUSDT", Side::Sell, dec!(110));
        let result = preventer.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &open_orders,
            now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn market_intent_rejected_against_any_opposite_side_open_order() {
        let preventer = SelfTradePreventer;
        let open_orders =
            open_orders_with(resting_limit_order("BTCUSDT", Side::Sell, dec!(999999)));
        let intent = market_intent("BTCUSDT", Side::Buy);
        let result = preventer.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &open_orders,
            now(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn same_side_open_order_never_triggers_self_trade_risk() {
        let preventer = SelfTradePreventer;
        let open_orders = open_orders_with(resting_limit_order("BTCUSDT", Side::Buy, dec!(100)));
        let intent = limit_intent("BTCUSDT", Side::Buy, dec!(100));
        let result = preventer.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &open_orders,
            now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn different_symbol_open_order_never_triggers_self_trade_risk() {
        let preventer = SelfTradePreventer;
        let open_orders = open_orders_with(resting_limit_order("ETHUSDT", Side::Sell, dec!(1)));
        let intent = limit_intent("BTCUSDT", Side::Buy, dec!(999999));
        let result = preventer.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &open_orders,
            now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn no_open_orders_never_triggers_self_trade_risk() {
        let preventer = SelfTradePreventer;
        let intent = limit_intent("BTCUSDT", Side::Buy, dec!(100));
        let result = preventer.check(
            &intent,
            &empty_positions(),
            &empty_balances(),
            &HashMap::new(),
            now(),
        );
        assert!(result.is_ok());
    }
}
