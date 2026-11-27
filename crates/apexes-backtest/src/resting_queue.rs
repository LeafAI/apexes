//! F03.02 — shared, conservative trade-volume accounting for resting orders.

use super::*;
use apexes_core::TradeEvent;

impl RestingOrder {
    fn eligible(&self, trade: &TradeEvent) -> bool {
        trade.timestamp >= self.order.created_at
            && self.order.price.is_some_and(|price| match self.order.side {
                Side::Buy => trade.price <= price,
                Side::Sell => trade.price >= price,
            })
    }

    fn maker_fill(
        &mut self,
        quantity: Decimal,
        now: DateTime<Utc>,
        cost: &CostModel,
        ids: &mut MatchingIds,
    ) -> Option<Fill> {
        let price = self.order.price?;
        if quantity <= Decimal::ZERO {
            return None;
        }
        self.remaining_quantity -= quantity;
        self.order.filled_quantity += quantity;
        self.order.updated_at = now;
        self.order.status = if self.remaining_quantity.is_zero() {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };
        Some(Fill {
            id: ids.fill(),
            order_id: self.order_id,
            symbol: self.order.symbol.clone(),
            venue: self.order.venue,
            side: self.order.side,
            price,
            quantity,
            fee: cost.fee_for_fill(self.order.venue, FillLiquidity::Maker, price * quantity),
            fee_currency: None,
            filled_at: now,
            leverage: self.order.leverage,
            margin_mode: self.order.margin_mode,
        })
    }
}

fn maker_side(orders: &[RestingOrder], trade: &TradeEvent) -> Option<Side> {
    match trade.side {
        Some(Side::Buy) => Some(Side::Sell),
        Some(Side::Sell) => Some(Side::Buy),
        None => {
            let mut sides = orders
                .iter()
                .filter(|o| o.eligible(trade))
                .map(|o| o.order.side);
            let side = sides.next()?;
            sides.all(|candidate| candidate == side).then_some(side)
        }
    }
}

#[derive(Default)]
struct TradeBudget {
    remaining: Decimal,
    external_spent: Decimal,
}

impl TradeBudget {
    fn allocate(&mut self, order: &mut RestingOrder) -> Decimal {
        // Earlier external consumption is part of the same side's
        // cumulative at-or-better queue estimate. Own fills are not.
        order.external_ahead_remaining -= order.external_ahead_remaining.min(self.external_spent);
        let external = order.external_ahead_remaining.min(self.remaining);
        order.external_ahead_remaining -= external;
        self.remaining -= external;
        self.external_spent += external;
        let quantity = order.remaining_quantity.min(self.remaining);
        self.remaining -= quantity;
        quantity
    }
}

impl MatchingEngine {
    pub(super) fn fill_resting_orders(
        orders: &mut Vec<RestingOrder>,
        trade: &TradeEvent,
        now: DateTime<Utc>,
        cost_model: &CostModel,
        ids: &mut MatchingIds,
        check: &mut Option<&mut apexes_strategy::order_executor::SimulatedRestingCheck<'_>>,
    ) -> Vec<(OrderId, Vec<Fill>)> {
        if trade.quantity <= Decimal::ZERO || trade.price <= Decimal::ZERO || trade.timestamp > now
        {
            return Vec::new();
        }
        let Some(side) = maker_side(orders, trade) else {
            return Vec::new();
        };
        // Stable sort preserves FIFO admission order at equal prices.
        orders.sort_by(|a, b| match (a.order.side, b.order.side) {
            (Side::Buy, Side::Buy) => b.order.price.cmp(&a.order.price),
            (Side::Sell, Side::Sell) => a.order.price.cmp(&b.order.price),
            (Side::Buy, Side::Sell) => std::cmp::Ordering::Less,
            (Side::Sell, Side::Buy) => std::cmp::Ordering::Greater,
        });
        let mut budget = TradeBudget {
            remaining: trade.quantity,
            ..Default::default()
        };
        let mut results = Vec::new();
        for order in orders
            .iter_mut()
            .filter(|o| o.order.side == side && o.eligible(trade))
        {
            let quantity = budget.allocate(order);
            let mut candidate = order.clone();
            if let Some(fill) = candidate.maker_fill(quantity, now, cost_model, ids) {
                if let Some(check) = check.as_deref_mut()
                    && let Err(reason) = check(
                        order.order_id,
                        std::slice::from_ref(&fill),
                        candidate.remaining_quantity.is_zero(),
                    )
                {
                    // Cancel the unfundable order. Its rejected quantity
                    // remains available to later price-time orders.
                    budget.remaining += quantity;
                    order.remaining_quantity = Decimal::ZERO;
                    tracing::warn!(order_id = %order.order_id.0, %reason, "simulated resting fill rejected");
                    continue;
                }
                *order = candidate;
                results.push((order.order_id, vec![fill]));
            }
        }
        orders.retain(|o| !o.remaining_quantity.is_zero());
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, PriceLevel};
    use apexes_strategy::OrderRouting;
    use chrono::TimeDelta;
    use rust_decimal_macros::dec;

    fn engine(factor: Decimal) -> MatchingEngine {
        MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(crate::cost_model::FlatFeeSchedule {
                    maker_bps: dec!(5),
                    taker_bps: dec!(20),
                }),
                funding_rate_bps: None,
            },
            factor,
        )
    }

    fn intent(side: Side, price: Decimal, quantity: Decimal) -> OrderIntent {
        OrderIntent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side,
            order_type: OrderType::Limit,
            quantity,
            price: Some(price),
            stop_price: None,
            execution_algo: None,
            routing: OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    fn snapshot(engine: &mut MatchingEngine, at: DateTime<Utc>, quantity: Decimal, sequence: u64) {
        engine.on_depth_update(&DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: dec!(100),
                quantity,
            }],
            asks: vec![PriceLevel {
                price: dec!(110),
                quantity: dec!(10),
            }],
            sequence: Some(sequence),
            source_range: None,
            is_snapshot: true,
            timestamp: at,
        });
    }

    fn trade(
        at: DateTime<Utc>,
        price: Decimal,
        quantity: Decimal,
        side: Option<Side>,
    ) -> TradeEvent {
        TradeEvent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            price,
            quantity,
            side,
            trade_id: None,
            timestamp: at,
        }
    }

    #[test]
    fn rejected_direct_crossing_does_not_consume_depth() {
        let mut e = engine(dec!(0));
        let at = Utc::now();
        snapshot(&mut e, at, dec!(10), 1);
        let mut order = intent(Side::Sell, dec!(90), dec!(10));
        order.asset_class = AssetClass::Perpetual;
        order.leverage = Some(dec!(2));
        let mut guard = |_: &[Fill], _: Decimal| Err("unfunded margin".into());
        assert!(e.submit_direct_checked(&order, at, &mut guard).is_err());
        let mut accept = |fills: &[Fill], remaining: Decimal| {
            assert_eq!(remaining, Decimal::ZERO);
            assert_eq!(fills[0].quantity, dec!(10));
            Ok(())
        };
        let (order, fills) = e.submit_direct_checked(&order, at, &mut accept).unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(fills[0].price, dec!(100));
    }

    #[test]
    fn rejected_maker_does_not_consume_other_orders_print_budget() {
        let mut e = engine(dec!(0));
        let at = Utc::now();
        let first = e
            .submit_limit_order(&intent(Side::Buy, dec!(100), dec!(1)), at)
            .unwrap();
        let second = e
            .submit_limit_order(&intent(Side::Buy, dec!(100), dec!(1)), at)
            .unwrap();
        let t = at + TimeDelta::seconds(1);
        let mut guard = |id: OrderId, _: &[Fill], _: bool| {
            if id == first.id {
                Err("unfundable".into())
            } else {
                Ok(())
            }
        };
        let fills = e.on_trade_event_checked(
            &trade(t, dec!(100), dec!(1), Some(Side::Sell)),
            t,
            Some(&mut guard),
        );
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].0, second.id);
        assert_eq!(fills[0].1[0].quantity, dec!(1));
        assert!(!e.is_working(first.id));
        assert!(!e.is_working(second.id));
    }

    #[test]
    fn rejected_crossing_stop_limit_leaves_depth_for_next_order() {
        let mut e = engine(dec!(0));
        let at = Utc::now();
        snapshot(&mut e, at, dec!(10), 1);
        let mut stop = intent(Side::Buy, dec!(110), dec!(10));
        stop.order_type = OrderType::StopLimit;
        stop.stop_price = Some(dec!(105));
        let first = e.submit_stop_order(&stop, at).unwrap();
        let second = e.submit_stop_order(&stop, at).unwrap();
        let t = at + TimeDelta::seconds(1);
        let mut guard = |id: OrderId, _: &[Fill], _: bool| {
            if id == first.id {
                Err("unfundable".into())
            } else {
                Ok(())
            }
        };
        let fills = e.on_trade_event_checked(
            &trade(t, dec!(105), dec!(1), Some(Side::Buy)),
            t,
            Some(&mut guard),
        );
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].0, second.id);
        assert_eq!(fills[0].1[0].quantity, dec!(10));
        assert!(!e.is_working(first.id));
        assert!(!e.is_working(second.id));
    }

    #[test]
    fn limit_budget_uses_larger_fee_and_price_bound() {
        let e = engine(dec!(0));
        assert_eq!(
            e.simulated_buy_budget(&intent(Side::Buy, dec!(100), dec!(2)), Utc::now())
                .unwrap(),
            dec!(200.4)
        );
    }

    #[test]
    fn external_queue_and_fills_share_one_print_budget() {
        let mut e = engine(dec!(1));
        let at = Utc::now();
        snapshot(&mut e, at, dec!(10), 1);
        let first = e
            .submit_limit_order(&intent(Side::Buy, dec!(100), dec!(1)), at)
            .unwrap();
        let later = at + TimeDelta::seconds(1);
        snapshot(&mut e, later, dec!(0), 2);
        let second = e
            .submit_limit_order(&intent(Side::Buy, dec!(100), dec!(10)), later)
            .unwrap();
        let t = later + TimeDelta::seconds(1);
        let fills = e.on_trade_event(&trade(t, dec!(100), dec!(11), Some(Side::Sell)), t);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].0, first.id);
        assert_eq!(fills[0].1[0].quantity, dec!(1));
        let t = t + TimeDelta::seconds(1);
        let fills = e.on_trade_event(&trade(t, dec!(100), dec!(2), Some(Side::Sell)), t);
        assert_eq!(fills[0].0, second.id);
        assert_eq!(fills[0].1[0].quantity, dec!(2));
        assert_eq!(fills[0].1[0].fee, dec!(0.1));
    }

    #[test]
    fn shared_queue_depletion_is_not_charged_twice() {
        let mut e = engine(dec!(1));
        let at = Utc::now();
        snapshot(&mut e, at, dec!(10), 1);
        let first = e
            .submit_limit_order(&intent(Side::Buy, dec!(100), dec!(2)), at)
            .unwrap();
        let second = e
            .submit_limit_order(&intent(Side::Buy, dec!(100), dec!(3)), at)
            .unwrap();
        let t = at + TimeDelta::seconds(1);
        let fills = e.on_trade_event(&trade(t, dec!(100), dec!(13), Some(Side::Sell)), t);
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].0, first.id);
        assert_eq!(fills[0].1[0].quantity, dec!(2));
        assert_eq!(fills[1].0, second.id);
        assert_eq!(fills[1].1[0].quantity, dec!(1));
        let t = t + TimeDelta::seconds(1);
        let fills = e.on_trade_event(&trade(t, dec!(100), dec!(2), Some(Side::Sell)), t);
        assert_eq!(fills[0].0, second.id);
        assert_eq!(fills[0].1[0].quantity, dec!(2));
        assert!(!e.is_working(second.id));
    }

    #[test]
    fn better_price_precedes_earlier_worse_price_on_both_sides() {
        for (side, aggressor, worse, better, price) in [
            (Side::Buy, Side::Sell, dec!(98), dec!(100), dec!(98)),
            (Side::Sell, Side::Buy, dec!(112), dec!(110), dec!(112)),
        ] {
            let mut e = engine(dec!(0));
            let at = Utc::now();
            let first = e
                .submit_limit_order(&intent(side, worse, dec!(1)), at)
                .unwrap();
            let second = e
                .submit_limit_order(&intent(side, better, dec!(1)), at)
                .unwrap();
            let t = at + TimeDelta::seconds(1);
            let fills = e.on_trade_event(&trade(t, price, dec!(1), Some(aggressor)), t);
            assert_eq!(fills.len(), 1);
            assert_eq!(fills[0].0, second.id);
            assert!(e.is_working(first.id));
        }
    }

    #[test]
    fn wrong_side_ambiguous_and_noncausal_prints_do_not_fill() {
        let mut e = engine(dec!(0));
        let at = Utc::now();
        let buy = e
            .submit_limit_order(&intent(Side::Buy, dec!(100), dec!(1)), at)
            .unwrap();
        let t = at + TimeDelta::seconds(1);
        for event in [
            trade(t, dec!(100), dec!(1), Some(Side::Buy)),
            trade(
                at - TimeDelta::seconds(1),
                dec!(100),
                dec!(1),
                Some(Side::Sell),
            ),
            trade(
                t + TimeDelta::seconds(1),
                dec!(100),
                dec!(1),
                Some(Side::Sell),
            ),
            trade(t, dec!(100), dec!(0), Some(Side::Sell)),
        ] {
            assert!(e.on_trade_event(&event, t).is_empty());
        }
        let sell = e
            .submit_limit_order(&intent(Side::Sell, dec!(100), dec!(1)), at)
            .unwrap();
        assert!(
            e.on_trade_event(&trade(t, dec!(100), dec!(1), None), t)
                .is_empty()
        );
        let fills = e.on_trade_event(&trade(t, dec!(100), dec!(1), Some(Side::Sell)), t);
        assert_eq!(fills[0].0, buy.id);
        assert!(e.is_working(sell.id));
    }
}
