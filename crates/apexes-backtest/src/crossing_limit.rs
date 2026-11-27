//! F03.02 direct and triggered marketable limits: take causal opposite-side liquidity,
//! then enqueue only the unfilled GTC remainder.
use super::*;

impl MatchingEngine {
    pub fn submit_executable_limit_order(
        &mut self,
        intent: &OrderIntent,
        now: DateTime<Utc>,
    ) -> Result<(Order, Vec<Fill>), ApexesError> {
        let order = self.prepare_limit_order(intent, now)?;
        self.execute_prepared_limit(order, now)
    }

    pub(super) fn execute_prepared_limit(
        &mut self,
        order: Order,
        now: DateTime<Utc>,
    ) -> Result<(Order, Vec<Fill>), ApexesError> {
        self.execute_prepared_limit_checked(order, now, &mut None)
    }

    pub(super) fn execute_prepared_limit_checked(
        &mut self,
        mut order: Order,
        now: DateTime<Utc>,
        check: &mut Option<&mut apexes_strategy::order_executor::SimulatedRestingCheck<'_>>,
    ) -> Result<(Order, Vec<Fill>), ApexesError> {
        let limit = order.price.ok_or_else(|| ApexesError::OrderRejected {
            reason: "Limit order requires a price".into(),
        })?;
        let mut fills = Vec::new();
        if let Some(book) = self.books.get_mut(&(order.venue, order.symbol.clone()))
            && book.is_causal_at(now)
        {
            let levels = match order.side {
                Side::Buy => book.asks(),
                Side::Sell => book.bids(),
            };
            for level in levels {
                let crosses = match order.side {
                    Side::Buy => level.price <= limit,
                    Side::Sell => level.price >= limit,
                };
                if !crosses || order.filled_quantity == order.quantity {
                    break;
                }
                let quantity = (order.quantity - order.filled_quantity).min(level.quantity);
                fills.push(Fill {
                    id: self.ids.fill(),
                    order_id: order.id,
                    symbol: order.symbol.clone(),
                    venue: order.venue,
                    side: order.side,
                    price: level.price,
                    quantity,
                    fee: self.cost_model.fee_for_fill(
                        order.venue,
                        FillLiquidity::Taker,
                        level.price * quantity,
                    ),
                    fee_currency: None,
                    filled_at: now,
                    leverage: order.leverage,
                    margin_mode: order.margin_mode,
                });
                order.filled_quantity += quantity;
            }
        }
        if let Some(check) = check.as_deref_mut() {
            check(order.id, &fills, order.filled_quantity == order.quantity)
                .map_err(|reason| ApexesError::OrderRejected { reason })?;
        }
        if let Some(book) = self.books.get_mut(&(order.venue, order.symbol.clone())) {
            for fill in &fills {
                book.consume(order.side, fill.price, fill.quantity);
            }
        }
        order.status = if order.filled_quantity == order.quantity {
            OrderStatus::Filled
        } else if order.filled_quantity > Decimal::ZERO {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::New
        };
        if order.status != OrderStatus::Filled {
            self.rest_limit_order(&order);
        }
        Ok((order, fills))
    }
}
