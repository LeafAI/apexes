//! F03.02 direct marketable limits: take causal opposite-side liquidity,
//! then enqueue only the unfilled GTC remainder.
use super::*;

impl MatchingEngine {
    pub fn submit_executable_limit_order(
        &mut self,
        intent: &OrderIntent,
        now: DateTime<Utc>,
    ) -> Result<(Order, Vec<Fill>), ApexesError> {
        let mut order = self.prepare_limit_order(intent, now)?;
        let limit = intent.price.ok_or_else(|| ApexesError::OrderRejected {
            reason: "Limit order requires a price".into(),
        })?;
        let mut fills = Vec::new();
        if let Some(book) = self.books.get_mut(&(intent.venue, intent.symbol.clone()))
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
                    id: FillId(Uuid::new_v4()),
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
                book.consume(order.side, level.price, quantity);
                order.filled_quantity += quantity;
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
