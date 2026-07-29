use crate::cost_model::{CostModel, FillLiquidity};
use crate::order_book::ReplayOrderBook;
use crate::strategy::OrderIntent;
use apexes_core::{
    ApexesError, DepthUpdate, Fill, FillId, Order, OrderId, OrderStatus, OrderType, PriceLevel,
    Side,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use uuid::Uuid;

/// Result of walking a sequence of price levels to fill `quantity`.
#[derive(Debug, Clone, PartialEq)]
pub struct BookWalkResult {
    /// Quantity actually filled (may be less than requested if the levels
    /// don't have enough total depth).
    pub filled_quantity: Decimal,
    /// Volume-weighted average price across all consumed levels.
    pub avg_price: Decimal,
    /// Total notional value (`filled_quantity * avg_price`, computed
    /// directly as the sum of each level's `price * consumed_quantity`
    /// for precision).
    pub notional: Decimal,
}

/// Walks `levels` (assumed pre-sorted best-price-first, as returned by
/// `ReplayOrderBook::bids`/`asks`) from best to worst, consuming quantity
/// from each level until `quantity` is filled or the levels are
/// exhausted. Produces a single volume-weighted average fill price,
/// naturally modeling market-impact/slippage for orders larger than the
/// top-of-book depth (design spec §7.2).
///
/// Returns `None` if `levels` is empty or `quantity` is not positive (no
/// fill possible); otherwise returns `Some(BookWalkResult)` even if the
/// full requested `quantity` couldn't be filled (see
/// `BookWalkResult::filled_quantity` -- a partial fill due to insufficient
/// visible liquidity is not an error, per design spec §7.2/§9).
pub fn walk_book(levels: &[PriceLevel], quantity: Decimal) -> Option<BookWalkResult> {
    if levels.is_empty() || quantity <= Decimal::ZERO {
        return None;
    }

    let mut remaining = quantity;
    let mut total_notional = Decimal::ZERO;
    let mut total_filled = Decimal::ZERO;

    for level in levels {
        if remaining <= Decimal::ZERO {
            break;
        }
        let consumed = remaining.min(level.quantity);
        total_notional += consumed * level.price;
        total_filled += consumed;
        remaining -= consumed;
    }

    if total_filled.is_zero() {
        return None;
    }

    Some(BookWalkResult {
        filled_quantity: total_filled,
        avg_price: total_notional / total_filled,
        notional: total_notional,
    })
}

/// Simulates order matching against locally-reconstructed Level-2 depth:
/// order book reconstruction (via `ReplayOrderBook`), book-walk fills for
/// market orders, deterministic depth-consumption fills for limit orders
/// with an optional probabilistic queue-position adjustment (Task 10), and
/// Stop/StopLimit trigger simulation (Task 11).
pub struct MatchingEngine {
    books: HashMap<String, ReplayOrderBook>,
    cost_model: CostModel,
    /// 0.0 (optimistic) .. 1.0 (pessimistic), see design spec §7.3. Used by
    /// Task 10's limit-order fill logic.
    #[allow(dead_code)]
    queue_position_factor: Decimal,
}

impl MatchingEngine {
    pub fn new(cost_model: CostModel, queue_position_factor: Decimal) -> Self {
        Self {
            books: HashMap::new(),
            cost_model,
            queue_position_factor,
        }
    }

    /// Updates the local order book for `update.symbol` from a new
    /// `DepthUpdate`. Must be called for every `MarketEvent::Depth` in
    /// chronological order before any fills against that symbol are
    /// computed.
    pub fn on_depth_update(&mut self, update: &DepthUpdate) {
        self.books
            .entry(update.symbol.clone())
            .or_default()
            .apply(update);
    }

    /// Submits a Market `OrderIntent`, filling it immediately by walking
    /// the current local order book from best price outward. If total
    /// visible depth is insufficient to fill the full requested quantity,
    /// fills whatever is available and returns a `PartiallyFilled` (or
    /// `New` if nothing could be filled) `Order` -- this is NOT an error,
    /// per design spec §7.2 (logged via `tracing::warn`).
    pub fn submit_market_order(
        &mut self,
        intent: &OrderIntent,
        now: DateTime<Utc>,
    ) -> Result<(Order, Vec<Fill>), ApexesError> {
        if intent.order_type != OrderType::Market {
            return Err(ApexesError::OrderRejected {
                reason: format!(
                    "submit_market_order called with non-Market OrderType: {:?}",
                    intent.order_type
                ),
            });
        }

        let book = self.books.entry(intent.symbol.clone()).or_default();
        let levels = match intent.side {
            // Buying consumes the ask side (lowest price first).
            Side::Buy => book.asks(),
            // Selling consumes the bid side (highest price first).
            Side::Sell => book.bids(),
        };

        let order_id = OrderId(Uuid::new_v4());
        let mut remaining = intent.quantity;
        let mut fills = Vec::new();

        for level in &levels {
            if remaining.is_zero() {
                break;
            }
            let fill_quantity = remaining.min(level.quantity);
            let notional = level.price * fill_quantity;
            let fee = self
                .cost_model
                .fee_for_fill(intent.venue, FillLiquidity::Taker, notional);

            fills.push(Fill {
                id: FillId(Uuid::new_v4()),
                order_id,
                symbol: intent.symbol.clone(),
                venue: intent.venue,
                side: intent.side,
                price: level.price,
                quantity: fill_quantity,
                fee,
                fee_currency: None,
                filled_at: now,
            });

            remaining -= fill_quantity;
        }

        if remaining > Decimal::ZERO && remaining < intent.quantity {
            tracing::warn!(
                symbol = %intent.symbol,
                requested = %intent.quantity,
                filled = %(intent.quantity - remaining),
                "submit_market_order: insufficient visible liquidity, partial fill only"
            );
        } else if remaining == intent.quantity {
            tracing::warn!(
                symbol = %intent.symbol,
                "submit_market_order: no visible liquidity at all, order unfilled"
            );
        }

        let filled_quantity = intent.quantity - remaining;
        let status = if filled_quantity.is_zero() {
            OrderStatus::New
        } else if remaining.is_zero() {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };

        let order = Order {
            id: order_id,
            exchange_order_id: None,
            symbol: intent.symbol.clone(),
            venue: intent.venue,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: None,
            stop_price: None,
            filled_quantity,
            status,
            created_at: now,
            updated_at: now,
        };

        Ok((order, fills))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost_model::FlatFeeSchedule;
    use apexes_core::Venue;
    use rust_decimal_macros::dec;

    fn level(price: Decimal, quantity: Decimal) -> PriceLevel {
        PriceLevel { price, quantity }
    }

    #[test]
    fn small_order_fills_entirely_at_top_of_book() {
        let levels = vec![level(dec!(100), dec!(5)), level(dec!(101), dec!(5))];
        let result = walk_book(&levels, dec!(2)).unwrap();

        assert_eq!(result.filled_quantity, dec!(2));
        assert_eq!(result.avg_price, dec!(100));
        assert_eq!(result.notional, dec!(200));
    }

    #[test]
    fn large_order_walks_multiple_levels_and_computes_vwap() {
        let levels = vec![level(dec!(100), dec!(2)), level(dec!(101), dec!(3))];
        let result = walk_book(&levels, dec!(4)).unwrap();

        // 2 @ 100 + 2 @ 101 = 200 + 202 = 402, / 4 = 100.5
        assert_eq!(result.filled_quantity, dec!(4));
        assert_eq!(result.notional, dec!(402));
        assert_eq!(result.avg_price, dec!(100.5));
    }

    #[test]
    fn order_larger_than_total_depth_produces_partial_fill() {
        let levels = vec![level(dec!(100), dec!(2)), level(dec!(101), dec!(3))];
        let result = walk_book(&levels, dec!(10)).unwrap();

        // Total depth is only 5.
        assert_eq!(result.filled_quantity, dec!(5));
        assert_eq!(result.notional, dec!(200) + dec!(303)); // 2*100 + 3*101
    }

    #[test]
    fn empty_levels_returns_none() {
        let result = walk_book(&[], dec!(1));
        assert!(result.is_none());
    }

    #[test]
    fn zero_quantity_returns_none() {
        let levels = vec![level(dec!(100), dec!(5))];
        let result = walk_book(&levels, dec!(0));
        assert!(result.is_none());
    }

    #[test]
    fn negative_quantity_returns_none() {
        let levels = vec![level(dec!(100), dec!(5))];
        let result = walk_book(&levels, dec!(-1));
        assert!(result.is_none());
    }

    fn no_fee_engine() -> MatchingEngine {
        MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(0),
                    taker_bps: dec!(0),
                }),
                funding_rate_bps: None,
            },
            dec!(0.5),
        )
    }

    fn snapshot(symbol: &str, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) -> DepthUpdate {
        DepthUpdate {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            bids,
            asks,
            sequence: Some(1),
            is_snapshot: true,
            timestamp: Utc::now(),
        }
    }

    fn buy_intent(symbol: &str, quantity: Decimal) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity,
            price: None,
            stop_price: None,
        }
    }

    #[test]
    fn market_buy_fills_entirely_at_single_level() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![],
            vec![PriceLevel {
                price: dec!(65000),
                quantity: dec!(5),
            }],
        ));

        let (order, fills) = engine
            .submit_market_order(&buy_intent("BTCUSDT", dec!(2)), Utc::now())
            .unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, dec!(65000));
        assert_eq!(fills[0].quantity, dec!(2));
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.filled_quantity, dec!(2));
    }

    #[test]
    fn market_buy_walks_multiple_levels_for_vwap() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![],
            vec![
                PriceLevel {
                    price: dec!(65000),
                    quantity: dec!(1),
                },
                PriceLevel {
                    price: dec!(65010),
                    quantity: dec!(1),
                },
            ],
        ));

        let (order, fills) = engine
            .submit_market_order(&buy_intent("BTCUSDT", dec!(1.5)), Utc::now())
            .unwrap();

        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].price, dec!(65000));
        assert_eq!(fills[0].quantity, dec!(1));
        assert_eq!(fills[1].price, dec!(65010));
        assert_eq!(fills[1].quantity, dec!(0.5));
        assert_eq!(order.status, OrderStatus::Filled);
    }

    #[test]
    fn market_buy_partially_fills_when_depth_insufficient() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![],
            vec![PriceLevel {
                price: dec!(65000),
                quantity: dec!(1),
            }],
        ));

        let (order, fills) = engine
            .submit_market_order(&buy_intent("BTCUSDT", dec!(5)), Utc::now())
            .unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].quantity, dec!(1));
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.filled_quantity, dec!(1));
    }

    #[test]
    fn market_buy_with_no_liquidity_returns_unfilled_new_order() {
        let mut engine = no_fee_engine();
        // No depth update at all -- empty book.
        let (order, fills) = engine
            .submit_market_order(&buy_intent("BTCUSDT", dec!(1)), Utc::now())
            .unwrap();

        assert!(fills.is_empty());
        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.filled_quantity, dec!(0));
    }

    #[test]
    fn market_sell_consumes_bid_side() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![PriceLevel {
                price: dec!(64990),
                quantity: dec!(3),
            }],
            vec![],
        ));

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Sell,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
        };
        let (order, fills) = engine.submit_market_order(&intent, Utc::now()).unwrap();

        assert_eq!(fills[0].price, dec!(64990));
        assert_eq!(order.status, OrderStatus::Filled);
    }

    #[test]
    fn submit_market_order_rejects_non_market_intent() {
        let mut engine = no_fee_engine();
        let mut intent = buy_intent("BTCUSDT", dec!(1));
        intent.order_type = OrderType::Limit;
        intent.price = Some(dec!(65000));

        let result = engine.submit_market_order(&intent, Utc::now());

        assert!(matches!(result, Err(ApexesError::OrderRejected { .. })));
    }

    #[test]
    fn fees_are_applied_using_taker_rate() {
        let mut engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(0),
                    taker_bps: dec!(10), // 0.10%
                }),
                funding_rate_bps: None,
            },
            dec!(0.5),
        );
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![],
            vec![PriceLevel {
                price: dec!(65000),
                quantity: dec!(1),
            }],
        ));

        let (_order, fills) = engine
            .submit_market_order(&buy_intent("BTCUSDT", dec!(1)), Utc::now())
            .unwrap();

        // notional = 65000, fee = 65000 * 0.10% = 65
        assert_eq!(fills[0].fee, dec!(65));
    }
}
