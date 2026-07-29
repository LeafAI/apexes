use apexes_core::PriceLevel;
use rust_decimal::Decimal;

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

#[cfg(test)]
mod tests {
    use super::*;
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
}
