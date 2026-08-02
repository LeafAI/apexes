use crate::cost_model::{CostModel, FillLiquidity};
use crate::order_book::ReplayOrderBook;
use apexes_core::{
    ApexesError, DepthUpdate, Fill, FillId, Order, OrderId, OrderStatus, OrderType, PriceLevel,
    Side,
};
use apexes_strategy::OrderIntent;
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
/// market orders, TradeEvent-driven fills for resting limit orders with
/// an optional queue-position threshold (Task 10, design spec §7.3), and
/// Stop/StopLimit trigger simulation (Task 11).
///
/// Limit-order fills are driven by real `TradeEvent`s arriving via
/// `on_trade_event` (not by depth-diffing); `DepthUpdate`/order-book
/// state is used only to seed the INITIAL `volume_ahead_required`
/// threshold at submission time via `submit_limit_order`.
pub struct MatchingEngine {
    books: HashMap<String, ReplayOrderBook>,
    cost_model: CostModel,
    /// 0.0 (optimistic) .. 1.0 (pessimistic), see design spec §7.3.
    queue_position_factor: Decimal,
    /// Resting limit orders, keyed by symbol -> list of `RestingOrder`.
    resting_orders: HashMap<String, Vec<RestingOrder>>,
    /// Pending Stop/StopLimit orders awaiting their trigger price, keyed
    /// by symbol -> list of `PendingTrigger`.
    pending_triggers: HashMap<String, Vec<PendingTrigger>>,
}

struct PendingTrigger {
    order_id: OrderId,
    intent: OrderIntent,
    stop_price: Decimal,
}

/// The outcome of `MatchingEngine::check_pending_triggers`: fills from
/// triggered Stop orders (keyed by the ORIGINAL Stop order's `OrderId`,
/// so they flow through `on_trade_event`'s results exactly like
/// resting-order fills), plus triggered StopLimit orders awaiting
/// deferred conversion into resting Limit orders.
struct TriggerCheckResult {
    stop_fills: Vec<(OrderId, Vec<Fill>)>,
    stop_limit_conversions: Vec<(OrderId, OrderIntent)>,
}

impl TriggerCheckResult {
    fn empty() -> Self {
        Self {
            stop_fills: Vec::new(),
            stop_limit_conversions: Vec::new(),
        }
    }
}

struct RestingOrder {
    order_id: OrderId,
    order: Order,
    remaining_quantity: Decimal,
    /// Volume that must trade through (via subsequent `TradeEvent`s at
    /// this order's limit price or better) before this order is
    /// considered "our turn" to start filling. Computed once at
    /// submission time as `visible_volume_at_price_at_submission *
    /// queue_position_factor` (design spec §7.3); 0 if
    /// `queue_position_factor` is 0.0 (always eligible immediately).
    ///
    /// KNOWN LIMITATION: each resting order's `volume_ahead_required` is
    /// computed independently at submission time and does not account
    /// for OTHER resting orders on the same symbol/side. When multiple
    /// resting orders share a trade, this can allocate more total fill
    /// volume across them than the trade actually had available past the
    /// shared queue -- see task-10-review.md for a worked example.
    /// Acceptable for single-resting-order-per-symbol backtests; revisit
    /// with a shared-queue-cursor model if multi-order queue-position
    /// accuracy becomes a requirement.
    volume_ahead_required: Decimal,
    /// Cumulative volume from `TradeEvent`s at this order's limit price
    /// or better, observed since submission. Once this reaches
    /// `volume_ahead_required`, any FURTHER traded volume at/through our
    /// price is treated as filling our own order (see `on_trade_event`).
    volume_observed: Decimal,
}

impl MatchingEngine {
    pub fn new(cost_model: CostModel, queue_position_factor: Decimal) -> Self {
        debug_assert!(
            (Decimal::ZERO..=Decimal::ONE).contains(&queue_position_factor),
            "queue_position_factor should be in [0.0, 1.0], got {queue_position_factor}"
        );
        Self {
            books: HashMap::new(),
            cost_model,
            queue_position_factor,
            resting_orders: HashMap::new(),
            pending_triggers: HashMap::new(),
        }
    }

    /// Read-only access to the engine's `CostModel` -- used by
    /// `run_event_loop`'s periodic funding-settlement pass, which needs
    /// the same `funding_rate_bps` configuration the engine was built
    /// with (`CostModel` is not `Clone`, and the engine owns the only
    /// instance).
    pub fn cost_model(&self) -> &CostModel {
        &self.cost_model
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
        if intent.quantity <= Decimal::ZERO {
            return Err(ApexesError::OrderRejected {
                reason: format!(
                    "submit_market_order requires a positive quantity, got {}",
                    intent.quantity
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
                leverage: intent.leverage,
                margin_mode: intent.margin_mode,
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
            asset_class: intent.asset_class,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: None,
            stop_price: None,
            leverage: intent.leverage,
            margin_mode: intent.margin_mode,
            filled_quantity,
            status,
            created_at: now,
            updated_at: now,
        };

        Ok((order, fills))
    }

    /// Submits a Limit `OrderIntent`. The order rests in the simulated
    /// order queue at `intent.price` -- no immediate fill is attempted
    /// here. Fills happen later via `on_trade_event` as real trades occur
    /// at or through the order's limit price.
    pub fn submit_limit_order(
        &mut self,
        intent: &OrderIntent,
        now: DateTime<Utc>,
    ) -> Result<Order, ApexesError> {
        if intent.order_type != OrderType::Limit {
            return Err(ApexesError::OrderRejected {
                reason: format!(
                    "submit_limit_order called with non-Limit OrderType: {:?}",
                    intent.order_type
                ),
            });
        }
        if intent.quantity <= Decimal::ZERO {
            return Err(ApexesError::OrderRejected {
                reason: format!(
                    "submit_limit_order requires a positive quantity, got {}",
                    intent.quantity
                ),
            });
        }
        let limit_price = intent.price.ok_or_else(|| ApexesError::OrderRejected {
            reason: "Limit order requires a price".to_string(),
        })?;

        let order_id = OrderId(Uuid::new_v4());
        let order = Order {
            id: order_id,
            exchange_order_id: None,
            symbol: intent.symbol.clone(),
            venue: intent.venue,
            asset_class: intent.asset_class,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: Some(limit_price),
            stop_price: None,
            leverage: intent.leverage,
            margin_mode: intent.margin_mode,
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };

        // Determine how much visible volume is currently resting ahead of
        // us at (or better than) our limit price, on the SAME side of the
        // book we're joining (other resting orders we'd have to queue
        // behind), to seed queue_position_factor's threshold.
        let book = self.books.entry(intent.symbol.clone()).or_default();
        let levels = match intent.side {
            Side::Buy => book.bids(),  // joining the bid side
            Side::Sell => book.asks(), // joining the ask side
        };
        let visible_volume_at_price: Decimal = levels
            .iter()
            .filter(|level| match intent.side {
                Side::Buy => level.price >= limit_price,
                Side::Sell => level.price <= limit_price,
            })
            .map(|level| level.quantity)
            .sum();

        let resting = RestingOrder {
            order_id,
            order: order.clone(),
            remaining_quantity: intent.quantity,
            volume_ahead_required: visible_volume_at_price * self.queue_position_factor,
            volume_observed: Decimal::ZERO,
        };
        self.resting_orders
            .entry(intent.symbol.clone())
            .or_default()
            .push(resting);

        Ok(order)
    }

    /// Submits a Stop or StopLimit `OrderIntent`. The order does not rest
    /// in the order book -- it waits as a pending trigger until a
    /// `TradeEvent` at or through `intent.stop_price` is observed (via
    /// `on_trade_event`), at which point it converts to a Market order
    /// (`Stop`) or is submitted as a Limit order (`StopLimit`), per design
    /// spec §7.4.
    pub fn submit_stop_order(
        &mut self,
        intent: &OrderIntent,
        now: DateTime<Utc>,
    ) -> Result<Order, ApexesError> {
        if intent.order_type != OrderType::Stop && intent.order_type != OrderType::StopLimit {
            return Err(ApexesError::OrderRejected {
                reason: format!(
                    "submit_stop_order called with non-Stop/StopLimit OrderType: {:?}",
                    intent.order_type
                ),
            });
        }
        let stop_price = intent
            .stop_price
            .ok_or_else(|| ApexesError::OrderRejected {
                reason: "Stop/StopLimit order requires a stop_price".to_string(),
            })?;
        if intent.order_type == OrderType::StopLimit && intent.price.is_none() {
            return Err(ApexesError::OrderRejected {
                reason: "StopLimit order requires both stop_price and price".to_string(),
            });
        }
        if intent.quantity <= Decimal::ZERO {
            return Err(ApexesError::OrderRejected {
                reason: format!(
                    "submit_stop_order requires a positive quantity, got {}",
                    intent.quantity
                ),
            });
        }

        let order_id = OrderId(Uuid::new_v4());
        let order = Order {
            id: order_id,
            exchange_order_id: None,
            symbol: intent.symbol.clone(),
            venue: intent.venue,
            asset_class: intent.asset_class,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: intent.price,
            stop_price: Some(stop_price),
            leverage: intent.leverage,
            margin_mode: intent.margin_mode,
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };

        self.pending_triggers
            .entry(intent.symbol.clone())
            .or_default()
            .push(PendingTrigger {
                order_id,
                intent: intent.clone(),
                stop_price,
            });

        Ok(order)
    }

    /// Processes a real executed `TradeEvent`: triggers any pending
    /// Stop/StopLimit orders at `trade.symbol` whose stop price was
    /// crossed, then fills any eligible resting limit orders. A resting
    /// Buy limit order is triggered by trades at or below its limit
    /// price; a resting Sell limit order is triggered by trades at or
    /// above its limit price.
    /// Once `volume_ahead_required` (see `RestingOrder`) has been observed
    /// via cumulative trade volume, further trade volume at/through the
    /// order's price fills the order itself, up to `trade.quantity` or the
    /// order's `remaining_quantity`, whichever is smaller. Returns
    /// `(OrderId, Vec<Fill>)` for every order that received a fill from
    /// this trade -- including triggered Stop orders (whose Market-order
    /// fills are keyed by the ORIGINAL Stop order's ID) -- and fully
    /// filled resting orders are removed from `resting_orders`.
    pub fn on_trade_event(
        &mut self,
        trade: &apexes_core::TradeEvent,
        now: DateTime<Utc>,
    ) -> Vec<(OrderId, Vec<Fill>)> {
        // Check pending triggers FIRST: triggered Stop orders convert to
        // Market orders and fill immediately (their fills are returned
        // through `results` below, keyed by the ORIGINAL Stop order's ID);
        // triggered StopLimit orders are returned as Limit intents and
        // submitted AFTER this call's resting-order pass below, so the
        // trade that triggered a StopLimit can never also fill it in the
        // same call (the order was not yet resting when that trade
        // occurred).
        let trigger_result = self.check_pending_triggers(trade, now);

        let mut results = trigger_result.stop_fills;
        if let Some(orders) = self.resting_orders.get_mut(&trade.symbol)
            && !orders.is_empty()
        {
            results.extend(Self::fill_resting_orders(
                orders,
                trade,
                now,
                &self.cost_model,
            ));
        }

        for (original_order_id, limit_intent) in trigger_result.stop_limit_conversions {
            match self.submit_limit_order(&limit_intent, now) {
                Ok(_) => tracing::info!(
                    symbol = %trade.symbol,
                    order_id = ?original_order_id,
                    "StopLimit order triggered and converted to resting Limit order"
                ),
                Err(error) => tracing::warn!(
                    symbol = %trade.symbol,
                    order_id = ?original_order_id,
                    ?error,
                    "StopLimit order triggered but failed to convert to a resting Limit order"
                ),
            }
        }

        results
    }

    /// The resting-limit-order fill pass of `on_trade_event`, extracted so
    /// the early-return cases above don't skip the StopLimit conversion
    /// submissions. Operates directly on the symbol's resting-order list.
    fn fill_resting_orders(
        orders: &mut Vec<RestingOrder>,
        trade: &apexes_core::TradeEvent,
        now: DateTime<Utc>,
        cost_model: &CostModel,
    ) -> Vec<(OrderId, Vec<Fill>)> {
        let mut results = Vec::new();
        let mut still_resting = Vec::new();
        let mut remaining_trade_quantity = trade.quantity;

        for mut resting in orders.drain(..) {
            // A resting order always carries a limit price; skip it if
            // somehow absent rather than panicking.
            let Some(price) = resting.order.price else {
                still_resting.push(resting);
                continue;
            };
            let triggered = match resting.order.side {
                Side::Buy => trade.price <= price,
                Side::Sell => trade.price >= price,
            };

            if !triggered || remaining_trade_quantity.is_zero() {
                still_resting.push(resting);
                continue;
            }

            resting.volume_observed += remaining_trade_quantity.min(trade.quantity);

            if resting.volume_observed < resting.volume_ahead_required {
                // Still queued behind other volume; no fill yet, but we
                // keep tracking cumulative observed volume for next time.
                still_resting.push(resting);
                continue;
            }

            // Eligible: fill from the portion of this trade's volume that
            // exceeds our required queue-ahead threshold, capped by our
            // remaining order quantity and the trade's remaining quantity.
            let volume_past_threshold = resting.volume_observed - resting.volume_ahead_required;
            let fill_quantity = volume_past_threshold
                .min(resting.remaining_quantity)
                .min(remaining_trade_quantity);

            if fill_quantity.is_zero() {
                still_resting.push(resting);
                continue;
            }

            let notional = price * fill_quantity;
            let fee = cost_model.fee_for_fill(resting.order.venue, FillLiquidity::Maker, notional);
            let fill = Fill {
                id: FillId(Uuid::new_v4()),
                order_id: resting.order_id,
                symbol: resting.order.symbol.clone(),
                venue: resting.order.venue,
                side: resting.order.side,
                price,
                quantity: fill_quantity,
                fee,
                fee_currency: None,
                filled_at: now,
                leverage: resting.order.leverage,
                margin_mode: resting.order.margin_mode,
            };

            resting.remaining_quantity -= fill_quantity;
            resting.order.filled_quantity += fill_quantity;
            resting.order.updated_at = now;
            remaining_trade_quantity -= fill_quantity;

            if resting.remaining_quantity.is_zero() {
                resting.order.status = OrderStatus::Filled;
                results.push((resting.order_id, vec![fill]));
            } else {
                resting.order.status = OrderStatus::PartiallyFilled;
                results.push((resting.order_id, vec![fill]));
                still_resting.push(resting);
            }
        }

        *orders = still_resting;
        results
    }

    /// Checks every pending Stop/StopLimit trigger at `trade.symbol`
    /// against `trade.price`: a Buy stop triggers when price rises to or
    /// through `stop_price`; a Sell stop triggers when price falls to or
    /// through `stop_price`. Triggered orders are removed from
    /// `pending_triggers`. A triggered `Stop` is immediately submitted as
    /// a Market order (filled synchronously via `submit_market_order`'s
    /// book-walk logic against the CURRENT order book state) and its
    /// fills are returned as `(original OrderId, Vec<Fill>)` pairs so
    /// `on_trade_event` can merge them into its own results -- they flow
    /// through the exact same downstream mechanism as resting-order
    /// fills (ledger, equity curve, `BacktestReport.trades`). A triggered
    /// `StopLimit` is returned as an `(OrderId, Limit OrderIntent)` pair
    /// (the original order's ID alongside the converted intent) for the
    /// caller (`on_trade_event`) to submit via `submit_limit_order` AFTER the
    /// current call's resting-order pass -- the trade that triggered the
    /// StopLimit must not also fill it in the same call, because the
    /// order was not yet resting when that trade occurred. The converted
    /// Limit order is eligible for fills from the NEXT `on_trade_event`
    /// call onward, exactly like any other resting limit order.
    ///
    /// Returns a `TriggerCheckResult`: triggered-Stop
    /// fills keyed by the ORIGINAL `OrderId` `submit_stop_order` returned,
    /// and triggered-StopLimit conversions awaiting submission.
    fn check_pending_triggers(
        &mut self,
        trade: &apexes_core::TradeEvent,
        now: DateTime<Utc>,
    ) -> TriggerCheckResult {
        let Some(pending) = self.pending_triggers.get_mut(&trade.symbol) else {
            return TriggerCheckResult::empty();
        };
        if pending.is_empty() {
            return TriggerCheckResult::empty();
        }

        let mut still_pending = Vec::new();
        let mut triggered = Vec::new();

        for trigger in pending.drain(..) {
            let is_triggered = match trigger.intent.side {
                Side::Buy => trade.price >= trigger.stop_price,
                Side::Sell => trade.price <= trigger.stop_price,
            };
            if is_triggered {
                triggered.push(trigger);
            } else {
                still_pending.push(trigger);
            }
        }
        *pending = still_pending;

        let mut stop_fills = Vec::new();
        let mut stop_limit_conversions = Vec::new();
        for trigger in triggered {
            match trigger.intent.order_type {
                OrderType::Stop => {
                    let mut market_intent = trigger.intent.clone();
                    market_intent.order_type = OrderType::Market;
                    match self.submit_market_order(&market_intent, now) {
                        Ok((_order, fills)) if !fills.is_empty() => {
                            tracing::info!(
                                symbol = %trade.symbol,
                                order_id = ?trigger.order_id,
                                "Stop order triggered and converted to Market order"
                            );
                            // Key the fills by the ORIGINAL Stop order's ID
                            // (not the throwaway Market order's freshly-minted
                            // ID), so the runner's `open_orders` bookkeeping
                            // and `BacktestReport.trades` attribute them to
                            // the order the strategy actually placed.
                            stop_fills.push((trigger.order_id, fills));
                        }
                        Ok(_) => {
                            tracing::warn!(
                                symbol = %trade.symbol,
                                order_id = ?trigger.order_id,
                                "Stop order triggered but produced no fills (no visible liquidity)"
                            );
                        }
                        Err(error) => {
                            tracing::warn!(
                                symbol = %trade.symbol,
                                order_id = ?trigger.order_id,
                                ?error,
                                "Stop order triggered but failed to submit as a Market order"
                            );
                        }
                    }
                }
                OrderType::StopLimit => {
                    // submit_limit_order rejects non-Limit order types, so
                    // convert the intent to Limit (same pattern as the
                    // Stop -> Market arm above) and defer submission to
                    // the caller -- see this method's doc comment.
                    let mut limit_intent = trigger.intent.clone();
                    limit_intent.order_type = OrderType::Limit;
                    stop_limit_conversions.push((trigger.order_id, limit_intent));
                }
                _ => unreachable!(
                    "PendingTrigger only ever holds Stop/StopLimit intents, enforced by submit_stop_order"
                ),
            }
        }
        TriggerCheckResult {
            stop_fills,
            stop_limit_conversions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost_model::FlatFeeSchedule;
    use apexes_core::{AssetClass, Venue};
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
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity,
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
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
            asset_class: AssetClass::Spot,
            side: Side::Sell,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
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

    fn trade_event(symbol: &str, price: Decimal, quantity: Decimal) -> apexes_core::TradeEvent {
        apexes_core::TradeEvent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            price,
            quantity,
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn submit_limit_order_rests_without_immediate_fill() {
        let mut engine = no_fee_engine();
        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(dec!(64000)),
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };

        let order = engine.submit_limit_order(&intent, Utc::now()).unwrap();

        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.filled_quantity, dec!(0));
    }

    #[test]
    fn resting_limit_order_fills_immediately_when_optimistic_and_trade_triggers() {
        // queue_position_factor = 0.0 -> always eligible once triggered (empty book at submission means 0 required anyway).
        let mut engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(0),
                    taker_bps: dec!(0),
                }),
                funding_rate_bps: None,
            },
            dec!(0.0),
        );

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(dec!(65000)),
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };
        let order = engine.submit_limit_order(&intent, Utc::now()).unwrap();

        // A real trade executes at/below our limit price.
        let results =
            engine.on_trade_event(&trade_event("BTCUSDT", dec!(64900), dec!(2)), Utc::now());

        assert_eq!(results.len(), 1);
        let (order_id, fills) = &results[0];
        assert_eq!(*order_id, order.id);
        assert_eq!(fills[0].quantity, dec!(1));
        assert_eq!(fills[0].price, dec!(65000)); // fills at OUR limit price, not the trade's price
    }

    #[test]
    fn resting_limit_order_does_not_fill_when_trade_price_not_triggered() {
        let mut engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(0),
                    taker_bps: dec!(0),
                }),
                funding_rate_bps: None,
            },
            dec!(0.0),
        );

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(dec!(64000)), // limit below market
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };
        engine.submit_limit_order(&intent, Utc::now()).unwrap();

        // Trade executes above our limit price: not triggered.
        let results =
            engine.on_trade_event(&trade_event("BTCUSDT", dec!(65000), dec!(5)), Utc::now());

        assert!(results.is_empty());
    }

    #[test]
    fn pessimistic_queue_position_factor_requires_volume_to_trade_through_first() {
        // queue_position_factor = 1.0 -> must wait for ALL visible volume
        // ahead (at submission time) to trade through before our order fills.
        let mut engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(0),
                    taker_bps: dec!(0),
                }),
                funding_rate_bps: None,
            },
            dec!(1.0),
        );

        // Pre-existing bid volume at our intended limit price before we submit (we're joining the bid side).
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![PriceLevel {
                price: dec!(65000),
                quantity: dec!(10),
            }],
            vec![],
        ));

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(dec!(65000)),
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };
        engine.submit_limit_order(&intent, Utc::now()).unwrap();
        // volume_ahead_required = 10 * 1.0 = 10

        // A trade of only 5 at/below our price occurs -- not enough to clear the 10-unit threshold yet.
        let results =
            engine.on_trade_event(&trade_event("BTCUSDT", dec!(65000), dec!(5)), Utc::now());
        assert!(
            results.is_empty(),
            "should not fill until volume_ahead_required (10) is met"
        );

        // A second trade of 6 brings cumulative observed volume to 11, past the threshold of 10;
        // 1 unit past the threshold triggers our fill.
        let results =
            engine.on_trade_event(&trade_event("BTCUSDT", dec!(65000), dec!(6)), Utc::now());
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[0].quantity, dec!(1));
    }

    #[test]
    fn resting_order_partial_fill_keeps_remainder_resting() {
        let mut engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(0),
                    taker_bps: dec!(0),
                }),
                funding_rate_bps: None,
            },
            dec!(0.0),
        );

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(5),
            price: Some(dec!(65000)),
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };
        engine.submit_limit_order(&intent, Utc::now()).unwrap();

        // Only 2 units trade at/through our price -- partial fill.
        let results =
            engine.on_trade_event(&trade_event("BTCUSDT", dec!(64900), dec!(2)), Utc::now());

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[0].quantity, dec!(2));

        // A further trade of 1 unit fills more of the remainder.
        let results =
            engine.on_trade_event(&trade_event("BTCUSDT", dec!(64900), dec!(1)), Utc::now());
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[0].quantity, dec!(1));
    }

    #[test]
    fn maker_fee_rate_is_applied_to_resting_order_fills() {
        let mut engine = MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(5),
                    taker_bps: dec!(20),
                }),
                funding_rate_bps: None,
            },
            dec!(0.0),
        );

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(dec!(65000)),
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };
        engine.submit_limit_order(&intent, Utc::now()).unwrap();

        let results =
            engine.on_trade_event(&trade_event("BTCUSDT", dec!(65000), dec!(1)), Utc::now());

        // notional = 65000 * 1 = 65000, fee = 65000 * 0.05% (5 bps) = 32.5
        assert_eq!(results[0].1[0].fee, dec!(32.5));
    }

    #[test]
    fn trade_event_for_different_symbol_does_not_affect_resting_order() {
        let mut engine = no_fee_engine();
        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(dec!(65000)),
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };
        engine.submit_limit_order(&intent, Utc::now()).unwrap();

        let results =
            engine.on_trade_event(&trade_event("ETHUSDT", dec!(3000), dec!(10)), Utc::now());

        assert!(results.is_empty());
    }

    #[test]
    fn submit_limit_order_rejects_non_positive_quantity() {
        let mut engine = no_fee_engine();

        for quantity in [dec!(0), dec!(-1)] {
            let intent = OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Limit,
                quantity,
                price: Some(dec!(65000)),
                stop_price: None,
                execution_algo: None,
                routing: apexes_strategy::OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            };

            let result = engine.submit_limit_order(&intent, Utc::now());

            assert!(
                matches!(result, Err(ApexesError::OrderRejected { .. })),
                "quantity {quantity} should be rejected"
            );
        }
    }

    #[test]
    fn submit_market_order_rejects_non_positive_quantity() {
        let mut engine = no_fee_engine();

        for quantity in [dec!(0), dec!(-1)] {
            let intent = OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity,
                price: None,
                stop_price: None,
                execution_algo: None,
                routing: apexes_strategy::OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            };

            let result = engine.submit_market_order(&intent, Utc::now());

            assert!(
                matches!(result, Err(ApexesError::OrderRejected { .. })),
                "quantity {quantity} should be rejected"
            );
        }
    }

    #[test]
    fn submit_stop_order_rests_as_pending_trigger() {
        let mut engine = no_fee_engine();
        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Stop,
            quantity: dec!(1),
            price: None,
            stop_price: Some(dec!(66000)),
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };

        let order = engine.submit_stop_order(&intent, Utc::now()).unwrap();

        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.stop_price, Some(dec!(66000)));
    }

    #[test]
    fn submit_stop_order_rejects_stop_limit_without_price() {
        let mut engine = no_fee_engine();
        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::StopLimit,
            quantity: dec!(1),
            price: None, // missing -- should be rejected
            stop_price: Some(dec!(66000)),
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };

        let result = engine.submit_stop_order(&intent, Utc::now());

        assert!(matches!(result, Err(ApexesError::OrderRejected { .. })));
    }

    #[test]
    fn submit_stop_order_rejects_missing_stop_price() {
        let mut engine = no_fee_engine();
        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Stop,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };

        let result = engine.submit_stop_order(&intent, Utc::now());

        assert!(matches!(result, Err(ApexesError::OrderRejected { .. })));
    }

    #[test]
    fn submit_stop_order_rejects_non_stop_intent() {
        let mut engine = no_fee_engine();
        let intent = buy_intent("BTCUSDT", dec!(1)); // OrderType::Market

        let result = engine.submit_stop_order(&intent, Utc::now());

        assert!(matches!(result, Err(ApexesError::OrderRejected { .. })));
    }

    #[test]
    fn submit_stop_order_rejects_non_positive_quantity() {
        let mut engine = no_fee_engine();

        for quantity in [dec!(0), dec!(-1)] {
            let intent = OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Stop,
                quantity,
                price: None,
                stop_price: Some(dec!(66000)),
                execution_algo: None,
                routing: apexes_strategy::OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            };

            let result = engine.submit_stop_order(&intent, Utc::now());

            assert!(
                matches!(result, Err(ApexesError::OrderRejected { .. })),
                "quantity {quantity} should be rejected"
            );
        }
    }

    #[test]
    fn buy_stop_triggers_and_fills_as_market_order_when_price_rises_through_stop() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![],
            vec![PriceLevel {
                price: dec!(66100),
                quantity: dec!(5),
            }],
        ));

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Stop,
            quantity: dec!(1),
            price: None,
            stop_price: Some(dec!(66000)),
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };
        let order = engine.submit_stop_order(&intent, Utc::now()).unwrap();

        // A trade at/above the stop price triggers it; the resulting
        // Market-order fill MUST be returned through on_trade_event's
        // results (keyed by the ORIGINAL Stop order's ID) so the runner's
        // ledger/equity/trades bookkeeping actually observes it --
        // regression guard for the final-review finding that these fills
        // were previously computed and then silently discarded.
        let results =
            engine.on_trade_event(&trade_event("BTCUSDT", dec!(66050), dec!(1)), Utc::now());

        assert_eq!(results.len(), 1);
        let (result_order_id, fills) = &results[0];
        assert_eq!(*result_order_id, order.id);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].quantity, dec!(1));
        assert_eq!(fills[0].price, dec!(66100)); // walked the ask level
        assert_eq!(fills[0].side, Side::Buy);

        // The ask level's 5 quantity should also have been consumed by
        // the triggered Stop's market fill (reduced by 1, to 4).
        let (_order, fills) = engine
            .submit_market_order(
                &OrderIntent {
                    symbol: "BTCUSDT".to_string(),
                    venue: Venue::Binance,
                    asset_class: AssetClass::Spot,
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    quantity: dec!(4),
                    price: None,
                    stop_price: None,
                    execution_algo: None,
                    routing: apexes_strategy::OrderRouting::Fixed,
                    leverage: None,
                    margin_mode: None,
                },
                Utc::now(),
            )
            .unwrap();
        assert_eq!(fills[0].quantity, dec!(4)); // exactly the remaining depth after the triggered Stop consumed 1
    }

    #[test]
    fn buy_stop_does_not_trigger_when_price_stays_below_stop() {
        let mut engine = no_fee_engine();
        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Stop,
            quantity: dec!(1),
            price: None,
            stop_price: Some(dec!(66000)),
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };
        engine.submit_stop_order(&intent, Utc::now()).unwrap();

        // Trade stays below the stop price -- not triggered.
        engine.on_trade_event(&trade_event("BTCUSDT", dec!(65000), dec!(1)), Utc::now());

        // No liquidity was consumed by a phantom fill: submitting a fresh
        // market order against an untouched (empty) book should find zero fills.
        let (_order, fills) = engine
            .submit_market_order(
                &OrderIntent {
                    symbol: "BTCUSDT".to_string(),
                    venue: Venue::Binance,
                    asset_class: AssetClass::Spot,
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    quantity: dec!(1),
                    price: None,
                    stop_price: None,
                    execution_algo: None,
                    routing: apexes_strategy::OrderRouting::Fixed,
                    leverage: None,
                    margin_mode: None,
                },
                Utc::now(),
            )
            .unwrap();
        assert!(fills.is_empty());
    }

    #[test]
    fn sell_stop_triggers_when_price_falls_through_stop() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![PriceLevel {
                price: dec!(63900),
                quantity: dec!(5),
            }],
            vec![],
        ));

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Sell,
            order_type: OrderType::Stop,
            quantity: dec!(1),
            price: None,
            stop_price: Some(dec!(64000)),
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };
        let order = engine.submit_stop_order(&intent, Utc::now()).unwrap();

        // A trade at/below the stop price triggers a sell stop; the fill
        // flows back through on_trade_event's results (same regression
        // guard as the Buy-stop test above).
        let results =
            engine.on_trade_event(&trade_event("BTCUSDT", dec!(63950), dec!(1)), Utc::now());

        assert_eq!(results.len(), 1);
        let (result_order_id, fills) = &results[0];
        assert_eq!(*result_order_id, order.id);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].quantity, dec!(1));
        assert_eq!(fills[0].price, dec!(63900)); // walked the bid level
        assert_eq!(fills[0].side, Side::Sell);

        let (_order, fills) = engine
            .submit_market_order(
                &OrderIntent {
                    symbol: "BTCUSDT".to_string(),
                    venue: Venue::Binance,
                    asset_class: AssetClass::Spot,
                    side: Side::Sell,
                    order_type: OrderType::Market,
                    quantity: dec!(4),
                    price: None,
                    stop_price: None,
                    execution_algo: None,
                    routing: apexes_strategy::OrderRouting::Fixed,
                    leverage: None,
                    margin_mode: None,
                },
                Utc::now(),
            )
            .unwrap();
        assert_eq!(fills[0].quantity, dec!(4));
    }

    #[test]
    fn stop_limit_triggers_and_rests_as_limit_order_not_immediate_fill() {
        let mut engine = no_fee_engine();
        engine.on_depth_update(&snapshot(
            "BTCUSDT",
            vec![],
            vec![PriceLevel {
                price: dec!(66100),
                quantity: dec!(5),
            }],
        ));

        let intent = OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::StopLimit,
            quantity: dec!(1),
            price: Some(dec!(66050)), // limit price once triggered
            stop_price: Some(dec!(66000)),
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        };
        engine.submit_stop_order(&intent, Utc::now()).unwrap();

        // Trigger: a trade at/above the stop price, but BELOW the StopLimit's
        // own limit price (66020 < 66050), so the newly-resting Buy limit
        // order (which fills on trades AT/BELOW its price) is not yet
        // triggered by this same trade.
        let results =
            engine.on_trade_event(&trade_event("BTCUSDT", dec!(66020), dec!(1)), Utc::now());
        assert!(
            results.is_empty(),
            "StopLimit becomes a resting Limit order on trigger, not an immediate fill"
        );

        // A subsequent trade at/below the limit price fills the now-resting Limit order.
        let results =
            engine.on_trade_event(&trade_event("BTCUSDT", dec!(66040), dec!(1)), Utc::now());
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[0].price, dec!(66050)); // fills at the StopLimit's own limit price
    }
}
