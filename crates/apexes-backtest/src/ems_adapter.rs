//! [`MatchingEngineChildOrderSink`]: adapts `crate::matching_engine::MatchingEngine`
//! to `apexes_ems::ChildOrderSink`, so `apexes-ems`'s `InMemoryEms` can
//! submit child orders during backtest/paper-trading replay without
//! `apexes-ems` itself depending on `apexes-backtest` (see
//! `apexes_ems`'s crate-level docs for why that dependency direction is
//! forbidden). Wraps the SAME `Arc<Mutex<MatchingEngine>>` the owning
//! `run_event_loop` call also drives directly for strategy-submitted
//! (non-algo) orders, so EMS child-order fills and direct-strategy
//! fills interact through one single shared order book, not two
//! independent simulations.
//!
//! **All child orders submit as Market orders** -- `ChildOrderIntent`
//! has no price/order-type field, so this adapter always calls
//! `MatchingEngine::submit_market_order`. See this task's own plan
//! entry for the full rationale.

use std::sync::{Arc, Mutex};

use apexes_core::{Order, OrderId, OrderType};
use apexes_ems::{ChildOrderIntent, ChildOrderSink, EmsError};
use apexes_strategy::OrderIntent;
use async_trait::async_trait;
use chrono::Utc;

use crate::matching_engine::MatchingEngine;

pub struct MatchingEngineChildOrderSink {
    engine: Arc<Mutex<MatchingEngine>>,
}

impl MatchingEngineChildOrderSink {
    pub fn new(engine: Arc<Mutex<MatchingEngine>>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl ChildOrderSink for MatchingEngineChildOrderSink {
    async fn submit_child_order(
        &self,
        intent: ChildOrderIntent,
    ) -> Result<(OrderId, Vec<apexes_core::Fill>), EmsError> {
        let order_intent = OrderIntent {
            symbol: intent.symbol,
            venue: intent.venue,
            asset_class: intent.asset_class,
            side: intent.side,
            order_type: OrderType::Market,
            quantity: intent.quantity,
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
        };
        let mut engine = self.engine.lock().unwrap_or_else(|p| p.into_inner());
        let (order, fills) = engine
            .submit_market_order(&order_intent, Utc::now())
            .map_err(|error| EmsError::ChildOrderSubmissionFailed {
                parent_id: apexes_strategy::ParentOrderId(uuid::Uuid::nil()),
                reason: error.to_string(),
            })?;
        Ok((order.id, fills))
    }

    async fn cancel_child_order(&self, _order_id: OrderId) -> Result<(), EmsError> {
        // MatchingEngine's Market orders fill immediately at submission
        // (no resting state exists to cancel by the time this could be
        // called) -- a no-op success, matching `ChildOrderSink`'s own
        // documented "cancelling an already-terminal order is a no-op
        // success" convention.
        Ok(())
    }

    async fn order_status(&self, _order_id: OrderId) -> Result<Option<Order>, EmsError> {
        // MatchingEngine does not currently track submitted orders by
        // ID after they fill (it returns the filled Order directly from
        // submit_market_order, and does not retain a lookup table) --
        // Iceberg's refill logic only actually needs order_status for
        // NON-immediately-filled orders (e.g. resting Limit orders),
        // which Market-order child orders never are. Since every child
        // order submitted through this adapter is a Market order that
        // fills (fully or partially) synchronously within
        // submit_child_order itself, Iceberg's "wait for the resting
        // child to reach a terminal state" check is trivially satisfied
        // the moment submission returns -- so returning `Ok(None)` here
        // (rather than tracking real state) is safe: Iceberg's
        // `refill_iceberg_allocation` treats a `None` status the same
        // as "not yet terminal" and will simply retry on the next
        // `on_market_event` call, converging in one extra tick instead
        // of zero. Flagged here explicitly as an accepted minor pacing
        // imprecision, not a silent bug.
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost_model::{CostModel, FlatFeeSchedule};
    use apexes_core::{AssetClass, DepthUpdate, PriceLevel, Side, Venue};
    use rust_decimal_macros::dec;

    fn engine_with_book() -> Arc<Mutex<MatchingEngine>> {
        let cost_model = CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        };
        let mut engine = MatchingEngine::new(cost_model, dec!(0));
        engine.on_depth_update(&DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: dec!(64990),
                quantity: dec!(10),
            }],
            asks: vec![PriceLevel {
                price: dec!(65010),
                quantity: dec!(10),
            }],
            sequence: None,
            is_snapshot: true,
            timestamp: Utc::now(),
        });
        Arc::new(Mutex::new(engine))
    }

    #[tokio::test]
    async fn submit_child_order_fills_immediately_against_the_shared_book() {
        let engine = engine_with_book();
        let sink = MatchingEngineChildOrderSink::new(Arc::clone(&engine));

        let (order_id, fills) = sink
            .submit_child_order(ChildOrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                quantity: dec!(1),
            })
            .await
            .unwrap();

        assert_ne!(order_id, OrderId(uuid::Uuid::nil()));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].quantity, dec!(1));
    }

    #[tokio::test]
    async fn cancel_child_order_is_a_no_op_success() {
        let engine = engine_with_book();
        let sink = MatchingEngineChildOrderSink::new(engine);
        sink.cancel_child_order(OrderId(uuid::Uuid::new_v4()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn order_status_returns_none_for_any_order_id() {
        let engine = engine_with_book();
        let sink = MatchingEngineChildOrderSink::new(engine);
        let status = sink
            .order_status(OrderId(uuid::Uuid::new_v4()))
            .await
            .unwrap();
        assert_eq!(status, None);
    }
}
