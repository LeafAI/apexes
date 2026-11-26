//! [`MatchingEngineChildOrderSink`]: adapts `crate::matching_engine::MatchingEngine`
//! to `apexes_ems::ChildOrderSink`, so `apexes-ems`'s `InMemoryEms` can
//! submit child orders during backtest/paper-trading replay without
//! `apexes-ems` itself depending on `apexes-backtest` (see
//! `apexes_ems`'s crate-level docs for why that dependency direction is
//! forbidden). Wraps the SAME `Arc<Mutex<MatchingEngine>>` the owning
//! `run_unified_event_loop` call also drives directly for strategy-submitted
//! (non-algo) orders, so EMS child-order fills and direct-strategy
//! fills interact through one single shared order book, not two
//! independent simulations.
//!
//! **All child orders submit as Market orders** -- `ChildOrderIntent`
//! has no price/order-type field, so this adapter always calls
//! `MatchingEngine::submit_market_order`. See this task's own plan
//! entry for the full rationale.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use apexes_core::{Order, OrderId, OrderType};
use apexes_ems::{ChildOrderIntent, ChildOrderSink, EmsError};
use apexes_strategy::OrderIntent;
use async_trait::async_trait;
use chrono::Utc;

use crate::matching_engine::MatchingEngine;

pub struct MatchingEngineChildOrderSink {
    engine: Arc<Mutex<MatchingEngine>>,
    /// Every order this sink has ever submitted, keyed by `OrderId`,
    /// recording the terminal (or partially-filled) `Order` snapshot
    /// `submit_market_order` returned at submission time. `MatchingEngine`
    /// itself does not retain a lookup table for orders after they fill
    /// (see `order_status`'s doc comment for why a lookup table is
    /// needed HERE instead), so this adapter keeps its own -- required
    /// for `Ems`'s Iceberg scheduling, whose `refill_iceberg_allocation`
    /// polls `order_status` to detect when a resting child has reached a
    /// terminal state before submitting the next visible slice.
    submitted: Mutex<HashMap<OrderId, Order>>,
}

impl MatchingEngineChildOrderSink {
    pub fn new(engine: Arc<Mutex<MatchingEngine>>) -> Self {
        Self {
            engine,
            submitted: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl ChildOrderSink for MatchingEngineChildOrderSink {
    async fn reserve_parent(
        &self,
        _id: apexes_strategy::ParentOrderId,
        request: &apexes_ems::AlgoOrderRequest,
    ) -> Result<(), EmsError> {
        let engine = self.engine.lock().unwrap_or_else(|p| p.into_inner());
        let intent = OrderIntent {
            symbol: request.symbol.clone(),
            venue: request.venue,
            asset_class: apexes_core::AssetClass::Spot,
            side: request.side,
            order_type: OrderType::Market,
            quantity: request.quantity,
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: request.routing.clone(),
            leverage: request.leverage,
            margin_mode: request.margin_mode,
        };
        engine
            .execution_policy()
            .validate_intent(&intent)
            .map_err(|e| EmsError::ParentAdmissionRejected {
                reason: e.to_string(),
            })
    }
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
            leverage: intent.leverage,
            margin_mode: intent.margin_mode,
        };
        let mut engine = self.engine.lock().unwrap_or_else(|p| p.into_inner());
        let now = engine.event_time().unwrap_or_else(Utc::now);
        let (order, fills) = engine
            .submit_market_child(&order_intent, now)
            .map_err(|error| EmsError::ChildOrderSubmissionFailed {
                parent_id: apexes_strategy::ParentOrderId(uuid::Uuid::nil()),
                reason: error.to_string(),
            })?;

        // The matching engine already returns the truthful terminal
        // status and actual executed quantity for immediate orders.
        self.submitted
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(order.id, order.clone());

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

    async fn order_status(&self, order_id: OrderId) -> Result<Option<Order>, EmsError> {
        // Every order submitted through this adapter is a Market order
        // that fills (fully or partially) synchronously within
        // submit_child_order itself -- there is no resting-order concept
        // here, so by the time this method could ever be called, the
        // order this adapter recorded is already permanently terminal.
        // Returns that recorded snapshot (with `status` normalized to
        // `Filled`/`Rejected` at submission time, see
        // `submit_child_order`'s doc comment) so Iceberg's
        // `refill_iceberg_allocation` correctly detects completion on
        // its very next poll instead of waiting forever.
        Ok(self
            .submitted
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&order_id)
            .cloned())
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
                leverage: None,
                margin_mode: None,
            })
            .await
            .unwrap();

        assert_ne!(order_id, OrderId(uuid::Uuid::nil()));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].quantity, dec!(1));
    }

    #[tokio::test]
    async fn child_shares_consumed_depth_and_uses_replay_time() {
        use chrono::TimeZone;
        let engine = engine_with_book();
        let at = Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap();
        let child = ChildOrderIntent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            quantity: dec!(8),
            leverage: None,
            margin_mode: None,
        };
        {
            let mut shared = engine.lock().unwrap();
            shared.on_depth_update(&DepthUpdate {
                symbol: child.symbol.clone(),
                venue: child.venue,
                bids: vec![],
                asks: vec![PriceLevel {
                    price: dec!(100),
                    quantity: dec!(10),
                }],
                sequence: Some(2),
                is_snapshot: true,
                timestamp: at,
            });
            let direct = OrderIntent {
                symbol: child.symbol.clone(),
                venue: child.venue,
                asset_class: child.asset_class,
                side: child.side,
                order_type: OrderType::Market,
                quantity: dec!(6),
                price: None,
                stop_price: None,
                execution_algo: None,
                routing: apexes_strategy::OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            };
            assert_eq!(
                shared
                    .submit_market_order(&direct, at)
                    .unwrap()
                    .0
                    .filled_quantity,
                dec!(6)
            );
        }
        let sink = MatchingEngineChildOrderSink::new(engine);
        let (id, fills) = sink.submit_child_order(child).await.unwrap();
        assert_eq!(fills[0].filled_at, at);
        assert_eq!(fills[0].quantity, dec!(4));
        let order = sink.order_status(id).await.unwrap().unwrap();
        assert_eq!(order.status, apexes_core::OrderStatus::Cancelled);
        assert_eq!(order.filled_quantity, dec!(4));
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
    async fn order_status_returns_none_for_an_unknown_order_id() {
        let engine = engine_with_book();
        let sink = MatchingEngineChildOrderSink::new(engine);
        let status = sink
            .order_status(OrderId(uuid::Uuid::new_v4()))
            .await
            .unwrap();
        assert_eq!(status, None);
    }

    #[tokio::test]
    async fn order_status_reports_filled_for_a_submitted_order_with_liquidity() {
        // Regression test for a real Critical bug caught in sub-project
        // #9's Task 18 whole-branch review: this method used to always
        // return `Ok(None)` regardless of the order_id, which made
        // `InMemoryEms`'s Iceberg scheduling (`refill_iceberg_allocation`,
        // which treats `None` identically to "still resting, not yet
        // terminal") wait forever for a resting child that, for this
        // Market-only sink, never actually rests -- permanently
        // stalling every real Iceberg parent order after its first
        // slice in backtest/paper-trading.
        let engine = engine_with_book();
        let sink = MatchingEngineChildOrderSink::new(Arc::clone(&engine));

        let (order_id, fills) = sink
            .submit_child_order(ChildOrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                quantity: dec!(1),
                leverage: None,
                margin_mode: None,
            })
            .await
            .unwrap();
        assert_eq!(fills.len(), 1, "sufficient book depth must fully fill");

        let status = sink.order_status(order_id).await.unwrap();
        assert_eq!(
            status.map(|order| order.status),
            Some(apexes_core::OrderStatus::Filled)
        );
    }

    #[tokio::test]
    async fn order_status_reports_expired_for_a_submitted_order_with_no_liquidity() {
        // Same bug as above, exercised via the "no book depth at all"
        // path (`filled_quantity` stays zero) -- must still report a
        // TERMINAL status, not `None`/`New`, so Iceberg does not stall.
        let cost_model = CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        };
        let engine = Arc::new(Mutex::new(MatchingEngine::new(cost_model, dec!(0))));
        let sink = MatchingEngineChildOrderSink::new(Arc::clone(&engine));

        let (order_id, fills) = sink
            .submit_child_order(ChildOrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                quantity: dec!(1),
                leverage: None,
                margin_mode: None,
            })
            .await
            .unwrap();
        assert!(fills.is_empty(), "no book depth means no fill at all");

        let status = sink.order_status(order_id).await.unwrap();
        assert_eq!(
            status.map(|order| order.status),
            Some(apexes_core::OrderStatus::Cancelled)
        );
    }

    #[tokio::test]
    async fn leveraged_algo_parent_order_produces_child_fills_carrying_leverage() {
        // Regression test for the whole-branch-review HIGH finding:
        // the EMS execution-algorithm path (TWAP/VWAP/Iceberg/POV) used
        // to strip leverage/margin_mode entirely -- `AlgoOrderRequest`
        // and `ChildOrderIntent` had no such fields, and both
        // `ChildOrderSink` implementations hardcoded `None`, so a
        // strategy submitting an `OrderIntent` with BOTH
        // `execution_algo: Some(..)` AND `leverage: Some(..)` had every
        // child fill silently spot-accounted. This test drives the REAL
        // `InMemoryEms` TWAP scheduling path end-to-end (submit parent
        // order -> on_market_event -> slice submission through
        // `MatchingEngineChildOrderSink` against a real
        // `MatchingEngine`) and asserts the resulting child fills carry
        // the parent order's leverage/margin_mode.
        let engine = engine_with_book();
        let sink = MatchingEngineChildOrderSink::new(Arc::clone(&engine));
        let ems = apexes_ems::InMemoryEms::new(
            Arc::new(sink),
            Arc::new(apexes_strategy::PriceAndFeeScorer),
            HashMap::new(),
            None,
        );

        let parent_id = apexes_ems::Ems::submit_algo_order(
            &ems,
            apexes_ems::AlgoOrderRequest {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                side: Side::Buy,
                quantity: dec!(4),
                execution_algo: apexes_strategy::ExecutionAlgoSpec::Twap {
                    duration: std::time::Duration::from_secs(40),
                    slices: 4,
                },
                routing: apexes_strategy::OrderRouting::Fixed,
                leverage: Some(dec!(10)),
                margin_mode: Some(apexes_core::MarginMode::Isolated),
            },
        )
        .await
        .unwrap();

        // The first TWAP slice is due at submission time (next_slice_at
        // is set to Utc::now() by submit_algo_order) -- one
        // on_market_event call at "now" submits it against the shared
        // book, filling synchronously.
        let fills = apexes_ems::Ems::on_market_event(
            &ems,
            &apexes_core::MarketEvent::Depth(DepthUpdate {
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
                is_snapshot: false,
                timestamp: Utc::now(),
            }),
        )
        .await
        .unwrap();

        assert_eq!(
            fills.len(),
            1,
            "the first TWAP slice must fill against the book"
        );
        assert_eq!(fills[0].quantity, dec!(1));
        assert_eq!(
            fills[0].leverage,
            Some(dec!(10)),
            "child fill must carry the parent order's leverage, not degrade to spot"
        );
        assert_eq!(
            fills[0].margin_mode,
            Some(apexes_core::MarginMode::Isolated)
        );
        let status = apexes_ems::Ems::parent_order_status(&ems, parent_id)
            .await
            .unwrap();
        assert_eq!(status, Some(apexes_strategy::ParentOrderStatus::Scheduling));
    }
}
