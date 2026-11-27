//! Event-time transit for simulated EMS children. No sleeps or live gateways.
use super::*;
use apexes_core::{Fill, OrderStatus};
use chrono::{DateTime, TimeDelta};
use rust_decimal::Decimal;
use std::collections::VecDeque;

use crate::{ChildLatencyReport, EventLatencyPolicy};

pub(super) struct ChildArrivalQueue {
    policy: EventLatencyPolicy,
    pending: VecDeque<PendingChild>,
    watermark: Option<DateTime<Utc>>,
    report: ChildLatencyReport,
}

struct PendingChild {
    deadline: DateTime<Utc>,
    id: OrderId,
    parent: Option<apexes_strategy::ParentOrderId>,
    intent: OrderIntent,
}

fn refusal(reason: impl Into<String>) -> EmsError {
    EmsError::ChildOrderSubmissionFailed {
        parent_id: apexes_strategy::ParentOrderId(uuid::Uuid::nil()),
        reason: reason.into(),
    }
}

impl MatchingEngineChildOrderSink {
    pub fn with_event_latency(mut self, policy: Option<EventLatencyPolicy>) -> Self {
        self.arrivals = policy.map(|policy| {
            Mutex::new(ChildArrivalQueue {
                policy,
                pending: VecDeque::new(),
                watermark: None,
                report: ChildLatencyReport::default(),
            })
        });
        self
    }

    pub(super) fn schedule_child(
        &self,
        arrivals: &Mutex<ChildArrivalQueue>,
        parent: Option<apexes_strategy::ParentOrderId>,
        intent: OrderIntent,
        now: DateTime<Utc>,
    ) -> Result<(OrderId, Vec<Fill>), EmsError> {
        let mut queue = arrivals.lock().unwrap_or_else(|p| p.into_inner());
        let deadline =
            now.checked_add_signed(TimeDelta::milliseconds(i64::from(queue.policy.delay_ms)));
        let validation = if queue.policy.validate().is_err() {
            Err(refusal("invalid child transit delay"))
        } else if queue.watermark.is_some_and(|at| now < at)
            || queue
                .pending
                .back()
                .is_some_and(|child| deadline.is_some_and(|d| d < child.deadline))
        {
            Err(refusal("regressing child submission timestamp"))
        } else if queue.pending.len() >= 1024 {
            Err(refusal("EMS child arrival queue full"))
        } else if intent.quantity <= Decimal::ZERO {
            Err(refusal("nonpositive child quantity"))
        } else {
            self.engine
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .execution_policy()
                .validate_intent(&intent)
                .map_err(|e| refusal(e.to_string()))
        };
        if let Err(error) = validation {
            queue.report.refused += 1;
            return Err(error);
        }
        let Some(deadline) = deadline else {
            queue.report.refused += 1;
            return Err(refusal("child arrival timestamp overflow"));
        };
        let id = OrderId(uuid::Uuid::new_v4());
        let order = Order {
            id,
            exchange_order_id: None,
            symbol: intent.symbol.clone(),
            venue: intent.venue,
            asset_class: intent.asset_class,
            side: intent.side,
            order_type: OrderType::Market,
            quantity: intent.quantity,
            price: None,
            stop_price: None,
            leverage: intent.leverage,
            margin_mode: intent.margin_mode,
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
        self.submitted
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id, order);
        queue.pending.push_back(PendingChild {
            deadline,
            id,
            parent,
            intent,
        });
        queue.report.scheduled += 1;
        Ok((id, Vec::new()))
    }

    pub(super) fn deliver_arrivals(
        &self,
        now: DateTime<Utc>,
        mut check: Option<&mut apexes_ems::SimulatedChildCheck<'_>>,
    ) -> Vec<Fill> {
        let Some(arrivals) = &self.arrivals else {
            return Vec::new();
        };
        let mut queue = arrivals.lock().unwrap_or_else(|p| p.into_inner());
        if queue.watermark.is_some_and(|at| now < at) {
            return Vec::new();
        }
        queue.watermark = Some(now);
        let mut fills = Vec::new();
        while queue
            .pending
            .front()
            .is_some_and(|child| child.deadline <= now)
        {
            let Some(PendingChild {
                id, parent, intent, ..
            }) = queue.pending.pop_front()
            else {
                break;
            };
            queue.report.arrived += 1;
            let result = {
                let mut engine = self.engine.lock().unwrap_or_else(|p| p.into_inner());
                match check.as_deref_mut() {
                    Some(check) => {
                        engine.submit_market_child_checked(&intent, now, id, parent, check)
                    }
                    None => engine.submit_market_child(&intent, now),
                }
            };
            let mut orders = self.submitted.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(order) = orders.get_mut(&id) {
                order.updated_at = now;
                match result {
                    Ok((executed, mut child_fills)) => {
                        order.status = executed.status;
                        order.filled_quantity = executed.filled_quantity;
                        for fill in &mut child_fills {
                            fill.order_id = id;
                        }
                        fills.extend(child_fills);
                    }
                    Err(error) => {
                        order.status = OrderStatus::Rejected;
                        queue.report.rejected_at_arrival += 1;
                        tracing::warn!(order_id = %id.0, %error, "simulated EMS child arrival rejected");
                    }
                }
            }
        }
        fills
    }

    pub(super) fn cancel_pending_child(&self, id: OrderId) {
        let Some(arrivals) = &self.arrivals else {
            return;
        };
        let mut queue = arrivals.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(index) = queue.pending.iter().position(|child| child.id == id) {
            queue.pending.remove(index);
            queue.report.cancelled_before_arrival += 1;
            if let Some(order) = self
                .submitted
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get_mut(&id)
            {
                order.status = OrderStatus::Cancelled;
                order.updated_at = queue
                    .watermark
                    .map_or(order.created_at, |at| at.max(order.created_at));
            }
        }
    }

    /// End-of-replay cancels undelivered orders, without inventing a future event.
    pub fn finish_child_latency(&self) -> Option<ChildLatencyReport> {
        let arrivals = self.arrivals.as_ref()?;
        let mut queue = arrivals.lock().unwrap_or_else(|p| p.into_inner());
        let now = queue.watermark;
        let mut orders = self.submitted.lock().unwrap_or_else(|p| p.into_inner());
        while let Some(PendingChild { id, .. }) = queue.pending.pop_front() {
            queue.report.expired_at_end += 1;
            if let Some(order) = orders.get_mut(&id) {
                order.status = OrderStatus::Cancelled;
                order.updated_at = now.map_or(order.created_at, |at| at.max(order.created_at));
            }
        }
        Some(queue.report.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost_model::{CostModel, FlatFeeSchedule};
    use apexes_core::{AssetClass, DepthUpdate, MarketEvent, PriceLevel, Side, Venue};
    use apexes_ems::{AlgoOrderRequest, Ems, InMemoryEms};
    use apexes_strategy::{ExecutionAlgoSpec, OrderRouting, ParentOrderStatus, PriceAndFeeScorer};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn engine() -> Arc<Mutex<MatchingEngine>> {
        Arc::new(Mutex::new(MatchingEngine::new(
            CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(0),
                    taker_bps: dec!(10),
                }),
                funding_rate_bps: None,
            },
            dec!(0),
        )))
    }

    fn depth(at: DateTime<Utc>, price: Decimal, quantity: Decimal) -> DepthUpdate {
        DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![PriceLevel { price, quantity }],
            sequence: None,
            is_snapshot: true,
            timestamp: at,
        }
    }

    fn child(quantity: Decimal) -> ChildOrderIntent {
        ChildOrderIntent {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            quantity,
            leverage: None,
            margin_mode: None,
        }
    }

    #[tokio::test]
    async fn deferred_children_use_arrival_depth_fifo_and_stable_identity() {
        let engine = engine();
        let t = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        engine
            .lock()
            .unwrap()
            .on_depth_update(&depth(t, dec!(100), dec!(20)));
        let sink = MatchingEngineChildOrderSink::new(engine.clone())
            .with_event_latency(Some(EventLatencyPolicy { delay_ms: 500 }));
        let (first, fills) = sink.submit_child_order_at(child(dec!(3)), t).await.unwrap();
        let (second, _) = sink.submit_child_order_at(child(dec!(3)), t).await.unwrap();
        assert!(fills.is_empty());
        assert_eq!(
            sink.order_status(first).await.unwrap().unwrap().status,
            OrderStatus::New
        );
        assert!(
            sink.advance_simulated_time(t + TimeDelta::milliseconds(499))
                .await
                .is_empty()
        );

        let at = t + TimeDelta::milliseconds(600);
        engine
            .lock()
            .unwrap()
            .on_depth_update(&depth(at, dec!(200), dec!(4)));
        let fills = sink.advance_simulated_time(at).await;
        assert_eq!(
            fills
                .iter()
                .map(|f| (f.order_id, f.quantity, f.price, f.filled_at))
                .collect::<Vec<_>>(),
            vec![
                (first, dec!(3), dec!(200), at),
                (second, dec!(1), dec!(200), at)
            ]
        );
        assert_eq!(
            sink.order_status(first).await.unwrap().unwrap().status,
            OrderStatus::Filled
        );
        let order = sink.order_status(second).await.unwrap().unwrap();
        assert_eq!(order.status, OrderStatus::Cancelled);
        assert_eq!(order.filled_quantity, dec!(1));
        assert_eq!(order.created_at, t);
        assert!(sink.advance_simulated_time(at).await.is_empty());
        let report = sink.finish_child_latency().unwrap();
        assert_eq!(
            (report.scheduled, report.arrived, report.expired_at_end),
            (2, 2, 0)
        );
    }

    #[tokio::test]
    async fn refused_child_does_not_consume_depth_or_hide_later_fills() {
        let engine = engine();
        let t = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        engine
            .lock()
            .unwrap()
            .on_depth_update(&depth(t, dec!(100), dec!(2)));
        let sink = MatchingEngineChildOrderSink::new(engine)
            .with_event_latency(Some(EventLatencyPolicy { delay_ms: 500 }));
        let (first, _) = sink.submit_child_order_at(child(dec!(2)), t).await.unwrap();
        let (second, _) = sink.submit_child_order_at(child(dec!(2)), t).await.unwrap();
        let mut seen = Vec::new();
        let mut check =
            |_: Option<apexes_strategy::ParentOrderId>, _: &OrderIntent, fills: &[Fill]| {
                seen.push(fills[0].order_id);
                if fills[0].order_id == first {
                    Err("test rejection".into())
                } else {
                    Ok(())
                }
            };
        let fills = sink
            .advance_simulated_time_checked(t + TimeDelta::milliseconds(500), &mut check)
            .await;
        assert_eq!(seen, vec![first, second]);
        assert_eq!(fills.len(), 1);
        assert_eq!((fills[0].order_id, fills[0].quantity), (second, dec!(2)));
        assert_eq!(
            sink.order_status(first).await.unwrap().unwrap().status,
            OrderStatus::Rejected
        );
        assert_eq!(
            sink.order_status(second).await.unwrap().unwrap().status,
            OrderStatus::Filled
        );
        assert_eq!(sink.finish_child_latency().unwrap().rejected_at_arrival, 1);
    }

    #[tokio::test]
    async fn cancellation_and_end_expiry_prevent_late_execution() {
        let engine = engine();
        let t = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        engine
            .lock()
            .unwrap()
            .on_depth_update(&depth(t, dec!(100), dec!(20)));
        let sink = MatchingEngineChildOrderSink::new(engine)
            .with_event_latency(Some(EventLatencyPolicy { delay_ms: 500 }));
        let (first, _) = sink.submit_child_order_at(child(dec!(1)), t).await.unwrap();
        let (second, _) = sink.submit_child_order_at(child(dec!(1)), t).await.unwrap();
        sink.cancel_child_order(first).await.unwrap();
        sink.cancel_child_order(first).await.unwrap();
        let report = sink.finish_child_latency().unwrap();
        assert_eq!(
            (report.cancelled_before_arrival, report.expired_at_end),
            (1, 1)
        );
        for id in [first, second] {
            assert_eq!(
                sink.order_status(id).await.unwrap().unwrap().status,
                OrderStatus::Cancelled
            );
        }
        assert!(
            sink.advance_simulated_time(t + TimeDelta::seconds(2))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn stale_depth_rejection_is_terminal_and_counted() {
        let engine = engine();
        let t = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        {
            let mut guard = engine.lock().unwrap();
            let cost = CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(0),
                    taker_bps: dec!(10),
                }),
                funding_rate_bps: None,
            };
            *guard = MatchingEngine::new(cost, dec!(0)).with_depth_policy(Some(
                crate::SimulatedDepthPolicy {
                    max_age_ms: 100,
                    sequence: crate::DepthSequence::SnapshotOnly,
                },
            ));
            guard.on_depth_update(&depth(t, dec!(100), dec!(20)));
        }
        let sink = MatchingEngineChildOrderSink::new(engine)
            .with_event_latency(Some(EventLatencyPolicy { delay_ms: 500 }));
        let (id, _) = sink.submit_child_order_at(child(dec!(1)), t).await.unwrap();
        assert!(
            sink.advance_simulated_time(t + TimeDelta::milliseconds(500))
                .await
                .is_empty()
        );
        let order = sink.order_status(id).await.unwrap().unwrap();
        assert_eq!(order.status, OrderStatus::Rejected);
        assert_eq!(order.filled_quantity, dec!(0));
        let report = sink.finish_child_latency().unwrap();
        assert_eq!(
            (report.scheduled, report.arrived, report.rejected_at_arrival),
            (1, 1, 1)
        );
    }

    #[tokio::test]
    async fn parent_remains_open_until_all_deferred_children_are_terminal() {
        let engine = engine();
        let t = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let sink = Arc::new(
            MatchingEngineChildOrderSink::new(engine.clone())
                .with_event_latency(Some(EventLatencyPolicy { delay_ms: 500 })),
        );
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
        )
        .with_event_time(true);
        let first = depth(t, dec!(100), dec!(20));
        engine.lock().unwrap().on_depth_update(&first);
        assert!(
            ems.on_market_event(&MarketEvent::Depth(first))
                .await
                .unwrap()
                .is_empty()
        );
        let parent = ems
            .submit_algo_order(AlgoOrderRequest {
                symbol: "BTCUSDT".into(),
                venue: Venue::Binance,
                side: Side::Buy,
                quantity: dec!(1),
                execution_algo: ExecutionAlgoSpec::Twap {
                    duration: std::time::Duration::from_secs(1),
                    slices: 1,
                },
                routing: OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            })
            .await
            .unwrap();
        let scheduled = depth(t + TimeDelta::milliseconds(1), dec!(101), dec!(20));
        engine.lock().unwrap().on_depth_update(&scheduled);
        assert!(
            ems.on_market_event(&MarketEvent::Depth(scheduled))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            ems.parent_order_status(parent).await.unwrap(),
            Some(ParentOrderStatus::Scheduling)
        );
        let arrived = depth(t + TimeDelta::milliseconds(501), dec!(102), dec!(20));
        engine.lock().unwrap().on_depth_update(&arrived);
        let fills = ems
            .on_market_event(&MarketEvent::Depth(arrived))
            .await
            .unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, dec!(102));
        assert_eq!(
            ems.parent_order_status(parent).await.unwrap(),
            Some(ParentOrderStatus::Completed)
        );
    }
}
