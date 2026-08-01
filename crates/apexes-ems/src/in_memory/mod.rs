//! [`InMemoryEms`]: the `Ems` implementation used by `apexes-backtest`
//! and `apexes-paper` (never by `apexes-live-trading`, which uses
//! `crate::postgres::PostgresEms` instead). Paced by virtual/event time
//! via `Ems::on_market_event` -- see this crate's `lib.rs` docs for the
//! design rationale (a real `tokio::time::sleep`-paced scheduler would
//! make backtests take as long as the algorithm's real-time duration,
//! defeating the purpose of historical replay).

mod routing;
mod state;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use apexes_core::{AssetClass, MarketEvent};
use apexes_strategy::{ParentOrderId, ParentOrderStatus, VenueMarketState, VenueScorer};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::child_order_sink::{ChildOrderIntent, ChildOrderSink};
use crate::ems::{AlgoOrderRequest, Ems};
use crate::error::EmsError;
use state::{MarketStateTracker, ParentOrderState, SchedulingState};

pub struct InMemoryEms {
    sink: Arc<dyn ChildOrderSink>,
    scorer: Arc<dyn VenueScorer>,
    /// Fixed per-`AssetClass` fee estimate, supplied at construction --
    /// see `state.rs`'s `MarketStateTracker` doc comment for why this
    /// isn't a `FeeSchedule` dependency.
    estimated_fee_bps: HashMap<AssetClass, Decimal>,
    parent_orders: Mutex<HashMap<ParentOrderId, ParentOrderState>>,
    market_state: Mutex<MarketStateTracker>,
}

impl InMemoryEms {
    pub fn new(
        sink: Arc<dyn ChildOrderSink>,
        scorer: Arc<dyn VenueScorer>,
        estimated_fee_bps: HashMap<AssetClass, Decimal>,
    ) -> Self {
        Self {
            sink,
            scorer,
            estimated_fee_bps,
            parent_orders: Mutex::new(HashMap::new()),
            market_state: Mutex::new(MarketStateTracker::default()),
        }
    }

    fn build_market_state(&self, candidates: &[AssetClass], symbol: &str) -> VenueMarketState {
        let tracker = self.market_state.lock().unwrap_or_else(|p| p.into_inner());
        let mut best_bid_ask = HashMap::new();
        for candidate in candidates {
            if let Some(quote) = tracker.best_bid_ask.get(&(symbol.to_string(), *candidate)) {
                best_bid_ask.insert(*candidate, *quote);
            }
        }
        VenueMarketState {
            best_bid_ask,
            estimated_fee_bps: self.estimated_fee_bps.clone(),
        }
    }

    /// Submits one child-order slice of `quantity` for `allocation`'s
    /// `asset_class` -- shared by every algorithm's slice-submission
    /// logic (TWAP here; Iceberg/POV/VWAP in Task 14).
    async fn submit_slice(
        &self,
        parent_id: ParentOrderId,
        symbol: &str,
        venue: apexes_core::Venue,
        side: apexes_core::Side,
        asset_class: AssetClass,
        quantity: Decimal,
    ) -> Result<apexes_core::OrderId, EmsError> {
        self.sink
            .submit_child_order(ChildOrderIntent {
                symbol: symbol.to_string(),
                venue,
                asset_class,
                side,
                quantity,
            })
            .await
            .map_err(|error| EmsError::ChildOrderSubmissionFailed {
                parent_id,
                reason: error.to_string(),
            })
    }

    /// Submits one TWAP slice for `parent_id` (whichever venue
    /// allocation(s) it has) and advances that parent's `next_slice_at`/
    /// `slices_remaining`, marking it `Completed` once no slices remain.
    async fn submit_due_twap_slice(
        &self,
        parent_id: ParentOrderId,
        now: DateTime<Utc>,
    ) -> Result<(), EmsError> {
        let (symbol, venue, side, slice_quantity, slice_interval, allocation_quantities) = {
            let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            let Some(state) = parent_orders.get(&parent_id) else {
                return Ok(());
            };
            let SchedulingState::Twap {
                slice_quantity,
                slice_interval,
                ..
            } = &state.scheduling
            else {
                return Ok(());
            };
            (
                state.request.symbol.clone(),
                state.request.venue,
                state.request.side,
                *slice_quantity,
                *slice_interval,
                state
                    .allocations
                    .iter()
                    .map(|a| (a.asset_class, a.quantity))
                    .collect::<Vec<_>>(),
            )
        };

        let total_quantity: Decimal = allocation_quantities.iter().map(|(_, q)| *q).sum();
        if !total_quantity.is_zero() {
            // Split a TWAP slice proportionally across every venue
            // allocation from routing, same normalized-by-quantity logic
            // as the parent's own allocation split (each allocation's
            // own share of `slice_quantity`, proportional to its share
            // of the parent's total quantity).
            for (asset_class, allocation_quantity) in allocation_quantities {
                let this_venue_slice = slice_quantity * (allocation_quantity / total_quantity);
                if this_venue_slice.is_zero() {
                    continue;
                }
                let child_id = self
                    .submit_slice(
                        parent_id,
                        &symbol,
                        venue,
                        side,
                        asset_class,
                        this_venue_slice,
                    )
                    .await?;

                let mut parent_orders =
                    self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(state) = parent_orders.get_mut(&parent_id) {
                    state.child_order_ids.push(child_id);
                    if let Some(allocation) = state
                        .allocations
                        .iter_mut()
                        .find(|a| a.asset_class == asset_class)
                    {
                        allocation.submitted_quantity += this_venue_slice;
                    }
                }
            }
        }

        let mut parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(state) = parent_orders.get_mut(&parent_id)
            && let SchedulingState::Twap {
                next_slice_at,
                slices_remaining,
                ..
            } = &mut state.scheduling
        {
            *slices_remaining = slices_remaining.saturating_sub(1);
            if *slices_remaining == 0 {
                state.status = ParentOrderStatus::Completed;
            } else {
                *next_slice_at = now + slice_interval;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Ems for InMemoryEms {
    async fn submit_algo_order(
        &self,
        request: AlgoOrderRequest,
    ) -> Result<ParentOrderId, EmsError> {
        let parent_id = ParentOrderId(Uuid::new_v4());

        let candidates = [AssetClass::Spot, AssetClass::Perpetual];
        let market_state = self.build_market_state(&candidates, &request.symbol);
        let allocations =
            match routing::resolve_allocations(&request, self.scorer.as_ref(), &market_state) {
                Ok(allocations) => allocations,
                Err(error) => {
                    let mut parent_orders =
                        self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
                    parent_orders.insert(
                        parent_id,
                        ParentOrderState {
                            request,
                            status: ParentOrderStatus::Failed {
                                reason: error.to_string(),
                            },
                            allocations: Vec::new(),
                            child_order_ids: Vec::new(),
                            scheduling: SchedulingState::Twap {
                                next_slice_at: Utc::now(),
                                slice_interval: chrono::Duration::zero(),
                                slice_quantity: Decimal::ZERO,
                                slices_remaining: 0,
                            },
                        },
                    );
                    return Ok(parent_id);
                }
            };

        let scheduling = match &request.execution_algo {
            apexes_strategy::ExecutionAlgoSpec::Twap { duration, slices } => {
                let slices = (*slices).max(1);
                let slice_interval = chrono::Duration::from_std(*duration)
                    .unwrap_or(chrono::Duration::zero())
                    / slices as i32;
                SchedulingState::Twap {
                    next_slice_at: Utc::now(),
                    slice_interval,
                    slice_quantity: request.quantity / Decimal::from(slices),
                    slices_remaining: slices,
                }
            }
            // Populated by Task 14.
            apexes_strategy::ExecutionAlgoSpec::Vwap { .. } => SchedulingState::Iceberg {
                visible_qty: Decimal::ZERO,
            },
            apexes_strategy::ExecutionAlgoSpec::Iceberg { visible_qty } => {
                SchedulingState::Iceberg {
                    visible_qty: *visible_qty,
                }
            }
            apexes_strategy::ExecutionAlgoSpec::Pov { participation_rate } => {
                SchedulingState::Pov {
                    participation_rate: *participation_rate,
                }
            }
        };

        let mut parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
        parent_orders.insert(
            parent_id,
            ParentOrderState {
                request,
                status: ParentOrderStatus::Scheduling,
                allocations,
                child_order_ids: Vec::new(),
                scheduling,
            },
        );

        Ok(parent_id)
    }

    async fn parent_order_status(
        &self,
        id: ParentOrderId,
    ) -> Result<Option<ParentOrderStatus>, EmsError> {
        let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
        Ok(parent_orders.get(&id).map(|state| state.status.clone()))
    }

    async fn cancel_parent_order(&self, id: ParentOrderId) -> Result<(), EmsError> {
        let child_ids = {
            let mut parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            let Some(state) = parent_orders.get_mut(&id) else {
                return Err(EmsError::ParentOrderNotFound { id });
            };
            state.status = ParentOrderStatus::Cancelled;
            state.child_order_ids.clone()
        };
        for child_id in child_ids {
            self.sink.cancel_child_order(child_id).await?;
        }
        Ok(())
    }

    async fn on_market_event(&self, event: &MarketEvent) -> Result<(), EmsError> {
        if let MarketEvent::Depth(update) = event {
            let mut tracker = self.market_state.lock().unwrap_or_else(|p| p.into_inner());
            for asset_class in [AssetClass::Spot, AssetClass::Perpetual] {
                tracker.record_depth(&update.symbol, asset_class, update);
            }
        }

        let now = event_timestamp(event);
        let due: Vec<ParentOrderId> = {
            let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            parent_orders
                .iter()
                .filter(|(_, state)| {
                    matches!(state.status, ParentOrderStatus::Scheduling)
                        && matches!(
                            &state.scheduling,
                            SchedulingState::Twap { next_slice_at, .. } if *next_slice_at <= now
                        )
                })
                .map(|(id, _)| *id)
                .collect()
        };

        for parent_id in due {
            self.submit_due_twap_slice(parent_id, now).await?;
        }

        Ok(())
    }
}

fn event_timestamp(event: &MarketEvent) -> DateTime<Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{OrderId, Side, Venue};
    use apexes_strategy::{ExecutionAlgoSpec, OrderRouting, PriceAndFeeScorer};
    use rust_decimal_macros::dec;
    use std::sync::Mutex as StdMutex;

    struct RecordingSink {
        submitted: StdMutex<Vec<ChildOrderIntent>>,
    }

    impl RecordingSink {
        fn new() -> Self {
            Self {
                submitted: StdMutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ChildOrderSink for RecordingSink {
        async fn submit_child_order(&self, intent: ChildOrderIntent) -> Result<OrderId, EmsError> {
            self.submitted.lock().unwrap().push(intent);
            Ok(OrderId(Uuid::new_v4()))
        }

        async fn cancel_child_order(&self, _order_id: OrderId) -> Result<(), EmsError> {
            Ok(())
        }

        async fn order_status(
            &self,
            _order_id: OrderId,
        ) -> Result<Option<apexes_core::Order>, EmsError> {
            Ok(None)
        }
    }

    fn sample_request() -> AlgoOrderRequest {
        AlgoOrderRequest {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            quantity: dec!(4),
            execution_algo: ExecutionAlgoSpec::Twap {
                duration: std::time::Duration::from_secs(40),
                slices: 4,
            },
            routing: OrderRouting::Fixed,
        }
    }

    fn depth_event(now: DateTime<Utc>) -> MarketEvent {
        MarketEvent::Depth(apexes_core::DepthUpdate {
            symbol: "OTHERSYMBOL".to_string(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![],
            sequence: None,
            is_snapshot: true,
            timestamp: now,
        })
    }

    #[tokio::test]
    async fn twap_submits_one_slice_immediately_on_submission() {
        let sink = Arc::new(RecordingSink::new());
        let ems = InMemoryEms::new(sink.clone(), Arc::new(PriceAndFeeScorer), HashMap::new());
        let parent_id = ems.submit_algo_order(sample_request()).await.unwrap();

        // The FIRST slice is due at submission time (`next_slice_at:
        // Utc::now()` set in submit_algo_order) -- one on_market_event
        // call at "now" should trigger it.
        ems.on_market_event(&depth_event(Utc::now())).await.unwrap();

        assert_eq!(sink.submitted.lock().unwrap().len(), 1);
        assert_eq!(sink.submitted.lock().unwrap()[0].quantity, dec!(1));
        assert_eq!(
            ems.parent_order_status(parent_id).await.unwrap(),
            Some(ParentOrderStatus::Scheduling)
        );
    }

    #[tokio::test]
    async fn twap_completes_after_all_slices_are_submitted_across_advancing_virtual_time() {
        let sink = Arc::new(RecordingSink::new());
        let ems = InMemoryEms::new(sink.clone(), Arc::new(PriceAndFeeScorer), HashMap::new());
        let parent_id = ems.submit_algo_order(sample_request()).await.unwrap();

        let mut now = Utc::now();
        // 4 slices, 10s apart (40s / 4) -- advance virtual time in 10s
        // steps, calling on_market_event once per step, exactly as
        // run_event_loop would for a sequence of real MarketEvents.
        for _ in 0..4 {
            ems.on_market_event(&depth_event(now)).await.unwrap();
            now += chrono::Duration::seconds(10);
        }

        assert_eq!(sink.submitted.lock().unwrap().len(), 4);
        let total: Decimal = sink
            .submitted
            .lock()
            .unwrap()
            .iter()
            .map(|intent| intent.quantity)
            .sum();
        assert_eq!(total, dec!(4));
        assert_eq!(
            ems.parent_order_status(parent_id).await.unwrap(),
            Some(ParentOrderStatus::Completed)
        );
    }

    #[tokio::test]
    async fn cancel_parent_order_stops_further_slices_and_cancels_submitted_children() {
        let sink = Arc::new(RecordingSink::new());
        let ems = InMemoryEms::new(sink.clone(), Arc::new(PriceAndFeeScorer), HashMap::new());
        let parent_id = ems.submit_algo_order(sample_request()).await.unwrap();

        let now = Utc::now();
        ems.on_market_event(&depth_event(now)).await.unwrap(); // 1st slice submitted
        ems.cancel_parent_order(parent_id).await.unwrap();

        // Advance virtual time well past all remaining slice deadlines --
        // no further slices should be submitted since status is Cancelled.
        ems.on_market_event(&depth_event(now + chrono::Duration::seconds(100)))
            .await
            .unwrap();

        assert_eq!(sink.submitted.lock().unwrap().len(), 1);
        assert_eq!(
            ems.parent_order_status(parent_id).await.unwrap(),
            Some(ParentOrderStatus::Cancelled)
        );
    }
}
