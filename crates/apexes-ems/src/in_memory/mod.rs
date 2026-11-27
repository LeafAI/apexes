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

use apexes_core::{AssetClass, Fill, MarketEvent};
use apexes_strategy::{ParentOrderId, ParentOrderStatus, VenueMarketState, VenueScorer};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::child_order_sink::{ChildOrderIntent, ChildOrderSink};
use crate::ems::{AlgoOrderRequest, Ems};
use crate::error::EmsError;
use crate::volume_curve_provider::VolumeCurveProvider;
use state::{MarketStateTracker, ParentOrderState, SchedulingState};

pub struct InMemoryEms {
    sink: Arc<dyn ChildOrderSink>,
    scorer: Arc<dyn VenueScorer>,
    /// Fixed per-`AssetClass` fee estimate, supplied at construction --
    /// see `state.rs`'s `MarketStateTracker` doc comment for why this
    /// isn't a `FeeSchedule` dependency.
    estimated_fee_bps: HashMap<AssetClass, Decimal>,
    /// Historical intraday volume curves for VWAP pacing. `None` makes
    /// every `Vwap` request fail immediately with
    /// `EmsError::InsufficientVwapHistory`; `Twap`/`Iceberg`/`Pov` are
    /// unaffected.
    volume_curve_provider: Option<Arc<dyn VolumeCurveProvider>>,
    parent_orders: Mutex<HashMap<ParentOrderId, ParentOrderState>>,
    market_state: Mutex<MarketStateTracker>,
    causal: bool,
    event_time: Mutex<Option<DateTime<Utc>>>,
}

/// The fixed per-slice attributes every algorithm's slice-submission
/// path threads from a parent order's retained `AlgoOrderRequest` into
/// each child `ChildOrderIntent` (see `submit_slice`). Grouped into one
/// struct so `submit_slice`/`refill_iceberg_allocation` stay under the
/// argument-count lint instead of growing a parameter per new field.
struct SliceParams {
    symbol: String,
    venue: apexes_core::Venue,
    side: apexes_core::Side,
    asset_class: AssetClass,
    leverage: Option<Decimal>,
    margin_mode: Option<apexes_core::MarginMode>,
}

impl InMemoryEms {
    pub fn new(
        sink: Arc<dyn ChildOrderSink>,
        scorer: Arc<dyn VenueScorer>,
        estimated_fee_bps: HashMap<AssetClass, Decimal>,
        volume_curve_provider: Option<Arc<dyn VolumeCurveProvider>>,
    ) -> Self {
        Self {
            sink,
            scorer,
            estimated_fee_bps,
            volume_curve_provider,
            parent_orders: Mutex::new(HashMap::new()),
            market_state: Mutex::new(MarketStateTracker::default()),
            causal: false,
            event_time: Mutex::new(None),
        }
    }

    pub fn with_event_time(mut self, enabled: bool) -> Self {
        self.causal = enabled;
        self
    }

    /// In causal simulation each fixed-route parent has one allocation.
    /// A failed parent must not erase earlier parents' executed fills.
    async fn collect_parent_result(
        &self,
        id: ParentOrderId,
        result: Result<Vec<Fill>, EmsError>,
        fills: &mut Vec<Fill>,
    ) -> Result<(), EmsError> {
        match result {
            Ok(child_fills) => fills.extend(child_fills),
            Err(error) if self.causal => {
                let reason = error.to_string();
                let children = {
                    let mut parents = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
                    if let Some(parent) = parents.get_mut(&id) {
                        parent.status = ParentOrderStatus::Failed {
                            reason: reason.clone(),
                        };
                        parent.child_order_ids.clone()
                    } else {
                        Vec::new()
                    }
                };
                if self.sink.defers_children() {
                    for child in children {
                        if let Err(error) = self.sink.cancel_child_order(child).await {
                            tracing::warn!(parent_id = %id.0, child_id = %child.0, %error, "failed to cancel deferred child");
                        }
                    }
                }
                tracing::warn!(parent_id = %id.0, %reason, "causal simulated parent failed");
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    fn sort_parents<T>(&self, items: &mut [T], id: impl Fn(&T) -> ParentOrderId) {
        let parents = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
        items.sort_by_key(|item| parents.get(&id(item)).map_or(usize::MAX, |p| p.ordinal));
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

    /// Submits one child-order slice of `quantity` per `params` --
    /// shared by every algorithm's slice-submission logic (TWAP,
    /// Iceberg, POV, and VWAP). `params.leverage`/`params.margin_mode`
    /// are the parent order's own values (captured from its retained
    /// `AlgoOrderRequest` at every call site), threaded onto the child
    /// order unchanged so leveraged parent orders do not silently
    /// degrade to spot accounting. Returns the child order's
    /// `OrderId` and any fills that happened synchronously at
    /// submission (propagated from the sink, see
    /// `ChildOrderSink::submit_child_order`).
    async fn submit_slice(
        &self,
        parent_id: ParentOrderId,
        params: &SliceParams,
        quantity: Decimal,
    ) -> Result<(apexes_core::OrderId, Vec<Fill>), EmsError> {
        let intent = ChildOrderIntent {
            symbol: params.symbol.clone(),
            venue: params.venue,
            asset_class: params.asset_class,
            side: params.side,
            quantity,
            leverage: params.leverage,
            margin_mode: params.margin_mode,
        };
        let at = *self.event_time.lock().unwrap_or_else(|p| p.into_inner());
        let result = if self.causal {
            self.sink
                .submit_parent_child_at(parent_id, intent, at.ok_or(EmsError::SchedulingSuspended)?)
                .await
        } else {
            self.sink.submit_child_order(intent).await
        }
        .map_err(|error| EmsError::ChildOrderSubmissionFailed {
            parent_id,
            reason: error.to_string(),
        });
        match &result {
            Ok((order_id, _)) => {
                if let Some(state) = self
                    .parent_orders
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get_mut(&parent_id)
                {
                    state.child_order_ids.push(*order_id);
                }
                tracing::info!(target: "apexes::decision", stage = "ems_child_accepted", parent_id = %parent_id.0, order_id = %order_id.0)
            }
            Err(_) => {
                tracing::info!(target: "apexes::decision", stage = "ems_child_failed", parent_id = %parent_id.0, reason = "child_submission_failed")
            }
        }
        result
    }

    /// Submits one TWAP slice for `parent_id` (whichever venue
    /// allocation(s) it has) and advances that parent's `next_slice_at`/
    /// `slices_remaining`, marking it `Completed` once no slices remain.
    /// Returns every fill produced synchronously by this call's slice
    /// submissions, in submission order.
    async fn submit_due_twap_slice(
        &self,
        parent_id: ParentOrderId,
        now: DateTime<Utc>,
    ) -> Result<Vec<Fill>, EmsError> {
        let (
            symbol,
            venue,
            side,
            slice_quantity,
            slice_interval,
            allocation_quantities,
            leverage,
            margin_mode,
        ) = {
            let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            let Some(state) = parent_orders.get(&parent_id) else {
                return Ok(Vec::new());
            };
            let SchedulingState::Twap {
                slice_quantity,
                slice_interval,
                ..
            } = &state.scheduling
            else {
                return Ok(Vec::new());
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
                state.request.leverage,
                state.request.margin_mode,
            )
        };

        let mut fills = Vec::new();
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
                let params = SliceParams {
                    symbol: symbol.clone(),
                    venue,
                    side,
                    asset_class,
                    leverage,
                    margin_mode,
                };
                let (_child_id, slice_fills) = self
                    .submit_slice(parent_id, &params, this_venue_slice)
                    .await?;
                fills.extend(slice_fills);

                let mut parent_orders =
                    self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(state) = parent_orders.get_mut(&parent_id)
                    && let Some(allocation) = state
                        .allocations
                        .iter_mut()
                        .find(|a| a.asset_class == asset_class)
                {
                    allocation.submitted_quantity += this_venue_slice;
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

        Ok(fills)
    }

    /// Records `parent_id` as `Failed { reason }` with no allocations
    /// and no child orders submitted -- used when a request cannot even
    /// begin scheduling (VWAP history insufficient/no provider
    /// configured, or routing couldn't resolve any venue).
    async fn fail_parent_order(
        &self,
        parent_id: ParentOrderId,
        request: AlgoOrderRequest,
        reason: String,
    ) -> Result<ParentOrderId, EmsError> {
        if self.causal {
            return Err(EmsError::ParentAdmissionRejected { reason });
        }
        let mut parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
        let ordinal = parent_orders.len();
        parent_orders.insert(
            parent_id,
            ParentOrderState {
                ordinal,
                request,
                status: ParentOrderStatus::Failed { reason },
                allocations: Vec::new(),
                child_order_ids: Vec::new(),
                scheduling: SchedulingState::Iceberg {
                    visible_qty: Decimal::ZERO,
                },
            },
        );
        Ok(parent_id)
    }

    /// For every `Scheduling` parent order using `Iceberg` pacing,
    /// checks each venue allocation's `current_resting_child` (if any)
    /// via `sink.order_status` -- if it has reached a terminal status
    /// (`Filled`/`Cancelled`/`Rejected`), clears it and (if quantity
    /// remains for that allocation) submits the next `visible_qty`
    /// slice. Marks the parent `Completed` once every allocation has
    /// fully submitted its total quantity AND has no resting child left.
    /// Returns every fill produced synchronously by this call's slice
    /// submissions, in submission order.
    async fn process_iceberg_parents(&self) -> Result<Vec<Fill>, EmsError> {
        let mut candidates: Vec<ParentOrderId> = {
            let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            parent_orders
                .iter()
                .filter(|(_, state)| {
                    matches!(state.status, ParentOrderStatus::Scheduling)
                        && matches!(state.scheduling, SchedulingState::Iceberg { .. })
                })
                .map(|(id, _)| *id)
                .collect()
        };

        if self.causal {
            self.sort_parents(&mut candidates, |id| *id);
        }
        let mut fills = Vec::new();
        for parent_id in candidates {
            let result = self.process_one_iceberg_parent(parent_id).await;
            self.collect_parent_result(parent_id, result, &mut fills)
                .await?;
        }
        Ok(fills)
    }

    async fn process_one_iceberg_parent(
        &self,
        parent_id: ParentOrderId,
    ) -> Result<Vec<Fill>, EmsError> {
        let (symbol, venue, side, visible_qty, allocations, leverage, margin_mode) = {
            let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            let Some(state) = parent_orders.get(&parent_id) else {
                return Ok(Vec::new());
            };
            let SchedulingState::Iceberg { visible_qty } = &state.scheduling else {
                return Ok(Vec::new());
            };
            (
                state.request.symbol.clone(),
                state.request.venue,
                state.request.side,
                *visible_qty,
                state.clone_as_asset_classes(),
                state.request.leverage,
                state.request.margin_mode,
            )
        };

        let mut fills = Vec::new();
        let mut all_done = true;
        for asset_class in allocations {
            let params = SliceParams {
                symbol: symbol.clone(),
                venue,
                side,
                asset_class,
                leverage,
                margin_mode,
            };
            let (done, allocation_fills) = self
                .refill_iceberg_allocation(parent_id, &params, visible_qty)
                .await?;
            fills.extend(allocation_fills);
            all_done &= done;
        }

        if all_done {
            let mut parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(state) = parent_orders.get_mut(&parent_id) {
                state.status = ParentOrderStatus::Completed;
            }
        }
        Ok(fills)
    }

    /// Returns `(true, fills)` if this allocation is fully done (all
    /// quantity submitted and no resting child left); `fills` is every
    /// fill produced synchronously by this call's slice submission (if
    /// any).
    async fn refill_iceberg_allocation(
        &self,
        parent_id: ParentOrderId,
        params: &SliceParams,
        visible_qty: Decimal,
    ) -> Result<(bool, Vec<Fill>), EmsError> {
        let resting_child = {
            let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            parent_orders[&parent_id]
                .allocations
                .iter()
                .find(|a| a.asset_class == params.asset_class)
                .and_then(|a| a.current_resting_child)
        };

        if let Some(child_id) = resting_child {
            let status = self.sink.order_status(child_id).await?;
            let terminal = matches!(
                status.map(|o| o.status),
                Some(
                    apexes_core::OrderStatus::Filled
                        | apexes_core::OrderStatus::Cancelled
                        | apexes_core::OrderStatus::Rejected
                )
            );
            if !terminal {
                return Ok((false, Vec::new()));
            }
            let mut parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(state) = parent_orders.get_mut(&parent_id)
                && let Some(allocation) = state
                    .allocations
                    .iter_mut()
                    .find(|a| a.asset_class == params.asset_class)
            {
                allocation.current_resting_child = None;
            }
        }

        let (remaining, _total) = {
            let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            parent_orders[&parent_id]
                .allocations
                .iter()
                .find(|a| a.asset_class == params.asset_class)
                .map(|a| a.cloned_summary())
                .unwrap_or((Decimal::ZERO, Decimal::ZERO))
        };
        if remaining.is_zero() {
            return Ok((true, Vec::new()));
        }

        let slice_quantity = visible_qty.min(remaining);
        let (child_id, slice_fills) = self.submit_slice(parent_id, params, slice_quantity).await?;

        let mut parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(state) = parent_orders.get_mut(&parent_id)
            && let Some(allocation) = state
                .allocations
                .iter_mut()
                .find(|a| a.asset_class == params.asset_class)
        {
            allocation.submitted_quantity += slice_quantity;
            allocation.current_resting_child = Some(child_id);
        }
        Ok((false, slice_fills))
    }

    /// For every `Scheduling` parent order using `Pov` pacing whose
    /// symbol matches `trade.symbol`, submits
    /// `trade.quantity * participation_rate` (capped at each
    /// allocation's own remaining quantity, split proportionally across
    /// allocations the same way TWAP's slice-splitting works) and marks
    /// the parent `Completed` once every allocation's full quantity has
    /// been submitted. Returns every fill produced synchronously by
    /// this call's slice submissions, in submission order.
    async fn process_pov_parents(
        &self,
        trade: &apexes_core::TradeEvent,
    ) -> Result<Vec<Fill>, EmsError> {
        let mut candidates: Vec<(ParentOrderId, Decimal)> = {
            let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            parent_orders
                .iter()
                .filter_map(|(id, state)| {
                    let SchedulingState::Pov { participation_rate } = &state.scheduling else {
                        return None;
                    };
                    if (self.causal
                        && (state.request.venue != trade.venue || trade.quantity <= Decimal::ZERO))
                        || state.request.symbol != trade.symbol
                        || !matches!(state.status, ParentOrderStatus::Scheduling)
                    {
                        return None;
                    }
                    Some((*id, *participation_rate))
                })
                .collect()
        };

        if self.causal {
            self.sort_parents(&mut candidates, |item| item.0);
        }
        let mut fills = Vec::new();
        for (parent_id, participation_rate) in candidates {
            let target_quantity = trade.quantity * participation_rate;
            let result = self.submit_pov_increment(parent_id, target_quantity).await;
            self.collect_parent_result(parent_id, result, &mut fills)
                .await?;
        }
        Ok(fills)
    }

    async fn submit_pov_increment(
        &self,
        parent_id: ParentOrderId,
        target_quantity: Decimal,
    ) -> Result<Vec<Fill>, EmsError> {
        let (symbol, venue, side, allocations, leverage, margin_mode) = {
            let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            let Some(state) = parent_orders.get(&parent_id) else {
                return Ok(Vec::new());
            };
            (
                state.request.symbol.clone(),
                state.request.venue,
                state.request.side,
                state.clone_as_asset_classes(),
                state.request.leverage,
                state.request.margin_mode,
            )
        };

        let mut fills = Vec::new();
        let mut remaining_to_submit = target_quantity;
        for asset_class in allocations {
            if remaining_to_submit.is_zero() {
                break;
            }
            let remaining_for_allocation = {
                let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
                parent_orders[&parent_id]
                    .allocations
                    .iter()
                    .find(|a| a.asset_class == asset_class)
                    .map(|a| a.quantity - a.submitted_quantity)
                    .unwrap_or(Decimal::ZERO)
            };
            let slice = remaining_to_submit.min(remaining_for_allocation);
            if slice.is_zero() {
                continue;
            }
            let params = SliceParams {
                symbol: symbol.clone(),
                venue,
                side,
                asset_class,
                leverage,
                margin_mode,
            };
            let (_child_id, slice_fills) = self.submit_slice(parent_id, &params, slice).await?;
            fills.extend(slice_fills);
            remaining_to_submit -= slice;

            let mut parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(state) = parent_orders.get_mut(&parent_id)
                && let Some(allocation) = state
                    .allocations
                    .iter_mut()
                    .find(|a| a.asset_class == asset_class)
            {
                allocation.submitted_quantity += slice;
            }
        }

        let mut parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(state) = parent_orders.get_mut(&parent_id)
            && state
                .allocations
                .iter()
                .all(|a| a.submitted_quantity >= a.quantity)
        {
            state.status = ParentOrderStatus::Completed;
        }
        Ok(fills)
    }

    /// For every `Scheduling` parent order using `Vwap` pacing, computes
    /// the curve's target cumulative fraction at the current elapsed
    /// time since `started_at` and submits whatever INCREMENTAL
    /// quantity is needed to catch the total submitted quantity up to
    /// `target_fraction * request.quantity` (split proportionally
    /// across allocations, same as TWAP/POV). Marks `Completed` once
    /// the curve's final fraction (`1.0`) has been reached AND fully
    /// submitted. Returns every fill produced synchronously by this
    /// call's slice submissions, in submission order.
    async fn process_vwap_parents(&self, now: DateTime<Utc>) -> Result<Vec<Fill>, EmsError> {
        let mut candidates: Vec<(ParentOrderId, Decimal)> = {
            let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            parent_orders
                .iter()
                .filter_map(|(id, state)| {
                    let SchedulingState::Vwap {
                        curve, started_at, ..
                    } = &state.scheduling
                    else {
                        return None;
                    };
                    if !matches!(state.status, ParentOrderStatus::Scheduling) {
                        return None;
                    }
                    let elapsed = now.signed_duration_since(*started_at);
                    let target_fraction = target_fraction_for_elapsed(curve, elapsed);
                    Some((*id, target_fraction))
                })
                .collect()
        };

        if self.causal {
            self.sort_parents(&mut candidates, |item| item.0);
        }
        let mut fills = Vec::new();
        for (parent_id, target_fraction) in candidates {
            let result = self.submit_vwap_increment(parent_id, target_fraction).await;
            self.collect_parent_result(parent_id, result, &mut fills)
                .await?;
        }
        Ok(fills)
    }

    async fn submit_vwap_increment(
        &self,
        parent_id: ParentOrderId,
        target_fraction: Decimal,
    ) -> Result<Vec<Fill>, EmsError> {
        let (
            symbol,
            venue,
            side,
            total_quantity,
            already_submitted_fraction,
            allocations,
            leverage,
            margin_mode,
        ) = {
            let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            let Some(state) = parent_orders.get(&parent_id) else {
                return Ok(Vec::new());
            };
            let SchedulingState::Vwap {
                last_target_fraction_submitted,
                ..
            } = &state.scheduling
            else {
                return Ok(Vec::new());
            };
            (
                state.request.symbol.clone(),
                state.request.venue,
                state.request.side,
                state.request.quantity,
                *last_target_fraction_submitted,
                state.clone_as_asset_classes(),
                state.request.leverage,
                state.request.margin_mode,
            )
        };

        if target_fraction <= already_submitted_fraction {
            return Ok(Vec::new());
        }
        let mut increment = total_quantity * (target_fraction - already_submitted_fraction);

        let mut fills = Vec::new();
        for asset_class in allocations {
            if increment.is_zero() {
                break;
            }
            let remaining_for_allocation = {
                let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
                parent_orders[&parent_id]
                    .allocations
                    .iter()
                    .find(|a| a.asset_class == asset_class)
                    .map(|a| a.quantity - a.submitted_quantity)
                    .unwrap_or(Decimal::ZERO)
            };
            let slice = increment.min(remaining_for_allocation);
            if slice.is_zero() {
                continue;
            }
            let params = SliceParams {
                symbol: symbol.clone(),
                venue,
                side,
                asset_class,
                leverage,
                margin_mode,
            };
            let (_child_id, slice_fills) = self.submit_slice(parent_id, &params, slice).await?;
            fills.extend(slice_fills);
            increment -= slice;

            let mut parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(state) = parent_orders.get_mut(&parent_id)
                && let Some(allocation) = state
                    .allocations
                    .iter_mut()
                    .find(|a| a.asset_class == asset_class)
            {
                allocation.submitted_quantity += slice;
            }
        }

        let mut parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(state) = parent_orders.get_mut(&parent_id) {
            if let SchedulingState::Vwap {
                last_target_fraction_submitted,
                ..
            } = &mut state.scheduling
            {
                *last_target_fraction_submitted = target_fraction;
            }
            if target_fraction >= Decimal::ONE
                && state
                    .allocations
                    .iter()
                    .all(|a| a.submitted_quantity >= a.quantity)
            {
                state.status = ParentOrderStatus::Completed;
            }
        }
        Ok(fills)
    }
}

#[async_trait]
impl Ems for InMemoryEms {
    async fn submit_algo_order(
        &self,
        request: AlgoOrderRequest,
    ) -> Result<ParentOrderId, EmsError> {
        let started_at = if self.causal {
            let at = (*self.event_time.lock().unwrap_or_else(|p| p.into_inner())).ok_or(
                EmsError::ParentAdmissionRejected {
                    reason: "no observed event time".into(),
                },
            )?;
            validate_causal_request(&request, at)?;
            at
        } else {
            Utc::now()
        };
        let parent_id = ParentOrderId(Uuid::new_v4());
        self.sink.reserve_parent(parent_id, &request).await?;

        let candidates = [AssetClass::Spot, AssetClass::Perpetual];
        let market_state = self.build_market_state(&candidates, &request.symbol);
        let allocations = if self.causal && request.leverage.is_some() {
            vec![state::VenueAllocation {
                asset_class: AssetClass::Perpetual,
                quantity: request.quantity,
                submitted_quantity: Decimal::ZERO,
                current_resting_child: None,
            }]
        } else {
            match routing::resolve_allocations(&request, self.scorer.as_ref(), &market_state) {
                Ok(allocations) => allocations,
                Err(error) => {
                    return self
                        .fail_parent_order(parent_id, request, error.to_string())
                        .await;
                }
            }
        };

        // Matched on a clone so the failure paths below can move
        // `request` into `fail_parent_order` while the match is still
        // borrowing the spec.
        let execution_algo = request.execution_algo.clone();
        let scheduling = match &execution_algo {
            apexes_strategy::ExecutionAlgoSpec::Twap { duration, slices } => {
                let slices = (*slices).max(1);
                let slice_interval = chrono::Duration::from_std(*duration)
                    .unwrap_or(chrono::Duration::zero())
                    / slices as i32;
                SchedulingState::Twap {
                    next_slice_at: started_at,
                    slice_interval,
                    slice_quantity: request.quantity / Decimal::from(slices),
                    slices_remaining: slices,
                }
            }
            apexes_strategy::ExecutionAlgoSpec::Vwap { lookback_days, .. } => {
                let Some(provider) = &self.volume_curve_provider else {
                    return self
                        .fail_parent_order(
                            parent_id,
                            request,
                            "no VolumeCurveProvider configured for this InMemoryEms instance"
                                .to_string(),
                        )
                        .await;
                };
                let curve = if self.causal {
                    provider
                        .intraday_volume_curve_as_of(
                            &request.symbol,
                            request.venue,
                            *lookback_days,
                            started_at,
                        )
                        .await
                        .and_then(|curve| scale_causal_curve(curve, &execution_algo))
                } else {
                    provider
                        .intraday_volume_curve(&request.symbol, request.venue, *lookback_days)
                        .await
                };
                match curve {
                    Ok(curve) => SchedulingState::Vwap {
                        curve,
                        started_at,
                        last_target_fraction_submitted: Decimal::ZERO,
                    },
                    Err(error) => {
                        return self
                            .fail_parent_order(parent_id, request, error.to_string())
                            .await;
                    }
                }
            }
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
        let ordinal = parent_orders.len();
        parent_orders.insert(
            parent_id,
            ParentOrderState {
                ordinal,
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
        let (status, children) = {
            let parent_orders = self.parent_orders.lock().unwrap_or_else(|p| p.into_inner());
            let Some(state) = parent_orders.get(&id) else {
                return Ok(None);
            };
            let children =
                if state.status == ParentOrderStatus::Completed && self.sink.defers_children() {
                    state.child_order_ids.clone()
                } else {
                    Vec::new()
                };
            (state.status.clone(), children)
        };
        // Internally Completed means the scheduling plan is exhausted.
        // Externally it must also wait for all deferred children to be terminal.
        if status == ParentOrderStatus::Completed && self.sink.defers_children() {
            for child in children {
                if !matches!(
                    self.sink.order_status(child).await?.map(|o| o.status),
                    Some(
                        apexes_core::OrderStatus::Filled
                            | apexes_core::OrderStatus::Cancelled
                            | apexes_core::OrderStatus::Rejected
                    )
                ) {
                    return Ok(Some(ParentOrderStatus::Scheduling));
                }
            }
        }
        Ok(Some(status))
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

    async fn on_market_event(&self, event: &MarketEvent) -> Result<Vec<Fill>, EmsError> {
        self.process_market_event(event, None).await
    }

    async fn on_market_event_checked(
        &self,
        event: &MarketEvent,
        check: &mut crate::SimulatedChildCheck<'_>,
    ) -> Result<Vec<Fill>, EmsError> {
        self.process_market_event(event, Some(check)).await
    }
}

impl InMemoryEms {
    async fn process_market_event(
        &self,
        event: &MarketEvent,
        check: Option<&mut crate::SimulatedChildCheck<'_>>,
    ) -> Result<Vec<Fill>, EmsError> {
        let now = if self.causal {
            match event {
                MarketEvent::Bar(bar) => bar.close_time,
                _ => event_timestamp(event),
            }
        } else {
            event_timestamp(event)
        };
        if self.causal {
            let mut clock = self.event_time.lock().unwrap_or_else(|p| p.into_inner());
            if clock.is_some_and(|at| now < at) {
                return Ok(Vec::new());
            }
            *clock = Some(now);
        }
        if let MarketEvent::Depth(update) = event {
            let mut tracker = self.market_state.lock().unwrap_or_else(|p| p.into_inner());
            for asset_class in [AssetClass::Spot, AssetClass::Perpetual] {
                tracker.record_depth(&update.symbol, asset_class, update);
            }
        }

        let mut due: Vec<ParentOrderId> = {
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

        if self.causal {
            self.sort_parents(&mut due, |id| *id);
        }
        let mut all_fills = if self.causal {
            match check {
                Some(check) => self.sink.advance_simulated_time_checked(now, check).await,
                None => self.sink.advance_simulated_time(now).await,
            }
        } else {
            Vec::new()
        };
        for parent_id in due {
            let result = self.submit_due_twap_slice(parent_id, now).await;
            self.collect_parent_result(parent_id, result, &mut all_fills)
                .await?;
        }

        all_fills.extend(self.process_iceberg_parents().await?);
        all_fills.extend(self.process_vwap_parents(now).await?);
        if let MarketEvent::Trade(trade) = event {
            all_fills.extend(self.process_pov_parents(trade).await?);
        }

        Ok(all_fills)
    }
}

mod causal;
use causal::{scale_causal_curve, validate_causal_request};

fn event_timestamp(event: &MarketEvent) -> DateTime<Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
        MarketEvent::MarkPrice(mark_price) => mark_price.timestamp,
    }
}

/// Looks up the cumulative fraction target for `elapsed` time since a
/// VWAP schedule started, per `curve`'s `cumulative_fractions` (sorted
/// ascending by offset). Returns the fraction of the LAST entry whose
/// offset is `<= elapsed` (i.e. "how far should we be by now,
/// according to history"), or `0.0` if `elapsed` precedes the curve's
/// first entry, or `1.0` if `elapsed` is past the curve's last entry
/// (the algorithm's full duration has elapsed -- submit everything
/// remaining).
pub(crate) fn target_fraction_for_elapsed(
    curve: &apexes_storage::reader::VolumeCurve,
    elapsed: chrono::Duration,
) -> Decimal {
    curve
        .cumulative_fractions
        .iter()
        .rev()
        .find(|(offset, _)| *offset <= elapsed)
        .map(|(_, fraction)| *fraction)
        .unwrap_or(Decimal::ZERO)
        .max(
            if curve
                .cumulative_fractions
                .last()
                .is_some_and(|(offset, _)| elapsed >= *offset)
            {
                Decimal::ONE
            } else {
                Decimal::ZERO
            },
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{OrderId, OrderStatus, Side, Venue};
    use apexes_storage::reader::VolumeCurve;
    use apexes_strategy::{ExecutionAlgoSpec, OrderRouting, PriceAndFeeScorer};
    use rust_decimal_macros::dec;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct RecordingSink {
        submitted: StdMutex<Vec<ChildOrderIntent>>,
        /// Fills returned (a clone of the whole Vec) from every
        /// `submit_child_order` call -- empty by default; set via
        /// `new_with_fills` for tests exercising the fill pass-through
        /// path from sink -> `on_market_event`'s return value.
        fills_to_return: Vec<Fill>,
        reject_quantity: Option<Decimal>,
    }

    impl RecordingSink {
        fn new() -> Self {
            Self {
                submitted: StdMutex::new(Vec::new()),
                fills_to_return: Vec::new(),
                reject_quantity: None,
            }
        }

        fn new_with_fills(fills_to_return: Vec<Fill>) -> Self {
            Self {
                submitted: StdMutex::new(Vec::new()),
                fills_to_return,
                reject_quantity: None,
            }
        }
    }

    #[async_trait]
    impl ChildOrderSink for RecordingSink {
        async fn submit_child_order(
            &self,
            intent: ChildOrderIntent,
        ) -> Result<(OrderId, Vec<Fill>), EmsError> {
            let reject = self.reject_quantity == Some(intent.quantity);
            self.submitted.lock().unwrap().push(intent);
            if reject {
                return Err(EmsError::ParentAdmissionRejected {
                    reason: "injected child refusal".into(),
                });
            }
            Ok((OrderId(Uuid::new_v4()), self.fills_to_return.clone()))
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
            leverage: None,
            margin_mode: None,
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
    async fn causal_parent_refusal_preserves_other_fills_and_stops_only_failed_parent() {
        let at = Utc::now();
        let synthetic_fill = Fill {
            id: apexes_core::FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            side: Side::Buy,
            price: dec!(100),
            quantity: dec!(1),
            fee: Decimal::ZERO,
            fee_currency: Some("USDT".into()),
            filled_at: at,
            leverage: None,
            margin_mode: None,
        };
        let sink = Arc::new(RecordingSink {
            reject_quantity: Some(dec!(2)),
            ..RecordingSink::new_with_fills(vec![synthetic_fill])
        });
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
        )
        .with_event_time(true);
        ems.on_market_event(&depth_event(at)).await.unwrap();
        let first = ems.submit_algo_order(sample_request()).await.unwrap();
        let mut failed = sample_request();
        failed.quantity = dec!(8);
        let failed = ems.submit_algo_order(failed).await.unwrap();
        let mut third = sample_request();
        third.quantity = dec!(12);
        let third = ems.submit_algo_order(third).await.unwrap();
        let fills = ems
            .on_market_event(&depth_event(at + chrono::Duration::seconds(1)))
            .await
            .unwrap();
        assert_eq!(fills.len(), 2);
        assert!(
            matches!(ems.parent_order_status(failed).await.unwrap(),Some(ParentOrderStatus::Failed{reason}) if reason.contains("injected child refusal"))
        );
        for id in [first, third] {
            assert_eq!(
                ems.parent_order_status(id).await.unwrap(),
                Some(ParentOrderStatus::Scheduling)
            );
        }
        let fills = ems
            .on_market_event(&depth_event(at + chrono::Duration::seconds(11)))
            .await
            .unwrap();
        assert_eq!(fills.len(), 2);
        assert_eq!(
            sink.submitted
                .lock()
                .unwrap()
                .iter()
                .map(|i| i.quantity)
                .collect::<Vec<_>>(),
            vec![dec!(1), dec!(2), dec!(3), dec!(1), dec!(3)]
        );
    }

    #[tokio::test]
    async fn causal_twap_uses_historical_clock_and_admission_order() {
        use chrono::TimeZone;
        let sink = Arc::new(RecordingSink::new());
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
        )
        .with_event_time(true);
        let at = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        assert!(ems.submit_algo_order(sample_request()).await.is_err());
        ems.on_market_event(&depth_event(at)).await.unwrap();
        let first = ems.submit_algo_order(sample_request()).await.unwrap();
        let mut other = sample_request();
        other.quantity = dec!(8);
        let second = ems.submit_algo_order(other).await.unwrap();
        ems.on_market_event(&depth_event(at - chrono::Duration::seconds(1)))
            .await
            .unwrap();
        assert!(sink.submitted.lock().unwrap().is_empty());
        for offset in [1, 5, 11, 21, 31] {
            ems.on_market_event(&depth_event(at + chrono::Duration::seconds(offset)))
                .await
                .unwrap();
        }
        let quantities = sink
            .submitted
            .lock()
            .unwrap()
            .iter()
            .map(|c| c.quantity)
            .collect::<Vec<_>>();
        assert_eq!(
            quantities,
            vec![
                dec!(1),
                dec!(2),
                dec!(1),
                dec!(2),
                dec!(1),
                dec!(2),
                dec!(1),
                dec!(2)
            ]
        );
        for id in [first, second] {
            assert_eq!(
                ems.parent_order_status(id).await.unwrap(),
                Some(ParentOrderStatus::Completed)
            );
        }
    }

    #[tokio::test]
    async fn causal_pov_rejects_other_venues_and_regressing_trades() {
        use chrono::TimeZone;
        let sink = Arc::new(RecordingSink::new());
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
        )
        .with_event_time(true);
        let at = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        ems.on_market_event(&depth_event(at)).await.unwrap();
        assert!(ems.submit_algo_order(pov_request(dec!(1.1))).await.is_err());
        let id = ems.submit_algo_order(pov_request(dec!(0.1))).await.unwrap();
        let MarketEvent::Trade(mut wrong) = trade_event("BTCUSDT", dec!(10), at) else {
            panic!("fixture")
        };
        wrong.venue = Venue::Okx;
        ems.on_market_event(&MarketEvent::Trade(wrong))
            .await
            .unwrap();
        ems.on_market_event(&trade_event(
            "BTCUSDT",
            dec!(10),
            at - chrono::Duration::seconds(1),
        ))
        .await
        .unwrap();
        ems.on_market_event(&trade_event("BTCUSDT", dec!(-1), at))
            .await
            .unwrap();
        assert!(sink.submitted.lock().unwrap().is_empty());
        ems.on_market_event(&trade_event(
            "BTCUSDT",
            dec!(10),
            at + chrono::Duration::seconds(1),
        ))
        .await
        .unwrap();
        assert_eq!(sink.submitted.lock().unwrap()[0].quantity, dec!(1));
        ems.cancel_parent_order(id).await.unwrap();
        ems.on_market_event(&trade_event(
            "BTCUSDT",
            dec!(10),
            at + chrono::Duration::seconds(2),
        ))
        .await
        .unwrap();
        assert_eq!(sink.submitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn causal_vwap_uses_as_of_curve_and_requested_duration() {
        use chrono::TimeZone;
        struct CausalCurve(DateTime<Utc>);
        #[async_trait]
        impl VolumeCurveProvider for CausalCurve {
            async fn intraday_volume_curve(
                &self,
                _: &str,
                _: Venue,
                _: u32,
            ) -> Result<VolumeCurve, EmsError> {
                panic!("noncausal provider called")
            }
            async fn intraday_volume_curve_as_of(
                &self,
                _: &str,
                _: Venue,
                days: u32,
                at: DateTime<Utc>,
            ) -> Result<VolumeCurve, EmsError> {
                assert_eq!(at, self.0);
                assert_eq!(days, 5);
                Ok(VolumeCurve {
                    cumulative_fractions: vec![
                        (chrono::Duration::zero(), dec!(0.25)),
                        (chrono::Duration::hours(12), dec!(0.5)),
                        (chrono::Duration::hours(24), Decimal::ONE),
                    ],
                })
            }
        }
        let at = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let sink = Arc::new(RecordingSink::new());
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            Some(Arc::new(CausalCurve(at))),
        )
        .with_event_time(true);
        ems.on_market_event(&depth_event(at)).await.unwrap();
        let id = ems.submit_algo_order(vwap_request()).await.unwrap();
        for offset in [1, 1799, 1800, 3600] {
            ems.on_market_event(&depth_event(at + chrono::Duration::seconds(offset)))
                .await
                .unwrap();
        }
        assert_eq!(
            sink.submitted
                .lock()
                .unwrap()
                .iter()
                .map(|c| c.quantity)
                .collect::<Vec<_>>(),
            vec![dec!(2.5), dec!(2.5), dec!(5)]
        );
        assert_eq!(
            ems.parent_order_status(id).await.unwrap(),
            Some(ParentOrderStatus::Completed)
        );
        let legacy = InMemoryEms::new(
            sink,
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            Some(Arc::new(AlwaysInsufficientHistory)),
        )
        .with_event_time(true);
        legacy.on_market_event(&depth_event(at)).await.unwrap();
        assert!(legacy.submit_algo_order(vwap_request()).await.is_err());
    }

    #[tokio::test]
    async fn twap_submits_one_slice_immediately_on_submission() {
        let sink = Arc::new(RecordingSink::new());
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
        );
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
    async fn twap_slice_fills_are_returned_from_on_market_event() {
        let synthetic_fill = Fill {
            id: apexes_core::FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            price: dec!(65010),
            quantity: dec!(1),
            fee: dec!(0),
            fee_currency: Some("USDT".to_string()),
            filled_at: Utc::now(),
            leverage: None,
            margin_mode: None,
        };
        let sink = Arc::new(RecordingSink::new_with_fills(vec![synthetic_fill]));
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
        );
        ems.submit_algo_order(sample_request()).await.unwrap();

        // The first TWAP slice is due immediately (see
        // twap_submits_one_slice_immediately_on_submission) -- the fills
        // the sink returns for that slice must come back out of
        // on_market_event's own return value, not be discarded.
        let fills = ems.on_market_event(&depth_event(Utc::now())).await.unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].symbol, "BTCUSDT");
        assert_eq!(fills[0].quantity, dec!(1));
        assert_eq!(fills[0].price, dec!(65010));
    }

    #[tokio::test]
    async fn twap_completes_after_all_slices_are_submitted_across_advancing_virtual_time() {
        let sink = Arc::new(RecordingSink::new());
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
        );
        let parent_id = ems.submit_algo_order(sample_request()).await.unwrap();

        let mut now = Utc::now();
        // 4 slices, 10s apart (40s / 4) -- advance virtual time in 10s
        // steps, calling on_market_event once per step, exactly as
        // the shared unified event loop would for a sequence of real
        // MarketEvents.
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
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
        );
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

    /// Extends RecordingSink with settable order statuses (needed for
    /// Iceberg's order_status-polling refill logic) and a submission
    /// counter (needed to assert "no more slices submitted after
    /// completion").
    struct SteppableSink {
        submitted: StdMutex<Vec<ChildOrderIntent>>,
        statuses: StdMutex<HashMap<OrderId, OrderStatus>>,
        next_id: AtomicU32,
        last_id: StdMutex<Option<OrderId>>,
    }

    impl SteppableSink {
        fn new() -> Self {
            Self {
                submitted: StdMutex::new(Vec::new()),
                statuses: StdMutex::new(HashMap::new()),
                next_id: AtomicU32::new(0),
                last_id: StdMutex::new(None),
            }
        }

        fn set_status(&self, order_id: OrderId, status: OrderStatus) {
            self.statuses.lock().unwrap().insert(order_id, status);
        }

        fn last_submitted_id(&self) -> OrderId {
            self.last_id
                .lock()
                .unwrap()
                .expect("no order submitted yet")
        }
    }

    #[async_trait]
    impl ChildOrderSink for SteppableSink {
        async fn submit_child_order(
            &self,
            intent: ChildOrderIntent,
        ) -> Result<(OrderId, Vec<Fill>), EmsError> {
            self.submitted.lock().unwrap().push(intent);
            let n = self.next_id.fetch_add(1, Ordering::SeqCst);
            let id = OrderId(Uuid::from_u128(n as u128 + 1));
            *self.last_id.lock().unwrap() = Some(id);
            Ok((id, Vec::new()))
        }

        async fn cancel_child_order(&self, _order_id: OrderId) -> Result<(), EmsError> {
            Ok(())
        }

        async fn order_status(
            &self,
            order_id: OrderId,
        ) -> Result<Option<apexes_core::Order>, EmsError> {
            let status = self.statuses.lock().unwrap().get(&order_id).copied();
            Ok(status.map(|status| apexes_core::Order {
                id: order_id,
                exchange_order_id: None,
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: apexes_core::OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
                leverage: None,
                margin_mode: None,
                filled_quantity: dec!(1),
                status,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            }))
        }
    }

    fn iceberg_request(visible_qty: Decimal, quantity: Decimal) -> AlgoOrderRequest {
        AlgoOrderRequest {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            quantity,
            execution_algo: ExecutionAlgoSpec::Iceberg { visible_qty },
            routing: OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    #[tokio::test]
    async fn iceberg_submits_one_visible_slice_then_waits_for_it_to_fill_before_refilling() {
        let sink = Arc::new(SteppableSink::new());
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
        );
        let parent_id = ems
            .submit_algo_order(iceberg_request(dec!(1), dec!(3)))
            .await
            .unwrap();

        ems.on_market_event(&depth_event(Utc::now())).await.unwrap();
        assert_eq!(sink.submitted.lock().unwrap().len(), 1);

        // Second call before the resting slice fills -- no new slice.
        ems.on_market_event(&depth_event(Utc::now())).await.unwrap();
        assert_eq!(sink.submitted.lock().unwrap().len(), 1);

        // Mark the resting child Filled -- next call should refill.
        sink.set_status(sink.last_submitted_id(), OrderStatus::Filled);
        ems.on_market_event(&depth_event(Utc::now())).await.unwrap();
        assert_eq!(sink.submitted.lock().unwrap().len(), 2);

        assert_eq!(
            ems.parent_order_status(parent_id).await.unwrap(),
            Some(ParentOrderStatus::Scheduling)
        );
    }

    #[tokio::test]
    async fn iceberg_completes_once_all_quantity_submitted_and_last_slice_fills() {
        let sink = Arc::new(SteppableSink::new());
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
        );
        let _parent_id = ems
            .submit_algo_order(iceberg_request(dec!(2), dec!(2)))
            .await
            .unwrap();

        ems.on_market_event(&depth_event(Utc::now())).await.unwrap();
        assert_eq!(sink.submitted.lock().unwrap().len(), 1);
        assert_eq!(sink.submitted.lock().unwrap()[0].quantity, dec!(2));

        sink.set_status(sink.last_submitted_id(), OrderStatus::Filled);
        ems.on_market_event(&depth_event(Utc::now())).await.unwrap();

        // Fully submitted (2 of 2) and the resting child is terminal ->
        // no further slice submitted, parent marked Completed.
        assert_eq!(sink.submitted.lock().unwrap().len(), 1);
    }

    fn pov_request(participation_rate: Decimal) -> AlgoOrderRequest {
        AlgoOrderRequest {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            quantity: dec!(100),
            execution_algo: ExecutionAlgoSpec::Pov { participation_rate },
            routing: OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    fn trade_event(symbol: &str, quantity: Decimal, now: DateTime<Utc>) -> MarketEvent {
        MarketEvent::Trade(apexes_core::TradeEvent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity,
            side: None,
            trade_id: None,
            timestamp: now,
        })
    }

    #[tokio::test]
    async fn pov_submits_a_fraction_of_each_observed_trades_quantity() {
        let sink = Arc::new(RecordingSink::new());
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
        );
        ems.submit_algo_order(pov_request(dec!(0.1))).await.unwrap();

        ems.on_market_event(&trade_event("BTCUSDT", dec!(10), Utc::now()))
            .await
            .unwrap();

        assert_eq!(sink.submitted.lock().unwrap().len(), 1);
        assert_eq!(sink.submitted.lock().unwrap()[0].quantity, dec!(1));
    }

    #[tokio::test]
    async fn pov_ignores_trades_for_a_different_symbol() {
        let sink = Arc::new(RecordingSink::new());
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
        );
        ems.submit_algo_order(pov_request(dec!(0.1))).await.unwrap();

        ems.on_market_event(&trade_event("ETHUSDT", dec!(10), Utc::now()))
            .await
            .unwrap();

        assert_eq!(sink.submitted.lock().unwrap().len(), 0);
    }

    struct AlwaysInsufficientHistory;

    #[async_trait]
    impl VolumeCurveProvider for AlwaysInsufficientHistory {
        async fn intraday_volume_curve(
            &self,
            _symbol: &str,
            _venue: Venue,
            _lookback_days: u32,
        ) -> Result<VolumeCurve, EmsError> {
            Err(EmsError::InsufficientVwapHistory {
                reason: "only 1 of 5 requested days available".to_string(),
            })
        }
    }

    fn vwap_request() -> AlgoOrderRequest {
        AlgoOrderRequest {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            quantity: dec!(10),
            execution_algo: ExecutionAlgoSpec::Vwap {
                duration: std::time::Duration::from_secs(3600),
                lookback_days: 5,
            },
            routing: OrderRouting::Fixed,
            leverage: None,
            margin_mode: None,
        }
    }

    #[tokio::test]
    async fn vwap_fails_the_parent_order_when_history_is_insufficient() {
        let sink = Arc::new(RecordingSink::new());
        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            None,
        );
        let parent_id = ems.submit_algo_order(vwap_request()).await.unwrap();

        match ems.parent_order_status(parent_id).await.unwrap() {
            Some(ParentOrderStatus::Failed { reason }) => {
                assert!(reason.contains("VolumeCurveProvider") || reason.contains("history"));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
        assert_eq!(sink.submitted.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn vwap_paces_submission_against_the_curves_cumulative_fractions() {
        let sink = Arc::new(RecordingSink::new());
        let provider = Arc::new(AlwaysInsufficientHistory);
        // Deliberately unused for the success path below -- swapped for
        // a curve-returning provider inline via a second small mock.
        let _ = provider;

        struct FixedCurveProvider;
        #[async_trait]
        impl VolumeCurveProvider for FixedCurveProvider {
            async fn intraday_volume_curve(
                &self,
                _symbol: &str,
                _venue: Venue,
                _lookback_days: u32,
            ) -> Result<VolumeCurve, EmsError> {
                Ok(VolumeCurve {
                    cumulative_fractions: vec![
                        (chrono::Duration::zero(), dec!(0.5)),
                        (chrono::Duration::seconds(60), dec!(1.0)),
                    ],
                })
            }
        }

        let ems = InMemoryEms::new(
            sink.clone(),
            Arc::new(PriceAndFeeScorer),
            HashMap::new(),
            Some(Arc::new(FixedCurveProvider)),
        );
        let parent_id = ems.submit_algo_order(vwap_request()).await.unwrap();

        let started_at = Utc::now();
        // At t=0, target fraction is already 0.5 (first curve entry's
        // offset is zero) -- expect an immediate first submission of
        // half the total quantity.
        ems.on_market_event(&depth_event(started_at)).await.unwrap();
        assert_eq!(sink.submitted.lock().unwrap().len(), 1);
        assert_eq!(sink.submitted.lock().unwrap()[0].quantity, dec!(5));

        // At t=60s, target fraction is 1.0 -- expect the remaining half.
        ems.on_market_event(&depth_event(started_at + chrono::Duration::seconds(60)))
            .await
            .unwrap();
        assert_eq!(sink.submitted.lock().unwrap().len(), 2);
        assert_eq!(sink.submitted.lock().unwrap()[1].quantity, dec!(5));

        assert_eq!(
            ems.parent_order_status(parent_id).await.unwrap(),
            Some(ParentOrderStatus::Completed)
        );
    }
}
