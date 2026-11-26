//! Internal per-parent-order scheduling state for `InMemoryEms`.
//! Not part of this crate's public API (kept private to `in_memory`).

use apexes_core::{AssetClass, OrderId};
use apexes_strategy::ParentOrderStatus;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::ems::AlgoOrderRequest;

/// Per-`AssetClass` allocation of a parent order's total quantity,
/// decided once at submission time by routing (`Fixed`/`RouteToBest`/
/// `Split`) and never recomputed afterward -- mid-flight re-routing is
/// out of scope (design spec's "not included" list: no dynamic
/// `ExecutionAlgoSpec`/routing changes after submission).
#[derive(Clone)]
pub(crate) struct VenueAllocation {
    pub asset_class: AssetClass,
    pub quantity: Decimal,
    pub submitted_quantity: Decimal,
    /// Iceberg only: the child order currently resting for this
    /// allocation, if any. `None` means "no resting order -- safe to
    /// submit the next visible slice".
    pub current_resting_child: Option<OrderId>,
}

impl VenueAllocation {
    /// Remaining quantity not yet submitted for this allocation.
    pub fn cloned_summary(&self) -> (Decimal, Decimal) {
        (self.quantity - self.submitted_quantity, self.quantity)
    }
}

pub(crate) struct ParentOrderState {
    pub request: AlgoOrderRequest,
    pub ordinal: usize,
    pub status: ParentOrderStatus,
    /// One entry per venue this parent order was routed to (one entry
    /// for `Fixed`/`RouteToBest`, one per candidate for `Split`).
    pub allocations: Vec<VenueAllocation>,
    pub child_order_ids: Vec<OrderId>,
    /// Algorithm-specific scheduling state, populated based on
    /// `request.execution_algo`'s variant.
    pub scheduling: SchedulingState,
}

impl ParentOrderState {
    /// The `AssetClass` of every venue allocation this parent order has,
    /// as a plain `Vec` (used by iteration sites that need to release
    /// the parent-order lock before making further per-allocation calls).
    pub fn clone_as_asset_classes(&self) -> Vec<AssetClass> {
        self.allocations.iter().map(|a| a.asset_class).collect()
    }
}

pub(crate) enum SchedulingState {
    Twap {
        next_slice_at: DateTime<Utc>,
        slice_interval: chrono::Duration,
        slice_quantity: Decimal,
        slices_remaining: u32,
    },
    Vwap {
        curve: apexes_storage::reader::VolumeCurve,
        started_at: DateTime<Utc>,
        /// The highest cumulative-fraction target this schedule has
        /// already submitted up through -- used so `on_market_event`
        /// only submits the INCREMENTAL quantity needed to catch up to
        /// the curve's current target fraction, not the whole target
        /// fraction again every call.
        last_target_fraction_submitted: Decimal,
    },
    Iceberg {
        visible_qty: Decimal,
    },
    Pov {
        participation_rate: Decimal,
    },
}

/// Tracks the most recently observed best bid/ask per `(symbol,
/// asset_class)`, fed by `MarketEvent::Depth` events via
/// `InMemoryEms::on_market_event` -- used to build a `VenueMarketState`
/// for `VenueScorer::score` when a parent order needs `RouteToBest`/
/// `Split` routing decisions. There is no fee-schedule dependency here
/// (`apexes-ems` cannot depend on `apexes-backtest`'s `CostModel`) --
/// `InMemoryEms::new` takes a fixed `estimated_fee_bps` map supplied by
/// the caller instead (see Step 2).
#[derive(Default)]
pub(crate) struct MarketStateTracker {
    pub best_bid_ask: HashMap<(String, AssetClass), (Decimal, Decimal)>,
}

impl MarketStateTracker {
    pub fn record_depth(
        &mut self,
        symbol: &str,
        asset_class: AssetClass,
        update: &apexes_core::DepthUpdate,
    ) {
        let best_bid = update.bids.first().map(|level| level.price);
        let best_ask = update.asks.first().map(|level| level.price);
        if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
            self.best_bid_ask
                .insert((symbol.to_string(), asset_class), (bid, ask));
        }
    }
}
