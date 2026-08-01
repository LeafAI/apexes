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
pub(crate) struct VenueAllocation {
    pub asset_class: AssetClass,
    pub quantity: Decimal,
    pub submitted_quantity: Decimal,
}

pub(crate) struct ParentOrderState {
    pub request: AlgoOrderRequest,
    pub status: ParentOrderStatus,
    /// One entry per venue this parent order was routed to (one entry
    /// for `Fixed`/`RouteToBest`, one per candidate for `Split`).
    pub allocations: Vec<VenueAllocation>,
    pub child_order_ids: Vec<OrderId>,
    /// Algorithm-specific scheduling state, populated based on
    /// `request.execution_algo`'s variant -- see Task 14 for
    /// `Vwap`/`Iceberg`/`Pov` fields; this task only populates `Twap`'s.
    pub scheduling: SchedulingState,
}

pub(crate) enum SchedulingState {
    Twap {
        next_slice_at: DateTime<Utc>,
        slice_interval: chrono::Duration,
        slice_quantity: Decimal,
        slices_remaining: u32,
    },
    /// Populated by Task 14 (constructed there; until then, dead code
    /// by design -- the variant exists so Task 14 only fills in fields
    /// rather than reshaping this enum).
    #[allow(dead_code)]
    Vwap {
        deadline: DateTime<Utc>,
        started_at: DateTime<Utc>,
        curve: apexes_storage::reader::VolumeCurve,
        last_target_fraction_submitted: Decimal,
    },
    /// Populated by Task 14 (see `Vwap`'s note).
    #[allow(dead_code)]
    Iceberg { visible_qty: Decimal },
    /// Populated by Task 14 (see `Vwap`'s note).
    #[allow(dead_code)]
    Pov { participation_rate: Decimal },
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
