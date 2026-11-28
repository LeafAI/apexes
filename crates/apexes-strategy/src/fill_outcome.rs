//! F04.04 — Locally applied fill notifications, not order completion or
//! exchange reconciliation attestations.
use apexes_core::Fill;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillSource {
    Direct,
    Resting,
    Ems,
    Liquidation,
    Derisk,
    Supplemental,
}

impl FillSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Resting => "resting",
            Self::Ems => "ems",
            Self::Liquidation => "liquidation",
            Self::Derisk => "derisk",
            Self::Supplemental => "supplemental",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FillOutcome<'a> {
    /// Known local decision identity; absent for uncorrelated evidence.
    pub intent_id: Option<crate::IntentId>,
    /// Explicit EMS ownership, if known. Not inferred from delivery path.
    pub parent_id: Option<crate::ParentOrderId>,
    /// Cumulative locally notified quantity for this parent, not scheduled quantity.
    pub parent_executed_quantity: Option<rust_decimal::Decimal>,
    /// Local delivery/decision clock; not the execution timestamp.
    pub at: DateTime<Utc>,
    pub source: FillSource,
    pub fill: &'a Fill,
}
