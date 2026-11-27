//! F03.05: realized closing slices classified by their actual execution path.
use apexes_core::{Fill, FillId};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitReason {
    Strategy,
    Resting,
    Ems,
    Liquidation,
    Derisk,
}

impl ExitReason {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Strategy => "strategy",
            Self::Resting => "resting",
            Self::Ems => "ems",
            Self::Liquidation => "liquidation",
            Self::Derisk => "derisk",
        }
    }
}

/// A reducing portion, not necessarily a full position exit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosingExecution {
    pub fill_id: FillId,
    pub filled_at: DateTime<Utc>,
    pub reason: ExitReason,
    pub closed_quantity: Decimal,
    pub allocated_entry_fee: Decimal,
    pub allocated_exit_fee: Decimal,
    pub gross_realized: Decimal,
    pub net_realized: Decimal,
}

pub(crate) struct ClosingSlice {
    pub quantity: Decimal,
    pub entry_fee: Decimal,
    pub exit_fee: Decimal,
    pub gross: Decimal,
    pub net: Decimal,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitTotals {
    pub closing_count: u64,
    pub closed_quantity: Decimal,
    pub allocated_entry_fees: Decimal,
    pub allocated_exit_fees: Decimal,
    pub gross_realized: Decimal,
    pub net_realized: Decimal,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitAttribution {
    pub closes: Vec<ClosingExecution>,
    pub by_reason: BTreeMap<ExitReason, ExitTotals>,
}

impl ExitAttribution {
    pub(crate) fn record(
        &mut self,
        fill: &Fill,
        reason: ExitReason,
        slice: ClosingSlice,
    ) -> Option<()> {
        let totals = self.by_reason.entry(reason).or_default();
        totals.closing_count += 1;
        totals.closed_quantity = totals.closed_quantity.checked_add(slice.quantity)?;
        totals.allocated_entry_fees = totals.allocated_entry_fees.checked_add(slice.entry_fee)?;
        totals.allocated_exit_fees = totals.allocated_exit_fees.checked_add(slice.exit_fee)?;
        totals.gross_realized = totals.gross_realized.checked_add(slice.gross)?;
        totals.net_realized = totals.net_realized.checked_add(slice.net)?;
        self.closes.push(ClosingExecution {
            fill_id: fill.id,
            filled_at: fill.filled_at,
            reason,
            closed_quantity: slice.quantity,
            allocated_entry_fee: slice.entry_fee,
            allocated_exit_fee: slice.exit_fee,
            gross_realized: slice.gross,
            net_realized: slice.net,
        });
        Some(())
    }
}
