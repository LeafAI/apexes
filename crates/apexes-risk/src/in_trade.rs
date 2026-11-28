//! [`InTradeMonitor`]: called after a `Fill` is applied to the ledger,
//! to catch violations that only become visible from *actual* fills
//! rather than a single proposed order (e.g. several partial fills
//! cumulatively pushing a position over its limit, which no single
//! `PreTradeCheck` call could have seen in advance since each
//! individual partial fill's originating order may have looked fine in
//! isolation).

use apexes_core::Fill;
use apexes_strategy::Position;
use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::error::RiskViolation;

pub trait InTradeMonitor: Send {
    /// Read-only inspection; custom implementations default to explicit
    /// unsupported coverage rather than an inferred empty state.
    /// Actual built-in shared monitor, when supported. No serialized
    /// address or caller-chosen identity is retained.
    fn shared_monitor(&self) -> Option<crate::SharedMonitorRef<'_>> {
        None
    }

    fn inspect_registered_state(
        &self,
    ) -> Result<crate::RegisteredRiskState, crate::RiskStateError> {
        Ok(crate::RegisteredRiskState::Unsupported)
    }

    /// Returns `Ok(())` if the post-fill state passes this monitor, or
    /// `Err(violations)` describing what was breached. Implementations
    /// that carry mutable state (e.g. `CircuitBreaker`) update that
    /// state as part of this call -- `on_fill` takes `&mut self`.
    fn on_fill(
        &mut self,
        fill: &Fill,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Result<(), Vec<RiskViolation>>;
}
