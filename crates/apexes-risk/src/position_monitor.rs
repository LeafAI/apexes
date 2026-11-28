//! [`PositionMonitor`]: called once per event-loop tick (not tied to
//! fills, unlike `InTradeMonitor`) with the latest margin state of
//! every leveraged position, so implementations (e.g. `MarginMonitor`)
//! can react to margin deterioration caused purely by mark-price
//! movement -- which `InTradeMonitor::on_fill` alone cannot observe,
//! since it only fires after a NEW fill.

use std::collections::HashMap;

use apexes_strategy::PositionMarginStatus;

use crate::error::RiskViolation;

pub trait PositionMonitor: Send + Sync {
    fn inspect_registered_state(
        &self,
    ) -> Result<crate::RegisteredRiskState, crate::RiskStateError> {
        Ok(crate::RegisteredRiskState::Unsupported)
    }

    fn on_tick(
        &mut self,
        margin_status: &HashMap<String, PositionMarginStatus>,
    ) -> Result<(), Vec<RiskViolation>>;
}
