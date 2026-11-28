//! F04.03 — Read-only OMS submission-gate observation, not an admission
//! decision or a comprehensive exchange-health assessment.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthPhase {
    Initializing,
    Reconciling,
    Running,
    Degraded,
    Halted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthObservation {
    /// None means the kill switch exists but managed readiness is absent.
    pub phase: Option<HealthPhase>,
    pub kill_switch_engaged: bool,
    pub blocks_submissions: bool,
}
