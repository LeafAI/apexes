//! F04.04 — Explicit terminal evidence for simulated orders and transit
//! intents. Missing open orders alone are never terminal evidence.
use crate::OrderIntent;
use apexes_core::{Order, OrderId};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy)]
pub enum TerminalKind<'a> {
    /// Previously working order whose recorded fill quantity reached its quantity.
    Filled { order: &'a Order },
    /// Actual ID returned by the simulator's successful cancellation operation.
    Cancelled { order_id: OrderId },
    /// Unsubmitted intent removed from transit by emergency preemption.
    TransitCancelled { intent: &'a OrderIntent },
    /// Unsubmitted intent still in transit at session termination.
    TransitExpired { intent: &'a OrderIntent },
}

#[derive(Debug, Clone, Copy)]
pub struct TerminalOutcome<'a> {
    pub at: DateTime<Utc>,
    pub kind: TerminalKind<'a>,
}
