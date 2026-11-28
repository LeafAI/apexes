//! F04.04 — Explicit terminal evidence for live and simulated orders and transit
//! intents. Missing open orders alone are never terminal evidence.
use crate::{OrderIntent, ParentOrderId, PersistedFillLink};
use apexes_core::{Order, OrderId};
use chrono::{DateTime, Utc};

/// A positively reconciled exchange terminal status, not a local refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveTerminalStatus {
    Filled,
    Cancelled,
    Rejected,
}

/// An actual persisted OMS terminal order, with its immutable linkage.
/// Consumed by the unified loop only after the execution watermark is ready.
#[derive(Debug, Clone)]
pub struct LiveTerminalEvidence {
    pub order: Order,
    pub status: LiveTerminalStatus,
    pub link: PersistedFillLink,
}

#[derive(Debug, Clone, Copy)]
pub enum TerminalKind<'a> {
    /// Previously working order whose recorded fill quantity reached its quantity.
    Filled { order: &'a Order },
    /// Actual ID returned by the simulator's successful cancellation operation.
    Cancelled { order_id: OrderId },
    /// Positive durable exchange evidence, after its locally applied fills.
    Live {
        order: &'a Order,
        status: LiveTerminalStatus,
        parent_id: Option<ParentOrderId>,
        dispatch_id: Option<uuid::Uuid>,
    },
    /// Unsubmitted intent removed from transit by emergency preemption.
    TransitCancelled { intent: &'a OrderIntent },
    /// Unsubmitted intent still in transit at session termination.
    TransitExpired { intent: &'a OrderIntent },
}

#[derive(Debug, Clone, Copy)]
pub struct TerminalOutcome<'a> {
    /// Known local decision identity; absent for uncorrelated evidence.
    pub intent_id: Option<crate::IntentId>,
    pub at: DateTime<Utc>,
    pub kind: TerminalKind<'a>,
}
