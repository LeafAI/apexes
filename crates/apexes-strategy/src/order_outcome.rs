//! F04.04 — Direct strategy order attempt evidence. These notifications
//! are not exchange reconciliation or a complete order lifecycle journal.
use crate::{OrderIntent, Strategy};
use apexes_core::Order;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptStage {
    Decision,
    Arrival,
}

#[derive(Debug, Clone, Copy)]
pub enum OrderOutcomeKind<'a> {
    /// Rejected locally, before the executor was invoked.
    Rejected { code: &'static str },
    /// Accepted into the event-time transit queue, not submitted.
    Scheduled,
    /// The executor returned an order. Inspect its actual status;
    /// even an Ok return need not represent an accepted working order.
    Returned { order: &'a Order },
    /// No successful executor result; exchange acceptance may be unknown.
    SubmissionFailed { code: &'static str },
}

#[derive(Debug, Clone, Copy)]
pub struct OrderOutcome<'a> {
    pub at: DateTime<Utc>,
    pub stage: AttemptStage,
    /// Original intent on rule failure; otherwise the conformed intent.
    pub intent: &'a OrderIntent,
    pub kind: OrderOutcomeKind<'a>,
}

/// Direct strategy attempts only. EMS parent/child and automatic
/// liquidation paths have separate lifecycle semantics.
pub fn notify_direct(
    strategy: &mut impl Strategy,
    intent: &OrderIntent,
    at: DateTime<Utc>,
    stage: AttemptStage,
    kind: OrderOutcomeKind<'_>,
) {
    if intent.execution_algo.is_none() {
        strategy.on_order_outcome(&OrderOutcome {
            at,
            stage,
            intent,
            kind,
        });
    }
}
