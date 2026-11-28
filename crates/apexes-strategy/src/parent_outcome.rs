//! F04.04 — Observed EMS parent lifecycle, not child fill attribution.
use crate::{OrderIntent, ParentOrderId, ParentOrderStatus};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentPhase {
    Scheduling,
    Completed,
    Cancelled,
    Failed,
}

impl From<&ParentOrderStatus> for ParentPhase {
    fn from(status: &ParentOrderStatus) -> Self {
        match status {
            ParentOrderStatus::Scheduling => Self::Scheduling,
            ParentOrderStatus::Completed => Self::Completed,
            ParentOrderStatus::Cancelled => Self::Cancelled,
            ParentOrderStatus::Failed { .. } => Self::Failed,
        }
    }
}

impl ParentPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scheduling => "scheduling",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ParentOutcomeKind<'a> {
    /// EMS returned an identity; actual child execution is separate.
    Submitted {
        id: ParentOrderId,
        intent: &'a OrderIntent,
    },
    /// Submission error does not establish absence of a remote parent.
    SubmissionFailed { intent: &'a OrderIntent },
    /// Newly observed aggregate phase, not cumulative executed quantity.
    StatusChanged {
        id: ParentOrderId,
        previous: ParentPhase,
        current: ParentPhase,
    },
    /// EMS explicitly returned None. This is not a terminal phase.
    Unavailable { id: ParentOrderId },
}

#[derive(Debug, Clone, Copy)]
pub struct ParentOutcome<'a> {
    pub at: DateTime<Utc>,
    pub kind: ParentOutcomeKind<'a>,
}
