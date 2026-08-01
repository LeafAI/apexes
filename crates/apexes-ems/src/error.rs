use apexes_core::OrderId;
use apexes_strategy::ParentOrderId;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EmsError {
    #[error("registry lookup failed: {0}")]
    RegistryLookupFailed(#[from] apexes_core::registry::RegistryError),

    #[error("failed to connect to PostgreSQL: {reason}")]
    ConnectionFailed { reason: String },

    #[error("database query failed: {reason}")]
    QueryFailed { reason: String },

    #[error("parent order {id:?} not found")]
    ParentOrderNotFound { id: ParentOrderId },

    #[error("insufficient historical data for VWAP scheduling: {reason}")]
    InsufficientVwapHistory { reason: String },

    #[error("child order submission failed for parent {parent_id:?}: {reason}")]
    ChildOrderSubmissionFailed {
        parent_id: ParentOrderId,
        reason: String,
    },

    #[error("child order {order_id:?} not found")]
    ChildOrderNotFound { order_id: OrderId },
}

impl From<sqlx::Error> for EmsError {
    fn from(error: sqlx::Error) -> Self {
        EmsError::QueryFailed {
            reason: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn parent_order_not_found_display() {
        let err = EmsError::ParentOrderNotFound {
            id: ParentOrderId(Uuid::nil()),
        };
        assert!(err.to_string().contains("not found"));
    }
}
