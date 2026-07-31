//! Error type for `apexes-oms`.

use apexes_core::{OrderId, OrderStatus};
use thiserror::Error;

/// Errors returned by `OrderStore`/`OrderManager`.
#[derive(Debug, Error)]
pub enum OmsError {
    #[error("failed to connect to PostgreSQL: {reason}")]
    ConnectionFailed { reason: String },

    #[error("database query failed: {reason}")]
    QueryFailed { reason: String },

    #[error("failed to convert database row: {reason}")]
    InvalidRow { reason: String },

    #[error("order rejected by risk checker: {violations:?}")]
    RiskCheckFailed {
        violations: Vec<apexes_risk::RiskViolation>,
    },

    #[error("order rejected by exchange: {reason}")]
    OrderRejected { reason: String },

    #[error("order {order_id:?} not found")]
    OrderNotFound { order_id: OrderId },

    #[error("order {order_id:?} cannot be cancelled in status {current_status:?}")]
    OrderNotCancellable {
        order_id: OrderId,
        current_status: OrderStatus,
    },

    #[error(
        "order {order_id:?} has no known exchange_order_id yet (submission outcome was Uncertain) -- wait for reconcile() to resolve it before cancelling"
    )]
    ExchangeOrderIdUnknown { order_id: OrderId },
}

impl From<sqlx::Error> for OmsError {
    fn from(error: sqlx::Error) -> Self {
        OmsError::QueryFailed {
            reason: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn order_not_found_display() {
        let err = OmsError::OrderNotFound {
            order_id: OrderId(Uuid::nil()),
        };
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn order_not_cancellable_display() {
        let err = OmsError::OrderNotCancellable {
            order_id: OrderId(Uuid::nil()),
            current_status: OrderStatus::Filled,
        };
        assert!(err.to_string().contains("cannot be cancelled"));
        assert!(err.to_string().contains("Filled"));
    }

    #[test]
    fn exchange_order_id_unknown_display() {
        let err = OmsError::ExchangeOrderIdUnknown {
            order_id: OrderId(Uuid::nil()),
        };
        assert!(err.to_string().contains("Uncertain"));
    }
}
