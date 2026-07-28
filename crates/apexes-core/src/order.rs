use crate::error::ApexesError;
use serde::{Deserialize, Serialize};

/// Lifecycle state of an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

impl OrderStatus {
    /// Returns true if `self -> next` is a valid transition.
    pub fn can_transition_to(&self, next: OrderStatus) -> bool {
        use OrderStatus::*;
        matches!(
            (self, next),
            (New, PartiallyFilled)
                | (New, Filled)
                | (New, Cancelled)
                | (New, Rejected)
                | (PartiallyFilled, PartiallyFilled)
                | (PartiallyFilled, Filled)
                | (PartiallyFilled, Cancelled)
        )
    }

    /// Attempts to transition `self` to `next`, mutating in place on success.
    pub fn transition_to(&mut self, next: OrderStatus) -> Result<(), ApexesError> {
        if self.can_transition_to(next) {
            *self = next;
            Ok(())
        } else {
            Err(ApexesError::InvalidStateTransition {
                from: *self,
                to: next,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_to_partially_filled_is_valid() {
        let mut status = OrderStatus::New;
        assert!(status.transition_to(OrderStatus::PartiallyFilled).is_ok());
        assert_eq!(status, OrderStatus::PartiallyFilled);
    }

    #[test]
    fn new_to_filled_is_valid() {
        let mut status = OrderStatus::New;
        assert!(status.transition_to(OrderStatus::Filled).is_ok());
        assert_eq!(status, OrderStatus::Filled);
    }

    #[test]
    fn new_to_cancelled_is_valid() {
        let mut status = OrderStatus::New;
        assert!(status.transition_to(OrderStatus::Cancelled).is_ok());
        assert_eq!(status, OrderStatus::Cancelled);
    }

    #[test]
    fn new_to_rejected_is_valid() {
        let mut status = OrderStatus::New;
        assert!(status.transition_to(OrderStatus::Rejected).is_ok());
        assert_eq!(status, OrderStatus::Rejected);
    }

    #[test]
    fn partially_filled_to_partially_filled_is_valid() {
        let mut status = OrderStatus::PartiallyFilled;
        assert!(status.transition_to(OrderStatus::PartiallyFilled).is_ok());
        assert_eq!(status, OrderStatus::PartiallyFilled);
    }

    #[test]
    fn partially_filled_to_filled_is_valid() {
        let mut status = OrderStatus::PartiallyFilled;
        assert!(status.transition_to(OrderStatus::Filled).is_ok());
        assert_eq!(status, OrderStatus::Filled);
    }

    #[test]
    fn partially_filled_to_cancelled_is_valid() {
        let mut status = OrderStatus::PartiallyFilled;
        assert!(status.transition_to(OrderStatus::Cancelled).is_ok());
        assert_eq!(status, OrderStatus::Cancelled);
    }

    #[test]
    fn filled_to_partially_filled_is_invalid() {
        let mut status = OrderStatus::Filled;
        let result = status.transition_to(OrderStatus::PartiallyFilled);
        assert!(matches!(
            result,
            Err(ApexesError::InvalidStateTransition {
                from: OrderStatus::Filled,
                to: OrderStatus::PartiallyFilled
            })
        ));
        // status is unchanged on failure
        assert_eq!(status, OrderStatus::Filled);
    }

    #[test]
    fn rejected_to_new_is_invalid() {
        let mut status = OrderStatus::Rejected;
        let result = status.transition_to(OrderStatus::New);
        assert!(result.is_err());
        assert_eq!(status, OrderStatus::Rejected);
    }

    #[test]
    fn partially_filled_to_rejected_is_invalid() {
        let mut status = OrderStatus::PartiallyFilled;
        let result = status.transition_to(OrderStatus::Rejected);
        assert!(result.is_err());
    }

    #[test]
    fn cancelled_to_filled_is_invalid() {
        let mut status = OrderStatus::Cancelled;
        let result = status.transition_to(OrderStatus::Filled);
        assert!(result.is_err());
    }

    #[test]
    fn new_to_new_is_invalid() {
        // New has no self-loop (unlike PartiallyFilled)
        let mut status = OrderStatus::New;
        let result = status.transition_to(OrderStatus::New);
        assert!(result.is_err());
    }
}
