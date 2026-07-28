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

use crate::instrument::Venue;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub venue: Venue,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub filled_quantity: Decimal,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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

    #[test]
    fn order_serde_round_trip() {
        let now = Utc::now();
        let order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: rust_decimal_macros::dec!(1.5),
            price: Some(rust_decimal_macros::dec!(65000)),
            filled_quantity: rust_decimal_macros::dec!(0),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&order).unwrap();
        let round_tripped: Order = serde_json::from_str(&json).unwrap();
        assert_eq!(order, round_tripped);
    }

    #[test]
    fn market_order_has_no_price() {
        let now = Utc::now();
        let order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "AAPL".to_string(),
            venue: Venue::Nasdaq,
            side: Side::Sell,
            order_type: OrderType::Market,
            quantity: rust_decimal_macros::dec!(10),
            price: None,
            filled_quantity: rust_decimal_macros::dec!(0),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
        assert_eq!(order.price, None);
        assert_eq!(order.order_type, OrderType::Market);
    }
}
