use crate::error::ApexesError;
use crate::instrument::{AssetClass, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
    /// Triggers a Market order once `Order.stop_price` is touched.
    Stop,
    /// Triggers a Limit order (at `Order.price`) once `Order.stop_price` is touched.
    StopLimit,
}

/// Which margin allocation model a leveraged position uses. `Isolated`:
/// the position has its own dedicated margin allocation -- liquidation
/// only risks that position. `Cross`: the position shares the whole
/// account's net value (see `apexes_strategy`'s `PortfolioLedger::
/// margin_status` design) as its margin pool. Defined here (not in
/// `apexes-strategy`, where `Position`/`OrderIntent` live) specifically
/// because `Order` (this file) needs to carry it, and `apexes-strategy`
/// already depends on `apexes-core` -- the reverse dependency direction
/// would cycle. `apexes-strategy` re-exports this type via `pub use
/// apexes_core::MarginMode;`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarginMode {
    Isolated,
    Cross,
}

/// An order to buy or sell a quantity of an instrument at a venue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    /// Limit price. `None` for Market/Stop orders; `Some` for Limit/StopLimit orders.
    pub price: Option<Decimal>,
    /// Trigger price. `None` for Market/Limit orders; `Some` for Stop/StopLimit orders.
    pub stop_price: Option<Decimal>,
    /// `None`: spot order (today's behavior, unchanged). `Some(leverage)`:
    /// this order opens/adds to a leveraged perpetual position at this
    /// leverage. Set by `OrderManager::submit_order` (`apexes-oms`) from
    /// the originating `OrderIntent.leverage`, so `ExecutionGateway`
    /// implementations (e.g. `BinanceExecutionGateway`) can see it and
    /// set the exchange-side leverage/margin-mode before submitting.
    pub leverage: Option<Decimal>,
    /// Only meaningful when `leverage.is_some()`; `None` is treated as
    /// `MarginMode::Isolated` by convention (matching
    /// `OrderIntent.margin_mode`'s own default).
    pub margin_mode: Option<MarginMode>,
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
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: rust_decimal_macros::dec!(1.5),
            price: Some(rust_decimal_macros::dec!(65000)),
            stop_price: None,
            leverage: None,
            margin_mode: None,
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
            asset_class: AssetClass::Equity,
            side: Side::Sell,
            order_type: OrderType::Market,
            quantity: rust_decimal_macros::dec!(10),
            price: None,
            stop_price: None,
            leverage: None,
            margin_mode: None,
            filled_quantity: rust_decimal_macros::dec!(0),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
        assert_eq!(order.price, None);
        assert_eq!(order.order_type, OrderType::Market);
    }

    #[test]
    fn stop_order_serde_round_trip() {
        let now = Utc::now();
        let order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Sell,
            order_type: OrderType::Stop,
            quantity: rust_decimal_macros::dec!(1.0),
            price: None,
            stop_price: Some(rust_decimal_macros::dec!(60000)),
            leverage: None,
            margin_mode: None,
            filled_quantity: rust_decimal_macros::dec!(0),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&order).unwrap();
        let round_tripped: Order = serde_json::from_str(&json).unwrap();
        assert_eq!(order, round_tripped);
        assert_eq!(order.price, None);
        assert_eq!(order.stop_price, Some(rust_decimal_macros::dec!(60000)));
    }

    #[test]
    fn stop_limit_order_serde_round_trip() {
        let now = Utc::now();
        let order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::StopLimit,
            quantity: rust_decimal_macros::dec!(1.0),
            price: Some(rust_decimal_macros::dec!(65500)),
            stop_price: Some(rust_decimal_macros::dec!(65450)),
            leverage: None,
            margin_mode: None,
            filled_quantity: rust_decimal_macros::dec!(0),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&order).unwrap();
        let round_tripped: Order = serde_json::from_str(&json).unwrap();
        assert_eq!(order, round_tripped);
        assert_eq!(order.price, Some(rust_decimal_macros::dec!(65500)));
        assert_eq!(order.stop_price, Some(rust_decimal_macros::dec!(65450)));
    }

    #[test]
    fn margin_mode_serde_round_trip() {
        for mode in [MarginMode::Isolated, MarginMode::Cross] {
            let json = serde_json::to_string(&mode).unwrap();
            let round_tripped: MarginMode = serde_json::from_str(&json).unwrap();
            assert_eq!(mode, round_tripped);
        }
    }
}
