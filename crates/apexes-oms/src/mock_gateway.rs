//! [`MockExecutionGateway`]: an in-memory `ExecutionGateway` for tests.
//! Lets a test pre-program specific `GatewayOutcome`s per call and
//! simulate exchange-side state changes that occur "during a
//! disconnect" for `OrderManager::reconcile` tests.

use std::collections::HashMap;
use std::sync::Mutex;

use apexes_core::{Order, OrderId, OrderStatus};
use async_trait::async_trait;
use uuid::Uuid;

use crate::gateway::{ExecutionGateway, GatewayOutcome};

pub struct MockExecutionGateway {
    /// Programmed outcome for the next `submit_order` call, consumed
    /// (removed) on use. `None` means "accept with a freshly generated
    /// exchange_order_id" (the convenient default for tests that don't
    /// care about submission failure paths).
    next_submit_outcome: Mutex<Option<GatewayOutcome>>,
    next_cancel_outcome: Mutex<Option<GatewayOutcome>>,
    /// Exchange-side authoritative state, keyed by OUR client order ID
    /// (`OrderId`) -- tests mutate this directly (simulating "the
    /// exchange processed something while we were disconnected")
    /// before calling `OrderManager::reconcile`. Each entry stores the
    /// exchange-assigned ID string this mock generated at
    /// `submit_order` time, so `query_all_open_orders` can return both.
    exchange_state: Mutex<HashMap<OrderId, (String, OrderStatus)>>,
}

impl Default for MockExecutionGateway {
    fn default() -> Self {
        Self::new()
    }
}

impl MockExecutionGateway {
    pub fn new() -> Self {
        Self {
            next_submit_outcome: Mutex::new(None),
            next_cancel_outcome: Mutex::new(None),
            exchange_state: Mutex::new(HashMap::new()),
        }
    }

    pub fn program_next_submit_outcome(&self, outcome: GatewayOutcome) {
        *self.next_submit_outcome.lock().unwrap() = Some(outcome);
    }

    pub fn program_next_cancel_outcome(&self, outcome: GatewayOutcome) {
        *self.next_cancel_outcome.lock().unwrap() = Some(outcome);
    }

    /// Directly sets the exchange-side status for the order previously
    /// submitted as `client_order_id`, simulating a state change a test
    /// wants `reconcile()` to discover. Panics if `client_order_id` was
    /// never submitted through this mock (a test setup error).
    pub fn set_exchange_state(&self, client_order_id: OrderId, status: OrderStatus) {
        let mut state = self.exchange_state.lock().unwrap();
        let entry = state
            .get_mut(&client_order_id)
            .expect("set_exchange_state called for an order never submitted through this mock");
        entry.1 = status;
    }
}

#[async_trait]
impl ExecutionGateway for MockExecutionGateway {
    async fn submit_order(&self, order: &Order) -> GatewayOutcome {
        if let Some(outcome) = self.next_submit_outcome.lock().unwrap().take() {
            // Even for a programmed Accepted outcome, record the
            // exchange-side state so query_all_open_orders/reconcile
            // tests can find it.
            if let GatewayOutcome::Accepted { exchange_order_id } = &outcome {
                self.exchange_state
                    .lock()
                    .unwrap()
                    .insert(order.id, (exchange_order_id.clone(), OrderStatus::New));
            }
            return outcome;
        }

        let exchange_order_id = Uuid::new_v4().to_string();
        self.exchange_state
            .lock()
            .unwrap()
            .insert(order.id, (exchange_order_id.clone(), OrderStatus::New));
        GatewayOutcome::Accepted { exchange_order_id }
    }

    async fn cancel_order(&self, exchange_order_id: &str) -> GatewayOutcome {
        if let Some(outcome) = self.next_cancel_outcome.lock().unwrap().take() {
            return outcome;
        }

        let mut state = self.exchange_state.lock().unwrap();
        if let Some((_, status)) = state.values_mut().find(|(id, _)| id == exchange_order_id) {
            *status = OrderStatus::Cancelled;
            GatewayOutcome::Accepted {
                exchange_order_id: exchange_order_id.to_string(),
            }
        } else {
            GatewayOutcome::Rejected {
                reason: format!("no such exchange_order_id: {exchange_order_id}"),
            }
        }
    }

    async fn query_order_status(
        &self,
        exchange_order_id: &str,
    ) -> Result<Option<OrderStatus>, String> {
        let state = self.exchange_state.lock().unwrap();
        Ok(state
            .values()
            .find(|(id, _)| id == exchange_order_id)
            .map(|(_, status)| *status))
    }

    async fn query_all_open_orders(
        &self,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
        let state = self.exchange_state.lock().unwrap();
        Ok(state
            .iter()
            .filter(|(_, (_, status))| {
                matches!(status, OrderStatus::New | OrderStatus::PartiallyFilled)
            })
            .map(|(client_order_id, (exchange_order_id, status))| {
                (*client_order_id, Some(exchange_order_id.clone()), *status)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{Order, OrderType, Side, Venue};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn sample_order() -> Order {
        Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            filled_quantity: dec!(0),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn submit_order_defaults_to_accepted_with_a_generated_exchange_order_id() {
        let gateway = MockExecutionGateway::new();
        let order = sample_order();
        let outcome = gateway.submit_order(&order).await;
        assert!(matches!(outcome, GatewayOutcome::Accepted { .. }));
    }

    #[tokio::test]
    async fn programmed_submit_outcome_is_consumed_once() {
        let gateway = MockExecutionGateway::new();
        gateway.program_next_submit_outcome(GatewayOutcome::Rejected {
            reason: "insufficient balance".to_string(),
        });
        let order = sample_order();

        let first = gateway.submit_order(&order).await;
        assert_eq!(
            first,
            GatewayOutcome::Rejected {
                reason: "insufficient balance".to_string()
            }
        );

        // Second call: no programmed outcome left, defaults to Accepted.
        let second = gateway.submit_order(&sample_order()).await;
        assert!(matches!(second, GatewayOutcome::Accepted { .. }));
    }

    #[tokio::test]
    async fn set_exchange_state_and_query_all_open_orders_reflect_it() {
        let gateway = MockExecutionGateway::new();
        let order = sample_order();
        gateway.submit_order(&order).await;

        gateway.set_exchange_state(order.id, OrderStatus::Filled);

        let open_orders = gateway.query_all_open_orders().await.unwrap();
        // Filled is a terminal state -- no longer "open".
        assert!(open_orders.iter().all(|(id, _, _)| *id != order.id));
    }

    #[tokio::test]
    async fn query_all_open_orders_includes_new_and_partially_filled_only() {
        let gateway = MockExecutionGateway::new();
        let order1 = sample_order();
        let order2 = sample_order();
        gateway.submit_order(&order1).await;
        gateway.submit_order(&order2).await;
        gateway.set_exchange_state(order2.id, OrderStatus::PartiallyFilled);

        let open_orders = gateway.query_all_open_orders().await.unwrap();
        assert_eq!(open_orders.len(), 2);
        assert!(open_orders.iter().any(|(id, _, _)| *id == order1.id));
        assert!(open_orders.iter().any(|(id, _, _)| *id == order2.id));
    }

    #[tokio::test]
    async fn query_order_status_returns_none_for_unknown_exchange_order_id() {
        let gateway = MockExecutionGateway::new();
        let result = gateway.query_order_status("never-submitted").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn cancel_order_marks_it_cancelled_by_default() {
        let gateway = MockExecutionGateway::new();
        let order = sample_order();
        let outcome = gateway.submit_order(&order).await;
        let GatewayOutcome::Accepted { exchange_order_id } = outcome else {
            panic!("expected Accepted");
        };

        let cancel_outcome = gateway.cancel_order(&exchange_order_id).await;
        assert!(matches!(cancel_outcome, GatewayOutcome::Accepted { .. }));

        let status = gateway
            .query_order_status(&exchange_order_id)
            .await
            .unwrap();
        assert_eq!(status, Some(OrderStatus::Cancelled));
    }
}
