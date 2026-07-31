//! [`ExchangeUpdate`]: gateway-agnostic representation of a push-based
//! order status update (e.g. from a Binance User Data Stream WebSocket
//! message, via `apexes-gateway-binance`). Lives in `apexes-oms` (not
//! `apexes-live-trading`) so both `apexes-gateway-binance` (which
//! produces a `Stream<Item = ExchangeUpdate>`) and `apexes-live-trading`
//! (which consumes it, calling `OrderManager::apply_exchange_update`
//! for each item) depend on it via the one crate they already both
//! depend on for `ExecutionGateway`/`OrderManager`, keeping the
//! dependency graph a DAG (`apexes-gateway-binance` and
//! `apexes-live-trading` never depend on each other).

use apexes_core::{OrderId, OrderStatus};

#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeUpdate {
    pub client_order_id: OrderId,
    pub exchange_order_id: Option<String>,
    pub status: OrderStatus,
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn exchange_update_is_constructible_and_comparable() {
        let a = ExchangeUpdate {
            client_order_id: OrderId(Uuid::nil()),
            exchange_order_id: Some("EX1".to_string()),
            status: OrderStatus::PartiallyFilled,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }
}
