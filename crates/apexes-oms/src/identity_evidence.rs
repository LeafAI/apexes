//! F04.04 — Read-only evidence for a durable OMS client order identity.
//! Never changes the submission guard, ledger, exchange state or order status.
use crate::{ExecutionGateway, SubmissionState};
use apexes_core::{Order, OrderStatus};

/// A strategy intent ordinal scoped to one live executor run, not a
/// globally reusable strategy ID or exchange idempotency key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntentLink {
    pub run_scope: uuid::Uuid,
    pub intent_id: apexes_strategy::IntentId,
}

impl IntentLink {
    pub(crate) fn validate(self) -> Result<Self, crate::OmsError> {
        if self.run_scope.is_nil() || self.intent_id.0 == 0 {
            return Err(crate::OmsError::InvalidRow {
                reason: "invalid scoped intent identity".into(),
            });
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteIdentity {
    /// Actual client-ID lookup response, consistent with the stored exchange ID.
    Found {
        exchange_order_id: String,
        status: OrderStatus,
    },
    /// Negative lookup is NOT proof that a submission never executed.
    NotFound,
    /// Query failed, unsupported, or otherwise unavailable.
    Unavailable,
    /// Response identity contradicts the durable order.
    Conflict,
}

#[derive(Debug, Clone)]
pub struct IdentityEvidence {
    /// One database-row snapshot, not an assertion of current exchange status.
    pub local: Order,
    pub submission_state: SubmissionState,
    pub intent_link: Option<IntentLink>,
    pub remote: RemoteIdentity,
}

pub(crate) async fn observe(
    gateway: &dyn ExecutionGateway,
    local: Order,
    submission_state: SubmissionState,
    intent_link: Option<IntentLink>,
) -> IdentityEvidence {
    let remote = match gateway.query_order_by_client_id(&local).await {
        Ok(Some((exchange_order_id, status))) => {
            if exchange_order_id.trim().is_empty()
                || local
                    .exchange_order_id
                    .as_ref()
                    .is_some_and(|id| *id != exchange_order_id)
                || submission_state == SubmissionState::Rejected
            {
                RemoteIdentity::Conflict
            } else {
                RemoteIdentity::Found {
                    exchange_order_id,
                    status,
                }
            }
        }
        Ok(None) => RemoteIdentity::NotFound,
        Err(_) => RemoteIdentity::Unavailable,
    };
    IdentityEvidence {
        local,
        submission_state,
        intent_link,
        remote,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MockExecutionGateway;
    use apexes_core::{AssetClass, OrderId, OrderType, Side, Venue};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn order() -> Order {
        Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: Decimal::ONE,
            price: None,
            stop_price: None,
            leverage: None,
            margin_mode: None,
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn client_lookup_preserves_local_snapshot_and_distinguishes_conflict() {
        let gateway = MockExecutionGateway::new();
        let mut local = order();
        let evidence = observe(&gateway, local.clone(), SubmissionState::Submitting, None).await;
        assert_eq!(evidence.remote, RemoteIdentity::NotFound);
        assert_eq!(evidence.local.exchange_order_id, None);
        gateway.force_register_exchange_order(
            local.id,
            "123",
            OrderStatus::Filled,
            local.asset_class,
        );
        let evidence = observe(&gateway, local.clone(), SubmissionState::Submitting, None).await;
        assert_eq!(
            evidence.remote,
            RemoteIdentity::Found {
                exchange_order_id: "123".into(),
                status: OrderStatus::Filled
            }
        );
        assert_eq!(evidence.local.status, OrderStatus::New);
        assert_eq!(evidence.local.filled_quantity, Decimal::ZERO);
        assert_eq!(evidence.submission_state, SubmissionState::Submitting);
        assert_eq!(
            observe(&gateway, local.clone(), SubmissionState::Rejected, None)
                .await
                .remote,
            RemoteIdentity::Conflict
        );
        local.exchange_order_id = Some("456".into());
        assert_eq!(
            observe(&gateway, local.clone(), SubmissionState::Accepted, None)
                .await
                .remote,
            RemoteIdentity::Conflict
        );
        local.exchange_order_id = Some("123".into());
        assert!(matches!(
            observe(&gateway, local.clone(), SubmissionState::Accepted, None)
                .await
                .remote,
            RemoteIdentity::Found { .. }
        ));
        gateway.force_register_exchange_order(local.id, "", OrderStatus::New, local.asset_class);
        assert_eq!(
            observe(&gateway, local, SubmissionState::Uncertain, None)
                .await
                .remote,
            RemoteIdentity::Conflict
        );
    }

    struct Unsupported;
    #[async_trait::async_trait]
    impl ExecutionGateway for Unsupported {
        async fn query_fills_for_order(
            &self,
            _: &Order,
        ) -> Result<Vec<crate::ExchangeFill>, String> {
            panic!("unexpected fills query")
        }
        async fn submit_order(&self, _: &Order) -> crate::GatewayOutcome {
            panic!("unexpected submit")
        }
        async fn cancel_order(&self, _: &Order) -> crate::GatewayOutcome {
            panic!("unexpected cancel")
        }
        async fn query_order_status(&self, _: &Order) -> Result<Option<OrderStatus>, String> {
            panic!("unexpected status query")
        }
        async fn query_all_open_orders(
            &self,
            _: AssetClass,
        ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
            panic!("unexpected account query")
        }
    }

    #[tokio::test]
    async fn unsupported_lookup_is_unavailable_not_negative() {
        assert_eq!(
            observe(&Unsupported, order(), SubmissionState::Uncertain, None)
                .await
                .remote,
            RemoteIdentity::Unavailable
        );
    }
}
