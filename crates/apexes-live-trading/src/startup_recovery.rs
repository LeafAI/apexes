//! Live startup recovery: before the trading loop starts, bring local
//! state (Postgres orders/fills) and the exchange's authoritative state
//! into agreement and seed the ledger from the exchange rather than
//! from operator-supplied `initial_balances`.
//!
//! Sequence (ordering matters):
//! 1. `reconcile()` local open orders against the exchange. Fills found
//!    here are recorded in the order store but deliberately NOT applied
//!    to the ledger -- step 4's snapshot already includes them.
//! 2. Refuse to start if the exchange has open orders we have no record
//!    of (orphans), or if any local order is still unresolvable (no
//!    exchange order id, so we cannot tell whether it is live).
//! 3. Only then read balances/positions, so the snapshot is taken after
//!    local order state has caught up.

use std::collections::HashMap;
use std::future::Future;

use apexes_core::{AssetClass, OrderId, OrderStatus};
use apexes_oms::{OmsError, OrderManager};
use apexes_strategy::Position;
use async_trait::async_trait;
use rust_decimal::Decimal;

/// Exchange account state used to seed the ledger.
#[derive(Debug, Clone, Default)]
pub struct ExchangeSnapshot {
    pub balances: HashMap<String, Decimal>,
    pub positions: HashMap<String, Position>,
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("order reconciliation failed: {0}")]
    Oms(#[from] OmsError),
    #[error("exchange has open orders with no local record: {0:?}")]
    OrphanOrders(Vec<OrderId>),
    #[error(
        "exchange has {0} open order(s) not placed through this system (e.g. entered manually); cancel them or place the account under this system's control before starting"
    )]
    ForeignOrders(usize),
    #[error("local orders cannot be resolved against the exchange (no exchange order id): {0:?}")]
    UnresolvableOrders(Vec<OrderId>),
    #[error("failed to read exchange account state: {0}")]
    Snapshot(String),
}

/// The order-side operations recovery needs; implemented by
/// `OrderManager`, faked in tests.
#[async_trait]
pub trait RecoveryOrders: Send {
    async fn reconcile_all(&mut self) -> Result<usize, OmsError>;
    async fn unknown_exchange_orders(
        &self,
        asset_classes: &[AssetClass],
    ) -> Result<Vec<OrderId>, OmsError>;
    async fn foreign_open_order_count(
        &self,
        asset_classes: &[AssetClass],
    ) -> Result<usize, OmsError>;
    fn unresolvable_orders(&self) -> Vec<OrderId>;
}

#[async_trait]
impl RecoveryOrders for OrderManager {
    async fn reconcile_all(&mut self) -> Result<usize, OmsError> {
        Ok(self.reconcile().await?.len())
    }

    async fn unknown_exchange_orders(
        &self,
        asset_classes: &[AssetClass],
    ) -> Result<Vec<OrderId>, OmsError> {
        OrderManager::unknown_exchange_orders(self, asset_classes).await
    }

    async fn foreign_open_order_count(
        &self,
        asset_classes: &[AssetClass],
    ) -> Result<usize, OmsError> {
        OrderManager::foreign_open_order_count(self, asset_classes).await
    }

    fn unresolvable_orders(&self) -> Vec<OrderId> {
        self.open_orders()
            .iter()
            .filter(|(_, o)| o.status == OrderStatus::New && o.exchange_order_id.is_none())
            .map(|(id, _)| *id)
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct RecoveredState {
    pub snapshot: ExchangeSnapshot,
    /// Number of local orders whose state changed during startup reconcile.
    pub reconciled_orders: usize,
}

pub async fn recover_live_state<O, F>(
    orders: &mut O,
    asset_classes: &[AssetClass],
    fetch_snapshot: F,
) -> Result<RecoveredState, RecoveryError>
where
    O: RecoveryOrders,
    F: Future<Output = Result<ExchangeSnapshot, String>>,
{
    let reconciled_orders = orders.reconcile_all().await?;

    let orphans = orders.unknown_exchange_orders(asset_classes).await?;
    if !orphans.is_empty() {
        return Err(RecoveryError::OrphanOrders(orphans));
    }
    let foreign = orders.foreign_open_order_count(asset_classes).await?;
    if foreign > 0 {
        return Err(RecoveryError::ForeignOrders(foreign));
    }
    let unresolvable = orders.unresolvable_orders();
    if !unresolvable.is_empty() {
        return Err(RecoveryError::UnresolvableOrders(unresolvable));
    }

    let snapshot = fetch_snapshot.await.map_err(RecoveryError::Snapshot)?;
    tracing::info!(
        balances = snapshot.balances.len(),
        positions = snapshot.positions.len(),
        reconciled_orders,
        "live startup recovery complete"
    );
    Ok(RecoveredState {
        snapshot,
        reconciled_orders,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    #[derive(Default)]
    struct FakeOrders {
        orphans: Vec<OrderId>,
        unresolvable: Vec<OrderId>,
        foreign: usize,
        reconcile_fails: bool,
    }

    #[async_trait]
    impl RecoveryOrders for FakeOrders {
        async fn reconcile_all(&mut self) -> Result<usize, OmsError> {
            if self.reconcile_fails {
                return Err(OmsError::QueryFailed {
                    reason: "boom".into(),
                });
            }
            Ok(2)
        }
        async fn unknown_exchange_orders(
            &self,
            _: &[AssetClass],
        ) -> Result<Vec<OrderId>, OmsError> {
            Ok(self.orphans.clone())
        }
        async fn foreign_open_order_count(&self, _: &[AssetClass]) -> Result<usize, OmsError> {
            Ok(self.foreign)
        }
        fn unresolvable_orders(&self) -> Vec<OrderId> {
            self.unresolvable.clone()
        }
    }

    fn snapshot() -> ExchangeSnapshot {
        ExchangeSnapshot {
            balances: HashMap::from([("USDT".to_string(), dec!(500))]),
            positions: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn clean_state_returns_the_exchange_snapshot() {
        let mut orders = FakeOrders::default();
        let state = recover_live_state(&mut orders, &[AssetClass::Spot], async { Ok(snapshot()) })
            .await
            .unwrap();
        assert_eq!(state.snapshot.balances["USDT"], dec!(500));
        assert_eq!(state.reconciled_orders, 2);
    }

    #[tokio::test]
    async fn orphan_exchange_orders_refuse_startup() {
        let id = OrderId(Uuid::new_v4());
        let mut orders = FakeOrders {
            orphans: vec![id],
            ..Default::default()
        };
        let err = recover_live_state(&mut orders, &[AssetClass::Spot], async { Ok(snapshot()) })
            .await
            .unwrap_err();
        assert!(matches!(err, RecoveryError::OrphanOrders(v) if v == vec![id]));
    }

    #[tokio::test]
    async fn manually_placed_exchange_orders_refuse_startup() {
        let mut orders = FakeOrders {
            foreign: 2,
            ..Default::default()
        };
        let err = recover_live_state(&mut orders, &[AssetClass::Spot], async { Ok(snapshot()) })
            .await
            .unwrap_err();
        assert!(matches!(err, RecoveryError::ForeignOrders(2)));
    }

    #[tokio::test]
    async fn unresolvable_local_orders_refuse_startup() {
        let id = OrderId(Uuid::new_v4());
        let mut orders = FakeOrders {
            unresolvable: vec![id],
            ..Default::default()
        };
        let err = recover_live_state(&mut orders, &[], async { Ok(snapshot()) })
            .await
            .unwrap_err();
        assert!(matches!(err, RecoveryError::UnresolvableOrders(_)));
    }

    #[tokio::test]
    async fn reconcile_and_snapshot_failures_refuse_startup() {
        let mut orders = FakeOrders {
            reconcile_fails: true,
            ..Default::default()
        };
        assert!(matches!(
            recover_live_state(&mut orders, &[], async { Ok(snapshot()) }).await,
            Err(RecoveryError::Oms(_))
        ));
        let mut orders = FakeOrders::default();
        assert!(matches!(
            recover_live_state(&mut orders, &[], async { Err("down".to_string()) }).await,
            Err(RecoveryError::Snapshot(_))
        ));
    }
}
