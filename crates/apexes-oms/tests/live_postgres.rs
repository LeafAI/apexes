//! End-to-end integration test proving `OrderManager` correctly
//! recovers its in-flight order state after a simulated process
//! restart (constructing a fresh `OrderManager` instance against the
//! same PostgreSQL database) and that `reconcile()` then correctly
//! resolves any exchange-side changes that happened in between.
//! Requires a live PostgreSQL instance -- run explicitly with:
//!   docker compose up -d
//!   cargo test -p apexes-oms --test live_postgres -- --ignored

use std::collections::HashMap;
use std::sync::Arc;

use apexes_core::registry::Registry;
use apexes_core::{AssetClass, OrderStatus, OrderType, Side, Venue};
use apexes_oms::{ExecutionGateway, MockExecutionGateway, OrderManager, OrderStore};
use apexes_strategy::{OrderIntent, OrderRouting};
use rust_decimal_macros::dec;

const POSTGRES_URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

fn sample_intent() -> OrderIntent {
    OrderIntent {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Market,
        quantity: dec!(1),
        price: None,
        stop_price: None,
        execution_algo: None,
        routing: OrderRouting::Fixed,
    }
}

/// Deletes all rows from both tables before the test runs, so this test
/// doesn't see leftover state from other tests sharing the same live
/// PostgreSQL database (mirroring `manager.rs`'s own `clean_db` test
/// helper -- `OrderStore` deliberately exposes no public
/// "delete everything" method, so this reconnects independently for the
/// sole purpose of clearing state; production code never does this).
async fn clean_db() {
    let pool = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
    sqlx::query("TRUNCATE fills, orders")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore]
async fn order_manager_recovers_open_orders_after_simulated_restart_and_reconciles() {
    clean_db().await;
    // Keep a concrete `Arc<MockExecutionGateway>` (`gateway`) for
    // test-only inspection (`set_exchange_state`), and register a
    // second `Arc` handle to the SAME underlying instance, upcast to
    // `Arc<dyn ExecutionGateway>`, in the registry -- both `Arc`s point
    // at the same allocation, so mutating through `gateway` is visible
    // through whatever `OrderManager` resolves via the registry.
    let gateway = Arc::new(MockExecutionGateway::new());
    // One Registry instance shared across both simulated "process
    // instances" below, since both need to resolve to the SAME
    // underlying gateway (simulating that the exchange connection
    // itself survives the restart, only the local process doesn't).
    let registry = Registry::new();
    registry
        .register::<Arc<dyn ExecutionGateway>, ()>(
            (),
            Arc::clone(&gateway) as Arc<dyn ExecutionGateway>,
        )
        .unwrap();

    // "Process instance 1": submit an order, then drop the manager
    // (simulating a crash/restart) without cancelling or otherwise
    // cleaning up.
    let order_id = {
        let store = OrderStore::new(POSTGRES_URL).await.unwrap();
        let mut manager = OrderManager::new(store, &registry).await.unwrap();
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            manager.open_orders().get(&order_id).unwrap().status,
            OrderStatus::New
        );
        order_id
        // `manager` (and its `OrderStore`/`PgPool`) is dropped here.
    };

    // While "disconnected", the exchange fills the order.
    gateway.set_exchange_state(order_id, OrderStatus::Filled);

    // "Process instance 2": a fresh OrderManager against the same
    // PostgreSQL database must recover the order as open (from
    // persisted state) before any reconcile() call.
    let store = OrderStore::new(POSTGRES_URL).await.unwrap();
    let mut manager = OrderManager::new(store, &registry).await.unwrap();
    assert_eq!(
        manager.open_orders().get(&order_id).unwrap().status,
        OrderStatus::New,
        "a freshly constructed OrderManager must recover in-flight order state from PostgreSQL"
    );

    // Now reconcile discovers the fill that happened while disconnected.
    // Filled is terminal, so `query_all_open_orders` (pass 1) will not
    // report it -- this is resolved by `reconcile`'s pass 2, which
    // queries `query_order_status` individually for local orders still
    // open that pass 1 didn't report.
    let changed = manager.reconcile().await.unwrap();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].0, order_id);
    assert!(manager.open_orders().get(&order_id).is_none());

    // `OrderManager::store` is a private field, so verify persistence
    // through a fresh `OrderStore` connection rather than reaching into
    // `manager`'s internals (as the plan's literal `manager.store...`
    // snippet would require).
    let verify_store = OrderStore::new(POSTGRES_URL).await.unwrap();
    let persisted = verify_store.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(persisted.status, OrderStatus::Filled);
}
