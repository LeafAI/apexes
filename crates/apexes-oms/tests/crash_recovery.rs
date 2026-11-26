//! Crash-recovery tests for the order/fill persistence path.
//!
//! A "crash" is simulated by building the exact database state a process
//! would have left behind at a given point, then constructing a FRESH
//! `OrderManager` against the same PostgreSQL database and the same
//! (surviving) exchange, and running `reconcile()`. Every scenario
//! asserts there are no lost orders, no lost fills and no duplicated
//! fills afterwards.
//!
//! Requires PostgreSQL (see `live_postgres.rs`):
//!   cargo test -p apexes-oms --test crash_recovery -- --ignored --test-threads=1

use std::sync::Arc;
use std::time::Duration;

use apexes_core::registry::Registry;
use apexes_core::{AssetClass, Fill, FillId, Order, OrderId, OrderStatus, OrderType, Side, Venue};
use apexes_oms::{
    ExchangeFill, ExecutionGateway, GatewayOutcome, MockExecutionGateway, OmsError, OrderManager,
    OrderStore, SubmissionState,
};
use apexes_strategy::{OrderIntent, OrderRouting};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use uuid::Uuid;

const POSTGRES_URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

/// The tests share one database and TRUNCATE it, so they must not overlap.
static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Harness {
    gateway: Arc<MockExecutionGateway>,
    registry: Registry,
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

async fn harness() -> Harness {
    let guard = DB_LOCK.lock().await;
    let store = OrderStore::new(POSTGRES_URL).await.unwrap();
    drop(store); // ensures tables exist
    let pool = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
    sqlx::query("TRUNCATE fills, orders")
        .execute(&pool)
        .await
        .unwrap();
    let gateway = Arc::new(MockExecutionGateway::new());
    let registry = Registry::new();
    registry
        .register::<Arc<dyn ExecutionGateway>, ()>(
            (),
            Arc::clone(&gateway) as Arc<dyn ExecutionGateway>,
        )
        .unwrap();
    Harness {
        gateway,
        registry,
        _guard: guard,
    }
}

impl Harness {
    /// "Process restart": a brand-new manager recovering from the database.
    async fn restart(&self) -> (OrderManager, OrderStore) {
        let manager =
            OrderManager::new(OrderStore::new(POSTGRES_URL).await.unwrap(), &self.registry)
                .await
                .unwrap();
        (manager, OrderStore::new(POSTGRES_URL).await.unwrap())
    }
}

fn order(age: Duration, exchange_order_id: Option<&str>, status: OrderStatus) -> Order {
    let created: DateTime<Utc> = Utc::now() - chrono::Duration::from_std(age).unwrap();
    Order {
        id: OrderId(Uuid::new_v4()),
        exchange_order_id: exchange_order_id.map(str::to_string),
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Market,
        quantity: dec!(2),
        price: None,
        stop_price: None,
        leverage: None,
        margin_mode: None,
        filled_quantity: Decimal::ZERO,
        status,
        created_at: created,
        updated_at: created,
    }
}

fn exchange_fill(trade_id: &str, qty: Decimal) -> ExchangeFill {
    ExchangeFill {
        exchange_trade_id: trade_id.to_string(),
        price: dec!(100),
        quantity: qty,
        fee: dec!(0.1),
        fee_currency: Some("USDT".to_string()),
        filled_at: Utc::now(),
    }
}

fn stored_fill(order: &Order, qty: Decimal) -> Fill {
    Fill {
        id: FillId(Uuid::new_v4()),
        order_id: order.id,
        symbol: order.symbol.clone(),
        venue: order.venue,
        side: order.side,
        price: dec!(100),
        quantity: qty,
        fee: dec!(0.1),
        fee_currency: Some("USDT".to_string()),
        filled_at: Utc::now(),
        leverage: None,
        margin_mode: None,
    }
}

async fn fills_total(store: &OrderStore, id: OrderId) -> (usize, Decimal) {
    let fills = store.get_fills_for_order(id).await.unwrap();
    (fills.len(), fills.iter().map(|f| f.quantity).sum())
}

#[tokio::test]
#[ignore]
async fn crash_with_no_exchange_ack_remains_uncertain_even_after_grace_period() {
    let h = harness().await;
    let setup = OrderStore::new(POSTGRES_URL).await.unwrap();
    // The process wrote the order, then died before calling the gateway.
    let stale = order(Duration::from_secs(300), None, OrderStatus::New);
    let young = order(Duration::from_secs(1), None, OrderStatus::New);
    setup.upsert_order(&stale).await.unwrap();
    setup.upsert_order(&young).await.unwrap();

    let (mut manager, store) = h.restart().await;
    assert_eq!(manager.open_orders().len(), 2);
    manager.reconcile().await.unwrap();

    // A negative lookup and the age of a record cannot distinguish
    // pre-dispatch crash from a delayed exchange acceptance.
    assert_eq!(
        store.get_order(stale.id).await.unwrap().unwrap().status,
        OrderStatus::New
    );
    assert!(manager.open_orders().contains_key(&stale.id));
    assert_eq!(
        store.get_submission_state(stale.id).await.unwrap(),
        Some(SubmissionState::Uncertain)
    );
    // The young order is also unresolved.
    assert!(manager.open_orders().contains_key(&young.id));
    assert_eq!(
        store.get_order(young.id).await.unwrap().unwrap().status,
        OrderStatus::New
    );
}

#[tokio::test]
#[ignore]
async fn crash_after_exchange_accepted_before_ack_persisted_recovers_the_exchange_id() {
    let h = harness().await;
    let setup = OrderStore::new(POSTGRES_URL).await.unwrap();
    let o = order(Duration::from_secs(5), None, OrderStatus::New);
    setup.upsert_order(&o).await.unwrap();
    // The exchange accepted and is working it.
    h.gateway
        .force_register_exchange_order(o.id, "EX-1", OrderStatus::New, AssetClass::Spot);

    let (mut manager, store) = h.restart().await;
    manager.reconcile().await.unwrap();

    let stored = store.get_order(o.id).await.unwrap().unwrap();
    assert_eq!(stored.exchange_order_id.as_deref(), Some("EX-1"));
    assert_eq!(stored.status, OrderStatus::New);
    assert_eq!(
        store.get_submission_state(o.id).await.unwrap(),
        Some(SubmissionState::Accepted)
    );
    // And it is now cancellable (needs the id).
    manager.cancel_order(o.id).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn crash_after_exchange_fill_before_ack_persisted_recovers_status_id_and_fills() {
    let h = harness().await;
    let setup = OrderStore::new(POSTGRES_URL).await.unwrap();
    // Uncertain submit / lost ack, and the order then filled on the exchange.
    let o = order(Duration::from_secs(5), None, OrderStatus::New);
    setup.upsert_order(&o).await.unwrap();
    h.gateway
        .force_register_exchange_order(o.id, "EX-2", OrderStatus::Filled, AssetClass::Spot);
    h.gateway.set_fills_for_order(
        o.id,
        vec![
            exchange_fill("T1", dec!(1.5)),
            exchange_fill("T2", dec!(0.5)),
        ],
    );

    let (mut manager, store) = h.restart().await;
    let changes = manager.reconcile().await.unwrap();

    let new_fills: usize = changes.iter().map(|(_, f)| f.len()).sum();
    assert_eq!(new_fills, 2, "both fills must be surfaced for the ledger");
    let stored = store.get_order(o.id).await.unwrap().unwrap();
    assert_eq!(stored.status, OrderStatus::Filled);
    assert_eq!(stored.exchange_order_id.as_deref(), Some("EX-2"));
    assert_eq!(stored.filled_quantity, dec!(2));
    assert_eq!(fills_total(&store, o.id).await, (2, dec!(2)));
    assert!(manager.open_orders().is_empty());
}

#[tokio::test]
#[ignore]
async fn failed_fill_query_does_not_lose_fills_and_is_retried() {
    let h = harness().await;
    let setup = OrderStore::new(POSTGRES_URL).await.unwrap();
    let o = order(Duration::from_secs(5), Some("EX-3"), OrderStatus::New);
    setup.upsert_order(&o).await.unwrap();
    h.gateway
        .force_register_exchange_order(o.id, "EX-3", OrderStatus::Filled, AssetClass::Spot);
    h.gateway
        .set_fills_for_order(o.id, vec![exchange_fill("T1", dec!(2))]);
    h.gateway.fail_next_fill_queries(1);

    let (mut manager, store) = h.restart().await;
    let first = manager.reconcile().await.unwrap();
    assert!(first.iter().all(|(_, f)| f.is_empty()));
    // Status must NOT have advanced: advancing would orphan the fills forever.
    assert_eq!(
        store.get_order(o.id).await.unwrap().unwrap().status,
        OrderStatus::New
    );
    assert!(manager.open_orders().contains_key(&o.id));

    let second = manager.reconcile().await.unwrap();
    assert_eq!(second.iter().map(|(_, f)| f.len()).sum::<usize>(), 1);
    let stored = store.get_order(o.id).await.unwrap().unwrap();
    assert_eq!(stored.status, OrderStatus::Filled);
    assert_eq!(stored.filled_quantity, dec!(2));
    assert_eq!(fills_total(&store, o.id).await, (1, dec!(2)));
}

#[tokio::test]
#[ignore]
async fn crash_after_fills_persisted_before_status_converges_without_duplicates() {
    let h = harness().await;
    let setup = OrderStore::new(POSTGRES_URL).await.unwrap();
    // Fills reached the database; the process died before the order row
    // moved to Filled (and so before filled_quantity was updated).
    let o = order(Duration::from_secs(5), Some("EX-4"), OrderStatus::New);
    setup.upsert_order(&o).await.unwrap();
    setup
        .insert_fill(&stored_fill(&o, dec!(2)), "T1")
        .await
        .unwrap();
    h.gateway
        .force_register_exchange_order(o.id, "EX-4", OrderStatus::Filled, AssetClass::Spot);
    h.gateway
        .set_fills_for_order(o.id, vec![exchange_fill("T1", dec!(2))]);

    let (mut manager, store) = h.restart().await;
    manager.reconcile().await.unwrap();
    manager.reconcile().await.unwrap(); // and again: must stay idempotent

    let stored = store.get_order(o.id).await.unwrap().unwrap();
    assert_eq!(stored.status, OrderStatus::Filled);
    assert_eq!(
        stored.filled_quantity,
        dec!(2),
        "filled_quantity must be recomputed from stored fills, not left at 0"
    );
    assert_eq!(
        fills_total(&store, o.id).await,
        (1, dec!(2)),
        "no duplicate fill"
    );
}

#[tokio::test]
#[ignore]
async fn partial_fills_on_a_cancelled_order_are_recorded() {
    let h = harness().await;
    let setup = OrderStore::new(POSTGRES_URL).await.unwrap();
    let o = order(
        Duration::from_secs(5),
        Some("EX-5"),
        OrderStatus::PartiallyFilled,
    );
    setup.upsert_order(&o).await.unwrap();
    h.gateway
        .force_register_exchange_order(o.id, "EX-5", OrderStatus::Cancelled, AssetClass::Spot);
    h.gateway
        .set_fills_for_order(o.id, vec![exchange_fill("T1", dec!(0.75))]);

    let (mut manager, store) = h.restart().await;
    let changes = manager.reconcile().await.unwrap();

    assert_eq!(changes.iter().map(|(_, f)| f.len()).sum::<usize>(), 1);
    let stored = store.get_order(o.id).await.unwrap().unwrap();
    assert_eq!(stored.status, OrderStatus::Cancelled);
    assert_eq!(stored.filled_quantity, dec!(0.75));
    assert_eq!(fills_total(&store, o.id).await, (1, dec!(0.75)));
}

fn intent(symbol: &str) -> OrderIntent {
    OrderIntent {
        symbol: symbol.into(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Market,
        quantity: dec!(1),
        price: None,
        stop_price: None,
        execution_algo: None,
        routing: OrderRouting::Fixed,
        leverage: None,
        margin_mode: None,
    }
}

#[tokio::test]
#[ignore]
async fn uncertain_order_blocks_same_instrument_across_restart_until_late_fill_is_reconciled() {
    let h = harness().await;
    let (mut manager, store) = h.restart().await;
    h.gateway
        .program_next_submit_outcome(GatewayOutcome::Uncertain {
            reason: "lost acknowledgement".into(),
        });
    let id = manager
        .submit_order(intent("BTCUSDT"), &HashMap::new(), &HashMap::new())
        .await
        .unwrap();
    assert_eq!(
        store.get_submission_state(id).await.unwrap(),
        Some(SubmissionState::Uncertain)
    );
    // Repeated negative lookups, even after the warning threshold, do not
    // establish non-execution or authorize a replacement.
    manager = manager.with_unsent_order_grace(Duration::ZERO);
    for _ in 0..2 {
        manager.reconcile().await.unwrap();
        assert!(
            matches!(manager.submit_order(intent("BTCUSDT"), &HashMap::new(), &HashMap::new()).await,
            Err(OmsError::SubmissionUnresolved { order_id }) if order_id == id)
        );
    }
    drop(manager);
    let (mut recovered, _) = h.restart().await;
    assert!(
        matches!(recovered.submit_flatten_order(intent("BTCUSDT")).await,
        Err(OmsError::SubmissionUnresolved { order_id }) if order_id == id)
    );
    // Different instruments are independent; account-wide risk is a separate control.
    let other = recovered
        .submit_order(intent("ETHUSDT"), &HashMap::new(), &HashMap::new())
        .await
        .unwrap();
    assert_eq!(
        store.get_submission_state(other).await.unwrap(),
        Some(SubmissionState::Accepted)
    );
    h.gateway
        .force_register_exchange_order(id, "EX-LATE", OrderStatus::Filled, AssetClass::Spot);
    h.gateway
        .set_fills_for_order(id, vec![exchange_fill("LATE-1", dec!(1))]);
    let changes = recovered.reconcile().await.unwrap();
    assert_eq!(
        changes
            .iter()
            .filter(|(oid, _)| *oid == id)
            .map(|(_, f)| f.len())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        store.get_submission_state(id).await.unwrap(),
        Some(SubmissionState::Accepted)
    );
    assert_eq!(
        store.get_order(id).await.unwrap().unwrap().status,
        OrderStatus::Filled
    );
    assert!(!recovered.open_orders().contains_key(&id));
    recovered.reconcile().await.unwrap();
    assert_eq!(fills_total(&store, id).await, (1, dec!(1)));
    assert!(
        recovered
            .submit_order(intent("BTCUSDT"), &HashMap::new(), &HashMap::new())
            .await
            .is_ok()
    );
}

#[tokio::test]
#[ignore]
async fn explicit_rejection_releases_guard_and_is_durable() {
    let h = harness().await;
    let (mut manager, store) = h.restart().await;
    h.gateway
        .program_next_submit_outcome(GatewayOutcome::Rejected {
            reason: "insufficient balance".into(),
        });
    assert!(matches!(
        manager
            .submit_order(intent("BTCUSDT"), &HashMap::new(), &HashMap::new())
            .await,
        Err(OmsError::OrderRejected { .. })
    ));
    assert!(manager.open_orders().is_empty());
    let pool = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
    let rejected: Uuid = sqlx::query_scalar("SELECT id FROM orders WHERE status = 'Rejected'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        store.get_submission_state(OrderId(rejected)).await.unwrap(),
        Some(SubmissionState::Rejected)
    );
    assert!(
        manager
            .submit_order(intent("BTCUSDT"), &HashMap::new(), &HashMap::new())
            .await
            .is_ok()
    );
}
