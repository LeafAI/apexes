//! Integration tests requiring a live local PostgreSQL. Run explicitly:
//! `cargo test -p apexes-ems --test live_postgres -- --ignored --test-threads=1`
//! (matching `apexes-oms`'s own `live_postgres.rs` conventions exactly).

use std::collections::HashMap;
use std::sync::Arc;

use apexes_core::{OrderId, Side, Venue};
use apexes_ems::{AlgoOrderRequest, ChildOrderIntent, ChildOrderSink, Ems, EmsError, PostgresEms};
use apexes_strategy::{ExecutionAlgoSpec, OrderRouting, ParentOrderStatus, PriceAndFeeScorer};
use async_trait::async_trait;
use rust_decimal_macros::dec;
use tokio::sync::Mutex as TokioMutex;

const POSTGRES_URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

/// Give each test its own schema, including the deliberately unresolved
/// dispatch fixtures. Even a panicking test cannot poison the public
/// schema used by the subsequent live-trading integration suite.
struct TestSchema {
    admin: sqlx::PgPool,
    name: String,
    url: String,
}

impl TestSchema {
    async fn new() -> Self {
        let admin = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
        // SQL identifier is generated exclusively from a fixed prefix and UUID hex.
        let name = format!("ems_test_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {name}")))
            .execute(&admin)
            .await
            .unwrap();
        let url = format!("{POSTGRES_URL}?options[search_path]={name}");
        Self { admin, name, url }
    }

    async fn cleanup(self) {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.name
        )))
        .execute(&self.admin)
        .await
        .unwrap();
    }
}

struct RecordingSink {
    submitted: TokioMutex<Vec<ChildOrderIntent>>,
}

impl RecordingSink {
    fn new() -> Self {
        Self {
            submitted: TokioMutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl ChildOrderSink for RecordingSink {
    async fn submit_child_order(
        &self,
        intent: ChildOrderIntent,
    ) -> Result<(OrderId, Vec<apexes_core::Fill>), EmsError> {
        self.submitted.lock().await.push(intent);
        Ok((OrderId(uuid::Uuid::new_v4()), Vec::new()))
    }

    async fn cancel_child_order(&self, _order_id: OrderId) -> Result<(), EmsError> {
        Ok(())
    }

    async fn order_status(
        &self,
        _order_id: OrderId,
    ) -> Result<Option<apexes_core::Order>, EmsError> {
        Ok(None)
    }
}

fn twap_request() -> AlgoOrderRequest {
    AlgoOrderRequest {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        side: Side::Buy,
        quantity: dec!(4),
        execution_algo: ExecutionAlgoSpec::Twap {
            duration: std::time::Duration::from_millis(200),
            slices: 4,
        },
        routing: OrderRouting::Fixed,
        leverage: None,
        margin_mode: None,
    }
}

#[tokio::test]
#[ignore]
async fn managed_pov_parent_reserves_before_dispatch_and_recovers_budget() {
    use apexes_core::registry::Registry;
    use apexes_ems::postgres::OrderManagerChildOrderSink;
    use apexes_oms::{
        ExecutionGateway, KillSwitch, MockExecutionGateway, OrderManager, OrderStore,
    };
    let fixture = TestSchema::new().await;
    let registry = Registry::new();
    let gateway = Arc::new(MockExecutionGateway::new());
    registry
        .register::<Arc<dyn ExecutionGateway>, ()>((), gateway.clone())
        .unwrap();
    let health = Arc::new(KillSwitch::managed());
    let mut store = OrderStore::new(&fixture.url).await.unwrap();
    store.bind_spot_inventory("managed-ems-test").await.unwrap();
    let mut manager = OrderManager::new(store, &registry)
        .await
        .unwrap()
        .with_kill_switch(health.clone());
    manager.spot_inventory_fills().await.unwrap();
    health.begin_reconciliation();
    health.startup_ready();
    let manager = Arc::new(TokioMutex::new(manager));
    let sink = Arc::new(OrderManagerChildOrderSink::new(manager.clone()));
    let ems = PostgresEms::new_with_kill_switch(
        &fixture.url,
        sink.clone(),
        Arc::new(PriceAndFeeScorer),
        HashMap::new(),
        None,
        Some(health.clone()),
    )
    .await
    .unwrap();
    let scope = uuid::Uuid::new_v4();
    let ordinal = apexes_strategy::IntentId(u64::MAX);
    let id = ems
        .submit_correlated_algo_order(pov_request(), Some((scope, ordinal)))
        .await
        .unwrap();
    assert_eq!(
        ems.parent_for_intent(scope, ordinal).await.unwrap(),
        Some(id)
    );
    assert!(matches!(
        ems.submit_correlated_algo_order(pov_request(), Some((scope, ordinal)))
            .await,
        Err(EmsError::ParentAdmissionRejected { .. })
    ));
    let mut other = pov_request();
    other.symbol = "ETHUSDT".into();
    assert!(matches!(
        ems.submit_algo_order(other.clone()).await,
        Err(EmsError::ParentAdmissionRejected { .. })
    ));
    assert!(!health.is_engaged());
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    let budget: rust_decimal::Decimal =
        sqlx::query_scalar("SELECT budget FROM spot_parent_allocations WHERE id=$1")
            .bind(id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(budget, dec!(404));
    ems.on_market_event(&trade(Venue::Binance, "BTCUSDT"))
        .await
        .unwrap();
    let child = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let row: Option<(uuid::Uuid,)> = sqlx::query_as(
                "SELECT child_order_id FROM parent_order_children WHERE parent_order_id=$1",
            )
            .bind(id.0)
            .fetch_optional(&pool)
            .await
            .unwrap();
            if let Some((child,)) = row {
                break OrderId(child);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let (allocated,linked):(rust_decimal::Decimal,uuid::Uuid)=sqlx::query_as("SELECT p.allocated_amount,o.parent_allocation_id FROM spot_parent_allocations p JOIN orders o ON o.parent_allocation_id=p.id WHERE o.id=$1")
        .bind(child.0).fetch_one(&pool).await.unwrap();
    assert_eq!(allocated, dec!(101));
    assert_eq!(linked, id.0);
    let evidence = manager
        .lock()
        .await
        .inspect_client_identity(child)
        .await
        .unwrap()
        .unwrap();
    let link = evidence.child_link.unwrap();
    assert_eq!(link.parent_id, id);
    assert_eq!(evidence.intent_link, None);
    assert_eq!(
        manager
            .lock()
            .await
            .client_order_for_dispatch(link)
            .await
            .unwrap(),
        Some(child)
    );
    assert_eq!(ems.pending_dispatch(id).await.unwrap(), None);
    let recorded:uuid::Uuid=sqlx::query_scalar("SELECT dispatch_id FROM parent_order_children WHERE parent_order_id=$1 AND child_order_id=$2").bind(id.0).bind(child.0).fetch_one(&pool).await.unwrap();
    assert_eq!(recorded, link.dispatch_id);

    manager.lock().await.cancel_order(child).await.unwrap();
    drop(ems);
    let ems = PostgresEms::new_with_kill_switch(
        &fixture.url,
        sink.clone(),
        Arc::new(PriceAndFeeScorer),
        HashMap::new(),
        None,
        Some(health.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        ems.parent_order_status(id).await.unwrap(),
        Some(ParentOrderStatus::Scheduling)
    );
    assert!(matches!(
        ems.submit_algo_order(other.clone()).await,
        Err(EmsError::ParentAdmissionRejected { .. })
    ));
    assert_eq!(
        ems.parent_for_intent(scope, ordinal).await.unwrap(),
        Some(id)
    );
    ems.cancel_parent_order(id).await.unwrap();
    assert!(ems.submit_algo_order(other).await.is_ok());
    drop(ems);
    drop(sink);
    drop(manager);
    pool.close().await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn twap_parent_order_persists_and_completes() {
    let fixture = TestSchema::new().await;
    let sink = Arc::new(RecordingSink::new());
    let ems = PostgresEms::new(
        &fixture.url,
        sink.clone(),
        Arc::new(PriceAndFeeScorer),
        HashMap::new(),
        None,
    )
    .await
    .unwrap();

    let parent_id = ems.submit_algo_order(twap_request()).await.unwrap();

    // Poll for completion -- the background task paces itself over
    // ~200ms of real wall-clock time (4 slices, 50ms apart).
    let mut status = ems.parent_order_status(parent_id).await.unwrap();
    for _ in 0..50 {
        if matches!(status, Some(ParentOrderStatus::Completed)) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        status = ems.parent_order_status(parent_id).await.unwrap();
    }

    assert_eq!(status, Some(ParentOrderStatus::Completed));
    assert_eq!(sink.submitted.lock().await.len(), 4);
    let total: rust_decimal::Decimal = sink
        .submitted
        .lock()
        .await
        .iter()
        .map(|intent| intent.quantity)
        .sum();
    assert_eq!(total, dec!(4));
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn recovering_a_scheduling_parent_order_resumes_from_where_it_left_off() {
    let fixture = TestSchema::new().await;
    let sink = Arc::new(RecordingSink::new());
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();

    // Simulate a crash: insert a parent order directly with 2 of 4
    // slices already "submitted" (filled_quantity = 2), no
    // PostgresEms instance alive to schedule it yet.
    sqlx::query(apexes_ems::postgres::PARENT_ORDERS_TABLE_DDL)
        .execute(&pool)
        .await
        .ok(); // idempotent create, ignore "already exists"-shaped errors from a fresh clean_db()

    let parent_id = uuid::Uuid::new_v4();
    let algo_spec = serde_json::json!({"kind": "Twap", "duration_secs": 1, "slices": 4});
    let routing = serde_json::json!({"kind": "Fixed"});
    sqlx::query(
        "INSERT INTO parent_orders (id, symbol, venue, side, total_quantity, filled_quantity, algo_spec, routing, status, failure_reason, created_at, updated_at)
         VALUES ($1, 'BTCUSDT', 'Binance', 'Buy', 4, 2, $2, $3, 'Scheduling', NULL, now(), now())",
    )
    .bind(parent_id)
    .bind(algo_spec)
    .bind(routing)
    .execute(&pool)
    .await
    .unwrap();

    // Constructing a NEW PostgresEms should recover this row and
    // resume scheduling (submitting only the REMAINING quantity, not
    // the already-recorded 2 units).
    let ems = PostgresEms::new(
        &fixture.url,
        sink.clone(),
        Arc::new(PriceAndFeeScorer),
        HashMap::new(),
        None,
    )
    .await
    .unwrap();

    let mut status = ems
        .parent_order_status(apexes_strategy::ParentOrderId(parent_id))
        .await
        .unwrap();
    for _ in 0..50 {
        if matches!(status, Some(ParentOrderStatus::Completed)) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        status = ems
            .parent_order_status(apexes_strategy::ParentOrderId(parent_id))
            .await
            .unwrap();
    }

    assert_eq!(status, Some(ParentOrderStatus::Completed));
    let total: rust_decimal::Decimal = sink
        .submitted
        .lock()
        .await
        .iter()
        .map(|intent| intent.quantity)
        .sum();
    assert_eq!(total, dec!(2)); // only the REMAINING 2 units, not the already-recorded 2
    fixture.cleanup().await;
}

fn pov_request() -> AlgoOrderRequest {
    AlgoOrderRequest {
        execution_algo: ExecutionAlgoSpec::Pov {
            participation_rate: dec!(0.5),
        },
        ..twap_request()
    }
}

fn trade(venue: Venue, symbol: &str) -> apexes_core::MarketEvent {
    apexes_core::MarketEvent::Trade(apexes_core::TradeEvent {
        symbol: symbol.into(),
        venue,
        quantity: dec!(2),
        price: dec!(100),
        side: None,
        trade_id: None,
        timestamp: chrono::Utc::now(),
    })
}

async fn wait_completed(ems: &PostgresEms, id: apexes_strategy::ParentOrderId) {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if ems.parent_order_status(id).await.unwrap() == Some(ParentOrderStatus::Completed) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore]
async fn pov_index_matches_venue_and_symbol_and_recovers_from_persisted_quantity() {
    let fixture = TestSchema::new().await;
    let sink = Arc::new(RecordingSink::new());
    let ems = PostgresEms::new(
        &fixture.url,
        sink.clone(),
        Arc::new(PriceAndFeeScorer),
        HashMap::new(),
        None,
    )
    .await
    .unwrap();
    let id = ems.submit_algo_order(pov_request()).await.unwrap();
    for event in [
        trade(Venue::Okx, "BTCUSDT"),
        trade(Venue::Binance, "ETHUSDT"),
    ] {
        ems.on_market_event(&event).await.unwrap();
    }
    assert!(sink.submitted.lock().await.is_empty());
    ems.on_market_event(&trade(Venue::Binance, "BTCUSDT"))
        .await
        .unwrap();
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let quantity: rust_decimal::Decimal = sqlx::query_scalar(
                "SELECT filled_quantity FROM parent_orders WHERE id = $1 AND NOT dispatch_pending",
            )
            .bind(id.0)
            .fetch_optional(&pool)
            .await
            .unwrap()
            .unwrap_or_default();
            if quantity == dec!(1) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(ems);
    let ems = PostgresEms::new(
        &fixture.url,
        sink.clone(),
        Arc::new(PriceAndFeeScorer),
        HashMap::new(),
        None,
    )
    .await
    .unwrap();
    for _ in 0..5 {
        ems.on_market_event(&trade(Venue::Binance, "BTCUSDT"))
            .await
            .unwrap();
    }
    wait_completed(&ems, id).await;
    assert_eq!(sink.submitted.lock().await.len(), 4);
    let quantity: rust_decimal::Decimal =
        sqlx::query_scalar("SELECT filled_quantity FROM parent_orders WHERE id=$1")
            .bind(id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(quantity, dec!(4));
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM parent_order_children WHERE parent_order_id=$1")
            .bind(id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 4);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn cancelled_pov_is_removed_from_index_and_never_dispatched() {
    let fixture = TestSchema::new().await;
    let sink = Arc::new(RecordingSink::new());
    let ems = PostgresEms::new(
        &fixture.url,
        sink.clone(),
        Arc::new(PriceAndFeeScorer),
        HashMap::new(),
        None,
    )
    .await
    .unwrap();
    let id = ems.submit_algo_order(pov_request()).await.unwrap();
    ems.cancel_parent_order(id).await.unwrap();
    for _ in 0..100 {
        ems.on_market_event(&trade(Venue::Binance, "BTCUSDT"))
            .await
            .unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(
        ems.parent_order_status(id).await.unwrap(),
        Some(ParentOrderStatus::Cancelled)
    );
    assert!(sink.submitted.lock().await.is_empty());
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn unresolved_child_marker_refuses_restart_without_submitting_replacement() {
    let fixture = TestSchema::new().await;
    let sink = Arc::new(RecordingSink::new());
    let ems = PostgresEms::new(
        &fixture.url,
        sink.clone(),
        Arc::new(PriceAndFeeScorer),
        HashMap::new(),
        None,
    )
    .await
    .unwrap();
    let id = ems.submit_algo_order(pov_request()).await.unwrap();
    drop(ems);
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    sqlx::query("UPDATE parent_orders SET dispatch_pending=TRUE WHERE id=$1")
        .bind(id.0)
        .execute(&pool)
        .await
        .unwrap();
    let ks = Arc::new(apexes_oms::KillSwitch::new());
    let result = PostgresEms::new_with_kill_switch(
        &fixture.url,
        sink.clone(),
        Arc::new(PriceAndFeeScorer),
        HashMap::new(),
        None,
        Some(ks.clone()),
    )
    .await;
    assert!(matches!(result, Err(EmsError::SchedulingSuspended)));
    assert!(ks.is_engaged());
    assert!(sink.submitted.lock().await.is_empty());
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn child_link_write_failure_latches_halt_and_preserves_pending_marker() {
    let fixture = TestSchema::new().await;
    let sink = Arc::new(RecordingSink::new());
    let ks = Arc::new(apexes_oms::KillSwitch::new());
    let ems = PostgresEms::new_with_kill_switch(
        &fixture.url,
        sink.clone(),
        Arc::new(PriceAndFeeScorer),
        HashMap::new(),
        None,
        Some(ks.clone()),
    )
    .await
    .unwrap();
    let id = ems.submit_algo_order(pov_request()).await.unwrap();
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    // Make the child-link INSERT fail after the sink has accepted the order.
    sqlx::query(
        "ALTER TABLE parent_order_children ADD CONSTRAINT inject_link_failure CHECK (FALSE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    ems.on_market_event(&trade(Venue::Binance, "BTCUSDT"))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !ks.is_engaged() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(sink.submitted.lock().await.len(), 1);
    let (pending, quantity): (bool, rust_decimal::Decimal) =
        sqlx::query_as("SELECT dispatch_pending, filled_quantity FROM parent_orders WHERE id=$1")
            .bind(id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(pending);
    let dispatch = ems.pending_dispatch(id).await.unwrap().unwrap();
    assert!(!dispatch.is_nil());
    assert_eq!(quantity, dec!(0));
    for _ in 0..10 {
        ems.on_market_event(&trade(Venue::Binance, "BTCUSDT"))
            .await
            .unwrap();
    }
    drop(ems);
    let result = PostgresEms::new(
        &fixture.url,
        sink.clone(),
        Arc::new(PriceAndFeeScorer),
        HashMap::new(),
        None,
    )
    .await;
    assert!(matches!(result, Err(EmsError::SchedulingSuspended)));
    assert_eq!(sink.submitted.lock().await.len(), 1);
    fixture.cleanup().await;
}

struct SlowSink {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    submitted: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl ChildOrderSink for SlowSink {
    async fn submit_child_order(
        &self,
        _: ChildOrderIntent,
    ) -> Result<(OrderId, Vec<apexes_core::Fill>), EmsError> {
        self.submitted
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;
        Ok((OrderId(uuid::Uuid::new_v4()), Vec::new()))
    }

    async fn cancel_child_order(&self, _: OrderId) -> Result<(), EmsError> {
        Ok(())
    }

    async fn order_status(&self, _: OrderId) -> Result<Option<apexes_core::Order>, EmsError> {
        Ok(None)
    }
}

#[tokio::test]
#[ignore]
async fn slow_child_does_not_block_market_events_and_queue_overflow_halts() {
    let fixture = TestSchema::new().await;
    let sink = Arc::new(SlowSink {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        submitted: std::sync::atomic::AtomicUsize::new(0),
    });
    let ks = Arc::new(apexes_oms::KillSwitch::new());
    let ems = PostgresEms::new_with_kill_switch(
        &fixture.url,
        sink.clone(),
        Arc::new(PriceAndFeeScorer),
        HashMap::new(),
        None,
        Some(ks.clone()),
    )
    .await
    .unwrap();
    let id = ems.submit_algo_order(pov_request()).await.unwrap();
    ems.on_market_event(&trade(Venue::Binance, "BTCUSDT"))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), sink.entered.notified())
        .await
        .unwrap();
    // The exchange acknowledgement is deliberately outstanding. Even
    // matching trades must return without awaiting the slow child sink.
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        for _ in 0..300 {
            ems.on_market_event(&trade(Venue::Binance, "BTCUSDT"))
                .await
                .unwrap();
        }
    })
    .await
    .unwrap();
    assert!(ks.is_engaged());
    assert!(ks.reason().unwrap().contains("queue overflow"));
    sink.release.notify_one();
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let (pending, quantity): (bool, rust_decimal::Decimal) = sqlx::query_as(
                "SELECT dispatch_pending, filled_quantity FROM parent_orders WHERE id=$1",
            )
            .bind(id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
            if !pending && quantity == dec!(1) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(sink.submitted.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(ks.is_engaged());
    fixture.cleanup().await;
}
