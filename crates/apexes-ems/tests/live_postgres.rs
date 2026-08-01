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

async fn clean_db() {
    let pool = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
    sqlx::query("DROP TABLE IF EXISTS parent_order_children, parent_orders")
        .execute(&pool)
        .await
        .unwrap();
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
    }
}

#[tokio::test]
#[ignore]
async fn twap_parent_order_persists_and_completes() {
    clean_db().await;
    let sink = Arc::new(RecordingSink::new());
    let ems = PostgresEms::new(
        POSTGRES_URL,
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
}

#[tokio::test]
#[ignore]
async fn recovering_a_scheduling_parent_order_resumes_from_where_it_left_off() {
    clean_db().await;
    let sink = Arc::new(RecordingSink::new());
    let pool = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();

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
        POSTGRES_URL,
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
}
