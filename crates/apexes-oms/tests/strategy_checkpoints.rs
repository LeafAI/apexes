//! F04.05: real PostgreSQL durable checkpoint head, publication and exact
//! recovery selection. Dedicated schema in the disposable test DB only.
use apexes_core::MarketEvent;
use apexes_oms::{CheckpointStoreError as Error, OrderStore};
use apexes_strategy::checkpoint::{
    CheckpointError, CheckpointReceipt, StateCursor, StateIdentity, ValidatedState, capture,
    restore, verify_checkpoint,
};
use apexes_strategy::{OrderIntent, Strategy, StrategyContext};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

const URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

struct Fixture {
    admin: PgPool,
    schema: String,
    url: String,
}

impl Fixture {
    async fn new() -> Self {
        let admin = PgPool::connect(URL).await.unwrap();
        let schema = format!("checkpoint_test_{}", Uuid::new_v4().simple());
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let url = format!("{URL}?options[search_path]={schema}");
        Self { admin, schema, url }
    }

    async fn store(&self) -> OrderStore {
        let mut store = OrderStore::new(&self.url).await.unwrap();
        store.bind_spot_inventory("checkpoint-owner").await.unwrap();
        store
    }

    async fn pool(&self) -> PgPool {
        PgPool::connect(&self.url).await.unwrap()
    }

    async fn cleanup(self) {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await
        .unwrap();
    }
}

#[derive(Default)]
struct Counter(u64);

impl Strategy for Counter {
    fn on_market_event(&mut self, _: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
        Vec::new()
    }
    fn checkpoint_schema_version(&self) -> Option<u32> {
        Some(1)
    }
    fn checkpoint_payload(&self) -> Result<String, CheckpointError> {
        Ok(self.0.to_string())
    }
    fn restore_checkpoint(&mut self, state: &ValidatedState<'_>) -> Result<(), CheckpointError> {
        self.0 = state
            .payload()
            .parse()
            .map_err(|_| CheckpointError::Payload)?;
        Ok(())
    }
}

fn identity() -> StateIdentity {
    StateIdentity {
        strategy: "counter".into(),
        revision: "v1".into(),
        config_sha256: "f".repeat(64),
        scope: "live:checkpoint-owner:BTCUSDT".into(),
        payload_version: 1,
    }
}

fn cursor(ordinal: u64, ns: u32) -> StateCursor {
    StateCursor {
        ordinal,
        observed_at: DateTime::<Utc>::from_timestamp(1_800_000_000, ns).unwrap(),
    }
}

fn bytes(value: u64, cursor: StateCursor) -> Vec<u8> {
    capture(&Counter(value), identity(), cursor).unwrap()
}

async fn publish(
    store: &mut OrderStore,
    previous: Option<&CheckpointReceipt>,
    cursor: StateCursor,
    value: u64,
) -> CheckpointReceipt {
    store
        .publish_strategy_checkpoint(previous, &identity(), cursor, &bytes(value, cursor))
        .await
        .unwrap()
}

#[tokio::test]
#[ignore]
async fn acknowledged_head_survives_reopen_and_exact_retry() {
    let fixture = Fixture::new().await;
    let mut store = fixture.store().await;
    assert!(
        store
            .load_strategy_checkpoint(None)
            .await
            .unwrap()
            .is_none()
    );
    // PostgreSQL timestamps only retain microseconds. The receipt retains
    // exact nanoseconds and the full u64 observation ordinal.
    let first_cursor = cursor(u64::MAX - 1, 123);
    let next_cursor = cursor(u64::MAX, 124);
    let first = publish(&mut store, None, first_cursor, 10).await;
    assert_eq!(publish(&mut store, None, first_cursor, 10).await, first);
    assert!(matches!(
        store.load_strategy_checkpoint(None).await,
        Err(Error::Unexpected)
    ));
    let second = publish(&mut store, Some(&first), next_cursor, 11).await;
    drop(store);

    let mut store = fixture.store().await;
    assert_eq!(
        publish(&mut store, Some(&first), next_cursor, 11).await,
        second
    );
    assert!(matches!(
        store.load_strategy_checkpoint(Some(&first)).await,
        Err(Error::Conflict)
    ));
    let loaded = store
        .load_strategy_checkpoint(Some(&second))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded, bytes(11, next_cursor));
    assert_eq!(
        verify_checkpoint(&loaded, &identity(), next_cursor).unwrap(),
        second
    );
    let mut target = Counter(99);
    restore(
        &mut target,
        Counter::default(),
        &loaded,
        &identity(),
        next_cursor,
    )
    .unwrap();
    assert_eq!(target.0, 11);
    let mut wrong = second.clone();
    wrong.cursor.observed_at = cursor(u64::MAX, 125).observed_at;
    assert!(matches!(
        store.load_strategy_checkpoint(Some(&wrong)).await,
        Err(Error::Conflict)
    ));
    drop(store);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn publication_rejects_stale_or_conflicting_predecessors_and_cursor_regression() {
    let fixture = Fixture::new().await;
    let mut store = fixture.store().await;
    let c1 = cursor(1, 100);
    let c2 = cursor(2, 200);
    let c3 = cursor(3, 300);
    let expected = verify_checkpoint(&bytes(1, c1), &identity(), c1).unwrap();
    assert!(matches!(
        store.load_strategy_checkpoint(Some(&expected)).await,
        Err(Error::Missing)
    ));
    assert!(matches!(
        store
            .publish_strategy_checkpoint(Some(&expected), &identity(), c2, &bytes(2, c2))
            .await,
        Err(Error::Missing)
    ));
    let first = publish(&mut store, None, c1, 1).await;
    let next = publish(&mut store, Some(&first), c2, 2).await;
    for prior in [None, Some(&first)] {
        assert!(matches!(
            store
                .publish_strategy_checkpoint(prior, &identity(), c3, &bytes(3, c3))
                .await,
            Err(Error::Conflict)
        ));
    }
    // Same cursor but different state and a timestamp regression both fail.
    for c in [c2, cursor(1, 400), cursor(3, 199)] {
        assert!(matches!(
            store
                .publish_strategy_checkpoint(Some(&next), &identity(), c, &bytes(999, c))
                .await,
            Err(Error::NonMonotonic)
        ));
    }
    let mut changed = identity();
    changed.revision = "v2".into();
    let changed_bytes = capture(&Counter(3), changed.clone(), c3).unwrap();
    assert!(matches!(
        store
            .publish_strategy_checkpoint(Some(&next), &changed, c3, &changed_bytes)
            .await,
        Err(Error::NonMonotonic)
    ));
    let mut corrupt = bytes(3, c3);
    corrupt.pop();
    assert!(matches!(
        store
            .publish_strategy_checkpoint(Some(&next), &identity(), c3, &corrupt)
            .await,
        Err(Error::Envelope(_))
    ));
    assert_eq!(
        store
            .load_strategy_checkpoint(Some(&next))
            .await
            .unwrap()
            .unwrap(),
        bytes(2, c2)
    );
    // Equal observation time is valid for a strictly later ordinal.
    let c3 = cursor(3, 200);
    publish(&mut store, Some(&next), c3, 3).await;
    drop(store);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn failed_transaction_preserves_previous_head_and_corruption_blocks_recovery() {
    let fixture = Fixture::new().await;
    let mut store = fixture.store().await;
    let pool = fixture.pool().await;
    let c1 = cursor(1, 100);
    let c2 = cursor(2, 200);
    let first = publish(&mut store, None, c1, 1).await;
    // A deferred trigger fails at COMMIT, after the proposed head write.
    sqlx::query("CREATE FUNCTION refuse_checkpoint_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected commit failure'; END; $$")
        .execute(&pool).await.unwrap();
    sqlx::query("CREATE CONSTRAINT TRIGGER checkpoint_commit_failure AFTER UPDATE ON strategy_checkpoint_head DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION refuse_checkpoint_commit()")
        .execute(&pool).await.unwrap();
    assert!(matches!(
        store
            .publish_strategy_checkpoint(Some(&first), &identity(), c2, &bytes(2, c2))
            .await,
        Err(Error::Database)
    ));
    drop(store);
    let mut store = fixture.store().await;
    assert_eq!(
        store
            .load_strategy_checkpoint(Some(&first))
            .await
            .unwrap()
            .unwrap(),
        bytes(1, c1)
    );
    sqlx::query("DROP TRIGGER checkpoint_commit_failure ON strategy_checkpoint_head")
        .execute(&pool)
        .await
        .unwrap();
    let second = publish(&mut store, Some(&first), c2, 2).await;
    // A valid old envelope under a new receipt is still corrupt.
    sqlx::query("UPDATE strategy_checkpoint_head SET envelope=$1")
        .bind(bytes(1, c1))
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.load_strategy_checkpoint(Some(&second)).await,
        Err(Error::Corrupt)
    ));
    assert!(matches!(
        store.load_strategy_checkpoint(Some(&first)).await,
        Err(Error::Corrupt)
    ));
    assert!(matches!(
        store
            .publish_strategy_checkpoint(
                Some(&second),
                &identity(),
                cursor(3, 300),
                &bytes(3, cursor(3, 300))
            )
            .await,
        Err(Error::Corrupt)
    ));
    drop(store);
    pool.close().await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore]
async fn bound_owner_excludes_unbound_and_competing_access_and_orphan_adoption() {
    let fixture = Fixture::new().await;
    let mut unbound = OrderStore::new(&fixture.url).await.unwrap();
    let c = cursor(1, 0);
    assert!(matches!(
        unbound.load_strategy_checkpoint(None).await,
        Err(Error::Unbound)
    ));
    assert!(matches!(
        unbound
            .publish_strategy_checkpoint(None, &identity(), c, &bytes(1, c))
            .await,
        Err(Error::Unbound)
    ));
    let mut owner = fixture.store().await;
    let receipt = publish(&mut owner, None, c, 1).await;
    assert!(
        unbound
            .bind_spot_inventory("checkpoint-owner")
            .await
            .is_err()
    );
    drop(owner);
    assert!(
        unbound
            .bind_spot_inventory("different-owner")
            .await
            .is_err()
    );
    unbound
        .bind_spot_inventory("checkpoint-owner")
        .await
        .unwrap();
    let pool = fixture.pool().await;
    sqlx::query("UPDATE strategy_checkpoint_head SET owner='wrong-owner'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        unbound.load_strategy_checkpoint(Some(&receipt)).await,
        Err(Error::Owner)
    ));
    sqlx::query("DELETE FROM spot_inventory_owner")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        unbound.load_strategy_checkpoint(Some(&receipt)).await,
        Err(Error::Owner)
    ));
    drop(unbound);
    let mut reopened = OrderStore::new(&fixture.url).await.unwrap();
    assert!(
        reopened
            .bind_spot_inventory("checkpoint-owner")
            .await
            .is_err()
    );
    drop(reopened);
    pool.close().await;
    fixture.cleanup().await;
}
