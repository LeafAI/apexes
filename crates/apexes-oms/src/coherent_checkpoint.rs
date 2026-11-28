//! Atomic checkpoint and current-process lifecycle prefix evidence.
//! Direct managed Spot only, at an explicitly requested quiescent stop.
//! Verification is read-only; this does not authorize repeatable restart.
use super::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAX_JOURNAL_ROWS: i64 = 100_000;
const PAGE: i64 = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoherentCheckpointReceipt {
    pub version: u32,
    pub checkpoint: CheckpointReceipt,
    pub run_scope: Uuid,
    pub last_intent: u64,
    pub fill_sequence: i64,
    pub terminal_sequence: i64,
    pub order_count: i64,
    pub fill_count: i64,
    pub journal_sha256: String,
}

/// Constructed only from the manager's contiguous, process-local progress.
/// The caller still owns the actual synchronous strategy observation and
/// attempt callback boundary, not inferred from database rows.
#[derive(Clone, Copy)]
pub(crate) struct CompletedPrefix {
    pub run_scope: Uuid,
    pub last_intent: u64,
    pub fill_sequence: i64,
    pub terminal_sequence: i64,
}

pub(super) async fn migrate(pool: &PgPool) -> std::result::Result<(), OmsError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS strategy_checkpoint_boundary (
        singleton BOOLEAN PRIMARY KEY CHECK (singleton),
        owner TEXT NOT NULL,
        receipt TEXT NOT NULL CHECK (octet_length(receipt) BETWEEN 1 AND 16384)
    )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub(super) async fn require_absent(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM strategy_checkpoint_boundary)")
            .fetch_one(&mut **tx)
            .await?;
    if exists {
        return Err(CheckpointStoreError::Boundary);
    }
    Ok(())
}

// Locks prevent background writes between verification and publication.
// NOWAIT deliberately refuses a busy boundary instead of holding the
// manager lock while waiting on a competing database writer.
async fn lock_journal(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query("SET LOCAL statement_timeout = '5s'")
        .execute(&mut **tx)
        .await?;
    sqlx::query("LOCK TABLE orders, fills, spot_parent_allocations IN SHARE MODE NOWAIT")
        .execute(&mut **tx)
        .await?;
    for (exists, lock, rows) in [
        (
            "SELECT to_regclass('parent_orders') IS NOT NULL",
            "LOCK TABLE parent_orders IN SHARE MODE NOWAIT",
            "SELECT EXISTS(SELECT 1 FROM parent_orders)",
        ),
        (
            "SELECT to_regclass('parent_order_children') IS NOT NULL",
            "LOCK TABLE parent_order_children IN SHARE MODE NOWAIT",
            "SELECT EXISTS(SELECT 1 FROM parent_order_children)",
        ),
    ] {
        if sqlx::query_scalar::<_, bool>(exists)
            .fetch_one(&mut **tx)
            .await?
        {
            sqlx::query(lock).execute(&mut **tx).await?;
            if sqlx::query_scalar::<_, bool>(rows)
                .fetch_one(&mut **tx)
                .await?
            {
                return Err(CheckpointStoreError::Boundary);
            }
        }
    }
    if sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM spot_parent_allocations)")
        .fetch_one(&mut **tx)
        .await?
    {
        return Err(CheckpointStoreError::Boundary);
    }
    sqlx::query("SET LOCAL TIME ZONE 'UTC'")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn hash_rows(
    tx: &mut Transaction<'_, Postgres>,
    query: &'static str,
    count: i64,
    digest: &mut Sha256,
) -> Result<()> {
    if !(0..=MAX_JOURNAL_ROWS).contains(&count) {
        return Err(CheckpointStoreError::Boundary);
    }
    let mut after: Option<Uuid> = None;
    let mut seen = 0;
    loop {
        let rows: Vec<(Uuid, Vec<u8>)> = sqlx::query_as(query)
            .bind(after)
            .bind(PAGE)
            .fetch_all(&mut **tx)
            .await?;
        if rows.is_empty() {
            break;
        }
        for (id, hash) in rows {
            if hash.len() != 32 || after.is_some_and(|previous| id <= previous) {
                return Err(CheckpointStoreError::Corrupt);
            }
            digest.update(id.as_bytes());
            digest.update(hash);
            after = Some(id);
            seen += 1;
            if seen > count {
                return Err(CheckpointStoreError::Corrupt);
            }
        }
    }
    if seen != count {
        return Err(CheckpointStoreError::Corrupt);
    }
    Ok(())
}

async fn witness(
    tx: &mut Transaction<'_, Postgres>,
    prefix: CompletedPrefix,
    checkpoint: CheckpointReceipt,
) -> Result<CoherentCheckpointReceipt> {
    if prefix.run_scope.is_nil() || prefix.fill_sequence < 0 || prefix.terminal_sequence < 0 {
        return Err(CheckpointStoreError::Boundary);
    }
    lock_journal(tx).await?;
    // No old, unlinked, working, uncertain, breached, or EMS executions.
    // Every accepted direct order must have an authoritative terminal
    // marker; definite submission refusals have only the attempt callback.
    let invalid_orders: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM orders o WHERE
          o.intent_scope IS DISTINCT FROM $1 OR o.intent_ordinal IS NULL
          OR o.intent_ordinal <= 0 OR o.intent_ordinal > $2
          OR o.asset_class <> 'Spot' OR o.venue <> 'Binance'
          OR o.parent_allocation_id IS NOT NULL OR o.ems_parent_id IS NOT NULL
          OR o.ems_dispatch_id IS NOT NULL OR o.reservation_breached
          OR NOT (
            (o.status IN ('Filled','Cancelled','Rejected') AND o.submission_state='Accepted'
             AND o.exchange_order_id IS NOT NULL AND o.exchange_order_id <> ''
             AND o.terminal_delivery_seq IS NOT NULL AND o.terminal_delivery_seq > 0)
            OR (o.status='Rejected' AND o.submission_state='Rejected'
             AND o.exchange_order_id IS NULL AND o.filled_quantity=0 AND o.terminal_delivery_seq IS NULL)
          )
          OR o.filled_quantity <> COALESCE((SELECT SUM(f.quantity) FROM fills f WHERE f.order_id=o.id),0)
        )",
    ).bind(prefix.run_scope).bind(Decimal::from(prefix.last_intent)).fetch_one(&mut **tx).await?;
    let (orders, terminal): (i64, i64) =
        sqlx::query_as("SELECT COUNT(*), COALESCE(MAX(terminal_delivery_seq),0) FROM orders")
            .fetch_one(&mut **tx)
            .await?;
    let (fills, fill_seq, bad_fills): (i64,i64,i64) = sqlx::query_as(
        "SELECT COUNT(*), COALESCE(MAX(delivery_seq),0),
          COUNT(*) FILTER (WHERE delivery_seq IS NULL OR delivery_seq <= 0 OR exchange_trade_id = '' OR quantity <= 0)
         FROM fills",
    ).fetch_one(&mut **tx).await?;
    if invalid_orders
        || bad_fills != 0
        || terminal != prefix.terminal_sequence
        || fill_seq != prefix.fill_sequence
    {
        return Err(CheckpointStoreError::Boundary);
    }
    // Fixed-size hashes of canonical JSONB row projections, keyed and
    // ordered by UUID. Include every order/fill column, including fee
    // evidence; later changes conservatively invalidate this witness.
    // Limit the total history and memory; never aggregate an unbounded blob.
    let mut digest = Sha256::new();
    digest.update(b"apexes-coherent-journal-v1\0orders\0");
    digest.update(orders.to_be_bytes());
    hash_rows(tx, "SELECT id, sha256(convert_to(to_jsonb(t)::text,'UTF8')) FROM orders t WHERE ($1::uuid IS NULL OR id>$1) ORDER BY id LIMIT $2", orders, &mut digest).await?;
    digest.update(b"\0fills\0");
    digest.update(fills.to_be_bytes());
    hash_rows(tx, "SELECT id, sha256(convert_to(to_jsonb(t)::text,'UTF8')) FROM fills t WHERE ($1::uuid IS NULL OR id>$1) ORDER BY id LIMIT $2", fills, &mut digest).await?;
    Ok(CoherentCheckpointReceipt {
        version: 1,
        checkpoint,
        run_scope: prefix.run_scope,
        last_intent: prefix.last_intent,
        fill_sequence: fill_seq,
        terminal_sequence: terminal,
        order_count: orders,
        fill_count: fills,
        journal_sha256: format!("{:x}", digest.finalize()),
    })
}

impl OrderStore {
    pub(crate) async fn publish_coherent_boundary(
        &mut self,
        previous: &CheckpointReceipt,
        cursor: StateCursor,
        envelope: &[u8],
        prefix: CompletedPrefix,
    ) -> Result<CoherentCheckpointReceipt> {
        let next = verify_checkpoint(envelope, &previous.identity, cursor)?;
        validate_transition(Some(previous), &next)?;
        let (owner, connection) = self.checkpoint_connection()?;
        let mut tx = connection.begin().await?;
        let result = async {
            require_absent(&mut tx).await?;
            // The one-time claim is the only eligible predecessor. No
            // inferred history or checkpoint publication from legacy runs.
            let claim: Option<(String,String)> = sqlx::query_as(
                "SELECT owner, receipt FROM strategy_checkpoint_startup_claim WHERE singleton FOR UPDATE",
            ).fetch_optional(&mut *tx).await?;
            let previous_json = serde_json::to_string(previous).map_err(|_| CheckpointStoreError::Corrupt)?;
            if claim != Some((owner.to_owned(), previous_json)) {
                return Err(CheckpointStoreError::Boundary);
            }
            select_head(&mut tx, owner, Some(previous)).await?;
            let receipt = witness(&mut tx, prefix, next.clone()).await?;
            let head_json = serde_json::to_string(&next).map_err(|_| CheckpointStoreError::Corrupt)?;
            write_head(&mut tx, owner, Some(previous), &next, &head_json, envelope).await?;
            let receipt_json = serde_json::to_string(&receipt).map_err(|_| CheckpointStoreError::Corrupt)?;
            sqlx::query("INSERT INTO strategy_checkpoint_boundary (singleton, owner, receipt) VALUES (TRUE,$1,$2)")
                .bind(owner).bind(receipt_json).execute(&mut *tx).await?;
            Ok(receipt)
        }.await;
        finish(tx, result).await
    }

    /// Read-only qualification against an independently retained exact
    /// receipt. This is intentionally not a restore/startup API.
    pub async fn verify_coherent_boundary(
        &mut self,
        expected: &CoherentCheckpointReceipt,
    ) -> Result<()> {
        let (owner, connection) = self.checkpoint_connection()?;
        let mut tx = connection.begin().await?;
        let result = async {
            if expected.version != 1 {
                return Err(CheckpointStoreError::Corrupt);
            }
            select_head(&mut tx, owner, Some(&expected.checkpoint)).await?;
            let row: Option<(String, String)> = sqlx::query_as(
                "SELECT owner, receipt FROM strategy_checkpoint_boundary WHERE singleton",
            )
            .fetch_optional(&mut *tx)
            .await?;
            let encoded =
                serde_json::to_string(expected).map_err(|_| CheckpointStoreError::Corrupt)?;
            if row != Some((owner.to_owned(), encoded)) {
                return Err(CheckpointStoreError::Conflict);
            }
            let prefix = CompletedPrefix {
                run_scope: expected.run_scope,
                last_intent: expected.last_intent,
                fill_sequence: expected.fill_sequence,
                terminal_sequence: expected.terminal_sequence,
            };
            let actual = witness(&mut tx, prefix, expected.checkpoint.clone()).await?;
            if &actual != expected {
                return Err(CheckpointStoreError::JournalChanged);
            }
            Ok(())
        }
        .await;
        finish(tx, result).await
    }
}
