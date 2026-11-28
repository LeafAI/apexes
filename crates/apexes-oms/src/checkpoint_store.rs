//! F04.05: owner-bound, transactional checkpoint head and exact recovery
//! selection. Not a runner integration or an atomic OMS/callback checkpoint.
use apexes_strategy::checkpoint::{
    CheckpointError, CheckpointReceipt, StateCursor, StateIdentity, verify_checkpoint,
};
use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection, PgPool, Postgres, Transaction};

use super::OrderStore;

#[path = "coherent_checkpoint.rs"]
mod coherent_checkpoint;
use crate::OmsError;
pub use coherent_checkpoint::CoherentCheckpointReceipt;
pub use coherent_checkpoint::CoherentRecoverySnapshot;
pub(crate) use coherent_checkpoint::CompletedPrefix;

#[derive(Debug, thiserror::Error)]
pub enum CheckpointStoreError {
    #[error("checkpoint store requires a bound inventory lease")]
    Unbound,
    #[error("checkpoint store owner does not match its bound inventory owner")]
    Owner,
    #[error("required checkpoint is missing")]
    Missing,
    #[error("checkpoint exists; explicit recovery receipt required")]
    Unexpected,
    #[error("checkpoint startup has already been claimed; atomic recovery is not available")]
    AlreadyClaimed,
    #[error("checkpoint startup requires an execution-history-free inventory")]
    ExecutionHistory,
    #[error("checkpoint startup requires the same database URL as the inventory")]
    DatabaseScope,
    #[error("checkpoint startup requires a fresh reconciled managed session")]
    StartupPhase,
    #[error("checkpoint boundary is not quiescent or has unverified lifecycle progress")]
    Boundary,
    #[error("checkpoint journal differs from its committed boundary")]
    JournalChanged,
    #[error("checkpoint receipt does not match current head")]
    Conflict,
    #[error("checkpoint publication would change identity or regress the observation cursor")]
    NonMonotonic,
    #[error("invalid stored checkpoint receipt or envelope")]
    Corrupt,
    #[error("checkpoint validation failed: {0}")]
    Envelope(#[from] CheckpointError),
    // Do not include database error detail here: it may contain payload data.
    #[error("checkpoint database operation failed; publication outcome may be unknown")]
    Database,
}

impl From<sqlx::Error> for CheckpointStoreError {
    fn from(_: sqlx::Error) -> Self {
        Self::Database
    }
}

type Result<T> = std::result::Result<T, CheckpointStoreError>;

#[derive(sqlx::FromRow)]
struct StoredHead {
    owner: String,
    receipt: String,
    envelope: Vec<u8>,
}

impl StoredHead {
    fn verified(&self, owner: &str) -> Result<CheckpointReceipt> {
        if self.owner != owner {
            return Err(CheckpointStoreError::Owner);
        }
        let receipt: CheckpointReceipt =
            serde_json::from_str(&self.receipt).map_err(|_| CheckpointStoreError::Corrupt)?;
        if serde_json::to_string(&receipt).map_err(|_| CheckpointStoreError::Corrupt)?
            != self.receipt
        {
            return Err(CheckpointStoreError::Corrupt);
        }
        let verified = verify_checkpoint(&self.envelope, &receipt.identity, receipt.cursor)
            .map_err(|_| CheckpointStoreError::Corrupt)?;
        if verified != receipt {
            return Err(CheckpointStoreError::Corrupt);
        }
        Ok(receipt)
    }
}

async fn read_head(tx: &mut Transaction<'_, Postgres>, owner: &str) -> Result<Option<StoredHead>> {
    // Validate the binding in the transaction holding the dedicated
    // advisory-lock connection. A lost connection is never replaced.
    let bound: Option<String> =
        sqlx::query_scalar("SELECT owner FROM spot_inventory_owner WHERE singleton FOR SHARE")
            .fetch_optional(&mut **tx)
            .await?;
    if bound.as_deref() != Some(owner) {
        return Err(CheckpointStoreError::Owner);
    }
    Ok(sqlx::query_as(
        "SELECT owner, receipt, envelope FROM strategy_checkpoint_head WHERE singleton FOR UPDATE",
    )
    .fetch_optional(&mut **tx)
    .await?)
}

fn validate_transition(
    previous: Option<&CheckpointReceipt>,
    next: &CheckpointReceipt,
) -> Result<()> {
    if let Some(previous) = previous
        && (previous.identity != next.identity
            || previous.cursor.ordinal >= next.cursor.ordinal
            || previous.cursor.observed_at > next.cursor.observed_at)
    {
        return Err(CheckpointStoreError::NonMonotonic);
    }
    Ok(())
}

async fn finish<T>(tx: Transaction<'_, Postgres>, result: Result<T>) -> Result<T> {
    match result {
        Ok(value) => {
            tx.commit().await?;
            Ok(value)
        }
        Err(error) => {
            // This lease connection survives the operation. Flush rollback
            // now, rather than leaving row locks held until its next query.
            tx.rollback().await?;
            Err(error)
        }
    }
}

async fn select_head(
    tx: &mut Transaction<'_, Postgres>,
    owner: &str,
    expected: Option<&CheckpointReceipt>,
) -> Result<Option<Vec<u8>>> {
    match (expected, read_head(tx, owner).await?) {
        (None, None) => Ok(None),
        (Some(_), None) => Err(CheckpointStoreError::Missing),
        (None, Some(_)) => Err(CheckpointStoreError::Unexpected),
        (Some(expected), Some(head)) => {
            if &head.verified(owner)? != expected {
                return Err(CheckpointStoreError::Conflict);
            }
            Ok(Some(head.envelope))
        }
    }
}

async fn write_head(
    tx: &mut Transaction<'_, Postgres>,
    owner: &str,
    previous: Option<&CheckpointReceipt>,
    next: &CheckpointReceipt,
    receipt: &str,
    envelope: &[u8],
) -> Result<()> {
    // Do not inherit asynchronous commit settings from the connection.
    sqlx::query("SET LOCAL synchronous_commit = on")
        .execute(&mut **tx)
        .await?;
    let head = read_head(tx, owner).await?;
    if let Some(head) = &head {
        let current = head.verified(owner)?;
        if &current == next && head.envelope == envelope {
            return Ok(());
        }
        if previous != Some(&current) {
            return Err(CheckpointStoreError::Conflict);
        }
    } else if previous.is_some() {
        return Err(CheckpointStoreError::Missing);
    }
    // A plain insert on an empty head must fail on a racing insert,
    // never turn it into an unvalidated overwrite. An existing row
    // is held by the FOR UPDATE lock until the commit.
    let statement = if head.is_some() {
        "UPDATE strategy_checkpoint_head SET receipt=$2, envelope=$3 WHERE singleton AND owner=$1"
    } else {
        "INSERT INTO strategy_checkpoint_head (singleton, owner, receipt, envelope) VALUES (TRUE, $1, $2, $3)"
    };
    let result = sqlx::query(statement)
        .bind(owner)
        .bind(receipt)
        .bind(envelope)
        .execute(&mut **tx)
        .await?;
    if result.rows_affected() != 1 {
        return Err(CheckpointStoreError::Conflict);
    }
    Ok(())
}

async fn require_unclaimed(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    let claimed: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM strategy_checkpoint_startup_claim)")
            .fetch_one(&mut **tx)
            .await?;
    if claimed {
        return Err(CheckpointStoreError::AlreadyClaimed);
    }
    Ok(())
}

async fn require_empty_execution_history(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM orders) OR EXISTS(SELECT 1 FROM fills)
         OR EXISTS(SELECT 1 FROM spot_parent_allocations)",
    )
    .fetch_one(&mut **tx)
    .await?;
    if exists {
        return Err(CheckpointStoreError::ExecutionHistory);
    }
    // EMS tables are installed lazily. Absence in the bound schema is
    // valid; any parent or child history requires a later recovery protocol.
    for (exists_sql, rows_sql) in [
        (
            "SELECT to_regclass('parent_orders') IS NOT NULL",
            "SELECT EXISTS(SELECT 1 FROM parent_orders)",
        ),
        (
            "SELECT to_regclass('parent_order_children') IS NOT NULL",
            "SELECT EXISTS(SELECT 1 FROM parent_order_children)",
        ),
    ] {
        if sqlx::query_scalar::<_, bool>(exists_sql)
            .fetch_one(&mut **tx)
            .await?
            && sqlx::query_scalar::<_, bool>(rows_sql)
                .fetch_one(&mut **tx)
                .await?
        {
            return Err(CheckpointStoreError::ExecutionHistory);
        }
    }
    Ok(())
}

async fn claim_head(
    tx: &mut Transaction<'_, Postgres>,
    owner: &str,
    expected: &CheckpointReceipt,
) -> Result<Vec<u8>> {
    sqlx::query("SET LOCAL synchronous_commit = on")
        .execute(&mut **tx)
        .await?;
    let envelope = select_head(tx, owner, Some(expected))
        .await?
        .ok_or(CheckpointStoreError::Missing)?;
    require_unclaimed(tx).await?;
    coherent_checkpoint::require_absent(tx).await?;
    require_empty_execution_history(tx).await?;
    let receipt = serde_json::to_string(expected).map_err(|_| CheckpointStoreError::Corrupt)?;
    let result = sqlx::query(
        "INSERT INTO strategy_checkpoint_startup_claim (singleton, owner, receipt)
         VALUES (TRUE, $1, $2) ON CONFLICT (singleton) DO NOTHING",
    )
    .bind(owner)
    .bind(receipt)
    .execute(&mut **tx)
    .await?;
    if result.rows_affected() != 1 {
        return Err(CheckpointStoreError::AlreadyClaimed);
    }
    Ok(envelope)
}

impl OrderStore {
    pub(crate) async fn migrate_strategy_checkpoints(
        pool: &PgPool,
    ) -> std::result::Result<(), OmsError> {
        // One head per exclusively bound schema. No historical fallback.
        // Bound the stored bytes before any application-level decoding.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS strategy_checkpoint_head (
                singleton BOOLEAN PRIMARY KEY CHECK (singleton),
                owner TEXT NOT NULL,
                receipt TEXT NOT NULL CHECK (octet_length(receipt) BETWEEN 1 AND 8192),
                envelope BYTEA NOT NULL CHECK (octet_length(envelope) BETWEEN 1 AND 7340032)
            )",
        )
        .execute(pool)
        .await?;
        // A one-time startup claim is deliberately separate from the head.
        // Even a later head replacement cannot authorize a second launch
        // before atomic execution/callback recovery is available.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS strategy_checkpoint_startup_claim (
                singleton BOOLEAN PRIMARY KEY CHECK (singleton),
                owner TEXT NOT NULL,
                receipt TEXT NOT NULL CHECK (octet_length(receipt) BETWEEN 1 AND 8192)
            )",
        )
        .execute(pool)
        .await?;
        coherent_checkpoint::migrate(pool).await?;
        Ok(())
    }

    fn checkpoint_connection(&mut self) -> Result<(&str, &mut PgConnection)> {
        let owner = self
            .inventory_owner
            .as_deref()
            .ok_or(CheckpointStoreError::Unbound)?;
        let connection = self
            .inventory_lease
            .as_mut()
            .ok_or(CheckpointStoreError::Unbound)?;
        Ok((owner, connection))
    }

    /// The recovery caller independently establishes the exact receipt.
    /// None means an explicitly empty store is required, not "latest".
    /// An absent, changed or corrupt head never yields usable state.
    pub async fn load_strategy_checkpoint(
        &mut self,
        expected: Option<&CheckpointReceipt>,
    ) -> Result<Option<Vec<u8>>> {
        let (owner, connection) = self.checkpoint_connection()?;
        let mut tx = connection.begin().await?;
        let selected = select_head(&mut tx, owner, expected).await;
        finish(tx, selected).await
    }

    /// Startup must not silently ignore a durable head or a previous claim.
    /// Legacy unbound standalone OMS stores have no checkpoint contract.
    pub(crate) async fn check_legacy_checkpoint_startup(&mut self) -> Result<()> {
        if !self.inventory_bound() {
            return Ok(());
        }
        let (owner, connection) = self.checkpoint_connection()?;
        let mut tx = connection.begin().await?;
        let result = async {
            require_unclaimed(&mut tx).await?;
            coherent_checkpoint::require_absent(&mut tx).await?;
            select_head(&mut tx, owner, None).await?;
            Ok(())
        }
        .await;
        finish(tx, result).await
    }

    /// A conservative, one-use startup reservation for an independently
    /// selected pre-execution checkpoint. Never infer callback receipts from
    /// the OMS recovery baseline. A successful claim is not reversible by
    /// this API, even if later semantic restoration or startup fails.
    pub(crate) async fn claim_strategy_checkpoint_startup(
        &mut self,
        expected: &CheckpointReceipt,
        owner_sha256: &str,
        ems_postgres_url: &str,
    ) -> Result<Vec<u8>> {
        let url_hash: [u8; 32] = Sha256::digest(ems_postgres_url.as_bytes()).into();
        if self.checkpoint_database_sha256 != url_hash {
            return Err(CheckpointStoreError::DatabaseScope);
        }
        let (owner, connection) = self.checkpoint_connection()?;
        if format!("{:x}", Sha256::digest(owner.as_bytes())) != owner_sha256 {
            return Err(CheckpointStoreError::Owner);
        }
        let mut tx = connection.begin().await?;
        let result = claim_head(&mut tx, owner, expected).await;
        finish(tx, result).await
    }

    /// CAS publication with strict identity/cursor monotonicity and exact
    /// idempotent retry after an uncertain commit response. A returned
    /// receipt is acknowledged only after commit; callers must persist the
    /// independently selected recovery receipt separately. This does not
    /// commit strategy state atomically with OMS executions.
    pub async fn publish_strategy_checkpoint(
        &mut self,
        previous: Option<&CheckpointReceipt>,
        identity: &StateIdentity,
        cursor: StateCursor,
        envelope: &[u8],
    ) -> Result<CheckpointReceipt> {
        let next = verify_checkpoint(envelope, identity, cursor)?;
        validate_transition(previous, &next)?;
        let receipt = serde_json::to_string(&next).map_err(|_| CheckpointStoreError::Corrupt)?;
        let (owner, connection) = self.checkpoint_connection()?;
        let mut tx = connection.begin().await?;
        let written = async {
            // A plain publication must never detach a coherent execution
            // receipt from its state. No downgrade/replacement API yet.
            coherent_checkpoint::require_absent(&mut tx).await?;
            write_head(&mut tx, owner, previous, &next, &receipt, envelope).await
        }
        .await;
        finish(tx, written).await?;
        Ok(next)
    }
}
