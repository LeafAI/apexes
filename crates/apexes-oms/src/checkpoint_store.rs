//! F04.05: owner-bound, transactional checkpoint head and exact recovery
//! selection. Not a runner integration or an atomic OMS/callback checkpoint.
use apexes_strategy::checkpoint::{
    CheckpointError, CheckpointReceipt, StateCursor, StateIdentity, verify_checkpoint,
};
use sqlx::{Connection, PgConnection, PgPool, Postgres, Transaction};

use super::OrderStore;
use crate::OmsError;

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
        let written = write_head(&mut tx, owner, previous, &next, &receipt, envelope).await;
        finish(tx, written).await?;
        Ok(next)
    }
}
