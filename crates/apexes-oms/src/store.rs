//! [`OrderStore`]: pure PostgreSQL read/write for `Order`/`Fill` -- no
//! business logic (no risk checks, no gateway calls, no state-machine
//! validation). [`crate::manager::OrderManager`] is the business-logic
//! layer built on top of this.

use apexes_core::{Fill, Order, OrderId, OrderStatus};
use sqlx::PgPool;

use crate::error::OmsError;

#[path = "fill_reconciliation.rs"]
mod fill_reconciliation;
use crate::schema::{
    FILLS_ORDER_ID_INDEX_DDL, FILLS_TABLE_DDL, FillRow, ORDERS_ADD_LEVERAGE_COLUMNS_DDL,
    ORDERS_EXCHANGE_ID_INDEX_DDL, ORDERS_TABLE_DDL, OrderRow,
};

/// Durable submission state, separate from the exchange order lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmissionState {
    Submitting,
    Uncertain,
    Accepted,
    Rejected,
}

impl SubmissionState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Submitting => "Submitting",
            Self::Uncertain => "Uncertain",
            Self::Accepted => "Accepted",
            Self::Rejected => "Rejected",
        }
    }

    fn from_order(order: &Order) -> Self {
        if order.exchange_order_id.is_some() {
            Self::Accepted
        } else if order.status == OrderStatus::Rejected {
            Self::Rejected
        } else {
            Self::Uncertain
        }
    }
}

pub struct OrderStore {
    pool: PgPool,
    // A detached connection owns the schema-scoped advisory lock for the
    // lifetime of the managed order store, rather than returning it to a pool.
    inventory_lease: Option<sqlx::PgConnection>,
}

impl OrderStore {
    /// Connects to PostgreSQL at `url` and ensures both tables (plus
    /// their indexes) exist.
    pub async fn new(url: &str) -> Result<Self, OmsError> {
        let pool = PgPool::connect(url)
            .await
            .map_err(|error| OmsError::ConnectionFailed {
                reason: error.to_string(),
            })?;

        sqlx::query(ORDERS_TABLE_DDL).execute(&pool).await?;
        sqlx::query(ORDERS_ADD_LEVERAGE_COLUMNS_DDL)
            .execute(&pool)
            .await?;
        // Existing rows without an exchange ID are conservatively uncertain.
        // No migration infers non-execution from an order's age.
        sqlx::query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS submission_state TEXT NOT NULL DEFAULT 'Uncertain'")
            .execute(&pool).await?;
        sqlx::query("UPDATE orders SET submission_state = CASE WHEN exchange_order_id IS NOT NULL THEN 'Accepted' ELSE 'Rejected' END WHERE submission_state = 'Uncertain' AND (exchange_order_id IS NOT NULL OR status = 'Rejected')")
            .execute(&pool).await?;
        sqlx::query(ORDERS_EXCHANGE_ID_INDEX_DDL)
            .execute(&pool)
            .await?;
        sqlx::query(FILLS_TABLE_DDL).execute(&pool).await?;
        sqlx::query(FILLS_ORDER_ID_INDEX_DDL).execute(&pool).await?;
        sqlx::query("ALTER TABLE fills ADD COLUMN IF NOT EXISTS delivery_seq BIGSERIAL")
            .execute(&pool)
            .await?;
        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_delivery_seq ON fills(delivery_seq)",
        )
        .execute(&pool)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_fills_exchange_identity ON fills(venue, symbol, exchange_trade_id)")
            .execute(&pool).await?;

        crate::FeeJournal::migrate(&pool).await?;
        Ok(Self {
            pool,
            inventory_lease: None,
        })
    }

    /// Pricing is a separate, read-mostly task using the same schema.
    pub fn fee_journal(&self) -> Option<crate::FeeJournal> {
        self.inventory_lease.as_ref()?;
        Some(crate::FeeJournal {
            pool: self.pool.clone(),
        })
    }

    /// Bind a dedicated live schema to one stable credential/strategy and
    /// instrument set. Never guess ownership of legacy unbound orders.
    pub async fn bind_spot_inventory(&mut self, owner: &str) -> Result<(), OmsError> {
        if self.inventory_lease.is_some() || owner.is_empty() {
            return Err(OmsError::InvalidRow {
                reason: "invalid inventory binding".into(),
            });
        }
        let mut connection = self.pool.acquire().await?.detach();
        let locked: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtext(current_schema()), 20261126)")
                .fetch_one(&mut connection)
                .await?;
        if !locked {
            return Err(OmsError::InvalidRow {
                reason: "another live engine owns this inventory schema".into(),
            });
        }
        sqlx::query("CREATE TABLE IF NOT EXISTS spot_inventory_owner (singleton BOOLEAN PRIMARY KEY CHECK (singleton), owner TEXT NOT NULL)")
            .execute(&mut connection).await?;
        let existing: Option<String> =
            sqlx::query_scalar("SELECT owner FROM spot_inventory_owner WHERE singleton")
                .fetch_optional(&mut connection)
                .await?;
        match existing {
            Some(value) if value == owner => {}
            Some(_) => return Err(OmsError::InvalidRow {
                reason:
                    "inventory schema belongs to a different credential, strategy or instrument set"
                        .into(),
            }),
            None => {
                let has_orders: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM orders)")
                    .fetch_one(&mut connection)
                    .await?;
                let has_parents: bool =
                    sqlx::query_scalar("SELECT to_regclass('parent_orders') IS NOT NULL")
                        .fetch_one(&mut connection)
                        .await?;
                let has_parent_rows = if has_parents {
                    sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM parent_orders)")
                        .fetch_one(&mut connection)
                        .await?
                } else {
                    false
                };
                if has_orders || has_parent_rows {
                    return Err(OmsError::InvalidRow { reason: "legacy unbound inventory history requires audited migration; no automatic adoption".into() });
                }
                sqlx::query(
                    "INSERT INTO spot_inventory_owner (singleton, owner) VALUES (TRUE, $1)",
                )
                .bind(owner)
                .execute(&mut connection)
                .await?;
            }
        }
        self.inventory_lease = Some(connection);
        Ok(())
    }

    /// A bound store must never silently reconnect its lease connection.
    /// A lost lease latches the live session through the OMS caller.
    pub(crate) async fn verify_inventory_lease(&mut self) -> Result<(), OmsError> {
        if let Some(connection) = self.inventory_lease.as_mut() {
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                sqlx::query("SELECT 1").execute(connection),
            )
            .await;
            if !matches!(result, Ok(Ok(_))) {
                return Err(OmsError::QueryFailed {
                    reason: "live inventory lease unavailable".into(),
                });
            }
        }
        Ok(())
    }

    /// Replay source: the durable order/fill history, not account balances.
    /// Validate the persisted fill quantities before returning a stable
    /// ordering of the exchange fills. F02.02 adds ongoing reconciliation.
    pub async fn spot_inventory_fills(&mut self) -> Result<Vec<Fill>, OmsError> {
        let connection = self
            .inventory_lease
            .as_mut()
            .ok_or_else(|| OmsError::InvalidRow {
                reason: "inventory history is not bound to a live owner".into(),
            })?;
        let inconsistent: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM orders o LEFT JOIN
              (SELECT order_id, SUM(quantity) AS quantity FROM fills GROUP BY order_id) f ON f.order_id = o.id
              WHERE o.asset_class <> 'Spot' OR o.leverage IS NOT NULL
              OR o.filled_quantity <> COALESCE(f.quantity, 0)
              OR (o.status = 'Filled' AND o.filled_quantity <> o.quantity)
              OR o.filled_quantity > o.quantity)"
        ).fetch_one(&mut *connection).await?;
        let mismatched: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM fills f JOIN orders o ON f.order_id = o.id
              WHERE f.symbol <> o.symbol OR f.venue <> o.venue OR f.side <> o.side)",
        )
        .fetch_one(&mut *connection)
        .await?;
        if inconsistent || mismatched {
            return Err(OmsError::InvalidRow {
                reason: "durable spot order/fill history inconsistent; reconcile before startup"
                    .into(),
            });
        }
        let rows: Vec<FillRow> = sqlx::query_as(
            "SELECT * FROM fills ORDER BY filled_at, venue, symbol, length(exchange_trade_id), exchange_trade_id, id"
        ).fetch_all(&mut *connection).await?;
        rows.into_iter().map(Fill::try_from).collect()
    }

    /// Inserts a new order, or updates it if `order.id` already exists
    /// (`ON CONFLICT (id) DO UPDATE`) -- an idempotent upsert, since
    /// `OrderManager` writes the same order multiple times across its
    /// lifecycle: once at creation, again after gateway submission
    /// updates `exchange_order_id`, again on every status change.
    pub async fn upsert_order(&self, order: &Order) -> Result<(), OmsError> {
        self.write_order(order, SubmissionState::from_order(order))
            .await
    }

    /// Persist the client ID and a pre-dispatch marker before any exchange I/O.
    pub(crate) async fn begin_submission(&self, order: &Order) -> Result<(), OmsError> {
        self.write_order(order, SubmissionState::Submitting).await
    }

    pub async fn get_submission_state(
        &self,
        id: OrderId,
    ) -> Result<Option<SubmissionState>, OmsError> {
        let state: Option<String> =
            sqlx::query_scalar("SELECT submission_state FROM orders WHERE id = $1")
                .bind(id.0)
                .fetch_optional(&self.pool)
                .await?;
        state
            .map(|value| match value.as_str() {
                "Submitting" => Ok(SubmissionState::Submitting),
                "Uncertain" => Ok(SubmissionState::Uncertain),
                "Accepted" => Ok(SubmissionState::Accepted),
                "Rejected" => Ok(SubmissionState::Rejected),
                _ => Err(OmsError::InvalidRow {
                    reason: "invalid submission state".into(),
                }),
            })
            .transpose()
    }

    async fn write_order(&self, order: &Order, state: SubmissionState) -> Result<(), OmsError> {
        let row = OrderRow::from(order);
        let result = sqlx::query(
            "INSERT INTO orders (id, exchange_order_id, symbol, venue, asset_class, side, order_type, quantity, price, stop_price, leverage, margin_mode, filled_quantity, status, created_at, updated_at, submission_state)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
             ON CONFLICT (id) DO UPDATE SET
                exchange_order_id = EXCLUDED.exchange_order_id,
                leverage = EXCLUDED.leverage,
                margin_mode = EXCLUDED.margin_mode,
                filled_quantity = EXCLUDED.filled_quantity,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at,
                submission_state = EXCLUDED.submission_state
             WHERE $17 <> 'Submitting'",
        )
        .bind(row.id)
        .bind(&row.exchange_order_id)
        .bind(&row.symbol)
        .bind(&row.venue)
        .bind(&row.asset_class)
        .bind(&row.side)
        .bind(&row.order_type)
        .bind(row.quantity)
        .bind(row.price)
        .bind(row.stop_price)
        .bind(row.leverage)
        .bind(&row.margin_mode)
        .bind(row.filled_quantity)
        .bind(&row.status)
        .bind(row.created_at)
        .bind(row.updated_at)
        .bind(state.as_str())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(OmsError::QueryFailed {
                reason: "client order ID already exists".into(),
            });
        }
        Ok(())
    }

    pub async fn get_order(&self, id: OrderId) -> Result<Option<Order>, OmsError> {
        let row: Option<OrderRow> = sqlx::query_as("SELECT * FROM orders WHERE id = $1")
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await?;
        row.map(Order::try_from).transpose()
    }

    /// Returns every order currently NOT in a terminal state (`New` or
    /// `PartiallyFilled`) -- used by `OrderManager::reconcile` (and
    /// `OrderManager::new`'s startup recovery) to know which local
    /// orders need to be checked against the exchange.
    pub async fn get_open_orders(&self) -> Result<Vec<Order>, OmsError> {
        let rows: Vec<OrderRow> =
            sqlx::query_as("SELECT * FROM orders WHERE status IN ('New', 'PartiallyFilled')")
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter().map(Order::try_from).collect()
    }

    /// Inserts a fill, keyed for deduplication on `(order_id,
    /// exchange_trade_id)` -- a real exchange's "my trades" endpoint
    /// returns the full trade history for an order every call, so this
    /// must be safely callable multiple times with the same
    /// `exchange_trade_id` without creating duplicate rows. Returns
    /// `Ok(true)` if a new row was inserted, `Ok(false)` if it already
    /// existed (the `ON CONFLICT DO NOTHING` path) -- callers (see
    /// `OrderManager::apply_exchange_update`, Task 10) use this to know
    /// whether to propagate the fill onward (e.g. into a
    /// `PortfolioLedger`).
    pub async fn insert_fill(
        &self,
        fill: &Fill,
        exchange_trade_id: &str,
    ) -> Result<bool, OmsError> {
        let mut connection = self.pool.acquire().await?;
        fill_reconciliation::insert_fill_on(&mut connection, fill, exchange_trade_id).await
    }

    pub async fn get_fills_for_order(&self, order_id: OrderId) -> Result<Vec<Fill>, OmsError> {
        let rows: Vec<FillRow> = sqlx::query_as("SELECT * FROM fills WHERE order_id = $1")
            .bind(order_id.0)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(Fill::try_from).collect()
    }
}
