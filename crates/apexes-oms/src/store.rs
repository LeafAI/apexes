//! [`OrderStore`]: pure PostgreSQL read/write for `Order`/`Fill` -- no
//! business logic (no risk checks, no gateway calls, no state-machine
//! validation). [`crate::manager::OrderManager`] is the business-logic
//! layer built on top of this.

use apexes_core::{Fill, Order, OrderId};
use sqlx::PgPool;

use crate::error::OmsError;
use crate::schema::{
    FILLS_ORDER_ID_INDEX_DDL, FILLS_TABLE_DDL, FillRow, ORDERS_EXCHANGE_ID_INDEX_DDL,
    ORDERS_TABLE_DDL, OrderRow,
};

pub struct OrderStore {
    pool: PgPool,
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
        sqlx::query(ORDERS_EXCHANGE_ID_INDEX_DDL)
            .execute(&pool)
            .await?;
        sqlx::query(FILLS_TABLE_DDL).execute(&pool).await?;
        sqlx::query(FILLS_ORDER_ID_INDEX_DDL).execute(&pool).await?;

        Ok(Self { pool })
    }

    /// Inserts a new order, or updates it if `order.id` already exists
    /// (`ON CONFLICT (id) DO UPDATE`) -- an idempotent upsert, since
    /// `OrderManager` writes the same order multiple times across its
    /// lifecycle: once at creation, again after gateway submission
    /// updates `exchange_order_id`, again on every status change.
    pub async fn upsert_order(&self, order: &Order) -> Result<(), OmsError> {
        let row = OrderRow::from(order);
        sqlx::query(
            "INSERT INTO orders (id, exchange_order_id, symbol, venue, side, order_type, quantity, price, stop_price, filled_quantity, status, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             ON CONFLICT (id) DO UPDATE SET
                exchange_order_id = EXCLUDED.exchange_order_id,
                filled_quantity = EXCLUDED.filled_quantity,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at",
        )
        .bind(row.id)
        .bind(&row.exchange_order_id)
        .bind(&row.symbol)
        .bind(&row.venue)
        .bind(&row.side)
        .bind(&row.order_type)
        .bind(row.quantity)
        .bind(row.price)
        .bind(row.stop_price)
        .bind(row.filled_quantity)
        .bind(&row.status)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(&self.pool)
        .await?;
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

    pub async fn insert_fill(&self, fill: &Fill) -> Result<(), OmsError> {
        let row = FillRow::from(fill);
        sqlx::query(
            "INSERT INTO fills (id, order_id, symbol, venue, side, price, quantity, fee, fee_currency, filled_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(row.id)
        .bind(row.order_id)
        .bind(&row.symbol)
        .bind(&row.venue)
        .bind(&row.side)
        .bind(row.price)
        .bind(row.quantity)
        .bind(row.fee)
        .bind(&row.fee_currency)
        .bind(row.filled_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_fills_for_order(&self, order_id: OrderId) -> Result<Vec<Fill>, OmsError> {
        let rows: Vec<FillRow> = sqlx::query_as("SELECT * FROM fills WHERE order_id = $1")
            .bind(order_id.0)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(Fill::try_from).collect()
    }
}
