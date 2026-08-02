//! [`OrderStore`]: pure PostgreSQL read/write for `Order`/`Fill` -- no
//! business logic (no risk checks, no gateway calls, no state-machine
//! validation). [`crate::manager::OrderManager`] is the business-logic
//! layer built on top of this.

use apexes_core::{Fill, Order, OrderId};
use sqlx::PgPool;

use crate::error::OmsError;
use crate::schema::{
    FILLS_ORDER_ID_INDEX_DDL, FILLS_TABLE_DDL, FillRow, ORDERS_ADD_LEVERAGE_COLUMNS_DDL,
    ORDERS_EXCHANGE_ID_INDEX_DDL, ORDERS_TABLE_DDL, OrderRow,
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
        sqlx::query(ORDERS_ADD_LEVERAGE_COLUMNS_DDL)
            .execute(&pool)
            .await?;
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
            "INSERT INTO orders (id, exchange_order_id, symbol, venue, asset_class, side, order_type, quantity, price, stop_price, leverage, margin_mode, filled_quantity, status, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
             ON CONFLICT (id) DO UPDATE SET
                exchange_order_id = EXCLUDED.exchange_order_id,
                leverage = EXCLUDED.leverage,
                margin_mode = EXCLUDED.margin_mode,
                filled_quantity = EXCLUDED.filled_quantity,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at",
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
        let row = FillRow::from_fill(fill, exchange_trade_id);
        let result = sqlx::query(
            "INSERT INTO fills (id, order_id, exchange_trade_id, symbol, venue, side, price, quantity, fee, fee_currency, filled_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (order_id, exchange_trade_id) DO NOTHING",
        )
        .bind(row.id)
        .bind(row.order_id)
        .bind(&row.exchange_trade_id)
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
        Ok(result.rows_affected() > 0)
    }

    pub async fn get_fills_for_order(&self, order_id: OrderId) -> Result<Vec<Fill>, OmsError> {
        let rows: Vec<FillRow> = sqlx::query_as("SELECT * FROM fills WHERE order_id = $1")
            .bind(order_id.0)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(Fill::try_from).collect()
    }
}
