//! [`OrderStore`]: pure PostgreSQL read/write for `Order`/`Fill` -- no
//! business logic (no risk checks, no gateway calls, no state-machine
//! validation). [`crate::manager::OrderManager`] is the business-logic
//! layer built on top of this.

use apexes_core::{Fill, Order, OrderId, OrderStatus};
use sqlx::PgPool;

use crate::error::OmsError;
use crate::manager::SpotReservation;
use std::collections::HashMap;

#[path = "fill_reconciliation.rs"]
mod fill_reconciliation;
#[path = "parent_capital.rs"]
mod parent_capital;
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

    fn parse(value: &str) -> Result<Self, OmsError> {
        match value {
            "Submitting" => Ok(Self::Submitting),
            "Uncertain" => Ok(Self::Uncertain),
            "Accepted" => Ok(Self::Accepted),
            "Rejected" => Ok(Self::Rejected),
            _ => Err(OmsError::InvalidRow {
                reason: "invalid submission state".into(),
            }),
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
        // Existing rows intentionally remain unlinked; no inferred identity.
        sqlx::query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS intent_scope UUID, ADD COLUMN IF NOT EXISTS intent_ordinal NUMERIC(20,0)")
            .execute(&pool).await?;
        sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_scoped_intent ON orders(intent_scope, intent_ordinal) WHERE intent_scope IS NOT NULL AND intent_ordinal IS NOT NULL")
            .execute(&pool).await?;
        sqlx::query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS ems_parent_id UUID, ADD COLUMN IF NOT EXISTS ems_dispatch_id UUID")
            .execute(&pool).await?;
        sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_ems_dispatch ON orders(ems_dispatch_id) WHERE ems_dispatch_id IS NOT NULL")
            .execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_orders_ems_parent ON orders(ems_parent_id) WHERE ems_parent_id IS NOT NULL")
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
        sqlx::query(
            "ALTER TABLE orders
            ADD COLUMN IF NOT EXISTS reserved_spend_asset TEXT,
            ADD COLUMN IF NOT EXISTS reserved_receipt_asset TEXT,
            ADD COLUMN IF NOT EXISTS reserved_amount NUMERIC,
            ADD COLUMN IF NOT EXISTS reserved_owned_quantity NUMERIC,
            ADD COLUMN IF NOT EXISTS reserved_reference_price NUMERIC,
            ADD COLUMN IF NOT EXISTS reserved_market_buy_cap NUMERIC,
            ADD COLUMN IF NOT EXISTS reserved_commission_rate NUMERIC,
            ADD COLUMN IF NOT EXISTS reserved_commission_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS reserved_discount_asset TEXT,
            ADD COLUMN IF NOT EXISTS reservation_breached BOOLEAN NOT NULL DEFAULT FALSE",
        )
        .execute(&pool)
        .await?;
        Self::migrate_parent_capital(&pool).await?;
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

    /// Strategy-owned Spot inventory, including native base fees.
    /// Called under the single-writer manager lock, after delivery ack.
    pub(crate) async fn owned_spot_quantity(
        &self,
        symbol: &str,
        venue: apexes_core::Venue,
        base: &str,
    ) -> Result<rust_decimal::Decimal, OmsError> {
        Ok(sqlx::query_scalar(
            "SELECT COALESCE(SUM(
            CASE WHEN side='Buy' THEN quantity ELSE -quantity END
            - CASE WHEN fee_currency=$3 THEN fee ELSE 0 END),0)
            FROM fills WHERE symbol=$1 AND venue=$2",
        )
        .bind(symbol)
        .bind(format!("{venue:?}"))
        .bind(base)
        .fetch_one(&self.pool)
        .await?)
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
        let breached: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM orders WHERE reservation_breached)")
                .fetch_one(&mut *connection)
                .await?;
        if breached {
            return Err(OmsError::InvalidRow { reason: "persisted spot reservation breach requires audited reconciliation before startup".into() });
        }
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
        self.write_order(
            order,
            SubmissionState::from_order(order),
            None,
            None,
            None,
            None,
        )
        .await
    }

    /// Persist the client ID and a pre-dispatch marker before any exchange I/O.
    pub(crate) async fn begin_submission(
        &self,
        order: &Order,
        reservation: Option<&SpotReservation>,
        parent: Option<uuid::Uuid>,
        link: Option<crate::IntentLink>,
        child: Option<crate::ChildLink>,
    ) -> Result<(), OmsError> {
        if self.inventory_bound() && reservation.is_none_or(|r| r.commission.is_none()) {
            return Err(OmsError::InvalidRow {
                reason: "managed submission requires durable reservation".into(),
            });
        }
        let link = link.map(crate::IntentLink::validate).transpose()?;
        let child = child.map(crate::ChildLink::validate).transpose()?;
        self.write_order(
            order,
            SubmissionState::Submitting,
            reservation,
            parent,
            link,
            child,
        )
        .await
    }

    pub(crate) async fn has_reservation_breach(&self) -> Result<bool, OmsError> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM orders WHERE reservation_breached)")
                .fetch_one(&self.pool)
                .await?,
        )
    }

    /// Restore evidence only for orders that may still consume capital.
    /// Legacy working orders have no evidence and remain blocked by OMS.
    /// Terminal rows retain their original reservation for audit.
    pub(crate) async fn load_spot_reservations(
        &self,
    ) -> Result<HashMap<OrderId, SpotReservation>, OmsError> {
        #[derive(sqlx::FromRow)]
        struct Row {
            id: uuid::Uuid,
            reserved_spend_asset: Option<String>,
            reserved_receipt_asset: Option<String>,
            reserved_amount: Option<rust_decimal::Decimal>,
            reserved_owned_quantity: Option<rust_decimal::Decimal>,
            reserved_reference_price: Option<rust_decimal::Decimal>,
            reserved_market_buy_cap: Option<rust_decimal::Decimal>,
            reserved_commission_rate: Option<rust_decimal::Decimal>,
            reserved_commission_at: Option<chrono::DateTime<chrono::Utc>>,
            reserved_discount_asset: Option<String>,
        }
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT id, reserved_spend_asset, reserved_receipt_asset,
            reserved_amount, reserved_owned_quantity, reserved_reference_price, reserved_market_buy_cap, reserved_commission_rate, reserved_commission_at, reserved_discount_asset FROM orders
            WHERE status IN ('New','PartiallyFilled')",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut result = HashMap::new();
        for row in rows {
            let commission = match (
                row.reserved_commission_rate,
                row.reserved_commission_at,
                row.reserved_discount_asset,
            ) {
                (None, None, None) => None,
                (Some(rate), Some(observed_at), discount_asset) => {
                    let c = crate::SpotCommission {
                        rate,
                        observed_at,
                        discount_asset,
                    };
                    if !c.valid() {
                        return Err(OmsError::InvalidRow {
                            reason: "invalid persisted commission evidence".into(),
                        });
                    }
                    Some(c)
                }
                _ => {
                    return Err(OmsError::InvalidRow {
                        reason: "incomplete persisted commission evidence".into(),
                    });
                }
            };
            match (
                row.reserved_spend_asset,
                row.reserved_receipt_asset,
                row.reserved_amount,
                row.reserved_owned_quantity,
                row.reserved_reference_price,
            ) {
                (None, None, None, None, None)
                    if row.reserved_market_buy_cap.is_none() && commission.is_none() => {}
                (
                    Some(spend_asset),
                    Some(receipt_asset),
                    Some(amount),
                    Some(owned_quantity),
                    Some(reference_price),
                ) if !spend_asset.is_empty()
                    && !receipt_asset.is_empty()
                    && spend_asset != receipt_asset
                    && amount > rust_decimal::Decimal::ZERO
                    && owned_quantity >= rust_decimal::Decimal::ZERO
                    && reference_price > rust_decimal::Decimal::ZERO
                    && row
                        .reserved_market_buy_cap
                        .is_none_or(|cap| cap == reference_price) =>
                {
                    result.insert(
                        OrderId(row.id),
                        SpotReservation {
                            spend_asset,
                            receipt_asset,
                            amount,
                            owned_quantity,
                            reference_price,
                            market_buy_cap: row.reserved_market_buy_cap,
                            commission,
                        },
                    );
                }
                _ => {
                    return Err(OmsError::InvalidRow {
                        reason: "invalid persisted spot reservation".into(),
                    });
                }
            }
        }
        Ok(result)
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
        state.as_deref().map(SubmissionState::parse).transpose()
    }

    async fn write_order(
        &self,
        order: &Order,
        state: SubmissionState,
        reservation: Option<&SpotReservation>,
        parent: Option<uuid::Uuid>,
        link: Option<crate::IntentLink>,
        child: Option<crate::ChildLink>,
    ) -> Result<(), OmsError> {
        let row = OrderRow::from(order);
        let mut tx = self.pool.begin().await?;
        if let Some(parent) = parent {
            let r = reservation.ok_or_else(|| OmsError::InvalidRow {
                reason: "missing child reservation".into(),
            })?;
            Self::consume_parent_capital(&mut tx, parent, order, r).await?;
        }
        let result = sqlx::query(
            "INSERT INTO orders (id, exchange_order_id, symbol, venue, asset_class, side, order_type, quantity, price, stop_price, leverage, margin_mode, filled_quantity, status, created_at, updated_at, submission_state,
                reserved_spend_asset, reserved_receipt_asset, reserved_amount, reserved_owned_quantity, reserved_reference_price, reserved_market_buy_cap, reserved_commission_rate, reserved_commission_at, reserved_discount_asset, parent_allocation_id, intent_scope, intent_ordinal, ems_parent_id, ems_dispatch_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31)
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
        .bind(reservation.map(|r| r.spend_asset.as_str()))
        .bind(reservation.map(|r| r.receipt_asset.as_str()))
        .bind(reservation.map(|r| r.amount))
        .bind(reservation.map(|r| r.owned_quantity))
        .bind(reservation.map(|r| r.reference_price))
        .bind(reservation.and_then(|r| r.market_buy_cap))
        .bind(reservation.and_then(|r| r.commission.as_ref().map(|c| c.rate)))
        .bind(reservation.and_then(|r| r.commission.as_ref().map(|c| c.observed_at)))
        .bind(reservation.and_then(|r| r.commission.as_ref().and_then(|c| c.discount_asset.as_deref())))
        .bind(parent)
        .bind(link.map(|l|l.run_scope))
        .bind(link.map(|l|rust_decimal::Decimal::from(l.intent_id.0)))
        .bind(child.map(|c|c.parent_id.0))
        .bind(child.map(|c|c.dispatch_id))
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(OmsError::QueryFailed {
                reason: "client order ID already exists".into(),
            });
        }
        tx.commit().await?;
        Ok(())
    }

    /// Read identity and submission state from the same durable row.
    pub(crate) async fn identity_snapshot(
        &self,
        id: OrderId,
    ) -> Result<
        Option<(
            Order,
            SubmissionState,
            Option<crate::IntentLink>,
            Option<crate::ChildLink>,
        )>,
        OmsError,
    > {
        #[derive(sqlx::FromRow)]
        struct IdentityRow {
            #[sqlx(flatten)]
            order: OrderRow,
            submission_state: String,
            intent_scope: Option<uuid::Uuid>,
            intent_ordinal: Option<rust_decimal::Decimal>,
            ems_parent_id: Option<uuid::Uuid>,
            ems_dispatch_id: Option<uuid::Uuid>,
        }
        let row: Option<IdentityRow> = sqlx::query_as("SELECT * FROM orders WHERE id = $1")
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            Ok((
                Order::try_from(row.order)?,
                SubmissionState::parse(&row.submission_state)?,
                decode_intent_link(row.intent_scope, row.intent_ordinal)?,
                decode_child_link(row.ems_parent_id, row.ems_dispatch_id)?,
            ))
        })
        .transpose()
    }

    /// Read a persisted execution and immutable OMS linkage in one SQL snapshot.
    /// An unknown fill ID is not evidence that an exchange never executed.
    pub async fn fill_identity(
        &self,
        id: apexes_core::FillId,
    ) -> Result<Option<crate::FillIdentity>, OmsError> {
        Ok(self.fill_identities(&[id]).await?.pop().flatten())
    }

    /// Batch lookup, aligned to the caller's fill IDs. No execution or
    /// delivery state is modified by this independent evidence query.
    pub async fn fill_identities(
        &self,
        ids: &[apexes_core::FillId],
    ) -> Result<Vec<Option<crate::FillIdentity>>, OmsError> {
        #[derive(sqlx::FromRow)]
        struct Row {
            #[sqlx(flatten)]
            fill: FillRow,
            delivery_seq: i64,
            intent_scope: Option<uuid::Uuid>,
            intent_ordinal: Option<rust_decimal::Decimal>,
            ems_parent_id: Option<uuid::Uuid>,
            ems_dispatch_id: Option<uuid::Uuid>,
            matching_order: bool,
        }
        let keys: Vec<_> = ids.iter().map(|id| id.0).collect();
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT f.*, o.intent_scope, o.intent_ordinal, o.ems_parent_id, o.ems_dispatch_id,
             (f.symbol=o.symbol AND f.venue=o.venue AND f.side=o.side
              AND f.quantity>0 AND f.quantity<=o.quantity AND f.price>0 AND f.fee>=0)
             AS matching_order
             FROM fills f JOIN orders o ON o.id=f.order_id WHERE f.id=ANY($1)",
        )
        .bind(&keys)
        .fetch_all(&self.pool)
        .await?;
        let mut by_id = HashMap::with_capacity(rows.len());
        for row in rows {
            let intent_link = decode_intent_link(row.intent_scope, row.intent_ordinal)?;
            let child_link = decode_child_link(row.ems_parent_id, row.ems_dispatch_id)?;
            if !row.matching_order
                || row.delivery_seq <= 0
                || row.fill.exchange_trade_id.is_empty()
                || (intent_link.is_some() && child_link.is_some())
            {
                return Err(OmsError::InvalidRow {
                    reason: "conflicting durable fill attribution".into(),
                });
            }
            let identity = crate::FillIdentity {
                delivery_sequence: row.delivery_seq,
                exchange_trade_id: row.fill.exchange_trade_id.clone(),
                fill: Fill::try_from(row.fill)?,
                intent_link,
                child_link,
            };
            by_id.insert(identity.fill.id, identity);
        }
        Ok(ids.iter().map(|id| by_id.remove(id)).collect())
    }

    /// Resolve a previously persisted run-scoped intent, including terminal
    /// orders. Absence is not authority to retry a previous submission.
    pub async fn client_order_for_intent(
        &self,
        link: crate::IntentLink,
    ) -> Result<Option<OrderId>, OmsError> {
        let link = link.validate()?;
        let id: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT id FROM orders WHERE intent_scope = $1 AND intent_ordinal = $2",
        )
        .bind(link.run_scope)
        .bind(rust_decimal::Decimal::from(link.intent_id.0))
        .fetch_optional(&self.pool)
        .await?;
        Ok(id.map(OrderId))
    }

    /// Read-only exact dispatch lookup, including terminal children.
    pub async fn client_order_for_dispatch(
        &self,
        link: crate::ChildLink,
    ) -> Result<Option<OrderId>, OmsError> {
        let link = link.validate()?;
        let id: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT id FROM orders WHERE ems_parent_id=$1 AND ems_dispatch_id=$2",
        )
        .bind(link.parent_id.0)
        .bind(link.dispatch_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(id.map(OrderId))
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

fn decode_child_link(
    parent: Option<uuid::Uuid>,
    dispatch: Option<uuid::Uuid>,
) -> Result<Option<crate::ChildLink>, OmsError> {
    match (parent, dispatch) {
        (None, None) => Ok(None),
        (Some(parent), Some(dispatch_id)) => crate::ChildLink {
            parent_id: apexes_strategy::ParentOrderId(parent),
            dispatch_id,
        }
        .validate()
        .map(Some),
        _ => Err(OmsError::InvalidRow {
            reason: "incomplete EMS child linkage".into(),
        }),
    }
}

fn decode_intent_link(
    scope: Option<uuid::Uuid>,
    ordinal: Option<rust_decimal::Decimal>,
) -> Result<Option<crate::IntentLink>, OmsError> {
    use rust_decimal::prelude::ToPrimitive;
    match (scope, ordinal) {
        (None, None) => Ok(None),
        (Some(run_scope), Some(ordinal)) if ordinal.fract().is_zero() => {
            let id = ordinal.to_u64().ok_or_else(|| OmsError::InvalidRow {
                reason: "invalid intent ordinal".into(),
            })?;
            crate::IntentLink {
                run_scope,
                intent_id: apexes_strategy::IntentId(id),
            }
            .validate()
            .map(Some)
        }
        _ => Err(OmsError::InvalidRow {
            reason: "incomplete or invalid intent linkage".into(),
        }),
    }
}

#[cfg(test)]
mod intent_link_tests {
    use super::*;
    use rust_decimal::Decimal;
    use uuid::Uuid;
    #[test]
    fn decode_scoped_identity_without_rounding_or_partial_links() {
        let scope = Uuid::new_v4();
        assert_eq!(decode_intent_link(None, None).unwrap(), None);
        assert_eq!(
            decode_intent_link(Some(scope), Some(Decimal::from(u64::MAX))).unwrap(),
            Some(crate::IntentLink {
                run_scope: scope,
                intent_id: apexes_strategy::IntentId(u64::MAX)
            })
        );
        for (scope, ordinal) in [
            (Some(scope), None),
            (None, Some(Decimal::ONE)),
            (Some(Uuid::nil()), Some(Decimal::ONE)),
            (Some(scope), Some(Decimal::ZERO)),
            (Some(scope), Some(Decimal::NEGATIVE_ONE)),
            (Some(scope), Some(Decimal::new(15, 1))),
            (Some(scope), Some(Decimal::from(u64::MAX) + Decimal::ONE)),
        ] {
            assert!(decode_intent_link(scope, ordinal).is_err());
        }
    }
}
