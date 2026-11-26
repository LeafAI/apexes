//! F02.05: durable, conservative capital budgets for managed Spot EMS
//! parents. Allocation is exclusive across all potentially affected
//! assets; child reservations consume budget before exchange dispatch.
use super::OrderStore;
use crate::{OmsError, manager::SpotReservation};
use apexes_core::{Order, Side};
use apexes_strategy::OrderIntent;
use rust_decimal::Decimal;
use sqlx::PgConnection;
use uuid::Uuid;

#[derive(sqlx::FromRow)]
struct Allocation {
    id: Uuid,
    symbol: String,
    side: String,
    total_quantity: Decimal,
    allocated_quantity: Decimal,
    budget: Decimal,
    allocated_amount: Decimal,
    spend_asset: String,
    receipt_asset: String,
    discount_asset: Option<String>,
    release_requested: bool,
}

const ACTIVE: &str = "SELECT p.* FROM spot_parent_allocations p
 WHERE NOT p.release_requested OR EXISTS(
 SELECT 1 FROM orders o WHERE o.parent_allocation_id=p.id
 AND o.status IN ('New','PartiallyFilled'))";

fn reject(reason: &str) -> OmsError {
    OmsError::OrderRejected {
        reason: reason.into(),
    }
}

impl Allocation {
    fn matches(&self, intent: &OrderIntent, reservation: &SpotReservation) -> bool {
        self.symbol == intent.symbol
            && self.side == format!("{:?}", intent.side)
            && self.spend_asset == reservation.spend_asset
            && self.receipt_asset == reservation.receipt_asset
            && self.discount_asset.as_deref()
                == reservation
                    .commission
                    .as_ref()
                    .and_then(|c| c.discount_asset.as_deref())
    }

    fn overlaps(&self, reservation: &SpotReservation) -> bool {
        let own = [
            Some(self.spend_asset.as_str()),
            Some(self.receipt_asset.as_str()),
            self.discount_asset.as_deref(),
        ];
        let other = [
            Some(reservation.spend_asset.as_str()),
            Some(reservation.receipt_asset.as_str()),
            reservation
                .commission
                .as_ref()
                .and_then(|c| c.discount_asset.as_deref()),
        ];
        own.into_iter()
            .flatten()
            .any(|asset| other.contains(&Some(asset)))
    }
}

impl OrderStore {
    pub(super) async fn migrate_parent_capital(pool: &sqlx::PgPool) -> Result<(), OmsError> {
        sqlx::query("CREATE TABLE IF NOT EXISTS spot_parent_allocations (
            id UUID PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL,
            total_quantity NUMERIC NOT NULL CHECK(total_quantity>0),
            allocated_quantity NUMERIC NOT NULL DEFAULT 0 CHECK(allocated_quantity>=0 AND allocated_quantity<=total_quantity),
            budget NUMERIC NOT NULL CHECK(budget>0),
            allocated_amount NUMERIC NOT NULL DEFAULT 0 CHECK(allocated_amount>=0 AND allocated_amount<=budget),
            spend_asset TEXT NOT NULL, receipt_asset TEXT NOT NULL, discount_asset TEXT,
            commission_rate NUMERIC NOT NULL, commission_at TIMESTAMPTZ NOT NULL,
            reference_price NUMERIC NOT NULL, release_requested BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())").execute(pool).await?;
        sqlx::query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS parent_allocation_id UUID REFERENCES spot_parent_allocations(id)")
            .execute(pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_orders_parent_allocation ON orders(parent_allocation_id)").execute(pool).await?;
        Ok(())
    }

    pub(crate) async fn check_parent_capital(
        &self,
        parent: Option<Uuid>,
        intent: &OrderIntent,
        reservation: &SpotReservation,
    ) -> Result<(), OmsError> {
        let rows: Vec<Allocation> = sqlx::query_as(ACTIVE).fetch_all(&self.pool).await?;
        let mut found = parent.is_none();
        for row in rows {
            if Some(row.id) == parent {
                found = true;
                if row.release_requested
                    || !row.matches(intent, reservation)
                    || row.allocated_quantity + intent.quantity > row.total_quantity
                    || row.allocated_amount + reservation.amount > row.budget
                {
                    return Err(reject(
                        "EMS child exceeds or mismatches durable parent allocation",
                    ));
                }
            } else if row.overlaps(reservation) {
                return Err(reject("asset reserved by an EMS parent"));
            }
        }
        if !found {
            return Err(reject("EMS parent allocation missing"));
        }
        Ok(())
    }

    pub(crate) async fn reserve_parent_capital(
        &self,
        id: Uuid,
        intent: &OrderIntent,
        reservation: &SpotReservation,
    ) -> Result<(), OmsError> {
        let commission = reservation
            .commission
            .as_ref()
            .ok_or_else(|| reject("parent commission missing"))?;
        sqlx::query("INSERT INTO spot_parent_allocations(id,symbol,side,total_quantity,budget,spend_asset,receipt_asset,
            discount_asset,commission_rate,commission_at,reference_price)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
            .bind(id).bind(&intent.symbol).bind(format!("{:?}",intent.side)).bind(intent.quantity)
            .bind(reservation.amount).bind(&reservation.spend_asset).bind(&reservation.receipt_asset)
            .bind(&commission.discount_asset).bind(commission.rate).bind(commission.observed_at).bind(reservation.reference_price)
            .execute(&self.pool).await?;
        Ok(())
    }

    pub(crate) async fn consume_parent_capital(
        connection: &mut PgConnection,
        parent: Uuid,
        order: &Order,
        reservation: &SpotReservation,
    ) -> Result<(), OmsError> {
        let result=sqlx::query("UPDATE spot_parent_allocations SET allocated_quantity=allocated_quantity+$4,
            allocated_amount=allocated_amount+$5 WHERE id=$1 AND symbol=$2 AND side=$3 AND NOT release_requested
            AND allocated_quantity+$4<=total_quantity AND allocated_amount+$5<=budget
            AND spend_asset=$6 AND receipt_asset=$7 AND discount_asset IS NOT DISTINCT FROM $8")
            .bind(parent).bind(&order.symbol).bind(format!("{:?}",order.side)).bind(order.quantity).bind(reservation.amount)
            .bind(&reservation.spend_asset).bind(&reservation.receipt_asset)
            .bind(reservation.commission.as_ref().and_then(|c|c.discount_asset.as_deref()))
            .execute(connection).await?;
        if result.rows_affected() != 1 {
            return Err(reject("EMS parent allocation unavailable or exhausted"));
        }
        Ok(())
    }

    pub(crate) async fn verify_parent_capital(
        &self,
        id: Uuid,
        symbol: &str,
        side: Side,
        total: Decimal,
        submitted: Decimal,
    ) -> Result<(), OmsError> {
        let row: Option<Allocation> =
            sqlx::query_as("SELECT * FROM spot_parent_allocations WHERE id=$1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        let (quantity, amount): (Decimal, Decimal) = sqlx::query_as(
            "SELECT COALESCE(SUM(quantity),0), COALESCE(SUM(reserved_amount),0)
             FROM orders WHERE parent_allocation_id=$1",
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await?;
        if !row.is_some_and(|p| {
            !p.release_requested
                && p.symbol == symbol
                && p.side == format!("{side:?}")
                && p.total_quantity == total
                && p.allocated_quantity == submitted
                && p.allocated_quantity == quantity
                && p.allocated_amount == amount
        }) {
            return Err(reject(
                "EMS parent allocation does not match recovered scheduling journal",
            ));
        }
        Ok(())
    }

    pub(crate) async fn release_parent_capital(&self, id: Uuid) -> Result<(), OmsError> {
        sqlx::query("UPDATE spot_parent_allocations SET release_requested=TRUE WHERE id=$1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
