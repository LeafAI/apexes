//! F02.02: atomic exchange reconciliation and ordered durable fill delivery.
//! The managed schema has one writer (OrderManager), so insertion sequence
//! is a commit watermark. Restarts rebuild from all durable fills.
use super::{OrderStore, SubmissionState};
use crate::{ExchangeFill, OmsError, schema::FillRow};
use apexes_core::{Fill, FillId, Order, OrderStatus};
use rust_decimal::Decimal;
use sqlx::PgConnection;
use uuid::Uuid;

fn discrepancy(reason: &str) -> OmsError {
    OmsError::InvalidRow {
        reason: reason.into(),
    }
}

fn consistent_status(order: &Order, quantity: Decimal) -> bool {
    quantity >= Decimal::ZERO
        && quantity <= order.quantity
        && match order.status {
            OrderStatus::New | OrderStatus::Rejected => quantity.is_zero(),
            OrderStatus::PartiallyFilled => quantity > Decimal::ZERO && quantity < order.quantity,
            OrderStatus::Filled => quantity == order.quantity,
            OrderStatus::Cancelled => true,
        }
}

pub(super) async fn insert_fill_on(
    connection: &mut PgConnection,
    fill: &Fill,
    exchange_trade_id: &str,
) -> Result<bool, OmsError> {
    let row = FillRow::from_fill(fill, exchange_trade_id);
    let result = sqlx::query(
        "INSERT INTO fills (id, order_id, exchange_trade_id, symbol, venue, side, price, quantity, fee, fee_currency, filled_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT (order_id, exchange_trade_id) DO NOTHING",
    ).bind(row.id).bind(row.order_id).bind(&row.exchange_trade_id).bind(&row.symbol)
        .bind(&row.venue).bind(&row.side).bind(row.price).bind(row.quantity)
        .bind(row.fee).bind(&row.fee_currency).bind(row.filled_at)
        .execute(connection).await?;
    Ok(result.rows_affected() > 0)
}

async fn validate_fill(
    connection: &mut PgConnection,
    fill: &Fill,
    trade_id: &str,
) -> Result<(), OmsError> {
    if trade_id.is_empty()
        || fill.price <= Decimal::ZERO
        || fill.quantity <= Decimal::ZERO
        || fill.fee < Decimal::ZERO
    {
        return Err(discrepancy("invalid authoritative fill"));
    }
    let rows: Vec<FillRow> =
        sqlx::query_as("SELECT * FROM fills WHERE venue=$1 AND symbol=$2 AND exchange_trade_id=$3")
            .bind(format!("{:?}", fill.venue))
            .bind(&fill.symbol)
            .bind(trade_id)
            .fetch_all(connection)
            .await?;
    for row in rows {
        if row.order_id != fill.order_id.0
            || row.side != format!("{:?}", fill.side)
            || row.price != fill.price
            || row.quantity != fill.quantity
            || row.fee != fill.fee
            || row.fee_currency != fill.fee_currency
            || row.filled_at.timestamp_micros() != fill.filled_at.timestamp_micros()
        {
            return Err(discrepancy(
                "conflicting exchange trade identity or execution payload",
            ));
        }
    }
    Ok(())
}

impl OrderStore {
    pub(crate) fn inventory_bound(&self) -> bool {
        self.inventory_lease.is_some()
    }

    /// Persist executions and cumulative status in one transaction. If a
    /// page is incomplete or a duplicate contradicts persisted evidence,
    /// the whole update rolls back and the order stays recoverable.
    pub(crate) async fn commit_exchange_update(
        &self,
        candidate: &mut Order,
        exchange_fills: Vec<ExchangeFill>,
        expected: Option<Decimal>,
    ) -> Result<(Vec<Fill>, bool), OmsError> {
        if let Some(quantity) = expected
            && (!consistent_status(candidate, quantity) || quantity < candidate.filled_quantity)
        {
            return Err(discrepancy(
                "inconsistent or regressive cumulative execution quantity",
            ));
        }
        let mut tx = self.pool.begin().await?;
        let mut new_fills = Vec::new();
        for execution in exchange_fills {
            let fill = Fill {
                id: FillId(Uuid::new_v4()),
                order_id: candidate.id,
                symbol: candidate.symbol.clone(),
                venue: candidate.venue,
                side: candidate.side,
                price: execution.price,
                quantity: execution.quantity,
                fee: execution.fee,
                fee_currency: execution.fee_currency,
                filled_at: execution.filled_at,
                leverage: candidate.leverage,
                margin_mode: candidate.margin_mode,
            };
            if expected.is_some() {
                validate_fill(&mut tx, &fill, &execution.exchange_trade_id).await?;
            }
            if insert_fill_on(&mut tx, &fill, &execution.exchange_trade_id).await? {
                new_fills.push(fill);
            }
        }
        let total: Decimal =
            sqlx::query_scalar("SELECT COALESCE(SUM(quantity), 0) FROM fills WHERE order_id=$1")
                .bind(candidate.id.0)
                .fetch_one(&mut *tx)
                .await?;
        if expected.is_some_and(|quantity| quantity != total) {
            return Err(discrepancy(
                "exchange cumulative execution quantity disagrees with persisted fills",
            ));
        }
        let result = sqlx::query(
            "UPDATE orders SET exchange_order_id=$2, status=$3, filled_quantity=$4,
              updated_at=$5, submission_state=$6,
              terminal_delivery_seq=CASE
                WHEN $7 AND status IN ('New','PartiallyFilled')
                  AND $3 IN ('Filled','Cancelled','Rejected')
                  AND NULLIF(btrim($2),'') IS NOT NULL
                THEN COALESCE(terminal_delivery_seq,nextval('oms_terminal_delivery_seq'))
                ELSE terminal_delivery_seq END
              WHERE id=$1",
        )
        .bind(candidate.id.0)
        .bind(&candidate.exchange_order_id)
        .bind(format!("{:?}", candidate.status))
        .bind(total)
        .bind(candidate.updated_at)
        .bind(SubmissionState::from_order(candidate).as_str())
        .bind(expected.is_some())
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(discrepancy(
                "order disappeared during execution reconciliation",
            ));
        }
        // Do not discard an authoritative execution because it exceeded
        // its pre-dispatch budget. Persist the breach with the execution,
        // including cumulative native spend-asset fees, then latch health.
        // For legacy rows without a reservation, no budget is invented.
        let breached: bool = sqlx::query_scalar(
            "UPDATE orders o SET reservation_breached = o.reservation_breached OR
             (o.reserved_amount IS NOT NULL AND o.reserved_amount <
               (SELECT COALESCE(SUM(
                 CASE WHEN o.side='Buy' THEN f.price*f.quantity ELSE f.quantity END
                 + CASE WHEN f.fee_currency=o.reserved_spend_asset
                     OR (f.fee_currency IS NULL AND o.side='Buy')
                   THEN f.fee ELSE 0 END),0) FROM fills f WHERE f.order_id=o.id))
             OR (o.reserved_market_buy_cap IS NOT NULL AND EXISTS
                (SELECT 1 FROM fills f WHERE f.order_id=o.id AND f.price>o.reserved_market_buy_cap))
             WHERE o.id=$1 RETURNING reservation_breached",
        )
        .bind(candidate.id.0)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        candidate.filled_quantity = total;
        Ok((new_fills, breached))
    }

    pub(crate) async fn latest_fill_sequence(&self) -> Result<i64, OmsError> {
        Ok(
            sqlx::query_scalar("SELECT COALESCE(MAX(delivery_seq), 0) FROM fills")
                .fetch_one(&self.pool)
                .await?,
        )
    }

    /// A bounded batch, ordered by exchange execution order. A fill older
    /// than already applied history for its symbol cannot be applied
    /// incrementally without changing the weighted cost basis: halt and
    /// reconstruct on restart instead. Never skip that fill.
    pub(crate) async fn fills_after(&self, checkpoint: i64) -> Result<(i64, Vec<Fill>), OmsError> {
        let high: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(delivery_seq) FROM (SELECT delivery_seq FROM fills WHERE delivery_seq>$1 ORDER BY delivery_seq LIMIT 256) next"
        ).bind(checkpoint).fetch_one(&self.pool).await?;
        let Some(high) = high else {
            return Ok((checkpoint, Vec::new()));
        };
        let late: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM fills f JOIN fills previous
              ON previous.venue=f.venue AND previous.symbol=f.symbol
              WHERE f.delivery_seq>$1 AND f.delivery_seq<=$2 AND previous.delivery_seq<=$1
              AND (previous.filled_at, length(previous.exchange_trade_id), previous.exchange_trade_id)
                > (f.filled_at, length(f.exchange_trade_id), f.exchange_trade_id))"
        ).bind(checkpoint).bind(high).fetch_one(&self.pool).await?;
        if late {
            return Err(discrepancy(
                "late exchange execution precedes applied inventory history",
            ));
        }
        let rows: Vec<FillRow> = sqlx::query_as(
            "SELECT * FROM fills WHERE delivery_seq>$1 AND delivery_seq<=$2
              ORDER BY filled_at, venue, symbol, length(exchange_trade_id), exchange_trade_id, id",
        )
        .bind(checkpoint)
        .bind(high)
        .fetch_all(&self.pool)
        .await?;
        let fills = rows
            .into_iter()
            .map(Fill::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((high, fills))
    }
}
