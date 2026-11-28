//! F04.04 — Authoritative terminal evidence and the existing fill watermark.
//! No inference from absence, legacy status, or a failed submission.
use super::{OrderStore, decode_child_link, decode_intent_link};
use crate::{ChildLink, IntentLink, OmsError, schema::OrderRow};
use apexes_core::{Order, OrderId, OrderStatus};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct TerminalEvidence {
    pub order: Order,
    pub sequence: i64,
    /// All persisted executions for this order must be represented in the
    /// recovered ledger or acknowledged notification prefix before delivery.
    pub required_fill_sequence: i64,
    pub intent_link: Option<IntentLink>,
    pub child_link: Option<ChildLink>,
}

#[derive(sqlx::FromRow)]
struct Row {
    #[sqlx(flatten)]
    order: OrderRow,
    terminal_delivery_seq: i64,
    submission_state: String,
    required_fill_sequence: i64,
    execution_quantity: Decimal,
    matching_fills: bool,
    intent_scope: Option<Uuid>,
    intent_ordinal: Option<Decimal>,
    ems_parent_id: Option<Uuid>,
    ems_dispatch_id: Option<Uuid>,
}

const SELECT: &str = "SELECT o.*, x.required_fill_sequence, x.execution_quantity, x.matching_fills
    FROM orders o CROSS JOIN LATERAL
    (SELECT COALESCE(MAX(f.delivery_seq),0) AS required_fill_sequence,
      COALESCE(SUM(f.quantity),0) AS execution_quantity,
      COALESCE(BOOL_AND(f.symbol=o.symbol AND f.venue=o.venue AND f.side=o.side
          AND f.quantity>0 AND f.price>0 AND f.fee>=0 AND f.delivery_seq>0
          AND f.exchange_trade_id<>''),TRUE) AS matching_fills
      FROM fills f WHERE f.order_id=o.id) x
    WHERE o.terminal_delivery_seq IS NOT NULL";

impl TryFrom<Row> for TerminalEvidence {
    type Error = OmsError;

    fn try_from(row: Row) -> Result<Self, OmsError> {
        let order = Order::try_from(row.order)?;
        let intent_link = decode_intent_link(row.intent_scope, row.intent_ordinal)?;
        let child_link = decode_child_link(row.ems_parent_id, row.ems_dispatch_id)?;
        let qty = order.filled_quantity;
        let valid = match order.status {
            OrderStatus::Filled => qty == order.quantity,
            OrderStatus::Cancelled => qty >= Decimal::ZERO && qty <= order.quantity,
            OrderStatus::Rejected => qty.is_zero(),
            _ => false,
        };
        if !valid
            || order.id.0.is_nil()
            || order.quantity <= Decimal::ZERO
            || row.execution_quantity != qty
            || !row.matching_fills
            || row.terminal_delivery_seq <= 0
            || row.required_fill_sequence < 0
            || row.submission_state != "Accepted"
            || order
                .exchange_order_id
                .as_ref()
                .is_none_or(|id| id.trim().is_empty())
            || (intent_link.is_some() && child_link.is_some())
        {
            return Err(OmsError::InvalidRow {
                reason: "contradictory terminal execution evidence".into(),
            });
        }
        Ok(Self {
            order,
            sequence: row.terminal_delivery_seq,
            required_fill_sequence: row.required_fill_sequence,
            intent_link,
            child_link,
        })
    }
}

impl OrderStore {
    /// Read-only inspection of a positively reconciled terminal order.
    /// None means no qualified marker, never proof of non-execution.
    pub async fn terminal_evidence(
        &self,
        id: OrderId,
    ) -> Result<Option<TerminalEvidence>, OmsError> {
        let sql = sqlx::AssertSqlSafe(format!("{SELECT} AND o.id=$1"));
        let row: Option<Row> = sqlx::query_as(sql)
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await?;
        row.map(TerminalEvidence::try_from).transpose()
    }

    pub(crate) async fn latest_terminal_sequence(&self) -> Result<i64, OmsError> {
        Ok(
            sqlx::query_scalar("SELECT COALESCE(MAX(terminal_delivery_seq),0) FROM orders")
                .fetch_one(&self.pool)
                .await?,
        )
    }

    /// A bounded prefix; never overtake an earlier terminal whose fill
    /// notifications have not returned. Sequence gaps from rollback are OK.
    pub(crate) async fn terminals_after(
        &self,
        checkpoint: i64,
        fill_checkpoint: i64,
    ) -> Result<Vec<TerminalEvidence>, OmsError> {
        let sql = sqlx::AssertSqlSafe(format!(
            "{SELECT} AND o.terminal_delivery_seq>$1
            ORDER BY o.terminal_delivery_seq LIMIT 128"
        ));
        let rows: Vec<Row> = sqlx::query_as(sql)
            .bind(checkpoint)
            .fetch_all(&self.pool)
            .await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let evidence = TerminalEvidence::try_from(row)?;
            if evidence.required_fill_sequence > fill_checkpoint {
                break;
            }
            result.push(evidence);
        }
        Ok(result)
    }
}
