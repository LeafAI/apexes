//! F04.04: read-only evidence for an interrupted EMS dispatch.
//! Does not construct an EMS or clear its startup guard.
use apexes_core::OrderId;
use apexes_oms::{ChildLink, SubmissionState};
use apexes_strategy::ParentOrderId;
use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

use crate::EmsError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchEvidence {
    ParentMissing,
    NotPending,
    LegacyPending,
    MissingOms {
        link: ChildLink,
    },
    Conflict,
    Linked {
        link: ChildLink,
        order_id: OrderId,
        submission_state: SubmissionState,
    },
}

#[derive(sqlx::FromRow)]
struct Parent {
    dispatch_pending: bool,
    pending_dispatch_id: Option<Uuid>,
    status: String,
    symbol: String,
    venue: String,
    side: String,
    total_quantity: Decimal,
    filled_quantity: Decimal,
}

#[derive(sqlx::FromRow)]
struct Child {
    id: Uuid,
    ems_parent_id: Option<Uuid>,
    symbol: String,
    venue: String,
    side: String,
    asset_class: String,
    quantity: Decimal,
    submission_state: String,
    intent_scope: Option<Uuid>,
    intent_ordinal: Option<Decimal>,
}

/// Use an already migrated, correctly account/schema-bound pool. All
/// local evidence comes from one read-only repeatable-read snapshot.
/// Neither absence nor a linked row establishes exchange execution.
/// The caller can separately inspect the exact OMS client identity at
/// the gateway; no state mutation or recovery authorization is implied.
pub async fn inspect_pending_dispatch(
    pool: &PgPool,
    parent_id: ParentOrderId,
) -> Result<DispatchEvidence, EmsError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL statement_timeout = '2000ms'")
        .execute(&mut *tx)
        .await?;
    let parent: Option<Parent> = sqlx::query_as("SELECT dispatch_pending,pending_dispatch_id,status,symbol,venue,side,total_quantity,filled_quantity FROM parent_orders WHERE id=$1")
        .bind(parent_id.0).fetch_optional(&mut *tx).await?;
    let evidence = match parent {
        None => DispatchEvidence::ParentMissing,
        Some(parent) if !parent.dispatch_pending => {
            if parent.pending_dispatch_id.is_some() {
                DispatchEvidence::Conflict
            } else {
                DispatchEvidence::NotPending
            }
        }
        Some(parent) => inspect_marker(&mut tx, parent_id, parent).await?,
    };
    tx.commit().await?;
    Ok(evidence)
}

async fn inspect_marker(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    parent_id: ParentOrderId,
    parent: Parent,
) -> Result<DispatchEvidence, EmsError> {
    let Some(dispatch_id) = parent.pending_dispatch_id else {
        return Ok(DispatchEvidence::LegacyPending);
    };
    let link = ChildLink {
        parent_id,
        dispatch_id,
    };
    if dispatch_id.is_nil()
        || parent_id.0.is_nil()
        || parent.status != "Scheduling"
        || parent.filled_quantity < Decimal::ZERO
        || parent.total_quantity <= parent.filled_quantity
    {
        return Ok(DispatchEvidence::Conflict);
    }
    // Search globally within the bound schema by dispatch, not by parent:
    // a different owner must be reported as conflict, not missing.
    let children: Vec<Child> = sqlx::query_as("SELECT id,ems_parent_id,symbol,venue,side,asset_class,quantity,submission_state,intent_scope,intent_ordinal FROM orders WHERE ems_dispatch_id=$1 LIMIT 2")
        .bind(dispatch_id).fetch_all(&mut **tx).await?;
    let child_id = children.first().map(|child| child.id);
    let completed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM parent_order_children WHERE dispatch_id=$1 OR child_order_id=$2)")
        .bind(dispatch_id).bind(child_id).fetch_one(&mut **tx).await?;
    if completed || children.len() > 1 {
        return Ok(DispatchEvidence::Conflict);
    }
    let Some(child) = children.into_iter().next() else {
        return Ok(DispatchEvidence::MissingOms { link });
    };
    Ok(classify_child(link, &parent, child))
}

fn classify_child(link: ChildLink, parent: &Parent, child: Child) -> DispatchEvidence {
    let remaining = parent.total_quantity.checked_sub(parent.filled_quantity);
    if child.id.is_nil()
        || child.ems_parent_id != Some(link.parent_id.0)
        || child.symbol != parent.symbol
        || child.venue != parent.venue
        || child.side != parent.side
        || child.asset_class != "Spot"
        || child.quantity <= Decimal::ZERO
        || remaining.is_none_or(|qty| child.quantity > qty)
        || child.intent_scope.is_some()
        || child.intent_ordinal.is_some()
    {
        return DispatchEvidence::Conflict;
    }
    let submission_state = match child.submission_state.as_str() {
        "Submitting" => SubmissionState::Submitting,
        "Uncertain" => SubmissionState::Uncertain,
        "Accepted" => SubmissionState::Accepted,
        "Rejected" => SubmissionState::Rejected,
        _ => return DispatchEvidence::Conflict,
    };
    DispatchEvidence::Linked {
        link,
        order_id: OrderId(child.id),
        submission_state,
    }
}
