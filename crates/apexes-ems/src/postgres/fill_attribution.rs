//! F04.04: read-only parent attribution for an actual persisted execution.
//! The caller supplies an already migrated, account/schema-bound pool.
use apexes_core::{FillId, OrderId};
use apexes_oms::{ChildLink, IntentLink};
use apexes_strategy::{IntentId, ParentOrderId};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::{ChildAssociation, EmsError, FillParentEvidence};

#[derive(sqlx::FromRow)]
struct Child {
    order_id: Uuid,
    ems_parent_id: Option<Uuid>,
    ems_dispatch_id: Option<Uuid>,
    symbol: String,
    venue: String,
    side: String,
    asset_class: String,
    quantity: Decimal,
    matching_fill: bool,
    has_direct_intent: bool,
}

#[derive(sqlx::FromRow)]
struct Parent {
    symbol: String,
    venue: String,
    side: String,
    total_quantity: Decimal,
    filled_quantity: Decimal,
    status: String,
    dispatch_pending: bool,
    pending_dispatch_id: Option<Uuid>,
    intent_scope: Option<Uuid>,
    intent_ordinal: Option<Decimal>,
}

fn parent_intent(parent: &Parent) -> Option<Option<IntentLink>> {
    match (parent.intent_scope, parent.intent_ordinal) {
        (None, None) => Some(None),
        (Some(run_scope), Some(ordinal)) if !run_scope.is_nil() && ordinal.fract().is_zero() => {
            let id = ordinal.to_u64().filter(|id| *id != 0)?;
            Some(Some(IntentLink {
                run_scope,
                intent_id: IntentId(id),
            }))
        }
        _ => None,
    }
}

/// Inspect exact fill/OMS/EMS identities from one repeatable-read snapshot,
/// even when EMS startup is refused. A pending association is not repaired,
/// and no absence or inconsistency establishes exchange nonexecution.
pub async fn inspect_fill_parent(
    pool: &PgPool,
    fill_id: FillId,
) -> Result<FillParentEvidence, EmsError> {
    let mut evidence = inspect_fill_parents(pool, &[fill_id]).await?;
    Ok(evidence.pop().unwrap_or(FillParentEvidence::Unavailable))
}

/// A single read-only snapshot for the bounded set of child fills in one
/// supplemental batch. No worker is constructed or restarted.
pub(crate) async fn inspect_fill_parents(
    pool: &PgPool,
    fill_ids: &[FillId],
) -> Result<Vec<FillParentEvidence>, EmsError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL statement_timeout = '2000ms'")
        .execute(&mut *tx)
        .await?;
    let mut evidence = Vec::with_capacity(fill_ids.len());
    for id in fill_ids {
        evidence.push(inspect_fill_parent_in(&mut tx, *id).await?);
    }
    tx.commit().await?;
    Ok(evidence)
}

/// Resolve child ownership for a positively reconciled terminal order,
/// including a zero-fill cancellation. Same classifier as fill evidence.
pub(crate) async fn inspect_terminal_parents(
    pool: &PgPool,
    order_ids: &[OrderId],
) -> Result<Vec<FillParentEvidence>, EmsError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL statement_timeout = '2000ms'")
        .execute(&mut *tx)
        .await?;
    let mut evidence = Vec::with_capacity(order_ids.len());
    for id in order_ids {
        let child: Option<Child> = sqlx::query_as(
            "SELECT o.id AS order_id,o.ems_parent_id,o.ems_dispatch_id,o.symbol,o.venue,o.side,
             o.asset_class,o.quantity,
             (o.filled_quantity>=0 AND o.filled_quantity<=o.quantity) AS matching_fill,
             (o.intent_scope IS NOT NULL OR o.intent_ordinal IS NOT NULL) AS has_direct_intent
             FROM orders o WHERE o.id=$1 AND o.terminal_delivery_seq IS NOT NULL",
        )
        .bind(id.0)
        .fetch_optional(&mut *tx)
        .await?;
        evidence.push(match child {
            Some(child) => inspect_child(&mut tx, child).await?,
            None => FillParentEvidence::Unavailable,
        });
    }
    tx.commit().await?;
    Ok(evidence)
}

async fn inspect_fill_parent_in(
    tx: &mut Transaction<'_, Postgres>,
    fill_id: FillId,
) -> Result<FillParentEvidence, EmsError> {
    let child: Option<Child> = sqlx::query_as(
        "SELECT o.id AS order_id,o.ems_parent_id,o.ems_dispatch_id,o.symbol,o.venue,o.side,
         o.asset_class,o.quantity,
         (f.symbol=o.symbol AND f.venue=o.venue AND f.side=o.side
          AND f.quantity>0 AND f.quantity<=o.quantity) AS matching_fill,
         (o.intent_scope IS NOT NULL OR o.intent_ordinal IS NOT NULL) AS has_direct_intent
         FROM fills f JOIN orders o ON o.id=f.order_id WHERE f.id=$1",
    )
    .bind(fill_id.0)
    .fetch_optional(&mut **tx)
    .await?;
    match child {
        None => Ok(FillParentEvidence::MissingFill),
        Some(child) => inspect_child(tx, child).await,
    }
}

async fn inspect_child(
    tx: &mut Transaction<'_, Postgres>,
    child: Child,
) -> Result<FillParentEvidence, EmsError> {
    let link = match (child.ems_parent_id, child.ems_dispatch_id) {
        (None, None) => return Ok(FillParentEvidence::NotChild),
        (Some(parent), Some(dispatch)) if !parent.is_nil() && !dispatch.is_nil() => ChildLink {
            parent_id: ParentOrderId(parent),
            dispatch_id: dispatch,
        },
        _ => return Ok(FillParentEvidence::Conflict),
    };
    let parent: Option<Parent> = sqlx::query_as(
        "SELECT symbol,venue,side,total_quantity,filled_quantity,status,dispatch_pending,
         pending_dispatch_id,intent_scope,intent_ordinal FROM parent_orders WHERE id=$1",
    )
    .bind(link.parent_id.0)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(parent) = parent else {
        return Ok(FillParentEvidence::Conflict);
    };
    let Some(intent_link) = parent_intent(&parent) else {
        return Ok(FillParentEvidence::Conflict);
    };
    if !child_matches(&child, &parent) {
        return Ok(FillParentEvidence::Conflict);
    }
    // Search by both keys across the bound schema. A contradictory owner
    // cannot be hidden by filtering by the expected parent first.
    let associations: Vec<(Uuid, Uuid, Option<Uuid>)> = sqlx::query_as(
        "SELECT parent_order_id,child_order_id,dispatch_id FROM parent_order_children
         WHERE child_order_id=$1 OR dispatch_id=$2 LIMIT 2",
    )
    .bind(child.order_id)
    .bind(link.dispatch_id)
    .fetch_all(&mut **tx)
    .await?;
    let pending = parent.dispatch_pending && parent.pending_dispatch_id == Some(link.dispatch_id);
    let association = match associations.as_slice() {
        [(owner, order, Some(dispatch))]
            if *owner == link.parent_id.0
                && *order == child.order_id
                && *dispatch == link.dispatch_id
                && !pending
                && child.quantity <= parent.filled_quantity =>
        {
            ChildAssociation::Recorded
        }
        [] if pending
            && parent.status == "Scheduling"
            && child.quantity <= parent.total_quantity - parent.filled_quantity =>
        {
            ChildAssociation::Pending
        }
        _ => return Ok(FillParentEvidence::Conflict),
    };
    Ok(FillParentEvidence::Linked {
        order_id: OrderId(child.order_id),
        link,
        intent_link,
        association,
    })
}

fn child_matches(child: &Child, parent: &Parent) -> bool {
    child.matching_fill
        && !child.has_direct_intent
        && !child.order_id.is_nil()
        && child.asset_class == "Spot"
        && child.symbol == parent.symbol
        && child.venue == parent.venue
        && child.side == parent.side
        && child.quantity > Decimal::ZERO
        && child.quantity <= parent.total_quantity
        && parent.filled_quantity >= Decimal::ZERO
        && parent.filled_quantity <= parent.total_quantity
        && (parent.dispatch_pending || parent.pending_dispatch_id.is_none())
}
