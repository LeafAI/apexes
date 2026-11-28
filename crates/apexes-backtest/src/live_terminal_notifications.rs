//! F04.04 — authoritative live terminal callbacks in the unified loop.
//! No inference from disappeared orders; only the managed executor opts in.
use apexes_core::OrderStatus;
use apexes_ems::{Ems, FillParentEvidence};
use apexes_strategy::{
    IntentId, LiveTerminalEvidence, LiveTerminalStatus, OrderExecutor, ParentOrderId,
    PersistedFillLink, Strategy, TerminalKind, TerminalOutcome,
};
use uuid::Uuid;

pub(crate) async fn deliver(
    strategy: &mut impl Strategy,
    executor: &dyn OrderExecutor,
    ems: &dyn Ems,
) {
    let batch = executor.ready_live_terminals().await;
    if batch.is_empty() {
        return;
    }
    let children: Vec<_> = batch
        .iter()
        .filter_map(|e| matches!(e.link, PersistedFillLink::Child { .. }).then_some(e.order.id))
        .collect();
    let mut parents = if children.is_empty() {
        Vec::new()
    } else {
        match ems.inspect_live_terminal_parents(&children).await {
            Ok(parents) if parents.len() == children.len() => parents,
            _ => vec![FillParentEvidence::Unavailable; children.len()],
        }
    }
    .into_iter();
    let mut returned = Vec::with_capacity(batch.len());
    for evidence in &batch {
        if !valid_status(evidence) {
            executor.report_live_fill_attribution_fault();
            break;
        }
        let parent = matches!(evidence.link, PersistedFillLink::Child { .. })
            .then(|| parents.next())
            .flatten();
        let attribution = attribute(evidence, parent, executor.intent_run_scope());
        if attribution.is_none() {
            executor.report_live_fill_attribution_fault();
        }
        let (intent_id, parent_id, dispatch_id) = attribution.unwrap_or_default();
        strategy.on_order_terminal(&TerminalOutcome {
            intent_id,
            at: chrono::Utc::now(),
            kind: TerminalKind::Live {
                order: &evidence.order,
                status: evidence.status,
                parent_id,
                dispatch_id,
            },
        });
        returned.push(evidence.order.id);
    }
    executor.acknowledge_live_terminals(&returned).await;
}

type Attribution = (Option<IntentId>, Option<ParentOrderId>, Option<Uuid>);

fn attribute(
    evidence: &LiveTerminalEvidence,
    parent: Option<FillParentEvidence>,
    scope: Option<Uuid>,
) -> Option<Attribution> {
    match evidence.link {
        PersistedFillLink::Unlinked => Some((None, None, None)),
        PersistedFillLink::Unavailable => None,
        PersistedFillLink::Direct {
            run_scope,
            intent_id,
        } => Some(((scope == Some(run_scope)).then_some(intent_id), None, None)),
        PersistedFillLink::Child {
            parent_id,
            dispatch_id,
        } => match parent {
            Some(FillParentEvidence::Linked {
                order_id,
                link,
                intent_link,
                ..
            }) if order_id == evidence.order.id
                && link.parent_id == parent_id
                && link.dispatch_id == dispatch_id =>
            {
                Some((
                    intent_link
                        .filter(|intent| scope == Some(intent.run_scope))
                        .map(|intent| intent.intent_id),
                    Some(parent_id),
                    Some(dispatch_id),
                ))
            }
            _ => None,
        },
    }
}

fn valid_status(evidence: &LiveTerminalEvidence) -> bool {
    let order = &evidence.order;
    !order.id.0.is_nil()
        && order
            .exchange_order_id
            .as_ref()
            .is_some_and(|id| !id.trim().is_empty())
        && matches!(
            (order.status, evidence.status),
            (OrderStatus::Filled, LiveTerminalStatus::Filled)
                | (OrderStatus::Cancelled, LiveTerminalStatus::Cancelled)
                | (OrderStatus::Rejected, LiveTerminalStatus::Rejected)
        )
}
