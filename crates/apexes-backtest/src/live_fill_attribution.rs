//! F04.04: use durable evidence for managed live notifications only.
//! Historical and unmanaged executors keep their original semantics.
use std::collections::HashMap;

use apexes_core::{Fill, FillId};
use apexes_ems::{Ems, FillParentEvidence};
use apexes_strategy::{OrderExecutor, PersistedFillLink};

use crate::fill_notifications::NotificationAttribution;

pub(crate) struct LiveFillAttributions {
    pub links: HashMap<FillId, NotificationAttribution>,
    pub complete: bool,
}

pub(crate) async fn resolve(
    executor: &dyn OrderExecutor,
    ems: &dyn Ems,
    fills: &[Fill],
) -> Option<LiveFillAttributions> {
    if fills.is_empty() {
        return None;
    }
    let oms = executor.persisted_supplemental_fill_links(fills).await?;
    if oms.len() != fills.len() {
        executor.report_live_fill_attribution_fault();
        return Some(LiveFillAttributions {
            links: HashMap::new(),
            complete: false,
        });
    }
    let child_ids: Vec<_> = fills
        .iter()
        .zip(&oms)
        .filter_map(|(fill, link)| {
            matches!(link, PersistedFillLink::Child { .. }).then_some(fill.id)
        })
        .collect();
    let mut parent_evidence = if child_ids.is_empty() {
        Vec::new()
    } else {
        match ems.inspect_live_fill_parents(&child_ids).await {
            Ok(evidence) if evidence.len() == child_ids.len() => evidence,
            _ => vec![FillParentEvidence::Unavailable; child_ids.len()],
        }
    }
    .into_iter();
    let mut complete = true;
    let mut links = HashMap::with_capacity(fills.len());
    let scope = executor.intent_run_scope();
    for (fill, link) in fills.iter().zip(oms) {
        let parent = matches!(link, PersistedFillLink::Child { .. })
            .then(|| parent_evidence.next())
            .flatten();
        let attribution = resolve_link(fill, link, parent, scope);
        complete &= attribution.is_some();
        links.insert(fill.id, attribution.unwrap_or_default());
    }
    if !complete {
        tracing::warn!("durable live fill attribution incomplete; no inferred ownership");
        executor.report_live_fill_attribution_fault();
    }
    Some(LiveFillAttributions { links, complete })
}

fn resolve_link(
    fill: &Fill,
    link: PersistedFillLink,
    parent: Option<FillParentEvidence>,
    scope: Option<uuid::Uuid>,
) -> Option<NotificationAttribution> {
    match link {
        PersistedFillLink::Unlinked => Some(NotificationAttribution::default()),
        PersistedFillLink::Unavailable => None,
        PersistedFillLink::Direct {
            run_scope,
            intent_id,
        } => Some(NotificationAttribution {
            intent_id: (scope == Some(run_scope)).then_some(intent_id),
            ..Default::default()
        }),
        PersistedFillLink::Child {
            parent_id,
            dispatch_id,
        } => match parent {
            Some(FillParentEvidence::Linked {
                order_id,
                link,
                intent_link,
                ..
            }) if order_id == fill.order_id
                && link.parent_id == parent_id
                && link.dispatch_id == dispatch_id =>
            {
                Some(NotificationAttribution {
                    intent_id: intent_link
                        .filter(|intent| scope == Some(intent.run_scope))
                        .map(|intent| intent.intent_id),
                    parent_id: Some(parent_id),
                    parent_progress_valid: true,
                })
            }
            _ => None,
        },
    }
}
