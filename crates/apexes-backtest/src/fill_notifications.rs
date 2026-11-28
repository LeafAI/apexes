//! F04.04 — Session-local notification deduplication. Never participates
//! in settlement or exchange acknowledgement.
use apexes_core::{Fill, FillId};
use apexes_strategy::ParentOrderId;
use apexes_strategy::{FillOutcome, FillSource, IntentId, Strategy};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(crate) struct FillNotifications {
    seen: HashSet<FillId>,
    parent_executed: HashMap<ParentOrderId, Option<Decimal>>,
    live_progress_uncertain: bool,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct NotificationAttribution {
    pub intent_id: Option<IntentId>,
    pub parent_id: Option<ParentOrderId>,
    pub parent_progress_valid: bool,
}

impl FillNotifications {
    pub(crate) fn archive_evidence(
        &self,
        correlation: &crate::lifecycle_correlation::LifecycleCorrelation,
    ) -> crate::lifecycle_archive::Evidence {
        let mut evidence = correlation.archive_evidence();
        evidence.seen_fills = self.seen.iter().map(|id| id.0).collect();
        evidence.seen_fills.sort_unstable();
        evidence.parent_executed = self
            .parent_executed
            .iter()
            .map(|(id, total)| {
                (
                    id.0,
                    if !self.live_progress_uncertain && correlation.parent_progress_valid(*id) {
                        *total
                    } else {
                        None
                    },
                )
            })
            .collect();
        evidence.parent_executed.sort_unstable_by_key(|x| x.0);
        evidence
    }

    /// Caller supplies only newly applied fills, after bookkeeping.
    /// Preserve the batch order. Duplicate IDs never invoke the callback.
    pub(crate) fn deliver(
        &mut self,
        strategy: &mut impl Strategy,
        at: DateTime<Utc>,
        source: FillSource,
        fills: &[Fill],
        correlation: &crate::lifecycle_correlation::LifecycleCorrelation,
    ) -> bool {
        self.deliver_with(strategy, at, source, fills, |fill| {
            let parent_id = correlation.child_parent(fill.order_id);
            NotificationAttribution {
                intent_id: correlation.order(fill.order_id),
                parent_id,
                parent_progress_valid: parent_id
                    .is_some_and(|id| correlation.parent_progress_valid(id)),
            }
        })
    }

    /// Managed live callbacks use positive persisted evidence only. Never
    /// fall back to a possibly stale in-memory ordinal or child association.
    pub(crate) fn deliver_persisted(
        &mut self,
        strategy: &mut impl Strategy,
        at: DateTime<Utc>,
        fills: &[Fill],
        evidence: &crate::live_fill_attribution::LiveFillAttributions,
    ) -> bool {
        self.live_progress_uncertain |= !evidence.complete;
        let valid = !self.live_progress_uncertain;
        self.deliver_with(strategy, at, FillSource::Supplemental, fills, |fill| {
            let mut attribution = evidence.links.get(&fill.id).copied().unwrap_or_default();
            attribution.parent_progress_valid &= valid;
            attribution
        })
    }

    fn deliver_with(
        &mut self,
        strategy: &mut impl Strategy,
        at: DateTime<Utc>,
        source: FillSource,
        fills: &[Fill],
        mut attribute: impl FnMut(&Fill) -> NotificationAttribution,
    ) -> bool {
        let mut all_returned = true;
        for fill in fills {
            if self.seen.insert(fill.id) {
                let link = attribute(fill);
                let parent_executed_quantity = link
                    .parent_id
                    .filter(|_| link.parent_progress_valid)
                    .and_then(|id| {
                        let total = self
                            .parent_executed
                            .entry(id)
                            .or_insert(Some(Decimal::ZERO));
                        *total = total
                            .filter(|_| fill.quantity > Decimal::ZERO)
                            .and_then(|q| q.checked_add(fill.quantity));
                        *total
                    });
                strategy.on_fill(&FillOutcome {
                    parent_id: link.parent_id,
                    parent_executed_quantity,
                    intent_id: link.intent_id,
                    at,
                    source,
                    fill,
                });
            } else {
                tracing::warn!(fill_id=?fill.id, "duplicate fill notification suppressed");
                all_returned = false;
            }
        }
        all_returned
    }
}
