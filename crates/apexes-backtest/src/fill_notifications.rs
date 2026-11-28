//! F04.04 — Session-local notification deduplication. Never participates
//! in settlement or exchange acknowledgement.
use apexes_core::{Fill, FillId};
use apexes_strategy::ParentOrderId;
use apexes_strategy::{FillOutcome, FillSource, Strategy};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(crate) struct FillNotifications {
    seen: HashSet<FillId>,
    parent_executed: HashMap<ParentOrderId, Option<Decimal>>,
}

impl FillNotifications {
    /// Caller supplies only newly applied fills, after bookkeeping.
    /// Preserve the batch order. Duplicate IDs never invoke the callback.
    pub(crate) fn deliver(
        &mut self,
        strategy: &mut impl Strategy,
        at: DateTime<Utc>,
        source: FillSource,
        fills: &[Fill],
        correlation: &crate::lifecycle_correlation::LifecycleCorrelation,
    ) {
        for fill in fills {
            if self.seen.insert(fill.id) {
                let parent_id = correlation.child_parent(fill.order_id);
                let parent_executed_quantity = parent_id
                    .filter(|id| correlation.parent_progress_valid(*id))
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
                    parent_id,
                    parent_executed_quantity,
                    intent_id: correlation.order(fill.order_id),
                    at,
                    source,
                    fill,
                });
            } else {
                tracing::warn!(fill_id=?fill.id, "duplicate fill notification suppressed");
            }
        }
    }
}
