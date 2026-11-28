//! F04.04 — Session-local notification deduplication. Never participates
//! in settlement or exchange acknowledgement.
use apexes_core::{Fill, FillId};
use apexes_strategy::{FillOutcome, FillSource, Strategy};
use chrono::{DateTime, Utc};
use std::collections::HashSet;

#[derive(Default)]
pub(crate) struct FillNotifications {
    seen: HashSet<FillId>,
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
                strategy.on_fill(&FillOutcome {
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
