//! F04.04 — Session-local intent correlation, independent of admission.
//! Historical and live IDs are only linked using actual returned identities.
use apexes_core::OrderId;
use apexes_strategy::{IntentId, ParentOrderId};
use std::{collections::HashMap, hash::Hash};

#[derive(Default)]
pub(crate) struct LifecycleCorrelation {
    next: u64,
    orders: HashMap<OrderId, Option<IntentId>>,
    parents: HashMap<ParentOrderId, Option<IntentId>>,
}

fn link<K: Eq + Hash + std::fmt::Debug>(
    map: &mut HashMap<K, Option<IntentId>>,
    key: K,
    id: Option<IntentId>,
) {
    map.entry(key)
        .and_modify(|prior| {
            if *prior != id {
                *prior = None;
                tracing::warn!("conflicting lifecycle correlation; identity unavailable");
            }
        })
        .or_insert(id);
}

impl LifecycleCorrelation {
    pub(crate) fn allocate(&mut self) -> Option<IntentId> {
        self.next = self.next.checked_add(1)?;
        Some(IntentId(self.next))
    }
    pub(crate) fn bind_order(&mut self, order: OrderId, id: Option<IntentId>) {
        link(&mut self.orders, order, id);
    }
    pub(crate) fn bind_parent(&mut self, parent: ParentOrderId, id: Option<IntentId>) {
        link(&mut self.parents, parent, id);
    }
    pub(crate) fn order(&self, order: OrderId) -> Option<IntentId> {
        self.orders.get(&order).copied().flatten()
    }
    pub(crate) fn parent(&self, parent: ParentOrderId) -> Option<IntentId> {
        self.parents.get(&parent).copied().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ordinal_is_checked_and_conflicting_bindings_remain_unknown() {
        let mut c = LifecycleCorrelation::default();
        let a = c.allocate();
        let b = c.allocate();
        assert_eq!(a, Some(IntentId(1)));
        assert_eq!(b, Some(IntentId(2)));
        let order = OrderId(uuid::Uuid::from_u128(1));
        let parent = ParentOrderId(order.0);
        assert_eq!(c.order(order), None);
        c.bind_order(order, a);
        c.bind_parent(parent, b);
        assert_eq!(c.order(order), a);
        assert_eq!(c.parent(parent), b);
        c.bind_order(order, b);
        c.bind_order(order, a);
        assert_eq!(c.order(order), None);
        assert_eq!(c.parent(parent), b);
        c.next = u64::MAX;
        assert_eq!(c.allocate(), None);
        assert_eq!(c.allocate(), None);
    }
}
