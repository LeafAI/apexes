//! F04.04 — Session-local intent correlation, independent of admission.
//! Historical and live IDs are only linked using actual returned identities.
use apexes_core::OrderId;
use apexes_strategy::{IntentId, ParentOrderId};
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

#[derive(Default)]
pub(crate) struct LifecycleCorrelation {
    next: u64,
    orders: HashMap<OrderId, Option<IntentId>>,
    parents: HashMap<ParentOrderId, Option<IntentId>>,
    children: HashMap<OrderId, Option<ParentOrderId>>,
    ambiguous_parents: HashSet<ParentOrderId>,
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
    pub(crate) fn archive_evidence(&self) -> crate::lifecycle_archive::Evidence {
        let mut orders: Vec<_> = self
            .orders
            .iter()
            .map(|(key, id)| (key.0, id.map(|i| i.0)))
            .collect();
        let mut parents: Vec<_> = self
            .parents
            .iter()
            .map(|(key, id)| (key.0, id.map(|i| i.0)))
            .collect();
        let mut children: Vec<_> = self
            .children
            .iter()
            .map(|(key, id)| (key.0, id.map(|i| i.0)))
            .collect();
        let mut ambiguous_parents: Vec<_> = self.ambiguous_parents.iter().map(|id| id.0).collect();
        orders.sort_unstable_by_key(|x| x.0);
        parents.sort_unstable_by_key(|x| x.0);
        children.sort_unstable_by_key(|x| x.0);
        ambiguous_parents.sort_unstable();
        crate::lifecycle_archive::Evidence {
            last_intent: self.next,
            orders,
            parents,
            children,
            ambiguous_parents,
            ..Default::default()
        }
    }

    pub(crate) fn allocate(&mut self) -> Option<IntentId> {
        self.next = self.next.checked_add(1)?;
        Some(IntentId(self.next))
    }
    pub(crate) fn bind_order(&mut self, order: OrderId, id: Option<IntentId>) {
        if let Some(parent) = self.children.get_mut(&order) {
            if let Some(id) = parent.take() {
                self.ambiguous_parents.insert(id);
            }
            self.orders.insert(order, None);
            return;
        }
        link(&mut self.orders, order, id);
    }
    pub(crate) fn bind_parent(&mut self, parent: ParentOrderId, id: Option<IntentId>) {
        link(&mut self.parents, parent, id);
    }
    pub(crate) fn order(&self, order: OrderId) -> Option<IntentId> {
        if let Some(parent) = self.children.get(&order) {
            return parent.and_then(|id| self.parent(id));
        }
        self.orders.get(&order).copied().flatten()
    }
    /// Consume new authoritative EMS membership once, without scanning
    /// the full historical parent/child lists at every market event.
    pub(crate) fn observe_children(&mut self, ems: &dyn apexes_ems::Ems) {
        for (child, parent) in ems.take_child_associations() {
            if self.parents.contains_key(&parent) {
                self.bind_child(child, parent);
            }
        }
    }

    pub(crate) fn bind_child(&mut self, child: OrderId, parent: ParentOrderId) {
        if self.orders.contains_key(&child) {
            if let Some(Some(prior)) = self.children.insert(child, None) {
                self.ambiguous_parents.insert(prior);
            }
            self.orders.insert(child, None);
            self.ambiguous_parents.insert(parent);
        } else {
            self.children
                .entry(child)
                .and_modify(|prior| {
                    if *prior != Some(parent) {
                        if let Some(id) = prior.take() {
                            self.ambiguous_parents.insert(id);
                        }
                        self.ambiguous_parents.insert(parent);
                        tracing::warn!(?child, "conflicting EMS child ownership");
                    }
                })
                .or_insert(Some(parent));
        }
    }

    pub(crate) fn parent_progress_valid(&self, parent: ParentOrderId) -> bool {
        !self.ambiguous_parents.contains(&parent)
    }

    pub(crate) fn child_parent(&self, child: OrderId) -> Option<ParentOrderId> {
        self.children.get(&child).copied().flatten()
    }

    pub(crate) fn parent(&self, parent: ParentOrderId) -> Option<IntentId> {
        self.parents.get(&parent).copied().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn child_associations_never_guess_or_recover_conflicts() {
        let mut c = LifecycleCorrelation::default();
        let a = ParentOrderId(uuid::Uuid::from_u128(1));
        let b = ParentOrderId(uuid::Uuid::from_u128(2));
        c.bind_parent(a, Some(IntentId(1)));
        c.bind_parent(b, Some(IntentId(2)));
        let child = OrderId(uuid::Uuid::from_u128(3));
        assert_eq!(c.child_parent(child), None);
        c.bind_child(child, a);
        c.bind_child(child, a);
        assert_eq!(c.child_parent(child), Some(a));
        assert_eq!(c.order(child), Some(IntentId(1)));
        assert!(c.parent_progress_valid(a));
        c.bind_child(child, b);
        c.bind_child(child, a);
        assert_eq!(c.child_parent(child), None);
        assert_eq!(c.order(child), None);
        assert!(!c.parent_progress_valid(a));
        assert!(!c.parent_progress_valid(b));
        let direct = OrderId(uuid::Uuid::from_u128(4));
        c.bind_order(direct, Some(IntentId(3)));
        c.bind_child(direct, a);
        assert_eq!(c.order(direct), None);
        assert_eq!(c.child_parent(direct), None);
        let reverse = OrderId(uuid::Uuid::from_u128(5));
        c.bind_child(reverse, b);
        c.bind_order(reverse, Some(IntentId(3)));
        assert_eq!(c.order(reverse), None);
        assert_eq!(c.child_parent(reverse), None);
    }

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
