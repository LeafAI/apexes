//! F03.04: deterministic traversal at portfolio arithmetic and execution boundaries.
use std::collections::HashMap;

pub(crate) fn entries<V>(map: &HashMap<String, V>) -> Vec<(&String, &V)> {
    let mut entries: Vec<_> = map.iter().collect();
    entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
    entries
}
