//! F04.01: bounded instrument-keyed state for native and scripted strategies.
use apexes_core::{MarketEvent, Venue};
use std::collections::HashMap;

/// Session-local state. Venue and symbol are both part of the identity.
/// No implicit eviction: callers must handle a refused insertion.
pub struct SymbolState<T> {
    values: HashMap<(Venue, String), T>,
    max_symbols: usize,
}

impl<T> SymbolState<T> {
    pub fn new(max_symbols: usize) -> Self {
        Self {
            values: HashMap::new(),
            max_symbols,
        }
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = (&(Venue, String), &T)> {
        self.values.iter()
    }

    pub(crate) fn insert_checkpoint(&mut self, key: (Venue, String), value: T) -> Result<(), T> {
        if self.values.len() >= self.max_symbols || self.values.contains_key(&key) {
            return Err(value);
        }
        self.values.insert(key, value);
        Ok(())
    }

    pub fn get(&self, event: &MarketEvent) -> Option<&T> {
        self.values.get(&(event.venue(), event.symbol().to_owned()))
    }

    pub fn get_mut(&mut self, event: &MarketEvent) -> Option<&mut T> {
        self.values
            .get_mut(&(event.venue(), event.symbol().to_owned()))
    }

    pub fn can_insert(&self, event: &MarketEvent) -> bool {
        self.values.len() < self.max_symbols || self.get(event).is_some()
    }

    /// On capacity refusal, return the rejected value to the caller.
    pub fn insert(&mut self, event: &MarketEvent, value: T) -> Result<Option<T>, T> {
        if !self.can_insert(event) {
            return Err(value);
        }
        Ok(self
            .values
            .insert((event.venue(), event.symbol().to_owned()), value))
    }
}
