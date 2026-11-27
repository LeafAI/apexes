//! F04.03 — Read-only per-event instrument metadata and session rule snapshot.
//! A symbol-keyed ledger cannot safely disambiguate duplicate symbols.
use apexes_core::{Instrument, MarketEvent, SymbolRules};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
pub struct InstrumentContext<'a> {
    instrument: &'a Instrument,
    rules: Option<&'a SymbolRules>,
}

impl<'a> InstrumentContext<'a> {
    /// Resolve only a unique configured symbol with the event's venue.
    /// MarketEvent does not identify asset class; duplicate symbols are
    /// ambiguous even if their venues or asset classes differ.
    pub fn for_event(
        event: &MarketEvent,
        instruments: &'a [Instrument],
        rules: &'a HashMap<String, SymbolRules>,
    ) -> Option<Self> {
        let mut candidates = instruments.iter().filter(|i| i.symbol == event.symbol());
        let instrument = candidates.next()?;
        if candidates.next().is_some() || instrument.venue != event.venue() {
            return None;
        }
        Some(Self {
            instrument,
            rules: rules.get(&instrument.symbol),
        })
    }

    pub fn instrument(self) -> &'a Instrument {
        self.instrument
    }

    /// Session-supplied rules, not a claim of current exchange validity.
    pub fn rules(self) -> Option<&'a SymbolRules> {
        self.rules
    }

    pub fn matches(self, event: &MarketEvent) -> bool {
        self.instrument.symbol == event.symbol() && self.instrument.venue == event.venue()
    }
}
