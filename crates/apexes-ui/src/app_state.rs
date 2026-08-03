//! In-memory accumulator turning incoming `apexes_core::MarketEvent`s
//! into a per-symbol table of bid/ask/last-trade price -- pure logic,
//! no I/O, no `egui`/network dependency, so it can be unit-tested
//! directly. Used by both `ui.rs` (Task 5, to render) and by tests in
//! `server.rs`/`client.rs` (Tasks 2/3) as a shared fixture shape.

use apexes_core::MarketEvent;
use rust_decimal::Decimal;
use std::collections::BTreeMap;

/// One instrument's current bid/ask/last-trade state. `None` fields
/// mean "no event of that kind has arrived yet for this symbol" --
/// the UI renders those as "-" rather than a stale/fabricated value.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RowState {
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
    pub last_trade: Option<Decimal>,
}

/// Accumulates `MarketEvent`s into a `RowState` per symbol.
/// `BTreeMap` (not `HashMap`) so `rows()`'s iteration order is
/// deterministic -- the table renders symbols in a stable order run
/// to run.
pub struct AppState {
    rows: BTreeMap<String, RowState>,
}

impl AppState {
    /// Pre-populates one empty `RowState` per symbol in `symbols`, so
    /// the table always shows every hardcoded instrument (as "-"
    /// cells) even before any event for it has arrived.
    pub fn new(symbols: &[String]) -> Self {
        let mut rows = BTreeMap::new();
        for symbol in symbols {
            rows.insert(symbol.clone(), RowState::default());
        }
        Self { rows }
    }

    /// Updates this symbol's row from `event`. Events for a symbol
    /// not in the pre-populated set are ignored (this phase's table
    /// only ever shows the hardcoded instrument list -- see the
    /// design spec's Non-Goals on runtime-configurable instruments).
    /// `Depth`/`Bar`/`MarkPrice` variants are ignored -- only
    /// `Trade`/`Tick` affect the table, matching what the server task
    /// (Task 2) filters before ever sending a frame.
    pub fn apply_event(&mut self, event: &MarketEvent) {
        match event {
            MarketEvent::Trade(trade) => {
                if let Some(row) = self.rows.get_mut(&trade.symbol) {
                    row.last_trade = Some(trade.price);
                }
            }
            MarketEvent::Tick(tick) => {
                if let Some(row) = self.rows.get_mut(&tick.symbol) {
                    row.bid = Some(tick.bid_price);
                    row.ask = Some(tick.ask_price);
                }
            }
            MarketEvent::Depth(_) | MarketEvent::Bar(_) | MarketEvent::MarkPrice(_) => {}
        }
    }

    pub fn rows(&self) -> &BTreeMap<String, RowState> {
        &self.rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{Tick, TradeEvent, Venue};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn symbols() -> Vec<String> {
        vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
    }

    #[test]
    fn new_pre_populates_one_empty_row_per_symbol() {
        let state = AppState::new(&symbols());
        assert_eq!(state.rows().len(), 2);
        assert_eq!(state.rows().get("BTCUSDT"), Some(&RowState::default()));
        assert_eq!(state.rows().get("ETHUSDT"), Some(&RowState::default()));
    }

    #[test]
    fn trade_event_updates_last_trade_only() {
        let mut state = AppState::new(&symbols());
        state.apply_event(&MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        }));
        let row = state.rows().get("BTCUSDT").unwrap();
        assert_eq!(row.last_trade, Some(dec!(65000)));
        assert_eq!(row.bid, None);
        assert_eq!(row.ask, None);
    }

    #[test]
    fn tick_event_updates_bid_and_ask_only() {
        let mut state = AppState::new(&symbols());
        state.apply_event(&MarketEvent::Tick(Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(64990),
            bid_size: dec!(1),
            ask_price: dec!(65010),
            ask_size: dec!(1),
            timestamp: Utc::now(),
        }));
        let row = state.rows().get("BTCUSDT").unwrap();
        assert_eq!(row.bid, Some(dec!(64990)));
        assert_eq!(row.ask, Some(dec!(65010)));
        assert_eq!(row.last_trade, None);
    }

    #[test]
    fn event_for_an_unknown_symbol_is_ignored() {
        let mut state = AppState::new(&symbols());
        state.apply_event(&MarketEvent::Trade(TradeEvent {
            symbol: "DOGEUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(1),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        }));
        assert_eq!(state.rows().len(), 2);
        assert!(!state.rows().contains_key("DOGEUSDT"));
    }
}
