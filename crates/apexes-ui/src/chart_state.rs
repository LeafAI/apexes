//! Per-symbol chart data accumulation for the K-line/tick-line
//! widgets (sub-project #13b): one `BarAggregator` and one
//! time-windowed tick ring buffer per hardcoded symbol, fed by every
//! incoming `MarketEvent`. Pure logic -- no `egui`/`wgpu` dependency,
//! unit-testable directly.

use apexes_chart::geometry::TickPoint;
use apexes_core::{Bar, BarInterval, MarketEvent, Venue};
use apexes_market_data::bar_aggregator::BarAggregator;
use chrono::Duration;
use std::collections::HashMap;

/// How much tick history each symbol's ring buffer retains, per the
/// design spec's explicit choice (retain by time window, not by a
/// fixed item count).
const TICK_RETENTION: Duration = Duration::hours(1);

pub struct TickRingBuffer {
    points: Vec<TickPoint>,
}

impl Default for TickRingBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl TickRingBuffer {
    pub fn new() -> Self {
        Self { points: Vec::new() }
    }

    /// Appends `point`, then evicts every point older than
    /// `TICK_RETENTION` relative to `point`'s own timestamp (not
    /// relative to "now") -- eviction runs on every push, never on a
    /// timer, so the buffer can never grow unbounded regardless of
    /// tick rate.
    pub fn push(&mut self, point: TickPoint) {
        let cutoff = point.timestamp - TICK_RETENTION;
        self.points.push(point);
        self.points.retain(|p| p.timestamp >= cutoff);
    }

    pub fn points(&self) -> &[TickPoint] {
        &self.points
    }
}

pub struct ChartState {
    aggregators: HashMap<String, BarAggregator>,
    tick_buffers: HashMap<String, TickRingBuffer>,
}

impl ChartState {
    pub fn new(symbols: &[String]) -> Self {
        let mut aggregators = HashMap::new();
        let mut tick_buffers = HashMap::new();
        for symbol in symbols {
            aggregators.insert(
                symbol.clone(),
                BarAggregator::new(symbol.clone(), Venue::Binance),
            );
            tick_buffers.insert(symbol.clone(), TickRingBuffer::new());
        }
        Self {
            aggregators,
            tick_buffers,
        }
    }

    /// Feeds `event` into the relevant symbol's `BarAggregator`
    /// (`Trade` events) or tick ring buffer (`Tick` events). Events
    /// for a symbol not in the pre-populated set are ignored, same
    /// convention as `app_state::AppState::apply_event`. Returns any
    /// bars that closed as a result of this event (empty for
    /// `Tick`/non-matching events) -- `BarAggregator`/`ChartState` do
    /// not retain closed-bar history themselves (`on_trade` only ever
    /// hands back bars AT THE MOMENT they close), so the caller
    /// (`DashboardApp`, Step 4 below) is responsible for accumulating
    /// this method's returned bars into its own history across calls.
    pub fn apply_event(&mut self, event: &MarketEvent) -> Vec<Bar> {
        match event {
            MarketEvent::Trade(trade) => {
                if let Some(aggregator) = self.aggregators.get_mut(&trade.symbol) {
                    return aggregator.on_trade(trade.price, trade.quantity, trade.timestamp);
                }
                Vec::new()
            }
            MarketEvent::Tick(tick) => {
                if let Some(buffer) = self.tick_buffers.get_mut(&tick.symbol) {
                    buffer.push(TickPoint {
                        timestamp: tick.timestamp,
                        bid: tick.bid_price,
                        ask: tick.ask_price,
                    });
                }
                Vec::new()
            }
            MarketEvent::Depth(_) | MarketEvent::Bar(_) | MarketEvent::MarkPrice(_) => Vec::new(),
        }
    }

    /// Returns `symbol`'s currently still-forming (not yet closed)
    /// bar for `interval`, if any -- the caller (`DashboardApp`) is
    /// responsible for prepending its own retained closed-bar history
    /// (accumulated from `apply_event`'s return values) before this
    /// method's result, matching this task's own `Interfaces`
    /// contract of "closed bars... plus its current open bar, if any,
    /// appended last".
    pub fn current_open_bar_for(&self, symbol: &str, interval: BarInterval) -> Option<Bar> {
        self.aggregators.get(symbol)?.current_open_bar(interval)
    }

    pub fn ticks_for(&self, symbol: &str) -> &[TickPoint] {
        self.tick_buffers
            .get(symbol)
            .map(|buffer| buffer.points())
            .unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{Tick, TradeEvent};
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    fn t(epoch_secs: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(epoch_secs, 0).single().unwrap()
    }

    fn symbols() -> Vec<String> {
        vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
    }

    #[test]
    fn tick_ring_buffer_evicts_points_older_than_the_retention_window() {
        let mut buffer = TickRingBuffer::new();
        buffer.push(TickPoint {
            timestamp: t(0),
            bid: dec!(100),
            ask: dec!(101),
        });
        buffer.push(TickPoint {
            timestamp: t(3600 + 1),
            bid: dec!(102),
            ask: dec!(103),
        }); // 1 second past the 1h retention window
        assert_eq!(buffer.points().len(), 1);
        assert_eq!(buffer.points()[0].bid, dec!(102));
    }

    #[test]
    fn tick_ring_buffer_keeps_points_within_the_retention_window() {
        let mut buffer = TickRingBuffer::new();
        buffer.push(TickPoint {
            timestamp: t(0),
            bid: dec!(100),
            ask: dec!(101),
        });
        buffer.push(TickPoint {
            timestamp: t(3000),
            bid: dec!(102),
            ask: dec!(103),
        }); // within 1h
        assert_eq!(buffer.points().len(), 2);
    }

    #[test]
    fn apply_event_trade_feeds_the_correct_symbols_aggregator() {
        let mut state = ChartState::new(&symbols());
        state.apply_event(&MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: t(0),
        }));
        let open_bar = state.current_open_bar_for("BTCUSDT", BarInterval::OneMinute);
        assert!(open_bar.is_some());
        assert_eq!(
            state.current_open_bar_for("ETHUSDT", BarInterval::OneMinute),
            None
        );
    }

    #[test]
    fn apply_event_tick_feeds_the_correct_symbols_ring_buffer() {
        let mut state = ChartState::new(&symbols());
        state.apply_event(&MarketEvent::Tick(Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(64990),
            bid_size: dec!(1),
            ask_price: dec!(65010),
            ask_size: dec!(1),
            timestamp: t(0),
        }));
        assert_eq!(state.ticks_for("BTCUSDT").len(), 1);
        assert_eq!(state.ticks_for("ETHUSDT").len(), 0);
    }

    #[test]
    fn apply_event_for_an_unknown_symbol_is_ignored() {
        let mut state = ChartState::new(&symbols());
        state.apply_event(&MarketEvent::Trade(TradeEvent {
            symbol: "DOGEUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(1),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: t(0),
        }));
        assert_eq!(
            state.current_open_bar_for("DOGEUSDT", BarInterval::OneMinute),
            None
        );
    }
}
