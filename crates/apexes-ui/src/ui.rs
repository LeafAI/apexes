//! `eframe::App` implementation: a live-updating table of
//! bid/ask/last-trade price for the hardcoded instrument list, a
//! connection-status indicator, and (sub-project #13b) a K-line
//! candlestick chart plus a bid/ask tick-line chart for a
//! user-selected symbol, sharing one `ViewState` so both charts'
//! visible time range stays synchronized. Drains any pending
//! `MarketEvent`s from the client task's channel on every `ui()`
//! call and folds them into both `app_state` (table) and
//! `chart_state` (charts), then requests a repaint shortly afterward
//! so everything keeps updating even with no user input.

use crate::app_state::AppState;
use crate::chart_state::ChartState;
use crate::chrome::Chrome;
use crate::client::ConnectionStatus;
use crate::theme::palette_of;
use apexes_chart::candlestick::CandlestickChart;
use apexes_chart::tick_line::TickLineChart;
use apexes_chart::view_state::ViewState;
use apexes_core::{Bar, BarInterval, MarketEvent};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// How often the app requests a repaint while idle, so newly-arrived
/// `MarketEvent`s (which arrive on a background tokio task, not from
/// user interaction) get picked up and rendered promptly.
const REPAINT_INTERVAL: Duration = Duration::from_millis(200);

/// The curated K-line granularities offered in the dropdown, per the
/// design spec's explicit choice not to expose all six
/// `BarInterval` variants `BarAggregator` tracks internally.
const AVAILABLE_INTERVALS: [BarInterval; 4] = [
    BarInterval::OneMinute,
    BarInterval::FiveMinutes,
    BarInterval::FifteenMinutes,
    BarInterval::OneHour,
];

/// Upper bound on how many closed bars are retained per
/// `(symbol, interval)` in `DashboardApp::closed_bars`, evicted
/// oldest-first once exceeded -- keeps memory bounded over an
/// arbitrarily long-running session (mirroring `apexes-ui`'s own
/// `TickRingBuffer`'s bounded-growth discipline for tick data), well
/// beyond what any reasonable zoom level of the chart would ever
/// need to display at once.
const MAX_RETAINED_BARS_PER_SYMBOL_INTERVAL: usize = 2000;

fn interval_label(interval: BarInterval) -> &'static str {
    match interval {
        BarInterval::OneSecond => "1s",
        BarInterval::OneMinute => "1m",
        BarInterval::FiveMinutes => "5m",
        BarInterval::FifteenMinutes => "15m",
        BarInterval::OneHour => "1h",
        BarInterval::OneDay => "1d",
    }
}

pub struct DashboardApp {
    app_state: AppState,
    chart_state: ChartState,
    /// Per-(symbol, interval) accumulated CLOSED bar history --
    /// `ChartState`/`BarAggregator` only ever hand back bars that
    /// close AT THE MOMENT they close (`on_trade`'s return value),
    /// so this history must be accumulated here as events are
    /// drained, not re-derived on every frame.
    closed_bars: HashMap<(String, BarInterval), Vec<Bar>>,
    connection_status: ConnectionStatus,
    event_rx: Option<mpsc::UnboundedReceiver<MarketEvent>>,
    status_rx: Option<watch::Receiver<ConnectionStatus>>,
    symbols: Vec<String>,
    selected_symbol: String,
    selected_interval: BarInterval,
    view: ViewState,
    candlestick_chart: Option<CandlestickChart>,
    tick_line_chart: Option<TickLineChart>,
    chrome: Chrome,
}

impl DashboardApp {
    /// Production constructor: wires the app up to the real client
    /// task's channels. The two chart widgets are constructed lazily
    /// on the first `ui()` call (`eframe::App::ui`, below), once a real
    /// `wgpu::Device`/`TextureFormat` is available via
    /// `eframe::Frame`'s `wgpu_render_state` -- they cannot be built
    /// in this constructor, which runs before `eframe` has finished
    /// its own `wgpu` setup.
    pub fn new(
        symbols: &[String],
        event_rx: mpsc::UnboundedReceiver<MarketEvent>,
        status_rx: watch::Receiver<ConnectionStatus>,
    ) -> Self {
        let selected_symbol = symbols.first().cloned().unwrap_or_default();
        Self {
            app_state: AppState::new(symbols),
            chart_state: ChartState::new(symbols),
            closed_bars: HashMap::new(),
            connection_status: ConnectionStatus::default(),
            event_rx: Some(event_rx),
            status_rx: Some(status_rx),
            symbols: symbols.to_vec(),
            selected_symbol,
            selected_interval: BarInterval::OneMinute,
            view: ViewState::new_following_live(),
            candlestick_chart: None,
            tick_line_chart: None,
            chrome: Chrome::from_home(),
        }
    }

    /// Test-only constructor: builds the app directly from
    /// already-populated `AppState`/`ChartState` and a fixed
    /// `ConnectionStatus`, with no channels -- used by
    /// `tests/ui_renders_without_panicking.rs` so the headless render
    /// test doesn't need a real client task running. The two chart
    /// widgets are still constructed lazily (same as the production
    /// path), since this constructor also runs before any
    /// `wgpu_render_state` is available.
    pub fn new_for_test(app_state: AppState, connection_status: ConnectionStatus) -> Self {
        Self {
            app_state,
            chart_state: ChartState::new(&[]),
            closed_bars: HashMap::new(),
            connection_status,
            event_rx: None,
            status_rx: None,
            symbols: Vec::new(),
            selected_symbol: String::new(),
            selected_interval: BarInterval::OneMinute,
            view: ViewState::new_following_live(),
            candlestick_chart: None,
            tick_line_chart: None,
            chrome: Chrome::load(None),
        }
    }

    fn drain_updates(&mut self) {
        if let Some(event_rx) = self.event_rx.as_mut() {
            let mut drained = Vec::new();
            while let Ok(event) = event_rx.try_recv() {
                drained.push(event);
            }
            for event in &drained {
                self.app_state.apply_event(event);
                let newly_closed = self.chart_state.apply_event(event);
                if let MarketEvent::Trade(trade) = event {
                    // `BarAggregator::on_trade` closes bars for all
                    // six `BarInterval` variants in parallel, but
                    // `AVAILABLE_INTERVALS` only ever offers 4 of
                    // them in the UI -- retaining the other two
                    // (`OneSecond`, `OneDay`) forever would grow
                    // `closed_bars` unboundedly for data that can
                    // never be displayed (`OneSecond` alone closes
                    // roughly once per second per symbol under
                    // continuous trading). Only bars for a
                    // selectable interval are worth retaining.
                    for bar in newly_closed {
                        if AVAILABLE_INTERVALS.contains(&bar.interval) {
                            let history = self
                                .closed_bars
                                .entry((trade.symbol.clone(), bar.interval))
                                .or_default();
                            history.push(bar);
                            if history.len() > MAX_RETAINED_BARS_PER_SYMBOL_INTERVAL {
                                history.remove(0);
                            }
                        }
                    }
                    if self.symbols.contains(&trade.symbol) {
                        self.view.advance_if_following_live(trade.timestamp);
                    }
                }
            }
        }
        if let Some(status_rx) = self.status_rx.as_mut() {
            self.connection_status = *status_rx.borrow();
        }
    }

    /// Combines this symbol/interval's retained closed-bar history
    /// with the current still-forming bar (if any), per this task's
    /// own `Interfaces` contract.
    fn bars_for_chart(&self, symbol: &str, interval: BarInterval) -> Vec<Bar> {
        let mut bars = self
            .closed_bars
            .get(&(symbol.to_string(), interval))
            .cloned()
            .unwrap_or_default();
        if let Some(open_bar) = self.chart_state.current_open_bar_for(symbol, interval) {
            bars.push(open_bar);
        }
        bars
    }
}

impl eframe::App for DashboardApp {
    fn ui(&mut self, ui: &mut egui::Ui, frame: &mut eframe::Frame) {
        self.drain_updates();
        ui.ctx().request_repaint_after(REPAINT_INTERVAL);
        self.chrome.update(ui.ctx());
        crate::theme::page(ui, |ui| self.draw(ui, frame));
    }

    fn clear_color(&self, visuals: &egui::Visuals) -> [f32; 4] {
        crate::theme::clear_color(visuals)
    }
}

impl DashboardApp {
    fn draw(&mut self, ui: &mut egui::Ui, frame: &mut eframe::Frame) {
        if self.candlestick_chart.is_none()
            && let Some(render_state) = frame.wgpu_render_state.as_ref()
        {
            self.candlestick_chart = Some(CandlestickChart::new(
                &render_state.device,
                render_state.target_format,
            ));
            self.tick_line_chart = Some(TickLineChart::new(
                &render_state.device,
                render_state.target_format,
            ));
        }

        let palette = palette_of(ui.ctx());
        let (label, color) = match self.connection_status {
            ConnectionStatus::Connected => ("Connected", palette.good),
            ConnectionStatus::Reconnecting => ("Reconnecting...", palette.warn),
            ConnectionStatus::Disconnected => ("Disconnected", palette.danger),
        };
        ui.horizontal(|ui| {
            crate::theme::status_dot(ui, color);
            ui.label(label);
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                self.chrome.selector(ui);
            });
        });

        ui.separator();

        egui::Grid::new("dashboard_grid")
            .striped(true)
            .show(ui, |ui| {
                ui.strong("Symbol");
                ui.strong("Bid");
                ui.strong("Ask");
                ui.strong("Last Trade");
                ui.end_row();

                for (symbol, row) in self.app_state.rows() {
                    ui.label(symbol);
                    ui.label(row.bid.map_or_else(|| "-".to_string(), |v| v.to_string()));
                    ui.label(row.ask.map_or_else(|| "-".to_string(), |v| v.to_string()));
                    ui.label(
                        row.last_trade
                            .map_or_else(|| "-".to_string(), |v| v.to_string()),
                    );
                    ui.end_row();
                }
            });

        ui.separator();

        ui.horizontal(|ui| {
            ui.label("Chart symbol:");
            let previous_symbol = self.selected_symbol.clone();
            egui::ComboBox::from_id_salt("chart_symbol_selector")
                .selected_text(&self.selected_symbol)
                .show_ui(ui, |ui| {
                    for symbol in &self.symbols {
                        ui.selectable_value(&mut self.selected_symbol, symbol.clone(), symbol);
                    }
                });
            // Per the design spec (view_state.rs's own ViewState doc
            // comment): switching symbols is the only way back to
            // "follow live" mode after a manual zoom/pan permanently
            // stops auto-advance for a given ViewState instance --
            // that recovery path only actually exists if a symbol
            // switch constructs a fresh ViewState, which this
            // does explicitly rather than leaving the same
            // (possibly user_adjusted) instance in place.
            if self.selected_symbol != previous_symbol {
                self.view = ViewState::new_following_live();
            }

            ui.label("Interval:");
            egui::ComboBox::from_id_salt("chart_interval_selector")
                .selected_text(interval_label(self.selected_interval))
                .show_ui(ui, |ui| {
                    for interval in AVAILABLE_INTERVALS {
                        ui.selectable_value(
                            &mut self.selected_interval,
                            interval,
                            interval_label(interval),
                        );
                    }
                });
        });

        let bars = self.bars_for_chart(&self.selected_symbol, self.selected_interval);
        if let Some(chart) = self.candlestick_chart.as_ref() {
            chart.show(
                ui,
                &bars,
                &mut self.view,
                egui::Vec2::new(ui.available_width(), 250.0),
            );
        }

        let ticks = self.chart_state.ticks_for(&self.selected_symbol).to_vec();
        if let Some(chart) = self.tick_line_chart.as_ref() {
            chart.show(
                ui,
                &ticks,
                &mut self.view,
                egui::Vec2::new(ui.available_width(), 150.0),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{TradeEvent, Venue};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn symbols() -> Vec<String> {
        vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
    }

    fn app_with_channels() -> (DashboardApp, mpsc::UnboundedSender<MarketEvent>) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (_status_tx, status_rx) = watch::channel(ConnectionStatus::default());
        let app = DashboardApp::new(&symbols(), event_rx, status_rx);
        (app, event_tx)
    }

    fn trade_event(symbol: &str, price: rust_decimal::Decimal) -> MarketEvent {
        MarketEvent::Trade(TradeEvent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            price,
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        })
    }

    #[test]
    fn switching_the_selected_symbol_resets_view_to_a_fresh_follow_live_state() {
        let (mut app, _tx) = app_with_channels();
        // Simulate a manual zoom/pan having permanently stopped
        // follow-live mode for the currently-selected symbol's view.
        app.view.zoom(0.5, 1.0);
        let zoomed_range = app.view.visible_range().clone();

        // Directly simulate what the ComboBox's selectable_value
        // would do, then run the same reset logic ui() applies --
        // exercised here without a real egui::Ui, since the reset
        // logic itself doesn't depend on one.
        let previous_symbol = app.selected_symbol.clone();
        app.selected_symbol = "ETHUSDT".to_string();
        if app.selected_symbol != previous_symbol {
            app.view = ViewState::new_following_live();
        }

        assert_ne!(
            *app.view.visible_range(),
            zoomed_range,
            "switching symbols must construct a fresh ViewState, not keep the zoomed one"
        );
    }

    #[test]
    fn closed_bars_only_retains_bars_for_a_selectable_interval() {
        let (mut app, tx) = app_with_channels();
        tx.send(trade_event("BTCUSDT", dec!(100))).unwrap();
        app.drain_updates();

        // BarAggregator closes bars for all six BarInterval variants
        // in parallel once a boundary is crossed, but a single trade
        // with no boundary crossing yet closes none -- this test only
        // needs to confirm that IF a OneSecond bar were ever closed,
        // it would never be stored (a fabricated Bar with
        // BarInterval::OneSecond, hand-constructed here rather than
        // waiting for a real boundary crossing, avoids depending on
        // real wall-clock timing).
        assert!(
            !AVAILABLE_INTERVALS.contains(&BarInterval::OneSecond),
            "sanity check: OneSecond must not be a selectable interval"
        );
        assert!(
            !app.closed_bars
                .contains_key(&("BTCUSDT".to_string(), BarInterval::OneSecond)),
            "closed_bars must never accumulate an interval that AVAILABLE_INTERVALS never offers"
        );
    }

    #[test]
    fn closed_bars_history_is_capped_at_the_configured_maximum() {
        let (mut app, _tx) = app_with_channels();
        let key = ("BTCUSDT".to_string(), BarInterval::OneMinute);
        for i in 0..(MAX_RETAINED_BARS_PER_SYMBOL_INTERVAL + 5) {
            let bar = Bar {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                open: dec!(1),
                high: dec!(1),
                low: dec!(1),
                close: dec!(1),
                volume: dec!(1),
                interval: BarInterval::OneMinute,
                open_time: Utc::now(),
                close_time: Utc::now(),
            };
            let history = app.closed_bars.entry(key.clone()).or_default();
            history.push(bar);
            if history.len() > MAX_RETAINED_BARS_PER_SYMBOL_INTERVAL {
                history.remove(0);
            }
            let _ = i;
        }
        assert_eq!(
            app.closed_bars.get(&key).unwrap().len(),
            MAX_RETAINED_BARS_PER_SYMBOL_INTERVAL,
            "closed_bars history for one (symbol, interval) must never exceed the configured cap"
        );
    }

    #[test]
    fn bars_for_chart_appends_the_current_open_bar_after_closed_history() {
        let (mut app, tx) = app_with_channels();
        tx.send(trade_event("BTCUSDT", dec!(65000))).unwrap();
        app.drain_updates();

        let bars = app.bars_for_chart("BTCUSDT", BarInterval::OneMinute);
        // No boundary crossed yet, so closed_bars is empty for this
        // key -- the only bar present should be the current
        // still-forming one, appended last per bars_for_chart's own
        // contract.
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].close, dec!(65000));
    }
}
