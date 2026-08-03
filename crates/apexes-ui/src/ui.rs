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
use crate::client::ConnectionStatus;
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
                    for bar in newly_closed {
                        self.closed_bars
                            .entry((trade.symbol.clone(), bar.interval))
                            .or_default()
                            .push(bar);
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

        let (label, color) = match self.connection_status {
            ConnectionStatus::Connected => ("Connected", egui::Color32::GREEN),
            ConnectionStatus::Reconnecting => ("Reconnecting...", egui::Color32::YELLOW),
            ConnectionStatus::Disconnected => ("Disconnected", egui::Color32::RED),
        };
        ui.horizontal(|ui| {
            ui.colored_label(color, "\u{25CF}");
            ui.label(label);
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
            egui::ComboBox::from_id_salt("chart_symbol_selector")
                .selected_text(&self.selected_symbol)
                .show_ui(ui, |ui| {
                    for symbol in &self.symbols {
                        ui.selectable_value(&mut self.selected_symbol, symbol.clone(), symbol);
                    }
                });

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
