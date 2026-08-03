//! `eframe::App` implementation: a live-updating table of
//! bid/ask/last-trade price for the hardcoded instrument list, plus a
//! connection-status indicator. Drains any pending `MarketEvent`s
//! from the client task's channel on every `ui()` call and folds them
//! into `app_state`, then requests a repaint shortly afterward so the
//! table keeps updating even with no user input (an `eframe::App`'s
//! `ui()` is otherwise only called in response to user interaction or
//! an explicit repaint request).

use crate::app_state::AppState;
use crate::client::ConnectionStatus;
use apexes_core::MarketEvent;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// How often the app requests a repaint while idle, so newly-arrived
/// `MarketEvent`s (which arrive on a background tokio task, not from
/// user interaction) get picked up and rendered promptly.
const REPAINT_INTERVAL: Duration = Duration::from_millis(200);

pub struct DashboardApp {
    app_state: AppState,
    connection_status: ConnectionStatus,
    event_rx: Option<mpsc::UnboundedReceiver<MarketEvent>>,
    status_rx: Option<watch::Receiver<ConnectionStatus>>,
}

impl DashboardApp {
    /// Production constructor: wires the app up to the real client
    /// task's channels.
    pub fn new(
        symbols: &[String],
        event_rx: mpsc::UnboundedReceiver<MarketEvent>,
        status_rx: watch::Receiver<ConnectionStatus>,
    ) -> Self {
        Self {
            app_state: AppState::new(symbols),
            connection_status: ConnectionStatus::default(),
            event_rx: Some(event_rx),
            status_rx: Some(status_rx),
        }
    }

    /// Test-only constructor: builds the app directly from an
    /// already-populated `AppState` and a fixed `ConnectionStatus`,
    /// with no channels -- used by
    /// `tests/ui_renders_without_panicking.rs` so the headless render
    /// test doesn't need a real client task running.
    pub fn new_for_test(app_state: AppState, connection_status: ConnectionStatus) -> Self {
        Self {
            app_state,
            connection_status,
            event_rx: None,
            status_rx: None,
        }
    }

    fn drain_updates(&mut self) {
        if let Some(event_rx) = self.event_rx.as_mut() {
            while let Ok(event) = event_rx.try_recv() {
                self.app_state.apply_event(&event);
            }
        }
        if let Some(status_rx) = self.status_rx.as_mut() {
            self.connection_status = *status_rx.borrow();
        }
    }
}

impl eframe::App for DashboardApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.drain_updates();
        ui.ctx().request_repaint_after(REPAINT_INTERVAL);

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
    }
}
