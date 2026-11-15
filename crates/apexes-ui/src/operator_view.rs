//! Rendering of the operator console (egui) and the `eframe` app around it.
//!
//! [`show`] is a pure function of an [`OperatorState`]: it draws, and returns
//! the commands the operator's clicks produced. All decisions (what needs
//! confirming, who may command) live in `crate::operator`.

use std::time::Duration;

use apexes_backtest::SessionCommand;
use apexes_engine::SessionMessage;
use chrono::Utc;
use egui::Color32;
use tokio::sync::{mpsc, watch};

use crate::client::ConnectionStatus;
use crate::operator::{OperatorAction, OperatorState, Requested};
use crate::theme::palette_of;

const REPAINT_INTERVAL: Duration = Duration::from_millis(250);
/// A snapshot older than this is flagged: the engine only pushes them as
/// market data arrives, so an old one is itself a warning sign.
const STALE_SNAPSHOT_SECS: i64 = 5;

fn dash(value: Option<impl ToString>) -> String {
    value.map_or_else(|| "-".to_string(), |v| v.to_string())
}

/// Draws the console; returns the commands to send to the engine.
pub fn show(ui: &mut egui::Ui, state: &mut OperatorState) -> Vec<SessionCommand> {
    let mut out = Vec::new();

    // The environment comes first and from the engine's own report: it is
    // the last thing an operator checks before trusting what follows.
    crate::evaluation_view::banner(
        ui,
        &crate::evaluation_view::environment_banner(
            state.snapshot.as_ref().and_then(|s| s.identity.as_ref()),
        ),
    );
    header(ui, state);
    ui.separator();
    controls(ui, state, &mut out);
    confirmation(ui, state, &mut out);
    if let Some(notice) = state.notice.clone() {
        ui.colored_label(palette_of(ui.ctx()).warn, notice);
    }
    ui.separator();

    egui::ScrollArea::vertical().show(ui, |ui| {
        summary(ui, state);
        if let Some(snapshot) = &state.snapshot {
            crate::evaluation_view::show_evaluation(ui, snapshot);
            ui.separator();
        }
        positions(ui, state);
        open_orders(ui, state);
        balances(ui, state);
        params(ui, state, &mut out);
    });
    out
}

fn header(ui: &mut egui::Ui, state: &OperatorState) {
    ui.horizontal(|ui| {
        let (text, color) = match state.connection {
            ConnectionStatus::Connected => ("Connected", palette_of(ui.ctx()).good),
            ConnectionStatus::Reconnecting => ("Reconnecting...", palette_of(ui.ctx()).warn),
            ConnectionStatus::Disconnected => ("Disconnected", palette_of(ui.ctx()).danger),
        };
        crate::theme::status_dot(ui, color);
        ui.label(text);
        if let Some(status) = &state.status {
            ui.separator();
            ui.label(format!("{} ({})", status.actor, status.role));
        }
    });

    // The one thing an operator must never have to hunt for.
    if !state.connected() {
        banner(
            ui,
            palette_of(ui.ctx()).warn,
            "NO CONNECTION - state below may be out of date",
        );
    } else if state.halted() {
        let reason = state
            .status
            .as_ref()
            .and_then(|s| s.halt_reason.clone())
            .unwrap_or_default();
        let flatten = state.status.as_ref().is_some_and(|s| s.flatten_requested);
        let suffix = if flatten {
            "  [flattening positions]"
        } else {
            ""
        };
        banner(
            ui,
            palette_of(ui.ctx()).danger,
            &format!("TRADING HALTED: {reason}{suffix}"),
        );
    } else if state.paused() {
        banner(
            ui,
            palette_of(ui.ctx()).warn,
            "STRATEGY PAUSED (orders still live)",
        );
    } else {
        banner(ui, palette_of(ui.ctx()).good, "TRADING");
    }

    if let Some(snapshot) = &state.snapshot {
        let age = (Utc::now() - snapshot.timestamp).num_seconds().max(0);
        let text = format!("snapshot {age}s old");
        if age > STALE_SNAPSHOT_SECS {
            ui.colored_label(
                palette_of(ui.ctx()).warn,
                format!("{text} - no recent market data?"),
            );
        } else {
            ui.weak(text);
        }
    } else {
        ui.weak("waiting for the first snapshot...");
    }
}

fn banner(ui: &mut egui::Ui, color: Color32, text: &str) {
    let palette = palette_of(ui.ctx());
    crate::theme::banner(ui, &palette, color, text);
}

fn controls(ui: &mut egui::Ui, state: &mut OperatorState, out: &mut Vec<SessionCommand>) {
    let enabled = state.can_command();
    let halted = state.halted();
    let paused = state.paused();
    ui.horizontal_wrapped(|ui| {
        let mut act = |ui: &mut egui::Ui, label: &str, hint: &str, action: OperatorAction| {
            let button = egui::Button::new(label);
            let response = ui.add_enabled(enabled, button).on_disabled_hover_text(hint);
            if response.clicked() {
                handle(state, action, out);
            }
        };
        let hint = "needs a connection with an operator token";
        if halted {
            act(ui, "Resume trading...", hint, OperatorAction::ResumeTrading);
        } else {
            act(ui, "HALT TRADING", hint, OperatorAction::Halt);
        }
        act(
            ui,
            "Halt + flatten...",
            hint,
            OperatorAction::HaltAndFlatten,
        );
        if paused {
            act(ui, "Resume strategy", hint, OperatorAction::ResumeStrategy);
        } else {
            act(ui, "Pause strategy", hint, OperatorAction::PauseStrategy);
        }
        act(ui, "Stop session...", hint, OperatorAction::Stop);
    });
}

fn handle(state: &mut OperatorState, action: OperatorAction, out: &mut Vec<SessionCommand>) {
    if let Requested::Send(command) = state.request(action) {
        out.push(command);
    }
}

fn confirmation(ui: &mut egui::Ui, state: &mut OperatorState, out: &mut Vec<SessionCommand>) {
    let Some(pending) = state.pending.clone() else {
        return;
    };
    egui::Frame::group(ui.style()).show(ui, |ui| {
        ui.colored_label(palette_of(ui.ctx()).warn, pending.warning());
        ui.horizontal(|ui| {
            if ui.button("Confirm").clicked()
                && let Some(command) = state.confirm()
            {
                out.push(command);
            }
            if ui.button("Cancel").clicked() {
                state.cancel();
            }
        });
    });
}

fn summary(ui: &mut egui::Ui, state: &OperatorState) {
    let Some(s) = &state.snapshot else { return };
    let realized: rust_decimal::Decimal = s.realized_pnl.values().copied().sum();
    ui.horizontal(|ui| {
        ui.strong("Equity");
        ui.label(s.equity.round_dp(2).to_string());
        ui.separator();
        ui.strong("Realized PnL");
        ui.label(realized.round_dp(2).to_string());
    });
}

fn positions(ui: &mut egui::Ui, state: &OperatorState) {
    let Some(s) = &state.snapshot else { return };
    ui.add_space(6.0);
    ui.heading("Positions");
    if s.positions.is_empty() {
        ui.weak("flat");
        return;
    }
    let mut rows: Vec<_> = s.positions.values().collect();
    rows.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    egui::Grid::new("positions").striped(true).show(ui, |ui| {
        for h in [
            "Symbol",
            "Qty",
            "Entry",
            "Leverage",
            "Margin ratio",
            "Liq. price",
        ] {
            ui.strong(h);
        }
        ui.end_row();
        for p in rows {
            let margin = s.margin_status.get(&p.symbol);
            ui.label(&p.symbol);
            ui.label(p.quantity.to_string());
            ui.label(p.average_entry_price.to_string());
            ui.label(dash(p.leverage));
            ui.label(dash(margin.and_then(|m| m.margin_ratio)));
            ui.label(dash(margin.and_then(|m| m.liquidation_price)));
            ui.end_row();
        }
    });
}

fn open_orders(ui: &mut egui::Ui, state: &OperatorState) {
    let Some(s) = &state.snapshot else { return };
    ui.add_space(6.0);
    ui.heading(format!("Open orders ({})", s.open_orders.len()));
    if s.open_orders.is_empty() {
        ui.weak("none");
        return;
    }
    egui::Grid::new("open_orders").striped(true).show(ui, |ui| {
        for h in [
            "Symbol", "Side", "Type", "Qty", "Filled", "Price", "Status", "Age",
        ] {
            ui.strong(h);
        }
        ui.end_row();
        for o in &s.open_orders {
            let age = (Utc::now() - o.created_at).num_seconds().max(0);
            ui.label(&o.symbol);
            ui.label(&o.side);
            ui.label(&o.order_type);
            ui.label(o.quantity.to_string());
            ui.label(o.filled_quantity.to_string());
            ui.label(dash(o.price));
            ui.label(&o.status);
            ui.label(format!("{age}s"));
            ui.end_row();
        }
    });
}

fn balances(ui: &mut egui::Ui, state: &OperatorState) {
    let Some(s) = &state.snapshot else { return };
    ui.add_space(6.0);
    ui.heading("Balances");
    let mut rows: Vec<_> = s.balances.iter().filter(|(_, v)| !v.is_zero()).collect();
    rows.sort_by(|a, b| a.0.cmp(b.0));
    if rows.is_empty() {
        ui.weak("none");
        return;
    }
    egui::Grid::new("balances").striped(true).show(ui, |ui| {
        for (asset, amount) in rows {
            ui.label(asset);
            ui.label(amount.to_string());
            ui.end_row();
        }
    });
}

fn params(ui: &mut egui::Ui, state: &mut OperatorState, out: &mut Vec<SessionCommand>) {
    let Some(s) = &state.snapshot else { return };
    let mut names: Vec<(String, String)> = s
        .params
        .iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect();
    if names.is_empty() {
        return;
    }
    names.sort();
    ui.add_space(6.0);
    ui.heading("Strategy parameters");
    let can = state.can_command();
    let mut to_set: Option<String> = None;
    egui::Grid::new("params").striped(true).show(ui, |ui| {
        for (name, current) in &names {
            ui.label(name);
            ui.label(current);
            let edit = state.param_edits.entry(name.clone()).or_default();
            ui.add_enabled(can, egui::TextEdit::singleline(edit).desired_width(90.0));
            if ui
                .add_enabled(can, egui::Button::new("Set"))
                .on_disabled_hover_text("needs a connection with an operator token")
                .clicked()
            {
                to_set = Some(name.clone());
            }
            ui.end_row();
        }
    });
    // Empty boxes are not edits.
    state.param_edits.retain(|_, v| !v.is_empty());
    if let Some(name) = to_set
        && let Requested::Send(command) = state.request_set_param(&name)
    {
        out.push(command);
    }
}

/// The `eframe` app: owns the state and the channels to the client task.
pub struct OperatorApp {
    state: OperatorState,
    events: Option<mpsc::UnboundedReceiver<SessionMessage>>,
    status_rx: Option<watch::Receiver<ConnectionStatus>>,
    commands: Option<mpsc::UnboundedSender<SessionCommand>>,
    engine_url: String,
}

impl OperatorApp {
    pub fn new(
        engine_url: String,
        events: mpsc::UnboundedReceiver<SessionMessage>,
        status_rx: watch::Receiver<ConnectionStatus>,
        commands: mpsc::UnboundedSender<SessionCommand>,
    ) -> Self {
        Self {
            state: OperatorState::default(),
            events: Some(events),
            status_rx: Some(status_rx),
            commands: Some(commands),
            engine_url,
        }
    }

    /// For headless render tests: fixed state, no channels.
    pub fn new_for_test(state: OperatorState) -> Self {
        Self {
            state,
            events: None,
            status_rx: None,
            commands: None,
            engine_url: "test".to_string(),
        }
    }

    pub fn state(&self) -> &OperatorState {
        &self.state
    }

    /// Takes in whatever the client task has delivered. Called every frame
    /// for every open console, shown or not, so a background tab is never
    /// behind and its health is current.
    pub fn pump(&mut self) {
        if let Some(status_rx) = &mut self.status_rx {
            let current = *status_rx.borrow_and_update();
            if current != self.state.connection {
                self.state.set_connection(current);
            }
        }
        if let Some(events) = &mut self.events {
            while let Ok(message) = events.try_recv() {
                self.state.apply(message);
            }
        }
    }
}

impl OperatorApp {
    /// Draws the console and forwards the operator's commands. Used by the
    /// standalone binary (via `eframe::App`) and by the launcher shell.
    pub fn render(&mut self, ui: &mut egui::Ui) {
        self.pump();
        ui.ctx().request_repaint_after(REPAINT_INTERVAL);
        ui.horizontal(|ui| {
            ui.heading("Apexes operator console");
            ui.weak(&self.engine_url);
        });
        let commands = show(ui, &mut self.state);
        if let Some(tx) = &self.commands {
            for command in commands {
                let _ = tx.send(command);
            }
        }
    }
}

/// The standalone console (`--engine`): an [`OperatorApp`] in its own window,
/// with the theme chrome the launcher shell also has.
pub struct StandaloneConsole {
    app: OperatorApp,
    chrome: crate::chrome::Chrome,
}

impl StandaloneConsole {
    pub fn new(app: OperatorApp, chrome: crate::chrome::Chrome) -> Self {
        Self { app, chrome }
    }
}

impl eframe::App for StandaloneConsole {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.chrome.update(ui.ctx());
        crate::theme::page(ui, |ui| {
            ui.horizontal(|ui| {
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    self.chrome.selector(ui);
                });
            });
            self.app.render(ui);
        });
    }

    fn clear_color(&self, visuals: &egui::Visuals) -> [f32; 4] {
        crate::theme::clear_color(visuals)
    }
}

impl eframe::App for OperatorApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.render(ui);
    }
}
