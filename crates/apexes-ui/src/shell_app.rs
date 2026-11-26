//! The operator application: the launcher and one console tab per open
//! session, in one window.
//!
//! Window management rules:
//! - Opening a session never replaces another: each console keeps its own
//!   connection and state, so switching tabs loses nothing.
//! - Every open console is read every frame, shown or not, so a background
//!   tab is current and its health dot is truthful.
//! - A launch opens its console and brings it forward.
//! - Cmd/Ctrl+1..9, [ ], W move between and close tabs; Cmd/Ctrl+Shift+L
//!   cycles the theme.

use std::time::{Duration, Instant};

use apexes_supervisor::{LaunchRequest, SessionInfo, Supervisor};
use tokio::sync::{mpsc, watch};

use crate::chrome::{self, Chrome};
use crate::client::ConnectionStatus;
use crate::operator_client::{OperatorClientConfig, run};
use crate::operator_view::OperatorApp;
use crate::shell::{
    Effect, JobKind, ShellState, Worker, WorkerJob, WorkerResult, read_sessions, refresh_due,
};
use crate::tabs::{TabHealth, Tabs, View};
use crate::theme::palette_of;

const REPAINT_EVERY: Duration = Duration::from_millis(250);

/// One open session: its console and the task that talks to its engine.
struct Console {
    id: String,
    app: OperatorApp,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Console {
    /// Closing the tab ends its connection. The engine itself keeps running:
    /// it is a separate process that outlives the window.
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct ShellApp {
    supervisor: Supervisor,
    worker: Worker,
    runtime: tokio::runtime::Handle,
    shell: ShellState,
    consoles: Vec<Console>,
    tabs: Tabs,
    chrome: Chrome,
    last_refresh: Option<Instant>,
}

impl ShellApp {
    pub fn new(supervisor: Supervisor, runtime: tokio::runtime::Handle) -> Self {
        Self::with_chrome(supervisor, runtime, Chrome::from_home())
    }

    pub fn with_chrome(
        supervisor: Supervisor,
        runtime: tokio::runtime::Handle,
        chrome: Chrome,
    ) -> Self {
        let worker = Worker::spawn(supervisor.clone());
        let mut shell = ShellState::default();
        shell.launcher.refresh_strategies();
        Self {
            supervisor,
            worker,
            runtime,
            shell,
            consoles: Vec::new(),
            tabs: Tabs::default(),
            chrome,
            last_refresh: None,
        }
    }

    /// Pre-fills the launcher's strategy box (the `--strategy` option). Only a
    /// starting value: nothing launches until the operator presses Launch.
    /// Blank input is ignored, so the default stays.
    pub fn with_strategy_path(mut self, path: Option<String>) -> Self {
        if let Some(path) = path.filter(|p| !p.trim().is_empty()) {
            self.shell.launcher.form.strategy_path = path;
        }
        self
    }

    /// What the launcher would start with, for tests.
    pub fn strategy_path(&self) -> &str {
        &self.shell.launcher.form.strategy_path
    }

    pub fn tabs(&self) -> &Tabs {
        &self.tabs
    }

    /// Opens a console for `info` and brings it forward. A session that is
    /// already open is only brought forward: its connection is kept.
    pub fn open_console(&mut self, info: &SessionInfo) {
        let id = info.id.to_string();
        if !self.tabs.is_open(&id) {
            self.consoles.push(self.spawn_console(&id, info));
        }
        self.tabs.open(&id);
    }

    /// Opens a tab for a console with a fixed state and no engine behind it,
    /// so a test can render the whole window (tab bar, page and console)
    /// exactly as an operator sees it.
    pub fn open_console_with_state(&mut self, id: &str, app: OperatorApp) {
        if !self.tabs.is_open(id) {
            self.consoles.push(Console {
                id: id.to_string(),
                app,
                task: self.runtime.spawn(async {}),
            });
        }
        self.tabs.open(id);
    }

    fn spawn_console(&self, id: &str, info: &SessionInfo) -> Console {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (status_tx, status_rx) = watch::channel(ConnectionStatus::default());
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let task = self.runtime.spawn(run(
            OperatorClientConfig {
                url: info.control_url.clone(),
                token: info.token.clone(),
            },
            event_tx,
            status_tx,
            command_rx,
        ));
        Console {
            id: id.to_string(),
            app: OperatorApp::new(info.control_url.clone(), event_rx, status_rx, command_tx),
            task,
        }
    }

    fn close_console(&mut self, id: &str) {
        self.tabs.close(id);
        self.consoles.retain(|c| c.id != id);
    }

    fn refresh(&mut self) {
        let (sessions, missing) = read_sessions(&self.supervisor, &self.shell.launcher);
        self.shell.apply_sessions(sessions, missing);
        self.last_refresh = Some(Instant::now());
    }

    fn drain_worker(&mut self) {
        let results = self.worker.try_results();
        let changed = !results.is_empty();
        for result in results {
            if let Some(launched) = self.shell.apply_result(result) {
                self.open_console(&launched);
            }
        }
        if changed {
            self.refresh();
        }
    }

    fn perform(&mut self, effect: Effect, request: Option<LaunchRequest>) {
        match effect {
            Effect::None => {}
            Effect::RefreshSessions => self.refresh(),
            Effect::Connect(info) => self.open_console(&info),
            Effect::Job(JobKind::Launch(_)) => {
                if let Some(request) = request
                    && !self.worker.submit(WorkerJob::Launch(Box::new(request)))
                {
                    self.shell.apply_result(WorkerResult::LaunchFailed(
                        "the supervisor worker has stopped; restart the app".to_string(),
                    ));
                }
            }
            Effect::Job(JobKind::Stop(session_id)) => {
                self.worker.submit(WorkerJob::Stop { session_id });
            }
            Effect::Job(JobKind::Forget(session_id)) => {
                self.worker.submit(WorkerJob::Forget { session_id });
            }
        }
    }

    fn tab_bar(&mut self, ui: &mut egui::Ui) {
        let palette = palette_of(ui.ctx());
        let mut focus = None;
        let mut close = None;
        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 4.0;
            let on_launcher = self.tabs.active() == &View::Launcher;
            if tab(ui, &palette, on_launcher, None, "Launcher").clicked() {
                focus = Some(View::Launcher);
            }
            for console in &self.consoles {
                let health = TabHealth::of(console.app.state());
                let (color, suffix) = match health {
                    TabHealth::Ok => (palette.good, ""),
                    TabHealth::Attention => (palette.warn, "  offline"),
                    TabHealth::Halted => (palette.danger, "  HALTED"),
                };
                let active = self.tabs.active() == &View::Console(console.id.clone());
                let response = tab(
                    ui,
                    &palette,
                    active,
                    Some(color),
                    &format!("{}{suffix}", console.id),
                )
                .on_hover_text("Cmd/Ctrl+W closes this tab; the session keeps running");
                if response.clicked() {
                    focus = Some(View::Console(console.id.clone()));
                }
                // A close button that is a button, not a stray glyph.
                let closer = ui
                    .add(egui::Button::new(egui::RichText::new("\u{00D7}").size(15.0)).frame(false))
                    .on_hover_text("Close this tab (the session keeps running)");
                if response.middle_clicked() || closer.clicked() {
                    close = Some(console.id.clone());
                }
                ui.add_space(8.0);
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                self.chrome.selector(ui);
            });
        });
        if let Some(view) = focus {
            self.tabs.focus(view);
        }
        if let Some(id) = close {
            self.close_console(&id);
        }
    }

    fn body(&mut self, ui: &mut egui::Ui) {
        match self.tabs.active().clone() {
            View::Launcher => {
                let pending = self.shell.pending_stop.clone();
                let actions =
                    crate::launcher_view::show(ui, &mut self.shell.launcher, pending.as_deref());
                for action in actions {
                    let (effect, request) = self.shell.handle(action);
                    self.perform(effect, request);
                }
            }
            View::Console(id) => match self.consoles.iter_mut().find(|c| c.id == id) {
                Some(console) => console.app.render(ui),
                None => {
                    ui.weak("This session is no longer open.");
                }
            },
        }
    }
}

/// One tab: a rounded button, with the active one filled and underlined so the
/// current view is obvious at a glance (the old selectable label was a faint
/// highlight). A health dot, when given, leads it.
fn tab(
    ui: &mut egui::Ui,
    palette: &crate::theme::Palette,
    active: bool,
    dot: Option<egui::Color32>,
    text: &str,
) -> egui::Response {
    let fill = if active {
        palette.card_fill
    } else {
        egui::Color32::TRANSPARENT
    };
    let stroke = if active {
        egui::Stroke::new(1.0, palette.card_stroke)
    } else {
        egui::Stroke::NONE
    };
    let inner = egui::Frame::new()
        .fill(fill)
        .stroke(stroke)
        .corner_radius(6)
        .inner_margin(egui::Margin::symmetric(10, 4))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                if let Some(color) = dot {
                    crate::theme::status_dot(ui, color);
                }
                let label = egui::RichText::new(text);
                ui.label(if active { label.strong() } else { label });
            });
        });
    let response = inner.response.interact(egui::Sense::click());
    if active {
        // The accent underline under the current tab.
        let rect = inner.response.rect;
        ui.painter().line_segment(
            [
                egui::pos2(rect.min.x + 6.0, rect.max.y),
                egui::pos2(rect.max.x - 6.0, rect.max.y),
            ],
            egui::Stroke::new(2.0, palette.info),
        );
    }
    response
}

impl eframe::App for ShellApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        ctx.request_repaint_after(REPAINT_EVERY);
        self.chrome.update(&ctx);
        if let Some(command) = chrome::tab_command(&ctx) {
            if command == crate::tabs::TabCommand::CloseActive
                && let View::Console(id) = self.tabs.active().clone()
            {
                self.close_console(&id);
            } else {
                self.tabs.apply(command);
            }
        }

        self.drain_worker();
        if refresh_due(self.last_refresh) {
            self.refresh();
        }
        for console in &mut self.consoles {
            console.app.pump();
        }

        crate::theme::page(ui, |ui| {
            self.tab_bar(ui);
            ui.separator();
            self.body(ui);
        });
    }

    fn clear_color(&self, visuals: &egui::Visuals) -> [f32; 4] {
        crate::theme::clear_color(visuals)
    }
}
