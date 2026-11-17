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
use crate::number_format;
use crate::operator::{OperatorAction, OperatorState, Requested};
use crate::orders_view::{self, Placement};
use crate::theme::palette_of;

const REPAINT_INTERVAL: Duration = Duration::from_millis(250);
/// A snapshot older than this is flagged: the engine only pushes them as
/// market data arrives, so an old one is itself a warning sign.
const STALE_SNAPSHOT_SECS: i64 = 5;

fn dash(value: Option<impl ToString>) -> String {
    value.map_or_else(|| "-".to_string(), |v| v.to_string())
}

/// Draws the console; returns the commands to send to the engine.
///
/// Top to bottom: the environment banner, one status line with the controls,
/// the key figures, the equity chart, then cards that sit side by side when
/// the window is wide enough and stack when it is not. The recent-orders panel
/// takes the whitespace on the right (or goes under the cards when narrow).
pub fn show(ui: &mut egui::Ui, state: &mut OperatorState) -> Vec<SessionCommand> {
    let mut out = Vec::new();
    let palette = palette_of(ui.ctx());

    // The environment comes first and from the engine's own report: it is
    // the last thing an operator checks before trusting what follows.
    crate::evaluation_view::banner(
        ui,
        &crate::evaluation_view::environment_banner(
            state.snapshot.as_ref().and_then(|s| s.identity.as_ref()),
        ),
    );
    ui.add_space(6.0);
    status_bar(ui, state, &mut out);
    confirmation(ui, state, &mut out);
    if let Some(notice) = state.notice.clone() {
        ui.colored_label(palette.warn, notice);
    }
    ui.add_space(4.0);

    // The recent-orders panel sits in the whitespace beside the cards when
    // the window is wide, and under them when it is not. Never a modal: HALT
    // must stay reachable, and this keeps it in view in both layouts.
    let placement = orders_view::placement(ui.available_width());
    if placement == Placement::Side {
        egui::Panel::right("recent_orders_panel")
            .resizable(true)
            // The cards carry their own borders, so a full-height rule beside
            // them only adds a line. egui still draws it while the edge is
            // hovered or dragged, which is when it tells you it is a handle.
            .show_separator_line(false)
            .default_size(orders_view::SIDE_PANEL_DEFAULT_WIDTH)
            .size_range(orders_view::SIDE_PANEL_MIN_WIDTH..=orders_view::SIDE_PANEL_MAX_WIDTH)
            .frame(egui::Frame::new().inner_margin(egui::Margin {
                left: 12,
                right: 0,
                top: 0,
                bottom: 0,
            }))
            .show(ui, |ui| {
                crate::widgets::card(ui, &palette, |ui| match &state.snapshot {
                    Some(snapshot) => orders_view::show(ui, snapshot, &mut state.show_all_orders),
                    None => {
                        ui.label(crate::widgets::caption(
                            &palette,
                            "waiting for the first snapshot...",
                        ));
                    }
                });
            });
    }

    egui::ScrollArea::vertical()
        .auto_shrink([false, false])
        .show(ui, |ui| {
            body(ui, state, placement, &mut out);
        });
    out
}

/// Everything under the status bar, in the main column.
fn body(
    ui: &mut egui::Ui,
    state: &mut OperatorState,
    placement: Placement,
    out: &mut Vec<SessionCommand>,
) {
    let palette = palette_of(ui.ctx());
    let Some(snapshot) = state.snapshot.clone() else {
        ui.label(crate::widgets::caption(
            &palette,
            "waiting for the first snapshot...",
        ));
        return;
    };
    let width = ui.available_width();
    let tiles = crate::evaluation_view::key_tiles(
        snapshot.equity,
        snapshot.equity_priced,
        snapshot.evaluation.as_ref(),
    );
    let layout = crate::console_layout::layout(width, tiles.len());

    key_figures(ui, &palette, &tiles, layout.tiles_per_row);
    ui.add_space(crate::widgets::GAP);
    crate::chart_view::chart_card(ui, &mut state.chart, &snapshot);
    ui.add_space(crate::widgets::GAP);

    // Two columns: what the session has done on the left (details, then what
    // it holds and has working), what constrains it on the right (limits, then
    // balances). Stacked in that same order when the window is narrow.
    two_up(
        ui,
        layout.split_cards,
        crate::console_layout::CARDS_LEFT_SHARE,
        |ui| {
            match &snapshot.evaluation {
                Some(evaluation) => crate::evaluation_view::details_card(ui, evaluation),
                None => no_evaluation(ui, &palette, snapshot.equity_priced),
            }
            ui.add_space(crate::widgets::GAP);
            crate::widgets::card(ui, &palette, |ui| {
                positions(ui, &snapshot);
                ui.add_space(10.0);
                open_orders(ui, &snapshot);
            });
        },
        |ui| {
            if let Some(evaluation) = &snapshot.evaluation {
                crate::evaluation_view::limits_card(ui, &evaluation.limit_usage);
                ui.add_space(crate::widgets::GAP);
            }
            crate::widgets::card(ui, &palette, |ui| balances(ui, &snapshot));
        },
    );

    if placement == Placement::Below {
        ui.add_space(crate::widgets::GAP);
        crate::widgets::card(ui, &palette, |ui| {
            orders_view::show(ui, &snapshot, &mut state.show_all_orders);
        });
    }
    ui.add_space(crate::widgets::GAP);
    crate::widgets::card(ui, &palette, |ui| {
        params(ui, state, layout.param_columns, out);
    });
    ui.add_space(crate::widgets::GAP);
}

/// Why the details card is empty: a session still waiting for a price of an
/// asset it holds is not an engine that lacks the feature.
pub fn no_evaluation_text(priced: bool) -> &'static str {
    if priced {
        "no evaluation data from this engine yet"
    } else {
        "waiting for a price of a held asset before measuring PnL"
    }
}

fn no_evaluation(ui: &mut egui::Ui, palette: &crate::theme::Palette, priced: bool) {
    crate::widgets::card(ui, palette, |ui| {
        ui.label(crate::widgets::caption(palette, no_evaluation_text(priced)));
    });
}

/// Two columns side by side (`split`) or stacked, so the narrow layout loses
/// nothing. The left one takes `left_share` of the row.
///
/// Each card is given an exact width and clipped to it: a `Ui` that is only
/// offered a width will take as much as its contents ask for, and a long row of
/// numbers made the left card wider than its column and pushed the right one
/// out under the orders panel.
fn two_up(
    ui: &mut egui::Ui,
    split: bool,
    left_share: f32,
    left: impl FnOnce(&mut egui::Ui),
    right: impl FnOnce(&mut egui::Ui),
) {
    if !split {
        left(ui);
        ui.add_space(crate::widgets::GAP);
        right(ui);
        return;
    }
    let (left_width, right_width) =
        crate::console_layout::two_columns(ui.available_width(), left_share);
    let top = ui.cursor().min;
    let gap = crate::widgets::GAP;
    let left_rect = egui::Rect::from_min_size(top, egui::vec2(left_width, 0.0));
    let right_rect = egui::Rect::from_min_size(
        egui::pos2(top.x + left_width + gap, top.y),
        egui::vec2(right_width, 0.0),
    );
    let mut left_ui = ui.new_child(egui::UiBuilder::new().max_rect(left_rect));
    left(&mut left_ui);
    let mut right_ui = ui.new_child(egui::UiBuilder::new().max_rect(right_rect));
    right(&mut right_ui);
    // The row is as tall as its taller card; reserve that in the parent.
    let bottom = left_ui.min_rect().max.y.max(right_ui.min_rect().max.y);
    ui.advance_cursor_after_rect(egui::Rect::from_min_max(
        top,
        egui::pos2(top.x + left_width + gap + right_width, bottom),
    ));
}

/// The key-figure tiles, wrapped into balanced rows.
fn key_figures(
    ui: &mut egui::Ui,
    palette: &crate::theme::Palette,
    tiles: &[crate::evaluation_view::Tile],
    per_row: usize,
) {
    let neutral = ui.visuals().text_color();
    let width = ui.available_width();
    let tile_width = crate::widgets::tile_width(width, per_row);
    for (row, chunk) in tiles.chunks(per_row.max(1)).enumerate() {
        if row > 0 {
            ui.add_space(crate::widgets::GAP);
        }
        ui.horizontal_top(|ui| {
            ui.spacing_mut().item_spacing.x = crate::widgets::GAP;
            for tile in chunk {
                let color = tile
                    .sign
                    .map(|sign| crate::widgets::sign_color(palette, sign, neutral));
                crate::widgets::kpi_tile(
                    ui,
                    palette,
                    tile_width,
                    tile.label,
                    &tile.value,
                    color,
                    tile.note.as_deref(),
                );
            }
        });
    }
}

/// One line: the connection, who is connected, how fresh the data is, and the
/// controls. HALT is red and always first, so it is never hunted for.
fn status_bar(ui: &mut egui::Ui, state: &mut OperatorState, out: &mut Vec<SessionCommand>) {
    let palette = palette_of(ui.ctx());
    ui.horizontal_wrapped(|ui| {
        let (text, color) = match state.connection {
            ConnectionStatus::Connected => ("Connected", palette.good),
            ConnectionStatus::Reconnecting => ("Reconnecting...", palette.warn),
            ConnectionStatus::Disconnected => ("Disconnected", palette.danger),
        };
        crate::theme::status_dot(ui, color);
        let connection = ui.label(text);
        if !state.engine_url.is_empty() {
            connection.on_hover_text(format!("engine: {}", state.engine_url));
        }
        if let Some(status) = &state.status {
            ui.label(crate::widgets::caption(
                &palette,
                &format!("{} ({})", status.actor, status.role),
            ));
        }
        ui.separator();
        run_state_pill(ui, state, &palette);
        freshness(ui, state, &palette);
        ui.separator();
        controls(ui, state, out);
    });

    // The one thing an operator must never have to hunt for.
    if !state.connected() {
        banner(
            ui,
            palette.warn,
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
            palette.danger,
            &format!("TRADING HALTED: {reason}{suffix}"),
        );
    }
}

/// What the session is doing, as a word on a tinted pill.
pub fn run_state(state: &OperatorState) -> (&'static str, RunTone) {
    if !state.connected() {
        ("OFFLINE", RunTone::Warn)
    } else if state.halted() {
        ("HALTED", RunTone::Bad)
    } else if state.paused() {
        ("PAUSED", RunTone::Warn)
    } else {
        ("TRADING", RunTone::Good)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunTone {
    Good,
    Warn,
    Bad,
}

fn run_state_pill(ui: &mut egui::Ui, state: &OperatorState, palette: &crate::theme::Palette) {
    let (word, tone) = run_state(state);
    let color = match tone {
        RunTone::Good => palette.good,
        RunTone::Warn => palette.warn,
        RunTone::Bad => palette.danger,
    };
    crate::widgets::pill(ui, word, palette.banner_fill(color), palette.on_banner);
    if word == "PAUSED" {
        ui.label(crate::widgets::caption(palette, "orders still live"));
    }
}

fn freshness(ui: &mut egui::Ui, state: &OperatorState, palette: &crate::theme::Palette) {
    match &state.snapshot {
        Some(snapshot) => {
            let age = (Utc::now() - snapshot.timestamp).num_seconds().max(0);
            if age > STALE_SNAPSHOT_SECS {
                ui.colored_label(
                    palette.warn,
                    format!("snapshot {age}s old - no recent market data?"),
                );
            } else {
                ui.label(crate::widgets::caption(
                    palette,
                    &format!("snapshot {age}s old"),
                ));
            }
        }
        None => {
            ui.label(crate::widgets::caption(
                palette,
                "waiting for the first snapshot...",
            ));
        }
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
    let palette = palette_of(ui.ctx());
    let mut act =
        |ui: &mut egui::Ui, label: &str, hint: &str, action: OperatorAction, urgent: bool| {
            let mut text = egui::RichText::new(label);
            let mut button = egui::Button::new(text.clone());
            if urgent {
                // HALT is the one control that is red: it is the one an operator
                // reaches for in a hurry.
                text = text.strong().color(palette.on_banner);
                button = egui::Button::new(text).fill(palette.banner_fill(palette.danger));
            }
            let response = ui.add_enabled(enabled, button).on_disabled_hover_text(hint);
            if response.clicked() {
                handle(state, action, out);
            }
        };
    let hint = "needs a connection with an operator token";
    if halted {
        act(
            ui,
            "Resume trading...",
            hint,
            OperatorAction::ResumeTrading,
            false,
        );
    } else {
        act(ui, "HALT TRADING", hint, OperatorAction::Halt, true);
    }
    act(
        ui,
        "Halt + flatten...",
        hint,
        OperatorAction::HaltAndFlatten,
        false,
    );
    if paused {
        act(
            ui,
            "Resume strategy",
            hint,
            OperatorAction::ResumeStrategy,
            false,
        );
    } else {
        act(
            ui,
            "Pause strategy",
            hint,
            OperatorAction::PauseStrategy,
            false,
        );
    }
    act(ui, "Stop session...", hint, OperatorAction::Stop, false);
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

/// Positions that still hold something, by symbol.
pub fn open_positions(
    snapshot: &apexes_backtest::SessionSnapshot,
) -> Vec<&apexes_backtest::Position> {
    let mut rows: Vec<_> = snapshot
        .positions
        .values()
        .filter(|p| !p.quantity.is_zero())
        .collect();
    rows.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    rows
}

fn positions(ui: &mut egui::Ui, s: &apexes_backtest::SessionSnapshot) {
    let palette = palette_of(ui.ctx());
    crate::widgets::section_title(ui, "Positions");
    // A position closed back to zero stays in the ledger with its old entry
    // price; showing it as a row reads as still holding something.
    let rows = open_positions(s);
    if rows.is_empty() {
        ui.label(crate::widgets::caption(&palette, "flat"));
        return;
    }
    egui::Grid::new("positions")
        .striped(true)
        .spacing([18.0, 5.0])
        .show(ui, |ui| {
            for h in [
                "Symbol",
                "Qty",
                "Entry",
                "Leverage",
                "Margin ratio",
                "Liq. price",
            ] {
                ui.label(crate::widgets::caption(&palette, h));
            }
            ui.end_row();
            for p in rows {
                let margin = s.margin_status.get(&p.symbol);
                ui.label(&p.symbol);
                ui.label(egui::RichText::new(number_format::amount(p.quantity)).monospace());
                ui.label(
                    egui::RichText::new(number_format::price(p.average_entry_price)).monospace(),
                );
                ui.label(dash(p.leverage));
                ui.label(dash(margin.and_then(|m| m.margin_ratio)));
                ui.label(dash(margin.and_then(|m| m.liquidation_price)));
                ui.end_row();
            }
        });
}

fn open_orders(ui: &mut egui::Ui, s: &apexes_backtest::SessionSnapshot) {
    let palette = palette_of(ui.ctx());
    crate::widgets::section_title(ui, &format!("Open orders ({})", s.open_orders.len()));
    if s.open_orders.is_empty() {
        ui.label(crate::widgets::caption(&palette, "none working"));
        return;
    }
    egui::Grid::new("open_orders")
        .striped(true)
        .spacing([18.0, 5.0])
        .show(ui, |ui| {
            for h in [
                "Symbol", "Side", "Type", "Qty", "Filled", "Price", "Status", "Age",
            ] {
                ui.label(crate::widgets::caption(&palette, h));
            }
            ui.end_row();
            for o in &s.open_orders {
                let age = (Utc::now() - o.created_at).num_seconds().max(0);
                ui.label(&o.symbol);
                ui.label(&o.side);
                ui.label(&o.order_type);
                ui.label(egui::RichText::new(number_format::amount(o.quantity)).monospace());
                ui.label(egui::RichText::new(number_format::amount(o.filled_quantity)).monospace());
                ui.label(
                    o.price
                        .map_or_else(|| "-".to_string(), number_format::price),
                );
                ui.label(&o.status);
                ui.label(format!("{age}s"));
                ui.end_row();
            }
        });
}

fn balances(ui: &mut egui::Ui, s: &apexes_backtest::SessionSnapshot) {
    let palette = palette_of(ui.ctx());
    crate::widgets::section_title(ui, "Balances");
    let mut rows: Vec<_> = s.balances.iter().filter(|(_, v)| !v.is_zero()).collect();
    rows.sort_by(|a, b| a.0.cmp(b.0));
    if rows.is_empty() {
        ui.label(crate::widgets::caption(&palette, "none"));
        return;
    }
    let width = ui.available_width();
    for (asset, amount) in rows {
        crate::widgets::pair_row(
            ui,
            &palette,
            width,
            asset,
            &number_format::amount(*amount),
            crate::widgets::LabelStyle::Normal,
        );
    }
}

/// The strategy's parameters, in as many columns as the width allows. Each is
/// its name, its current value (what the strategy is using now), a box to type
/// a new one and a Set button.
fn params(
    ui: &mut egui::Ui,
    state: &mut OperatorState,
    columns: usize,
    out: &mut Vec<SessionCommand>,
) {
    let palette = palette_of(ui.ctx());
    let Some(s) = &state.snapshot else { return };
    let mut names: Vec<(String, String)> = s
        .params
        .iter()
        .map(|(k, v)| (k.clone(), number_format::amount(*v)))
        .collect();
    if names.is_empty() {
        return;
    }
    names.sort();
    crate::widgets::section_title(ui, "Strategy parameters");
    ui.label(crate::widgets::caption(
        &palette,
        "Type a new value and press Set: it applies from the next market event, without a restart.",
    ));
    ui.add_space(6.0);
    let can = state.can_command();
    let mut to_set: Option<String> = None;
    let columns = columns.max(1);
    let per_column = names.len().div_ceil(columns);
    let column_width =
        ((ui.available_width() - 24.0 * (columns as f32 - 1.0)) / columns as f32).max(0.0);
    ui.horizontal_top(|ui| {
        ui.spacing_mut().item_spacing.x = 24.0;
        for (index, chunk) in names.chunks(per_column).enumerate() {
            ui.allocate_ui(egui::vec2(column_width, 0.0), |ui| {
                egui::Grid::new(("params", index))
                    .striped(true)
                    .spacing([12.0, 6.0])
                    .min_col_width(0.0)
                    .show(ui, |ui| {
                        for (name, current) in chunk {
                            ui.label(name);
                            ui.label(egui::RichText::new(current).strong().monospace());
                            // The box and its button are one control: a box
                            // with an outline, then Set touching it.
                            ui.horizontal(|ui| {
                                ui.spacing_mut().item_spacing.x = 4.0;
                                let edit = state.param_edits.entry(name.clone()).or_default();
                                ui.add_enabled(
                                    can,
                                    egui::TextEdit::singleline(edit)
                                        .hint_text(egui::RichText::new("new value").weak())
                                        .desired_width(88.0)
                                        .background_color(ui.visuals().extreme_bg_color),
                                );
                                if ui
                                    .add_enabled(can, egui::Button::new("Set"))
                                    .on_disabled_hover_text(
                                        "needs a connection with an operator token",
                                    )
                                    .clicked()
                                {
                                    to_set = Some(name.clone());
                                }
                            });
                            ui.end_row();
                        }
                    });
            });
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
        if self.state.engine_url != self.engine_url {
            self.state.engine_url.clone_from(&self.engine_url);
        }
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
