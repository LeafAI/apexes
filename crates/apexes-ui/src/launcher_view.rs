//! Drawing for the session launcher. [`show`] is a function of a
//! [`LauncherState`]: it draws and returns the actions the operator's clicks
//! produced; every decision lives in `crate::launcher`.

use apexes_core::binance_network::BinanceNetwork;
use apexes_supervisor::SessionInfo;
use egui::{Color32, RichText};

use crate::launcher::{LaunchStatus, LauncherState};
use crate::theme::{Palette, palette_of};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LauncherAction {
    Launch,
    Refresh,
    Connect {
        session_id: String,
    },
    /// First click on Stop: asks for confirmation.
    RequestStop {
        session_id: String,
    },
    ConfirmStop {
        session_id: String,
    },
    CancelStop,
    Forget {
        session_id: String,
    },
}

pub fn show(
    ui: &mut egui::Ui,
    state: &mut LauncherState,
    pending_stop: Option<&str>,
) -> Vec<LauncherAction> {
    let mut actions = Vec::new();
    ui.heading("Start a Binance Spot session");
    network_picker(ui, state);
    ui.add_space(4.0);
    form_fields(ui, state);
    ui.add_space(6.0);
    launch_button(ui, state, &mut actions);
    ui.separator();
    sessions(ui, state, pending_stop, &mut actions);
    actions
}

fn network_picker(ui: &mut egui::Ui, state: &mut LauncherState) {
    ui.horizontal(|ui| {
        ui.strong("Network");
        let mut network = state.form.network;
        ui.radio_value(
            &mut network,
            BinanceNetwork::Testnet,
            "Testnet (virtual funds)",
        );
        ui.radio_value(
            &mut network,
            BinanceNetwork::Mainnet,
            "Mainnet (REAL FUNDS)",
        );
        state.form.set_network(network);
    });
    match state.form.network {
        BinanceNetwork::Testnet => {
            ui.colored_label(
                palette_of(ui.ctx()).info,
                "Testnet: separate keys from testnet.binance.vision, virtual money, resets periodically.",
            );
        }
        BinanceNetwork::Mainnet => {
            let palette = palette_of(ui.ctx());
            crate::theme::banner(
                ui,
                &palette,
                palette.danger,
                "MAINNET: this trades real funds on your Binance account.",
            );
            ui.horizontal(|ui| {
                ui.label("Type MAINNET to confirm");
                ui.text_edit_singleline(&mut state.form.mainnet_confirmation);
            });
        }
    }
}

fn form_fields(ui: &mut egui::Ui, state: &mut LauncherState) {
    egui::Grid::new("launcher_form")
        .num_columns(2)
        .spacing([16.0, 6.0])
        .show(ui, |ui| {
            let form = &mut state.form;
            ui.label("Symbols");
            ui.text_edit_singleline(&mut form.symbols);
            ui.end_row();

            ui.label("Strategy file");
            ui.text_edit_singleline(&mut form.strategy_path);
            ui.end_row();

            ui.label("Credentials name");
            ui.text_edit_singleline(&mut form.credentials_ref);
            ui.end_row();

            ui.label("Max order notional");
            ui.text_edit_singleline(&mut form.max_order_notional);
            ui.end_row();
            ui.label("Max order quantity");
            ui.text_edit_singleline(&mut form.max_order_quantity);
            ui.end_row();
            ui.label("Max position per symbol");
            ui.text_edit_singleline(&mut form.max_position_per_symbol);
            ui.end_row();
            ui.label("Max trades per day");
            ui.text_edit_singleline(&mut form.max_daily_trade_count);
            ui.end_row();
            ui.label("Halt if no data for (s)");
            ui.text_edit_singleline(&mut form.stale_data_timeout_secs);
            ui.end_row();
            ui.label("Postgres URL");
            ui.text_edit_singleline(&mut form.postgres_url);
            ui.end_row();
            ui.label("Session id (optional)");
            ui.text_edit_singleline(&mut form.session_id);
            ui.end_row();
        });

    if !state.available_strategies.is_empty() {
        ui.horizontal_wrapped(|ui| {
            ui.weak("Found in the strategies folder:");
            for path in state.available_strategies.clone() {
                let name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();
                if ui.small_button(&name).clicked() {
                    state.form.strategy_path = path.to_string_lossy().into_owned();
                }
            }
        });
    }
    ui.weak(
        "API keys are never typed here. They come from environment variables of the app, named by the credentials name and the network.",
    );
}

fn launch_button(ui: &mut egui::Ui, state: &LauncherState, actions: &mut Vec<LauncherAction>) {
    if let Some(problem) = state
        .form
        .to_request()
        .err()
        .and_then(|p| p.into_iter().next())
        && !state.busy()
    {
        ui.colored_label(
            palette_of(ui.ctx()).warn,
            format!("{}: {}", problem.field, problem.message),
        );
    }
    if let Some(var) = &state.missing_credential {
        ui.colored_label(
            palette_of(ui.ctx()).danger,
            format!("Missing credential variable: {var}"),
        );
    }
    ui.horizontal(|ui| {
        let label = match state.form.network {
            BinanceNetwork::Testnet => "Launch on TESTNET",
            BinanceNetwork::Mainnet => "Launch on MAINNET (real funds)",
        };
        let button = ui.add_enabled(state.can_launch(), egui::Button::new(label));
        let button = match state.launch_blocker() {
            Some(reason) => button.on_disabled_hover_text(reason),
            None => button,
        };
        if button.clicked() {
            actions.push(LauncherAction::Launch);
        }
        match &state.status {
            LaunchStatus::Idle => {}
            LaunchStatus::Launching => {
                ui.spinner();
                ui.label("Starting (the engine checks the exchange before it goes live)...");
            }
            LaunchStatus::Failed(_) => {
                ui.colored_label(palette_of(ui.ctx()).danger, "Launch failed (details below)");
            }
            LaunchStatus::Launched { session_id } => {
                ui.colored_label(palette_of(ui.ctx()).good, format!("Launched {session_id}"));
            }
        }
    });
    if let LaunchStatus::Failed(message) = &state.status {
        egui::ScrollArea::vertical()
            .max_height(140.0)
            .show(ui, |ui| {
                ui.colored_label(palette_of(ui.ctx()).danger, message);
            });
    }
    if let Some(notice) = &state.notice {
        ui.colored_label(palette_of(ui.ctx()).warn, notice);
    }
}

fn network_label(info: &SessionInfo, palette: &Palette) -> (String, Color32) {
    match info.network {
        Some(BinanceNetwork::Testnet) => ("TESTNET".to_string(), palette.info),
        Some(BinanceNetwork::Mainnet) => ("MAINNET".to_string(), palette.danger),
        None => ("unknown network".to_string(), palette.warn),
    }
}

fn sessions(
    ui: &mut egui::Ui,
    state: &LauncherState,
    pending_stop: Option<&str>,
    actions: &mut Vec<LauncherAction>,
) {
    ui.horizontal(|ui| {
        ui.heading("Sessions");
        if ui.button("Refresh").clicked() {
            actions.push(LauncherAction::Refresh);
        }
    });
    if state.sessions.is_empty() {
        ui.weak("No sessions found.");
        return;
    }
    for info in &state.sessions {
        ui.horizontal(|ui| {
            let (network, color) = network_label(info, &palette_of(ui.ctx()));
            ui.colored_label(color, RichText::new(network).strong());
            ui.label(info.id.as_str());
            if info.alive {
                ui.colored_label(palette_of(ui.ctx()).good, "running");
            } else {
                ui.colored_label(palette_of(ui.ctx()).warn, "stopped");
            }
            let id = info.id.to_string();
            if info.alive {
                if info.token.is_none() {
                    ui.weak("(no saved token: cannot connect)");
                }
                if ui
                    .add_enabled(info.token.is_some(), egui::Button::new("Open console"))
                    .clicked()
                {
                    actions.push(LauncherAction::Connect {
                        session_id: id.clone(),
                    });
                }
                if pending_stop == Some(id.as_str()) {
                    ui.colored_label(palette_of(ui.ctx()).danger, "Stop this session?");
                    if ui.button("Yes, stop").clicked() {
                        actions.push(LauncherAction::ConfirmStop {
                            session_id: id.clone(),
                        });
                    }
                    if ui.button("Cancel").clicked() {
                        actions.push(LauncherAction::CancelStop);
                    }
                } else if ui.button("Stop").clicked() {
                    actions.push(LauncherAction::RequestStop {
                        session_id: id.clone(),
                    });
                }
            } else if ui.button("Remove files").clicked() {
                actions.push(LauncherAction::Forget {
                    session_id: id.clone(),
                });
            }
        });
    }
}
