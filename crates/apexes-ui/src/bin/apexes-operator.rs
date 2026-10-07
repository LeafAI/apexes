//! `apexes-operator`: the operator console for a running `apexes-engine`
//! session -- positions, orders, risk, strategy parameters, and the halt /
//! flatten / resume controls.
//!
//! Deliberately a separate binary from `apexes-ui` (the market dashboard):
//! the console must keep working when market-data connectivity is what is
//! broken, and `apexes-ui` exits at startup if its market-data subscription
//! fails.
//!
//! ```text
//! APEXES_CONTROL_TOKEN=<token> apexes-operator --engine ws://127.0.0.1:9100/session/ws
//! ```

use apexes_ui::client::ConnectionStatus;
use apexes_ui::operator_client::{OperatorClientConfig, run};
use apexes_ui::operator_view::OperatorApp;
use clap::Parser;
use tokio::sync::{mpsc, watch};

#[derive(Parser)]
#[command(
    name = "apexes-operator",
    about = "Operator console for an Apexes engine session"
)]
struct Cli {
    /// The engine's session endpoint.
    #[arg(long, default_value = "ws://127.0.0.1:9100/session/ws")]
    engine: String,
    /// Bearer token. Prefer the environment variable: command-line arguments
    /// are visible to other users of the machine.
    #[arg(long, env = "APEXES_CONTROL_TOKEN", hide_env_values = true)]
    token: Option<String>,
}

fn main() -> eframe::Result {
    apexes_telemetry::init_tracing();
    let cli = Cli::parse();
    let token = cli.token.filter(|t| !t.trim().is_empty());
    if token.is_none() {
        tracing::warn!(
            "no APEXES_CONTROL_TOKEN set: only an engine running without auth will accept this console"
        );
    }

    let runtime = tokio::runtime::Runtime::new().expect("failed to build tokio runtime");
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (status_tx, status_rx) = watch::channel(ConnectionStatus::default());
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    runtime.spawn(run(
        OperatorClientConfig {
            url: cli.engine.clone(),
            token,
        },
        event_tx,
        status_tx,
        command_rx,
    ));

    let engine = cli.engine;
    eframe::run_native(
        "Apexes operator console",
        eframe::NativeOptions::default(),
        Box::new(move |_cc| {
            Ok(Box::new(OperatorApp::new(
                engine, event_rx, status_rx, command_tx,
            )))
        }),
    )
}
