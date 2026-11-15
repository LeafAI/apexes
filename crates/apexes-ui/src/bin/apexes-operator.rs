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
//! apexes-operator                      # launcher + console (starts and finds sessions)
//! APEXES_CONTROL_TOKEN=<token> apexes-operator --engine ws://127.0.0.1:9100/session/ws
//!                                      # console only, for an engine started elsewhere
//! ```
//!
//! The launcher starts a market-data gateway and an engine as detached
//! processes, so closing this window never stops a running session.

use apexes_supervisor::Supervisor;
use apexes_ui::chrome::Chrome;
use apexes_ui::client::ConnectionStatus;
use apexes_ui::operator_client::{OperatorClientConfig, run};
use apexes_ui::operator_view::{OperatorApp, StandaloneConsole};
use apexes_ui::shell_app::ShellApp;
use clap::Parser;
use tokio::sync::{mpsc, watch};

#[derive(Parser)]
#[command(
    name = "apexes-operator",
    about = "Launcher and operator console for Apexes sessions"
)]
struct Cli {
    /// Connect straight to an engine's session endpoint (console only, no
    /// launcher). Omit to use the launcher.
    #[arg(long)]
    engine: Option<String>,
    /// Bearer token for `--engine`. Prefer the environment variable:
    /// command-line arguments are visible to other users of the machine.
    #[arg(long, env = "APEXES_CONTROL_TOKEN", hide_env_values = true)]
    token: Option<String>,
}

fn main() -> eframe::Result {
    apexes_telemetry::init_tracing();
    let cli = Cli::parse();
    let runtime = tokio::runtime::Runtime::new().expect("failed to build tokio runtime");

    match cli.engine {
        Some(engine) => run_console_only(runtime, engine, cli.token),
        None => run_launcher(runtime),
    }
}

fn run_launcher(runtime: tokio::runtime::Runtime) -> eframe::Result {
    let Some(home) = std::env::var_os("HOME").map(std::path::PathBuf::from) else {
        eprintln!("apexes-operator: HOME is not set; cannot locate ~/.apexes");
        std::process::exit(2);
    };
    let supervisor = Supervisor::new(home);
    let handle = runtime.handle().clone();
    // Keeps the runtime (and so the console's client task) alive for the
    // whole window lifetime.
    let _runtime = runtime;
    eframe::run_native(
        "Apexes",
        apexes_ui::chrome::native_options(),
        Box::new(move |_cc| Ok(Box::new(ShellApp::new(supervisor, handle)))),
    )
}

fn run_console_only(
    runtime: tokio::runtime::Runtime,
    engine: String,
    token: Option<String>,
) -> eframe::Result {
    let token = token.filter(|t| !t.trim().is_empty());
    if token.is_none() {
        tracing::warn!(
            "no APEXES_CONTROL_TOKEN set: only an engine running without auth will accept this console"
        );
    }
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (status_tx, status_rx) = watch::channel(ConnectionStatus::default());
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    runtime.spawn(run(
        OperatorClientConfig {
            url: engine.clone(),
            token,
        },
        event_tx,
        status_tx,
        command_rx,
    ));
    let _runtime = runtime;
    eframe::run_native(
        "Apexes operator console",
        apexes_ui::chrome::native_options(),
        Box::new(move |_cc| {
            Ok(Box::new(StandaloneConsole::new(
                OperatorApp::new(engine, event_rx, status_rx, command_tx),
                Chrome::from_home(),
            )))
        }),
    )
}
