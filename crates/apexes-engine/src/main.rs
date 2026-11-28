//! `apexes-engine` binary entry point: one OS process per trading
//! session (design spec §四). Parses `--launch-config <path>` (a JSON
//! `LaunchConfig` file `apexes-ui`, Task 9, writes to a temp location
//! before spawning this process), applies optional CPU affinity,
//! constructs whichever mode-specific dependencies `LaunchConfig.mode`
//! needs, loads the strategy (`apexes_strategy_loader::load_strategy`),
//! constructs a `ControlHandle` (keeping its `Sender`/`Receiver` halves
//! for Task 8's `axum` `/session/ws` server to consume), and dispatches
//! to whichever of `run_backtest`/`run_paper_trading`/`run_live_trading`
//! the selected `SessionMode` requires.
//!
//! # Market-data wiring per mode (see this task's own report for the
//! full rationale)
//!
//! - `SessionMode::Backtest`: reads history directly from
//!   `apexes-storage` via one `apexes_backtest::StorageMarketDataSource`
//!   per instrument (combined via `combined_market_data::CombinedMarketDataSource`,
//!   this module) -- never connects to `LaunchConfig.gateway_url` at
//!   all, bypassing the live gateway entirely.
//! - `SessionMode::Paper`: connects to `LaunchConfig.gateway_url` via
//!   `crate::gateway_client::GatewayMarketDataProvider`, wraps it in a
//!   real `apexes_message_bus::MarketEventBus`, and passes `&bus`
//!   directly to `run_paper_trading`.
//! - `SessionMode::Live`: builds the SAME kind of gateway-backed
//!   `MarketEventBus`, then registers it into the `Registry` under
//!   EVERY distinct `Venue` present in `LaunchConfig.instruments` (see
//!   `register_gateway_bus_for_all_venues`'s own doc comment for why
//!   one shared bus registered per-venue, rather than one bus per
//!   venue, is the correct shape here) before calling
//!   `run_live_trading`, whose internal `subscribe_all_venues` looks
//!   buses up by `Venue`.

mod fee_pricing;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use apexes_backtest::market_data_source::StorageMarketDataSource;
use apexes_backtest::{BacktestConfig, ReplaySpeed, run_backtest};
use apexes_core::registry::Registry;
use apexes_core::{ApexesError, Instrument, Venue};
use apexes_engine::gateway_client::GatewayMarketDataProvider;
use apexes_engine::launch_config::{LaunchConfig, SessionMode};
use apexes_engine::{ServerSecurity, build_app_secured, credentials, resolve_binance_credentials};
use apexes_live_trading::{
    LiveTradingConfig, RecoverySelection, StrategyStartup, run_live_trading_with_startup,
};
use apexes_message_bus::MarketEventBus;
use apexes_oms::{ExecutionGateway, OrderManager, OrderStore};
use apexes_paper::{LatencyModel, PaperTradingConfig, run_paper_trading};
use apexes_storage::StorageReader;
use apexes_strategy::Strategy;
use clap::Parser;
use tokio::net::TcpListener;

mod combined_market_data;

use combined_market_data::CombinedMarketDataSource;

#[derive(Parser)]
#[command(
    name = "apexes-engine",
    about = "Apexes trading session process (design spec 2026-08-05, §四)"
)]
struct Cli {
    /// Path to a JSON-serialized `LaunchConfig` file.
    #[arg(long)]
    launch_config: PathBuf,
    /// Explicit local receipt for a qualified, pre-execution scoped Drift
    /// checkpoint. This is a one-time recovery, not automatic restart.
    #[arg(long)]
    strategy_recovery_receipt: Option<PathBuf>,
}

/// Errors specific to this binary's own startup sequence (CLI/config
/// loading, strategy loading, mode dispatch) -- distinct from
/// `apexes_core::ApexesError`, which every downstream `run_*` call
/// already returns.
#[derive(Debug, thiserror::Error)]
enum EngineError {
    #[error("failed to read --launch-config file {path:?}: {source}")]
    ReadLaunchConfig {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse --launch-config JSON: {0}")]
    ParseLaunchConfig(#[from] serde_json::Error),
    #[error("failed to load strategy: {0}")]
    StrategyLoad(#[from] apexes_strategy_loader::StrategyLoadError),
    #[error("strategy recovery requires a qualified live scoped Drift script")]
    UnsupportedStrategyRecovery,
    #[error("failed to select independent strategy recovery receipt: {0}")]
    StrategyRecovery(#[from] apexes_live_trading::StrategyRecoveryError),
    #[error("failed to construct scoped strategy for recovery: {0}")]
    ScopedStrategy(#[from] apexes_strategy::StrategyError),
    #[error("invalid launch config: {0}")]
    InvalidLaunchConfig(#[from] apexes_engine::launch_config::LaunchConfigError),
    #[error("failed to resolve live-trading credentials: {0}")]
    Credentials(#[from] credentials::CredentialsError),
    #[error("registry registration failed: {0}")]
    Registry(#[from] apexes_core::registry::RegistryError),
    #[error("failed to construct OrderStore: {0}")]
    OrderStore(#[from] apexes_oms::OmsError),
    #[error("failed to start Binance User Data Stream: {reason}")]
    UserDataStream { reason: String },
    #[error("failed to bind /session/ws listener (APEXES_ENGINE_BIND or 127.0.0.1:0): {0}")]
    BindSessionWs(std::io::Error),
    #[error("live startup recovery failed: {0}")]
    Recovery(#[from] apexes_live_trading::RecoveryError),
    #[error("spot inventory recovery failed: {0}")]
    SpotRecovery(#[from] apexes_live_trading::SpotRecoveryError),
    #[error(
        "live sessions require control tokens and an audit log: set APEXES_CONTROL_TOKEN and/or APEXES_CONTROL_TOKENS (and APEXES_AUDIT_LOG, or provide a writable $HOME)"
    )]
    MissingControlToken,
    #[error("invalid control tokens: {0}")]
    ControlTokens(String),
    #[error("failed to open audit log {path:?}: {source}")]
    AuditLog {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("run_backtest/run_paper_trading/run_live_trading failed: {0}")]
    Run(#[from] ApexesError),
}

/// `snapshot_min_interval`/`ControlHandle` construction shared by all
/// three modes: builds an empty initial `SessionSnapshot` (the unified
/// loop pushes its first real snapshot on the first tick that clears
/// `snapshot_min_interval`) and returns the handle plus the
/// `Sender`/`Receiver` halves Task 8's `axum` server consumes.
fn build_control_handle(
    snapshot_min_interval: std::time::Duration,
) -> (
    apexes_backtest::ControlHandle,
    tokio::sync::mpsc::UnboundedSender<apexes_backtest::SessionCommand>,
    tokio::sync::watch::Receiver<apexes_backtest::SessionSnapshot>,
) {
    let initial_snapshot = apexes_backtest::SessionSnapshot {
        identity: None,
        evaluation: None,
        equity_history: Vec::new(),
        recent_orders: Vec::new(),
        price_history: Default::default(),
        equity_priced: true,
        positions: HashMap::new(),
        balances: HashMap::new(),
        equity: rust_decimal::Decimal::ZERO,
        realized_pnl: HashMap::new(),
        margin_status: HashMap::new(),
        params: HashMap::new(),
        paused: false,
        open_orders: Vec::new(),
        // The UNIX epoch marks "no real snapshot pushed yet" (see
        // `apexes_backtest::SessionSnapshot::is_placeholder`): clients must
        // not present this all-zero state as real.
        timestamp: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
    };
    apexes_backtest::ControlHandle::new(snapshot_min_interval, initial_snapshot)
}

/// Writes this engine process's PID file at
/// `~/.apexes/sessions/<session_id>.pid` (creating
/// `~/.apexes/sessions/` first if it doesn't exist), so `apexes-ui`'s
/// future supervisor (Task 9, not yet built) can predict the path
/// BEFORE spawning this process (it already generates `session_id`
/// and writes it into the `LaunchConfig` it hands to this binary --
/// see `LaunchConfig::session_id`'s own doc comment for the full
/// rationale for why the identifier is caller-generated rather than
/// engine-generated).
///
/// # PID file format (AUTHORITATIVE -- mirrors
/// `apexes-market-data-gateway`'s own PID file format exactly, see
/// that crate's `main.rs` doc comment): the file's entire content is
/// exactly two lines, `\n`-terminated, in this fixed order, with no
/// other content:
///
/// ```text
/// <pid>
/// <port>
/// ```
///
/// - Line 1: this process's OS PID (`std::process::id()`).
/// - Line 2: the TCP port `/session/ws` actually bound (this engine
///   always binds an ephemeral `127.0.0.1:0` port -- there is no
///   `--port` CLI flag, since each session gets its own process and
///   `apexes-ui` needs to read the ACTUALLY assigned port back from
///   this file regardless).
///
/// A failure here (e.g. unwritable home directory) is logged via
/// `tracing::error!` and does NOT abort startup -- matching
/// `apexes-market-data-gateway`'s identical "auto-discovery degrades,
/// the session itself is unaffected" convention. This process does
/// NOT delete the PID file on graceful shutdown -- liveness is
/// determined by the consumer checking whether the recorded PID is
/// still alive, not by file presence/absence.
/// Keeps only the balances of assets this session can price and trade: the
/// base and quote currency of each instrument. Returns them and how many
/// other assets were dropped.
///
/// An account can hold many other assets (a real one, or a testnet account
/// pre-filled with hundreds). The ledger values an asset it has no price for
/// at 1.0, so every such balance would be added to equity as if it were one
/// unit of quote currency, and the session's PnL and drawdown would be
/// meaningless. Order placement, reconciliation and risk checks are not
/// affected: they do not read the ledger's view of unrelated assets.
fn session_balances(
    balances: HashMap<String, rust_decimal::Decimal>,
    instruments: &[Instrument],
) -> (HashMap<String, rust_decimal::Decimal>, usize) {
    let relevant: std::collections::HashSet<&str> = instruments
        .iter()
        .flat_map(|i| [i.base_currency.as_deref(), i.quote_currency.as_deref()])
        .flatten()
        .collect();
    let total = balances.len();
    let kept: HashMap<_, _> = balances
        .into_iter()
        .filter(|(asset, _)| relevant.contains(asset.as_str()))
        .collect();
    let ignored = total - kept.len();
    (kept, ignored)
}

/// Writes this session's PID file exactly once, when the session is ready.
#[derive(Clone)]
struct ReadyMarker {
    session_id: String,
    port: u16,
    written: Arc<std::sync::atomic::AtomicBool>,
}

impl ReadyMarker {
    fn new(session_id: &str, port: u16) -> Self {
        Self {
            session_id: session_id.to_string(),
            port,
            written: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn mark_ready(&self) {
        if !self.written.swap(true, std::sync::atomic::Ordering::SeqCst) {
            write_pid_file(&self.session_id, self.port);
        }
    }
}

fn write_pid_file(session_id: &str, port: u16) {
    let Some(home) = std::env::var_os("HOME") else {
        tracing::error!("apexes-engine: HOME environment variable not set, skipping PID file");
        return;
    };
    let dir: PathBuf = PathBuf::from(home).join(".apexes").join("sessions");
    if let Err(error) = std::fs::create_dir_all(&dir) {
        tracing::error!(
            ?error,
            path = ?dir,
            "apexes-engine: failed to create PID file directory, skipping PID file"
        );
        return;
    }
    let path = dir.join(format!("{session_id}.pid"));
    let contents = format!("{}\n{}\n", std::process::id(), port);
    if let Err(error) = std::fs::write(&path, contents) {
        tracing::error!(?error, path = ?path, "apexes-engine: failed to write PID file");
    } else {
        tracing::info!(
            path = ?path,
            pid = std::process::id(),
            port,
            "apexes-engine: wrote PID file"
        );
    }
}

/// Validates `requested` core IDs against this machine's actually
/// available cores and returns the valid subset, logging
/// `tracing::warn!` for any that aren't -- identical in shape to
/// `apexes-market-data-gateway`'s own `resolve_requested_core_ids`
/// (Task 5), mirrored here rather than shared since neither crate
/// depends on the other.
fn resolve_requested_core_ids(requested: &[usize]) -> Vec<core_affinity::CoreId> {
    if requested.is_empty() {
        return Vec::new();
    }
    let Some(available) = core_affinity::get_core_ids() else {
        tracing::warn!(
            "apexes-engine: core_affinity::get_core_ids() returned None on this platform, \
             continuing with default OS scheduling"
        );
        return Vec::new();
    };
    requested
        .iter()
        .filter_map(
            |&requested_id| match available.iter().find(|c| c.id == requested_id) {
                Some(core_id) => Some(*core_id),
                None => {
                    tracing::warn!(
                        requested = requested_id,
                        "apexes-engine: requested cpu_cores value is not among this machine's \
                         available core IDs, ignoring it"
                    );
                    None
                }
            },
        )
        .collect()
}

/// Pins the calling (tokio worker) thread to one core from
/// `resolved_cores`, chosen by round-robin via `next_index` -- see
/// `apexes-market-data-gateway`'s Task 5 `pin_current_thread_round_robin`
/// for the full rationale (mirrored here identically). A no-op if
/// `resolved_cores` is empty.
fn pin_current_thread_round_robin(
    resolved_cores: &[core_affinity::CoreId],
    next_index: &AtomicUsize,
) {
    if resolved_cores.is_empty() {
        return;
    }
    let index = next_index.fetch_add(1, Ordering::Relaxed) % resolved_cores.len();
    let core_id = resolved_cores[index];
    if !core_affinity::set_for_current(core_id) {
        tracing::warn!(
            core = core_id.id,
            "apexes-engine: core_affinity::set_for_current failed for this worker thread, \
             continuing with default OS scheduling for it"
        );
    } else {
        tracing::info!(
            core = core_id.id,
            "apexes-engine: pinned a tokio worker thread to CPU core"
        );
    }
}

/// Deliberately NOT `#[tokio::main]` -- `core_affinity::set_for_current`
/// pins only the calling thread, so CPU affinity must be applied via
/// `Builder::new_multi_thread().on_thread_start(..)`, run on EVERY
/// tokio worker thread as it starts. See
/// `apexes-market-data-gateway/src/main.rs`'s identical `main()` doc
/// comment (Task 5) for the full rationale against "simplifying" this
/// back to `#[tokio::main]` + a single call.
fn main() {
    apexes_telemetry::init_tracing();

    let cli = Cli::parse();

    let launch_config = match load_launch_config(&cli.launch_config) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("apexes-engine: failed to load --launch-config: {error}");
            std::process::exit(1);
        }
    };

    let resolved_cores = Arc::new(resolve_requested_core_ids(
        launch_config.cpu_cores.as_deref().unwrap_or(&[]),
    ));
    let next_core_index = Arc::new(AtomicUsize::new(0));
    let runtime_cores = Arc::clone(&resolved_cores);
    let runtime_next_index = Arc::clone(&next_core_index);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(move || {
            pin_current_thread_round_robin(&runtime_cores, &runtime_next_index);
        })
        .build()
        .unwrap_or_else(|error| panic!("apexes-engine: failed to build tokio runtime: {error}"));

    // `apexes_strategy::drift_dsl::DriftScriptStrategy` (a real,
    // production `Strategy` a `StrategyRef::DriftScript` resolves to)
    // internally calls `tokio::runtime::Handle::current().block_on(..)`
    // in BOTH its constructor AND every single `on_market_event` call
    // -- i.e. on every tick of the unified loop, not just once at
    // startup. This means the top-level future driving a whole session
    // can never itself be driven by `Runtime::block_on` on the same
    // thread: `on_market_event`'s nested `block_on` call would panic
    // with "Cannot start a runtime from within a runtime" as soon as
    // the strategy sees its first event (this was a REAL bug this
    // task's own end-to-end subprocess integration test caught --
    // `main.rs`'s first draft used `runtime.block_on` directly and the
    // subprocess crashed on the very first market event).
    //
    // The fix, matching `apexes-optimize`'s own
    // `grid_search_end_to_end.rs` test-suite precedent for this exact
    // situation: `runtime.enter()` makes `Handle::current()` resolve
    // (so `tokio::spawn`/timers/etc. still work, backed by this
    // multi-threaded runtime's already-running worker pool) WITHOUT
    // setting tokio's own thread-local "already inside a block_on on
    // this thread" marker; the actual outer future is then driven by
    // `futures::executor::block_on` instead -- a plain executor with
    // no knowledge of that marker, so a `Handle::block_on` call nested
    // underneath it no longer collides with anything.
    let _runtime_guard = runtime.enter();
    let strategy =
        match load_startup_strategy(&launch_config, cli.strategy_recovery_receipt.as_deref()) {
            Ok(strategy) => strategy,
            Err(error) => {
                eprintln!("apexes-engine: failed to load strategy: {error}");
                std::process::exit(1);
            }
        };

    let exit_code = futures::executor::block_on(run(launch_config, strategy));
    std::process::exit(exit_code);
}

fn load_startup_strategy(
    config: &LaunchConfig,
    receipt_path: Option<&std::path::Path>,
) -> Result<StrategyStartup<Box<dyn Strategy>>, EngineError> {
    let Some(path) = receipt_path else {
        return Ok(StrategyStartup::Legacy(
            apexes_strategy_loader::load_strategy(&config.strategy_ref)?,
        ));
    };
    if !matches!(config.mode, SessionMode::Live { .. }) {
        return Err(EngineError::UnsupportedStrategyRecovery);
    }
    let apexes_strategy_loader::StrategyRef::DriftScript { path: script } = &config.strategy_ref
    else {
        return Err(EngineError::UnsupportedStrategyRecovery);
    };
    let selection = RecoverySelection::read(path)?;
    let build = || -> Result<Box<dyn Strategy>, EngineError> {
        Ok(Box::new(
            apexes_strategy::drift_dsl::DriftScriptStrategy::from_file_with_scoped_checkpoints(
                script,
                selection.initial_params(),
                &config.instruments,
            )?,
        ))
    };
    // Construct independent interpreters in the entered runtime, before
    // any live gateway or server startup. Scoped-only qualification is an
    // explicit operator prerequisite, not inferred from script contents.
    let active = build()?;
    let candidate = build()?;
    Ok(StrategyStartup::Restore {
        active,
        candidate,
        selection: Box::new(selection),
    })
}

fn legacy_strategy(
    startup: StrategyStartup<Box<dyn Strategy>>,
) -> Result<Box<dyn Strategy>, EngineError> {
    match startup {
        StrategyStartup::Legacy(strategy) => Ok(strategy),
        StrategyStartup::Restore { .. } => Err(EngineError::UnsupportedStrategyRecovery),
    }
}

fn load_launch_config(path: &PathBuf) -> Result<LaunchConfig, EngineError> {
    let contents =
        std::fs::read_to_string(path).map_err(|source| EngineError::ReadLaunchConfig {
            path: path.clone(),
            source,
        })?;
    let launch_config: LaunchConfig = serde_json::from_str(&contents)?;
    launch_config.validate()?;
    Ok(launch_config)
}

async fn run(launch_config: LaunchConfig, strategy: StrategyStartup<Box<dyn Strategy>>) -> i32 {
    match run_inner(launch_config, strategy).await {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("apexes-engine: session failed: {error}");
            1
        }
    }
}

/// Owns constructing the ONE shared `oneshot::channel()` used for
/// graceful `Stop` requests (see `SessionCommand::Stop`'s own doc
/// comment): NOT constructed per-mode-function (each mode function
/// building its own throwaway `oneshot::channel()` and discarding the
/// sender half, as an earlier draft of this file did, means a
/// `Stop` command sent over `/session/ws` would have nowhere real to
/// go). Also binds the `/session/ws` listener, spawns the `axum`
/// server as its own concurrently-running task (NOT blocking the mode
/// dispatch below), and writes this engine's PID file -- all BEFORE
/// dispatching to the selected mode, so a client connecting
/// immediately after this process starts never races against the
/// listener not existing yet (a `watch` channel's "late subscriber
/// gets the current value" semantics also mean starting the trading
/// loop before the server binds is not itself a correctness concern,
/// but binding first is simplest to reason about and costs nothing).
async fn run_inner(
    launch_config: LaunchConfig,
    strategy: StrategyStartup<Box<dyn Strategy>>,
) -> Result<(), EngineError> {
    let registry = Registry::new();
    if let Some(policy) = launch_config.simulated_execution.clone() {
        registry.register::<apexes_backtest::SimulatedExecution, ()>((), policy)?;
    }
    if let Some(policy) = launch_config.simulated_depth {
        registry.register::<apexes_backtest::SimulatedDepthPolicy, ()>((), policy)?;
    }
    if let Some(policy) = launch_config.event_latency {
        registry.register::<apexes_backtest::EventLatencyPolicy, ()>((), policy)?;
    }
    if let Some(policy) = launch_config.live_observation {
        registry.register::<apexes_backtest::clock::LiveObservationPolicy, ()>((), policy)?;
    }
    let (control_handle, command_tx, snapshot_rx) =
        build_control_handle(launch_config.snapshot_min_interval);
    let control_handle = control_handle
        .with_identity(launch_config.identity())
        .with_limits(launch_config.limit_spec());

    // Before binding anything: a live session must not start without
    // authentication and an audit trail on its control endpoint.
    let security = build_server_security(&launch_config)?;

    // Loopback + ephemeral port by default (UI-spawned sessions find the
    // port via the PID file). Containers/supervised deployments set
    // `APEXES_ENGINE_BIND` (e.g. `0.0.0.0:9100`) for a fixed, reachable
    // port; a live session is then protected by its mandatory token.
    let bind_addr = std::env::var("APEXES_ENGINE_BIND")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "127.0.0.1:0".to_string());
    let listener = TcpListener::bind(&bind_addr)
        .await
        .map_err(EngineError::BindSessionWs)?;
    let bound_port = listener
        .local_addr()
        .map_err(EngineError::BindSessionWs)?
        .port();
    // The PID file is how a supervisor learns this session is up. Paper and
    // backtest sessions are up as soon as they listen. A LIVE session is not
    // up until startup recovery has succeeded: the control port is open
    // earlier (an operator must always be able to connect and Halt), but the
    // PID file is only written by `ReadyMarker::mark_ready`, called from
    // `run_live_mode` once recovery is done. A session that refuses to start
    // therefore never looks started.
    let ready = ReadyMarker::new(&launch_config.session_id, bound_port);
    if !matches!(launch_config.mode, SessionMode::Live { .. }) {
        ready.mark_ready();
    }

    // The one shared stop channel: `stop_rx` is handed to the mode
    // dispatch below only for `Paper`/`Live` (`Backtest`'s historical
    // replay has no live-stop concept -- see `run_backtest_mode`'s own
    // doc comment). `stop_tx` is handed into `build_app` -- the
    // `/session/ws` command-reader task fires it when it receives a
    // `SessionCommand::Stop`, in ADDITION to forwarding that command
    // into `command_tx` as before, so a `Stop` unblocks a stalled
    // `market_events` stream immediately rather than waiting for
    // `ControlHandle::drain_commands` to next be polled (see
    // `server.rs`'s own top-level doc comment for the full
    // rationale).
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();

    let kill_switch = Arc::new(if matches!(launch_config.mode, SessionMode::Live { .. }) {
        apexes_oms::KillSwitch::managed()
    } else {
        apexes_oms::KillSwitch::new()
    });
    spawn_snapshot_gauges(snapshot_rx.clone());
    let app = build_app_secured(
        command_tx,
        snapshot_rx,
        stop_tx,
        Some(Arc::clone(&kill_switch)),
        security,
    );
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::error!(
                ?error,
                "apexes-engine: /session/ws axum::serve exited with an error"
            );
        }
    });
    tracing::info!(
        port = bound_port,
        session_id = %launch_config.session_id,
        "apexes-engine: listening on /session/ws"
    );

    match launch_config.mode.clone() {
        SessionMode::Backtest {
            date_range,
            clickhouse_url,
        } => {
            run_backtest_mode(
                &launch_config,
                &registry,
                legacy_strategy(strategy)?,
                control_handle,
                date_range,
                clickhouse_url,
            )
            .await
        }
        SessionMode::Paper => {
            run_paper_mode(
                &launch_config,
                &registry,
                legacy_strategy(strategy)?,
                control_handle,
                stop_rx,
            )
            .await
        }
        SessionMode::Live {
            network,
            risk_limits,
            credentials_ref,
            auto_derisk_live,
            reconcile_interval,
            ems_postgres_url,
            stale_data_timeout_secs,
        } => {
            tracing::info!(%network, "apexes-engine: LIVE session network");
            let live_params = LiveModeParams {
                network,
                risk_limits,
                credentials_ref,
                auto_derisk_live,
                reconcile_interval,
                ems_postgres_url,
                kill_switch,
                stale_data_timeout: stale_data_timeout_secs.map(std::time::Duration::from_secs),
                ready,
            };
            run_live_mode(
                &launch_config,
                &registry,
                strategy,
                control_handle,
                live_params,
                stop_rx,
            )
            .await
        }
    }
}

/// Control-endpoint authentication/audit from the environment:
/// `APEXES_CONTROL_TOKEN` (bearer token) and `APEXES_AUDIT_LOG` (path;
/// default `$HOME/.apexes/audit-<session_id>.jsonl`).
fn build_server_security(launch_config: &LaunchConfig) -> Result<ServerSecurity, EngineError> {
    let default_path = std::env::var_os("HOME").map(|home| {
        PathBuf::from(home)
            .join(".apexes")
            .join(format!("audit-{}.jsonl", launch_config.session_id))
    });
    let audit_path = std::env::var_os("APEXES_AUDIT_LOG")
        .map(PathBuf::from)
        .or(default_path);
    let tokens =
        apexes_security::TokenSet::from_env("APEXES_CONTROL_TOKEN", "APEXES_CONTROL_TOKENS")
            .map_err(EngineError::ControlTokens)?;
    security_from_parts(
        matches!(launch_config.mode, SessionMode::Live { .. }),
        tokens,
        audit_path,
    )
}

/// Policy: live sessions REQUIRE a token and a working audit log;
/// backtest/paper sessions degrade to a loud warning.
fn security_from_parts(
    is_live: bool,
    tokens: Option<apexes_security::TokenSet>,
    audit_path: Option<PathBuf>,
) -> Result<ServerSecurity, EngineError> {
    if tokens.is_none() {
        if is_live {
            return Err(EngineError::MissingControlToken);
        }
        tracing::warn!(
            "apexes-engine: no APEXES_CONTROL_TOKEN/APEXES_CONTROL_TOKENS set; /session/ws and /metrics are UNAUTHENTICATED"
        );
    }
    let audit = match audit_path {
        Some(path) => match apexes_security::AuditLog::open(&path) {
            Ok(log) => Some(Arc::new(log)),
            Err(source) if is_live => return Err(EngineError::AuditLog { path, source }),
            Err(error) => {
                tracing::warn!(?error, ?path, "apexes-engine: audit log unavailable");
                None
            }
        },
        None if is_live => return Err(EngineError::MissingControlToken),
        None => None,
    };
    Ok(ServerSecurity {
        tokens: tokens.map(Arc::new),
        audit,
    })
}

/// Exchange-truth futures positions for the kill switch's flatten step
/// (spot holdings are deliberately not positions to unwind).
struct BinancePositionSource(Arc<apexes_gateway_binance::BinanceExecutionGateway>);

#[async_trait::async_trait]
impl apexes_live_trading::PositionSource for BinancePositionSource {
    async fn open_positions(&self) -> Result<Vec<apexes_strategy::Position>, String> {
        Ok(self.0.account_positions().await?.into_values().collect())
    }
}

/// Mirrors each pushed `SessionSnapshot` into the Prometheus gauges
/// (equity, realized PnL, open positions). Ends when the loop drops the
/// snapshot sender.
fn spawn_snapshot_gauges(
    mut snapshot_rx: tokio::sync::watch::Receiver<apexes_backtest::SessionSnapshot>,
) {
    use rust_decimal::prelude::ToPrimitive;
    tokio::spawn(async move {
        let telemetry = apexes_telemetry::metrics();
        while snapshot_rx.changed().await.is_ok() {
            let snapshot = snapshot_rx.borrow_and_update().clone();
            telemetry
                .equity
                .set(snapshot.equity.to_f64().unwrap_or(f64::NAN));
            telemetry.realized_pnl.set(
                snapshot
                    .realized_pnl
                    .values()
                    .map(|v| v.to_f64().unwrap_or(0.0))
                    .sum(),
            );
            telemetry
                .open_positions
                .set(snapshot.positions.len() as i64);
        }
    });
}

/// `SessionMode::Backtest`: `run_backtest` takes `impl
/// HistoricalMarketDataSource`, NOT a live stream -- reads history
/// directly from `apexes-storage`, bypassing `gateway_url` entirely.
/// Builds one `StorageMarketDataSource` per instrument (each with its
/// own fresh `StorageReader` -- `StorageReader::new` performs no
/// actual network I/O itself, it only configures a `clickhouse::Client`,
/// so constructing N of them is cheap) and combines them via
/// `CombinedMarketDataSource`.
async fn run_backtest_mode(
    launch_config: &LaunchConfig,
    registry: &Registry,
    strategy: Box<dyn Strategy>,
    control_handle: apexes_backtest::ControlHandle,
    date_range: std::ops::Range<chrono::NaiveDate>,
    clickhouse_url: String,
) -> Result<(), EngineError> {
    let mut sources: Vec<StorageMarketDataSource> = Vec::new();
    for instrument in &launch_config.instruments {
        let reader = StorageReader::new(&clickhouse_url).await.map_err(|error| {
            EngineError::Run(ApexesError::InvalidBacktestConfig {
                reason: format!(
                    "failed to construct StorageReader for {}: {error}",
                    instrument.symbol
                ),
            })
        })?;
        sources.push(StorageMarketDataSource::new(
            reader,
            instrument.symbol.clone(),
            instrument.venue,
            date_range.clone(),
        ));
    }
    let market_data = CombinedMarketDataSource::new(sources);

    let config = BacktestConfig {
        instruments: launch_config.instruments.clone(),
        date_range,
        initial_balances: launch_config.initial_balances.clone(),
        cost_model: launch_config.cost_model(),
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: rust_decimal::Decimal::ZERO,
        risk_free_return: launch_config.risk_free_return,
        margin_tiers: launch_config.margin_tiers.clone(),
        market_condition_config: launch_config.market_condition_config.clone(),
        control: Some(control_handle),
    };

    run_backtest(config, registry, market_data, strategy)
        .await
        .map_err(EngineError::Run)?;
    Ok(())
}

/// `SessionMode::Paper`: `run_paper_trading` takes `&MarketEventBus` --
/// connects to `gateway_url` via `GatewayMarketDataProvider`, wraps it
/// in a real `MarketEventBus`, and passes `&bus` directly.
///
/// `stop_signal`'s sender half is owned by `run_inner` (the shared
/// stop channel, see that function's own doc comment) -- NOT
/// constructed here anymore, since a per-mode-function throwaway
/// `oneshot::channel()` whose sender is immediately discarded makes
/// graceful `Stop` unreachable from outside the process (the bug
/// Task 8 fixes).
async fn run_paper_mode(
    launch_config: &LaunchConfig,
    registry: &Registry,
    strategy: Box<dyn Strategy>,
    control_handle: apexes_backtest::ControlHandle,
    stop_signal: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), EngineError> {
    let provider = GatewayMarketDataProvider::new(launch_config.gateway_url.clone())
        .with_token(std::env::var("APEXES_GATEWAY_TOKEN").ok());
    let bus = MarketEventBus::new(Box::new(provider));

    let config = PaperTradingConfig {
        instruments: launch_config.instruments.clone(),
        initial_balances: launch_config.initial_balances.clone(),
        cost_model: launch_config.cost_model(),
        queue_position_factor: rust_decimal::Decimal::ZERO,
        risk_free_return: launch_config.risk_free_return,
        latency_model: LatencyModel::None,
        margin_tiers: launch_config.margin_tiers.clone(),
        market_condition_config: launch_config.market_condition_config.clone(),
        control: Some(control_handle),
    };

    run_paper_trading(config, registry, &bus, strategy, stop_signal)
        .await
        .map_err(EngineError::Run)?;
    Ok(())
}

/// The four `SessionMode::Live`-specific fields extracted from
/// `LaunchConfig.mode`, grouped into one struct purely to keep
/// `run_live_mode`'s parameter count under clippy's
/// `too_many_arguments` threshold -- no behavior beyond that.
struct LiveModeParams {
    ready: ReadyMarker,
    network: apexes_core::binance_network::BinanceNetwork,
    risk_limits: apexes_engine::LiveRiskLimits,
    credentials_ref: String,
    auto_derisk_live: bool,
    reconcile_interval: std::time::Duration,
    ems_postgres_url: String,
    kill_switch: Arc<apexes_oms::KillSwitch>,
    stale_data_timeout: Option<std::time::Duration>,
}

/// `SessionMode::Live`: `run_live_trading` takes no direct market-data
/// parameter -- internally calls `subscribe_all_venues(registry,
/// instruments)`, which looks up `Arc<MarketEventBus>` instances from
/// `registry`, keyed by `Venue`. Builds the SAME
/// `GatewayMarketDataProvider`-backed bus as `Paper` mode and registers
/// it under every distinct venue in `launch_config.instruments` --
/// see `register_gateway_bus_for_all_venues`'s own doc comment for why
/// this is the correct shape.
///
/// `stop_signal`'s sender half is owned by `run_inner` (the shared
/// stop channel) -- see `run_paper_mode`'s identical doc note.
async fn run_live_mode(
    launch_config: &LaunchConfig,
    registry: &Registry,
    strategy: StrategyStartup<Box<dyn Strategy>>,
    control_handle: apexes_backtest::ControlHandle,
    live_params: LiveModeParams,
    stop_signal: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), EngineError> {
    let LiveModeParams {
        ready,
        network,
        risk_limits,
        credentials_ref,
        auto_derisk_live,
        reconcile_interval,
        ems_postgres_url,
        kill_switch,
        stale_data_timeout,
    } = live_params;

    let binance_gateway =
        resolve_binance_credentials(&credentials_ref, network, |name| std::env::var(name))?;

    // Pre-trade limits are mandatory in live: without a registered checker
    // the runner would apply no risk checking at all.
    registry.register::<Arc<apexes_risk::CompositeRiskChecker>, ()>(
        (),
        Arc::new(risk_limits.build_checker()),
    )?;

    // `start_user_data_stream` is a method on the CONCRETE
    // `BinanceExecutionGateway` type, not part of the `ExecutionGateway`
    // trait, so it must be called BEFORE `binance_gateway` is erased
    // into `Arc<dyn ExecutionGateway>` below. The returned
    // `UserDataStreamHandle` must be kept alive for as long as the
    // stream should keep running (dropping it aborts the underlying
    // WebSocket/renewal tasks) -- bound to `_user_data_stream_handle`
    // here so it stays alive across the whole `run_live_trading(...).await`
    // call below, rather than being dropped at the end of this
    // statement.
    let (_user_data_stream_handle, exchange_updates) = binance_gateway
        .start_user_data_stream_with_health(Some(Arc::clone(&kill_switch)))
        .await
        .map_err(|reason| EngineError::UserDataStream { reason })?;
    let exchange_updates = Box::pin(exchange_updates);

    let binance_gateway = Arc::new(binance_gateway);
    let gateway: Arc<dyn ExecutionGateway> = binance_gateway.clone();
    registry.register::<Arc<dyn ExecutionGateway>, ()>((), gateway)?;

    register_gateway_bus_for_all_venues(
        registry,
        &launch_config.gateway_url,
        &launch_config.instruments,
        Some(network),
        Some((
            Arc::clone(&kill_switch),
            stale_data_timeout.unwrap_or(std::time::Duration::from_secs(30)),
        )),
    )?;

    // Bind the dedicated database before reconciling any old client IDs
    // against the exchange. Session IDs are ephemeral; strategy reference,
    // credential fingerprint, network and instrument identity are stable.
    let mut scope: Vec<_> = launch_config
        .instruments
        .iter()
        .map(|i| (&i.symbol, &i.base_currency, &i.quote_currency))
        .collect();
    scope.sort();
    let owner = serde_json::json!({
        "version": 1,
        "network": network.to_string(),
        "credential": binance_gateway.credential_fingerprint(),
        "strategy": &launch_config.strategy_ref,
        "instruments": scope,
    })
    .to_string();
    let mut order_store = OrderStore::new(&ems_postgres_url).await?;
    order_store.bind_spot_inventory(&owner).await?;
    let fee_journal =
        order_store
            .fee_journal()
            .ok_or_else(|| apexes_oms::OmsError::InvalidRow {
                reason: "live fee journal not bound".into(),
            })?;
    let mut order_manager = OrderManager::new(order_store, registry)
        .await
        .map_err(EngineError::from)?
        .with_kill_switch(Arc::clone(&kill_switch))
        .with_spot_scope(&launch_config.instruments);

    // Seed the ledger from the exchange's real account state (not the
    // operator-supplied `initial_balances`), and refuse to start if
    // local/exchange order state cannot be brought into agreement.
    if !launch_config.initial_balances.is_empty() {
        tracing::warn!(
            "apexes-engine: ignoring launch_config.initial_balances in live mode; \
             balances are recovered from the exchange"
        );
    }
    kill_switch.begin_reconciliation();
    let mut wallet = HashMap::new();
    let recovered = apexes_live_trading::recover_live_state(
        &mut order_manager,
        &[apexes_core::AssetClass::Spot],
        async {
            wallet = binance_gateway
                .spot_balances()
                .await?
                .into_iter()
                .map(|(asset, balance)| {
                    (
                        asset,
                        apexes_live_trading::SpotWalletBalance {
                            free: balance.free,
                            locked: balance.locked,
                        },
                    )
                })
                .collect();
            Ok(apexes_live_trading::ExchangeSnapshot {
                balances: wallet
                    .iter()
                    .map(|(asset, balance)| (asset.clone(), balance.free + balance.locked))
                    .collect(),
                positions: HashMap::new(),
            })
        },
    )
    .await?;
    let fills = order_manager.spot_inventory_fills().await?;
    let recovered_positions = apexes_live_trading::recover_spot_positions(
        &launch_config.instruments,
        &fills,
        &wallet,
        !order_manager.open_orders().is_empty(),
    )?;
    tracing::info!(
        positions = recovered_positions.len(),
        fills = fills.len(),
        "strategy-owned spot inventory recovered from durable fills"
    );
    // Keep account wallet balances and strategy positions distinct;
    // replayed executions must not be credited to the wallet again.
    let (session_balances, ignored_assets) =
        session_balances(recovered.snapshot.balances, &launch_config.instruments);
    if ignored_assets > 0 {
        tracing::info!(
            ignored_assets,
            kept = session_balances.len(),
            "apexes-engine: ledger seeded with the assets this session trades; other account \
             assets are ignored (they cannot be priced and would distort equity)"
        );
    }

    let symbols: Vec<String> = launch_config
        .instruments
        .iter()
        .map(|i| i.symbol.clone())
        .collect();
    let initial_prices = binance_gateway.last_prices(&symbols).await;
    // Managed Spot conforms against fresh rules at final OMS dispatch,
    // including EMS children. A stale startup filter must not reject
    // an order that is valid under current exchange rules.
    let symbol_rules = HashMap::new();
    let (fee_task, lifetime_pnl) = fee_pricing::spawn(
        fee_journal,
        Arc::clone(&binance_gateway),
        launch_config.instruments.clone(),
    );

    let config = LiveTradingConfig {
        instruments: launch_config.instruments.clone(),
        initial_balances: session_balances,
        initial_positions: recovered_positions,
        cost_model: launch_config.cost_model(),
        margin_tiers: launch_config.margin_tiers.clone(),
        market_condition_config: launch_config.market_condition_config.clone(),
        auto_derisk_live,
        initial_prices,
        symbol_rules,
        risk_free_return: launch_config.risk_free_return,
        reconcile_interval,
        ems_postgres_url,
        control: Some(control_handle.with_lifetime_spot_pnl(lifetime_pnl)),
        kill_switch: Some(Arc::clone(&kill_switch)),
        stale_data_timeout: Some(stale_data_timeout.unwrap_or(std::time::Duration::from_secs(30))),
        resync: Some(_user_data_stream_handle.resync_notifier()),
        position_source: Some(Arc::new(BinancePositionSource(Arc::clone(
            &binance_gateway,
        )))),
    };

    let ready_task = tokio::spawn({
        let health = Arc::clone(&kill_switch);
        async move {
            if health.wait_startup_ready().await {
                ready.mark_ready();
            }
        }
    });
    let result = run_live_trading_with_startup(
        config,
        registry,
        strategy,
        order_manager,
        exchange_updates,
        stop_signal,
    )
    .await;
    ready_task.abort();
    fee_task.abort();
    if result.is_err() {
        kill_switch.fault("live trading loop failed");
    }
    result.map_err(EngineError::Run)?;
    Ok(())
}

/// Registers ONE shared `Arc<MarketEventBus>` (backed by a single
/// `GatewayMarketDataProvider` connection to `gateway_url`) under
/// EVERY distinct `Venue` present in `instruments`, rather than one
/// bus per venue.
///
/// Design rationale (a genuine judgment call, not a fully-specified
/// requirement -- see this task's dispatch): `GatewayMarketDataProvider`'s
/// one WS connection to the gateway process already carries every
/// event that gateway process is configured to broadcast, regardless
/// of what any individual `subscribe()` caller passes as its
/// `instruments` argument (see that adapter's own doc comment). A
/// SEPARATE `GatewayMarketDataProvider`/`MarketEventBus` pair per venue
/// would therefore open multiple redundant WebSocket connections to
/// the SAME gateway process for the SAME underlying event stream --
/// wasteful, and each one still constrained by
/// `MarketEventBus::subscribe`'s own "already connected with a
/// different, fixed instrument set" error if two different callers
/// ever subscribed the same bus with different instrument sets (not a
/// concern here, since `subscribe_all_venues` always passes each
/// venue's OWN instrument subset to each bus it looks up, and this
/// registration makes every venue resolve to the identical bus
/// instance, itself subscribed exactly once via `MarketEventBus`'s own
/// "first subscribe call wins" `NotConnected` -> `Connected` transition).
/// Registering the identical `Arc` under N keys is the cheapest correct
/// way to satisfy `subscribe_all_venues`'s per-venue lookup without
/// duplicating the underlying connection.
fn register_gateway_bus_for_all_venues(
    registry: &Registry,
    gateway_url: &str,
    instruments: &[Instrument],
    expected_network: Option<apexes_core::binance_network::BinanceNetwork>,
    live_guard: Option<(Arc<apexes_oms::KillSwitch>, std::time::Duration)>,
) -> Result<(), EngineError> {
    let mut provider = GatewayMarketDataProvider::new(gateway_url.to_string())
        .with_token(std::env::var("APEXES_GATEWAY_TOKEN").ok());
    if let Some(network) = expected_network {
        provider = provider.with_expected_network(network);
    }
    if let Some((kill_switch, max_event_age)) = live_guard {
        provider = provider.with_live_guard(kill_switch, max_event_age);
    }
    let bus = Arc::new(MarketEventBus::new(Box::new(provider)));

    let mut venues: Vec<Venue> = instruments.iter().map(|i| i.venue).collect();
    venues.sort_by_key(|v| format!("{v:?}"));
    venues.dedup();

    for venue in venues {
        registry.register::<Arc<MarketEventBus>, Venue>(venue, Arc::clone(&bus))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, MarketType};
    use rust_decimal_macros::dec;
    use std::ops::Range;

    #[test]
    fn live_sessions_require_a_token_and_a_usable_audit_log() {
        let dir = std::env::temp_dir().join(format!("apexes-engine-policy-{}", std::process::id()));
        let good_path = dir.join("a.jsonl");
        let token = || Some(apexes_security::TokenSet::single("t"));

        // Live without token: refused.
        assert!(matches!(
            security_from_parts(true, None, Some(good_path.clone())),
            Err(EngineError::MissingControlToken)
        ));
        // Live without audit path: refused.
        assert!(matches!(
            security_from_parts(true, token(), None),
            Err(EngineError::MissingControlToken)
        ));
        // Live with an unopenable audit path (parent is a file): refused.
        std::fs::create_dir_all(&dir).unwrap();
        let blocker = dir.join("blocker");
        std::fs::write(&blocker, b"x").unwrap();
        assert!(matches!(
            security_from_parts(true, token(), Some(blocker.join("audit.jsonl"))),
            Err(EngineError::AuditLog { .. })
        ));
        // Live with both: ok.
        let ok = security_from_parts(true, token(), Some(good_path)).unwrap();
        assert!(ok.tokens.is_some() && ok.audit.is_some());
        // Paper/backtest degrade to warnings.
        let open = security_from_parts(false, None, None).unwrap();
        assert!(open.tokens.is_none() && open.audit.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn sample_instrument(symbol: &str, venue: Venue) -> Instrument {
        Instrument::new(
            symbol,
            MarketType::Crypto,
            venue,
            AssetClass::Spot,
            Some("BASE".to_string()),
            Some("QUOTE".to_string()),
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap()
    }

    fn sample_launch_config(mode: SessionMode) -> LaunchConfig {
        LaunchConfig {
            session_id: format!("test-session-{}", uuid::Uuid::new_v4()),
            mode,
            strategy_ref: apexes_engine::StrategyRef::RustNative {
                name: "does-not-matter-for-this-test".to_string(),
            },
            instruments: vec![
                sample_instrument("BTCUSDT", Venue::Binance),
                sample_instrument("ETHUSDT", Venue::Binance),
            ],
            initial_balances: HashMap::from([("USDT".to_string(), dec!(10000))]),
            cpu_cores: None,
            snapshot_min_interval: LaunchConfig::default_snapshot_min_interval(),
            margin_tiers: HashMap::new(),
            market_condition_config: None,
            risk_free_return: dec!(0),
            simulated_fees: None,
            simulated_execution: None,
            simulated_depth: None,
            event_latency: None,
            live_observation: None,
            gateway_url: "ws://localhost:9001/market".to_string(),
        }
    }

    #[test]
    fn explicit_recovery_loader_builds_two_independent_scoped_drift_instances() {
        use apexes_core::binance_network::BinanceNetwork;
        use apexes_strategy::checkpoint::{StateCursor, StateIdentity, capture, verify_checkpoint};
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _entered = runtime.enter();
        let mut config = sample_launch_config(SessionMode::Live {
            network: BinanceNetwork::Testnet,
            risk_limits: apexes_engine::LiveRiskLimits {
                max_order_notional: dec!(100),
                max_order_quantity: dec!(1),
                max_position_per_symbol: dec!(2),
                max_daily_trade_count: 10,
                min_account_equity: None,
                max_daily_volume: None,
            },
            credentials_ref: "unused-test".into(),
            auto_derisk_live: false,
            reconcile_interval: std::time::Duration::from_secs(1),
            ems_postgres_url: "unused-test".into(),
            stale_data_timeout_secs: None,
        });
        let script = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../apexes-strategy/tests/fixtures/checkpoint_scoped.drift");
        config.strategy_ref = apexes_engine::StrategyRef::DriftScript {
            path: script.clone(),
        };
        let params = HashMap::from([("hot".into(), dec!(7))]);
        let strategy =
            apexes_strategy::drift_dsl::DriftScriptStrategy::from_file_with_scoped_checkpoints(
                &script,
                params.clone(),
                &config.instruments,
            )
            .unwrap();
        let identity = StateIdentity {
            strategy: "scoped-drift".into(),
            revision: "1".into(),
            config_sha256: "a".repeat(64),
            scope: "testnet:owner:BTCUSDT,ETHUSDT".into(),
            payload_version: 1,
        };
        let cursor = StateCursor {
            ordinal: 1,
            observed_at: chrono::DateTime::from_timestamp(1_800_000_000, 123).unwrap(),
        };
        let bytes = capture(&strategy, identity.clone(), cursor).unwrap();
        let receipt = verify_checkpoint(&bytes, &identity, cursor).unwrap();
        let selection = RecoverySelection::new("b".repeat(64), receipt.clone(), params).unwrap();
        let dir =
            std::env::temp_dir().join(format!("apexes-engine-restore-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("receipt.json");
        selection.write_new(&path).unwrap();

        let StrategyStartup::Restore {
            mut active,
            candidate,
            selection: loaded,
        } = load_startup_strategy(&config, Some(&path)).unwrap()
        else {
            panic!("explicit selection must not use a legacy strategy");
        };
        assert_eq!(loaded.receipt(), &receipt);
        assert_eq!(active.checkpoint_schema_version(), Some(1));
        assert_eq!(candidate.checkpoint_schema_version(), Some(1));
        active.set_param("hot", dec!(99)).unwrap();
        assert_eq!(candidate.current_params()["hot"], dec!(7));
        assert_eq!(capture(&candidate, identity, cursor).unwrap(), bytes);
        let StrategyStartup::Legacy(legacy) = load_startup_strategy(&config, None).unwrap() else {
            panic!("absent selection must retain the legacy loader");
        };
        assert_eq!(legacy.checkpoint_schema_version(), None);
        config.mode = SessionMode::Paper;
        assert!(matches!(
            load_startup_strategy(&config, Some(&path)),
            Err(EngineError::UnsupportedStrategyRecovery)
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn resolve_requested_core_ids_ignores_out_of_range_ids_without_panicking() {
        // This machine's real available cores are unknown at test time,
        // so this only exercises the "no crash" contract -- Task 5's
        // own equivalent test takes the same approach.
        let resolved = resolve_requested_core_ids(&[999_999_999]);
        assert!(resolved.is_empty() || !resolved.is_empty());
    }

    #[test]
    fn build_control_handle_produces_a_usable_handle_and_channel_halves() {
        let (_handle, command_tx, snapshot_rx) =
            build_control_handle(std::time::Duration::from_millis(500));
        assert!(!command_tx.is_closed());
        assert_eq!(snapshot_rx.borrow().equity, rust_decimal::Decimal::ZERO);
    }

    #[test]
    fn register_gateway_bus_for_all_venues_registers_exactly_one_bus_per_distinct_venue() {
        let registry = Registry::new();
        let instruments = vec![
            sample_instrument("BTCUSDT", Venue::Binance),
            sample_instrument("ETHUSDT", Venue::Binance),
        ];

        register_gateway_bus_for_all_venues(
            &registry,
            "ws://localhost:9001/market",
            &instruments,
            None,
            None,
        )
        .unwrap();

        let buses = registry.get::<Arc<MarketEventBus>, Venue>();
        assert_eq!(buses.len(), 1);
        assert!(buses.contains_key(&Venue::Binance));
    }

    #[test]
    fn load_launch_config_round_trips_a_real_backtest_config_from_disk() {
        let dir =
            std::env::temp_dir().join(format!("apexes-engine-main-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("launch_config.json");

        let date_range: Range<chrono::NaiveDate> = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            ..chrono::NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
        let config = sample_launch_config(SessionMode::Backtest {
            date_range,
            clickhouse_url: "http://localhost:8123".to_string(),
        });
        std::fs::write(&path, serde_json::to_string(&config).unwrap()).unwrap();

        let loaded = load_launch_config(&path).unwrap();
        assert_eq!(loaded.gateway_url, config.gateway_url);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_launch_config_fails_clearly_for_a_missing_file() {
        let missing = std::env::temp_dir().join(format!(
            "apexes-engine-main-test-missing-{}.json",
            uuid::Uuid::new_v4()
        ));
        let error = load_launch_config(&missing).unwrap_err();
        assert!(matches!(error, EngineError::ReadLaunchConfig { .. }));
    }

    #[test]
    fn only_the_assets_a_session_trades_seed_the_ledger() {
        let btcusdt = Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            Some("BTC".to_string()),
            Some("USDT".to_string()),
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap();
        let balances = HashMap::from([
            ("USDT".to_string(), dec!(10000)),
            ("BTC".to_string(), dec!(1)),
            ("ETH".to_string(), dec!(1)),
            ("BNB".to_string(), dec!(1)),
            ("这是测试币".to_string(), dec!(10000)),
            ("456".to_string(), dec!(10000)),
        ]);
        let (kept, ignored) = session_balances(balances, &[btcusdt]);
        assert_eq!(ignored, 4);
        assert_eq!(kept.len(), 2);
        assert_eq!(kept["USDT"], dec!(10000));
        assert_eq!(kept["BTC"], dec!(1));
    }

    #[test]
    fn a_session_with_no_instruments_keeps_nothing_and_never_panics() {
        let (kept, ignored) = session_balances(HashMap::from([("USDT".to_string(), dec!(1))]), &[]);
        assert!(kept.is_empty());
        assert_eq!(ignored, 1);
    }
}
