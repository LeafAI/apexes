//! `apexes-market-data-gateway` binary: a standalone process that
//! holds ONE real upstream `BinanceMarketDataProvider` connection and
//! broadcasts every event to N connected WebSocket clients over
//! `GET /market` -- see this crate's `lib.rs` and
//! `docs/superpowers/specs/2026-08-05-apexes-13c-strategy-monitoring-tuning-design.md`
//! §3 for the full design and rationale.
//!
//! # PID file format (AUTHORITATIVE -- read this before writing any
//! code that parses this file, e.g. `apexes-ui`'s later supervisor
//! task)
//!
//! On successful startup (after binding the listener, before
//! `axum::serve` starts accepting connections), this process writes a
//! PID file at `~/.apexes/market-data-gateway.pid` (creating the
//! `~/.apexes/` directory first if it doesn't exist). The file's
//! entire content is exactly two lines, `\n`-terminated, in this
//! fixed order, with NO other content, comments, or blank lines:
//!
//! ```text
//! <pid>
//! <port>
//! ```
//!
//! - Line 1: this process's OS PID, as an unsigned decimal integer
//!   (`std::process::id()`, e.g. `12345`).
//! - Line 2: the TCP port this process actually bound and is serving
//!   `/market` on, as an unsigned decimal integer (e.g. `9741`) -- the
//!   resolved port after CLI/default-fallback logic, NOT necessarily
//!   the literal `--port` argument (e.g. if a future version supports
//!   OS-assigned port `0`, this line reflects the port actually
//!   assigned).
//!
//! A consumer (e.g. `apexes-ui`'s supervisor) should: read the file,
//! parse line 1 as the PID and check liveness (e.g. via `sysinfo` or a
//! platform `kill(pid, 0)` equivalent), and if alive, parse line 2 as
//! the port and connect to `ws://127.0.0.1:<port>/market`. If the file
//! is missing, unparseable, or the PID is dead, the consumer should
//! treat the gateway as not-running and may spawn a new instance
//! (which will overwrite this file with its own fresh PID/port).
//!
//! This process does NOT delete the PID file on graceful shutdown --
//! liveness is determined solely by checking whether the recorded PID
//! is still alive, not by file presence/absence alone (a stale file
//! left behind by an unclean exit, e.g. `SIGKILL`, is expected and
//! handled correctly by that liveness check on the next consumer read,
//! not by this process cleaning up after itself).

use apexes_core::{AssetClass, Instrument, MarketType, Venue};
use apexes_market_data::BinanceMarketDataProvider;
use apexes_market_data_gateway::Gateway;
use apexes_market_data_gateway::persistence::PersistenceBuffer;
use apexes_storage::{StorageReader, StorageWriter};
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;

const DEFAULT_PORT: u16 = 9741;

/// Default persistence batch-flush thresholds, matching the design
/// spec §3.2's example figures ("e.g. 100 events or 1 second, whichever
/// comes first"). Not exposed as CLI flags -- this crate's scope is
/// the minimum-viable gateway (row 8.3), and these values have no
/// established need for runtime tuning yet.
const PERSISTENCE_MAX_BATCH_SIZE: usize = 100;
const PERSISTENCE_MAX_BATCH_AGE: Duration = Duration::from_secs(1);
const PERSISTENCE_FLUSH_AGED_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Parser)]
#[command(
    name = "apexes-market-data-gateway",
    about = "Apexes cross-process market-data gateway (row 8.3 minimum-viable)"
)]
struct Cli {
    /// TCP port to bind `GET /market` on. Defaults to `DEFAULT_PORT`.
    #[arg(long)]
    port: Option<u16>,

    /// Comma-separated CPU core IDs to pin this process's main thread
    /// to (e.g. `--cpu-cores 2,3`). Omit to leave OS scheduling at its
    /// default (fully backward-compatible: this is a performance
    /// optimization, never a correctness precondition -- see
    /// `apply_cpu_affinity`'s doc comment).
    #[arg(long, value_delimiter = ',')]
    cpu_cores: Option<Vec<usize>>,

    /// ClickHouse URL (e.g. `http://localhost:8123`) enabling
    /// persistence and `?replay_from=` support. Omitting this disables
    /// both entirely -- a client requesting replay against a gateway
    /// started without this flag gets its socket closed gracefully
    /// (see `server::handle_socket`), not a panic.
    #[arg(long)]
    clickhouse_url: Option<String>,

    /// Symbols to subscribe to on Binance Spot (this crate's current
    /// scope -- see the design spec §3.1). Comma-separated, e.g.
    /// `--instruments BTCUSDT,ETHUSDT`.
    #[arg(long, value_delimiter = ',')]
    instruments: Vec<String>,
}

fn resolve_port(cli: &Cli) -> u16 {
    cli.port.unwrap_or(DEFAULT_PORT)
}

/// Builds `Instrument`s for `symbols` on `Venue::Binance` /
/// `AssetClass::Spot` -- this crate's current scope per the design
/// spec §3.1 ("symbol list, Binance Spot assumed"). Tick/lot-size
/// fields use the same placeholder values as `apexes-ui`'s own
/// `hardcoded_instruments` precedent (`crates/apexes-ui/src/main.rs`):
/// this gateway only broadcasts/persists raw market data, it never
/// places orders, so these fields' real values are immaterial here.
fn instruments_from_symbols(symbols: &[String]) -> Vec<Instrument> {
    symbols
        .iter()
        .map(|symbol| {
            Instrument::new(
                symbol,
                MarketType::Crypto,
                Venue::Binance,
                AssetClass::Spot,
                None,
                None,
                rust_decimal::Decimal::new(1, 2),
                rust_decimal::Decimal::new(1, 4),
                rust_decimal::Decimal::ONE,
            )
            .unwrap_or_else(|err| {
                panic!("apexes-market-data-gateway: invalid --instruments symbol {symbol:?}: {err}")
            })
        })
        .collect()
}

/// Validates `requested` core IDs against `core_affinity::get_core_ids()`
/// (this machine's actually-available cores) and returns the subset
/// that are valid, logging `tracing::warn!` for any that aren't. Runs
/// ONCE, synchronously, before the tokio runtime is built -- the
/// result is then shared (via `Arc`) with every worker thread's
/// `on_thread_start` hook (see `main`'s own doc comment on why a
/// per-thread hook, not a single call in `main`, is required for this
/// to actually work under tokio's multi-threaded runtime).
fn resolve_requested_core_ids(requested: &[usize]) -> Vec<core_affinity::CoreId> {
    if requested.is_empty() {
        return Vec::new();
    }
    let Some(available) = core_affinity::get_core_ids() else {
        tracing::warn!(
            "apexes-market-data-gateway: core_affinity::get_core_ids() returned None on this \
             platform, continuing with default OS scheduling"
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
                        "apexes-market-data-gateway: requested --cpu-cores value is not among \
                         this machine's available core IDs, ignoring it"
                    );
                    None
                }
            },
        )
        .collect()
}

/// Pins the CALLING thread (whichever tokio worker thread is running
/// when this is invoked from `on_thread_start`) to one core from
/// `resolved_cores`, chosen by round-robin via `next_index` (a shared
/// counter across all worker threads' startup, so N worker threads
/// starting up spread themselves across the N requested cores rather
/// than all piling onto `resolved_cores[0]`). A no-op if
/// `resolved_cores` is empty (no `--cpu-cores` was provided, or none
/// of the requested values were valid). `core_affinity`'s public API
/// only supports pinning to exactly one `CoreId` per call (there is no
/// "allow any of this set" call) -- round-robin is this crate's
/// chosen way to still spread a multi-core `--cpu-cores` request
/// across tokio's worker pool within that one-core-per-call
/// constraint, rather than pinning every worker thread to the same
/// single core. A pin failure is logged via `tracing::warn!` and never
/// aborts the thread's startup -- per the design spec §3.3, CPU
/// affinity is a performance optimization, never a correctness
/// precondition.
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
            "apexes-market-data-gateway: core_affinity::set_for_current failed for this \
             worker thread, continuing with default OS scheduling for it"
        );
    } else {
        tracing::info!(
            core = core_id.id,
            "apexes-market-data-gateway: pinned a tokio worker thread to CPU core"
        );
    }
}

/// Writes the PID file described in this file's own top-level doc
/// comment. Creates `~/.apexes/` if it doesn't exist. A failure here
/// (e.g. unwritable home directory) is logged via `tracing::error!`
/// and does NOT abort startup -- the gateway is still fully functional
/// for clients that already know its port; only auto-discovery by a
/// future supervisor degrades.
fn write_pid_file(port: u16) {
    let Some(home) = std::env::var_os("HOME") else {
        tracing::error!(
            "apexes-market-data-gateway: HOME environment variable not set, skipping PID file"
        );
        return;
    };
    let dir: PathBuf = PathBuf::from(home).join(".apexes");
    if let Err(error) = std::fs::create_dir_all(&dir) {
        tracing::error!(?error, path = ?dir, "apexes-market-data-gateway: failed to create PID file directory, skipping PID file");
        return;
    }
    let path = dir.join("market-data-gateway.pid");
    let contents = format!("{}\n{}\n", std::process::id(), port);
    if let Err(error) = std::fs::write(&path, contents) {
        tracing::error!(?error, path = ?path, "apexes-market-data-gateway: failed to write PID file");
    } else {
        tracing::info!(path = ?path, pid = std::process::id(), port, "apexes-market-data-gateway: wrote PID file");
    }
}

/// Constructs a `StorageWriter`/`StorageReader` pair and wraps them
/// into a `PersistenceBuffer` + `Arc<StorageReader>` for replay, if
/// `clickhouse_url` was provided. A connection failure here is logged
/// via `tracing::error!` and the gateway continues WITHOUT persistence
/// or replay (rather than exiting the process) -- per the design
/// spec's "monitoring availability over historical completeness"
/// convention: a broken ClickHouse should not take down live
/// broadcast to already-connected/connecting WS clients.
async fn build_persistence(
    clickhouse_url: Option<&str>,
) -> Option<(PersistenceBuffer, Arc<StorageReader>)> {
    let url = clickhouse_url?;
    let writer = match StorageWriter::new(url).await {
        Ok(writer) => writer,
        Err(error) => {
            tracing::error!(
                ?error,
                url,
                "apexes-market-data-gateway: StorageWriter::new failed, continuing without persistence/replay"
            );
            return None;
        }
    };
    let reader = match StorageReader::new(url).await {
        Ok(reader) => reader,
        Err(error) => {
            tracing::error!(
                ?error,
                url,
                "apexes-market-data-gateway: StorageReader::new failed, continuing without persistence/replay"
            );
            return None;
        }
    };
    let buffer = PersistenceBuffer::new(
        writer,
        PERSISTENCE_MAX_BATCH_SIZE,
        PERSISTENCE_MAX_BATCH_AGE,
    );
    Some((buffer, Arc::new(reader)))
}

/// Deliberately NOT `#[tokio::main]`: `core_affinity::set_for_current`
/// pins only the CALLING thread, not the whole process (see its own
/// upstream doc comment / this crate's `pin_current_thread_round_robin`).
/// `#[tokio::main]` builds tokio's default multi-threaded runtime
/// internally with no hook into its worker threads' startup, so a
/// single `core_affinity::set_for_current` call in `main()`'s body
/// would only ever pin `main()`'s own thread -- NOT the actual tokio
/// worker threads that run every spawned task and this binary's
/// `axum::serve` connection handling, making `--cpu-cores` silently
/// pin nothing that matters under that macro. `Builder::new_multi_thread`
/// exposes `on_thread_start`, a hook that runs on EVERY worker thread
/// as it starts, which is genuinely necessary here -- do not "simplify"
/// this back to `#[tokio::main]` + a single `apply_cpu_affinity()`
/// call without re-reading this comment; that regression has already
/// been caught and fixed once.
fn main() {
    apexes_telemetry::init_tracing();

    let cli = Cli::parse();
    let port = resolve_port(&cli);

    let resolved_cores = Arc::new(resolve_requested_core_ids(
        cli.cpu_cores.as_deref().unwrap_or(&[]),
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
        .unwrap_or_else(|error| {
            panic!("apexes-market-data-gateway: failed to build tokio runtime: {error}")
        });

    runtime.block_on(run(cli, port));
}

async fn run(cli: Cli, port: u16) {
    let instruments = instruments_from_symbols(&cli.instruments);

    let gateway = Arc::new(Gateway::new(Box::new(BinanceMarketDataProvider::new()), 0));

    // Design spec §3.2: subscribe once at startup and hold the
    // connection for the process lifetime -- a subscribe failure exits
    // the process with a clear error message rather than surfacing
    // only when the first WS client connects, mirroring `apexes-ui`'s
    // own Task 5 (13a) precedent.
    if let Err(error) = gateway.connect(&instruments).await {
        eprintln!(
            "apexes-market-data-gateway: failed to connect to upstream market data at startup: {error}"
        );
        std::process::exit(1);
    }

    let storage_reader = match build_persistence(cli.clickhouse_url.as_deref()).await {
        Some((buffer, reader)) => {
            gateway.attach_persistence(buffer).await;
            Some(reader)
        }
        None => None,
    };

    // Periodic age-based flush pass -- see `Gateway::flush_persistence_aged`'s
    // own doc comment. A no-op (cheap lock + immediate return) when no
    // persistence is attached, so this loop runs unconditionally.
    let flush_gateway = Arc::clone(&gateway);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(PERSISTENCE_FLUSH_AGED_INTERVAL);
        loop {
            interval.tick().await;
            flush_gateway.flush_persistence_aged().await;
        }
    });

    write_pid_file(port);

    let app =
        apexes_market_data_gateway::build_app(Arc::clone(&gateway), storage_reader, instruments);

    let listener = match TcpListener::bind(("127.0.0.1", port)).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("apexes-market-data-gateway: failed to bind 127.0.0.1:{port}: {error}");
            std::process::exit(1);
        }
    };
    tracing::info!(port, "apexes-market-data-gateway: listening on /market");

    if let Err(error) = axum::serve(listener, app).await {
        tracing::error!(
            ?error,
            "apexes-market-data-gateway: axum::serve exited with an error"
        );
        std::process::exit(1);
    }
}
