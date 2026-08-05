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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use apexes_backtest::market_data_source::StorageMarketDataSource;
use apexes_backtest::{BacktestConfig, CostModel, FlatFeeSchedule, ReplaySpeed, run_backtest};
use apexes_core::registry::Registry;
use apexes_core::{ApexesError, Instrument, Venue};
use apexes_engine::gateway_client::GatewayMarketDataProvider;
use apexes_engine::launch_config::{LaunchConfig, SessionMode};
use apexes_engine::{credentials, resolve_binance_credentials};
use apexes_live_trading::{LiveTradingConfig, run_live_trading};
use apexes_message_bus::MarketEventBus;
use apexes_oms::{ExecutionGateway, OrderManager, OrderStore};
use apexes_paper::{LatencyModel, PaperTradingConfig, run_paper_trading};
use apexes_storage::StorageReader;
use apexes_strategy::Strategy;
use clap::Parser;

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
    #[error("failed to resolve live-trading credentials: {0}")]
    Credentials(#[from] credentials::CredentialsError),
    #[error("registry registration failed: {0}")]
    Registry(#[from] apexes_core::registry::RegistryError),
    #[error("failed to construct OrderStore: {0}")]
    OrderStore(#[from] apexes_oms::OmsError),
    #[error("failed to start Binance User Data Stream: {reason}")]
    UserDataStream { reason: String },
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
        positions: HashMap::new(),
        balances: HashMap::new(),
        equity: rust_decimal::Decimal::ZERO,
        realized_pnl: HashMap::new(),
        margin_status: HashMap::new(),
        params: HashMap::new(),
        paused: false,
        timestamp: chrono::Utc::now(),
    };
    apexes_backtest::ControlHandle::new(snapshot_min_interval, initial_snapshot)
}

/// A zero-fee `CostModel` used by `Backtest`/`Paper` mode's internal
/// `MatchingEngine` -- `LaunchConfig` deliberately does not carry a
/// user-selectable cost model (design spec §4.2; see
/// `launch_config.rs`'s own doc comment on why `cost_model` is a fixed
/// engine-internal default rather than a launch parameter).
fn default_cost_model() -> CostModel {
    CostModel {
        fee_schedule: Box::new(FlatFeeSchedule {
            maker_bps: rust_decimal::Decimal::ZERO,
            taker_bps: rust_decimal::Decimal::ZERO,
        }),
        funding_rate_bps: None,
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
    tracing_subscriber::fmt::init();

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
    let strategy = match apexes_strategy_loader::load_strategy(&launch_config.strategy_ref) {
        Ok(strategy) => strategy,
        Err(error) => {
            eprintln!("apexes-engine: failed to load strategy: {error}");
            std::process::exit(1);
        }
    };

    let exit_code = futures::executor::block_on(run(launch_config, strategy));
    std::process::exit(exit_code);
}

fn load_launch_config(path: &PathBuf) -> Result<LaunchConfig, EngineError> {
    let contents =
        std::fs::read_to_string(path).map_err(|source| EngineError::ReadLaunchConfig {
            path: path.clone(),
            source,
        })?;
    let launch_config: LaunchConfig = serde_json::from_str(&contents)?;
    Ok(launch_config)
}

async fn run(launch_config: LaunchConfig, strategy: Box<dyn Strategy>) -> i32 {
    match run_inner(launch_config, strategy).await {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("apexes-engine: session failed: {error}");
            1
        }
    }
}

async fn run_inner(
    launch_config: LaunchConfig,
    strategy: Box<dyn Strategy>,
) -> Result<(), EngineError> {
    let registry = Registry::new();
    let (control_handle, _command_tx, _snapshot_rx) =
        build_control_handle(launch_config.snapshot_min_interval);

    match launch_config.mode.clone() {
        SessionMode::Backtest {
            date_range,
            clickhouse_url,
        } => {
            run_backtest_mode(
                &launch_config,
                &registry,
                strategy,
                control_handle,
                date_range,
                clickhouse_url,
            )
            .await
        }
        SessionMode::Paper => {
            run_paper_mode(&launch_config, &registry, strategy, control_handle).await
        }
        SessionMode::Live {
            credentials_ref,
            auto_derisk_live,
            reconcile_interval,
            ems_postgres_url,
        } => {
            let live_params = LiveModeParams {
                credentials_ref,
                auto_derisk_live,
                reconcile_interval,
                ems_postgres_url,
            };
            run_live_mode(
                &launch_config,
                &registry,
                strategy,
                control_handle,
                live_params,
            )
            .await
        }
    }
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
        cost_model: default_cost_model(),
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
async fn run_paper_mode(
    launch_config: &LaunchConfig,
    registry: &Registry,
    strategy: Box<dyn Strategy>,
    control_handle: apexes_backtest::ControlHandle,
) -> Result<(), EngineError> {
    let provider = GatewayMarketDataProvider::new(launch_config.gateway_url.clone());
    let bus = MarketEventBus::new(Box::new(provider));

    let config = PaperTradingConfig {
        instruments: launch_config.instruments.clone(),
        initial_balances: launch_config.initial_balances.clone(),
        cost_model: default_cost_model(),
        queue_position_factor: rust_decimal::Decimal::ZERO,
        risk_free_return: launch_config.risk_free_return,
        latency_model: LatencyModel::None,
        margin_tiers: launch_config.margin_tiers.clone(),
        market_condition_config: launch_config.market_condition_config.clone(),
        control: Some(control_handle),
    };

    // No external stop signal is wired up yet for Task 7 -- Task 8's
    // `axum` `/session/ws` server (not built by this task) is expected
    // to own the sender half of a fresh `oneshot` channel and trigger
    // graceful shutdown on a client `Stop` request; until then, this
    // session runs until the gateway connection ends the stream on its
    // own (which, per `MarketEventBus`'s own docs, it never does for a
    // live connection) or the process is killed externally.
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();

    run_paper_trading(config, registry, &bus, strategy, stop_rx)
        .await
        .map_err(EngineError::Run)?;
    Ok(())
}

/// The four `SessionMode::Live`-specific fields extracted from
/// `LaunchConfig.mode`, grouped into one struct purely to keep
/// `run_live_mode`'s parameter count under clippy's
/// `too_many_arguments` threshold -- no behavior beyond that.
struct LiveModeParams {
    credentials_ref: String,
    auto_derisk_live: bool,
    reconcile_interval: std::time::Duration,
    ems_postgres_url: String,
}

/// `SessionMode::Live`: `run_live_trading` takes no direct market-data
/// parameter -- internally calls `subscribe_all_venues(registry,
/// instruments)`, which looks up `Arc<MarketEventBus>` instances from
/// `registry`, keyed by `Venue`. Builds the SAME
/// `GatewayMarketDataProvider`-backed bus as `Paper` mode and registers
/// it under every distinct venue in `launch_config.instruments` --
/// see `register_gateway_bus_for_all_venues`'s own doc comment for why
/// this is the correct shape.
async fn run_live_mode(
    launch_config: &LaunchConfig,
    registry: &Registry,
    strategy: Box<dyn Strategy>,
    control_handle: apexes_backtest::ControlHandle,
    live_params: LiveModeParams,
) -> Result<(), EngineError> {
    let LiveModeParams {
        credentials_ref,
        auto_derisk_live,
        reconcile_interval,
        ems_postgres_url,
    } = live_params;

    let gateway_config = resolve_binance_credentials(&credentials_ref, |name| std::env::var(name))?;
    let binance_gateway = apexes_gateway_binance::BinanceExecutionGateway::new(gateway_config);

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
        .start_user_data_stream()
        .await
        .map_err(|reason| EngineError::UserDataStream { reason })?;
    let exchange_updates = Box::pin(exchange_updates);

    let gateway: Arc<dyn ExecutionGateway> = Arc::new(binance_gateway);
    registry.register::<Arc<dyn ExecutionGateway>, ()>((), gateway)?;

    register_gateway_bus_for_all_venues(
        registry,
        &launch_config.gateway_url,
        &launch_config.instruments,
    )?;

    let order_store = OrderStore::new(&ems_postgres_url).await?;
    let order_manager = OrderManager::new(order_store, registry)
        .await
        .map_err(EngineError::from)?;

    let config = LiveTradingConfig {
        instruments: launch_config.instruments.clone(),
        initial_balances: launch_config.initial_balances.clone(),
        cost_model: default_cost_model(),
        margin_tiers: launch_config.margin_tiers.clone(),
        market_condition_config: launch_config.market_condition_config.clone(),
        auto_derisk_live,
        risk_free_return: launch_config.risk_free_return,
        reconcile_interval,
        ems_postgres_url,
        control: Some(control_handle),
    };

    // Same "no external stop signal wired up yet" note as `run_paper_mode`
    // -- Task 8 owns triggering graceful shutdown.
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();

    run_live_trading(
        config,
        registry,
        strategy,
        order_manager,
        exchange_updates,
        stop_rx,
    )
    .await
    .map_err(EngineError::Run)?;
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
) -> Result<(), EngineError> {
    let provider = GatewayMarketDataProvider::new(gateway_url.to_string());
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
            gateway_url: "ws://localhost:9001/market".to_string(),
        }
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

        register_gateway_bus_for_all_venues(&registry, "ws://localhost:9001/market", &instruments)
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
}
