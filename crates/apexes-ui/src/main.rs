//! `apexes-ui` binary: a single process running three concurrent
//! pieces -- the WebSocket server task (Task 2, wrapping a real
//! `BinanceMarketDataProvider` via `MarketEventBus`), the WebSocket
//! client task (Task 3), and the `eframe`/`egui` UI loop (Task 5) --
//! communicating over a real `127.0.0.1:<port>` WebSocket connection.
//! See `docs/superpowers/specs/2026-08-04-apexes-ui-foundation-design.md`
//! for the full architecture rationale.

use apexes_core::{AssetClass, Instrument, MarketType, Venue};
use apexes_market_data::BinanceMarketDataProvider;
use apexes_message_bus::MarketEventBus;
use apexes_ui::client::ConnectionStatus;
use apexes_ui::ui::DashboardApp;
use clap::Parser;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

const DEFAULT_PORT: u16 = 9731;

#[derive(Parser)]
#[command(name = "apexes-ui", about = "Apexes real-time market dashboard")]
struct Cli {
    /// WebSocket port for the internal server<->client bridge.
    /// Overrides APEXES_UI_PORT if both are set.
    #[arg(long)]
    port: Option<u16>,
}

fn resolve_port(cli: &Cli) -> u16 {
    if let Some(port) = cli.port {
        return port;
    }
    if let Ok(value) = std::env::var("APEXES_UI_PORT") {
        match value.parse::<u16>() {
            Ok(port) => return port,
            Err(error) => {
                tracing::warn!(%value, ?error, "apexes-ui: unparseable APEXES_UI_PORT, falling back to default port");
            }
        }
    }
    DEFAULT_PORT
}

fn hardcoded_instruments() -> Vec<Instrument> {
    ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        .into_iter()
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
            .expect("hardcoded instrument construction should never fail")
        })
        .collect()
}

fn main() -> eframe::Result {
    tracing_subscriber_init();

    let cli = Cli::parse();
    let port = resolve_port(&cli);
    let instruments = hardcoded_instruments();
    let symbols: Vec<String> = instruments.iter().map(|i| i.symbol.clone()).collect();

    // `eframe::run_native` below blocks the calling thread running its
    // own native event loop, so the tokio runtime for the server/client
    // background tasks is built and driven separately here rather than
    // via `#[tokio::main]` -- `Runtime::spawn` (used below) does not
    // require the runtime to be "entered" on the calling thread, only
    // that the `Runtime` value itself stays alive for the process
    // lifetime (which it does, as a `main` local).
    let runtime = tokio::runtime::Runtime::new().expect("failed to build tokio runtime");

    let bus = Arc::new(MarketEventBus::new(Box::new(
        BinanceMarketDataProvider::new(),
    )));

    // Design spec section 4.1: subscribe once at startup and hold the
    // receiver for the life of the process, so the upstream Binance
    // connection is established eagerly at startup (a subscribe
    // failure exits the process with a clear error message rather
    // than surfacing only when the first WebSocket client connects).
    let _held_receiver = match runtime.block_on(bus.subscribe(&instruments)) {
        Ok(receiver) => receiver,
        Err(error) => {
            eprintln!("apexes-ui: failed to subscribe to market data at startup: {error}");
            std::process::exit(1);
        }
    };

    let app = apexes_ui::server::build_app(Arc::clone(&bus), instruments);

    // Bind BEFORE spawning the server task and BEFORE opening the UI
    // window: a panic inside a spawned tokio task only kills that
    // task, not the process, so a bind failure raised there would
    // leave the window up with the client stuck reconnecting forever
    // against a port nothing listens on -- design spec section 7
    // requires a bind failure to exit the process at startup with a
    // clear error message.
    let listener = match runtime.block_on(TcpListener::bind(("127.0.0.1", port))) {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("apexes-ui: failed to bind 127.0.0.1:{port}: {error}");
            std::process::exit(1);
        }
    };
    tracing::info!(port, "apexes-ui server: listening");

    runtime.spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("apexes-ui server: axum::serve exited unexpectedly");
    });

    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (status_tx, status_rx) = watch::channel(ConnectionStatus::default());
    let client_url = format!("ws://127.0.0.1:{port}/ws");
    runtime.spawn(apexes_ui::client::run(client_url, event_tx, status_tx));

    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "Apexes Dashboard",
        options,
        Box::new(move |_cc| Ok(Box::new(DashboardApp::new(&symbols, event_rx, status_rx)))),
    )
}

fn tracing_subscriber_init() {
    tracing_subscriber::fmt::init();
}
