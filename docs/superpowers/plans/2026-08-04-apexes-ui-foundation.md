# UI Foundation (Sub-project #13a) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the design in `docs/superpowers/specs/2026-08-04-apexes-ui-foundation-design.md`: a new `apexes-ui` crate/binary proving the end-to-end pipe from a real `apexes-message-bus::MarketEventBus` subscription, through a real localhost WebSocket bridge, into a live-updating `egui`/`eframe` desktop table of bid/ask/last-trade price for three hardcoded Binance Spot instruments (`BTCUSDT`/`ETHUSDT`/`BNBUSDT`) -- the foundation sub-project `#13`'s later UI phases (13b-13f) will extend.

**Architecture:** Six sequential tasks. Task 1 scaffolds the crate and its pure-logic `app_state` module (no I/O). Task 2 builds the `axum`-based WebSocket server task, parameterized over an already-constructed `Arc<MarketEventBus>` so it's independently testable with a mock provider. Task 3 builds the `tokio-tungstenite`-based WebSocket client task with exponential-backoff reconnect and a `ConnectionStatus` state machine. Task 4 wires the real end-to-end server<->client integration test. Task 5 builds the `egui`/`eframe` UI (table + connection-status indicator) plus its `egui_kittest` headless render test, and assembles `main.rs`. Task 6 is a full-workspace verification checkpoint plus the final whole-branch review and `docs/FEATURES.md` update.

**Tech Stack:** Rust 2024, new workspace dependencies `axum` (0.8), `eframe`/`egui` (0.35), `egui_kittest` (0.35, dev-dependency, `features = ["eframe"]`). Existing workspace deps reused: `apexes-core`, `apexes-message-bus`, `apexes-market-data` (for `BinanceMarketDataProvider`), `tokio`, `tokio-tungstenite`, `serde_json`, `futures`, `tracing`, `clap`.

## Global Constraints

- Rust edition 2024, workspace-wide `cargo check`/`cargo clippy --all-targets -- -D warnings`/`cargo fmt --check` must stay clean after every task.
- No `unsafe` blocks. Never bare `.unwrap()`/`.expect()` in production code (test code may use them freely).
- Every commit signed: `git commit -S -s`.
- No new `ApexesError` variants -- this plan relies entirely on `MarketEventBus`'s existing error handling (`SubscriptionFailed`) and its own new, crate-local error/status types where needed (e.g. `ConnectionStatus`), never modifying `apexes-core`.
- **No dedicated UI-layer wire protocol.** WebSocket messages are `serde_json`-serialized `apexes_core::MarketEvent` values directly, filtered to `Trade`/`Tick` variants only before sending. Do not introduce a new message envelope type.
- **The server task must be constructible from an already-built `Arc<MarketEventBus>`, not just from a real `BinanceMarketDataProvider`.** This is what makes the Task 4 integration test possible without a real Binance connection. Only `main.rs` (Task 5) constructs the real `MarketEventBus::new(Box::new(BinanceMarketDataProvider::new()))`.
- **A `Lagged` broadcast-receive error must always be logged via `tracing::warn!` and skipped -- never a panic, never a silent stop.** Same convention `apexes-message-bus` and its existing consumers already follow.
- **A malformed/unparseable JSON frame on the client side must be logged via `tracing::warn!` and skipped, not tear down the connection.**
- Port is configurable via a `--port` CLI arg and/or `APEXES_UI_PORT` environment variable (CLI arg wins if both set); default fixed port `9731`.
- Full design rationale (why single-process-two-tasks-over-real-WebSocket, why `axum` over hand-rolled `tokio-tungstenite::accept_async`, why `egui`/`eframe` over Tauri) is in the design spec -- read it before starting if anything below is unclear on *why*, not just *what*.

---

### Task 1: Crate scaffold + `app_state` module (pure logic, no I/O)

**Files:**
- Modify: `Cargo.toml` (workspace `members` + new `[workspace.dependencies]` entries)
- Create: `crates/apexes-ui/Cargo.toml`
- Create: `crates/apexes-ui/src/lib.rs`
- Create: `crates/apexes-ui/src/app_state.rs`

**Interfaces:**
- Produces: `apexes_ui::app_state::{RowState, AppState}` -- `AppState::new(symbols: &[String]) -> Self`, `AppState::apply_event(&mut self, event: &apexes_core::MarketEvent)`, `AppState::rows(&self) -> &std::collections::BTreeMap<String, RowState>`. Used by Task 5's `ui.rs` and Task 2/3's tests.

- [ ] **Step 1: Add the new crate to the workspace and pin the new dependencies**

```toml
# Cargo.toml -- change the workspace members list (add "crates/apexes-ui" at
# the end) and add three new entries to [workspace.dependencies] (keep
# every existing line exactly as-is, only append):

[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage", "crates/apexes-backtest", "crates/apexes-strategy", "crates/apexes-indicators", "crates/apexes-optimize", "crates/apexes-strategy-registry", "crates/apexes-risk", "crates/apexes-paper", "crates/apexes-oms", "crates/apexes-gateway-binance", "crates/apexes-gateway-xtp", "crates/apexes-gateway-ibkr", "crates/apexes-live-trading", "crates/apexes-ems", "crates/apexes-account", "crates/apexes-market-data-chain", "crates/apexes-market-data-ashare", "crates/apexes-market-data-hshare", "crates/apexes-market-data-usequity", "crates/apexes-message-bus", "crates/apexes-ui"]

[workspace.package]
edition = "2024"
version = "0.1.0"
license = "MIT"

[workspace.dependencies]
rust_decimal = { version = "1.42", features = ["serde", "maths"] }
serde = { version = "1.0", features = ["derive"] }
thiserror = "2.0"
chrono = { version = "0.4", features = ["serde"] }
tokio = { version = "1.53", features = ["rt-multi-thread", "macros"] }
futures = "0.3"
async-trait = "0.1.91"
uuid = { version = "1.24", features = ["v4", "serde"] }
clap = { version = "4.6", features = ["derive"] }
serde_json = "1.0"
barter-data = "0.11"
barter-instrument = "0.3"
tracing = "0.1"
clickhouse = { version = "0.15", features = ["chrono"] }
fixnum = { version = "0.9", features = ["i128", "serde"] }
barter = "0.12"
sha2 = "0.10"
rand = "0.10"
sqlx = { version = "0.9", features = ["runtime-tokio", "postgres", "chrono", "rust_decimal", "uuid"] }
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
tokio-tungstenite = { version = "0.26", features = ["rustls-tls-webpki-roots"] }
hmac = "0.12"
hex = "0.4"
axum = "0.8"
eframe = "0.35"
egui = "0.35"
egui_kittest = { version = "0.35", features = ["eframe"] }
```

- [ ] **Step 2: Create the crate manifest**

```toml
# crates/apexes-ui/Cargo.toml
[package]
name = "apexes-ui"
edition.workspace = true
version.workspace = true
license.workspace = true

[[bin]]
name = "apexes-ui"
path = "src/main.rs"

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-message-bus = { path = "../apexes-message-bus" }
apexes-market-data = { path = "../apexes-market-data" }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time", "sync"] }
axum = { workspace = true, features = ["ws"] }
tokio-tungstenite = { workspace = true }
futures = { workspace = true }
serde_json = { workspace = true }
tracing = { workspace = true }
clap = { workspace = true }
eframe = { workspace = true }
egui = { workspace = true }

[dev-dependencies]
egui_kittest = { workspace = true }
```

- [ ] **Step 3: Write the failing tests for `app_state`**

```rust
// crates/apexes-ui/src/app_state.rs -- create the file with this content:

//! In-memory accumulator turning incoming `apexes_core::MarketEvent`s
//! into a per-symbol table of bid/ask/last-trade price -- pure logic,
//! no I/O, no `egui`/network dependency, so it can be unit-tested
//! directly. Used by both `ui.rs` (Task 5, to render) and by tests in
//! `server.rs`/`client.rs` (Tasks 2/3) as a shared fixture shape.

use apexes_core::MarketEvent;
use rust_decimal::Decimal;
use std::collections::BTreeMap;

/// One instrument's current bid/ask/last-trade state. `None` fields
/// mean "no event of that kind has arrived yet for this symbol" --
/// the UI renders those as "-" rather than a stale/fabricated value.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RowState {
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
    pub last_trade: Option<Decimal>,
}

/// Accumulates `MarketEvent`s into a `RowState` per symbol.
/// `BTreeMap` (not `HashMap`) so `rows()`'s iteration order is
/// deterministic -- the table renders symbols in a stable order run
/// to run.
pub struct AppState {
    rows: BTreeMap<String, RowState>,
}

impl AppState {
    /// Pre-populates one empty `RowState` per symbol in `symbols`, so
    /// the table always shows every hardcoded instrument (as "-"
    /// cells) even before any event for it has arrived.
    pub fn new(symbols: &[String]) -> Self {
        let mut rows = BTreeMap::new();
        for symbol in symbols {
            rows.insert(symbol.clone(), RowState::default());
        }
        Self { rows }
    }

    /// Updates this symbol's row from `event`. Events for a symbol
    /// not in the pre-populated set are ignored (this phase's table
    /// only ever shows the hardcoded instrument list -- see the
    /// design spec's Non-Goals on runtime-configurable instruments).
    /// `Depth`/`Bar`/`MarkPrice` variants are ignored -- only
    /// `Trade`/`Tick` affect the table, matching what the server task
    /// (Task 2) filters before ever sending a frame.
    pub fn apply_event(&mut self, event: &MarketEvent) {
        match event {
            MarketEvent::Trade(trade) => {
                if let Some(row) = self.rows.get_mut(&trade.symbol) {
                    row.last_trade = Some(trade.price);
                }
            }
            MarketEvent::Tick(tick) => {
                if let Some(row) = self.rows.get_mut(&tick.symbol) {
                    row.bid = Some(tick.bid_price);
                    row.ask = Some(tick.ask_price);
                }
            }
            MarketEvent::Depth(_) | MarketEvent::Bar(_) | MarketEvent::MarkPrice(_) => {}
        }
    }

    pub fn rows(&self) -> &BTreeMap<String, RowState> {
        &self.rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{TradeEvent, Tick, Venue};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn symbols() -> Vec<String> {
        vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
    }

    #[test]
    fn new_pre_populates_one_empty_row_per_symbol() {
        let state = AppState::new(&symbols());
        assert_eq!(state.rows().len(), 2);
        assert_eq!(state.rows().get("BTCUSDT"), Some(&RowState::default()));
        assert_eq!(state.rows().get("ETHUSDT"), Some(&RowState::default()));
    }

    #[test]
    fn trade_event_updates_last_trade_only() {
        let mut state = AppState::new(&symbols());
        state.apply_event(&MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        }));
        let row = state.rows().get("BTCUSDT").unwrap();
        assert_eq!(row.last_trade, Some(dec!(65000)));
        assert_eq!(row.bid, None);
        assert_eq!(row.ask, None);
    }

    #[test]
    fn tick_event_updates_bid_and_ask_only() {
        let mut state = AppState::new(&symbols());
        state.apply_event(&MarketEvent::Tick(Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(64990),
            bid_size: dec!(1),
            ask_price: dec!(65010),
            ask_size: dec!(1),
            timestamp: Utc::now(),
        }));
        let row = state.rows().get("BTCUSDT").unwrap();
        assert_eq!(row.bid, Some(dec!(64990)));
        assert_eq!(row.ask, Some(dec!(65010)));
        assert_eq!(row.last_trade, None);
    }

    #[test]
    fn event_for_an_unknown_symbol_is_ignored() {
        let mut state = AppState::new(&symbols());
        state.apply_event(&MarketEvent::Trade(TradeEvent {
            symbol: "DOGEUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(1),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        }));
        assert_eq!(state.rows().len(), 2);
        assert!(!state.rows().contains_key("DOGEUSDT"));
    }
}
```

- [ ] **Step 4: Create `lib.rs`**

```rust
// crates/apexes-ui/src/lib.rs
pub mod app_state;
```

Run: `cargo test -p apexes-ui app_state`
Expected: FAIL to compile until `rust_decimal`/`rust_decimal_macros` are added as dev-dependencies for the test module (Step 5 fixes this).

- [ ] **Step 5: Add the missing test-only dependencies**

```toml
# crates/apexes-ui/Cargo.toml -- the [dependencies] section needs
# rust_decimal added (RowState's fields are Decimal, used in
# production code, not just tests) and [dev-dependencies] needs
# rust_decimal_macros + chrono (both test-only, per this project's
# established "rust_decimal_macros/dec! only in dev-dependencies"
# convention -- confirm no production code in this crate ever calls
# dec!() before finalizing this split):

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-message-bus = { path = "../apexes-message-bus" }
apexes-market-data = { path = "../apexes-market-data" }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time", "sync"] }
axum = { workspace = true, features = ["ws"] }
tokio-tungstenite = { workspace = true }
futures = { workspace = true }
serde_json = { workspace = true }
tracing = { workspace = true }
clap = { workspace = true }
eframe = { workspace = true }
egui = { workspace = true }
rust_decimal = { workspace = true }

[dev-dependencies]
egui_kittest = { workspace = true }
rust_decimal_macros = "1.40"
chrono = { workspace = true }
```

- [ ] **Step 6: Run the tests**

Run: `cargo test -p apexes-ui app_state`
Expected: all 4 tests pass.

- [ ] **Step 7: Run clippy, fmt, workspace check**

Run: `cargo clippy -p apexes-ui --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/apexes-ui/Cargo.toml crates/apexes-ui/src/lib.rs crates/apexes-ui/src/app_state.rs
git commit -S -s -m "feat(apexes-ui): crate scaffold + app_state accumulator (sub-project #13a)"
```

---

### Task 2: WebSocket server task (axum, `MarketEventBus`-parameterized)

**Files:**
- Create: `crates/apexes-ui/src/server.rs`
- Modify: `crates/apexes-ui/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_message_bus::MarketEventBus` (existing, `crates/apexes-message-bus/src/bus.rs`), `apexes_core::{Instrument, MarketEvent}` (existing).
- Produces: `apexes_ui::server::build_app(bus: std::sync::Arc<MarketEventBus>, instruments: Vec<Instrument>) -> axum::Router` (used by both `main.rs`, Task 5, and this task's own tests, and Task 4's integration test).

- [ ] **Step 1: Write the failing test**

```rust
// crates/apexes-ui/src/server.rs -- create the file with this content:

//! `axum`-based WebSocket server: subscribes to a caller-supplied
//! `MarketEventBus` for a caller-supplied instrument list, and for
//! every connecting WebSocket client, forwards each `Trade`/`Tick`
//! `MarketEvent` as a JSON text frame (`Depth`/`Bar`/`MarkPrice` are
//! filtered out -- this phase's UI only shows bid/ask/last-trade).
//! Parameterized over an already-constructed `Arc<MarketEventBus>`
//! (not a function that builds its own `BinanceMarketDataProvider`
//! internally) specifically so tests can inject
//! `apexes_message_bus::test_support::MockMarketDataProvider` instead
//! of a real Binance connection -- see this crate's own tests below
//! and Task 4's `tests/server_client_roundtrip.rs`.

use apexes_core::{Instrument, MarketEvent};
use apexes_message_bus::MarketEventBus;
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::any;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    bus: Arc<MarketEventBus>,
    instruments: Arc<Vec<Instrument>>,
}

/// Builds the `axum::Router` this crate's server task serves. Callers
/// (production: `main.rs`; tests: this file and
/// `tests/server_client_roundtrip.rs`) are responsible for binding a
/// listener and calling `axum::serve` themselves -- this function's
/// only job is producing the router, so it stays testable without a
/// real network bind.
pub fn build_app(bus: Arc<MarketEventBus>, instruments: Vec<Instrument>) -> Router {
    let state = AppState {
        bus,
        instruments: Arc::new(instruments),
    };
    Router::new().route("/ws", any(ws_handler)).with_state(state)
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let receiver = match state.bus.subscribe(&state.instruments).await {
        Ok(receiver) => receiver,
        Err(error) => {
            tracing::warn!(?error, "apexes-ui server: bus.subscribe failed, closing socket");
            return;
        }
    };
    let mut event_stream = apexes_message_bus::receiver_into_stream(receiver);
    let (mut sender, _receiver_half) = socket.split();

    while let Some(event) = event_stream.next().await {
        if !matches!(event, MarketEvent::Trade(_) | MarketEvent::Tick(_)) {
            continue;
        }
        let json = match serde_json::to_string(&event) {
            Ok(json) => json,
            Err(error) => {
                tracing::warn!(?error, "apexes-ui server: failed to serialize MarketEvent");
                continue;
            }
        };
        if sender.send(Message::Text(json.into())).await.is_err() {
            tracing::info!("apexes-ui server: client disconnected, stopping forward loop");
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, MarketType, TradeEvent, Venue};
    use apexes_message_bus::test_support::MockMarketDataProvider;
    use rust_decimal_macros::dec;

    fn btc_usdt() -> Instrument {
        Instrument::new(
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
        .unwrap()
    }

    #[test]
    fn build_app_does_not_panic_and_returns_a_router() {
        let mock = MockMarketDataProvider::new();
        let bus = Arc::new(MarketEventBus::new(Box::new(mock)));
        let _router = build_app(bus, vec![btc_usdt()]);
    }
}
```

- [ ] **Step 2: Register the module**

```rust
// crates/apexes-ui/src/lib.rs -- replace the file's content with:
pub mod app_state;
pub mod server;
```

- [ ] **Step 3: Run the test**

Run: `cargo test -p apexes-ui server::tests`
Expected: PASS immediately (this test only checks `build_app` doesn't panic; the real forwarding logic is exercised by Task 4's integration test, since it needs a real bind + real client).

- [ ] **Step 4: Run clippy, fmt, workspace check**

Run: `cargo clippy -p apexes-ui --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-ui/src/server.rs crates/apexes-ui/src/lib.rs
git commit -S -s -m "feat(apexes-ui): WebSocket server task (axum, MarketEventBus-parameterized)"
```

---

### Task 3: WebSocket client task (reconnect/backoff, `ConnectionStatus`)

**Files:**
- Create: `crates/apexes-ui/src/client.rs`
- Modify: `crates/apexes-ui/src/lib.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks except `apexes_core::MarketEvent` (existing).
- Produces: `apexes_ui::client::{ConnectionStatus, BackoffState}` -- `BackoffState::new() -> Self`, `BackoffState::next_delay(&mut self) -> std::time::Duration`, `BackoffState::reset(&mut self)`; `apexes_ui::client::run(url: String, tx: tokio::sync::mpsc::UnboundedSender<MarketEvent>, status: tokio::sync::watch::Sender<ConnectionStatus>)` (the task loop itself, used by `main.rs` in Task 5).

- [ ] **Step 1: Write the failing tests for the backoff state machine**

```rust
// crates/apexes-ui/src/client.rs -- create the file with this content:

//! WebSocket client task: connects to the local server task (Task 2)
//! over `ws://127.0.0.1:<port>/ws`, deserializes each incoming JSON
//! text frame as a `MarketEvent`, and forwards successfully-parsed
//! events into an `mpsc` channel. Reconnects with exponential backoff
//! on any error or unexpected close (mirroring
//! `apexes-market-data/src/mark_price_task.rs`'s existing
//! hand-rolled-reconnect loop shape -- connect, read-loop-until-error,
//! sleep, retry -- but with backoff instead of that file's fixed
//! delay). A malformed/unparseable JSON frame is logged and skipped,
//! not treated as a connection error.

use apexes_core::MarketEvent;
use futures::StreamExt;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionStatus {
    #[default]
    Disconnected,
    Connected,
    Reconnecting,
}

/// Tracks the exponential-backoff delay across reconnect attempts:
/// starts at `INITIAL_BACKOFF`, doubles on each consecutive call to
/// `next_delay`, caps at `MAX_BACKOFF`, and resets back to
/// `INITIAL_BACKOFF` the next time `reset` is called (on a successful
/// connection).
pub struct BackoffState {
    current: Duration,
}

impl Default for BackoffState {
    fn default() -> Self {
        Self::new()
    }
}

impl BackoffState {
    pub fn new() -> Self {
        Self {
            current: INITIAL_BACKOFF,
        }
    }

    /// Returns the delay to sleep before the next reconnect attempt,
    /// then doubles the internal state (capped at `MAX_BACKOFF`) for
    /// the following call.
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current * 2).min(MAX_BACKOFF);
        delay
    }

    pub fn reset(&mut self) {
        self.current = INITIAL_BACKOFF;
    }
}

/// Runs forever: connects to `url`, forwards parsed `MarketEvent`s
/// into `tx`, publishes connection state transitions to `status`, and
/// reconnects with backoff on any error. Returns only if `tx` is
/// closed (the receiving side -- the `egui` app -- was dropped).
pub async fn run(url: String, tx: mpsc::UnboundedSender<MarketEvent>, status: watch::Sender<ConnectionStatus>) {
    let mut backoff = BackoffState::new();
    loop {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((socket, _response)) => {
                tracing::info!(%url, "apexes-ui client: connected");
                let _ = status.send(ConnectionStatus::Connected);
                backoff.reset();
                let (_write, mut read) = socket.split();
                loop {
                    match read.next().await {
                        Some(Ok(Message::Text(text))) => match serde_json::from_str::<MarketEvent>(&text) {
                            Ok(event) => {
                                if tx.send(event).is_err() {
                                    tracing::info!("apexes-ui client: output channel closed, stopping");
                                    return;
                                }
                            }
                            Err(error) => {
                                tracing::warn!(?error, "apexes-ui client: failed to parse MarketEvent JSON, skipping frame");
                            }
                        },
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            tracing::warn!(?error, "apexes-ui client: WebSocket error, reconnecting");
                            break;
                        }
                        None => {
                            tracing::warn!("apexes-ui client: WebSocket stream ended, reconnecting");
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(?error, "apexes-ui client: failed to connect, retrying");
            }
        }
        let _ = status.send(ConnectionStatus::Reconnecting);
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_delay_doubles_each_call_and_caps_at_max_backoff() {
        let mut backoff = BackoffState::new();
        assert_eq!(backoff.next_delay(), Duration::from_millis(500));
        assert_eq!(backoff.next_delay(), Duration::from_millis(1000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(2000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(4000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(8000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(16000));
        assert_eq!(backoff.next_delay(), Duration::from_secs(30)); // capped
        assert_eq!(backoff.next_delay(), Duration::from_secs(30)); // stays capped
    }

    #[test]
    fn reset_returns_the_delay_sequence_to_the_initial_value() {
        let mut backoff = BackoffState::new();
        let _ = backoff.next_delay();
        let _ = backoff.next_delay();
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(500));
    }

    #[test]
    fn connection_status_default_is_disconnected() {
        assert_eq!(ConnectionStatus::default(), ConnectionStatus::Disconnected);
    }
}
```

- [ ] **Step 2: Register the module**

```rust
// crates/apexes-ui/src/lib.rs -- replace the file's content with:
pub mod app_state;
pub mod client;
pub mod server;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p apexes-ui client::tests`
Expected: all 3 tests pass.

- [ ] **Step 4: Run clippy, fmt, workspace check**

Run: `cargo clippy -p apexes-ui --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-ui/src/client.rs crates/apexes-ui/src/lib.rs
git commit -S -s -m "feat(apexes-ui): WebSocket client task with exponential-backoff reconnect"
```

---

### Task 4: Real end-to-end server<->client integration test

**Files:**
- Create: `crates/apexes-ui/tests/server_client_roundtrip.rs`

**Interfaces:**
- Consumes: `apexes_ui::server::build_app` (Task 2), `apexes_ui::client::{run, ConnectionStatus}` (Task 3), `apexes_message_bus::{MarketEventBus, test_support::MockMarketDataProvider}` (existing).
- Produces: nothing consumed by later tasks -- this is a standalone integration test.

- [ ] **Step 1: Write the test**

```rust
// crates/apexes-ui/tests/server_client_roundtrip.rs
//! Real end-to-end integration test: binds a real `127.0.0.1:0`
//! listener (mirroring the `TcpListener::bind("127.0.0.1:0")` pattern
//! already used in `crates/apexes-gateway-ibkr/tests/mock_tws.rs` and
//! `crates/apexes-market-data-chain/src/eth_rpc_provider.rs`), serves
//! the real `apexes_ui::server::build_app` router over it, runs the
//! real `apexes_ui::client::run` client task against that address,
//! and asserts a `MarketEvent` pushed through a mock upstream
//! provider is received end-to-end on the client side. Uses
//! `apexes_message_bus::test_support::MockMarketDataProvider`, not a
//! real Binance connection -- matches this project's established
//! "unit/integration tests never depend on real external services"
//! convention.

use apexes_core::{AssetClass, Instrument, MarketEvent, MarketType, TradeEvent, Venue};
use apexes_message_bus::MarketEventBus;
use apexes_message_bus::test_support::MockMarketDataProvider;
use apexes_ui::client::ConnectionStatus;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

fn btc_usdt() -> Instrument {
    Instrument::new(
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
    .unwrap()
}

#[tokio::test]
async fn a_trade_pushed_upstream_is_received_end_to_end_by_the_client() {
    let mock = MockMarketDataProvider::new();
    let handle = mock.handle();
    let bus = Arc::new(MarketEventBus::new(Box::new(mock)));
    let app = apexes_ui::server::build_app(Arc::clone(&bus), vec![btc_usdt()]);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let (tx, mut rx) = mpsc::unbounded_channel::<MarketEvent>();
    let (status_tx, mut status_rx) = watch::channel(ConnectionStatus::default());
    let url = format!("ws://{addr}/ws");
    tokio::spawn(apexes_ui::client::run(url, tx, status_tx));

    // Wait for the client to report Connected before pushing an
    // event, so the mock provider's subscribe() has definitely been
    // called (which only happens once the server's ws_handler runs).
    loop {
        status_rx.changed().await.unwrap();
        if *status_rx.borrow() == ConnectionStatus::Connected {
            break;
        }
    }

    handle.push_event(MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        price: dec!(65000),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: chrono::Utc::now(),
    }));

    let received = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("should receive an event within 5 seconds")
        .expect("channel should not be closed");

    match received {
        MarketEvent::Trade(trade) => {
            assert_eq!(trade.symbol, "BTCUSDT");
            assert_eq!(trade.price, dec!(65000));
        }
        other => panic!("expected a Trade event, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p apexes-ui --test server_client_roundtrip`
Expected: PASS (may take a moment for the WebSocket handshake to complete, well under the 5-second timeout).

- [ ] **Step 3: Run clippy, fmt, workspace check**

Run: `cargo clippy -p apexes-ui --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-ui/tests/server_client_roundtrip.rs
git commit -S -s -m "test(apexes-ui): real end-to-end server<->client WebSocket roundtrip"
```

---

### Task 5: `egui`/`eframe` UI + `main.rs` assembly

**Files:**
- Create: `crates/apexes-ui/src/ui.rs`
- Create: `crates/apexes-ui/src/main.rs`
- Create: `crates/apexes-ui/tests/ui_renders_without_panicking.rs`
- Modify: `crates/apexes-ui/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_ui::app_state::{AppState, RowState}` (Task 1), `apexes_ui::client::{run, ConnectionStatus}` (Task 3), `apexes_ui::server::build_app` (Task 2).
- Produces: `apexes_ui::ui::DashboardApp` (implements `eframe::App`; also exported so `tests/ui_renders_without_panicking.rs` can construct it directly).

- [ ] **Step 1: Write the failing headless-render test**

```rust
// crates/apexes-ui/tests/ui_renders_without_panicking.rs
//! Drives one `egui_kittest` headless render pass over
//! `apexes_ui::ui::DashboardApp` with synthetic state (a few
//! populated rows plus one still-empty row), asserting no panic --
//! this is the automatable half of this project's two-part UI
//! testing strategy for `egui` code (the other half is the plain
//! `app_state`/`client` unit tests, which need no rendering at all).
//! Visual correctness itself is not automated by this test.

use apexes_ui::app_state::AppState;
use apexes_ui::client::ConnectionStatus;
use apexes_ui::ui::DashboardApp;
use apexes_core::{MarketEvent, TradeEvent, Venue};
use egui_kittest::Harness;
use rust_decimal_macros::dec;

#[test]
fn dashboard_app_renders_one_frame_without_panicking() {
    let symbols = vec![
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "BNBUSDT".to_string(),
    ];
    let mut app_state = AppState::new(&symbols);
    app_state.apply_event(&MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        price: dec!(65000),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: chrono::Utc::now(),
    }));
    // ETHUSDT/BNBUSDT stay at their default (empty) RowState -- the
    // render pass must handle both populated and still-empty rows
    // without panicking.

    let mut harness = Harness::builder().build_eframe(|_cc| {
        DashboardApp::new_for_test(app_state, ConnectionStatus::Connected)
    });

    harness.run();
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p apexes-ui --test ui_renders_without_panicking`
Expected: FAIL to compile with "cannot find module `ui`" / "cannot find function `new_for_test`" -- expected; Step 3 implements them.

- [ ] **Step 3: Implement the `egui`/`eframe` app**

```rust
// crates/apexes-ui/src/ui.rs
//! `eframe::App` implementation: a live-updating table of
//! bid/ask/last-trade price for the hardcoded instrument list, plus a
//! connection-status indicator. Drains any pending `MarketEvent`s
//! from the client task's channel on every `ui()` call and folds them
//! into `app_state`, then requests a repaint shortly afterward so the
//! table keeps updating even with no user input (an `eframe::App`'s
//! `ui()` is otherwise only called in response to user interaction or
//! an explicit repaint request).

use crate::app_state::AppState;
use crate::client::ConnectionStatus;
use apexes_core::MarketEvent;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// How often the app requests a repaint while idle, so newly-arrived
/// `MarketEvent`s (which arrive on a background tokio task, not from
/// user interaction) get picked up and rendered promptly.
const REPAINT_INTERVAL: Duration = Duration::from_millis(200);

pub struct DashboardApp {
    app_state: AppState,
    connection_status: ConnectionStatus,
    event_rx: Option<mpsc::UnboundedReceiver<MarketEvent>>,
    status_rx: Option<watch::Receiver<ConnectionStatus>>,
}

impl DashboardApp {
    /// Production constructor: wires the app up to the real client
    /// task's channels.
    pub fn new(
        symbols: &[String],
        event_rx: mpsc::UnboundedReceiver<MarketEvent>,
        status_rx: watch::Receiver<ConnectionStatus>,
    ) -> Self {
        Self {
            app_state: AppState::new(symbols),
            connection_status: ConnectionStatus::default(),
            event_rx: Some(event_rx),
            status_rx: Some(status_rx),
        }
    }

    /// Test-only constructor: builds the app directly from an
    /// already-populated `AppState` and a fixed `ConnectionStatus`,
    /// with no channels -- used by
    /// `tests/ui_renders_without_panicking.rs` so the headless render
    /// test doesn't need a real client task running.
    pub fn new_for_test(app_state: AppState, connection_status: ConnectionStatus) -> Self {
        Self {
            app_state,
            connection_status,
            event_rx: None,
            status_rx: None,
        }
    }

    fn drain_updates(&mut self) {
        if let Some(event_rx) = self.event_rx.as_mut() {
            while let Ok(event) = event_rx.try_recv() {
                self.app_state.apply_event(&event);
            }
        }
        if let Some(status_rx) = self.status_rx.as_mut() {
            self.connection_status = *status_rx.borrow();
        }
    }
}

impl eframe::App for DashboardApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.drain_updates();
        ui.ctx().request_repaint_after(REPAINT_INTERVAL);

        let (label, color) = match self.connection_status {
            ConnectionStatus::Connected => ("Connected", egui::Color32::GREEN),
            ConnectionStatus::Reconnecting => ("Reconnecting...", egui::Color32::YELLOW),
            ConnectionStatus::Disconnected => ("Disconnected", egui::Color32::RED),
        };
        ui.horizontal(|ui| {
            ui.colored_label(color, "\u{25CF}");
            ui.label(label);
        });

        ui.separator();

        egui::Grid::new("dashboard_grid").striped(true).show(ui, |ui| {
            ui.strong("Symbol");
            ui.strong("Bid");
            ui.strong("Ask");
            ui.strong("Last Trade");
            ui.end_row();

            for (symbol, row) in self.app_state.rows() {
                ui.label(symbol);
                ui.label(row.bid.map_or_else(|| "-".to_string(), |v| v.to_string()));
                ui.label(row.ask.map_or_else(|| "-".to_string(), |v| v.to_string()));
                ui.label(row.last_trade.map_or_else(|| "-".to_string(), |v| v.to_string()));
                ui.end_row();
            }
        });
    }
}
```

- [ ] **Step 4: Register the module**

```rust
// crates/apexes-ui/src/lib.rs -- replace the file's content with:
pub mod app_state;
pub mod client;
pub mod server;
pub mod ui;
```

- [ ] **Step 5: Run the headless render test**

Run: `cargo test -p apexes-ui --test ui_renders_without_panicking`
Expected: PASS.

- [ ] **Step 6: Write `main.rs`**

```rust
// crates/apexes-ui/src/main.rs
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
    if let Ok(value) = std::env::var("APEXES_UI_PORT")
        && let Ok(port) = value.parse::<u16>()
    {
        return port;
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

    let bus = Arc::new(MarketEventBus::new(Box::new(BinanceMarketDataProvider::new())));
    let app = apexes_ui::server::build_app(Arc::clone(&bus), instruments);

    runtime.spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap_or_else(|error| panic!("apexes-ui: failed to bind 127.0.0.1:{port}: {error}"));
        tracing::info!(port, "apexes-ui server: listening");
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
```

- [ ] **Step 7: Add `tracing-subscriber` as a dependency (needed by `main.rs`'s `tracing_subscriber_init`)**

```toml
# Cargo.toml -- add one line to [workspace.dependencies] (this is the
# first crate in the workspace to need tracing-subscriber, so it is
# not yet a workspace dependency; add it alongside the other new
# entries from Task 1 Step 1):
tracing-subscriber = "0.3"
```

```toml
# crates/apexes-ui/Cargo.toml -- add one line to [dependencies] (after
# the `tracing = { workspace = true }` line), referencing the
# workspace entry added above, matching this project's existing
# convention of workspace-level version pinning:
tracing-subscriber = { workspace = true }
```

- [ ] **Step 8: Confirm the binary builds**

Run: `cargo build -p apexes-ui --bin apexes-ui`
Expected: builds successfully with 0 errors (this does not run the binary -- running it would attempt a real Binance connection and open a real window, neither of which belongs in an automated check).

- [ ] **Step 9: Run the full crate test suite, clippy, fmt, workspace check**

Run: `cargo test -p apexes-ui`
Expected: all tests pass (4 `app_state` + 1 `server` + 3 `client` + 1 `server_client_roundtrip` + 1 `ui_renders_without_panicking` = 10 tests).

Run: `cargo clippy -p apexes-ui --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 10: Commit**

```bash
git add Cargo.toml crates/apexes-ui/Cargo.toml crates/apexes-ui/src/ui.rs crates/apexes-ui/src/main.rs crates/apexes-ui/src/lib.rs crates/apexes-ui/tests/ui_renders_without_panicking.rs
git commit -S -s -m "feat(apexes-ui): eframe/egui dashboard UI + main.rs assembly (sub-project #13a)"
```

---

### Task 6: Full-workspace verification checkpoint + final whole-branch review + `docs/FEATURES.md` update

**Files:**
- Modify: `docs/FEATURES.md`

- [ ] **Step 1: Full-workspace verification**

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo test --workspace`
Expected: 0 failed (existing tests unaffected; `apexes-ui`'s 10 new tests included).

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 2: Generate the whole-branch review package and dispatch a final reviewer**

Use the base commit recorded right before Task 1 started (the tip of `master` immediately after this plan document itself was committed) and HEAD (the tip after Task 5). Dispatch a final reviewer (per this project's established subagent-tiering convention, a top-tier profile for final whole-branch reviews) covering:
- Scope match against the design spec: does `main.rs` construct exactly one real `MarketEventBus`/`BinanceMarketDataProvider`, with `server.rs`/`client.rs` genuinely reusable against a mock provider (confirmed by Task 4's test)?
- Safety-critical invariants: is every `Lagged`/parse-error/send-error path logged and non-panicking, consistently, at both the server and client ends?
- Whether the `egui_kittest` headless test and the real WebSocket roundtrip test are genuine, meaningful proofs, not testing something weaker than intended.
- Whether `docs/FEATURES.md` row `9.1` should be marked `🚧 In Progress` (per the design spec §9 -- this phase covers only a slice of the full "real-time market dashboard" row: no depth/order-book visualization, no trade heatmap).

- [ ] **Step 3: Process findings**

Fix any Critical/Important findings directly (the controller applies straightforward fixes itself rather than re-dispatching for trivial issues, re-verifying independently afterward). Accept Minor/advisory findings as documented scope decisions unless the user directs otherwise.

- [ ] **Step 4: Update `docs/FEATURES.md`**

Update row `9.1`'s status to `🚧 In Progress` and its Notes to record precisely what 13a delivers (real-time bid/ask/last-trade table for three hardcoded Binance Spot instruments via a new `apexes-ui` crate: `axum` WebSocket server wrapping a real `MarketEventBus`/`BinanceMarketDataProvider`, `tokio-tungstenite` client with exponential-backoff reconnect, `egui`/`eframe` desktop UI) and what remains open within row 9.1 itself (Level-2 depth visualization, tick-by-tick trade heatmap -- both explicitly deferred to a future UI sub-phase, not this one). Recompute the Summary table's status counts via `grep -c` (never estimated) for each status symbol, subtracting for the legend row and the Summary table's own self-referencing row exactly as done for every prior sub-project's `FEATURES.md` update in this project. Update the "Current active work" narrative paragraph at the bottom of the file to describe sub-project `#13a`'s completion, avoiding any raw status-emoji string inside prose.

Run: `cargo build --workspace` and `cargo test --workspace` one more time after any Step 3 fixes, to reconfirm a clean state before committing.

- [ ] **Step 5: Commit**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: update FEATURES.md row 9.1 (UI foundation -- WebSocket bridge + minimal dashboard, sub-project #13a)"
```

- [ ] **Step 6: Append a `.superpowers/sdd/progress.md` ledger entry**

Follow this project's established ledger format (one entry per task, plus a final summary entry), then declare this plan's work complete.
