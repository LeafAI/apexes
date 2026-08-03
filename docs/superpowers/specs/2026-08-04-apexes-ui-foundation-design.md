# Apexes: UI Foundation (Sub-project #13a) Design

## 1. Context and Scope

Sub-project `#13` (UI) in the roadmap (`docs/superpowers/specs/2026-07-28-apexes-vision-and-roadmap.md` §3, row 13) covers six distinct features tracked in `docs/FEATURES.md` §9 (rows 9.1–9.6): real-time market dashboard, GPU-rendered K-line/tick chart engine, strategy monitoring/tuning panel, order/position/risk management UI, backtest report visualization, and a multi-screen system monitoring wall. This is too broad for a single spec/plan/implementation cycle, so it is split into sequential sub-phases, each with its own design-spec → plan → implementation pass:

| Phase | Scope | FEATURES.md rows | Depends on |
|---|---|---|---|
| **13a (this spec)** | WebSocket bridge + minimal real-time market dashboard | 9.1 (partial) | `#1`, `#12` |
| 13b | GPU K-line/tick chart engine (`wgpu` rendering core) | 9.2 | 13a |
| 13c | Strategy monitoring & live tuning panel | 9.3 | 13a, `#4` |
| 13d | Order/position/risk management UI | 9.4 | 13a, `#7`, `#5`, `#11` |
| 13e | Backtest report visualization | 9.5 | 13a (or standalone) |
| 13f | Multi-screen system monitoring wall | 9.6 | 13a, `#14` (not yet started) |

This spec covers **13a only**: proving the end-to-end pipe from a real market-data source, through a message-bus subscription, through a WebSocket bridge, into a live-updating desktop UI — the foundation every later UI phase builds on. It deliberately does not include any charting, order/risk data, or strategy control surfaces.

## 2. Goals

- Prove a real `apexes-message-bus::MarketEventBus` subscriber can exist outside the trading path (the first genuine "extra consumer" in this project, since sub-project `#12`'s final review noted no production entrypoint yet exercises this).
- Establish the crate/binary/process shape that later UI phases (13b–13f) will extend, without over-building for needs that don't exist yet.
- Ship a visibly working, if minimal, desktop UI: a live-updating table of bid/ask/last-trade price for a few hardcoded instruments.

## 3. Non-Goals (explicitly deferred)

- K-line/tick chart rendering (13b).
- Strategy monitoring/tuning, order/position/risk display, backtest report visualization, system monitoring wall (13c–13f).
- A dedicated UI-layer wire protocol distinct from `apexes_core::MarketEvent` (may be introduced later if/when aggregation or multiple heterogeneous data sources make raw `MarketEvent` JSON insufficient).
- Multi-client support beyond "works, not specifically designed for" (single desktop client is the only exercised case).
- Real two-process deployment (the WebSocket boundary is preserved structurally — see §4 — but is not exercised across an actual process/machine boundary in this phase).
- Configurable instrument selection at runtime (the three demo instruments are hardcoded).

## 4. Architecture

**Single process, single binary (`apexes-ui`), two internal tasks communicating over a real localhost WebSocket connection** — not decoupled processes, but not an in-process channel either. This preserves the protocol boundary (real JSON-over-WebSocket, real TCP loopback) so a future phase can split the server task into its own long-running backend process (serving multiple desktop clients, or clients running on a different machine) without redesigning the wire format, while keeping 13a itself simple to run and test (`cargo run -p apexes-ui`, no separate process to manage).

```
┌─────────────────────────── apexes-ui process ───────────────────────────┐
│                                                                          │
│  ┌──────────────────┐        WS (127.0.0.1:PORT)      ┌──────────────┐ │
│  │   Server task     │ ───────────────────────────────▶│  Client task │ │
│  │  (axum, WS route) │◀─────────────────────────────── │ (tungstenite)│ │
│  └────────┬──────────┘                                  └──────┬───────┘ │
│           │                                                     │        │
│           │ subscribe()                                         │ channel│
│           ▼                                                     ▼        │
│  ┌──────────────────┐                                  ┌──────────────┐ │
│  │  MarketEventBus   │                                  │  egui/eframe │ │
│  │  (wraps a real     │                                  │  app: table  │ │
│  │  BinanceMarketData-│                                  │  view + conn │ │
│  │  Provider)         │                                  │  status      │ │
│  └──────────────────┘                                  └──────────────┘ │
└──────────────────────────────────────────────────────────────────────────┘
```

### 4.1 Server task

- Built with `axum` (new workspace dependency — chosen over hand-rolled `tokio-tungstenite::accept_async`, which the codebase already uses elsewhere for mock test servers and `mark_price_task.rs`'s hand-rolled client, specifically because `axum` leaves room for a REST endpoint or additional routes in a later phase without a framework migration; 13a itself only adds one WebSocket route).
- `server.rs` exposes an app-builder function taking an already-constructed `Arc<MarketEventBus>` and the hardcoded instrument list as parameters (not a function that constructs its own `BinanceMarketDataProvider` internally) -- this is what makes §8.2's integration test possible: the test builds its own `MarketEventBus` wrapping `apexes_message_bus::test_support::MockMarketDataProvider` and passes it to the same app-builder `main.rs` uses, rather than needing a separate test-only code path. `main.rs` is the only place that constructs the real `MarketEventBus::new(Box::new(BinanceMarketDataProvider::new()))`.
- On startup (in `main.rs`, via the app-builder above): calls `bus.subscribe(&instruments)` once for the three hardcoded instruments (`BTCUSDT`, `ETHUSDT`, `BNBUSDT`, all `Venue::Binance`/`AssetClass::Spot`), and holds the resulting `broadcast::Receiver` for the life of the process.
- Binds `127.0.0.1:<port>` (default fixed port; overridable via a `--port` CLI arg and/or `APEXES_UI_PORT` environment variable, CLI arg taking precedence if both are set).
- Per connected WebSocket client: spawns a task that reads from a fresh `bus.subscribe()` call (reusing the same instrument set, so `MarketEventBus`'s connection-reuse guarantee applies — only one upstream Binance connection regardless of client count) and forwards each `MarketEvent::Trade` and `MarketEvent::Tick` as a JSON text frame (`serde_json::to_string(&event)`). Other `MarketEvent` variants (`Depth`, `Bar`, `MarkPrice`) are filtered out before sending, since this phase's UI only displays bid/ask/last-trade.
- A `Lagged` broadcast-receive error is logged via `tracing::warn!` and skipped, matching `apexes-message-bus`'s own established convention (reuses `apexes_message_bus::receiver_into_stream`, filtered by variant afterward) — never a panic, never a silent stop.

### 4.2 Client task

- Connects to `ws://127.0.0.1:<port>` via `tokio-tungstenite::connect_async`, deserializes each incoming text frame as `MarketEvent` (`serde_json::from_str`), and forwards successfully-parsed events into a `tokio::sync::mpsc` channel consumed by the `egui` app's update loop.
- On any connection error or unexpected close, reconnects with exponential backoff (mirroring `apexes-market-data/src/mark_price_task.rs`'s existing hand-rolled-reconnect loop shape -- connect, read-loop-until-error, sleep, retry -- but replacing that file's fixed `RECONNECT_DELAY` with a small self-contained backoff: start at 500ms, double on each consecutive failure, cap at 30s, reset to 500ms on the next successful connection. No existing backoff helper exists elsewhere in the workspace to reuse -- confirmed by grepping `crates/apexes-gateway-binance` and the rest of the workspace for "backoff"/"exponential" -- so this is a new, small, self-contained implementation local to `client.rs`.).
- Exposes a `ConnectionStatus` enum (`Connected`, `Reconnecting`, `Disconnected`) via a shared `Arc<Mutex<..>>` or `tokio::sync::watch`, which the `egui` app polls to render a status indicator.
- A malformed/unparseable JSON frame is logged via `tracing::warn!` and skipped (does not tear down the connection) — the same "never silently stop, never panic" convention.

### 4.3 egui app

- Built with `egui`/`eframe` (new workspace dependency).
- Maintains an in-memory `HashMap<String, RowState>` (keyed by symbol), where `RowState { bid: Option<Decimal>, ask: Option<Decimal>, last_trade: Option<Decimal>, last_updated: DateTime<Utc> }`, updated as `MarketEvent::Tick`/`MarketEvent::Trade` arrive from the client-task channel.
- Renders one table: a row per hardcoded instrument (`BTCUSDT`/`ETHUSDT`/`BNBUSDT`), columns for bid price, ask price, last trade price — cells show "—" until the first event for that symbol arrives.
- Renders the `ConnectionStatus` indicator (e.g. a colored dot + label) above the table.
- No charts, no historical data, no user interaction beyond the window itself.

## 5. Crate and Module Layout

New crate `crates/apexes-ui`, added to the workspace `members` list.

```
crates/apexes-ui/
  Cargo.toml
  src/
    main.rs           -- spawns server task, client task, and eframe::run_native together
    server.rs          -- axum app, WebSocket route, MarketEventBus subscription + forwarding
    client.rs           -- tokio-tungstenite client, reconnect/backoff, ConnectionStatus
    app_state.rs        -- RowState/table-state accumulator (pure logic, no egui/network deps)
    ui.rs               -- egui::App impl: renders app_state + ConnectionStatus
  tests/
    server_client_roundtrip.rs  -- real localhost bind (127.0.0.1:0), real tungstenite client,
                                    asserts JSON round-trips correctly (mirrors the existing
                                    TcpListener::bind("127.0.0.1:0") pattern used in
                                    crates/apexes-gateway-ibkr/tests/mock_tws.rs and
                                    crates/apexes-market-data-chain/src/eth_rpc_provider.rs)
    ui_renders_without_panicking.rs -- egui headless test harness, synthetic app_state,
                                        asserts a render pass completes without panicking
```

`app_state.rs`'s `RowState`/accumulator logic, and `client.rs`'s reconnect/backoff state machine, are the two pieces most worth isolating for fast, dependency-free unit tests — both are plain data transformations with no `egui` or live-network dependency once separated from the task loops that drive them.

## 6. Dependencies (new workspace-level additions)

- `axum` (WebSocket-capable web framework, server task).
- `egui` + `eframe` (native cross-platform GUI rendering, per the roadmap doc's own technology choice in §2.4).
- `egui_kittest` (dev-dependency only, headless rendering test harness -- see §8.3).

No other new dependencies — `tokio-tungstenite` (client side), `serde_json`, `tracing`, `apexes-core`, `apexes-message-bus`, and `apexes-market-data`'s `BinanceMarketDataProvider` are all already available in the workspace.

## 7. Error Handling and Resilience

- Every fallible boundary (WebSocket connect, JSON parse, broadcast-receive) either logs-and-continues (never panics, never silently drops the whole pipeline) or is a startup-time `Result` that exits the process with a clear error message (e.g. failing to bind the configured port).
- The `MarketEventBus`'s existing `Lagged`-is-logged-and-skipped and `SubscriptionFailed`-on-instrument-mismatch behaviors are relied upon as-is; this phase adds no new `ApexesError` variants.
- Reconnect backoff prevents a tight retry loop if the internal server task is ever unreachable (should not normally happen within one process, but the client task treats the WebSocket boundary as a real network dependency regardless).

## 8. Testing Strategy

1. **Unit tests, no I/O:** `app_state.rs`'s accumulator (`MarketEvent` in, `RowState` map updated correctly, unknown symbols ignored) and `client.rs`'s backoff state machine (delay sequence, cap, reset-on-success) — plain function/struct tests.
2. **Integration test, real WebSocket, no GPU:** `tests/server_client_roundtrip.rs` binds a real `127.0.0.1:0` listener (axum test server), connects a real `tokio-tungstenite` client, and asserts a `MarketEvent` sent from a mock upstream is correctly received and JSON-round-tripped end-to-end through both tasks' logic (using a mock/injectable `MarketDataProvider`, e.g. `apexes_message_bus::test_support::MockMarketDataProvider`, not a real Binance connection — mirrors this project's established "unit/integration tests never depend on real external services" convention).
3. **Integration test, egui headless:** `tests/ui_renders_without_panicking.rs` constructs synthetic `app_state` (a few populated `RowState`s plus one still-empty row) and drives at least one render pass using `egui_kittest::Harness` (egui's own official headless test harness crate, confirmed current on docs.rs), asserting no panic — proving the rendering code path is exercised by CI even though visual correctness itself is not automatable. `egui_kittest` is a new workspace dev-dependency (test-only, not a runtime dependency of the `apexes-ui` binary itself).
4. No test depends on a real Binance connection; the `#[ignore]`-gated convention used elsewhere in this workspace (e.g. `apexes-market-data/tests/live_ingestion.rs`) is available if a genuinely-live end-to-end smoke test is wanted later, but is not required for 13a's own definition of done.

## 9. Documentation Honesty

Per this project's established convention, `docs/FEATURES.md` row 9.1 will be updated to reflect exactly what 13a delivers: a real-time bid/ask/last-trade table for three hardcoded Binance Spot instruments, not a full "market dashboard" (no depth/order-book visualization, no trade heatmap — both explicitly still open within row 9.1's own remaining scope, left for a future sub-phase). Row 9.1 should be marked `🚧 In Progress`, not `✅ Done`, reflecting that only a slice of its full description is implemented; the Notes column will state precisely what exists and what remains.
