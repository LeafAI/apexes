# Apexes

**A multi-market automated trading system written in Rust, built to trade real money.**

Apexes is meant to execute real trades on crypto exchanges, China A-shares, Hong Kong H-shares and US equities — with built-in strategies or with strategies you write and tune yourself. Because the end goal is unattended trading with real funds, correctness of the risk engine, the order lifecycle and backtest/paper/live parity is treated as a hard requirement, not a nice-to-have.

> **Status: under active development, pre-release.** Much of the stack is implemented and tested, but nothing here has been validated for production use with real funds. See [Status](#status) for what exists and what does not.

---

## Vision

1. **Trade real markets** — crypto (CEX and DEX), A-shares (SSE/SZSE), H-shares (HKEX), US equities (NASDAQ/NYSE).
2. **Make money with controlled loss** — through built-in strategies, and through user-defined strategies and risk rules built on the strategy SDK.
3. **Be safe enough to hold real capital** — low latency, high availability, strong risk controls, and security/compliance appropriate to real regulatory exposure.

### Design principles

- **Same code for backtest, paper and live.** One strategy trait, one matching kernel, one set of risk rules. All three modes run through a single event loop (`run_unified_event_loop`), so a strategy cannot behave differently in production than it did in testing.
- **Unified multi-market model.** `Instrument`, `Order` and `Fill` hide the differences between exchanges, broker counters and protocols; those differences live in gateway adapters.
- **Free first, paid pluggable.** Every external data source and trading channel sits behind a provider trait. Free sources are the default; paid ones are a configuration change.
- **Low latency.** The hot path is in-memory Rust with no GC pauses; sessions run in separate processes with optional CPU affinity.
- **Degrade, never silently drop orders.**

---

## Architecture

Apexes is a Cargo workspace. At runtime it is a small set of cooperating OS processes:

```
                    ┌──────────────────────────┐
  Binance  ───────▶ │ apexes-market-data-      │──▶ ClickHouse (persist + replay)
  (WebSocket)       │ gateway   (1 shared)     │
                    └────────────┬─────────────┘
                                 │  WS  /market?replay_from=…
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
      ┌───────────────┐  ┌───────────────┐  ┌───────────────┐
      │ apexes-engine │  │ apexes-engine │  │ apexes-engine │   one process per session:
      │  (backtest)   │  │   (paper)     │  │    (live)     │   exactly one of
      └───────┬───────┘  └───────┬───────┘  └───────┬───────┘   run_backtest / run_paper_trading /
              │  WS  /session/ws │ snapshots ↑ commands ↓      run_live_trading
              └──────────────────┼──────────────────┘
                                 ▼
            ┌───────────────────┐      ┌───────────────────┐
            │  apexes-operator  │      │     apexes-ui     │
            │ (operator console)│      │ (market dashboard)│
            └───────────────────┘      └───────────────────┘
              egui clients. The operator console connects to one engine, shows its state
              (equity, positions, orders, params) and sends pause / resume / stop /
              set-param / halt / flatten. It is a separate binary so it keeps working
              when market-data connectivity is what is broken.
```

> **Launching:** `apexes-operator` starts the gateway and an engine itself (as detached processes, so closing the window never stops a session) and finds running sessions again on the next start. Automatic respawn after repeated failure is deliberately not built: a dead live engine needs a human to look at why. Docker Compose is the alternative for a server deployment.

- **`apexes-market-data-gateway`** holds the single real exchange connection, fans events out to subscribers, persists them via `apexes-storage`, and can replay history.
- **`apexes-engine`** runs one trading session. It reads its own credentials, can pin its threads to CPU cores, and exposes a `/session/ws` endpoint streaming throttled state snapshots and accepting control commands.
- **`apexes-operator`** and **`apexes-ui`** are thin egui clients; heavy work never runs in a UI process. `/session/ws` and `/metrics` use bearer-token auth (named operator/viewer roles); the gateway's `/market` does when `APEXES_GATEWAY_TOKEN` is set. A live engine refuses to start without tokens, and every control command is audited.

The full communication model is in [`docs/superpowers/specs/assets/2026-08-05-apexes-13c-communication-model-v2-multiprocess.svg`](docs/superpowers/specs/assets/2026-08-05-apexes-13c-communication-model-v2-multiprocess.svg).

### Crates

| Area | Crates |
|---|---|
| Foundation | `apexes-core` (Instrument, Order/Fill, MarketEvent, errors), `apexes-cli` |
| Market data | `apexes-market-data` (Binance), `-chain` (DEX / EVM RPC), `-ashare`, `-hshare`, `-usequity`, `apexes-market-data-gateway` |
| Storage and bus | `apexes-storage` (ClickHouse), `apexes-message-bus` |
| Strategy | `apexes-strategy` (trait SDK + Drift Script), `apexes-indicators`, `apexes-optimize`, `apexes-strategy-registry` (versioning, canary), `apexes-strategy-loader` |
| Execution modes | `apexes-backtest` (also hosts the unified event loop), `apexes-paper`, `apexes-live-trading`, `apexes-engine` |
| Risk and orders | `apexes-risk`, `apexes-oms` (incl. kill switch), `apexes-ems`, `apexes-account` |
| Ops | `apexes-security` (bearer tokens, audit log), `apexes-telemetry` (Prometheus metrics, tracing) |
| Gateways | `apexes-gateway-binance`, `apexes-gateway-xtp`, `apexes-gateway-ibkr` |
| UI | `apexes-ui` (market dashboard; also hosts the `apexes-operator` console binary), `apexes-chart` |

### Strategies

Strategies are written either as Rust types implementing the `Strategy` trait or in **Drift Script**, a scripting language with `indicators.*` and `signals.*` bindings. Tunable values are declared with `param(name, default)`; for live tuning to take effect, call `param()` inside `on_market_event` so the value is re-read on every event.

---

## Status

[`docs/FEATURES.md`](docs/FEATURES.md) is the authoritative, row-by-row feature table (103 rows; currently 42 done, 17 in progress, 32 planned, 11 not started, 1 deferred). [`docs/PLAN.md`](docs/PLAN.md) tracks what is being built now and next.

**Implemented**
- Binance Spot market data, ClickHouse historical storage, tick-level backtesting with matching simulation and cost models.
- Strategy SDK, indicator library, signal combination, parameter optimization, strategy versioning and canary comparison.
- Pre-trade and in-trade risk controls, margin/leverage monitoring, extreme-market circuit breakers with automatic de-risking.
- Paper trading and a live Binance trading path sharing one event loop with backtesting; OMS, EMS (execution algorithms) and multi-account tracking.
- An in-process market-event bus, a market-data gateway with persistence and replay, a multi-process session engine, and an egui dashboard foundation with a chart engine.
- Live-trading safety and operations groundwork: a kill switch with cancel-all and flatten, a stale-market-data watchdog, startup recovery from exchange state, crash-safe order persistence, authenticated and audited control endpoints, Prometheus metrics with alert rules and a Grafana dashboard, container images, CI, and an operations runbook ([`docs/RUNBOOK.md`](docs/RUNBOOK.md)). Several of these map to `FEATURES.md` rows 10.x and 11.x that are still marked Not Started, so the table lags the code here.

**In progress**
- Quantitative trading enhancement: reproducible strategy baseline, followed by execution reliability, accounting, realistic backtesting and Drift runtime improvements. See [feature plan](docs/superpowers/plans/2026-11-25-apexes-quantitative-enhancement.md).
- Binance Spot **testnet** trading from the GUI (rows 9.3, 9.4): built, and verified end to end against the real Spot Test Network (launch, authenticated console, halt, stop), but not yet exercised by a human clicking through the window, with a real strategy-driven trade, or through a testnet reset. See [`docs/PLAN.md`](docs/PLAN.md) for exactly what is and is not verified. **Mainnet has never been run.**
- A-share, H-share, US-equity and DEX market data and gateways — scaffolded, with explicit scope limits recorded in `FEATURES.md`.

**Not started**
- Backtest report visualization, the system monitoring wall, and order entry/cancel from the UI.
- Distributed tracing (OpenTelemetry), high availability and disaster recovery.
- KMS/HSM key management, TLS/mTLS, regulatory reporting, post-trade compliance.
- A cross-machine message bus (NATS JetStream or a ring buffer; the current bus is in-process only), general PostgreSQL storage, and a shared cache.

**Platform scope:** developed and tested on macOS and Linux.

---

## Getting started

**Prerequisites:** a recent stable Rust toolchain (the workspace uses edition 2024) and Docker for the databases.

```sh
# Start ClickHouse and PostgreSQL
docker compose up -d

# Build, lint, and test everything
cargo build --workspace
cargo clippy --workspace --all-targets
cargo test --workspace
```

Tests that need external services are marked `#[ignore]`. Most need the databases from `docker-compose.yml`; the Binance gateway's live tests also need exchange testnet access. Run them explicitly with `cargo test -p <crate> -- --ignored`.

**Run a testnet session** (shortest path: `scripts/run-testnet.sh`, see [`USAGE.md`](USAGE.md)) (the Spot Test Network has virtual funds and its own keys from <https://testnet.binance.vision/>; full steps in [`docs/RUNBOOK.md`](docs/RUNBOOK.md)):

```sh
docker compose up -d postgres
export APEXES_CRED_BINANCE_MAIN_TESTNET_API_KEY=...      # never typed into the app
export APEXES_CRED_BINANCE_MAIN_TESTNET_API_SECRET=...
cargo build --release -p apexes-engine -p apexes-market-data-gateway -p apexes-ui
target/release/apexes-operator                           # launcher + console
```

`apexes-operator --engine ws://host:port/session/ws` opens just the console for an engine started elsewhere. For a server deployment use Docker Compose (`APEXES_NETWORK=testnet docker compose --profile live up -d`) with an engine launch config such as [`deploy/launch-config.example.json`](deploy/launch-config.example.json).

Live sessions **require** a `network` (`testnet`/`mainnet`, no default) and pre-trade `risk_limits` in the launch config, read credentials from `APEXES_CRED_<REF>_<NETWORK>_API_KEY` / `_API_SECRET`, and require control tokens (`APEXES_CONTROL_TOKENS` or `APEXES_CONTROL_TOKEN`; the launcher generates its own). Never commit credentials. Binance retired its old listenKey user-data stream in February 2026; this system uses the WebSocket-API subscription, which works with HMAC keys.

---

## Documentation

| Document | What it covers |
|---|---|
| [`docs/FEATURES.md`](docs/FEATURES.md) | Feature-level status — single source of truth |
| [`docs/PLAN.md`](docs/PLAN.md) | Current work, next steps, technical debt |
| [`docs/RUNBOOK.md`](docs/RUNBOOK.md) | Deploying and operating the live engine: kill switch, operator console, monitoring, audit log |
| [`AGENTS.md`](AGENTS.md) | Rules and architecture invariants for developers and coding agents |
| [`docs/superpowers/specs/2026-07-28-apexes-vision-and-roadmap.md`](docs/superpowers/specs/2026-07-28-apexes-vision-and-roadmap.md) | Goals, overall design, tech choices, sub-project roadmap #0–#15 (Chinese) |
| `docs/superpowers/specs/` | Per-sub-project design specs |
| `docs/superpowers/plans/` | Per-sub-project implementation plans |

## Contributing

Each sub-project goes through design → spec → plan → implementation. Use conventional commits (`feat:`, `fix:`, `docs:` …), sign commits (`git commit -S -s`), keep `cargo clippy` and `cargo fmt` clean, and update the relevant `FEATURES.md` rows when a sub-project's status changes.

## License

MIT, as declared in the workspace [`Cargo.toml`](Cargo.toml). The repository does not yet contain a `LICENSE` file.
