# Apexes — Execution Plan

> **Purpose.** This file tracks *what we are working on now, what is next, and what is blocked*. It is the execution-level companion to [`FEATURES.md`](FEATURES.md).
>
> | Question | Authoritative file |
> |---|---|
> | Is feature X done? | [`FEATURES.md`](FEATURES.md) (single source of truth for feature status) |
> | What are we doing now / next, and in what order? | **this file** |
> | Why was it designed this way? | `docs/superpowers/specs/` |
> | Step-by-step tasks for one sub-project | `docs/superpowers/plans/` |
> | Long-term vision and roadmap (#0–#15) | [`docs/superpowers/specs/2026-07-28-apexes-vision-and-roadmap.md`](superpowers/specs/2026-07-28-apexes-vision-and-roadmap.md) |
>
> **Maintenance rules**
> 1. Never record feature status here — link to the `FEATURES.md` row ID instead (e.g. "row 9.3").
> 2. Update this file whenever a task finishes, a new plan starts, or priorities change.
> 3. Keep "Now" to one active plan. Finished plans move to "Completed" with their commit range.
> 4. Every plan-level item links to its plan file; per-task detail lives in that plan, not here.

_Last updated: 2026-11-15 (Binance Spot testnet MVP scoped)_

---

## Now — Binance Spot testnet MVP (live on testnet, launched and evaluated from the UI)

**Goal.** One operator can start a Binance **Spot testnet** live session from a UI, watch it, tune it, halt it, and judge the strategy and its risk from a live timeline. Mainnet is an explicit opt-in (see the network switch design) and is not part of this MVP's acceptance.

**Scope note.** "Binance" here means trading **Spot on the Binance exchange** (BTCUSDT-style pairs, real order books). It does not mean BNB Smart Chain or the BNB token. Your Binance app account is the mainnet system and has no testnet switch. The **Spot Test Network** is a separate system at `testnet.binance.vision`: it has its own accounts and API keys (free, created by logging in with GitHub), all funds are virtual and cannot be moved in or out, and it is periodically reset to a blank state (open orders and history are lost, balances re-issued). Prices and order books there are not mainnet's.

**Prerequisite for you:** generate testnet API keys at `https://testnet.binance.vision/`. They are not your app's keys. They go in the engine's environment as `APEXES_CRED_<REF>_API_KEY` / `_API_SECRET`, never in a file or UI field.

**Network switch design (keep it simple).** One enum, `BinanceNetwork { Mainnet, Testnet }`, defined once (in `apexes-gateway-binance`) and the *only* thing that selects endpoints:
- A single function maps the network to every host: spot REST, user-data-stream WebSocket, and public market-data WebSocket. No other code contains a Binance URL; the hardcoded ones in `credentials.rs` and `user_data_stream.rs` are deleted. A test asserts no Binance hostname appears outside that function.
- It is set in exactly **one place per session**: the launcher's Network choice (default: Testnet), which writes `network` into the `LaunchConfig` and passes the same value to the gateway it starts (`--network`). Hand-written configs use the same field.
- **The field is required in live configs, with no default.** A missing value is an error, so mainnet can never be selected by omission. (This breaks the existing example config and Compose file; both get updated.)
- **Mismatch is refused, not trusted.** The gateway announces its network when an engine connects, and the engine exits if it differs from its own. A testnet engine can never trade on mainnet prices by accident, or the reverse.
- **Credentials are per network.** `credentials_ref` resolves to different env vars for each network (for example `APEXES_CRED_<REF>_TESTNET_API_KEY`), so mainnet keys are never loaded for a testnet session.
- **Mainnet needs an explicit act.** The launcher requires the operator to type the word `MAINNET` to start one, and the engine logs the network at startup. Switching between networks is a new session, never a live toggle.
- The network is part of `SessionSnapshot`, so the console banner (task B) shows what the engine is really using, not what the launcher intended.

**Decisions made (2026-11-15):** live on testnet first, no paper-mode work; sessions launched from a UI; Spot only (futures margin/flatten are out of scope); the strategy stays trivial, and the new value is an **evaluation view** (equity/PnL/drawdown timeline plus risk metrics) that works for any strategy.

**This reshapes the #13c plan below.** Its Tasks 9–11 are replaced by tasks B–D here; its Tasks 12–14 (docs, verification, final review) still apply and run last. A spec and a plan under `docs/superpowers/` are still to be written before dispatch.

| # | Task | Notes | State |
|---|---|---|---|
| A | **Network switch.** Implement `BinanceNetwork` as designed above: one endpoint map, required `network` field, gateway/engine mismatch check, per-network credentials. | Today `apexes-engine/src/credentials.rs` hardcodes `api.binance.com` / `fapi.binance.com` and `user_data_stream.rs` hardcodes `wss://stream.binance.com:9443`. Testnet hosts appear only in an `#[ignore]`d test. **Without this, the first live run uses real money.** Spot testnet REST is `https://testnet.binance.vision`. | ⬜ |
| A2 | **Testnet market data.** Per decision 5: spike a `BinanceServerSpotTestnet` type, then route the gateway's provider by `BinanceNetwork`. Depth is unavailable on testnet. | Confirmed from Binance's docs: testnet has its own public streams (`wss://stream.testnet.binance.vision/ws` and `/stream`) and REST (`https://testnet.binance.vision/api`). Still unconfirmed: whether `barter-data` (behind `BinanceMarketDataProvider`) can be pointed at them. If not, a small testnet-capable provider is needed. If we feed mainnet prices to a testnet account, testnet books differ and fills will not match, so the UI would have to say so. Prefer testnet data for testnet sessions. | ⬜ open |
| A3 | **Mandatory live risk limits.** Make `RiskLimits` required in the live `LaunchConfig`, register the `CompositeRiskChecker` in the engine, refuse to start without it. | Per the finding above. Add tests proving an over-limit order from a strategy is rejected in a live session. | ⬜ |
| B | **Environment visibility.** Put the environment (`TESTNET` / `MAINNET LIVE`), session id and strategy in the snapshot, and show an unmissable banner in the console. | Safety requirement: the console is the last check before real orders. | ⬜ |
| C | **Evaluation timeline.** Engine keeps a bounded, downsampled equity history and computes the decision-7 metrics; the console plots it with `egui_plot` and shows limit utilisation. | Build on `apexes-backtest::performance::PerformanceTracker` (equity curve, summary). Keep history engine-side so a late or reconnecting console gets it, and keep snapshot size bounded. Decide: add `egui_plot` (new dependency, none today) or reuse `apexes-chart`. | ⬜ |
| D | **Supervisor and launcher.** New `apexes-supervisor` crate (spawn detached, discover via PID files, stop, respawn after repeated failure, token and credential handling per decisions 2 and 3) plus a launcher view in `apexes-ui`. | Launcher fields: network (default Testnet, typing `MAINNET` required for mainnet), strategy, instruments, the risk limits, snapshot interval. The existing PID-file formats and the plan's earlier supervisor brief are the starting point. | ⬜ |
| E | **Sample strategy.** A deliberately trivial, parameterised Spot Drift Script with small fixed size and a position cap. | Demo only, not a trading recommendation. Exercises hot-reloaded `param()` inside `on_market_event`. | ⬜ |
| F | **Testnet acceptance run.** Startup recovery, a halt, a stale-feed halt, resume, stop, restart-and-reconcile, and an unclean engine kill, all on testnet. | Spot flatten does not sell balances (by design; see runbook), so "halt" means cancel-all plus no new orders. Plan for a testnet reset mid-run: local OMS state will then disagree with an empty exchange, so check that startup recovery handles it safely and does not trade on stale local orders. | ⬜ |
| G | **Docs.** Runbook section for testnet, `FEATURES.md` rows (9.3, 9.4, 10.2, 11.2 and 8.3 slice), README status, then #13c Tasks 12–14. | | ⬜ |

**Decisions made on best practice (2026-11-15, delegated by the owner)**

1. **Crates.** `apexes-ui` stays the one GUI crate (market dashboard and operator console binaries). All process management goes in a new **`apexes-supervisor`** library crate with no GUI dependencies, so it is unit-testable and the GUI stays a thin layer over it. The launcher and evaluation views are new modules in `apexes-ui`, shown in the operator console binary. *If you meant a brand-new GUI crate with a different name, say so; this is cheap to change before any code exists.*
2. **Engines outlive the GUI.** Engines are spawned detached (own process group), so closing or crashing the GUI never stops a live session. On startup the GUI rediscovers running engines from their PID files and reconnects. A live trading process must never depend on a window being open.
3. **Secrets.** Never typed into a UI field or put on a command line. The supervisor forwards only the named `APEXES_CRED_<REF>_<NETWORK>_*` variables from its own environment and refuses to launch (naming the missing variable) if they are absent. It generates a random per-session control token, passes it to the engine by environment, and stores it in a `0600` file under `~/.apexes/sessions/` so a restarted GUI can reconnect.
4. **`network` is a required field** with no default (see the switch design above).
5. **Testnet market data.** Testnet sessions use testnet market data, never mainnet prices. Reading `barter-data` 0.11's source (not yet compiled against): its `ExchangeServer` trait is public and `Binance<Server>` is generic, with trades and top-of-book (L1) generic over the server, so a small `BinanceServerSpotTestnet` type pointing at `wss://stream.testnet.binance.vision/ws` should work. **Order-book depth (L2) is implemented only for the concrete mainnet type, so depth events are not available on testnet** without writing our own. Live trading on Spot does not need depth, so this is accepted and shown in the UI. Verify with a compile-and-connect spike as the first step of task A2.
6. **Plotting:** add `egui_plot`. It is the standard companion to `egui`, `apexes-chart` is a market-candle renderer rather than a metrics plotter, and `egui_plot` 0.35 matches the workspace's `egui` 0.35. Confirm it resolves with the workspace's `egui` at implementation time.
7. **Evaluation metrics** (kept small and standard): equity curve; realised and unrealised PnL; current and maximum drawdown; gross exposure as a fraction of equity; fees paid; trade count and turnover; time in market; and each risk limit's utilisation (how close the session is to it). Computed engine-side from fills so a late-connecting console gets the full history. History is bounded and downsampled.
8. **Mandatory pre-trade risk limits for live (new task A3).** See the finding below.

**Finding that changes the plan: a live engine currently runs with no pre-trade risk limits.** `apexes-live-trading` treats a missing `CompositeRiskChecker` as "no risk checking configured", and `apexes-engine` registers only the execution gateway and market-data buses, so nothing registers one. Best practice for a system meant to hold real money is that limits are mandatory and conservative by default, so they become a required part of the live `LaunchConfig`: maximum order notional, maximum position per symbol, a minimum account equity floor and a daily trade-count cap, all from the existing `apexes-risk` `RiskLimits`. The launcher pre-fills conservative values (scaled to the instrument and quote currency), and the engine refuses to start a live session without them. Testnet uses the same path so the acceptance run exercises the real thing. This is a check I made by grep, so confirm it by reading `main.rs` before starting task A3.

**Testing strategy (owner's instruction, 2026-11-15).** Build all of A–E first, then run one comprehensive end-to-end verification (task F), instead of a full test pass after every small task. Two limits on that, so it stays safe:
- *Inner loop:* each change still has to compile and keep `cargo clippy` and `cargo fmt` clean, and carries its own focused unit tests written alongside the code. Those are cheap and catch most defects where they are introduced.
- *Bug-prone integration points get checked when built, not at the end:* the engine/gateway network-mismatch refusal, the mandatory-risk-limit rejection, and the supervisor's detached spawn and token handling. A late failure in these would otherwise be expensive to untangle.
- The final e2e is the gate for completion: full workspace build, test, clippy and fmt, then the testnet acceptance run in task F, then a whole-branch review. Nothing is called done before it passes.
- **Testnet keys are needed from the owner before task F** (see the prerequisite above). Everything before F is built and unit-tested without them.

**Still open for the owner:** nothing blocking. Revisit the crate naming in decision 1 and the metric list in decision 7 if they do not match what you want.

---

## Reshaped — remainder of #13c Strategy Monitoring & Live Tuning

- **Plan:** [`plans/2026-08-05-apexes-13c-strategy-monitoring-tuning.md`](superpowers/plans/2026-08-05-apexes-13c-strategy-monitoring-tuning.md) (14 tasks)
- **Spec:** [`specs/2026-08-05-apexes-13c-strategy-monitoring-tuning-design.md`](superpowers/specs/2026-08-05-apexes-13c-strategy-monitoring-tuning-design.md)
- **FEATURES rows:** 9.3 (strategy monitoring & live tuning panel; now 🚧 In Progress), plus the minimum viable slice of 8.3 (cross-process market-data gateway; still Not Started in the table)
- **Reconciliation after PR #9 (`e09cadd`):** a separate effort landed on `master` after Task 8. It delivered a standalone **`apexes-operator`** console (a second binary in the `apexes-ui` crate) covering most of the monitoring/tuning/control half of Task 11 and part of row 9.4, plus engine/gateway authentication, a kill switch, telemetry, CI, Docker and a runbook. The 13c plan was written before this and **has not been revised**; Tasks 9, 10 and 11 below must be re-scoped before dispatch (see "Decisions before resuming").
- **Architecture:** four OS processes — shared market-data gateway, one `apexes-engine` per session (backtest / paper / live), and `apexes-ui` as a thin supervisor and renderer. Diagram: [`specs/assets/2026-08-05-apexes-13c-communication-model-v2-multiprocess.svg`](superpowers/specs/assets/2026-08-05-apexes-13c-communication-model-v2-multiprocess.svg). The superseded in-process v1 diagram is kept for history.

| Task | Scope | State |
|---|---|---|
| 1 | `Strategy::current_params` / `set_param`; Drift Script `param()` hot-reload | ✅ done (`ea32bff`) |
| 2 | `ControlHandle` / `SessionSnapshot` / `SessionCommand` in the unified loop | ✅ done (`cf3022a`) |
| 3 | `core_affinity` dependency; gateway crate scaffold and broadcast core | ✅ done (`0caccd6`) |
| 4 | Gateway replay and async persistence (reuses `apexes-storage`) | ✅ done (`2b2d0e0`) |
| 5 | Gateway `main.rs`: CLI, CPU affinity, PID file, `/market` WebSocket | ✅ done (`ae4b2a9`) |
| 6 | `apexes-engine` scaffold: `LaunchConfig`; `apexes-strategy-loader` | ✅ done (`2c9a0ed`) |
| 7 | Engine `main.rs`: 3-mode dispatch, control wiring, credentials | ✅ done (`7250feb`, patch `dc11783`) |
| 8 | Engine `/session/ws` protocol and server | ✅ done (`f2d9ec2`, patch `f459d00`) |
| 9 | `apexes-ui` process supervisor (spawn/discover gateway, spawn engines, reconnect → respawn) | ⏭ next — brief written, not dispatched; needs re-scoping |
| 10 | `apexes-ui` session launcher tab | ⬜ not built (the `9.3` Notes in `FEATURES.md` list it as not done) |
| 11 | `apexes-ui` monitoring/tuning tab; `/session/ws` client extension | 🟡 mostly superseded: `apexes-operator` already does monitoring, `SetParam`, pause/resume/stop and a `/session/ws` client with auth. Automatic engine respawn is not built. |
| 12 | Docs: Drift Script `param()` hot-reload authoring convention | ⬜ |
| 13 | Full-workspace verification checkpoint | ⬜ |
| 14 | Final whole-branch review; `FEATURES.md` update (9.3, 8.3, recomputed counts); refresh `PLAN.md`, `README.md` status and `AGENTS.md` | ⬜ |

**Rough remaining effort (Tasks 9–14):** the earlier 4–6 hour estimate assumed the original plan. Task 11 has shrunk because of the operator console, but re-scoping Tasks 9–11 adds design work. Re-estimate once the decisions below are made.

**Decisions before resuming (Tasks 9-11)**
- **Where does the supervisor and launcher live?** The operator console was deliberately made a *separate binary* so it survives market-data failure, and `apexes-ui` (the dashboard) exits when its market-data subscription fails. Options: extend `apexes-operator` with the launcher and supervisor, keep them in `apexes-ui`, or add a third binary. This also decides what Task 11 still needs to build.
- **Engine invocation changed.** Engines now require control tokens for live (`APEXES_CONTROL_TOKENS` / `APEXES_CONTROL_TOKEN`) and the gateway takes `APEXES_GATEWAY_TOKEN`. A supervisor that spawns them must generate or pass these without leaking them on the command line. The Task 9 brief predates this and must be updated.
- `apexes-ui` depends on `apexes-strategy-loader`, not on the `apexes-engine` binary crate. Where `LaunchConfig` lives (engine lib vs. a shared crate) must be settled before dispatch. Note `LaunchConfig` gained `stale_data_timeout_secs` in PR #9.
- A cross-platform PID-liveness check needs a new dependency (e.g. `sysinfo`) or a Unix-only syscall. macOS/Linux is the de facto platform scope; document it rather than silently skipping Windows.
- **Task 14's final review checklist** must also cover PR #9's changes (the plan's "credentials never leave the engine" check now includes the token and audit-log paths).

---

## Next (not yet planned)

Ordered by the roadmap's "vertical slice first" principle. None of these has a spec yet; each starts with its own brainstorming → spec → plan cycle.

| Priority | Item | FEATURES rows | Notes |
|---|---|---|---|
| 1 | Remaining UI panels (#13) | 9.4, 9.5, 9.6 | 9.4 has an MVP in `apexes-operator`; still missing order entry/cancel, risk-limit editing and a multi-session view. 9.5 (backtest report) and 9.6 (monitoring wall) are not started. |
| 2 | Full message bus and relational storage (#12) | 8.2, 8.3, 8.4 | 13c delivers only a minimal gateway slice of 8.3. NATS JetStream or a lock-free ring buffer is still open. |
| 3 | Monitoring and ops (#14) | 10.1–10.4 | **Table lags the code:** PR #9 added Prometheus metrics, alert rules, a Grafana dashboard, CI and container images, which touch row 10.2 (CI, images and the runbook are deployment groundwork, not row 10.4's canary release and rollback). Audit row 10.2 and update it. OpenTelemetry tracing (10.1) and HA/DR (10.3) are not started. |
| 4 | Security and compliance (#15) | 6.3, 11.1–11.4 | **Table lags the code:** PR #9 added named operator/viewer bearer tokens and an audit log (touches 11.2). Credentials still come from environment variables; KMS/HSM, TLS/mTLS and compliance reporting are not started. |
| 5 | Market gateways beyond crypto (#10) | 1.1.2, 1.2.x, 1.3.x, 1.4.x, 5.3.2, 5.3.3 | Rows are *In Progress* in `FEATURES.md` with explicit scope boundaries in their Notes. |

---

## Carry-forward technical debt

Found during reviews and deliberately deferred. Close them in the 13c final review (Task 14) or schedule them as their own tasks.

- **Flaky test:** `apexes-live-trading::order_executor_adapter::tests::order_manager_executor_open_orders_reflects_submitted_orders` fails intermittently under parallel test threads; passes with `--test-threads=1`.
- `StrategyError::UnknownParam` is currently unused.
- `apexes-market-data-gateway`: `PersistenceBuffer::BatchVariant` duplicates a private type in `apexes-storage`.
- `Gateway::attach_persistence` can silently replace an existing buffer.
- `apexes-backtest/src/unified_loop.rs` is about 823 production lines, over the 800-line guideline; consider splitting.
- PyO3 bindings (part of row 2.1's full scope) remain deferred and unscheduled.
- **`FEATURES.md` rows 10.2 and 11.2 are stale relative to the code** (PR #9 updated only 9.3 and 9.4), and row 8.3 will be stale for the 13c gateway until Task 14 (see "Next"). No `docs/superpowers/` spec or plan documents PR #9's design.

---

## Completed plans

| Plan | Scope | Commits |
|---|---|---|
| Trading-loop unification | `run_backtest`, `run_paper_trading` and `run_live_trading` share one `run_unified_event_loop` via the `OrderExecutor`, `LiquidationPolicy` and `SupplementalFillSource` traits | `d413455`..`4fcf081` |
| #13c design and diagrams | Spec, 14-task plan, v1 and v2 architecture diagrams | `638f53a` |
| Production-readiness work (PR #9, merged separately) | Kill switch, startup recovery, crash-safe OMS persistence, resilient Binance streams, bearer-token auth and audit log, telemetry, CI, Docker, runbook, `apexes-operator` console | `e30d4a6`..`64c4360`, merge `e09cadd` (no plan file under `docs/superpowers/plans/`) |

Earlier sub-projects (#0–#12 partial and the plans under `docs/superpowers/plans/`) are recorded in [`FEATURES.md`](FEATURES.md); their plan files are the historical record.

---

## How work is executed

Each plan is run with subagent-driven development: one implementer subagent per task, then an independent reviewer subagent. The controller verifies claims against the real code before dispatch, and any gap found is fixed immediately rather than deferred. Commits are signed (`git commit -S -s`) and use conventional-commit messages. A per-task ledger is kept under `.superpowers/sdd/` (local working state, not part of the published docs).
