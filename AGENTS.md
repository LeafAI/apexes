# AGENTS.md — Guide for developers and coding agents

Read this before changing anything. It records the rules that are easy to break and expensive to discover. For what the project is, see [`README.md`](README.md); for what is being built now, see [`docs/PLAN.md`](docs/PLAN.md).

## 1. Orient yourself first

| You need to know… | Read |
|---|---|
| Vision, scope, sub-project roadmap #0–#15 | `docs/superpowers/specs/2026-07-28-apexes-vision-and-roadmap.md` (Chinese) |
| Is feature X done? | `docs/FEATURES.md` — **single source of truth** for feature status |
| What is in flight, what is next, known debt | `docs/PLAN.md` |
| Why a subsystem is designed the way it is | `docs/superpowers/specs/<date>-apexes-<name>-design.md` |
| The task list for a sub-project | `docs/superpowers/plans/<date>-apexes-<name>.md` |
| Runtime process architecture | `docs/superpowers/specs/assets/2026-08-05-apexes-13c-communication-model-v2-multiprocess.svg` (the `-v1-inprocess-superseded` file is history; do not implement against it) |

Do not keep feature status anywhere else (no parallel TODO lists). `FEATURES.md` holds status, `PLAN.md` holds sequencing.

## 2. Build, test, lint

```sh
cargo build --workspace
cargo test --workspace -- --test-threads=1
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

- All four must be clean before a task is considered done. Use `-p <crate>` for a fast inner loop.
- Run the full suite with `--test-threads=1`. One test, `apexes-live-trading::order_executor_adapter::tests::order_manager_executor_open_orders_reflects_submitted_orders`, is flaky under parallel threads (tracked in `PLAN.md`). Do not "fix" it by loosening assertions.
- Tests that need ClickHouse, PostgreSQL or Binance testnet are `#[ignore]`. Start databases with `docker compose up -d`, then run `cargo test -p <crate> -- --ignored`. An ignored test that you did not run is not a verified test; say so in your report.
- Workspace edition is 2024. All crates use it.
- CI (`.github/workflows/ci.yml`) pins the Rust toolchain (currently 1.99) and runs with `RUSTFLAGS=-D warnings`, `--locked`, and ClickHouse/Postgres service containers for the `#[ignore]`d tests. Match it locally; the Dockerfile's `RUST_VERSION` must be bumped together with CI. `apexes-chart`'s render tests need a GPU adapter and are excluded in CI.

## 3. Code rules

- **No `unsafe`.** In edition 2024 `std::env::set_var`/`remove_var` are unsafe, so tests must never call them. Inject a lookup closure instead (see `apexes-engine`'s credential tests).
- **No bare `.unwrap()` / `.expect()` in production code.** Tests may use them. Propagate errors with context; use `thiserror` for error types.
- Use `tracing`, not `println!`, for logging. Use `tokio` for async.
- Prefer immutability: build new values rather than mutating shared ones.
- Keep files under about 800 lines and functions under about 50. (`apexes-backtest/src/unified_loop.rs` is already over; do not make it worse — see `PLAN.md`.)
- Tests live in a `#[cfg(test)] mod tests` block in the same file, or under the crate's `tests/`.
- Do not add comments that restate the code. Do add module docs that say what a crate or module covers and which `FEATURES.md` row it implements, and state scope boundaries honestly.

## 4. Architecture invariants

These are load-bearing. A change that violates one needs a design discussion first.

1. **One trading loop.** `run_backtest`, `run_paper_trading` and `run_live_trading` all drive `apexes_backtest::unified_loop::run_unified_event_loop`. Mode differences are expressed only through the `OrderExecutor`, `LiquidationPolicy` and `SupplementalFillSource` traits. Never copy loop logic into a mode-specific runner; backtest/paper/live parity is a core safety property.
2. **`control: None` is a no-op.** `UnifiedLoopConfig.control: Option<ControlHandle>` set to `None` must behave exactly as before the field existed. The same "absent = previous behaviour" rule applies to other optional config (`risk_checker`, `market_condition_config`).
3. **Pause skips only `strategy.on_market_event`.** Funding, margin status, liquidation, de-risking and equity recording still run while paused.
4. **Credentials never cross a process boundary over the network.** `LaunchConfig.credentials_ref` is a name only. `apexes-engine` is the only place that resolves secrets (`APEXES_CRED_<REF>_API_KEY` / `_API_SECRET`) and builds gateway configs. `apexes-gateway-binance` never reads `std::env`. Never log a secret, put it in a `SessionSnapshot`, or send it over `/session/ws` or `/market`.
5. **Process model.** `apexes-market-data-gateway` (one shared instance, owns the real exchange connection), one `apexes-engine` per session, and thin egui clients (`apexes-ui` market dashboard, `apexes-operator` console). `apexes-operator` hosts the launcher: it uses the `apexes-supervisor` library to start the gateway and engines as detached processes (they must outlive the GUI), to rediscover them from their PID files, and to stop them through `/session/ws`. UI crates must not link against the `apexes-engine` binary crate; shared code goes in library crates (e.g. `apexes-strategy-loader`, `apexes-security`).
6. **PID files** (gateway: `--pid-file`, which the supervisor sets to `~/.apexes/gateways/<network>-<id>.pid` (default for a hand-run gateway: `~/.apexes/market-data-gateway-<network>.pid`); engine: `~/.apexes/sessions/<session_id>.pid`) are two newline-terminated lines: `<pid>\n<port>\n`.
7. **Async runtime and CPU affinity.** `core_affinity::set_for_current` pins one thread, so binaries build the tokio runtime with `on_thread_start` pinning rather than `#[tokio::main]`. `DriftScriptStrategy::on_market_event` calls `Handle::current().block_on` every event, so the engine drives it via `runtime.enter()` plus `futures::executor::block_on`, not `Runtime::block_on` (nested `block_on` panics).
8. **Stop is dual-path.** `SessionCommand::Stop` goes through the command channel (drained only on market events) and also fires a shared stop oneshot, so an idle stream cannot swallow it. Live and paper gate their event streams with `take_until(stop_signal)`. Passing the stop signal straight into the loop deadlocks live trading.
9. **Storage APIs are used as they are.** `StorageWriter::write_batch` requires homogeneous batches (group by `MarketEvent` variant first). `StorageReader::read_batch` is per symbol, venue and event type at day granularity.
10. **Drift Script hot reload.** `param(name, default)` is hot-reloadable only when called inside `on_market_event`. A top-level `param(...)` is read once at load and does not change on `set_param`. Both styles must keep working. Parameters are `Decimal`-only.

11. **Control surfaces are authenticated and audited.** `/session/ws` and `/metrics` use bearer tokens from `apexes-security` (constant-time compare), and the gateway's `/market` does too when `APEXES_GATEWAY_TOKEN` is set (if unset, the gateway runs unauthenticated and logs a warning, so set it whenever it is reachable beyond localhost). A live engine refuses to start without control tokens (`APEXES_CONTROL_TOKENS` named roles, or legacy `APEXES_CONTROL_TOKEN`). Viewer tokens are read-only and the engine enforces that, not just the client. Pass tokens by environment variable, never on a command line. Every command and denied attempt goes to the audit log.
12. **Safety machinery fails safe.** Halting trading must never sit behind a confirmation dialog; resuming, flatten and stop do. The operator console sends nothing while disconnected and discards clicks made against a dead link. The kill switch, stale-market-data watchdog and startup recovery (`apexes-oms`, `apexes-live-trading`) must not be weakened without a design discussion. See `docs/RUNBOOK.md`.

13. **One network switch, and secrets stay out of reach.** `BinanceNetwork` (in `apexes-core`) is the only source of Binance Spot endpoints; a test fails if a hostname appears elsewhere. `network` and `risk_limits` are required in live configs with no default, so mainnet cannot be selected or limits skipped by omission. The supervisor gives each child an **allow-listed environment** (`env_clear` plus named variables), so mainnet keys never reach a testnet session; secrets go by environment and `0600` files only, never on a command line or in a launch config. Mainnet in the launcher needs `MAINNET` typed. A gateway and an engine on different networks refuse each other.
14. **A live session is started only after startup recovery.** The engine writes its PID file (the supervisor's "started" signal) after recovery succeeds; its control port opens earlier so an operator can always Halt. Do not move the PID file earlier.
15. **Test against the real service before trusting mocks.** Binance's listenKey stream was removed (HTTP 410) and every mock-based test still passed; only running the real engine against the testnet showed it. Mocks pin behaviour you already know; for an exchange integration, also run the `#[ignore]`d live tests (testnet keys in `~/.apexes/binance.yaml` or the `APEXES_*` variables) and `scripts/smoke_binance_testnet_*.py` after any change to that code.

16. **Risk checks are questions, not events.** `PreTradeCheck::check` must not change state (the loop and `OrderManager` both ask about the same order); stateful limits advance only in `on_order_accepted`, called once by the unified loop after an order is accepted. Counting inside `check` made each order count twice and let rejected orders exhaust the daily cap.
17. **Never hold the `OrderManager` lock across a network call in a future that is only polled on the trading loop's turn.** It deadlocked the live engine (no events processed, false stale-data halt). Run such work as its own task (`run_detached`), and keep `open_orders()` non-blocking. `a_slow_exchange_query_must_not_stall_market_events...` guards it.
18. **Test suites and live sessions use different databases.** Tests TRUNCATE `apexes`; the launcher uses `apexes_live`. Live tests must cancel what they place on the shared testnet account (a stray resting order makes the next engine refuse to start).
19. **Quantities and prices go to Binance through `wire_decimal`** (plain, at most 8 places, never zero). A float-derived value can carry 20+ digits and is refused with `-1100`.

20. **Colours come from `theme::Palette`, never from a constant in a view.** A colour picked for a dark window is hard to read on a light one. Draw status text and banners with `palette_of(ctx)` and `theme::banner`, draw on `theme::page` (eframe's `ui()` has no background and clears to near-black), and use `theme::status_dot`, not a `●` character (egui's fonts lack it). **Look at rendered output**: `cargo test -p apexes-ui --test themes -- --include-ignored` writes PNGs; assertions did not catch any of the three theme bugs, a screenshot caught all of them.
21. **Opening a session never replaces another.** The operator window is one tab per console (`tabs.rs` holds the rules and is tested without a window); closing a tab drops its connection, never the engine.

22. **A market-data connection is dead when it goes quiet, not only when it errors.** `barter-data` reconnects on an error or a close; a socket that dies without a reset (Wi-Fi roam, sleep, NAT timeout) stays `ESTABLISHED` and silent, and the gateway served nothing for 30 minutes until the engine's stale-data watchdog halted trading. Every ingestion task runs under `apexes-market-data::supervised`, which drops a stream that delivers nothing for its idle window and reconnects with backoff, on a fresh runtime per attempt (so barter's un-stoppable forwarding tasks die with the dead socket). A new feed must use it. Idle windows: 30 s for quote/depth/mark-price feeds, 120 s for trades (the testnet is legitimately quiet).

23. **A gateway's instruments are fixed when it starts, so a new symbol means a new gateway, never a refusal or a restart.** The supervisor reuses the smallest running gateway that carries every requested symbol, else starts one for exactly those symbols; it must never restart a gateway under running sessions. Because a gateway can carry symbols a session did not ask for, `GatewayMarketDataProvider` drops events for any other symbol (a foreign price would value the session's position wrongly). Keep both halves.

24. **The testnet charges no fees, so it cannot test fee handling.** A ledger change must be checked with mainnet-style fills: a fee in the asset *received* (base on a buy, quote on a sell), a fee in an asset the session does not hold (BNB), and a fee in a held asset. A fill the exchange reports must reach the ledger; skipping one (the live loop logs and continues) silently desynchronises the session from the account.

25. **Report the strategy's result separately from the market's effect on what the account already held.** A testnet (or any live) account is rarely flat, and "PnL", "drawdown" and "exposure" computed from account equity alone credit or blame the strategy for a price move on assets it never traded. Keep the baseline split (`EvaluationTracker::set_baseline`, `strategy_pnl`/`holdings_pnl`). Prices arrive as `f64`; round derived differences (`PNL_DP`) so noise is not shown as a result. Format numbers for people with `number_format`, never `to_string()` on a `Decimal`.

26. **`scripts/run-testnet.sh` always rebuilds before it starts anything.** It once built only when a binary was missing, so after a code change it launched stale binaries and a fix looked like it had not worked. It now runs `cargo build --release` every time (about 1 s when nothing changed) and stops on a failed build. Already-running engines and gateways keep their old code, and the script warns about them but never stops them; a new session on symbols an old gateway carries *reuses that old gateway*, so stop it too after a gateway change.

27. **Anything an operator must be able to reach during a bad moment is a panel, never a modal.** HALT, the order list and the evaluation share one window; a dialog would put a click between the operator and the kill switch. Use side or inline panels (`orders_view::placement`). The order list comes from the trading loop's own `OrderLog`, not from the order manager (which only holds *working* orders). When a layout is wrong, render it (`egui_kittest` PNG) rather than guess: two layout bugs here were invisible to every assertion.

## 5. Working on a sub-project

1. **Design before code.** New sub-projects go brainstorm → spec (`docs/superpowers/specs/`) → plan (`docs/superpowers/plans/`) → implementation. Ask the user when a decision is genuinely ambiguous rather than guessing.
2. **Verify claims against the code.** Plans and briefs go stale (this has repeatedly produced wrong wiring and missed plumbing). Check struct fields, signatures and file contents before relying on a description, and tell the user when a brief is wrong.
3. **Fix gaps now.** A defect discovered during a task is fixed in that task or an immediate follow-up commit, not deferred. If you must defer something, record it in `docs/PLAN.md` under "Carry-forward technical debt".
4. **Review independently.** Non-trivial tasks get an implementer and a separate reviewer. A reviewer reads the real diff and the code, not the implementer's report.
5. **Be honest about scope.** A feature that is partly built is `🚧 In Progress` with Notes stating what exists and what does not. Do not mark `✅ Done` for a minimum-viable slice.

## 6. Documentation upkeep

When a task or sub-project finishes:

- Update the owning `FEATURES.md` rows, then **recompute the Summary counts from the table**. Use `grep -c` per status and subtract the legend row and the summary-table row for any status string that appears in those (verify per status rather than assuming; `✅ Done` and `🔲 Not Started` need the subtraction). Update the "Current active work" paragraph too.
- Check for work that landed on `master` outside the plan (`git log origin/master`) before trusting any plan or table. PR #9 shipped substantial features without updating most `FEATURES.md` rows or adding a plan, so rows can lag the code. Fix stale rows when you find them.
- Update `docs/PLAN.md` (task states, "Now"/"Next", carry-forward debt, completed plans with commit ranges).
- Refresh the status section of `README.md` if the implemented / in-progress / not-started picture changed.
- Keep `README.md` truthful: do not describe something as implemented unless `FEATURES.md` agrees.

## 7. Git

- Conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`, `perf:`, `ci:`; one logical change per commit.
- Commits are signed and signed-off: `git commit -S -s`.
- Check `git status` before committing and stage only intended files. Do not commit unless the user asked you to.
- Never commit secrets, API keys or credentials. `.superpowers/` is local working state (task ledger, briefs, review reports) and is not part of the repository.
- If signing hangs, the signing agent is waiting for the user to grant access. Wait, retry, or ask the user. Do not kill signing or agent processes.

## 8. Platform scope

Developed and tested on macOS and Linux. Process supervision and PID handling assume a Unix-like system. If you add code that does not work on Windows, document the limitation instead of silently skipping it.
