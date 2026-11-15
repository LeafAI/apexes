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

_Last updated: 2026-11-15 (reconciled after PR #9)_

---

## Now — #13c Strategy Monitoring & Live Tuning

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
