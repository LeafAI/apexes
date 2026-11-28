# F04.03 — OMS submission gate context

## Contract

Native StrategyContext gains optional HealthObservation. The shared loop
queries OrderExecutor::health_observation immediately before strategy
decisions. The default returns None, including matching simulation; no
live readiness is inferred from backtest or paper execution.

The live runner captures the actual OrderManager's submission gate before
moving the manager into the shared asynchronous mutex. The live adapter
samples the existing KillSwitch, not a separate health model, not an
exchange query, and not the possibly different runner configuration gate.
Gate transitions and observation share the same synchronous health lock.
The observer reads phase and engaged flag coherently and derives the
existing blocking predicate. The live runner does not wait on the
network-held manager lock. Standalone adapter callers whose initial
try_lock cannot capture the gate retry a nonblocking lookup on later
observations; absent evidence remains unavailable. The gate is assumed
attached before the live manager is shared, as in existing startup.

Managed phases are initializing, reconciling, running, degraded, halted.
An unmanaged kill switch has no managed phase, but its engagement and
blocking flags are still available. A missing gate is unavailable, not
healthy. Fault details are deliberately omitted; no free-form reasons
or credentials cross into strategy scripts.

## Drift API

ctx.health is null when unavailable; otherwise it is a detached object
with source ("oms_submission_gate"), phase (lowercase name or null),
kill_switch_engaged and blocks_submissions. A stored observation does
not refresh. It is an instantaneous gate observation at callback time,
not an admission authorization, promise of order success, comprehensive
market/connection health assessment, or asynchronous health notification.
The existing OMS and EMS checks remain authoritative. No changes to
submission, pause, cancellation, reconciliation, recovery or emergency
behavior. Rust StrategyContext literal callers now specify health
(None when unavailable); all workspace literals updated.

## Verification

Two new non-service regressions: real KillSwitch managed and unmanaged
transitions (including saved snapshot, latched halt and refused release);
native-to-Drift observation binding with unknown/unmanaged and all
managed phases. An additional PostgreSQL-backed adapter regression
checks that the actual captured OMS gate remains readable while the
manager lock is held and that halted admission still refuses an intent.
The service regression compiles but was not run locally.

Rust 1.99.0 locked offline: 598 tests passed in the serial backtest,
paper, strategy, indicators and loader suites, plus one focused OMS
gate test. Three ignored external tests in those suites were not run.
All-target Clippy with warnings denied passed for strategy, loader,
backtest, paper, live-trading, engine and OMS. Formatting and diff checks
passed. Workspace CI, independent review and live-service qualification
remain pending. No actual orders, production accounts or databases used.

Next: F04.04 deterministic lifecycle callbacks. F04.03 remains in
progress for broader live evidence, capital and cost context.
