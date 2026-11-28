# F04.05 — Isolated no-order scoped Drift warm-up

## Scope

Qualify a trusted, explicitly scoped Drift script's checkpointed state by
reconstructing it on a newly loaded offline interpreter from independently
selected ordered history. This does not restore a running strategy, assert
source completeness, or authorize a startup or post-execution transition.

## Callback and evidence contract

The script supplies a separate callable `on_warmup_event(event, ctx)`.
It may share pure indicator/state logic with the normal decision callback.
The warm-up context exposes only the receipt's observation time and scoped
state, indicators and time gates; no wallet, positions, orders, parent
orders, costs, market-rule or risk context is supplied. Exactly an empty
array is accepted. A missing hook, malformed/nonempty result, interpreter
error or rejected scoped state operation fails qualification. Discard the
whole candidate on any failure; interpreter globals and parameter writes
are not transactional. The normal callback remains unchanged.

The offline entry point takes a previously selected checkpoint receipt
and original parameters, the trusted script and instrument scope, and an
independent opening observation receipt and records. It verifies the
entire bounded sequence, owner, exact target, scope, availability clock,
cursor and digest before constructing the candidate. The fee-specific
five-minute lookback is not imposed on strategy history; the caller must
qualify the required strategy horizon. Replay one record at a time in
receipt order with the recorded observation time, never a wall clock.
Require exact canonical checkpoint receipt equality at the target cursor,
including script source, original parameter configuration and scoped
state. Return only the qualified observation count, not a strategy or
checkpoint. No executor, database or live account is passed to the API.
Trusted script construction and deterministic scoped-only state remain
caller obligations; this is not an OS sandbox or a general interpreter
global-state codec.

## Verification

Actual interpreter tests compare the warmed state with a separate ordinary
decision-callback reference across two instruments, including native
indicator state, parameters, recorded time and elapsed gates. Tests refuse
altered history before script load, independently re-receipted mismatching
history, owner/scope/config mismatches, future observations, nonempty and
malformed returns, invalid state and unavailable account context.
Rust 1.99.0, locked dependencies: 228 OMS/EMS/live tests (including
isolated PostgreSQL suites) and 892 core/risk/research/engine tests passed,
for 1,120 total, zero failures and four external tests ignored. Strict
workspace Clippy, formatting and diff checks passed. The disposable
PostgreSQL cluster was stopped and removed. Full workspace build/test,
independent review, remote CI and real-service qualification remain open.

## Remaining work

Authoritative source/history completeness, actual risk and ledger state
application, callback provenance and end-to-end nonzero external-fee
qualification are still open. Next qualify an exact offline restoration
of registered risk state into isolated checker instances with no live
submission authority before any post-execution transition. Keep the
one-time startup claim restriction and existing owned-position gates.
