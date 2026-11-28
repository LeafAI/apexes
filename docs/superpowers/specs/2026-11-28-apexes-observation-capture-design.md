# F04.05 — Actual completed observation capture

## Scope

Advance from independently supplied observation records to actual
loop-local capture, without changing the startup claim, execution
authorization, order handling, or recovery policy. This is an in-memory
capture slice, not an atomic database observation witness.

## Capture contract

The existing unified loop remains the sole execution loop. An explicit
empty observation output slot requires an existing checkpoint predecessor.
Ordinary callers and the existing checkpoint-only API pass no slot and
retain their previous behavior. The optional new library entry point
run_live_trading_with_observation_capture_on_stop delegates to the same
single-use checkpoint path, returning the report, coherent receipt and
CapturedObservations only on success.

The loop records a normalized MarketEvent and its actual completed
iteration cursor at the existing checkpoint cursor update. Filtered
events never reach the capture. Supplemental execution callbacks do not
advance market ordinals. The scope is derived from the actual configured
instruments and the clock policy from the existing clock instance.

Retain at most 8,192 records, 1 MiB per serialized record and 64 MiB total,
using the existing receipt limits and pre-clone size guards for variable
event fields. Exceeding capacity refuses capture rather than truncating
history. Finalize the receipt using the existing exact ordered
observation comparator before coherent checkpoint publication. Assign
the output only after publication acknowledges success. On error, no
successful capture is returned; the existing failure path remains
fail-closed. The checkpoint-only path is unchanged.

## Durability boundary

The capture is returned in memory. It is not part of the PostgreSQL
checkpoint transaction, and a crash after commit but before return can
lose the observation records. The caller must retain them independently.
The existing independent observation file and comparison APIs can verify
the returned receipt against the selected journal. A returned record
set is evidence of the completed loop iterations, not proof of gap-free
source data, all supplemental callbacks, control commands, risk counters,
or external evidence provenance. No replay or repeatable restart is
authorized.

## Qualification

Two new unit tests check actual capture records/cursor and bounded
capacity/refusal. Extend the real coherent-stop integration to call
the explicit capture API, compare the returned history against
independent mock-source events, verify exact receipt/clock/scope,
and run the existing durable inspection. Exercise the same opt-in
API in the injected PostgreSQL commit failure and no-completed-market
observation cases; both must refuse successful publication and preserve
the old boundary.

Rust 1.99.0, locked dependencies and serial tests: 206 OMS/EMS/live
tests (97/32/77) passed with isolated PostgreSQL; 810 core/risk and
60 engine tests passed (1,076 total), zero failures, six external
tests ignored. Workspace all-target Clippy with warnings denied,
formatting and diff checks passed. Disposable database stopped and
removed. Full workspace build/test, independent review, remote CI and
real-service qualification pending. No real orders, live exchange
requests or production database operations.

## Remaining work

Qualify actual registered risk-monitor capture and atomic durable
observation/risk evidence with the selected checkpoint boundary.
Full ledger/risk state restoration, source completeness, authoritative
collection and durable run transition remain unqualified. Maintain
the one-time startup claim restriction. Periodic capture, unresolved
and EMS recovery and causal no-order warm-up remain open.
