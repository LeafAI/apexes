# F04.05 — Offline risk-state inspection contract

## Scope

Advance independent risk-state inspection without changing risk decisions,
resetting counters, clearing trips, replaying callbacks or authorizing
another trading session. Authoritative evidence collection remains blocked.
The existing one-time startup claim restriction stays in force.

## Read-only monitor snapshots

LimitStateSnapshot captures the actual LimitChecker configuration and
UTC calendar day of the most recent accepted order, trade count and
accepted quantity volume. Check calls alone do not increment counters.
Read the shared mutex without mutation; refuse poisoned state. Refuse
invalid versions, negative configuration/volume, saturated count and
inconsistent empty/day/count/volume combinations. Do not clamp retained
counters to newly tightened limits or infer protective-exit eligibility.
The existing UTC rollover behavior is unchanged.

CircuitStateSnapshot captures the actual configured limits, sorted
unique bounded tripped symbol set and retained account-trip flag. The
pre-trade split adapter reads the same underlying shared monitor as the
in-trade adapter; refuse poisoned locks. Validate version, bounded
symbol count/size, canonical ordering and configured trip scope.
Comparison requires exact configuration and retained state. No DTO
can instantiate or restore a runnable monitor.

## Independent evidence

RiskBoundaryEvidence combines the exact acknowledged coherent receipt,
stable owner fingerprint, asserted capture timestamp, accepted-order
budget snapshot and explicit optional circuit-breaker snapshot. Use
the existing bounded canonical JSON receipt-file contract (64 KiB,
strict fields, create-new without overwrite, file and directory sync,
bounded regular-file read and canonical byte equality). Validate
encoding on comparison as well as construction and file read.
Capture must be no earlier than the selected observation cursor or
latest durable order/fill timestamp and no later than the independently
supplied inspection time. Reject a daily counter dated after capture.

CoherentRecoverySelection::inspect_risk_evidence compares the supplied
observed risk state against the independently retained evidence with
the exact owner-bound journal, then re-verifies the selected original
claim, head and journal. Missing/extra circuit-breaker evidence, changed
configuration, cleared trips or reset counters refuse inspection.
No recovery authorization token or live risk-state mutation is produced.

A separately retained snapshot does not prove the monitor was registered
in the actual session, or that capture was atomic with the checkpoint.
The integration uses explicitly synthetic independently supplied risk
evidence. Actual monitor capture is tested separately using real
LimitChecker and shared CircuitBreaker instances. No inference of
accepted counts/volumes from fills or terminal orders is attempted.

## Qualification

Four new risk-engine tests cover accepted-order accounting, configuration
change, read-only UTC rollover behavior, retained shared circuit trips,
invalid daily states and invalid trip scope/ordering. Three new
live-trading unit tests cover durable independent risk evidence files,
no overwrite, malformed/noncanonical/oversized files, invalid version
and timestamps, exact boundary and owner, monitor presence, reset
counters and cleared trips. Extend coherent-stop PostgreSQL integration
to inspect separately persisted synthetic evidence against the actual
reopened journal and reject a reset budget.

Rust 1.99.0, locked dependencies and serial tests: 206 OMS/EMS/live
tests (97/32/77) passed with isolated PostgreSQL; 803 core/risk tests
(including all 70 risk-engine tests) and 60 engine tests passed
(1,069 total), zero failures, six external tests ignored. Workspace
all-target Clippy with warnings denied, formatting and diff checks
passed. Disposable database stopped and removed. Full workspace
build/test, independent review, remote CI and real-service qualification
pending. No real orders, live exchange calls or production DB operations.

## Remaining work

Next independent task is offline observation continuity. Actual atomic
capture of all registered risk components, including market-condition
and margin monitor state when applicable, runnable risk-state restoration,
full ledger and observation continuity, authoritative read-only evidence
collection and durable run transition remain unqualified. No repeatable
post-execution restart, periodic capture, EMS recovery or causal no-order
warm-up is enabled.
