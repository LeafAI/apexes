# F04.05 — Offline observation receipt inspection

## Scope

Advance the independent offline observation comparison contract without
changing the unified loop, replaying callbacks, mutating risk state,
transitioning the startup claim or authorizing another trading session.
Authoritative exchange evidence acquisition remains blocked.

## Ordered observation receipt

CompletedObservation carries a completed iteration cursor and normalized
MarketEvent. ObservationReceipt binds the exact predecessor and terminal
cursors, independently selected clock policy and instrument scope,
record count and domain-separated SHA-256 of the header and ordered
length-prefixed serialized records. Sort scope deterministically by
venue and symbol, reject duplicates, empty or unbounded scope. Bound
scope to 128 instruments, history to 8,192 records, each serialized
record to 1 MiB and the aggregate to 64 MiB; bound individual depth
levels and trade identifiers before serialization.

Require consecutive ordinals, nondecreasing logical observation times
and exact terminal cursor equality. Equal logical timestamps can belong
to distinct iterations. Source availability is bar close, not open;
reject invalid bar intervals and invalid depth source range metadata.
The existing recovered-run lower-bound filter requires source
availability strictly after the predecessor observation time.
SourceAvailability requires equality with the availability timestamp;
ConsumerUtc permits later observation of a delayed source event
but never observation before availability or logical clock regression.
Reject incompatible scope and clock claims. These are inspection
constraints, not a change to any existing live/backtest clock.

## Independent evidence

ObservationBoundaryEvidence retains the exact predecessor checkpoint
receipt, acknowledged coherent boundary, stable owner fingerprint and
observation receipt. Use existing bounded canonical JSON receipt files,
strict fields, create-new, file and directory sync and exact canonical
byte validation. Compare only to the independently selected boundary.
CoherentRecoverySelection::inspect_observation_evidence verifies the
selected original claim/head/journal, compares the supplied records
against the retained digest, then re-verifies the same journal.

The receipt is an equality witness, not proof of capture from the
actual runner, source authenticity, event completeness, gap-free
exchange sequences, or absence of external changes. It does not bind
supplemental callbacks, control commands, risk state or valuation.
Records are supplied by the caller; this slice does not collect them
in the live loop. No replay or restart authorization is provided.

## Qualification

Five new backtest unit tests cover exact payload/scope/clock comparison,
missing/reordered/duplicated cursor ordinals, timestamp regression,
delayed consumer arrival, pre-availability and pre-boundary source
events, close-time bars, malformed intervals, scope and capacity.
Extend the actual coherent-stop PostgreSQL integration with an
independently retained synthetic market-source observation receipt
and reject altered or missing records. The synthetic fixture is
not actual atomic runtime capture.

Rust 1.99.0, locked dependencies and serial tests: 206 OMS/EMS/live
tests (97/32/77) passed with isolated PostgreSQL; 808 core/risk and
60 engine tests passed (1,074 total), zero failures, six external
tests ignored. Workspace all-target Clippy with warnings denied,
formatting and diff checks passed. Disposable database stopped
and removed. Full workspace build/test, independent review,
remote CI and real-service qualification pending. No real orders,
live exchange calls or production database operations.

## Remaining work

Actual atomic capture of registered risk monitors and completed
observations, qualified source completeness and provenance, complete
risk/ledger state restoration and authoritative collection remain
unqualified. Maintain the one-time claim restriction. No repeatable
post-execution restart, periodic capture, unresolved/EMS recovery
or causal no-order warm-up is enabled.
