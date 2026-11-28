# F04.05 — Supplied recovery evidence window

## Scope and boundary

Extend the offline comparison from PR #104 with explicit temporal and
account coverage checks. No network requests, order changes, callback
replay, startup authorization, claim transition or risk-gate changes.
Evidence intervals, completeness and identity remain assertions supplied
by an independently qualified collector. This module cannot prove their
authenticity, actual freshness, or atomicity on the exchange.

## Evidence contract

Require a complete terminal evidence set followed by a complete
account-wide Binance Spot open-order observation. Both must refer to
the independently selected stable owner fingerprint. A symbol-only
empty response is insufficient. Reject incomplete coverage, different
owner/venue/asset class, any reported open order or foreign order,
reversed intervals, overlapping terminal/open-order intervals and
observations ending after the independently supplied inspection time.

Conservatively bound the entire interval from terminal collection start
to inspection time to 60 seconds. This is a fixed initial policy, not
a guarantee of exchange freshness; evidence that exceeds the ceiling
requires a new collection. Collection must start no earlier than the
selected observation cursor or the latest durable order creation/update
or fill timestamp. Equal boundary timestamps are permitted. Do not
derive inspection time from the supplied evidence itself. The caller
must independently establish a trustworthy clock and evidence provenance.

CoherentRecoverySelection::inspect_evidence_window selects the exact
owner-bound journal and validates the supplied interval/coverage.
Then use the existing complete terminal evidence comparator and
reverify the same original claim, selected head and journal. The result
is unit, not an authorization token. The previous offline comparison
API remains available with its original semantics.

## Qualification

Five unit tests cover the 60-second inclusive boundary, stale durable
order/fill timestamps, reversed/overlapping/future intervals, incomplete
coverage, owner/venue mismatch and nonzero open/foreign counts. The
existing actual coherent-stop integration test now additionally checks
the supplied window against a reopened PostgreSQL checkpoint and
rejects stale, incomplete and foreign-order evidence. All exchange
evidence is synthetic, no real exchange calls were made.

Rust 1.99.0, locked dependencies, serial tests: 193 OMS/EMS/live-trading
tests (97/32/64) passed against isolated PostgreSQL, 733 core tests and
60 engine tests passed (986 total), zero failures, six external core
tests ignored. Workspace all-target Clippy with warnings denied,
formatting and diff checks passed. Disposable database shut down and
removed. Full workspace build/test, independent review, remote CI and
real-service qualification pending.

## Remaining work

Authoritative read-only evidence collection and freshness/provenance
qualification are not implemented. The supplied counts are not
independently verified exchange results. Keep the one-time claim
restriction and all execution safeguards. Fresh wallet evidence,
full ledger/risk/observation continuity, repeatable post-execution
restart, periodic checkpointing, EMS recovery and causal no-order
warm-up remain open.
