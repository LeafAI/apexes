# F04.05 — Retained conversion-price window provenance

## Scope

Read-only comparison of actual final retained conversion windows with
completed market observations. No new prices, runtime accounting
changes, state application, instrument filtering or recovery authority.

## Contract

Require the exact selected journal, independently declared instrument
roles, retained fee decision evidence and matching ledger scope.
Reopen the exact durable attachment and verify its completed records
against the independently retained owner/predecessor/boundary-bound
observation receipt. Use only completed trade observations for the
declared venue and asset pair. Other event types do not contribute
conversion prices. Match the existing positive-price rule, source
timestamps and consumer observation ordering: later observations at
the same source time replace earlier values; retain the newest 2,048
source timestamps per pair, including eviction of late older values.
Require exact pair coverage and final retained observation equality,
not just equal closing marks. Refuse absent or extra price windows,
incorrect identity, price or source timestamp, and omitted retained
observations. The comparison is bounded by the existing 8,192-record
observation cap and fee evidence limits.

## Qualification

Two offline unit tests verify exact final-window matching, delayed
same-timestamp replacement, ignored nonpositive prices, unknown
symbols, omitted or additional observations, and actual ledger
window eviction after 2,050 observations with a late older trade.
Extend the existing isolated coherent-stop integration with the new
inspection on the actual reopened fee-free attachment; its observation
receipt is generated from the fixture capture and retained separately,
not from an independent real market source.

Rust 1.99.0, locked dependencies, serial tests: 222 OMS/EMS/live tests
(97/32/93) with disposable PostgreSQL, 841 core/risk/research (17
research and 78 risk), and 60 engine tests passed: 1,123 total,
zero failures, six external tests ignored. Workspace all-target Clippy
with warnings denied, formatting and diff checks passed. Disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain pending.

## Remaining work

Next qualify offline fill-time observation availability evidence.
Final-window equality cannot prove the price available when a fill
callback executed, the original contents of evicted windows, or
market-source authenticity/completeness. Independently sourced
evidence is still required; matching a supplied receipt is not
authentication. End-to-end nonzero external-fee qualification remains
blocked by the existing conversion-instrument/owned-base restriction.
Preserve that restriction and all existing startup safeguards.
