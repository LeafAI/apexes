# F04.05 — Offline conversion-only scope evidence

## Scope

Read-only comparison of independently retained instrument roles at a
selected coherent checkpoint. This is a diagnostic classification of
the retained evidence, not an execution allowlist, ownership assertion
or authorization to exclude an instrument from recovery.

## Contract

Require a versioned declaration bound to the exact coherent receipt
and owner fingerprint. Compare the complete, canonical symbol-sorted
scope to the supplied instrument metadata and actual retained ledger.
Restrict to Binance Spot, at most 128 instruments, unique asset pairs,
valid explicit base/quote metadata and at least one strategy-inventory
role. Require actual fee attribution and per-fill decision evidence.

Require that every journal order (including rejected orders), fill,
retained position (including flat entries), realized P&L entry,
fee-attribution account and decision belongs to the declared
strategy-inventory role. Retained conversion price windows must map
to the exact declared venue and currency pair; each conversion-only
instrument must have a retained nonempty price window. Refuse missing
scope, conflicting metadata, unmatched price windows or execution
footprints. Merely holding a fee asset in an account balance is not
evidence of a strategy position.

The inspection reads the exact selected journal and durable attachment
without applying any state, filtering recovery instruments, invoking
callbacks, or changing existing inventory or account gates.

## Qualification

Two offline unit tests use actual in-memory ledger evidence from a
synthetic third-asset fee execution and conversion-only price history.
Verify complete scope, serialization, and refusal of wrong owner or
receipt, missing/reordered scope, conflicting pair metadata, missing or
unmatched price windows, rejected orders, fills, retained flat positions,
realized P&L entries and absent decisions. Confirm that the original
owned-position gate still refuses the unsupported third-asset history.
Extend the isolated coherent-stop integration to inspect the actual
reopened fee-free attachment with a strategy-inventory-only declaration.

Rust 1.99.0, locked dependencies, serial tests: 220 OMS/EMS/live tests
(97/32/91) with disposable PostgreSQL, 841 core/risk/research (including
17 research and 78 risk), and 60 engine tests passed: 1,121 total,
zero failures, six external tests ignored. Workspace all-target Clippy
with warnings denied, formatting and diff checks passed. Disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain pending.

## Remaining work

Next qualify retained conversion-price provenance against the completed
observation evidence offline. The classification only describes the
selected retained boundary; it does not establish absence of activity
outside that journal, authenticated market provenance, source
completeness, or actual runtime ownership. End-to-end nonzero
external-fee checkpoint qualification remains blocked by the existing
conversion-instrument/owned-base restriction. Preserve that restriction,
all account and startup safeguards, and avoid runnable state application.
