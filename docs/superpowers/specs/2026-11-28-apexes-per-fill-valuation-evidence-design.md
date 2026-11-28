# F04.05 — Actual per-fill valuation decision evidence

## Scope

Retain actual Spot attribution valuation decisions, not a reconstructed
historical price report. No changes to accounting decisions, execution,
state application, or startup authorization.

## Contract

The bounded decision log records actual attribution order, fill identity,
venue, symbol, effective fee asset, execution timestamp and price, fee
quantity, local external-fee flag, and optional actual valuation. Explicit
source kinds distinguish zero, direct base, direct quote, observed causal
conversion, missing conversion, and externally supplied valuation. The
observed source retains the selected timestamp and rate even when the
price window subsequently replaces or evicts the observation. Externally
supplied values do not establish the underlying conversion source.

Record at most 8,192 decisions, with bounded symbol and asset keys.
Overflow or invalid keys mark capture unavailable without changing normal
attribution. Reject duplicate fill IDs, unsupported values or inconsistent
source metadata, and reconcile fee quantities, external quantities,
known valuation subtotal and missing count against the actual retained
aggregate maps using checked arithmetic in attribution order. The legacy
optional field is omitted unchanged; missing evidence is not upgraded.

The market observation cursor bounds retained price observations, not
supplemental fill callbacks. A fill may arrive after the last market
observation; selected conversion rates must still satisfy the original
five-minute causal execution-time constraint. The existing coherent
checkpoint journal gates are unchanged. Independent per-fill journal
and conversion comparisons are a subsequent qualification.

## Qualification

Two new unit tests verify actual selected rates after price replacement,
zero/direct/observed/missing/provided distinctions, aggregate consistency,
duplicate and omitted nonzero evidence refusal, legacy encoding and
capacity overflow without changing attribution. The existing coherent
stop PostgreSQL fixture verifies the retained fee-free decision and exact
durable attachment reopening. An initial integration failure revealed
an invalid market-cursor bound on supplemental fill timestamps; removing
that bound while preserving price and conversion causality fixed it.

Rust 1.99.0, locked dependencies, serial tests: 211 OMS/EMS/live tests
(97/32/82) against disposable PostgreSQL, 824 core/risk (78 risk) and
60 engine tests passed, 1,095 total, zero failures on final run, six
external tests ignored. Workspace all-target Clippy with warnings denied,
formatting and diff checks passed. Disposable database stopped and
removed. Full workspace build/test, independent review, remote CI and
real-service qualification remain pending. No real orders or production
database operations.

## Remaining work

Next qualify exact retained decisions against independent journal and
conversion evidence. Complete historical coverage, external ownership,
fee-bearing realized P&L, actual ledger/risk application, runtime
atomicity and authoritative acquisition remain open. The bounded record
log is not proof of source completeness or a restored ledger. Preserve
the one-time startup restriction and do not replay completed callbacks.
