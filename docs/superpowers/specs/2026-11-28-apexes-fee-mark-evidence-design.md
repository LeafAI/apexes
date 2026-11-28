# F04.05 — Independent closing fee mark revaluation

## Scope

Read-only comparison of historical fee expense and closing mark
valuation. Do not acquire live rates, modify accounting decisions,
apply state, or grant startup authorization.

## Contract

Require the existing exact selected journal, independent wallet and
position checks, retained fee decision comparison and restricted
non-external fee-bearing realized P&L qualification. Reopen the exact
durable attachment. The new independently supplied versioned evidence
binds the exact coherent receipt and owner.

Require one canonical mark row for each distinct nonzero non-quote
fee asset/quote pair across traded Spot symbols. For quote-asset fees
the conversion rate is one, with no external rate row. Refuse missing,
duplicate or unused rows and any venue, pair, time or rate mismatch
against the latest retained conversion observation. The mark observation
must be no later than the selected market cursor and at most five minutes
earlier. This deliberately stricter qualification window does not alter
the existing accounting module's latest-observation mark behavior.

Compare the exact independently supplied per-symbol execution-valued
fee total, mark-valued fee total and difference using checked arithmetic.
Require all traded accounts, including zero-fee accounts, and no missing
historical valuations. Mark valuation is a derived value from retained
quantities and rates, not a separately persisted summary. Supplemental
fill time remains separate from the market cursor. No current mark
freshness relative to wall clock or source authenticity is implied.

## Qualification

Two offline unit tests use actual in-memory ledger captures with base,
quote and third-asset fees and replaced conversion rates. Verify known
execution subtotal, closing mark and revaluation against the actual
ledger report, and refuse incorrect, missing, duplicate, unused,
stale or future marks, offsetting valuation errors, wrong owner/receipt,
incorrect subtotal and missing historical valuations. Extend the
existing isolated coherent-stop integration with exact reopening and
independent zero-fee mark evidence.

Rust 1.99.0, locked dependencies, serial tests: 217 OMS/EMS/live tests
(97/32/88) against disposable PostgreSQL, 824 core/risk (78 risk),
and 60 engine tests passed: 1,101 total, zero failures, six external
tests ignored. Workspace all-target Clippy with warnings denied,
formatting and diff checks passed. Disposable database stopped and
removed. Full workspace build/test, independent review, remote CI
and real-service qualification remain pending.

## Remaining work

Next qualify combined external-fee valuation/P&L evidence. Nonzero
mixed-asset mark scenarios remain offline unit-tested, not end-to-end
database-qualified. Complete valuation history, independently
authenticated market evidence, external funding provenance, actual
risk/ledger state application, full runtime atomicity and restart
qualification remain open. Preserve existing execution and startup
restrictions.
