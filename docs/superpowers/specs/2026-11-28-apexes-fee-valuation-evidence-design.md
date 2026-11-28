# F04.05 — Retained Spot fee-valuation evidence

## Scope

Read-only inspection of actual retained Spot attribution state in the
loop-owned portfolio ledger. No changes to fee accounting, price
selection, execution, recovery authorization or runtime state application.

## Evidence contract

Retain versioned canonical symbol accounts: venue, base and quote,
fee-free gross inventory, entry and realized P&L, fee quantities per
asset, externally paid fee quantities, known execution valuation
subtotal and missing execution valuation count. A known subtotal is
not a complete valuation when the missing count is nonzero. Preserve
actual retained conversion price windows, ordered by venue and asset
pair then observation time, not an inferred valuation report.

Require valid scoped accounts, nonempty bounded keys, positive
conversion prices, canonical unique ordering, distinct base and quote,
nonnegative supported fee quantities and historical subtotal, external
fees no greater than the corresponding total, and no observations
after the checkpoint cursor. Limit accounts and price pairs to 128
each, fee assets to 128 per account map, individual price windows
to 2,048 rows, total price rows to 16,384 and missing count to 8,192
per account. Refuse invalid or oversized evidence instead of truncating
it. Gross shadow quantities can be negative. The attachment's existing
size and digest checks still apply.

Capture the actual retained state during existing opt-in basic ledger
inspection. Older receipts omit the optional field and retain their
canonical encoding. Absence is not evidence of zero fees.

## Limits

The evidence captures retained windows, not all observations ever
seen or per-fill valuation decisions. Evicted/replaced observations
cannot be recovered; missing rates remain missing. No independent
reconstruction or authentication of fee totals, asset classification,
valuation provenance, or exact instrument-pair mapping is claimed.
The current supported nonnegative-fee contract excludes rebates.
No runnable restored ledger or risk checker is returned.

## Qualification

A new Spot accounting unit test checks known execution subtotal
alongside missing valuations, external fee quantities, gross state,
retained conversion prices, deterministic serialization, invalid
ordering and future-time refusal. Extend the isolated coherent-stop
integration with actual fee-free attribution and conversion window
assertions and exact reopening.

Rust 1.99.0, locked dependencies, serial tests: 209 OMS/EMS/live tests
(97/32/80) against isolated PostgreSQL, 822 core/risk (78 risk), and
60 engine tests passed: 1,091 total, zero failures, six external tests
ignored. Workspace all-target Clippy with warnings denied, formatting
and diff checks passed. Disposable PostgreSQL stopped and removed.
Full workspace build/test, independent review, remote CI and real-service
qualification remain pending. No real orders or production DB operations.

## Remaining work

Next qualify independent fee-quantity and valuation evidence
reconciliation. Complete per-fill valuation history, fee-bearing realized
P&L, actual risk/ledger state application, full runtime atomicity,
source completeness and authoritative acquisition remain open. Preserve
the one-time startup restriction and do not replay completed callbacks.
