# F04.05 — Independent fee-bearing realized P&L

## Scope

Read-only comparison of actual retained basic Spot realized P&L and
gross shadow attribution with the selected journal and independently
reconciled fee decisions. No runtime accounting, execution, state
application or startup authorization changes.

## Contract

Require existing independent terminal, wallet, position, captured
ledger and exact per-fill fee decision comparisons. Starting from zero
strategy-owned and gross shadow inventory, independently calculate both
cost bases in retained attribution order. Require strictly increasing
per-symbol execution timestamps; do not sort an ambiguous history.

For owned inventory, a buy receives quantity less any base fee but
retains the entire execution cost. A sale realizes the difference
between sale price and owned entry on the sold quantity; a base fee
consumes additional owned inventory at its entry cost. Reject
nonpositive buy receipts and sales plus base fee exceeding owned
inventory. Quote and third-asset fees do not modify owned cost basis.
For shadow attribution, use full execution quantities and gross cost;
reset the shadow entry to execution price on a complete close, as in
existing attribution semantics. Require exact checked arithmetic and
exactly representable weighted averages.

Compare every actual owned position, including retained flat entries,
its entry price, explicit basic realized map, and the gross shadow
quantity, entry and realized P&L. Historical fee expense is taken from
the already independently qualified decisions. Derive net realized
attribution as gross realized less historical fee expense; the net is
not an independent stored field. Do not subtract historical fees from
the basic ledger's realized map a second time or use current mark
conversion rates to reprice historical expense.

## Qualification

Two offline unit tests cover mixed base/quote/third-asset fees,
independently calculated owned and shadow cost basis, a base-fee
sale, altered realized P&L, changed cost basis, reordered decisions,
inexact weighted average and unowned base fee refusal. Extend the
existing isolated coherent-stop integration with the stronger API
on the reopened fee-free attachment. Mixed nonzero fee scenarios
remain offline unit-tested, not end-to-end database-qualified.

Rust 1.99.0, locked dependencies, serial tests: 214 OMS/EMS/live tests
(97/32/85) against disposable PostgreSQL, 824 core/risk (78 risk),
and 60 engine tests passed: 1,098 total, zero failures, six external
tests ignored. Workspace all-target Clippy with warnings denied,
formatting and diff checks passed. Disposable PostgreSQL stopped
and removed. Full workspace build/test, independent review, remote
CI and real-service qualification remain pending.

## Remaining work

Next qualify external fee ownership evidence. Complete valuation
history, authoritative provenance, ambiguous or rounded histories,
mark revaluation, actual risk/ledger state application and runtime
atomicity remain unqualified. Preserve the one-time startup
restriction; do not replay completed callbacks or grant execution
authority.
