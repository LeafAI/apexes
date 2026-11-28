# F04.05 — Independent direct Spot fee reconciliation

## Scope

Read-only diagnostic comparison of retained Spot fee quantities and
historical execution valuation subtotals against the exact selected
journal. No changes to runtime accounting, trading decisions, state
application or startup permission.

## Contract

Require the existing captured-ledger reconciliation against independently
supplied wallet and position evidence. Reopen the exact selected
journal and durable attachment using the existing receipt checks.
Require actual retained fee evidence, bounded supported Spot scope,
and complete matching traded-symbol account coverage. Compare exact
instrument base, quote and venue metadata.

For each journal fill, validate supported scope, positive price and
quantity, nonnegative fee and absence of leverage/margin. Zero fees
contribute no fee-map entry. Nonzero fees require an explicit fee
asset. Base fees are valued at the execution price; quote fees are
valued at their own quantity. Use checked exact decimal multiplication
and addition for quantities and valuations. Match canonical per-asset
totals and the known execution valuation subtotal. Require zero
missing valuations and no external fee entries in the direct-fee case.
Refuse third-asset fees, rather than assume a historical rate from
the retained conversion windows.

## Limits

The comparison qualifies direct fee amounts and historical valuation
subtotals only. It does not reconstruct per-fill conversion decisions,
mark valuations, third-asset fees, external ownership, or fee-bearing
realized P&L. It does not prove source completeness, independent
provenance, runtime atomicity or permission to restart. No state is
applied or callbacks replayed.

## Qualification

The new unit test checks nonzero base and quote fees against independent
expected quantities and subtotal, plus refusal of third-asset fees,
missing fee currency or capture, incorrect subtotal or quantities,
missing valuations, unexpected external fees and mismatched quote
identity. Extend the isolated coherent-stop integration with direct
fee comparison of the actual reopened fee-free attachment.

Rust 1.99.0, locked dependencies, serial tests: 210 OMS/EMS/live tests
(97/32/81) against disposable PostgreSQL, 822 core/risk (78 risk),
and 60 engine tests passed: 1,092 total, zero failures, six external
tests ignored. Workspace all-target Clippy with warnings denied,
formatting and diff checks passed. Disposable database stopped and
removed. Full workspace build/test, independent review, remote CI
and real-service qualification remain pending. No real orders or
production database operations.

## Remaining work

Next qualify independent per-fill third-asset valuation evidence.
Complete fee history, fee-bearing realized P&L, actual risk/ledger
state application, full runtime atomicity, source completeness and
authoritative acquisition remain open. Preserve the one-time startup
restriction and do not replay completed callbacks.
