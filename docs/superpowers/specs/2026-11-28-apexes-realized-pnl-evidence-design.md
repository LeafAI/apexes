# F04.05 — Independent fee-free gross realized P&L evidence

## Scope

Offline diagnostic comparison of gross realized P&L in the actual
durable basic ledger snapshot against an independently reconstructed
restricted historical journal. No change to runtime accounting, strategy
decisions, order behavior, or recovery authorization.

## Contract

First qualify the existing independent wallet, terminal, closing
position and captured-ledger comparisons. Read the exact selected
journal and capture again, verifying the existing receipt and digest.
Validate the captured ledger scope. Restrict to the supported Binance
Spot history, at most 8,192 fills, no leverage or margin, positive
quantities and prices, and zero fees. Reject non-increasing per-symbol
fill timestamps rather than assume an ordering when timestamps tie.
Assume zero opening strategy-owned positions, as required by the
existing supported predecessor journal contract.

Maintain an isolated calculation of weighted average entry cost and
owned quantity per symbol. Use checked exact decimal addition and
multiplication, and reject a division when multiplication cannot
recover the original exact cost. Refuse sales exceeding owned quantity.
Accumulate gross realized differences on closes, including explicit
zero realized entries. Compare the exact sorted realized map without
mutating the ledger, replaying strategy callbacks, or returning
runnable restored state.

## Limits

This comparison is deliberately restricted to fee-free histories with
exactly representable arithmetic and unambiguous per-symbol ordering.
Fee-bearing accounting, conversion and valuation history, opening
positions outside the supported journal contract and rounding rules
remain unqualified. A successful comparison cannot authenticate
caller-supplied evidence or prove global runtime atomicity. No state
application or restart permission is implied.

## Qualification

Two new offline unit tests cover weighted average cost, multiple
profitable and losing closes, changed or omitted realized entries,
open-only history, nonzero fees, tied timestamps, inexact division and
unowned sales. Extend the existing isolated coherent-stop integration
with read-only gross realized P&L qualification of the reopened
fee-free capture. The integration's original unrelated wallet holding
refusal remains intact.

Rust 1.99.0, locked dependencies, serial tests: 209 OMS/EMS/live tests
(97/32/80) with isolated PostgreSQL, 821 core/risk (78 risk), and
60 engine tests passed: 1,090 total, zero failures, six external
tests ignored. Workspace all-target Clippy with warnings denied,
formatting and diff checks passed. Disposable PostgreSQL stopped and
removed. Full workspace build/test, independent review, remote CI
and real-service qualification remain pending. No real orders or
production database operations.

## Remaining work

Next qualify read-only fee-valuation history evidence. Fee-bearing
realized P&L, actual risk/ledger state application, full runtime
atomicity, source completeness and authoritative acquisition remain
open. Keep the one-time startup restriction and do not replay
completed callbacks.
