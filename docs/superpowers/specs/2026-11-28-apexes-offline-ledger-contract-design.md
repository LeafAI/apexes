# F04.05 — Offline closing position evidence

## Scope

Extend the existing read-only opening-to-closing wallet cash-flow
continuity check to compare independently supplied strategy-owned Spot
position quantities and weighted average entry prices. Do not change
runtime accounting, execution or recovery authorization.

## Contract

Versioned closing position evidence binds the exact selected coherent
receipt and owner, with a supplied capture timestamp within the closing
wallet evidence interval and no earlier than the checkpoint cursor.
Require explicit complete sorted instrument-symbol scope, including flat
instruments. Restrict to the existing supported Binance Spot scope.
Bound scope and position counts to 128; reject duplicate, malformed,
unknown or noncanonical symbols and nonpositive position quantities or
entry prices.

The read-only selection method first runs existing journal, strategy
preview, terminal evidence, wallet and cash-flow continuity checks.
Compare the independently supplied nonzero positions with the exact
reconstructed strategy-owned positions, rejecting missing or unexpected
positions, quantities, cost basis, or leveraged/margin state. Account
wallet holdings are not strategy-owned positions. No evidence is
synthesized from the preview. The new method returns only comparison
success or refusal, never a runnable restored ledger or authorization.

## Limits

The comparison uses the existing weighted cost basis semantics, not
complete accounting for all fee/valuation models. Supplied position
evidence is not authenticated or necessarily atomic with wallet
observations. No actual ledger snapshot is captured in this slice and
no state is applied. This is not proof of source completeness, current
exchange state, or authority to resume execution.

## Qualification

Extend the existing isolated coherent-stop PostgreSQL integration with
an independently expected synthetic position (one BTC at average entry
price 100), and refusal cases for changed cost basis, omitted owned
position and incorrect scope. All existing continuity and checkpoint
tests remain unchanged in behavior.

Rust 1.99.0, locked dependencies, serial tests: 207 OMS/EMS/live tests
(97/32/78), 820 core/risk (78 risk), and 60 engine tests passed,
1,087 total, zero failures, six external tests ignored. Workspace
all-target Clippy with warnings denied, formatting and diff checks
passed. Disposable PostgreSQL stopped and removed. Full workspace
build/test, independent review, remote CI and real-service qualification
remain pending. No real orders or production database operations.

## Remaining work

Next qualify actual read-only ledger evidence capture. Full accounting,
runtime atomicity, actual risk/ledger application, source completeness
and authoritative acquisition remain unqualified. Keep the one-time
startup restriction; no repeatable startup or callback replay.
