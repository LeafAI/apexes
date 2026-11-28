# F04.05 — Captured basic ledger reconciliation

## Scope

Compare actual durable basic Spot ledger evidence against the selected
journal and independently supplied wallet and strategy-owned position
evidence. This is offline comparison, not a ledger restore operation.

## Contract

Reuse the existing exact opening-to-closing cash-flow continuity and
wallet/terminal evidence checks, and verify the independently retained
closing owned positions against the reconstructed journal preview. Read
the actual durable capture again using the exact selected receipt and
journal verification. Refuse missing or invalid ledger evidence.

Require exact instrument scope and explicit equal asset coverage;
compare each captured balance with the qualified closing free balance
and require zero locked balance. Do not infer missing assets or attribute
unrelated account holdings to the strategy. Compare all nonzero
captured positions to the journal-owned preview, including quantities
and average entry prices. Retained flat entries are not open positions.

No changes to ledger state, callback receipts, startup claims, risk
decisions, accounting or execution behavior.

## Limits

This deliberately qualifies only the supported basic balances and
nonzero positions when account and local-ledger asset coverage match
exactly. It refuses accounts with unrelated holdings rather than
silently assuming a local ownership allocation. Realized P&L and fee
valuation history remain unqualified. Independently supplied evidence
is not authenticated by this API; equality cannot prove source
completeness, full runtime atomicity or restart authorization.

## Qualification

Extend the existing isolated coherent-stop PostgreSQL integration to
refuse four BTC of unrelated opening account holdings even when the
existing wallet and journal cash-flow checks pass, then qualify the
independently specified session-only wallet fixture with one BTC at
the closing boundary. Existing negative position and wallet checks
remain in place.

Rust 1.99.0, locked dependencies, serial tests: 207 OMS/EMS/live tests
(97/32/78), 821 core/risk (78 risk), and 60 engine tests passed,
1,088 total, zero failures, six external tests ignored. Workspace
all-target Clippy with warnings denied, formatting and diff checks
passed. Disposable PostgreSQL stopped and removed. Full workspace
build/test, independent review, remote CI and real-service qualification
remain pending. No real orders or production database operations.

## Remaining work

Next qualify independent realized P&L evidence and fee-valuation history.
Actual risk/ledger state application, full runtime atomicity, source
completeness and authoritative acquisition remain unqualified. Keep
the one-time startup restriction and do not replay completed callbacks.
