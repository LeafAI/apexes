# F04.05 — Actual basic Spot ledger evidence

## Scope

Inspect the actual loop-owned basic portfolio ledger at the completed
checkpoint boundary. This is partial read-only evidence, not a restore
format, independently authenticated wallet snapshot, or startup authority.
No accounting or execution decisions are changed.

## Capture and validation

Retain a versioned, canonical symbol-sorted actual instrument scope,
asset-sorted balances, position entries (including actual retained flat
entries), and symbol-sorted realized P&L. Inspect the ledger itself, not
a reconstructed report or independently supplied wallet. Refuse
non-Spot instruments, inconsistent map keys, unsupported leveraged or
negative position state, invalid keys, duplicate keys and invalid amounts.
Bound scope, positions and realized P&L to 128 entries and balances to
1,024 entries. Verify the exact captured observation scope in the
attachment, with unique symbols, regardless of input scope order.
Do not discard flat entries or invent absent asset balances.

The existing opt-in completed checkpoint attaches this evidence before
canonical encoding and publication. Existing attachment digest and
transaction cover the additional bytes. Legacy attachments omit the
optional field unchanged; absence does not establish ledger coverage.
Checkpoint-only and ordinary callers are unchanged.

## Limits

This is basic ledger-map capture only. It excludes internal Spot fee
attribution and valuation history, instrument rule/configuration details
beyond venue and symbol, external balance provenance, and independent
reconstruction of the retained realized P&L. Capture does not imply
that local and exchange balances match or that the risk and ledger
snapshots are globally atomic with the subsequent database commit.
Do not apply the captured state or use it as a restart token.

## Qualification

New ledger unit test compares actual balances, position cost basis and
realized P&L after a buy and partial sale, checks retained flat entries
after a complete sale, unchanged repeated capture, wrong scope and
duplicate balance refusal. Extend the existing isolated coherent-stop
PostgreSQL integration to compare actual captured ledger balances,
quantity and entry price against independent synthetic fixture inputs
and reopen the exact durable attachment.

Rust 1.99.0, locked dependencies, serial tests: 207 OMS/EMS/live tests
(97/32/78), 821 core/risk (78 risk), and 60 engine tests passed,
1,088 total, zero failures, six external tests ignored. Workspace
all-target Clippy with warnings denied, formatting and diff checks
passed. Disposable PostgreSQL stopped and removed. Full workspace
build/test, independent review, remote CI and real-service qualification
remain pending. No real orders or production database operations.

## Remaining work

Next qualify captured ledger reconciliation against independent evidence.
Complete fee-valuation history, actual risk/ledger state application,
full runtime atomicity, source completeness and authoritative acquisition
remain unqualified. Keep the one-time startup restriction and no replay
of completed callbacks.
