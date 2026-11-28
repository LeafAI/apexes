# F04.05 — Offline terminal execution evidence validation

## Scope

Extend the independent coherent recovery preview from PR #103 with
a pure read-only comparison of independently supplied terminal order
and trade evidence. This is the comparison layer, not a network evidence
collector or an authoritative live reconciliation result. The caller
must establish completeness, provenance and freshness separately.
No gateway requests, startup claims, callback replay, order mutations
or changes to execution eligibility are introduced.

## Comparison contract

Use the bounded journal snapshot and exact client order identity as the
join key. Require Binance direct Spot terminal orders, valid positive
order quantity, no leverage/margin, cumulative execution within bounds
and exact full quantity for filled status. Refuse missing or duplicate
local order/fill IDs, duplicate exchange order/trade identities within
venue/symbol, invalid local fill shape and inconsistent cumulative
quantity (checked decimal arithmetic).

Each supplied order record binds client ID, venue, asset class, symbol
and side. Require one record per local order, including zero-fill
cancellations and rejected orders. Unknown, duplicate or missing order
evidence fails. A local definite rejection without exchange ID or
fills requires an explicitly definite negative lookup; never reinterpret
an error or timeout as NotFound. For a previously accepted order require
the exact exchange order ID, terminal status and cumulative executed
quantity. Compare the complete trade set by trade ID independent of
ordering. Refuse extra/late, missing or duplicate trades and differences
in price, quantity, fee, fee currency or persisted timestamp. Compare
timestamps at PostgreSQL microsecond precision, not unretained
submicrosecond precision.

Bound the comparison to 2,048 orders and 8,192 fills, matching recovery
inspection limits. Do not infer that a matching set constitutes an
authenticated or current exchange response.

## Integration

CoherentRecoverySelection::inspect_terminal_evidence reads the exact
owner-bound journal using the independently selected original claim,
checkpoint receipt and coherent boundary. Compare the supplied evidence,
then reverify the original selection and journal. Do not initialize
delivery baselines or alter the startup claim. A successful comparison
returns only unit; it is not a startup authorization or a risk token.
A later journal mutation must cause inspection to fail, including when
sequence counts do not change.

## Qualification

Six new unit tests cover matching evidence in arbitrary order, zero-fill
terminal and rejected orders, missing/duplicate/unknown orders, scope
and remote identity changes, late cumulative execution, status changes,
exact trade-set mismatch, price/quantity/fee/currency/timestamp changes,
inconsistent local journal, capacity and storage precision.

Extend the existing coherent-stop integration test with independently
retained synthetic exchange fixture evidence. Compare it against the
actual reopened PostgreSQL checkpoint; reject modified fees, absent
order evidence and later journal mutation. No live exchange requests.

Rust 1.99.0, locked dependencies, serial tests: 188 OMS/EMS/live-trading
tests (97/32/59) passed using an isolated PostgreSQL test instance; 733
core and 60 engine tests passed, 981 total, zero failures, six external
tests ignored. Workspace-wide all-target Clippy with warnings denied,
format and diff checks passed. Disposable database stopped and removed.
Full workspace build/test, remote CI, independent review and real-service
qualification pending.

## Remaining work

Authoritative read-only collection of terminal identities and trade
history, completeness and freshness validation, foreign/open-order
checks and current wallet evidence remain unimplemented. Full
ledger/risk/observation continuity and durable run transition must be
qualified before repeatable restart is permitted. The existing one-time
claim restriction remains in force. No periodic capture or causal
no-order warm-up is enabled by this slice.
