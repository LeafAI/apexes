# F03.02 — Simulated Spot resting-order capital ownership

## Scope and contract

Event-time simulated Spot intents reserve spend assets before transit.
Limit and stop-limit buys reserve the full quantity at the limit price
plus the greater of the configured maker/taker fees. Stop-market buys
reserve an estimate at the stop price with the same fee allowance.
Spot sells reserve base inventory. Existing market-intent and EMS
parent holds participate in the same spend-asset accounting.

After direct admission, only the unfilled working quantity retains
its proportional hold, keyed by the original order ID. Before any
resting maker fill, triggered market stop, or triggered limit crossing
consumes liquidity, validate the proposed fills against a projected
ledger and the remaining holds. A partial maker or crossing fill
releases its own proportional share; a terminal market stop releases
its own entire hold. A price gap cannot consume another order's
reservation. Rejected maker proposals cancel the unfundable order
without spending the proposed print quantity; rejected stop proposals
do not consume book depth. Reconcile working-order identities after
each event to release terminal or invalidated orders; release residual
holds at replay end. Refused fills and reservation lifetimes have
separate report counters.

This is conservative event-time simulation accounting. Rejected
working orders retain their residual budget until the end of the
current event's reconciliation. Leveraged orders do not yet have
dedicated reservations, and emergency liquidation/de-risking does
not yet reconcile or preempt conflicting holds. Legacy immediate
and live execution paths are unchanged. Historical queue calibration,
source provenance and continuity remain unqualified; F03.02 remains
in progress.

## Local qualification

Rust 1.99.0 locked workspace build, all-target workspace Clippy with
warnings denied, formatting and diff checks passed. All 80 test
binaries ran serially in groups: 1,474 passed (1,004 core, 144 database,
63 supervisor, 263 UI), zero failures, 44 external/manual tests ignored.
All 30 doc-test suites passed. Six new regressions cover partial
resting execution and competing holds, stop-market gaps, replay-end
release, rejected maker print budgets, rejected stop-limit depth
preservation, and fee-inclusive limit valuation. Temporary isolated
PostgreSQL was stopped after qualification. No live orders or
production databases were used. Remote CI and independent review
remain pending.
