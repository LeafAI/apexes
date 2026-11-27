# F03.02 — Simulated Spot EMS parent capital ownership

## Scope and contract

Event-time Spot market-intent holds are revalued at parent admission,
then transferred to a reservation keyed by the admitted parent ID.
Causal EMS scheduling carries that ID with each child through transit.
A child can release only its own proportional share of the remaining
requested quantity's budget. The complete proposed fill batch is
validated against the projected ledger and remaining reservations
before depth is consumed. Price increases can draw from genuinely
unreserved funds, not another parent's or sibling slice's allocation.
Partial or zero execution of an accepted terminal IOC child releases
its allocated share. A child that fails the arrival guard or matcher
retains its conservative share until terminal parent reconciliation.
Unknown parent status does not release funds. Completed, cancelled or
failed parent status releases the residual hold; replay termination
releases any remaining reservations. Report counters record parent
reservations and release outcomes separately from transit holds.

Admission is restricted to market intents for simulated Spot parents.
Legacy immediate execution and live execution are unchanged. This is
simulation accounting, not durable live exchange reservation. Existing
resting limit/stop fills and emergency exits are not yet integrated into
the capital-hold model, and leveraged orders are outside this increment.
F03.02 remains in progress.

## Local qualification

Rust 1.99.0 locked workspace build, all-target workspace Clippy with
warnings denied, formatting and diff checks passed. All 80 test
binaries ran serially in groups: 1,468 passed (998 core, 144 database,
63 supervisor, 263 UI), zero failures, 44 external/manual tests ignored.
All 30 doc-test suites passed. Four new deterministic regressions cover
parent competition, proportional child allocation, terminal and unknown
status reconciliation, ownership mismatch and replay-end release.
Temporary isolated PostgreSQL was stopped after qualification.
No live orders or production databases used.
Remote CI and independent review pending.
