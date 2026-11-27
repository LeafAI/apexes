# F03.02 — Simulated emergency reservation preemption

## Contract

Opt-in event-time simulation gives automatic margin liquidation and
extreme-market de-risking priority over ordinary outstanding orders.
After updating observed marks and market-condition state, settle funding
for positions held entering the event and sample marked equity once.
When the configured liquidation policy permits an emergency exit and
a current position needs reduction, preempt all simulated account orders
before resting/child matching. Account-wide cancellation is deliberately
conservative: symbols can share the same spend assets and reservations.

Drain pending strategy intents, remove working limits and stop triggers
from the matcher, and cancel scheduling EMS parents and their pending
children. Release transit holds and reconcile confirmed working-order
and parent cancellations. Preserve event/depth ingestion so the emergency
exit can use observed liquidity. Defensive pre-commit checks reject
ordinary resting and child proposals on that emergency event even if
cancellation fails. Record failures and continue the emergency attempt.
If matching creates a new margin breach, preempt outstanding orders
before the later liquidation attempt. No new strategy orders may enter
on the emergency event; subsequent events are evaluated normally.

Report separate counts of emergency events, canceled transit/resting
orders and parents, cancellation failures and refused strategy intents.
No live cancellation API is used. Legacy immediate execution and live
funding/equity ordering remain unchanged.

## Boundaries

This is a simulation-only cancellation policy, not a persistent trading
halt or exchange liquidation model. Account-wide cancellation may remove
unrelated protective orders. The existing emergency market execution
and ledger settlement semantics are preserved, including liquidity and
fee constraints; cancellation does not guarantee execution. Equity
effects of fills after the pre-match sample enter the monitor on the
next event. Position-reducing margin optimization, historical queue
calibration and source provenance/continuity remain open.

## Local qualification

Rust 1.99.0 locked workspace build, all-target workspace Clippy with
warnings denied, formatting and diff checks passed. All 80 test
binaries exercised serially: 1,483 passed (1,013 core, 144 database,
63 supervisor, 263 UI), zero failures, 44 external/manual tests ignored.
All 30 doc-test suites passed. Two new end-to-end regressions cover
margin liquidation and extreme-market de-risking preempting competing
resting, EMS child and transit orders, including same-event arrival,
capital release and suppression of new intents. Temporary isolated
PostgreSQL was stopped. No live orders or production databases used.
Remote CI and independent review remain pending.
