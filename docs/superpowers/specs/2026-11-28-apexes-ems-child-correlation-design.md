# F04.04 — Simulated EMS child execution correlation

The in-memory EMS records newly associated (child OrderId, ParentOrderId)
pairs when submit_slice successfully returns a child ID and the existing
parent state records that ID. The single trading loop drains this
incremental evidence after successful EMS market processing and before
fill notifications. It does not rescan all historical children, change
order identity, submit orders, or query any service. Deferred simulated
child fills already preserve the scheduled child OrderId.

The correlation map accepts associations only for parents tracked from
actual submission results. Repeating the same association is harmless.
Conflicting parent ownership or a collision with an existing direct
order permanently makes that child association unavailable and marks the
affected parent's future cumulative progress unavailable. Previous
notifications are not retracted. No association is inferred from a
symbol, quantity, timestamp or delivery source.

FillOutcome adds optional parent_id and parent_executed_quantity; the
existing intent_id resolves through actual parent ownership. Drift uses
a UUID string or null for parent_id, and the existing numeric convention
for quantity. The native quantity is Decimal. Progress is the sum of
unique locally accounted and notified fills with known ownership in the
current session, not submitted quantity, percent complete, inventory,
or proof of full exchange execution. The existing fill-ID notification
deduplication runs before accumulation. Nonpositive quantity or checked
overflow makes subsequent progress unavailable. Unknown ownership does
not produce a fabricated zero total.

No new callback, report field, database schema, exchange query, matching
logic, risk check or reservation path. Native FillOutcome literal
callers must specify the new optional fields. The default EMS evidence
method returns no associations; PostgreSQL/live EMS and custom backends
without explicit support remain unassociated. The single-consumer
incremental evidence is not a durable journal; standalone in-memory EMS
callers that do not drain evidence retain it until instance destruction.
Session-local maps grow with execution history. Progress is not recovered
after restart. Ownership conflicts invalidate future affected totals, not
previously delivered snapshots.

Verification: extended ten-run deterministic two-parent replay asserts
actual fill/order/parent/intent IDs and separate cumulative progress
(1, 2, 2, 4 in fill occurrence order), identical fingerprints and
unchanged execution/reservation assertions. Two additional unit tests
cover repeated/contradictory child ownership, direct-order collision,
duplicate fill notification, checked overflow and unknown supplemental
ownership. A missing Decimal import in the extended test was corrected.

Rust 1.99.0 locked offline serial: 632 nonignored tests passed
(441 backtest, 123 strategy, 23 EMS, 15 paper, 25 indicators, 5 loader).
12 external tests ignored. All-target Clippy with warnings denied passed
for strategy, loader, backtest, paper, live-trading, engine and EMS.
Formatting and diff checks passed. Full workspace CI, independent review,
durable recovery and live-service qualification remain pending.
No live orders or production accounts/databases used.

Next: durable lifecycle correlation and recovery across restarts.
