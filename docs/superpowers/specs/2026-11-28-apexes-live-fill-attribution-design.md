# F04.04 — Managed live fill attribution and notification progress

## Scope and evidence

Reuse the existing schema-bound OMS fill journal, delivery sequence and
immutable direct intent / EMS dispatch links. Do not create another fill
table, notification journal or inferred ownership for legacy rows.
The existing managed single-writer inventory lease and startup replay
assumptions still apply.

OrderStore::fill_identity / fill_identities reads the exact persisted fill
ID, OMS order ID, exchange trade ID, delivery sequence, direct run-scoped
intent and/or EMS child dispatch evidence in one SQL snapshot. The batch
result is aligned to the requested IDs; absent rows are unknown, not
negative execution evidence. Inconsistent fill/order identity, malformed
partial links, conflicting direct/child ownership or invalid execution
payload is refused. Actual executions are never discarded on the basis
of attribution availability.

The read-only EMS inspect_fill_parent API uses an already migrated,
correctly account/schema-bound pool and a repeatable-read transaction.
It validates the exact fill, OMS child, EMS parent, instrument, side,
quantity bounds and parent intent. It searches recorded associations by
both order ID and dispatch ID across the schema; conflicting or multiple
owners are not treated as missing. Exact matching recorded associations
are distinguished from pending dispatch markers. Pending may be in flight
or interrupted; it is not a completed receipt or authorization to repair,
restart, retry or release capital. Parent filled_quantity remains
scheduled quantity, not actual executed quantity. The live EMS lookup
requires the same correctly bound OMS/EMS schema, not merely a similar
configuration or a matching symbol.

## Shared-loop delivery

Only the managed live supplemental path opts into persisted linkage.
After existing ledger bookkeeping and acknowledgement, the executor
loads aligned OMS evidence and checks the persisted payload against the
actual newly applied fill; the EMS backend resolves exact child evidence.
The attribution SQL operations are bounded to two seconds each for the
managed callback path (not a guarantee on unrelated manager-lock waits).
No additional queries are made on market-only events. No new submission,
reconciliation or scheduling authority is introduced.

The current strategy receives an intent ordinal only if its persisted
run scope equals the live executor's scope. A prior run's ordinal is
never reused, even if the number is equal. Exact child evidence supplies
the parent ID and locally notified quantity; dispatch identity is exposed
through the evidence API, not a breaking change to FillOutcome. When
evidence is missing or contradictory, keep the committed fill and
callback, omit inferred ownership, fault the live health gate, and
suppress cumulative live parent progress for the rest of that process.
Legacy/unmanaged executors and simulated/historical callbacks retain
their existing path, deduplication and economic replay fingerprints.

## Notification progress

The ledger acknowledgement cursor remains independent and authoritative.
For a successfully applied batch, retain its ordered IDs and sequence
watermark in memory. After each synchronous, deduplicated callback has
actually returned, acknowledge the exact newly applied batch. A duplicate,
mismatched or missing preceding receipt cannot advance notification
progress. A notification fault latches submission restrictions without
invalidating an applied ledger or preventing subsequent authoritative
fill delivery. The normal ledger-discrepancy halt remains unchanged.

FillDeliveryProgress distinguishes the inventory recovery baseline, the
ledger checkpoint, optional current-process notification-return watermark
and independent ledger/notification failures. Restart reconstructs
inventory and begins with no current-process notification receipt; it
does not replay callbacks or claim prior strategy code completed. There
is no exactly-once callback guarantee across process crashes. Durable
strategy-state/notification atomicity and audited recovery remain
separate follow-up work. Authoritative terminal outcomes and ambiguous
trigger evidence are next in PLAN.md.

## Verification

Rust 1.99.0, locked dependencies, warnings denied, serial tests. All 155
OMS/EMS/live-trading tests (80/32/43) passed in the exact PostgreSQL CI
sequence on a verified disposable PostgreSQL 17 cluster, stopped and
removed afterward. Regressions cover exact direct fill identity and
restart baseline, mismatched notification receipt with continuing ledger
delivery, recorded EMS child and interrupted child after reopened OMS,
conflicting ownership, and managed live end-to-end callbacks for a prior
run's identical ordinal, current direct order and current EMS child.
714 nonignored backtest/strategy/loader/paper/indicators/Binance gateway
tests passed; six external tests were ignored. Formatting and workspace-wide
all-target Clippy passed. Workspace CI and independent review remain
pending. No real exchange orders or production database operations.
