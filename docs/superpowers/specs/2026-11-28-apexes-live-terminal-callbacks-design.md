# F04.04 — Authoritative managed live terminal callbacks

## Durable evidence

Use the existing bound OMS execution journal and single-writer lease.
The existing order/fill reconciliation transaction now assigns a nullable
order-level terminal_delivery_seq using a schema-local sequence when an
actual working order transitions to Filled, Cancelled or Rejected with a
positive exchange order identity and a validated cumulative execution
snapshot. Failed reconciliation, incomplete pages, contradictory
quantities and unknown outcomes cannot produce this marker. Rollback
does not publish evidence. Legacy terminal rows are left unmarked;
immediate definite submission refusals stay on the existing attempt
callback path and are not duplicated as reconciled terminals.

OrderStore::terminal_evidence reads the marked order, actual persisted
execution total, required fill delivery sequence and immutable scoped
intent/EMS linkage in one SQL snapshot. It validates order/fill identity,
positive quantities, terminal status constraints and accepted submission
identity. Absence of the marker is not proof of nonexecution.
The marker is stable across repeated exchange updates. The returned order
is the latest reconciled snapshot, not an immutable history of every
exchange response or an acknowledgement of strategy execution.

## Delivery and ordering

After quiescent startup inventory recovery, the managed manager records
the current terminal sequence as its process-local baseline. No existing
historical terminal is automatically replayed. For new markers, query a
bounded prefix of up to 128 terminal orders in marker order. Do not
overtake an earlier marker whose persisted executions exceed the
recovered/notification-return fill watermark. A ledger acknowledgement
without returned fill callbacks is insufficient. Terminal-only work
uses the existing supplemental stream; a committed marker or completed
notification receipt wakes it through a coalescing Notify even without
market activity. An empty batch emitted for that wake does not invent
a fill, record an equity point or drive a market strategy decision.
Periodic reconciliation remains a fallback. Readiness probing is
separate from staging the receipt; only the shared loop stages and
acknowledges the actual callback batch.

The live-only terminal variant includes the reconciled order, typed
status, optional verified EMS parent and child dispatch IDs. The current
strategy sees an intent ordinal only from its own run scope. EMS child
ownership is checked by the same exact parent/dispatch classifier used
for fill attribution, including zero-fill terminals and pending
associations. If parent attribution is unavailable, preserve the positive
OMS terminal evidence, omit inferred ownership and latch health. The
Drift callback retains existing kind/order/quantity fields and adds
source=live, parent_id and dispatch_id for live evidence only; UUIDs and
intent ordinals remain strings. Existing simulated and transit payloads
are unchanged. Native exhaustive matches on TerminalKind must handle
the additive Live variant.

The bounded queries occur on supplemental work, never on every market
tick. The same loop, ledger, order admission, protective exits and
simulated execution path remain in place. No automatic dispatch repair,
exchange retry, order submission or capital release is introduced.

## Receipt and failure boundaries

A process-local terminal cursor advances only after the exact ordered
batch's synchronous callbacks return. A wrong or missing receipt or
invalid evidence faults the health gate and latches terminal delivery
failure, without disabling the existing authoritative fill ledger path.
If an already-terminal order receives a later positive revised
execution quantity, persist and deliver the execution as usual, fault
health and retain the original marker rather than issue a second
terminal callback. Read-only evidence reflects the revised quantity.
There is no separate strategy-state checkpoint or crash-safe callback
receipt; a crash before or during a callback may lose its delivery.
Startup deliberately does not claim callbacks returned. This is not
cross-restart exactly-once delivery or automatic recovery authorization.

The terminal status is the currently normalized exchange status, not
a newly inferred exchange-specific expiry/trigger motive. Ambiguous
trigger outcomes, terminal correction/replay qualification, atomic
strategy-state recovery and real-service qualification remain open.

## Verification

Rust 1.99.0, locked dependencies, serial tests, warnings denied.
The complete PostgreSQL CI sequence passed on a verified disposable
PostgreSQL 17 instance (85 OMS, 32 EMS, 44 live-trading = 161 tests),
then the instance was stopped and removed. The initial two fixture
failures correctly exercised existing same-spend-asset reservation
guards; the fixtures now respect admission and the final sequence is
green. Regressions cover fill-before-terminal acknowledgement, marker
ordering, rejected and zero-fill cancelled orders, stale push and failed
execution query, restart without historical replay, receipt mismatch,
late revised terminal quantity with continuing fill delivery, immediate
submission refusal, scoped direct/EMS child callbacks after actual fill
callbacks, and idle terminal-only wake. The Drift payload regression
checks exact IDs and actual partial execution quantity. 715 nonignored
core tests passed; six external tests were ignored. Workspace-wide
all-target Clippy and formatting/diff checks passed. Full workspace
build/test, remote CI and independent review remain pending. No real
exchange orders or production database operations.
