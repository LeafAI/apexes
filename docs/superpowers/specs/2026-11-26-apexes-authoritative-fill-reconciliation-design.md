# F02.02 — Authoritative fill reconciliation

## Purpose

Make the exchange execution journal authoritative. An order status alone
does not establish that its fills have arrived, and a successful database
write alone does not establish that the trading ledger applied those
fills. Preserve the single unified event loop.

## Exchange evidence and atomic persistence

Repeated PartiallyFilled updates and acknowledged cancellations query
executions, even if an order's status did not change. In the managed Spot
path, a push is only a wake-up: query current exchange status and
cumulative executed quantity, validate the symbol and exchange order ID,
then fetch the trades. Reject inconsistent cumulative quantities, a
regressive quantity, contradictory terminal status, conflicting exchange
trade identity/payload, or incomplete fill pages. Existing deduplication
by order ID and trade ID remains; the managed path also checks venue and
symbol trade identity. Exchange order ID and symbol identify a known
order even when Binance changes its client ID during cancellation.

Insert new fills and update order status/filled quantity in the same
PostgreSQL transaction. A failed check or write rolls back the update;
the order remains available for reconciliation. A confirmed cancellation
does not retire an order before reconciling executions. Standalone legacy
gateways may retain their status-only contract; managed Spot requires
authoritative cumulative quantity. No automatic retries submit new orders.

## Durable delivery and acknowledgement

Migrate fills with a unique delivery_seq watermark. Under the single
managed schema ownership lease, the manager serializes its writes.
Startup rebuilds the ledger from all durable fills as in F02.01, and
establishes a process-local acknowledgement checkpoint before starting
the event loop. The supplemental stream reads batches of at most 256
committed executions after that checkpoint, independently of whether a
push, reconciliation or cancellation task returned the original fills.

Sort each batch by exchange execution time and trade identity. A late
execution preceding already applied history for the same symbol is
durably retained but is not applied out of order: latch the fault and
require a chronological rebuild on restart. Ordinary submissions remain
blocked while a committed batch awaits acknowledgement. The shared loop
acknowledges the exact fill IDs after ledger application. Duplicate
local fill IDs do not change positions or performance twice. A rejected
ledger fill is not reported as an applied trade, and the batch failure
latches managed session health. The durable journal remains intact.
Reconcile/cancel paths remain available while halted.

## Limits and operational policy

The acknowledgement cursor is process-local; restart reconstructs the
whole ledger rather than trusting a persisted application cursor. This
depends on the dedicated, single-writer, quiescent startup boundary from
F02.01, not on distributed exactly-once delivery. Late execution,
contradictory exchange evidence or a rejected ledger update requires
manual reconciliation and restart; there is no online correction of an
already applied weighted cost basis. In-flight requests may finish.
A fill query whose page is incomplete relative to cumulative executed
quantity causes a safe halt, not an assumed complete history. Automatic
multi-page historical recovery, periodic account-wide balance checks,
historical terminal-order scanning, fee conversion and continuous
reservation tracking are outside this iteration; see F02.03/F02.05
and production qualification. No real-exchange orders are used for
qualification.

## Verification

Isolated PostgreSQL tests cover repeated partial updates, fills during
cancellation, acknowledgement gating, contradictory duplicates with
transaction rollback, incomplete terminal evidence, late execution,
and a failed ledger acknowledgement. WireMock tests the cumulative
quantity response. Shared-loop regressions check duplicate and rejected
fills are not counted as successful ledger applications. Workspace build,
fmt, Clippy, serial test binaries and doc tests, plus required remote CI,
remain the release gates.
