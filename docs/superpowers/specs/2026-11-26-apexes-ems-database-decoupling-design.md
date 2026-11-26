# F01.04 — EMS database decoupling

## Objective

Keep the live market-event loop independent of EMS persistence and slow
algorithmic child submission. Retain PostgreSQL as the authoritative
scheduling record. Never permit an unrecorded child dispatch to be
silently repeated after a database failure or restart.

## Index and scheduling

PostgresEms reconstructs an in-memory index of scheduling POV parents by
(venue, symbol) and a cache of known parent statuses from persisted rows
at startup, before any scheduling tasks are started. Parent creation is
persisted before registering a new parent; committed cancellation,
completion and failure remove it from the active index. Known parent
status polling and irrelevant market events need no PostgreSQL access.
The index lock is short and never spans network I/O. Historical parent
statuses are retained in memory for the lifetime of the instance; cache
retention and multi-session parent ownership are outside this feature.

For matching trades, enqueue the parent IDs observed at event arrival
into a bounded 256-item POV worker queue. The worker executes outside
the trading loop, validates the persisted parent before dispatch, and
caps each slice at remaining quantity. Queued work older than two
seconds, overflow or a stopped worker triggers a latched scheduling
fault. TWAP/VWAP/Iceberg remain background paced. Backtest and paper
retain their existing deterministic in-memory EMS semantics.

## Persistence and failure policy

Schema migration adds dispatch_pending (default false). Before contacting
the child sink, atomically reserve one dispatch on the scheduling parent
with a conditional UPDATE, checking remaining quantity and excluding any
other pending dispatch. After acknowledgement, record the child link and
advance the submitted quantity in the same PostgreSQL transaction, clearing
the marker only when that transaction commits. A failed or interrupted
write leaves the marker set. A marker at startup refuses recovery rather
than guessing whether the exchange received the child. Cancellation
refuses to declare a parent cancelled while a dispatch is pending; resolve
the child first. Markers are conservative: even a failure before actual
exchange submission can require reconciliation. This does not provide
full cross-process child-order attribution or automatically repair earlier
history; existing live OMS reconciliation and F02.02 remain necessary.

## Database health and live protection

A dedicated monitor checks PostgreSQL every two seconds with a two-second
probe timeout. Pool acquisition and server-side statement timeouts are
also two seconds. Failure, scheduling/persistence errors or lost queue
work latch the fault and engage the live session's existing shared OMS
kill switch. Market events and cached parent status remain available;
new algorithmic scheduling is refused. Already in-flight dispatches may
finish bookkeeping. Releasing the kill switch alone cannot clear the EMS
fault; reconcile orders, positions and parent/child records before restart.
A healthy connection alone is not authorization to resume. F01.05 will
add explicit session health and operator recovery. Explicit flatten and
OMS risk semantics are unchanged. A deployment must not share the same
unscoped EMS parent tables between independent live sessions; the existing
single-owner recovery model is retained, not replaced by distributed
scheduling or tenant isolation.

## Verification

Use isolated PostgreSQL and mock child sinks. Cover an inactive/unmatched
hot path with a closed pool, durable recovery and cancellation, a failed
child-link INSERT, refusal to restart with a pending marker, and a blocked
child sink with bounded queue overflow. Verify the already-dispatched child
is recorded once and no further child is dispatched after a fault. Run
workspace build, serial regressions, fmt, Clippy and remote CI. Do not use
live databases, exchange credentials or real orders for these tests.
