# F03.02 — Simulated EMS child transit

## Scope

Opt-in event-time latency now applies to each child separately from the
existing direct/parent-admission delay. Legacy immediate and live
Postgres EMS execution remain unchanged. The existing fixed-route
causal scheduler determines slice submissions, and a bounded FIFO
queue (1,024 pending children) gives each accepted child a stable ID
and a deadline at observed submission time plus configured delay.

After each non-regressing market event has updated matching depth,
drain due children before scheduling additional slices. Match against
the same causal book as other orders and record the observed arrival
timestamp, not an invented intermediate timestamp. Children remain
New until arrival; execution is terminal (Filled/Cancelled/Rejected),
and partial IOC quantities retain their original ID. Competing child
arrivals are FIFO; already resting maker execution precedes EMS
arrivals, which precede direct-order arrival processing.

Parent scheduling completion remains externally Scheduling until every
accepted child is terminal. Track child IDs for every algorithm so
parent cancellation stops pending children. Expire undelivered children
at replay end without inventing future events. Report scheduled, arrived,
refused, rejected-at-arrival, cancelled-before-arrival and expired counts
separately from direct/parent admission counts.

## Boundaries

A parent Completed status describes an exhausted schedule and terminal
children, not a guarantee of full execution. Child-specific independent
risk checks and shared capital reservations remain separate work; this
increment does not establish portfolio-safe execution. Historical queue
calibration, data provenance and range continuity remain open. No live
trading or production database changes are intended.

## Acceptance

Validate FIFO and shared arrival depth, stable order IDs, partial
execution, cancellation and end expiry, stale-depth rejection, parent
completion, and backtest/paper integration. Require locked workspace
build, Clippy, serial regressions, formatting and remote CI before
declaring this increment qualified.

## Local qualification

Rust 1.99.0, locked dependencies: workspace build, all-target workspace
Clippy with warnings denied, formatting and diff checks passed. All 80
test binaries ran serially in split groups: 1,455 passed (985 core,
144 database, 63 supervisor, 263 UI), zero failures, 44 ignored
external/manual tests. Isolated temporary PostgreSQL was stopped after
qualification. All 30 doc-test suites passed. Five new child-transit
regressions and the updated TWAP replay integration passed. No live
orders or production databases were used. Independent review and remote
CI remain pending.
