# F04.05 — Coherent stop checkpoint publication

## Scope

The previous verified startup accepts only a single-use checkpoint for
an execution-history-free managed Spot inventory. Keep that restriction.
This slice establishes a durable, coherent execution/observation boundary
at the completed stop of that same run. It does not authorize another
startup from a consumed claim, even if the new checkpoint verifies.
Default live, paper, backtest and engine CLI behavior remains unchanged
except that managed legacy startup also refuses an orphan boundary record.
Fix the existing owner adoption check to account for a claim even when
the checkpoint head is absent.

An explicit library caller uses
`run_live_trading_with_checkpoint_on_stop` with the existing independently
selected restored startup. No new CLI/UI/supervisor configuration and no
periodic publication. The caller must qualify deterministic, scoped-only
state and callback behavior. A native strategy may also opt in to the
existing checkpoint contract. Callback return is not a general proof of
successful semantic processing by arbitrary scripts; failures that the
strategy itself hides cannot be inferred by the checkpoint system.

## Actual unified-loop capture

Only an executor with an explicit predecessor requests capture. Start the
committed market observation cursor at the verified predecessor. Count
each fully processed market iteration (including paused decisions), after
synchronous order-attempt, market, applied-fill and terminal callbacks
have returned. Control commands drained before the loop stops are also
represented in the strategy payload. Require at least one newly completed
market iteration, no ordinal overflow, no regressing logical timestamp
or timestamp earlier than source availability (bars at close). No
simulated event-latency capture. A callback panic or loop error does not
publish. Supplemental deliveries do not fabricate a market ordinal:
their independent callback-return watermarks are part of the receipt.

Capture the exact payload once after the loop exits normally, with the
actual last allocated session-local intent ordinal. Delegate publication
through the live executor while holding the same OMS manager. Refuse
failure to obtain the manager lock within two seconds; no inferred
success or retry after an uncertain publication. The explicit runner
returns its report and acknowledged CoherentCheckpointReceipt only after
successful database commit. On failure return an error and fault managed
health. No changes to the legacy stop/report path.

## Current-run and journal evidence

Require the same manager that successfully claimed the predecessor,
healthy Running gate, initialized empty historical fill and terminal
baselines, no pending, dirty or faulted fill/notification/terminal
delivery, ledger and fill notification prefixes equal, no working orders
or reservation breach. Prune only stale cached reservations for orders
that have left the working set (same policy as existing order preflight);
the durable journal still independently refuses unresolved orders.
Do not reinterpret any recovered baseline as callback return evidence.

Use the existing exclusive owner lease and the exact one-time startup
claim and predecessor receipt. In one PostgreSQL transaction lock the
order, fill and parent-allocation tables in SHARE MODE NOWAIT, and
the lazily installed parent tables if present. Refuse busy tables, any
parent/child or parent-allocation history, non-Spot/non-Binance,
working or uncertain orders, unlinked/old-run orders, out-of-range
intent ordinals, reservation breach, inconsistent cumulative fill
quantities and invalid/missing sequence markers. Every accepted direct
order must have a terminal marker; definite zero-fill exchange
submission refusals can be represented by direct attempt callbacks
without a terminal marker. A zero-fill accepted cancellation requires
the terminal callback prefix just like a filled order. Compare durable
fill and terminal maxima exactly against the current manager callback
return watermarks. This is direct-only and does not qualify EMS or
auto-de-risking/flatten orders without direct strategy correlation.

Commit the new canonical checkpoint head and a separate, versioned
`strategy_checkpoint_boundary` singleton receipt in the same synchronous
transaction. Include exact checkpoint receipt, run scope, last intent
ordinal, fill and terminal sequences, row counts and SHA-256 of the
order/fill journal. Hash fixed-size per-row PostgreSQL JSONB projections
in stable UUID order, with UTC session timezone, explicit table domain
separation and row counts. Hash all columns including fee evidence;
schema/fee/row mutations may conservatively invalidate verification.
Limit each table to 100,000 rows, use bounded 256-row pages and five
second per-statement timeout; this is not a per-market-tick scan.
Reject plain checkpoint publication once the coherent boundary exists.
A failed or ambiguous commit does not return an acknowledged receipt,
and the manager cannot attempt another publication.

The independent read-only verifier requires the caller's exact receipt,
validates the head, matches the stored boundary record, then repeats the
locked journal witness. A change that preserves row count and marker
watermarks still changes the digest. This proves the database snapshot
at the committed boundary, not that an exchange has no later execution
or that the ledger, external market source, risk counters and strategy
state can already be reconstructed safely. Independent receipt
retention/protection is the caller's responsibility; hashes are not
authentication. A late execution, fee-pricing update or other post-stop
journal mutation must invalidate verification, not be ignored.

## Qualification

Six new real-PostgreSQL integration tests exercise actual unified-loop
state and refusal paths using a mock exchange/market transport:
- Restore prior native counter, execute a direct strategy order,
  account for the fill, deliver the terminal callback, stop, and
  verify that the checkpoint contains all four state transitions
  (market, attempt, fill, terminal). Exact cursor, run scope, fill
  and terminal watermarks survive reopening; altered receipt or row
  mutation with unchanged marker counts fails verification. Plain
  publication cannot detach the coherent receipt.
- Accepted cancellation with zero fills still commits its terminal
  callback state and marker.
- Interrupt at four incomplete fill/terminal stages (fill staged,
  ledger acknowledged, fill notified, terminal staged). None publishes.
- Working order refuses publication and preserves the previous head.
- Deferred trigger failure at boundary transaction commit rolls back
  both the new head and the new receipt.
- No newly completed market observation refuses publication.

Initial direct-fill qualification exposed the existing lazy reservation
cache cleanup; use its established pruning rule at this quiescent
boundary, never relax journal eligibility. Repeated on isolated
PostgreSQL 17 instances; all clusters shut down and removed. Final:
180 OMS/EMS/live-trading tests (91/32/57), 733 core tests and 60 engine
library/binary tests passed, zero failures; six external core tests
ignored. Rust 1.99.0, locked dependencies, serial tests, workspace-wide
all-target Clippy with warnings denied, formatting and diff checks
passed. Full workspace build/test, engine subprocess qualification,
independent review, remote CI and real-service/failover tests pending.
No production database or real orders used.

## Remaining work

Independently durable coherent receipt selection and verified
post-execution restart require authoritative startup reconciliation,
verified ledger/risk state, coherent observation continuation,
independent receipt publication, and a safe durable run/claim
transition without replaying completed callbacks. Do not remove the
one-time startup refusal until these are qualified. Periodic in-loop
publication, ambiguous dispatch/working orders, arbitrary interpreter
globals, EMS parent lifecycle and causal no-order history warm-up are
not covered by this direct-only stop boundary.
