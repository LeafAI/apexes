# F04.04 — EMS parent lifecycle callbacks

## Contract

Strategy gains optional on_parent_outcome(&ParentOutcome), default
no-op and forwarded by Box<dyn Strategy>. Drift optionally defines
on_parent_outcome(outcome). A defined noncallable entry point fails
construction. Runtime errors are logged and callback return values
ignored; script global mutations are not rolled back.

The shared loop publishes four evidence kinds:
- Submitted: submit_algo_order returned a real parent ID, after existing
  reservation and admission bookkeeping. Carries the conformed intent.
- SubmissionFailed: EMS submission returned an error; no parent ID is
  invented. Does not establish that a remote side effect is absent.
- StatusChanged: a successful existing status poll reports a different
  aggregate phase from the tracked phase. The initial Scheduling
  baseline is the existing loop's local tracking convention, not an
  independently queried phase. Only phase changes are reported, not
  changes in free-form failure reason. Emergency cancellation uses
  the existing successful cancellation result and reconciliation.
- Unavailable: the existing status poll returned None and the tracked
  entry was removed. This is not a terminal status. Poll errors retain
  the tracked phase and log as before; they do not fabricate outcomes.

Parent phases are Scheduling, Completed, Cancelled and Failed.
Completed means the EMS aggregate completion contract, not a claim
that the whole requested quantity was filled. No child fill quantity,
association or execution percentage is inferred. No extra queries,
scheduling changes, reservations, risk or accounting changes.
Ordinary phase notifications follow applied EMS fill callbacks, before
the next market decision and before existing parent reconciliation;
emergency notifications follow existing cancellation/reconciliation and
direct terminal callbacks. Status polling order is existing sorted UUID
order. Confirmed emergency parent notifications are sorted by UUID
without changing the existing cancellation execution order.

Drift fields: kind (submitted/submission_failed/status_changed/unavailable),
at (RFC3339 logical observation time), parent_id (string or null),
symbol, venue, quantity (submission evidence only), previous and status
(lowercase aggregate phase or null). Failure details are deliberately
omitted. Native submission evidence includes the borrowed intent.
Numeric quantity uses existing f64 representation. No new order
submission capabilities or recursive execution from callbacks.

## Boundaries

Only parent attempts actually reaching EMS submission are notified;
pre-EMS local refusals remain outside this slice. Drift can consume
parent notifications but its existing intent parser does not yet
construct EMS algorithms. No live polling when no market event arrives,
no durable delivery or exactly-once guarantee, no recovery of removed
or previously untracked parents. Observed transitions are snapshots,
not a guarantee that intermediate phases were seen. Parent status
poll errors retain prior tracking. Parent ID to child ID attribution,
executed progress, durable intent correlation and authoritative live
terminal evidence remain open.

## Verification

Two new tests: existing poll path distinguishes query errors, unchanged
phase, failed phase, changed private reason and missing record; actual
Drift callback consumes submission, completion, failure and unavailable
evidence. Extended sealed two-parent replay asserts identical callback
records and report fingerprint over ten runs, with exactly one
submission and one completed phase notification per parent. The existing
unsupported-policy replay now explicitly checks two submission failures
without IDs or fills. Both liquidation and extreme-market emergency
tests assert a single confirmed cancelled parent notification while
preserving all reservation, fill and direct terminal assertions.
Initial test expected only successful submissions in the unsupported
policy run; corrected to assert its actual failure evidence.

Rust 1.99.0 locked offline serial: 606 tests passed (438 backtest,
123 strategy, 15 paper, 25 indicators, 5 loader); three external tests
ignored. All-target Clippy with warnings denied passed for strategy,
loader, backtest, paper, live-trading and engine. Formatting and diff
checks passed. Full workspace CI, independent review and live-service
qualification remain pending. No live orders, production accounts or
databases accessed.

Next: persistent lifecycle correlation and child execution evidence.
