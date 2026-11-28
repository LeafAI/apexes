# F04.04 — Direct order attempt outcomes

## Contract

Strategy gains an optional on_order_outcome(&OrderOutcome) method,
default no-op, forwarded by Box<dyn Strategy>. This is synchronous,
notification-only and returns no new intents. The shared trading loop
notifies direct strategy attempts (no execution_algo) at existing
decision/arrival branches, with the same logical callback timestamp.
No new order queue, admission path or event clock is introduced.

Outcome kinds:
- rejected: local pre-executor refusal, with stable reason code.
- scheduled: successful event-time transit enqueue, not acceptance.
- returned: executor returned an Order; inspect its actual status
  and filled_quantity. New, PartiallyFilled and Filled are returned
  accepted states; Cancelled or Rejected are not working acceptance.
  Successful return is delivered after existing reservation, risk and
  ledger bookkeeping succeeds.
- submission_failed: executor returned an error. No order identifier
  is invented, and no claim of confirmed exchange rejection is made.
  A live submission may have an uncertain remote outcome.

The intent is the original on a rule-conformance failure, otherwise
the conformed intent. Outcomes follow existing vector order and due
arrivals precede the next market decision. Scheduled and arrival
outcomes do not yet have a separate shared intent correlation ID.
A scheduled order can expire at end or be cancelled by emergency
preemption without a subsequent notification in this slice. No
persistent delivery, exactly-once claim or replay of old outcomes.
Callbacks are synchronous and should remain short.

## Drift API

Optional top-level function on_order_outcome(outcome). The name is
reserved: if defined but noncallable, construction fails. Existing
scripts without it keep their behavior. Fields: kind, code (nullable
stable reason code), stage ("decision"/"arrival"), at (RFC3339),
symbol, venue, side, quantity, price, order_id, order_status,
order_quantity, filled_quantity. Order fields are null if no Order
was returned. Order IDs are strings; prices/quantities use existing
f64 representation. No exchange IDs or free-form backend errors are
passed to Drift. Native callbacks receive borrowed intent/order data.

Returned values are ignored, even valid-looking order intents. No
StrategyContext capabilities are attached to the lifecycle callback.
The optional callback may update script globals for future decisions;
runtime errors are logged and do not undo completed execution or
roll back global side effects. Existing staged market callback state
and indicators are not changed. No new hot-reload entry points.

## Verification

Three added regressions:
- Sealed native/Drift replay with risk rejection, duplicate pending
  rejection, two scheduled orders, a filled market arrival and a new
  resting arrival; exact sequence, order IDs and report fingerprint
  repeat. A subsequent decision consumes the lifecycle evidence.
  Returned callback intents do not cause additional submissions.
- Legacy no-liquidity market returns Cancelled, while strict missing
  depth produces submission_failed; neither is fabricated acceptance.
- Noncallable optional entry point, callback runtime error isolation,
  and backward-compatible absence of callback.

Rust 1.99.0 locked offline serial: 601 tests passed
(434 backtest, 122 strategy, 15 paper, 25 indicators, 5 loader);
three external tests ignored. All-target Clippy with warnings denied
passed for strategy, loader, backtest, paper, live-trading and engine.
Formatting and diff checks passed. Full workspace CI, independent
review and live-service qualification remain pending. No actual
orders, accounts or production databases accessed.

Next: later committed fills (direct, resting and supplemental),
terminal state/cancellation and EMS parent/child lifecycle. Preserve
existing automatic risk exits and delivery accounting. Do not mark
F04.04 complete based on direct submission notifications alone.
