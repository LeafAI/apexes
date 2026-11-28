# F04.04 — Explicit simulated terminal outcomes

## Contract

Strategy gains optional on_order_terminal(&TerminalOutcome), default
no-op and forwarded by Box<dyn Strategy>. Drift exposes optional
on_order_terminal(outcome). Noncallable definitions fail construction;
runtime errors are logged and returned values ignored. No new order
submission capability or rollback of script global mutations.

Four typed kinds:
- Filled: the matching adapter has updated a previously working order
  from actual fills to its exact requested quantity. The adapter retains
  completion evidence for the current event only; the shared loop drains
  it after all resting fill accounting and notification. No status
  inferred from absence, and no new fill or ledger calculation.
- Cancelled: ID explicitly returned by successful simulated working-order
  cancellation. The cancellation API supplies IDs, not full order
  snapshots; missing metadata is not fabricated.
- TransitCancelled: direct unsubmitted intent actually removed by the
  existing emergency transit cancellation operation.
- TransitExpired: direct unsubmitted intent still in the arrival queue
  at successful loop termination. Uses the queue's last accepted logical
  timestamp, not an extrapolated future deadline or fabricated exchange
  order expiry. Existing end-of-session report counters unchanged.

Emergency notifications occur after the existing cancellation and
parent-reconciliation operations, in transit queue order followed by the
matching engine's sorted cancellation ID order. No change to admission,
reservation, risk checks, parent cancellation, accounting or clocks.
Terminal completion follows actual resting fill callbacks. Immediate
terminal submission results remain in on_order_outcome(Returned); they
are not duplicated through the new terminal callback.

Drift payload: kind (filled/cancelled/transit_cancelled/transit_expired),
at (RFC3339), order_id (string or null), symbol, venue, quantity and
filled_quantity (nullable). Cancelled provides only ID and timestamp;
transit intents provide identity and quantity but no order ID or
fabricated execution quantity. Native callbacks retain actual Order
or OrderIntent evidence. Drift quantities use f64. No durable delivery
or cross-restart exactly-once guarantee.

## Boundaries

This is explicit local simulation evidence. Unavailable backends return
no completion evidence. A failed stop trigger, partial IOC stop remainder,
or a missing order cannot establish a cancellation solely from open-order
disappearance; these paths require explicit terminal evidence in a
later slice. Working orders left at replay end are not automatically
cancelled or expired. EMS transit/parent lifecycle remains separate
(the successful cancellation ID list may include simulator child orders).
Authoritative live terminal delivery and durable intent correlation
remain open. Lifecycle callbacks should remain short.

## Verification

One new integration regression verifies transit expiry at the actual
last observed event, no fills and the existing expiry counter.
The sealed native/Drift replay regression now requires a single complete
resting-order notification after the final partial execution, and uses
that notification to enable the next decision. Callback return intents
are ignored and replay fingerprints remain repeatable. Both existing
liquidation and extreme-market emergency regressions assert actual
cancelled order ID and transit cancellation in order, preserving all
reservation and fill assertions. Negative regressions verify failed stop
trigger disappearance and working remainder at replay end emit no
fabricated terminal notification.

Rust 1.99.0 locked offline serial: 604 tests passed (437 backtest,
122 strategy, 15 paper, 25 indicators, 5 loader); three external tests
ignored. All-target Clippy with warnings denied passed for strategy,
loader, backtest, paper, live-trading and engine. Formatting and diff
checks passed. Full workspace CI and independent review pending.
No live orders, production accounts or databases accessed.

Next: explicit EMS parent lifecycle evidence.
