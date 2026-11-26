# F03.02 — Causal execution simulation, first increment

The roadmap requires causal depth/spread/latency, correct partial fills
and no phantom working market orders. Preserve the F00.01 fixture's
unrestricted market behavior in this increment; managed Spot bounded
IOC policy and causal order-arrival latency require follow-up.

The shared simulated engine uses a separate book per venue/symbol.
Market fills consume a local overlay of displayed levels, shared by
direct and EMS child orders. A subsequent accepted snapshot replaces
the overlay; an accepted delta replenishes only explicitly updated
levels. Older timestamps and duplicate/non-advancing sequenced deltas
are ignored; no future book is eligible for an earlier submission.
A snapshot may rebase a sequence after reconnect. This is not an
exchange impact model: historical absolute updates may replenish
liquidity regardless of simulated actions.

Market orders are immediate, never resting. Full execution is Filled;
partial execution or no execution expires as Cancelled with actual
filled quantity. The adapter and EMS sink agree on that terminal
status and the order log marks cancellation after recording fills.
Existing resting orders and stop triggers are scoped by venue/symbol.
Stop-market expiry removes the original working order, including zero
fills; StopLimit conversion preserves the original ID. Match against
observed depth before dispatching EMS children for that event; child
timestamps use simulated event time, not wall clock. Preserve one
trading loop and all live safety gates.

No synthetic spread or arbitrary slippage charge: observed bid/ask
and depth already determine principal. Crossing limit behavior, queue
model calibration, strict stale-depth/gap policy, causal scheduled
arrivals, and bounded managed-Spot IOC are future increments. No real
exchange requests or orders. Verify consumption, delta replenishment,
venue isolation, expiry, stop ID and historical event time.
