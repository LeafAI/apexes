# F02.04 — Exposure-reducing order protection

## Decision boundary

The initial protective-exit classification is deliberately narrow:
a positive-quantity, immediate, fixed-route, unleveraged Binance Spot
market sell no larger than the current strategy-owned long position,
with no working order for the symbol. It is inferred from actual
position and order state, never from an untrusted strategy flag.
The unified loop additionally checks configured instrument identity,
base balance and absence of scheduling EMS parents before granting
the exemption. It does not grant the exemption to a routed, sliced,
leveraged, resting or exposure-increasing intent.

The limit checker waives the daily accepted-order count, daily volume
and position ceiling for this classification only. A reduction may
leave a position above a newly tightened ceiling without worsening it.
Accepted exits still increment the daily counters; they cannot create
new entry allowance. The check no longer mutates the daily counters
while asking a risk question. Per-order quantity/notional limits,
equity floor, symbol/venue lists, self-trade, circuit-breaker and
market-condition checks remain in force.

## Managed Spot enforcement

The OMS rechecks against its actual working-order map. All managed Spot
sells must fit strategy-owned durable executions (buy quantity minus
sell quantity minus native base fees), as well as the exchange's
authoritative free base balance. The gateway confirms symbol/base
identity from public metadata and obtains the current wallet snapshot.
A competing local working order prevents a verified reduction.
A shortfall between owned and free inventory latches health; unavailable
inventory or insufficient owned quantity refuses the sale. No order is
sent on these failures. The existing fill acknowledgement, ownership
lease, unresolved submission and health gates still apply. Managed Spot
cannot use the derivative flatten path to bypass these gates.

The check is serialized under the OMS mutex with the subsequent
submission, with a three-second bound on the new exchange lookup.
It does not reserve funds on the exchange or coordinate external
writers or other sessions. Dedicated account/schema ownership remains
required. The next feature, F02.05, addresses continuous reservations,
cross-symbol exposure and dynamic exchange rules. Real exchange
qualification is still required; this is not a native exchange stop
order, a reduce-only derivative implementation, or permission to
trade while stale/halted. Base-denominated fees on sells can consume
additional inventory; continuous fee-aware reservation is not yet
implemented.

## Trend Guard

The deployed script now updates its indicators but evaluates stop,
target and disabled-state exits without requiring indicator readiness.
A trend reversal needs ready fast and slow indicators. Entry warm-up,
entry gap, cooldown and entry count apply to new entries only.
An independent exit_retry_trades parameter (default five trades) paces
repeated exit attempts, including rejected requests, while any working
order still prevents another order. The first protective exit after
entry or restart is not subject to the entry gap. Retain the historical
pre-change V1 in Git for the later strategy comparison; this is a
safety correction rather than strategy optimization.

## Qualification

Exercise the shared limit checker, real Drift interpreter and managed
OMS using deterministic market data, mock gateway and isolated test
PostgreSQL. Verify the existing economic baseline, hard limits, no
oversale of unrelated holdings, native base fee reduction, dirty
delivery gate, working orders, wallet discrepancies and halted
submission refusal. No real order placement.
