# F02.05 — Dynamic Spot rules and reservations

## Scope and submission path

Managed Binance Spot submissions use current exchangeInfo, last price
(and matching exchange average-price window for market notional filters),
and the account free balance. Metadata, price or account failures refuse
the order. The five-second preflight runs under the existing serialized
OMS submission boundary. No order is persisted or dispatched on a failed
preflight. The existing durable submission and health gates are retained.

Market and limit orders are supported. Quantities conform down to current
LOT_SIZE and MARKET_LOT_SIZE; incompatible increments refuse. Limit prices
conform to the passive tick. Validate quantity and price bounds, wire
precision, symbol trading status, order type, and MIN_NOTIONAL/NOTIONAL
filters. Managed instruments must match the configured symbol/base/quote
scope. Exchange rules which are not modeled (including dynamic percent
price and exchange-wide order limits) remain exchange-enforced. The loop's
existing early risk checks may conservatively reject an unconformed
intent; the OMS checks the conformed intent again with a fresh reference
price. Offline simulation retains its configured static rules.

## Reservations

Each managed order reserves its spend asset exclusively before dispatch,
keyed by its durable order ID; buys reserve estimated quote notional plus
a one-percent headroom, sells reserve base quantity. The headroom is not
a guaranteed fee/slippage bound or a hard market-buy price cap. Use
exchange free rather than free plus locked balances. A subsequent order
cannot spend an asset reserved by or expected as a receipt from a working
order; pending receipts do not fund new orders. Keep reservations until
authoritative terminal state, and retain the fill-acknowledgement gate.
The follow-up persists reservation evidence atomically with the original
order before dispatch. The OMS restores working-order reservations from
that record; terminal reconciliation releases active reservations but
retains audit evidence. Unknown recovered reservations block submission.
Managed engine startup remains quiescent; restoring OMS evidence is not
yet permission to resume a session with working orders or locked funds.
Cumulative actual spend, including fees in the spend asset, is checked
against the reservation in the same transaction as execution persistence.
A breach is durably latched without discarding actual fills and prevents
new submissions and startup until audited reconciliation. There is no
automatic clearing path. The check is post-execution detection, not a
hard market-buy spending cap or exact pre-trade fee guarantee.
Durable owned Spot inventory guards sales and informs buy position
limits even when EMS passes no caller position snapshot. The risk checker
includes outstanding unfilled same-symbol buys and does not credit
pending sales as position capacity.

EMS child quantities must already satisfy current rules because the
parent journal reserves the requested child amount before dispatch.
Refuse rather than silently round an EMS child; direct orders can round
down. Parent-level capital reservation across independently scheduled
parents remains unimplemented. Caller-snapshot-dependent equity checks
can still conservatively refuse children.

## Limits and qualification

### Bounded market-buy follow-up

Managed market-buy intents dispatch LIMIT IOC with the original client
ID, conformed base quantity and a tick-aligned cap no greater than the
observed last price. Validate both market and limit filters. Persist the
cap atomically with the reservation; keep the original intent locally.
Unsupported gateways reject without fallback to MARKET. IOC can fill
partially or expire, including EXPIRED_IN_MATCH. No automatic resubmission
is introduced. Retain capital through terminal reconciliation and ledger
acknowledgement. Above-cap fills or fee-inclusive spend overruns durably
latch health without discarding executions. Principal is bounded by
quantity times cap; quote fees still use estimated 1% headroom. The
zero-slippage-allowance policy does not guarantee fills, exact total fees
or backtest equivalence. Ordinary limit and market-sell behavior is unchanged.

This is a single managed account/schema guard, not account-wide fencing
between multiple processes or coordination with external traders.
Managed market-buy principal is exchange-price-bounded, but fee
headroom is not an exact asset-specific fee reservation. No native
derivative, stop/stop-limit, or continuously refreshed operator balance
view is included. The serialized network preflight has latency costs;
real-service behavior and performance need separate qualification.
F02.05 remains in progress pending review of these remaining boundaries.

Regression tests cover changing filters, passive conformance, market
lot bounds, average-price window, free versus locked funds, shared
quote reservation through partial fill and cancellation, and durable
position/notional checks with no supplied position snapshot. Run all
workspace tests and the isolated PostgreSQL group. Do not use live
orders or a live database.
