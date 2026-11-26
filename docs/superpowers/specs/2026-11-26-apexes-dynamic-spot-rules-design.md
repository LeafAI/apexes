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
down. Parent-level capital reservation is described in the follow-up
below. Caller-snapshot-dependent equity checks
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

### Fee-aware reservation follow-up

Use signed GET /api/v3/account/commission for the exact symbol (see
[Binance commission FAQ](https://developers.binance.com/docs/binance-spot-api-docs/faqs/commission_faq)).
Sum standard, special and tax rates including the relevant buyer/seller
component, separately for maker and taker, then take the larger total.
Do not reduce the budget for a possible discount. Refuse missing,
malformed, mismatched or stale locally observed evidence. Persist the
rate, observation timestamp and possible discount asset atomically
with the reservation. The preflight still has a five-second deadline;
no cached fallback. New buy reservation equals capped principal times
(1 + observed rate), replacing the fixed 1% headroom. Normal sell fees
are taken from receipts; if the possible discount asset is the sold
base, reserve quantity times (1 + rate) against owned and free base
inventory. A possible fee asset is held exclusively against other
working orders' spend, receipt and discount assets. Refuse a fee asset
belonging to another configured managed base inventory, since its fee
attribution is not modeled across symbols. Legacy working orders
without commission evidence block new submissions until reconciled.

This is a conservative rate-derived budget, not an exact final fee
guarantee. The exchange can change rates, charge rounded fees and use
a discount asset or fall back to received-asset fees. External discount
tokens are protected by exclusivity but are not numerically reserved;
their precise quantity depends on execution-time conversion. The
existing actual-spend and price-cap breach checks remain mandatory.
Live-service compatibility and exact fee-token funding remain open.
No real exchange requests in tests.

### EMS parent capital follow-up

Reserve the complete parent quantity before scheduling in a separate
durable OMS allocation record using fresh managed Spot preflight.
Admission requires fixed routing, exact whole-quantity conformity,
available spend balance and owned sell inventory; all spend, receipt
and possible discount assets are exclusive against competing parents
and working orders. The admission uses the existing serialized OMS
boundary and a detached task for network preflight. A normal budget
refusal does not latch EMS health. The allocation stores quantity,
budget, commission evidence, reference price and potential fee asset.
Child dispatch revalidates fresh rules, fees, available funds and risk.
The child identity, amount and quantity are checked against the
parent's remaining budget; allocation consumption and the durable
child pre-dispatch order record commit atomically. A higher price or
fee rate may exhaust the budget. There is no automatic recycling of
unused child amounts, including rejected or partially filled children.
The existing EMS dispatch_pending marker still protects the separate
scheduler-link transaction. A failed or ambiguous dispatch is not
automatically retried. Parent completion remains submitted quantity,
not actual filled quantity.

Terminal scheduling requests release; the allocation remains active
while any linked durable child is working. The global fill delivery
gate still protects terminal proceeds. Terminal child cancellation
checks persisted terminal status rather than treating a missing
working-map entry as proof of cancellation. Startup verifies scheduling
identity, submitted quantity and durable child reservation totals;
legacy scheduling parents lacking allocations refuse recovery.
Reservation precedes scheduler insertion, so an interruption between
them leaves an orphan lock, never an unfunded scheduling parent.
Audited reconciliation is required to release an orphan. The managed
engine still requires quiescent startup.

This conservative scope requires the whole parent to pass single-order
exchange filters, excludes flexible shared-asset allocation and
does not guarantee execution or a numerically reserved external fee
token. Child risk checks remain necessary; no aggregate risk cap
is waived. Qualification uses isolated PostgreSQL and mock execution,
including an end-to-end managed POV parent through the actual sink.
Real-service behavior and simulation parity remain unqualified.

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
