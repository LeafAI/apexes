# F04.03 — Simulated Spot capital context

The shared loop attaches an optional local capital snapshot to
StrategyContext. The snapshot is calculated after current event
processing and due direct arrivals, from the existing ArrivalQueue
reservation totals and PortfolioLedger balances. It does not introduce
a second ledger or change admission, settlement, cancellation or
reservation behavior.

Scope is event-time simulated sessions with uniquely configured,
unit-multiplier Spot instruments, distinct nonempty base and quote
currencies and no negative, leveraged or unknown-symbol positions.
Legacy sessions without the event-time queue and ordinary live sessions
return no capital snapshot. Mixed or ambiguous instrument configurations
are unavailable. Only configured currencies with a recorded ledger
balance are returned. A missing balance is not inferred to be zero.

The existing reservation sum includes in-transit intents, tracked
resting orders and EMS parents across symbols spending the same asset.
A parent's residual hold is retained under unknown status. No second
child hold is added to the parent total. Native fields are Decimal:
ledger_balance, reserved and unreserved = max(ledger_balance - reserved,
0). An over-reserved asset remains visible as a shortfall between ledger
and reserved; invalid negative amounts or checked arithmetic failure
make the entry unavailable. This is a view of tracked local reservations,
not a guarantee of order admission, exchange free funds, fees for
unmodeled routes, or capital available across independent sessions.

Drift exposes ctx.capital (null when unavailable), with source
"simulated_spot_reservations" and get(asset). A valid lookup returns
an object with the three fields or null for unknown data. Invalid
argument shape returns undefined. Snapshots are detached and do not
refresh when retained by a script. Drift numbers use f64; Decimal
admission checks remain authoritative. Rust callers constructing
StrategyContext directly now specify capital (None when unavailable).
All workspace callers updated.

## Verification

Three added tests: native shortfall/invalid balances; queue-level
cross-symbol and parent hold aggregation including unknown/cancelled
parent status and unsupported scopes; actual native/Drift replay with
a BTC buy limit and ETH callbacks showing 99.099 USDT in transit and
resting, then 900.9505 USDT ledger/unreserved after a 99 USDT maker
fill charging 0.0495 USDT. Legacy replay yields no capital context.

Rust 1.99.0 locked offline serial: 597 nonignored tests passed
(432 backtest, 120 strategy, 15 paper, 25 indicators, 5 loader).
Three external tests ignored. All-target Clippy with warnings denied
passed for strategy, loader, backtest, paper, live-trading and engine.
Formatting and diff checks passed. Workspace CI and independent
review pending. No real orders or production accounts/databases used.

Next: F04.03 session health context. Live and margin-aware capital
require separate authoritative availability contracts.
