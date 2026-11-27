# F03.02 — Spot market-intent transit capital reservations

## Scope and contract

This increment covers event-time simulated Spot market intents while
awaiting direct execution or EMS parent admission. A pending buy holds
quote capital for its requested quantity at the worst observed ask
needed by the order (or the bounded IOC cap), plus modeled taker fees.
A pending sell holds its requested base quantity. Missing causal buy
depth, invalid amounts, overflow, or insufficient unreserved funds
cause refusal. Funds are shared by spend asset, not by trading symbol.

Holds remain in the arrival queue across market events and are not
deducted from actual portfolio balances or equity. Extracting a due
batch retains all holds until each intent is individually processed.
At arrival release only that intent's hold and revalue its spend
against current causal depth and remaining holds. A price increase
cannot consume another due market intent's reservation. The arriving
EMS child execution guard rejects proposals that would decrease
balances below outstanding transit holds before consuming depth.
Replay end releases any undelivered holds and reports their count;
report fields default for historical serialized reports.

This is a conservative estimated budget rather than a guaranteed
future execution price. It requires causal depth for unbounded
market buys, and a valid causal cap under bounded IOC. It does not
reserve funds after parent admission, for resting limit/stop orders,
or for leveraged intents. Emergency liquidation/de-risking and
existing resting-order fills do not participate in these holds.
Those paths may change available capital; arrival revalidation
remains mandatory. The next increment must extend reservation
ownership through parent/child lifetime, followed by resting-order
and shared-capital qualification. Legacy immediate and live paths
are unchanged. F03.02 remains in progress.

## Local qualification

Rust 1.99.0 locked workspace build, all-target Clippy with warnings
denied, formatting and diff checks passed. All 80 test binaries ran
serially in groups: 1,464 passed (994 core, 144 database, 63 supervisor,
263 UI), zero failures, 44 external/manual tests ignored. All 30
doc-test suites passed. Six new regressions cover shared quote funds,
due batch ownership, price increases, fee-inclusive holds, child
competition, and end expiry. Temporary isolated PostgreSQL stopped
after qualification; no live orders or production databases used.
Remote CI and independent review pending.
