# F03.02 — Shared simulated maker queue

## Scope and model

Direct limit remainders and other resting simulated orders share one trade
budget per venue/symbol event. Process eligible orders in price priority
(best bid first, best ask first) and stable admission order at equal prices.
For known aggressor sides, only the opposite maker side can participate;
unknown-side prints are usable only when exactly one side is eligible.

The existing configured queue factor estimates external volume ahead using
causal same-side depth at or better than the limit at admission. External
volume consumed by the current print is shared across eligible orders on
the same side: subtract the already accounted external volume from each
later estimate before consuming more of the print. Simulated fills never
count as external queue depletion. Each print's budget covers both external
queue depletion and simulated execution, without double allocation. Earlier
price/time orders consume budget first. Do not process a print predating the
resting order or later than the execution clock.

This is a deterministic aggregate L2 approximation, not exchange L3 queue
reconstruction. Cancellations, new external orders, hidden liquidity and
cross-price queue overlap cannot be known exactly; do not infer additional
fills from depth changes. Historical calibration of the queue factor and
source completeness remain separate qualification gates.

## Acceptance

- A print cannot both clear external queue volume and reuse it for fills.
- Later orders retain the correct residual estimate; same-price FIFO and
  cross-price priority are deterministic.
- Known wrong-side and ambiguous unknown-side prints do not fill orders.
- Noncausal, nonpositive prints cannot advance a resting queue.
- Existing maker fees, order identities, partial fills and stop conversion
  behavior remain unchanged.
- Qualify with isolated deterministic regressions, full workspace checks,
  and remote CI before promotion. F03.02 remains in progress.

## Local qualification

Rust 1.99.0, locked dependencies: workspace build, workspace all-target
Clippy with warnings denied, formatting and diff checks passed. All 80
test binaries ran serially in split groups: 1,447 passed (977 core,
144 database, 63 supervisor, 263 UI), zero failures, 44 ignored
external/manual tests. The isolated PostgreSQL group includes the 64
database-only ignored tests and was stopped afterward. All 30 doc-test
suites passed. The four new queue regressions passed. No real exchange
orders or live databases were used. Independent review, remote CI and
historical calibration remain pending.
