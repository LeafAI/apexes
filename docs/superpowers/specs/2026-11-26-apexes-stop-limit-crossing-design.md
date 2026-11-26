# F03.02 — Triggered stop-limit crossing

A valid causal trade activates a pending stop-limit only after admission.
Process existing stop-market execution and resting maker fills before
submitting the activated limit against the same causal depth used by
direct limits. Take eligible opposite-side depth at displayed prices
within the limit, charge taker fees, and rest only unfilled quantity at
activation priority. All fills retain the original stop order ID; a
fully executed stop-limit does not enter the working-order view.
The triggering print cannot also supply a maker fill to its own
newly activated order. Reject nonpositive stop or limit prices on
admission and invalid or future-dated prints before triggering.
Historical queue calibration, source deduplication, and EMS child
transit/risk/reservations are outside this slice.

Acceptance: passive activation cannot fill on its trigger print;
marketable buy/sell activations execute within their price bounds;
partial and terminal order views and shared depth remain consistent;
invalid trigger prices are rejected. Run focused and workspace
regressions, fmt, Clippy, and remote CI before promotion.

## Local qualification

Rust 1.99.0, locked dependencies: workspace build, all-target workspace
Clippy with warnings denied, formatting and diff checks passed. All 80
test binaries ran serially in split groups: 1,450 passed (980 core,
144 database, 63 supervisor, 263 UI), zero failures, 44 ignored
external/manual tests. Isolated temporary PostgreSQL was stopped after
qualification. All 30 doc-test suites passed. Three new stop-limit
regressions passed. No live orders or production databases were used.
Independent review and remote CI remain pending.
