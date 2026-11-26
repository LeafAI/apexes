# F03.02 — Direct marketable limit orders

## Scope
Backtest and paper direct Limit intents use the shared matching engine at
the actual submission/arrival timestamp. Market and EMS child execution
are unchanged. The existing stop-trigger conversion is a separate slice.

## Contract
- A buy takes only available asks at or below its limit; a sell takes
  only available bids at or above its limit, in best-price order.
- Each execution consumes shared depth and charges the taker fee.
- Full execution is terminal; a partially executed GTC limit rests its
  remainder with the same order ID and cumulative filled quantity.
- Passive fills use the existing queue model and maker fee. Only causal
  depth can be used to initialize that queue or execute immediately.
- Preserve strict depth checks and existing order status/fill delivery.
- Reject non-positive limit prices before touching liquidity.

## Acceptance
Regression tests cover buy and sell crossing, price bounds, shared
depth, partial remainder and order identity, separate maker/taker fees,
passive submission, and future depth exclusion. Queue calibration,
self-trade handling, stop-limit crossing, and real-source qualification
remain outside this slice; do not mark F03.02 complete.

## Local qualification

Rust 1.99.0, locked dependencies. Workspace build, all-target workspace
Clippy with warnings denied, formatting and diff checks passed.
All 80 test binaries were run serially in split groups (1,443 passed,
zero failures, 44 ignored). All 30 doc-test suites passed.
The 64 database-only ignored tests were also run using a fresh
temporary loopback PostgreSQL 17 instance, stopped after verification;
they are included in the 1,443 executed tests. No live database,
exchange credentials or real orders were used. Remaining ignored
external/manual tests, independent review and remote CI are pending.
