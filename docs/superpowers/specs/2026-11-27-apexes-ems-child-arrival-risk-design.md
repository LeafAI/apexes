# F03.02 — Independent simulated EMS child-arrival risk

## Scope

Event-time simulated children must pass independent execution checks
after transit, before consuming causal order-book liquidity. Parent
admission remains a separate check, and the existing parent-level
daily count/volume accounting must not be charged a second time.
Legacy immediate scheduling and the live OMS execution path retain
their existing behavior.

## Contract

- The matcher prepares proposed market-child fills with the original
  child order ID, exact available depth, price and simulated fees.
  Invoke the arrival guard under the matching-engine lock before
  consuming any proposed level. A rejected guard leaves shared depth
  available for subsequent children.
- The unified loop supplies current ledger positions and balances
  after existing resting-order fills. For each arriving child, check
  configured pre-trade limits and market-condition rules independently.
  Value the requested quantity at the highest proposed fill price
  for conservative notional checks when depth is executable.
- Built-in daily count/volume limits are parent-admission quotas;
  independent child checks do not count or charge them again and
  do not grant the protective-exit exemption. Other pre-trade rules
  retain their normal behavior.
- Apply all proposed fills to a private execution-account projection
  before accepting them, including applicable fees and margin. Reject
  the entire proposal if the projection cannot fund it. Advance the
  projected balances and positions for each accepted child in FIFO
  order so competing arrivals cannot spend the same funds.
- Deliver accepted fills exactly once to the authoritative ledger,
  retaining the existing fill-accounting path. Notify risk monitors
  at acceptance before checking later children, without duplicate
  notification on delivery. A refused child is terminal Rejected
  and contributes to the arrival rejection counter, with no fill.
- This is an arrival-time guard, not a persistent reservation across
  events. Outstanding direct-order and parent/child capital budgets,
  historical queue calibration, and source provenance remain open.

## Local qualification

Rust 1.99.0, locked dependencies: workspace build, workspace Clippy
(all targets, warnings denied), formatting and diff checks passed.
All 80 test binaries ran serially in split groups: 1,458 passed
(988 core, 144 database, 63 supervisor, 263 UI), zero failures,
44 ignored external/manual tests. All 30 doc-test suites passed.
The isolated temporary PostgreSQL instance was stopped after testing.
Three new regressions cover notional rechecking, same-event capital
competition with parent quota accounting, and rejected-child depth
preservation. No live orders or production databases were used.
Remote CI and independent review remain pending.
