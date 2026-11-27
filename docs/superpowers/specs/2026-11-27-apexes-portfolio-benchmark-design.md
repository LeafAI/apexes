# F03.05 — Static-weight Spot portfolio benchmark

Register a `portfolio_benchmark::PortfolioBenchmarkPolicy` with the
historical runner. The policy specifies an ordered list of symbol/weight
allocations, a common quote currency and a positive maximum final reference
age in milliseconds. Symbols resolve uniquely within the strategy universe.
All replay instruments must be unit-multiplier Spot with distinct base
assets and the policy quote. A policy can allocate to a subset of that
universe. Starting balances must consist of positive quote cash and
optional zero balances. The runner rejects simultaneous single-Spot and
portfolio policies.

Weights are positive decimal fractions with an exact sum at most one.
The remainder stays in zero-yield quote cash. Initial allocations are
held through the endpoint, with fractional exposure, zero fees, and no
rebalancing. The reference is frictionless. Strategy return uses the
runner's recorded account equity, inclusive of its simulated costs.

The evaluation horizon runs from the first verified replay observation
through the last. Every selected symbol needs a positive trade at that
first timestamp. The first trade per symbol at that timestamp sets the
allocation price. The last trade per symbol through the endpoint supplies
the terminal price, subject to freshness. Equal-time observations retain
their sealed order. Completed bars are available at close and require
explicit event-time replay. Bars, quotes and marks supply no benchmark
prices. Verified input establishes file integrity within the declared
scope. Feed completeness requires separate evidence.

Asset return is (final price − initial price) / initial price. Contribution
equals weight × asset return. The portfolio return is the checked sum of
contributions in lexical symbol order. Initial allocation capital is
initial equity × weight. Final holding value is allocation capital plus
initial equity × contribution. Benchmark equity is initial equity ×
(1 + portfolio return). Separate rounding of these operations can affect
reconciliation at Decimal precision. A positive allocation that rounds to
zero makes comparison unavailable. Excess returns are arithmetic differences.
The exact weight bound is checked on integer mantissas at scale 28 before
any financial calculation. Financial operations use checked Decimal
arithmetic and an overflow makes comparison unavailable.

The optional `portfolio_benchmark` report field records policy and
status. Available evidence includes horizon, quote, strategy and benchmark
equity, cash allocation, simple returns, arithmetic excess and lexical
per-symbol references and contributions. Unavailable evidence carries an
actionable reason. Comparison requires a complete replay equity curve
and compatible Spot fill economics. A strategy base balance needs an
observed trade valuation even if the benchmark assigns it zero weight.
Legacy report fields and serialized
fingerprints are preserved when this policy is absent.

Regression coverage includes weighted gains and losses, cash remainder,
single-symbol parity, stale/missing references, timestamp ordering, invalid
allocations and instrument metadata, truncated reports, unsupported fills,
arithmetic overflow, legacy JSON and repeatable sealed multi-symbol runs.
Leveraged benchmarks, FX, observed funding and explicit strategy exit
motives remain outside this implementation.
