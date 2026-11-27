# F03.05 — Reference returns and observed drawdown recovery

Historical reports add optional drawdown evidence calculated in
occurrence order from the recorded equity curve (not an interpolation
or a guarantee of full mark-to-market valuation). Require a positive
initial value and nondecreasing timestamps. Track the last observed
high-water timestamp, start-to-recovery duration, longest completed
episode, longest observed episode including an unrecovered one,
current episode and worst drawdown. Equal timestamps retain order;
equal high water resets the peak timestamp. Durations are milliseconds
of observation time, not number of events.

Opt-in BenchmarkPolicy selects one symbol and maximum age of the
last observed trade. Compare fee-free buy-and-hold and zero-yield cash
against the strategy's account equity return, in a single quote
currency. For this slice require a verified manifest, one unit-
multiplier Spot instrument, positive cash-only starting capital, no
external control, valid causal observation order, and a positive first
event trade in that exact instrument. Require positive prices for all
its trades and a fresh final reference. Require report endpoints to
match the input horizon, reject unsupported fills or fee assets, and
reject incomplete observation horizons. No forward-filled initial price, assumed
FX, funding, fees on the benchmark or annualization. Report explicit
unavailability rather than silently compare incomparable results.
Legacy reports and paper/live behavior remain unchanged.

## Verification and scope

Register BenchmarkPolicy { symbol, max_end_age_ms } alongside a
verified historical manifest. The report records the policy, prices,
reference timestamps, account equity, zero-yield cash and fee-free
buy-and-hold simple returns and arithmetic excess returns. A declined
benchmark is explicit unavailable evidence with reason, not a failed
strategy run. This is a frictionless reference, not an executable
buy-and-hold strategy, and does not establish upstream completeness.
No benchmark policy leaves the optional field absent.

Drawdown evidence describes the reported equity series, including
any limitations of its valuation (in particular open perpetual
positions), not independently verified true account equity.
An unrecovered episode is right-censored at the final observation.
The longest observed duration includes censored episodes and is
separate from the longest completed recovery. No annualization.

Two unit regressions and two historical-runner regressions cover
same-time observations, recovery, censoring, empty/nonpositive/
regressing curves, negative equity, stale/missing first reference,
unverified input, pre-existing inventory, and legacy JSON defaults.
Ten repeated mixed-event Spot runs reproduce the exact report digest:
strategy return -0.101%, buy-and-hold -10%, arithmetic excess
+9.899 percentage points, one 1900 ms completed recovery, and one
2500 ms unrecovered drawdown. A synthetic fixture, not a profitability
claim.

Rust 1.99.0 locked offline 404 nonignored tests passed serially
(389 backtest, 15 paper), three external tests ignored.
All-target Clippy (-D warnings), formatting/diff checks passed.
Workspace CI and independent review pending. Multi-asset or leveraged
benchmarks and funding/exit attribution remain outside this slice.
No live orders, accounts or production databases accessed.
