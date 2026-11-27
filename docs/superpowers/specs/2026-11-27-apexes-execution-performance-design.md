# F03.05 — Execution-based realized performance

Add an optional historical report field independent of the legacy
equity-point statistics. Reconstruct signed session inventory from
accepted fills in occurrence order, using weighted-average entry
price and proportional allocation of accumulated opening fees.
Each reducing fill (including the reducing part of a flip) is one
closing observation, not a completed round trip. Count wins, losses
and breakevens by net closing P&L, report expectancy per closing
observation and profit factor only when net losses are nonzero.
Turnover and fees include opening and closing fills.

Report per symbol in its own quote currency, not a sum across
currencies. Restrict the calculation to unit-multiplier Spot and
linear perpetual simulations with quote-denominated fees; an
unsupported symbol or arithmetic overflow yields unavailable evidence,
not silently omitted costs. Initial session inventory is zero (not
starting wallet holdings); a spot sale of starting holdings creates
negative shadow inventory, not an exchange short. Unallocated entry
fees are separately visible. Realized net excludes unrealized P&L,
funding, and external cash flows; it is not account total P&L.
Retain legacy fields and spot accounting. Populate only in the
historical runner, without changing the shared execution loop.
Benchmark, drawdown duration and exit-reason attribution follow.

## Verification

Four unit regressions cover weighted-average entries, partial closes,
flip allocation, long/short gains and losses, breakeven, undefined
ratios, unsupported fee currencies and overflow. The mixed-event
perpetual integration asserts gross realized -1, fees 0.199, net
realized -1.199 and turnover 199 USDT; the equity result separately
includes 0.1 USDT funding. Legacy JSON without the optional field
deserializes successfully. Repeated deterministic replay still matches.
Rust 1.99.0 locked offline 400 nonignored tests passed serially
(385 backtest, 15 paper); three external tests ignored.
All-target Clippy with warnings denied, formatting and diff checks
passed. Full workspace CI and independent review pending. No exchange
accounts, real orders or production databases accessed.
