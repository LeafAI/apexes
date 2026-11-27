# F04.03 — Frozen execution-cost context

## Contract

Native StrategyContext gains optional CostObservation; Drift exposes
ctx.costs (null when unavailable). The shared loop resolves the unique
configured event instrument and queries OrderExecutor::cost_observation.
The default returns None. MatchingEngineOrderExecutor reads the frozen
rates of the same CostModel used for simulated execution. Backtest and
paper already freeze configured venue rates at initialization (PR #30).
Unfrozen schedules and unconfigured fallback routes do not imply a known
fee. No change to fee charging, order admission or legacy strategy logic.

Require Spot or perpetual, unit multiplier, distinct nonempty base and
quote currencies, and rates in [0, 10000) bps. Instrument identity includes
symbol, venue, asset class and currencies. Missing, ambiguous or mismatched
instruments return unavailable. Live executors without authoritative fee
support return None. Modeled zero is a known zero, not a missing-data
fallback. No exchange or database queries are introduced.

Native rates are Decimal. estimate_fee(notional, role) computes a
hypothetical single-fill quote fee for positive supplied quote notional,
using checked multiplication followed by division by 10,000, matching
the simulation's arithmetic. Invalid notional or overflow returns None.

## Drift API

Fields: symbol, venue, quote_currency, source ("frozen_simulated_fees"),
maker_fee_bps, taker_fee_bps, estimate_fee(notional, role). The function
takes exactly two arguments (numeric notional and "maker" or "taker").
Wrong argument shape or role returns undefined; invalid numeric conversion,
nonpositive notional or arithmetic failure returns null. Numeric values
use f64 with possible precision loss; Decimal execution is authoritative.
Detached callback snapshots do not refresh. Rust StrategyContext literals
now specify costs (None if unavailable); all workspace callers updated.

No fill or execution price is guaranteed. This API does not estimate
slippage, spread, funding, rebates, discounts, third-currency fees, or
total execution costs. Live fee evidence remains separate work.

## Verification

Three new regressions: native checked fee estimates including zero,
invalid metadata/rates/notionals and overflow; frozen schedule scope
excluding unfrozen and unconfigured fallback quotes; actual native/Drift
backtest runner checking modeled maker 5 bps and taker 10 bps versus
actual fees of 0.0495 on a 99 quote maker fill and 0.1 on a 100 quote
taker fill, and zero fees in a second run. Callback rates agree with the
report; the custom schedule is read exactly once per rate at freezing.

Rust 1.99.0 locked offline serial: 570 tests passed (406 backtest,
119 strategy, 15 paper, 25 indicators, 5 loader); three external tests
ignored. All-target Clippy with warnings denied passed for strategy,
loader, backtest, paper, live-trading and engine. Formatting/diff checks
passed. Full workspace CI and independent review pending. No production
databases, exchange accounts or live orders accessed.

Next F04.03 slice: spendable capital with explicit reservation semantics.
