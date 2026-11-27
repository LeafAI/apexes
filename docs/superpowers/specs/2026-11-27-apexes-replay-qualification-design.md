# F03.04 — Mixed-event deterministic replay qualification

Exercise the real historical runner with sealed synthetic fixtures,
fresh registry/risk state and the same explicit replay namespace.
Verify fills, fees, timing, funding effects, risk refusal and exact
report fingerprints, not just equality of empty reports. Use two
distinct policy profiles: bounded Spot IOC with event-time latency and
strict depth, and unrestricted leveraged perpetual with funding.
Include depth, trade, tick, bar and mark-price events, including
equal timestamps. Preserve the recorded input order. Never claim
exchange data coverage or empirical calibration from synthetic data.

The fixtures constrain strategy behavior and providers; arbitrary
external strategy state is not attested by a report fingerprint.

## Causal observation preflight

The manifest preserves the legacy open-time ordering of bars. The
event-time execution loop observes bars at close time, so an
open-time-sorted mixed input can regress and silently skip subsequent
observations. For the opt-in ReplayIdentityPolicy only, reject bars
without explicit event-time latency, invalid bar intervals, and
regressing causal observation timestamps before strategy execution.
Equal observation timestamps preserve sealed order. No automatic
sorting, repair or modification of source evidence. Legacy replays
without ReplayIdentityPolicy are unchanged. Overlapping bars that
violate this contract require a future observation-time input format
rather than rewriting sealed events.

## Verification

The Spot profile exercises bounded IOC, snapshot-only depth freshness,
500 ms event latency, risk maximum order quantity, maker and taker fees.
The two fills occur at 500 ms and 3000 ms, fees 0.1 and 0.0495 USDT,
with two scheduled and arrived intents, zero expired or regressing events.
The leveraged perpetual profile uses unrestricted matching, event-time
latency, 10x isolated leverage and 10 bps funding. Final equity is
9998.701 USDT vs 9998.801 without funding; the fingerprints differ.
Both profiles contain all five market-event types and equal timestamps;
ten fresh runs per profile produce identical report digests. A third
regression rejects causally invalid sealed bars before strategy calls.

Rust 1.99.0 locked offline 419 nonignored targeted tests passed
(381 backtest, 23 EMS, 15 paper); twelve external tests ignored.
All-target Clippy with warnings denied passed. Full workspace CI and
independent review pending. No production database, accounts or orders
accessed. This is synthetic qualification, not empirical validation.
