# F03.02 — Position-aware simulated margin reservations

## Contract

For opt-in event-time unit-multiplier linear perpetual orders, reserve
initial margin only for the quantity that would open or increase
exposure, plus modeled fees on the entire requested quantity. Estimate
the margin and fee separately using causal depth or the order price
bound. Do not count unrealized gains, potential margin releases or
future closing proceeds as available funds at admission.

When no outstanding reservation on the symbol owns execution capacity,
an opposite-side intent may claim up to the current leveraged position
quantity as closing quantity. Reject other strategy intents on that
symbol while the claim remains outstanding; if other orders already
exist, use the conservative full-opening budget instead. Revalue
transit claims at arrival. Carry the claim through admitted resting
orders and fixed-route EMS parents; check the actual position direction
and remaining closing quantity before proposed fills commit.

Partial fills consume the closing allocation first, preserving the
opening-margin budget for a reversal's remaining opening quantity.
Terminal child IOC allocation releases use requested slice quantity,
which can conservatively reserve more margin after a partial or empty
execution. Actual fills remain subject to the projected ledger and
all other outstanding holds. Zero-cost pure reductions can have a zero
cash hold while retaining closing ownership. Existing emergency
preemption and terminal reconciliation release claims with their holds.

## Boundaries

The closing claim is intentionally exclusive per symbol, rather than a
shared close-quantity pool. This may refuse otherwise valid overlapping
orders. Fees still require available quote cash, and no speculative
closing proceeds fund admission. Margin-tier calibration, historical
queue-factor calibration and source provenance/continuity remain open.
Legacy immediate and live execution paths are unchanged. This is
simulation accounting, not a live exchange reduce-only order flag.

## Local qualification

Rust 1.99.0 locked workspace build, all-target workspace Clippy with
warnings denied, formatting and diff checks passed. All 80 test
binaries ran serially in groups: 1,490 passed (1,020 core, 144 database,
63 supervisor, 263 UI), zero failures, 44 external/manual tests ignored.
All 30 doc-test suites passed. Seven new regressions cover long
reduction, short close, reversal funding without prospective proceeds,
resting reversal partial fills and competition, EMS closing ownership,
allocation arithmetic and stale/reversed closing claims. Temporary
isolated PostgreSQL was stopped. No live orders or production databases
used. Remote CI and independent review pending.
