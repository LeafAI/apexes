# F03.03 — Public capture and receipt-bound replay qualification

PR #59 merged at 56fc5b4. This slice verifies the real public capture
transport separately from the storage/replay path and adds an operator
command to smoke-test a qualified bundle through the actual backtest
runner without placing any simulated or real orders.

## Real public probe

```sh
cargo test --locked -p apexes-market-data --test public_raw_depth -- --ignored --nocapture
```

The probe subscribes to BTCUSDT Spot and USD-M public depth for 20
seconds, requires synchronized snapshots and advancing native ranges,
and closes the bounded receiver. No credentials, orders or database.
On 2026-11-27 the initial probe observed Spot 0 snapshots / 0 deltas
and USD-M 1 snapshot / 186 deltas. Spot port 9443 failed a TLS
connection while port 443 returned WebSocket traffic. Use the standard
TLS port 443 only for the opt-in raw Spot capture via the centralized
BinanceNetwork endpoint. The existing trading endpoint is unchanged.
After the fix the probe passed: Spot 1 snapshot / 190 deltas, USD-M
1 snapshot / 186 deltas. These are in-memory transport observations,
not a durable capture or an independently attested coverage report.

## Offline replay qualification

Capture and qualify a stopped run using the PR #59 and PR #58 commands.
Provide instrument.json containing an explicit serialized Instrument
with the correct venue, market class, symbol and currencies, plus
positive tick size, lot size and multiplier. Then run:

```sh
cargo run --locked -p apexes-backtest --bin historical-manifest -- \\
  verify-collector-replay qualified.json instrument.json > replay-check.json
```

Verify the input manifest before the in-memory adapter can sort it.
Register the original manifest, journal and explicit contiguous depth
policy with a 30-second freshness bound. The actual runner must verify
the receipts, promote the range policy, observe every event at the
no-order strategy, generate zero fills and reproduce the input dataset
report exactly. The report includes the declared qualification method,
which is not independently established by reading the JSON. The
one-unit synthetic quote balance is not a trading-performance model.
This check does not assert empirical fill quality or market coverage.

## Qualification

Rust 1.99.0 locked offline targeted Clippy with warnings denied and
519 nonignored core/market-data/backtest tests passed serially
(80 core, 68 market-data, 371 backtest); three new offline tests and
one separately run live public probe passed. Five other external
tests were not run. The existing CI ClickHouse integration now also
exercises serialized bundle-to-replay CLI verification. Full workspace
CI and independent review pending.

Actual durable public-market capture remains unqualified: the default
Colima profile was stopped and left untouched; a separate temporary
profile timed out downloading its disk image, and an attempted native
ClickHouse download timed out without executing the partial binary.
No real database or orders were accessed. Repeat with an authorized
isolated ClickHouse store before claiming the complete live pipeline
is qualified. Production coverage and maker-fill calibration remain
open.
