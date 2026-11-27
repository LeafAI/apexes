# F03.02 — Offline historical queue-factor calibration

## Input and model

Calibrate the existing isolated passive-order queue approximation, not
a reconstruction of exchange L3 queue priority. A versioned observation
dataset identifies a single venue/symbol and its market and order-fill
sources. Each sample has a unique acknowledged passive order ID,
acceptance timestamp, limit price, requested quantity, complete
observation horizon, actual maker-filled quantity, a causal admission
snapshot, and observed trade tape through the horizon. Orders must be
isolated: no overlapping observation windows or competing own orders.
Public L2 depth and trade data alone do not provide observed maker
execution labels. Do not derive purported real labels from the same
simulator.

Validate snapshot age, complete positive sorted uncrossed sides, passive
admission, market identity, known trade aggressor side, positive price
and quantity, ordered in-window timestamps, globally unique trade IDs,
unique order IDs, label bounds, and nonoverlapping chronological
windows. Require an explicit train/holdout time boundary with no
straddling observations and nonempty partitions. These are structural
checks, not proof of source completeness or authenticity.

The isolated-order prediction is
`min(requested, max(eligible_trade_volume - factor * visible_ahead, 0))`.
Visible ahead uses same-side depth at or better than the limit;
eligible trades use the opposite aggressor side at or through the
limit. This mirrors the existing resting matcher without competing
own orders, queue replenishment or depth invalidations. Matcher parity
is exercised against actual MatchingEngine replay in regression tests.

## Selection and reporting

Evaluate a caller-specified grid of 2–1001 distinct decimal factors in
[0,1]. Rank by training mean absolute fill-fraction error; ties choose
the higher factor, reporting the number of equally scoring candidates.
The holdout is evaluated with the selected factor only. Report
per-candidate training errors, selected holdout metrics and per-order
predictions, observed/predicted fill fractions and full-fill mismatches,
dataset identity/version, evidence classification, model version, split
and snapshot-age policy. A unique training minimum is not proof of
statistical identification. No runtime configuration is changed.

## Usage

From the repository root:

```sh
cargo run --locked -p apexes-backtest --bin queue-calibrate -- \
  crates/apexes-backtest/tests/fixtures/queue-calibration-synthetic.json \
  > /tmp/queue-calibration-report.json
```

Use the fixture as the input schema example. For recorded observations
replace the synthetic source metadata and labels with real,
reconciled historical evidence, specify the intended holdout boundary
before fitting, and preserve the input and output for reproducibility.
The CLI reads local JSON and emits a JSON report; it makes no exchange
calls and never changes live or backtest settings.

## Qualification and limitations

The explicitly synthetic four-order fixture recovers its known factor
0.5 with zero held-out fill-fraction error. That is a procedure test,
not an empirically calibrated market parameter. Recorded maker-order
labels were not available in the repository fixtures; empirical
calibration, market-data provenance and source-range continuity remain
open. Input source metadata is caller-declared. No automatic
deployment, multi-order/L3 inference, or real-market performance
claim is made.

Rust 1.99.0 locked workspace build, all-target workspace Clippy with
warnings denied, formatting and diff checks passed. All 82 test
binaries exercised serially: 1,499 passed (1,029 core, 144 database,
63 supervisor, 263 UI), zero failures, 44 external/manual tests ignored.
All 30 doc-test suites passed. Nine new regressions cover recovery,
determinism, holdout isolation, ties, invalid inputs, matcher parity,
and CLI/report parity. Isolated temporary PostgreSQL was stopped.
No live orders or production databases used. Remote CI and independent
review pending.
