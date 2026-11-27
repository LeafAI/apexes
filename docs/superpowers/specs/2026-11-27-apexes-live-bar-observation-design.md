# F04.02 — Live closed-bar observation and UTC bucket correctness

Trade aggregation uses exact UTC-aligned half-open buckets, stripping
subsecond remainders as well as whole-second offsets. Refuse trades
whose exchange timestamps regress relative to the last accepted trade
for that symbol; no backwards bucket transitions or late revisions of
OHLC/volume. Equal timestamps retain received order. Skip empty
buckets and unrepresentable date boundaries. This affects aggregation
only: the raw trade stream is still forwarded unchanged.

An explicitly selected live consumer UTC clock samples at dequeue in
the single shared loop. All loop decision, risk, funding-boundary and
submission timestamps use that observation time rather than backdating
to bar open/close or exchange trade time. Source event timestamps
remain unchanged for source-specific valuation, freshness and exchange
fills. A future source timestamp or regressing consumer UTC sample
fails closed with a contextual error before processing that event.
This clock is not an exchange timestamp or a measurement of gateway
arrival. Use only for live sessions; old launch JSON omits the policy,
historical and paper simulated clocks are unchanged. No periodic
clock events, source-completeness guarantee or wall-clock bar flush
yet. Gateway freshness/kill-switch policy is unchanged.

## Activation and limits

Add `"live_observation": "consumer_utc"` to an explicit live
engine launch JSON. The engine validates this setting as live-only,
registers the policy, and the live runner selects the consumer clock.
The supervisor and old launch files omit the setting by default.
No deployed session configuration is changed by this PR.

The consumer time is sampled once per dequeued event, not at the
exchange or WebSocket arrival boundary. It measures when the shared
loop observes the event, not exact network latency. Source market
timestamps and exchange fill timestamps are not rewritten. The
existing gateway freshness policy still applies to scheduled bar
close time; the new clock cannot make a stale bar fresh. Regressing
local UTC or future event timestamps return an error, ending that
loop rather than fabricating a timestamp or placing another order.
This is not a flatten/cancel guarantee; live session supervision
and exchange reconciliation remain necessary. No automatic
paper execution-clock migration or GUI configuration is added.

Aggregation now rejects all strictly regressing per-symbol trades
from its OHLC computation, including regressions within a still-open
bucket. The raw trade is still forwarded, so this does not silently
filter the raw market stream. Equal timestamps retain received order.
Closed bars still require a subsequent accepted trade to advance
the interval; no timer-generated closes or synthetic empty bars.

## Verification

Five new regressions: fractional-second UTC buckets (including
negative epoch), late first/same-bucket trades, future and regressing
consumer observations, actual shared-loop delayed bar and duplicate
gate/submission timing, and live-only launch JSON compatibility.
The simulated shared loop receives a bar closing at 60s at consumer
time 65s, produces one fill at 65s, and keeps equity time ordered.
No actual exchange orders, accounts, live-service or production
database access.

Rust 1.99.0 locked offline serial 624 nonignored tests passed
(401 backtest, 74 engine, 70 market-data, 15 paper, 64 supervisor).
19 ignored external/service tests were not run. All-target Clippy
with warnings denied passed for those five crates and live-trading.
Formatting/diff checks passed. Full workspace CI, independent review
and real live-service qualification pending.
