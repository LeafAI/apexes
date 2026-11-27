# F04.02 — Quiet-market bar timing

## Scope

The gateway's optional `--quiet-bars` flag selects timer-driven
finalization via `BinanceMarketDataProvider::with_quiet_bars(true)`.
The provider default and supervisor-generated gateway configurations
remain unchanged. No production processes or configurations are modified.

A 250ms monotonic Tokio cadence samples UTC and advances a
nondecreasing aggregation watermark to UTC minus two seconds.
A populated bucket whose scheduled close is at or before that
watermark is delivered once as the existing Bar event. Empty buckets
are never synthesized. The original OHLCV and scheduled open/close
timestamps remain intact; simultaneous timer closures are sorted by
close time and symbol, with the fixed interval order breaking ties.
A subsequent trade crossing a bucket boundary can still close a bar
earlier, as before. The two-second allowance applies to timer-based
closure; it is not a universal reordering buffer.

Trades earlier than the watermark or last accepted trade timestamp
cannot revise or reopen aggregate history. Future-dated trades in
quiet mode are excluded from aggregation. The original raw trade
event still reaches the consumer unchanged. Equal timestamps retain
input order. No new market event variant is introduced.

## Recovery and timing boundaries

The isolated feed supervisor runs timer pulses independently of the
upstream trade idle watchdog. Only a real trade resets the watchdog.
Expired idle deadlines take precedence over pulses; missed timer
ticks are skipped. No timer runs during connection establishment
or retry backoff. Stream end, explicit upstream reconnect/error
notices or watchdog expiry abandon the attempt and discard its
unfinished populated buckets; the same discarded interval cannot be
reopened after reconnect. Quiet mode treats those reconnect/error
notices conservatively as a reason to restart the isolated stream.

UTC sampling assumes a synchronized local clock. A backwards
watermark does not move the finalization boundary backwards; a
forward UTC jump can advance the watermark. Timer closure is not
attestation of exchange feed completeness: a silent but unhealthy
connection may remain undetected until the watchdog or an explicit
error, and a new subscription may start mid-interval. This mode does
not reconstruct missed trades, deduplicate trade IDs or certify
complete exchange candles.

For live strategies selecting closed bars, use the explicit
`"live_observation": "consumer_utc"` policy from PR #73 to obtain
consumer-time decisions. This change does not alter legacy live
clock semantics, paper/backtest replay ordering, gateway freshness
rules, or recorded dataset coverage. No arbitrary strategy callback
is fabricated when there are no populated bars. Timer-only strategy
callbacks and real-service qualification remain separate work.

## Verification

Six new regressions:
- Populated-only watermark finalization, idempotence and late rejection.
- Discarded incomplete buckets cannot be reopened across a gap.
- Independent paused-clock upstream idle deadline.
- Timer stop and no flush on immediate EOF.
- Actual trade handler verifies grace boundary, deterministic cross-symbol
  order, raw late/future trade preservation, gap discard and resumption.
- Gateway CLI flag explicit opt-in.

Rust 1.99.0 locked offline serial: 660 nonignored tests passed
(75 market-data, 23 gateway, 401 backtest, 15 paper, 116 strategy,
25 indicators, 5 loader); 11 ignored external/service tests not run.
All-target Clippy with warnings denied passed for market-data, gateway,
engine and supervisor. Formatting/diff checks passed. Full workspace
CI, independent review and real-service qualification pending. No
real exchange orders or production databases accessed.
