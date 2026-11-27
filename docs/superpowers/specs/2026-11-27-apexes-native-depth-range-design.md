# F03.03 — Native historical depth range metadata

## Contract

DepthUpdate optionally carries DepthSourceRange(first, last,
previous_final). Preserve the original source identifiers without
deriving missing endpoints from final sequence numbers. Validate
that a range belongs to an incremental event, its final ID matches
sequence, first is no greater than last, and a previous-final ID
precedes last. Absent metadata is omitted in JSON, preserving
the existing schema-v1 historical digest representation.

The raw_depth module decodes recorded unwrapped Binance Spot and
USD-M depth frames, including U/u/pu. Require an explicit market
kind and matching canonical symbol, validate levels and retain
zero-quantity deletions. REST snapshots retain their lastUpdateId
and require an explicit observation timestamp. Delta decoding
supports exchange event time or a caller-supplied observed publication
time; use one consistent causal clock for replay, particularly for
buffered updates published after snapshot acquisition. The decoder
does not itself synchronize a live stream or validate cross-event
continuity; historical range verification provides the latter.

Existing barter-data conversion still emits absent source_range:
its normalized book events discard the original range endpoints.
Do not infer them or attach metadata from a separate uncorrelated
stream. Synthetic relay snapshots explicitly clear the incremental
range. The supervised live feed has not been replaced.

## Persistence and historical evidence

ClickHouse depth rows store three nullable UInt64 columns:
source_first, source_last, source_previous_final. StorageWriter
initialization adds them idempotently to an existing table. Old
records remain NULL. Run the writer/schema migration before upgrading
read-only consumers of old databases; reader initialization does not
mutate schema. Writer and reader row conversion reject malformed
or partial range metadata. No production migration was executed.

The manifest digest includes native metadata whenever present.
Existing external sidecars must agree with native values. A native
evidence builder accepts explicit per-stream contracts, refuses
missing original ranges, and verifies the generated sidecar against
the full sealed event sequence. This is available offline:

```sh
cargo run --locked -p apexes-backtest --bin historical-manifest -- \
  build-ranges manifest.json events.json contracts.json > ranges.json
```

contracts.json is an array of NativeRangeContract records (stream
and contract). Use inclusive_next or previous_final only according
to actual source semantics. Legacy manifests remain valid; old
records lacking native ranges can use the previous externally
recorded sidecar path, but cannot generate native evidence.

## Qualification and remaining work

Rust 1.99.0 locked workspace build and all-target Clippy with warnings
denied, formatting and diff checks passed. All 84 test binaries
exercised in serial groups: 1,548 passed (1,078 core, 144 isolated
PostgreSQL, 63 supervisor, 263 UI), zero failures, 44 external/manual
tests ignored. All 30 doc-test suites passed. Thirteen new tests
cover raw frames, timestamps, malformed ranges, legacy serialization,
storage row conversion and native evidence generation. Existing
gateway snapshot and real ClickHouse integration tests were extended.
Local Docker was unavailable, so real ClickHouse migration/round-trip
qualification awaits CI; existing-table migration must also be
qualified before production deployment. No live orders or production
data used; temporary PostgreSQL stopped.

Supervised raw capture and synchronization, collector receipts,
ingestion integration, production dataset qualification and empirical
maker-fill calibration remain open. No upstream completeness or
authenticity claim. Remote CI and independent review pending.
