# F03.02/F03.03 — Historical replay dataset provenance

## Contract

An optional historical dataset manifest identifies a versioned export,
its caller-declared non-secret source identifier, UTC day range
(start inclusive, end exclusive), and declared venue/symbol/event-kind
streams. Record a partition for each declared stream on every day,
including empty partitions. Record exact event counts and SHA-256 digests
of the serialized event sequence per partition, plus a whole-replay
digest that also binds equal-timestamp inter-stream order.

Schema version 1 hashes each serde_json MarketEvent encoding preceded
by its unsigned big-endian 64-bit byte length. Partition coverage is
bounded to 100,000 entries. Sealing refuses regressing timestamps,
events outside declared scope, and duplicate or empty known trade IDs
within venue/symbol. Anonymous records are not silently deduplicated.
Verification refuses unsupported schema, missing/duplicate/excess
partitions, malformed digests, changed counts/content, and replay-order
changes. Scope verification requires the backtest's configured market
universe and date range to match. Ambiguous repeated venue/symbol
instruments are refused because MarketEvent does not identify asset class.

## Backtest integration

Register a DatasetManifest in the backtest Registry to opt in to
verification. The runner verifies the complete loaded event sequence
before invoking the strategy and records the verified manifest,
event count and empty-partition count in BacktestReport.historical_dataset.
This records loaded input, not a claim that every event was executed
(e.g. a controlled early stop). With no manifest, the existing replay
path is unchanged and the report field is None; older reports still
deserialize. Paper/live execution is unchanged.

## Offline tooling

From the repository root, use the local JSON CLI:

```sh
cargo run --locked -p apexes-backtest --bin historical-manifest -- \
  seal definition.json events.json > manifest.json
cargo run --locked -p apexes-backtest --bin historical-manifest -- \
  verify manifest.json events.json > verification.json
```

The definition is a DatasetDefinition JSON object and events are an
ordered JSON array of MarketEvent records. Preserve the original
manifest independently of the data being checked. The CLI does not
mutate storage or execution settings.

## Boundaries

A sealed export fingerprints what was present; an empty partition
does not prove no market activity occurred. Manifest source metadata
is caller-declared and not independently attested. Verification checks
content against the supplied manifest, not the upstream exchange;
upstream_completeness_verified is explicitly false. Sealing already
incomplete data cannot discover missing upstream records. Timestamp
and market-scope checks do not yet establish exchange sequence-range
continuity, source-specific deduplication, complete collection windows,
or deterministic ordering at the storage ingestion boundary. These are
follow-up work. Recorded maker-fill empirical calibration also remains
open. No production dataset or live account was used.

## Local qualification

Rust 1.99.0 locked workspace build, all-target Clippy with warnings
denied, formatting and diff checks passed. All 84 test binaries
exercised serially: 1,509 passed (1,039 core, 144 database, 63 supervisor,
263 UI), zero failures, 44 external/manual tests ignored. All 30
doc-test suites passed. Ten new regressions cover daily coverage,
tampering, duplicate identities, tie order, malformed scope/schema,
fail-closed runner integration, backwards-compatible reports and
CLI parity. Isolated temporary PostgreSQL was stopped after testing.
Remote CI and independent review remain pending.
