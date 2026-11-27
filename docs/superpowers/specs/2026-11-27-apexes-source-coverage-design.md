# F03.02/F03.03 — Historical source-range continuity

## Collection evidence

A source coverage document binds dataset ID, version, source ID and
ordered replay digest to caller-supplied collection receipts.
For every declared market stream, require ordered half-open UTC windows
covering the complete requested time range without gaps or overlaps.
Each receipt must have a unique nonempty ID, successful completion
status and expected event count. Explicit successful empty windows
are allowed; absence of a receipt is not equivalent to zero activity.
Reconcile expected counts with the exact events in each window.
Receipts must originate from the collection process and be preserved
independently of the exported records; generating receipts from an
already truncated export cannot discover missing upstream records.

Sequence contracts are explicitly declared per stream, not inferred
from the venue. Unavailable checks counts only. ContiguousTradeId
requires numeric u64 trade IDs; ContiguousDepthUpdate requires depth
sequence numbers and rejects invalidations. A sequenced nonempty
window requires inclusive first/last identifiers and a matching count;
verify each event and require consecutive ranges across empty
windows. Empty windows carry no sequence range. The contract assumes
one identifier per record with no resets over the entire dataset.
Do not select a unit-increment contract for range-based exchange depth
updates (including Binance batched final update IDs) or feeds whose
identifiers are not contiguous. Range-based source adapters and
collector-side durable receipts remain follow-up work.

## Replay and offline verification

Register SourceCoverage alongside DatasetManifest in the backtest
Registry. A coverage document without a manifest is refused.
The runner verifies the manifest and reconciles receipts before
strategy execution. The report carries the original evidence and
window/sequence-check counts. Empty sequenced streams do not count
as sequence-checked. Older reports default source_coverage to None.
Manifest-only and non-manifest runs remain unchanged.

```sh
cargo run --locked -p apexes-backtest --bin historical-manifest -- \
  verify-coverage manifest.json events.json coverage.json
```

This checks both sealed replay identity and the supplied source
coverage document. No storage mutation, live exchange calls or
runtime trading configuration changes are involved. Verification
does not independently authenticate receipt provenance or guarantee
upstream completeness: independently_attested and
upstream_completeness_verified remain false.

## Qualification

Rust 1.99.0 locked workspace build, all-target Clippy with warnings
denied, formatting and diff checks passed. All 84 test binaries
exercised serially: 1,517 passed (1,047 core, 144 database, 63 supervisor,
263 UI), zero failures, 44 external/manual tests ignored. All 30
doc-test suites passed. Eight new regressions cover explicit empty
windows, missing/failed/overlapping receipts, omission despite
resealing, sequence gaps, identity mismatch, depth invalidations,
fail-closed runner integration and CLI verification. Temporary
isolated PostgreSQL was stopped after testing. No production data
or live orders used. Remote CI and independent review pending.
