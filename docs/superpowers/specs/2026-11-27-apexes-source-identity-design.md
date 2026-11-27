# F03.03 — Source-specific historical event identity

## Offline normalization contract

A versioned IdentityPolicy declares a contract for every stream in a
DatasetDefinition, bound to the same caller-declared source ID.
The supported identity contracts are:

- Unavailable: preserve records without attempting identity inference.
- OpaqueTradeId: nonempty exchange trade ID, unique within the stream.
- NumericTradeId: decimal u64 identity; spelling aliases denote the
  same identity but different serialized payloads are conflicts.
- DepthIncrementSequence: unique incremental sequence number throughout
  the export. Retain snapshots and invalidations independently; the
  contract cannot be used if incremental IDs reset or are reused.
- FinalBar: immutable finalized candle identified by interval and open
  time, with a positive time range. Mutable candle revisions are not
  appropriate for this contract.
- UniqueTimestamp: quote or mark stream explicitly guaranteeing one
  event per timestamp. Do not infer uniqueness merely from a timestamp.

Never infer a contract from venue name. Scope identities to source,
venue, symbol and event kind. The source must not mix distinct market
types or ID namespaces within that scope. Require an appropriate
contract for every declared stream. Reject missing required IDs,
unordered or out-of-scope raw events, and conflicting payloads with
the same identity. Only identical serialized MarketEvent payloads
are considered safe duplicates (compared by SHA-256). Keep the first
occurrence and preserve chronological and tie order. Preserve records
with unavailable identity; the existing manifest seal still rejects
duplicate known trade IDs, even with an unavailable contract.

Normalization produces a separate event array, sealed manifest with
the identity policy, and audit report with per-stream input/retained/
duplicate/unidentified counts and ordered input/output content hashes.
The raw input is untouched. Preserve the audit and original source
records independently. Normalizing a normalized export is idempotent.
Manifest verification checks the declared identity contract and refuses
duplicates, rather than silently changing replay input. Old manifests
without identity policies continue to verify. Older readers may not
understand the extended manifest field.

## Usage

```sh
cargo run --locked -p apexes-backtest --bin historical-manifest -- \
  normalize definition.json events.json identity-policy.json > normalized.json
```

The output bundle contains manifest, events and deduplication.
Supply the extracted manifest and events to the existing verification
or replay API. Reconcile collection receipts with unique retained
records; raw collection counts are not interchangeable with counts
after normalization. Do not alter original receipts to hide gaps.
Range-based depth continuity and durable collection receipt generation
remain follow-up work. This is offline export preparation, not
ClickHouse ingestion-time deduplication or an upstream authenticity
guarantee. Production data and live execution are unchanged.

## Local qualification

Rust 1.99.0 locked workspace build, all-target Clippy with warnings
denied, formatting and diff checks passed. All 84 test binaries
exercised serially: 1,526 passed (1,056 core, 144 database, 63 supervisor,
263 UI), zero failures, 44 external/manual tests ignored. Nine new
regressions cover duplicates, conflicts, missing IDs, numeric aliases,
depth snapshots/invalidation, finalized bars, timestamps, policy scope,
manifest uniqueness checks, and CLI output. All 30 doc-test suites
passed. Temporary isolated PostgreSQL stopped after testing.
No live orders or production data used. Remote CI and independent
review pending.
