# F03.03 — Recorded source qualification

PR #57 merged at 700447f. The prior offline reconciler cannot know
whether a caller omitted extra storage rows from an export. Qualify a
stopped, immutable run by reading its original independent journal and
querying ClickHouse for that run's entire ordered row set. Do not query
only receipt-listed indices. Select one extra row beyond the expected
count and fail on any missing, duplicate or unexpected index; no
silently truncated results. A separate read-only client never creates
tables or writes. The journal determines run identity and expected
count. Bound the query by 100,000 batches, 256 MiB raw payload, server
execution/result limits, and a 90-second client timeout. An oversized
run fails closed; qualify smaller runs instead. This is not a lock
against concurrent or subsequent database mutations.

After reconciliation (including original JSON byte digests, canonical
serialization and native reconnect chains), require at least one
synchronized segment. Derive dataset ID, version, source identity,
stream and UTC-day envelope from the observed run, not user-supplied
labels. Emit the original journal, manifest verification report and
exact ordered events in one JSON bundle. Include a method indicator
distinguishing direct ClickHouse reads from caller-supplied exports.
The day envelope is a replay scope, not full-day coverage. Neither
independent exchange attestation nor upstream completeness is claimed.
Keep invalidations and gaps. For replay, register the manifest and
original CollectorEvidence; the runner still rechecks original receipts
and instrument identity. No change to default trading provider.

## Usage

Point the explicitly required APEXES_CAPTURE_CLICKHOUSE_URL environment
variable at the authorized capture-only ClickHouse service. Use a
read-only database user/endpoint where possible. Only qualify a
successfully stopped run whose rows will no longer be changed.

```sh
cargo run --locked -p apexes-backtest --bin historical-manifest -- \\
  qualify-collector /path/to/run.jsonl > qualified.json
```

The bundle exposes qualification_method, journal, dataset and events.
Pass dataset.manifest and the journal to the historical replay registry;
use events in original order and the manifest's date/stream scope.
The CLI refuses failed, incomplete, corrupted, noncanonical and
invalidation-only runs. It does not write to the database.

## Verification

Locked Rust 1.99.0 check and Clippy for backtest/storage, formatting
and diff checks passed; all 402 nonignored tests passed serially
(368 backtest, 34 storage; three new offline regressions).
Eleven external storage tests ignored locally. One new ClickHouse-backed
integration test uses unique synthetic runs, verifies CLI output and
detects missing, nonconsecutive, duplicate and unexpected high indices.
It is included in the existing CI ClickHouse test command. Local Docker
service unavailable, so actual database integration remains pending CI.
Independent review and full workspace CI pending. No live exchange,
production database or orders accessed. Real recorded market data
qualification and empirical maker-fill calibration remain outstanding.
