# F03.02/F03.03 — Historical depth range verification

## Source metadata and scope

The normalized DepthUpdate currently carries only a final sequence
number. The barter-data converter does not expose the original first
or previous-final update ID. Never manufacture these missing fields
from adjacent final IDs. Instead, a versioned DepthRangeEvidence
sidecar retains original source metadata for each incremental event
in the sealed replay. Bind it to dataset ID/version, source ID and the
ordered replay SHA-256. Each depth stream has an explicitly declared
contract, and each increment is bound to its zero-based replay index
and its normalized final sequence. Require complete evidence for all
declared depth streams, including an initial snapshot and at least
one increment per stream.

## Range contracts

- InclusiveNext: the advancing range must include the previous final
  ID plus one; overlapping absolute-level updates are allowed.
  Previous-final links are not accepted under this contract.
- PreviousFinal: the first advancing range must bridge the snapshot
  ID and have a previous-final ID no later than that snapshot.
  Subsequent increments must link exactly to the preceding final ID.
  Numeric IDs need not be contiguous under this contract.

Reject malformed ranges, missing or extra evidence, nonadvancing
final IDs, missing initial snapshots, explicit invalidations and
later snapshots that jump or reset the established chain. Same-ID
snapshots cannot reset previous-final link validation. The policies
are explicit source contracts, not venue-name inference. A new
independently qualified export is required after a known discontinuity.
The verifier checks sequence metadata, not complete book contents
or independent authenticity of the evidence.

## Replay integration

Register DepthRangeEvidence with DatasetManifest in the historical
backtest Registry. Verify the manifest and complete range evidence
before the strategy runs. Record evidence and reconciliation metrics
in the report. If the caller has selected the existing strict
Contiguous depth policy, the historical runner selects VerifiedRanges
after successful verification, retaining the original maximum age
and strict book integrity checks. This avoids incorrectly treating
a valid advancing multi-ID range as a unit-increment gap. SnapshotOnly
and absent depth policies are unchanged. Direct registry selection of
VerifiedRanges is rejected, including in paper mode. Runtime freshness
expiry still invalidates depth and cannot be repaired by deltas alone.
Use count-only source receipts for range-based streams rather than
asserting a unit-increment sequence contract.

```sh
cargo run --locked -p apexes-backtest --bin historical-manifest -- \
  verify-ranges manifest.json events.json ranges.json
```

The CLI verifies sealed content and range evidence without mutating
inputs. Preserve original raw source metadata independently. The
sidecar is not authenticated; it does not prove upstream completeness.
Raw exchange endpoint capture, storage persistence, durable receipts,
ingestion integration and production qualification remain open.
Existing ingestion/storage schemas and paper/live behavior are
unchanged. Recorded maker-fill empirical calibration remains open.

## Local qualification

Rust 1.99.0 locked workspace build, all-target Clippy with warnings
denied, formatting and diff checks passed. All 84 test binaries
exercised serially: 1,535 passed (1,065 core, 144 database,
63 supervisor, 263 UI), zero failures on the final run, 44
external/manual tests ignored. All 30 doc-test suites passed.
Nine new regressions cover both contracts, gaps, previous-final
links, event binding, missing evidence, snapshot jumps/invalidation,
same-ID snapshot handling, strict replay freshness and CLI output.
The first database attempt hit an existing schema initialization
ordering dependency; a fresh isolated cluster with crate unit suites
first passed all 144 database tests. Both clusters were stopped.
No live orders or production data used. Remote CI and independent
review pending.
