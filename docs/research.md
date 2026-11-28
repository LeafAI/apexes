# F05.01 — Versioned research artifacts

## Design and scope

`apexes-research` is an offline Rust library, independent of exchange credentials,
ClickHouse and the strategy registry's mutable current-version pointer. It retains
strategies, datasets, hypotheses, configurations and experiment results in one
local content-addressed store. It does not introduce another trading loop, change
risk defaults, promote strategies, optimize parameters or qualify live execution.

Each artifact has schema version 1, a kind, logical name, caller-supplied revision,
sorted string metadata and exact content bytes. Its identity is SHA-256 of the
compact serialized envelope, including all those fields. Changing bytes, revision,
name, kind or provenance creates another identity; publishing identical content is
idempotent. Revisions are labels, not incrementing counters: distinct content may
share a label, so references must always use content identities. Metadata insertion
order does not affect identity. Arbitrary binary content is supported.

An experiment pins four typed artifact identities, a runtime identity and a seed.
`ResearchStore::run` validates schema, hashes and kinds before invoking an executor.
Successful nonempty output is stored as a Result artifact containing the complete
experiment manifest and exact result bytes. Failed executions produce no result
receipt. `verify` reloads that manifest and all inputs, invokes the supplied executor
again and requires byte-for-byte equal output. Multiple different results for the
same inputs are retained rather than overwritten; verification detects divergence.

The caller must pin an actual executor/build and dependency identity in `runtime`,
check it in the executor, honor the supplied seed, and decode all execution options
from the retained configuration. Include fees, latency, liquidity policy, instrument
rules, initial balances, risk settings, data filtering and experiment date ranges.
The library validates references and bytes, not application-specific configuration
semantics or whether an executor truthfully used the inputs. It cannot make a
nondeterministic or dishonest callback reproducible. For trading experiments, call
the existing `apexes_backtest::run_backtest` / unified loop, not a research-only
simulator. Source-coverage and data-quality receipts remain separate qualifications;
a dataset hash alone is not evidence of clean or complete market data.

## API usage

```rust,ignore
use apexes_research::{Artifact, ArtifactKind, Experiment, ResearchStore};
use std::collections::BTreeMap;

let store = ResearchStore::new("./research-artifacts")?;
let retain = |kind, name, bytes| {
    store.put(&Artifact::new(kind, name, "1", BTreeMap::new(), bytes)?)
};
let strategy = retain(ArtifactKind::Strategy, "candidate", strategy_bytes)?;
let dataset = retain(ArtifactKind::Dataset, "history", dataset_bytes)?;
let hypothesis = retain(ArtifactKind::Hypothesis, "cost-aware-edge", hypothesis_bytes)?;
let configuration = retain(ArtifactKind::Configuration, "simulation", config_bytes)?;
let experiment = Experiment::new(strategy, dataset, hypothesis, configuration,
    "<executor-build-digest>;<dependency-lock-digest>", 42)?;
let result_id = store.run(&experiment, execute_retained_inputs)?;
store.verify(&result_id, execute_retained_inputs)?;
```

`execute_retained_inputs` accepts `&ResolvedExperiment` and returns
`Result<Vec<u8>, String>`. It receives all four verified artifacts, runtime identity
and seed. Return deterministic serialized economic results: canonicalize maps,
exclude wall-clock durations, and normalize randomly generated correlation IDs
without discarding prices, quantities, fees or timestamps. `get(result_id)` exposes
the retained envelope and JSON receipt bytes for audit/export. Retain the result ID
and the complete store directory together; copying only a result file omits inputs.
Keep the matching executor build and dependency environment available separately:
the store records their identity, not their binaries or installation environment.
No CLI, database migration or automatic engine integration is required.

## Storage and safety

The store creates/canonicalizes an explicitly supplied root. Names and revision
labels are never filesystem paths: only validated lowercase 64-character hex IDs
become `<id>.json`. Reads verify regular files, size, schema and content identity;
symlink artifact entries, unknown fields and unsupported schemas are rejected.
Text fields and metadata counts are bounded. Both encoded files and reads are
limited to 64 MiB; JSON byte-array encoding means usable content can be smaller.
Large datasets should be partitioned before retention; remote/chunked stores are
not part of this implementation.

Writes use an exclusive temporary file, file sync, atomic same-directory hard-link
publication without replacement, and directory sync. Concurrent identical writes
converge on one artifact; a corrupt existing entry fails rather than being silently
repaired. A crash can leave `.pending-*` files, which are never treated as artifacts.
A storage error does not imply rollback: publication can precede a sync or cleanup
failure. Repeating `put` with the same artifact is safe; experiment callbacks must
be safe to rerun and must not place live orders.
Keep the root owned by a trusted user with appropriate filesystem permissions.
Concurrent hostile filesystem replacement is outside the threat model. Do not
store credentials or personal data. This is an integrity store, not encryption,
signature authentication or an access-control service. The persistence contract
requires a Unix-like filesystem with hard links and directory fsync; Windows and
filesystems lacking those operations are not qualified and return I/O errors.

## Reproducible verification

Use Rust 1.99, as in workspace CI:

```sh
cargo test --locked -p apexes-research -- --test-threads=1
cargo clippy --locked -p apexes-research --all-targets -- -D warnings
cargo test --locked -p apexes-backtest --test trend_guard_baseline -- --test-threads=1
cargo fmt --all --check
```

Optional coverage (requires `cargo-llvm-cov` and the Rust `llvm-tools-preview`
component):

```sh
cargo llvm-cov --locked -p apexes-research --summary-only -- --test-threads=1
```

The research tests cover stable identities, version changes, metadata ordering,
round trips, concurrent publication, byte corruption, invalid references, wrong
kinds, schema validation, symlinks, failures and replay divergence. The
`research_receipt_replays_retained_strategy_dataset_and_costs` integration test
retains the unchanged V1 Drift script, synthetic prices, hypothesis and nonzero
fee configuration, then reopens the store and executes retained inputs through
the existing Drift runtime and unified backtest loop. Its runtime label hashes the
executing binary, fixture harness and dependency lock; remaining backtest constants are defined by
that harness, not a generic BacktestConfig serializer. It compares the existing
normalized economic report (random correlation IDs removed), including fees and
equity. Synthetic results are reproducibility evidence, not profitability, verified
historical-source coverage, production readiness or external-service qualification.
