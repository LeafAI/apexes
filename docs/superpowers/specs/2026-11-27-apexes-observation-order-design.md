# F04.02 — Explicit historical observation-order manifests

Schema 1 remains byte-for-byte compatible: open-time order for
bars, original partition hashes, exact sequence verification.
Schema 2 explicitly seals the actual observation sequence (bar close
time, other events' timestamps). Stable equal-time order is preserved
from input; no inferred source arrival ordering. Partitions retain
source UTC day (bar open), while a bar must close no later than the
declared end boundary (an exactly midnight close is allowed). Reject
nonpositive bar intervals and regressing observation times. Domain
separate the schema-2 replay digest; no relabeling of v1 evidence.

Explicit migration verifies the exact original v1 manifest and
ordered events, requires a new dataset version, and stable-sorts
a copy by observation time; it preserves every event and normalized
identity contract. Return a new manifest and events with original
version and replay digest. Neither input is modified. Raw collector
receipt bundles are not migrated. Legacy coverage receipts do not
certify the derived sequence; schema-2 bar coverage is unsupported
until an interval-specific coverage contract exists. No automatic
reordering of a sealed export in the runner. New in-memory source
accepts already ordered events without sorting; legacy constructor
and storage reader unchanged. Schema 2 requires explicit event-time
latency even if no deterministic identity policy is registered.

## Usage

```sh
historical-manifest migrate-observation v1-manifest.json v1-events.json v2 > observation-export.json
historical-manifest seal-observation definition.json ordered-events.json > v2-manifest.json
historical-manifest verify v2-manifest.json ordered-events.json
```

The migration output contains the new manifest and ordered events;
pass these to the historical runner using the registered manifest
and `InMemoryMarketDataSource::from_observation_ordered`, plus
explicit `EventLatencyPolicy`. The new constructor validates and
preserves exact order. The old constructor still sorts by bar open
and therefore cannot substitute for the new path on overlapping
bars. The CLI operates on local JSON; storage-backed/engine launch
integration is not added in this slice. The migration provenance is
an operation result, not an independently authenticated chain of
custody. The model uses earliest availability at close, not actual
recorded delivery latency. Zero-length bars are refused in causal
validation (also for the existing deterministic identity policy);
legacy open-time seals are otherwise unchanged.

## Verification

Four new regressions: normalized v1 migration with stable ties,
schema domain separation/midnight horizon/malformed interval
rejection, read-only migration and sealing CLI, and an actual
Drift-backed overlapping-minute/second-bar replay. The latter
produces fills at fixture-relative 12500ms and 70500ms, never
at the wide bar's open, with zero regressing events and identical
canonical report fingerprints across ten runs. Missing event-time
latency and use of the legacy sorting constructor are refused.

Rust 1.99.0 locked offline serial 560 nonignored tests passed
(399 backtest, 15 paper, 116 strategy, 25 indicators, 5 loader).
Three external tests ignored. All-target Clippy with warnings denied
for backtest, paper and live-trading, formatting/diff checks passed.
Workspace CI and independent review pending. No production databases,
exchange accounts or actual orders accessed.
