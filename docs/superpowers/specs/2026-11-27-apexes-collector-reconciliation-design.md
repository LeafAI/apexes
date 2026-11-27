# F03.03 — Offline collector receipt reconciliation

Validate the original schema-v1 JSONL collector journal, not receipts
derived from an export. Require a newline-terminated header, alternating
intent/commit records with zero-based consecutive indices, and exactly
one successful terminal record with the matching committed batch count.
Reject failures, missing/duplicate/extra records, invalid counts and
noncanonical source identities. A journal with zero committed batches is
a valid stopped capture, but not a qualified historical dataset.

Reconcile a complete ordered JSON array of RawDepthBatch rows for the
run (including any duplicate or unexpected indices). Check exact stored
payload SHA-256, count, invalidation count, run ID, index, depth event
type, symbol, venue and nondecreasing publication timestamps across
batches. Do not sort, deduplicate or repair input. Return original
ordered events and a report with journal digest and explicit false
upstream-completeness/attestation flags. The export must include every
row in the source run; offline verification cannot discover rows withheld
from the export itself.

The reserved manifest source identity is
binance-raw:<spot|usd_m_perpetual>:<run UUID>. Such a manifest requires
the original journal in the backtest Registry. Verify the whole input
against the original per-batch receipts using schema-v1 canonical JSON
serialization, and verify the single depth stream and configured asset
class. Refuse missing evidence, even if a truncated dataset was resealed.
Other historical sources retain their existing behavior. Report the
reconciliation result with the verified manifest. The offline command
does not access exchange or database accounts. Reconnect segmentation,
source-range qualification and empirical calibration remain separate.

## Usage

Export every row for the requested run, ordered by batch_index, with
run_id, batch_index and events_json unchanged as a JSON array. Do not
filter by a claimed receipt count or remove duplicates. For a local
export, the read-only offline command is:

```sh
cargo run --locked -p apexes-backtest --bin historical-manifest -- \
  reconcile-collector journal.jsonl batches.json > reconciled.json
```

The output contains report and events. Use the report source_id and
single stream in the dataset definition, seal the complete events array,
and register CollectorEvidence with the original journal alongside the
manifest for replay. The gate applies to the reserved source namespace;
it does not authenticate caller-selected source labels. Journal schema v1
binds the exact serialized event arrays and is not a signed attestation.

## Qualification

Rust 1.99.0 locked offline all-target check and Clippy with warnings
denied, formatting and diff checks passed. All 359 nonignored backtest
tests passed serially (six new regression tests), one external storage
test ignored. Full workspace CI and independent review pending. No
production data, live exchange or accounts accessed.
