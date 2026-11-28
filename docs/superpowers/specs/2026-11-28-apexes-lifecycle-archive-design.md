# F04.04 — Completed-replay lifecycle archives

## Scope and API

Register lifecycle_archive::LifecycleArchivePolicy { path } in the
historical runner's registry to opt in. Requires ReplayIdentityPolicy
and therefore a verified manifest, fast replay, and no external control.
Preflight rejects existing destinations and missing directories.
Ordinary runs do not collect archive snapshots or write files. The
shared loop captures final correlation and notification state only on
successful termination when requested. The historical runner publishes
after all report fields and child-latency accounting are finalized.
No report schema, fingerprint, order admission, matching, ledger,
reservation, callback ordering or exchange acknowledgement change.

Schema 1 contains the deterministic replay namespace, canonical report
SHA-256, last allocated intent ordinal, sorted direct order and parent
intent links, child ownership, ambiguous parent identities, notified
fill IDs and locally accumulated parent execution quantities. Unknown
associations remain null; conflicting parents' progress is unavailable.
This is final local correlation state, not the exact earlier callback
payload history or complete lifecycle event stream. It does not retain
unsubmitted intent contents, strategy variables or pending orders.

LifecycleArchive::load_verified(path, &report) reopens inspection-only
evidence, requiring the exact finalized typed report (retain it
separately). The recovered type cannot be directly deserialized and
has no trading-state installation API. Lookups expose last_intent_ordinal,
order_intent, parent_intent, child_parent, was_fill_notified and
parent_executed_quantity. Version, namespace, canonical report digest,
payload SHA-256, sorted unique IDs, valid intent ordinal ranges, parent
references, direct/child collision constraints and unique notified fill
IDs are checked. Valid parent progress must reconcile to actual report
fills with known final ownership; ambiguous totals must be unavailable.
Missing or inconsistent evidence fails validation. The checksum is not
a signature, authentication mechanism or upstream completeness proof.

## Publication

Bound encoded archive size and reads to 64 MiB. Serialize deterministically
using sorted arrays. Write an exclusive temporary file beside the
destination (0600 on Unix), sync file data, publish via hard link without
replacement, sync destination directory, and remove the temporary name.
Requires a filesystem supporting these operations. Existing destination
is protected even against a concurrent publisher. No automatic overwrite
or retry. An I/O error after linking can leave a valid published file;
inspect it rather than assume no publication. A crash can leave a
temporary file. Caller must control the destination directory; the
archive does not provide hostile-directory isolation or encryption.

## Verification

Extend the ten-run sealed two-parent replay: first run without archive,
nine archive-enabled runs, identical report fingerprints, byte-identical
archives, real parent/child/intent/fill lookup and quantities 2 and 4,
typed report JSON round-trip, tampered/truncated archive refusal,
mismatched report refusal, and existing destination preservation.
Two additional unit tests cover exclusive publication, permission mode,
cleanup and lookups, and structural invalidity even with recomputed
checksums (unsupported version, wrong namespace, duplicate IDs, invalid
ordinals, orphan child, contradictory direct/child link, missing fill,
missing or incorrect progress, ambiguous non-null progress).

Rust 1.99.0 locked offline serial: 634 tests passed (443 backtest,
123 strategy, 23 EMS, 15 paper, 25 indicators, 5 loader). Twelve
external tests ignored. All-target Clippy with warnings denied passed
for strategy, loader, backtest, paper, live-trading, engine and EMS.
Formatting and diff checks passed. Workspace CI and independent review
pending. No live orders, production accounts or databases accessed.

## Remaining work

Completed historical replay archival is intentionally not an in-flight
journal, durable exactly-once callback delivery or automatic session
restart. The next slice must define crash-consistent intent identity
and reconciliation evidence, including live OMS/EMS storage and
pending/exchange outcome uncertainty. Never resume trading by restoring
only these correlation maps. F04.04 remains in progress.
