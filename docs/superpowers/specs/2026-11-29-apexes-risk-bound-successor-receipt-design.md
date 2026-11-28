# F04.05 — Durable risk-bound successor receipt

## Contract

Issue a distinct versioned historical receipt only after complete
receipt-bound successor and exact opening risk qualification. Bind the
owner, both full coherent checkpoint receipts (including run scope,
journal and capture digests), both independently selected transition
witness digests, and a domain-separated digest of the exact independently
selected opening risk snapshot. Retain its original consumer UTC
timestamp. Do not assign a new observation timestamp during reinspection.

Use the existing 64 KiB bounded canonical JSON format, create-new file
and directory sync protocol and separate receipt digest selection.
Reopening verifies the canonical representation, structural linkage
and independently retained digest. Re-run the entire qualification
with independently retained inputs and compare the exact result;
reopening alone does not qualify evidence. The receipt stores a digest
of the opening risk snapshot, not a replacement for that snapshot.

## Limits

Checksums establish equality, not source authentication or independent
provenance. The two transition witness selections and opening snapshot
must still be obtained and retained through a trusted process.
Historical qualification of cold flat boundaries does not carry risk
state into a live runner, prove continuous monitoring, establish
funding ownership, or authorize a repeatable startup claim. No claim,
watermark or order mutation.

## Verification

Extend the two-store PostgreSQL successor fixture to issue, persist,
reopen and requalify the risk-bound receipt. Reject overwrite,
incorrect digest, changed opening risk timestamp, modified witness
binding and noncanonical encoding. The opening snapshot in this
fixture is selected from the test capture; this exercises integrity
and comparison, not independent production acquisition.

Rust 1.99.0, locked dependencies: 239 OMS/EMS/live tests, 893
core/risk/research/engine tests and 27 Binance wiremock REST tests
passed (1,159 total, zero failures, four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Qualify an independently retained pre-event opening risk snapshot
against the exact original checkpoint and run scope with a trusted
capture-time handoff; no retrospective reconstruction or inferred
reset permission. Retain explicit source and funding provenance
limitations. Live application and repeatable startup remain gated.
