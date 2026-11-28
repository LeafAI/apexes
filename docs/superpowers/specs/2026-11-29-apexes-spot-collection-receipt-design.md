# F04.05 — Durable independently selected Spot collection receipt

## Contract

Seal a versioned receipt only after the strict read-only collector
successfully checks the exact selected coherent journal, owner-bound
terminal and trade observations, account-wide open-order coverage,
wallet and bounded observation intervals. Retain all comparison
evidence, including explicit zero asset amounts, the original inspection
timestamp and both checkpoint receipts. Bind the independent coherent
selection (including initial parameters), full instrument metadata and
the receipt body using separate SHA-256 domains. Sort wallet rows by
asset and refuse duplicates; retain the original terminal/trade sequence.

Write a bounded canonical receipt using the existing create-new,
file-and-directory-synced receipt writer. Limit the full file to 16 MiB;
do not enlarge the existing 64 KiB limit for other receipts. Read only
the selected regular file with an independently retained digest from
successful collection. Refuse mismatched digest, noncanonical encoding,
tampering, reversed intervals and incomplete owner/coverage fields.
The caller must retain the digest separately in a trusted location,
not obtain it from the same untrusted receipt.

Reinspect against the exact independently selected durable boundary
and instrument metadata. Re-run terminal, wallet and observation-window
comparisons with the original timestamp and recheck the journal.
Return the original historical evidence and time, never update the
inspection timestamp or issue a new freshness claim. No gateway
queries, order mutation, callback replay, claim or watermark changes.

## Limits

A checksum is not a signature, and the trusted source and independent
selection remain prerequisites. Neither persistence nor reinspection
authenticates exchange provenance, guarantees an atomic account
snapshot, or demonstrates absence of intervening activity. Current
freshness requires a fresh collection; the receipt is historical even
if it is reopened immediately. No automatic startup integration.

## Verification

The existing real-PostgreSQL flat and filled-run fixtures exercise
successful receipt collection, create-new persistence, exact independent
selection, reopening and reinspection, unchanged original time and
account evidence, and rejection of altered bytes, mismatched digests,
noncanonical encoding and changed instrument selection. The mock
source and isolated PostgreSQL are not a real exchange qualification.

Rust 1.99.0, locked dependencies: 235 OMS/EMS/live tests,
893 core/risk/research/engine tests, and 27 Binance wiremock REST tests
passed (1,155 total; zero failures; four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL was stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Qualify read-only collection of the opening checkpoint wallet and
tracked allocation independently of closing evidence, with a bounded
opening observation window and explicit funding/provenance limitations.
Keep actual risk/ledger application and post-execution startup
transitions gated.
