# F04.05 — Receipt-bound offline transition witness

## Contract

Require independent opening and closing collection receipts and their
independently retained digests, exact checkpoint selection, full
instrument metadata, and independently selected tracked amounts.
Reinspect the pair against the same preserved coherent journal,
including the original opening observation interval, closing terminal
and wallet evidence, ordered retained activity and exact account
cash-flow continuity.

Derive opening wallet, tracked allocation, closing terminal and wallet
evidence and inspection timestamp exclusively from this qualified pair.
The caller independently supplies closing positions, fee and mark
evidence, conversion scope, observation history and pristine risk
configuration. Apply the existing complete offline transition
qualification unchanged, including isolated ledger application,
retained fill observation checks and isolated risk restoration.

Bind the resulting original transition witness and both selected receipt
digests in a distinct versioned, domain-separated receipt-bound witness.
Use the existing bounded canonical create-new persistence, and require
an independently retained witness digest to read. Requalify the original
inputs and compare the exact result; substituting even another valid
collection changes the witness. Preserve the original supplied-evidence
witness format and do not treat it as a receipt-bound witness.

## Limits

A checksum is not authentication. Independent collection, selection
and protected retention are prerequisites. Historical account
observations do not prove funding ownership, continuous quiescence,
or current freshness. Neither witness is a startup permit or proof of
an atomic exchange snapshot. No order changes, startup claim changes,
watermark changes or runnable restored state.

## Verification

A disposable PostgreSQL flat-run fixture independently collects two
opening receipts before claiming, publishes a coherent boundary, and
collects closing evidence. It qualifies the full receipt-bound witness,
reopens and compares it after independent requalification, rejects
mismatched receipt digests, incorrect closing position owner, modified
witness and substitution of a different valid opening receipt. Original
claim and boundary remain unchanged. Exchange observations are mocked.

Rust 1.99.0, locked dependencies: 237 OMS/EMS/live tests, 893
core/risk/research/engine tests and 27 Binance wiremock REST tests
passed (1,157 total, zero failures, four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Qualify consecutive historical boundaries using independently selected
receipt-bound transition witnesses and exact run-scope linkage, without
introducing a repeatable startup claim or inferring funding provenance.
