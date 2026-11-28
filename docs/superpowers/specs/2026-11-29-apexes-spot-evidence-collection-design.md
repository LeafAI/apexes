# F04.05 — Read-only owner-bound Spot evidence collection

## Contract

A distinct strict read-only Spot evidence source has no submit, cancel,
reconcile or fallback methods. The Binance implementation derives the
existing inventory owner encoding from the actual configured recognized
Spot network, REST API-key fingerprint, selected strategy reference and
instrument scope. The engine and collector share that encoding. An
unknown endpoint, mismatched owner or unsupported scope is refused
before recovery queries; neither the API key nor secret is exposed.

Inspect the exact selected coherent journal before querying. Look up
each terminal order by client order identity, validate returned symbol,
side, exchange ID, terminal status and cumulative execution quantity.
Fetch order trades with an explicit page limit of 1000; refuse a saturated
page, invalid/duplicate trades, mismatched order/side or cumulative
quantity rather than asserting a partial page is complete. A definite
negative client lookup is distinct from a failed request; the existing
journal comparison decides whether that negative evidence is acceptable.

Count every raw account-wide open-order entry; no filtering of unknown
statuses or nonlocal client identifiers. Any entry prevents successful
collection. Fetch the complete Spot account wallet explicitly requesting
zero balances, retaining free and locked amounts, and refuse malformed
or duplicate balances. Missing required assets are not synthesized.

Timestamp the sequence in actual UTC, enforce a 60-second monotonic
query timeout and elapsed/freshness ceiling, and refuse backwards clock
observations. Reuse the existing exact journal, terminal, interval and
wallet validators, then recheck the selected boundary. Return evidence
only on complete success. No live gateway order changes or startup
claim/watermark changes.

## Qualification limits

Credential/network binding is the existing local owner identity, not an
independent proof of a permanent exchange account identifier. The trusted
adapter and authenticated transport supply the observations. A sequence
of REST queries is not an atomic exchange snapshot. Saturated trade pages
are refused rather than paginated; API history retention, intervening
activity, historical fee/funding/source provenance and real-service
qualification remain open. This slice is not wired into automatic startup.

## Verification

Local wiremock tests cover signed GET requests, full terminal trade
identity and quantity coverage, account-wide raw open order count,
explicit zero wallet balances and compatibility of the owner encoding
with the previous engine format. The disposable PostgreSQL fixture tests
owner refusal before queries, open-order refusal, failed/missing wallet
evidence, successful flat collection and unchanged claim/boundary.

Rust 1.99.0, locked dependencies: 235 OMS/EMS/live regression tests,
893 core/risk/research/engine tests and all 27 Binance wiremock REST
tests passed (1,155 total, zero failures, four external ignored).
Strict workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL was stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Qualify retained authoritative collection evidence in a durable receipt
with independent selection and exact reinspection, without claiming
current freshness on reopen. Keep live state application, cross-run risk
continuity and post-execution startup transitions gated.
