# F04.05 — Dual risk handoff-bound successor qualification

## Contract

Require separately selected prior closing and successor opening
risk receipts and their independently retained digests. Reinspect
each against its exact acknowledged durable capture, predecessor,
owner and run scope. Compare actual registered risk state and
separate market monitor state, refusing incomplete monitor coverage.
Require closing capture no later than closing retention, closing
retention no later than the start of the selected successor opening
wallet collection, and closing retention no later than the actual
successor opening risk capture.

Requalify the complete existing opening-handoff successor receipt
with the inspected opening snapshot, including exact transition
witnesses, journal, inventory, margin tiers and chronology. Bind it
with the prior closing handoff digest and original capture and
retention times in a distinct versioned checksum domain. Preserve
the existing weaker receipt formats; neither can substitute for
this stronger receipt. Use canonical bounded create-new persistence,
independent digest selection and full requalification before comparison.

## Limits

This is read-only historical qualification, not evidence of continuous
monitoring or authenticated production provenance. Local consumer UTC
does not establish exchange time. Independently retain both receipts,
digests and all underlying evidence. No live state application, funding
ownership, repeatable startup authorization, claim or watermark mutation.

## Verification

Extend the two-store PostgreSQL successor fixture to persist, reopen
and requalify the dual receipt and reject wrong digests, substituted
closing and opening receipts, tampered binding, overwrite and
legacy receipt interchange. Component tests refuse coverage loss and
incompatible retention chronology. Exchange I/O remains mocked.

Rust 1.99.0, locked dependencies: 242 OMS/EMS/live tests, 893
core/risk/research/engine tests and 27 Binance wiremock REST tests
passed (1,162 total, zero failures, four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Exercise a nontrivial registered risk-state successor boundary in the
isolated historical qualification path, including retained nonzero
state and refusal of a pristine reset. Do not enable live restoration
or bypass the existing one-time startup claim.
