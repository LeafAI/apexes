# F04.05 — Read-only opening checkpoint wallet and tracked allocation

## Contract

Select the original opening checkpoint, owner and original strategy
parameters independently. The read-only OMS inspection uses the bound
inventory lease, exact database URL, owner digest and verified checkpoint
head. It requires no startup claim, no coherent successor boundary and
no local order, fill, parent-allocation or EMS history. The same
inspection runs before and after the external observations without
reserving the claim.

Use the strict Spot evidence source and require its owner to match
before any exchange request. Refuse invalid or duplicate instrument
scope. Independently supplied tracked allocation must be explicit,
sorted and unique, nonnegative and within the existing capacity
limits. Never infer strategy allocation from the account wallet.

Bracket the full account wallet observation with two account-wide
zero-open-order checks. Require all scoped base/quote assets, including
explicit zero rows, well-formed asset identifiers and no locked amounts
anywhere. Compare each selected tracked amount to the complete free
balance of that asset; partial attribution is unsupported. Preserve
untracked balances separately, without asserting that they are strategy
funding. Timestamp the actual query intervals and enforce the same
60-second elapsed/freshness ceiling as closing collection; fail closed
on clock regression, failed queries or a changed opening store.
The allocation timestamp is the observation/comparison time, not a
funding transaction timestamp.

## Limits

This is historical opening evidence, not a claim or a runnable ledger.
Equal balances do not establish ownership, funding history, continuous
quiescence, or absence of intervening transfers. The supplied tracked
amounts must come from an independently established allocation. The
existing later continuity inspection still requires the opening wallet
to precede execution and match the exact selected predecessor.
The collector does not reserve startup, integrate with automatic
startup, or reconstruct a missing historical opening observation.

## Verification

A disposable PostgreSQL fixture exercises owner and database mismatch
before queries, open-order refusal, incorrect allocation, missing
explicit asset row, locked funds, successful bracketed collection with
an untracked asset, and refusal after a coherent successor is published.
It verifies that the opening claim is not consumed and the later
coherent boundary remains unchanged.

Rust 1.99.0, locked dependencies: 236 OMS/EMS/live tests, 893
core/risk/research/engine tests and 27 Binance wiremock REST tests
passed (1,156 total, zero failures, four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Persist independently selected opening collection evidence, including
the complete wallet, explicit tracked allocation and both account-wide
quiescence intervals, with durable digest selection and later exact
predecessor/closing-journal reinspection. Funding provenance and actual
live state application remain gated.
