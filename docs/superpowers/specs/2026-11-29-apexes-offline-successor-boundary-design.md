# F04.05 — Offline successor-boundary continuity

## Contract

Qualify two separately preserved, independently selected coherent
boundaries. Both stores must still expose their exact original startup
claim, coherent boundary, journal and capture. Re-run the existing
offline transition qualification against each store, using its supplied
historical evidence, and compare with its independently retained witness.
No discovered latest head or inferred historical claim is accepted.

The successor predecessor must exactly equal the prior closing checkpoint
receipt, including identity, cursor and envelope digest. The owner,
initial parameter values, instrument metadata, conversion scope roles and
pristine risk configuration must match. Run scopes must differ and the
successor closing cursor must advance. All prior inspection evidence must
precede the successor opening wallet interval; the first successor
observation must strictly follow that opening interval.

Require exact account-wide wallet amounts and asset coverage, including
explicit zero entries, and exact local tracked-asset allocation at the
link. This first contract is limited to flat prior strategy inventory:
the prior captured ledger and independent positions must contain no
nonzero strategy-owned position. Reject overlap in client-order IDs,
exchange order IDs and exchange trade IDs (venue/symbol scoped).
Do not infer that a distinct run scope alone proves disjoint execution.

## Limits

This is historical offline consistency across two isolated store
snapshots, not a repeatable live restart protocol. Matching endpoint
wallets cannot establish absence of unseen intervening transactions.
Risk configuration equivalence does not transfer the prior counters,
trips, or indicator accumulators into a successor. No ledger or strategy
instance, order authority or startup permit is returned. The singleton
startup claim and existing owned-position recovery gates are unchanged.

## Verification

Four focused unit tests cover exact predecessor, owner, configuration and
scope, wallet continuity and timing, duplicate execution identities and
flat tracked allocation. A real disposable PostgreSQL integration test
uses two independently preserved schemas and mock market/terminal data,
publishes flat coherent boundaries, and qualifies both witnesses; it
refuses a witness from the wrong run and checks the original claims and
boundaries remain intact. This test is not a production restart or
authoritative exchange qualification.

Rust 1.99.0, locked dependencies: 233 OMS/EMS/live serial regression tests,
893 core/risk/research/engine tests, plus the additional two-boundary
PostgreSQL test (1,127 total, zero failures, four external ignored).
Strict workspace Clippy, formatting and diff checks pass; disposable
PostgreSQL clusters removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Qualify a read-only owner-bound authoritative Spot evidence collector
for terminal orders, account-wide open orders and wallet observations,
with explicit coverage and timing failure handling. Keep live state
application, cross-run risk state continuity, atomic post-execution
transition and startup-claim changes gated.
