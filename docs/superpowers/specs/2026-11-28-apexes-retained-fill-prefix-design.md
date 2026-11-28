# F04.05 — Retained loop-local fill observation prefixes

## Scope

Extend the opt-in coherent-stop observation capture with actual loop-local
fill application prefixes. Qualify the exact durable attachment in a read-only
offline diagnostic, not strategy callback provenance, source authenticity,
runnable restoration or startup authorization.

## Capture contract

The shared event loop serializes market and supplemental fill application.
Every market-arm fill is applied after that event's price maintenance and
before any later market event. After successful completion of the arm's
decision path, retain the IDs of newly applied fills in application order,
with the number of completed observations (including that event).
Supplemental fills retain the current completed prefix without advancing
it; duplicate or failed applications are not included. No exchange fill
timestamp is used to infer availability. Simulated execution projections
do not contribute. An invalid completed cursor or failed publication
produces no qualified attachment.

The optional canonical attachment field is bounded by the existing 8,192
decision limit and checked against complete retained attribution decisions:
exact ordered IDs, equal cardinality, nondecreasing counts and no count
beyond the receipt. Missing legacy evidence remains distinct from a
retained empty prefix. The existing capture digest binds the new bytes
to the selected coherent checkpoint; no new database transaction, recovery
gate, accounting path or callback is introduced.

## Offline qualification

First require exact selected journal, conversion scope and independently
selected observation receipt with verified final conversion windows. Load
the durable capture; refuse missing actual prefixes. Reuse the prior
exact per-fill comparison with the retained prefixes, reconstructing the
corresponding bounded causal windows. The existing independently supplied
comparison remains available for legacy evidence and is not promoted to
actual capture. Neither API proves source completeness or independently
authenticated exchange provenance.

## Verification

The focused observation-receipt suite passes 10 tests, including prefix
counts before, during and after market iterations despite different source
fill timestamps. The coherent-stop fixture checks the actual supplemental
fill prefix after reopening, refuses invalid coverage/counts and checks
canonical legacy decoding. With Rust 1.99.0 and locked dependencies,
224 OMS/EMS/live tests (including PostgreSQL suites in a disposable cluster)
and 892 core/risk/research/engine regression tests passed, for 1,116 total,
zero failures and four external tests ignored. Strict workspace Clippy,
formatting and diff checks passed. The disposable PostgreSQL cluster was
stopped and removed. Full workspace build/test, independent review, remote
CI and real-service qualification remain open.

## Remaining work

Complete causal valuation history beyond the selected capture boundary,
nonzero external-fee end-to-end qualification (still blocked by the
conversion-instrument/owned-base restriction), authoritative observation
and funding provenance, causal no-order warm-up, risk/ledger restoration,
repeatable post-execution restart and in-loop periodic publication remain
open. No startup permissions change.
