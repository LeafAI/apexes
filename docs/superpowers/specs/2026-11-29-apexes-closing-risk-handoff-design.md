# F04.05 — Independently retained closing risk handoff

## Contract

The explicitly opted-in paired risk handoff runner persists the original
opening risk handoff before event consumption and the actual closing
risk snapshot after successful coherent checkpoint acknowledgement.
Record the closing inspection timestamp in the canonical observation
capture, rather than substituting a later file-retention timestamp.
Legacy captures remain readable; missing closing capture time cannot
qualify a closing handoff.

Bind the original predecessor, inventory owner, exact acknowledged
coherent boundary (including run scope, journal and capture digests),
registered risk, separate market monitor, margin tiers, coverage flags,
original capture timestamp and post-acknowledgement retention timestamp.
Check the canonical capture digest against the acknowledged receipt.
Persist bounded canonical JSON (16 MiB maximum) with a separate
checksum domain and the existing create-new synchronized file protocol.
Deliver the digest separately after persistence.

Read-only reinspection checks the independently selected digest, exact
predecessor, owner and acknowledged boundary, and exact risk snapshot
from the verified durable capture. A different run or missing closing
timestamp cannot qualify. No runnable risk state is returned.

## Failure semantics and limits

The closing handoff is not a database transaction participant. If it
fails after checkpoint acknowledgement, report that fact and fault
the existing gate. Do not roll back the committed boundary, reset the
startup claim or overwrite the destination. The caller must preserve
and independently retain the evidence and reconcile the acknowledged
boundary before further use. The handoff is explicitly opt-in and
library-only. Its timestamp is local consumer UTC, not exchange source
time, and its digest is not source authentication. No live risk-state
application, funding provenance or repeatable startup authorization is
inferred. Existing runner entry points remain compatible.

## Verification

The flat PostgreSQL fixture retains both risk handoffs, selects their
digests, reinspects exact acknowledged capture state and rejects wrong
digest, changed owner and different-run closing evidence. A separate
collision fixture checks that the closing destination is preserved,
the gate is faulted, the already acknowledged checkpoint remains
committed, and the existing startup claim is retained. Exchange I/O is
mocked.

Rust 1.99.0, locked dependencies: 241 OMS/EMS/live tests, 893
core/risk/research/engine tests and 27 Binance wiremock REST tests
passed (1,161 total, zero failures, four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Bind independently selected prior closing and successor opening risk
handoffs into the full durable successor qualification. Compare exact
risk state and original chronology, with no detached snapshot
substitution or startup authority.
