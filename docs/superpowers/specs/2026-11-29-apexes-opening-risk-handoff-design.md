# F04.05 — Pre-event opening risk snapshot handoff

## Contract

The opt-in managed observation-capture runner accepts a separate
create-new receipt path and one-shot digest sender. At unified loop
entry, after existing restored-startup qualification and before
consuming any market or supplemental event, hand off the exact
opening risk snapshot already captured by the loop.

Bind the full original checkpoint receipt, owner fingerprint, actual
executor intent run scope, and actual registered risk and separate
market monitor snapshot with its original consumer UTC timestamp.
Use a distinct versioned domain-separated checksum, bounded canonical
JSON (16 MiB maximum), and the existing create-new file and directory
sync protocol. Send the digest only after successful persistence.
Refuse I/O failure or abandoned digest receiver before entering the
event loop. A failed handoff does not undo the existing startup claim;
a file created before failure is not evidence of an acknowledged run.

The read-only inspection requires an independently retained digest,
exact original checkpoint, owner and run scope, and equality with the
actual opening snapshot of the exact acknowledged durable capture.
A different run, incorrect digest, or absent/mismatched snapshot
cannot qualify. Return historical snapshot evidence only.

## Limits

The explicit opt-in is library-only; the engine's existing startup
and live authorization paths are unchanged. File persistence is
synchronous at pre-event entry. The trusted caller must independently
retain the digest and protect the destination; channel delivery is
not an acknowledgement of external durable retention. The handoff
does not prove exchange provenance, continuous quiescence or funding
ownership, nor does it restore risk state or permit another claim.
It describes local loop entry, not an atomic exchange snapshot.
Existing non-handoff capture paths remain compatible.

## Verification

The flat PostgreSQL fixtures now use the opt-in capture-time file
and independently delivered digest, inspect the exact acknowledged
snapshot, and reject wrong digests and cross-run substitution. The
two-run successor fixture selects the opening snapshot from the
capture-time handoff, not a retrospective copy of its database state.
A separate fixture refuses an existing destination before strategy
callbacks, preserves the file and prior head, and retains the burned
startup claim. Exchange observations are mocked.

Rust 1.99.0, locked dependencies: 240 OMS/EMS/live tests, 893
core/risk/research/engine tests and 27 Binance wiremock REST tests
passed (1,160 total, zero failures, four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Bind the independently selected opening risk handoff receipt itself
into durable risk-bound successor qualification and requalification,
rather than accepting a detached snapshot as the stronger evidence.
Keep all existing historical checks and startup restrictions.
