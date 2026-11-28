# F04.05 — Durable offline post-execution transition witness

## Contract

Issue a historical witness only after the existing isolated cold-start Spot
ledger application and registered risk restoration qualifications succeed
against the same independently selected coherent checkpoint. The retained
observation prefix and conversion price checks, account-wide terminal and
wallet evidence, independent allocation, position and fee evidence, and
pristine risk configuration remain mandatory. The exact selected journal
and capture are read through the existing coherent recovery inspection.

The versioned witness binds the original checkpoint receipt, selected
coherent boundary (including journal and capture digests), owner,
independent selection including initial parameters, and inspection time.
Domain-separated SHA-256 digests bind all supplied independent evidence,
including instrument metadata, opening and closing wallet entries, terminal
and open-order coverage, allocation, positions, fee rates and marks,
observation receipt, conversion scope and risk candidate snapshots/tables.
Wallet maps are sorted; supplied ordered sequences retain their order.
The witness has a separate canonical body checksum. Persist using the
existing bounded create-new receipt writer and reopen with canonical
encoding and integrity verification. Compare a reopened witness only
against a newly qualified result using the same independent evidence.
The checksum is not authentication or proof that qualification ran.

## Limits

No runtime ledger, risk checker, strategy or execution authority is
returned. This does not consume, reset or replace any startup claim or
advance delivery watermarks. The independent inspection time is historical;
reopening never establishes current freshness. Provenance, source
completeness, general interpreter state, authoritative external funding,
atomic post-execution continuation and real-service qualification remain
open. The existing one-time startup restriction is preserved.

## Verification

The coherent-stop PostgreSQL fixture issues, persists and reopens a witness,
requalifies it, refuses overwrite and altered encoding, distinguishes a
changed independent inspection time and refuses missing risk evidence.
Rust 1.99.0, locked dependencies: 229 OMS/EMS/live tests including
disposable PostgreSQL suites and 893 core/risk/research/engine tests
passed (1,122 total, zero failures, four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL was stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Qualify an offline successor-boundary continuity contract linking
independently selected consecutive checkpoints and run scopes, with no
startup-claim mutation; actual restart remains gated on authoritative
collection and atomic transition semantics.
