# F04.05 — Opening causal valuation history preview

## Scope

Qualify independently selected bounded pre-start observations against the
original startup checkpoint and compare their hypothetical effect on the
exact retained runtime fee decisions. This is an offline, no-order,
valuation-only diagnostic, not strategy warm-up, ledger restoration,
source authentication or restart authority.

## Receipt and causal contract

The canonical opening observation receipt binds the original checkpoint
receipt, owner and exact completed observation history ending at the
checkpoint cursor. It is independently selected and can be written once
and read back without accepting noncanonical encodings. Scope, cursor
sequence, observation availability, digest and capacity are validated by
the existing observation receipt contract. The start time must be
strictly before the five-minute lookup horizon of both the checkpoint
and the earliest selected execution timestamp. This covers the lookup
interval conditionally on the supplied observations; it cannot prove
that no source records were omitted.

First require successful actual retained-prefix qualification, including
selected journal and final price-window verification. Reconstruct the
cold-start decisions using only retained run observations and compare
them exactly. Separately seed a counterfactual window using the supplied
opening observations. Process the same run prefixes in application order
with the same bounded source-time replacement and eviction. Never use
later prefixes or a source timestamp after the fill. Report compared
fills, missing valuations and exact per-fill changes of source or value.
Unchanged or missing results are not evidence of a complete history.
Opening prices never seed a live ledger, and no strategy callback,
executor, account state or startup permission changes.

## Verification

Focused offline regressions cover missing-to-valued decisions, stale
opening observations, same-source-time replacement, future prices,
receipt identity, scope, exact payload and lookback. The coherent-stop
fixture also reopens an independently selected opening receipt and
compares a fee-free journal; changed opening records fail closed.
With Rust 1.99.0 and locked dependencies, 226 OMS/EMS/live tests
(including isolated PostgreSQL suites) and 892 core/risk/research/engine
regressions passed: 1,118 total, zero failures, four external tests ignored.
Strict workspace Clippy, formatting and diff checks passed. The disposable
PostgreSQL cluster was stopped and removed. Full workspace build/test,
independent review, remote CI and real-service qualification remain open.

## Remaining work

Actual source completeness and independently authenticated market/funding
provenance remain open. Next qualify a causal no-order strategy warm-up
in isolation, separately from this valuation-only diagnostic. Nonzero
external-fee end-to-end qualification is still blocked by the existing
owned-base restriction. Risk and ledger state application, repeatable
post-execution restart, EMS callback recovery and periodic publication
remain open; existing one-time startup gates are unchanged.
