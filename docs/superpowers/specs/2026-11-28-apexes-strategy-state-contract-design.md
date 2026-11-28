# F04.05 — Versioned strategy state contract

## Scope

Introduce an opt-in checkpoint envelope and native Strategy hooks. A restore
must validate the complete envelope before calling strategy code. Legacy
strategies report unsupported rather than silently accepting an empty state.
Boxed strategies delegate the same contract.

The caller supplies independently established strategy identity, revision,
configuration fingerprint, and recovery scope (including account/session
ownership and instrument universe). The strategy owns its nonzero payload
schema version. The observation cursor consists of a nonzero committed
observation ordinal and UTC observation time. Restore requires the exact
independently selected cursor; a valid old checkpoint is not interchangeable
with a newer one. No wall clock is consulted.

## Integrity and boundaries

Use a versioned canonical strict JSON envelope, a UTF-8 opaque strategy payload, a
SHA-256 digest of the serialized body, and bounded input and payload sizes.
Reject malformed, unknown-version, wrong-scope, wrong-schema, wrong-cursor
and corrupt or noncanonical records before invoking a restore hook. SHA-256 is corruption
detection, not authentication; the caller must protect storage and establish
the expected identity/cursor independently. Restore runs only on a fresh isolated candidate and swaps the active strategy
only after successful validation and restoration. The candidate must not share
mutable strategy state or external side effects with the active instance.
Strategy restore implementations must validate their entire payload.

This is a contract, not a durable store or automatic runner integration.
Do not serialize interpreter globals, indicators or cooldowns incompletely,
and do not claim atomicity with OMS fills or callbacks. Drift serialization,
durable checkpoint publication, verified restore integration and causal
no-order history warm-up are subsequent slices. No exchange access is needed.
