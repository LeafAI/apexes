# F04.05 — Atomic durable observation and partial-risk evidence

## Scope

Close the persistence gap of the opt-in actual observation and partial
registered-risk capture from PRs #110 and #111. Preserve the original
one-time startup claim, quiescent direct managed Spot publication
conditions, execution journal witness and fail-closed publication
behavior. No risk restoration or new startup authorization.

## Canonical capture

Encode the actual loop-local completed observations, selected scope,
clock, cursor-bound observation receipt, registered risk snapshots and
explicit unsupported coverage flags in a versioned canonical JSON
envelope. Validate the observation digest and exact predecessor/end
cursors, nested risk snapshot versions, role and count bounds (128
slots per role), and calendar-day consistency with the logical cursor.
Bound serialized envelope to 68 MiB in addition to the existing
observation limits. Reject malformed, noncanonical, unsupported
versions and inconsistent states rather than returning a partial decode.
Unsupported risk slots remain explicit; they do not become qualified
by persistence.

The existing optional capture path finalizes and encodes evidence
before invoking the new explicit attachment publication hook. Default
backends refuse that hook. Ordinary and checkpoint-only callers continue
using the existing hook without an attachment.

## Atomic persistence and independent verification

Add a bounded nullable BYTEA capture column to the existing singleton
strategy_checkpoint_boundary table; existing rows remain valid.
The coherent receipt retains an optional capture_sha256 field, omitted
when absent so legacy canonical receipts remain byte-compatible.
The OMS stores opaque bounded inspection bytes with their digest in the
same transaction as the head and journal boundary, under the same
owner lease, original claim, callback-prefix and journal checks.
Only successful commit acknowledges publication. This is a trusted
caller inspection attachment, not an independently verified runtime
capture at the OMS boundary.

The read-only verifier locks and checks the independently selected
exact receipt, bounded attachment length and SHA-256 of the stored bytes
before accepting the journal witness. An absent expected attachment,
unexpected attachment, or digest mismatch refuses verification.
CoherentRecoverySelection::inspect_durable_capture reads the exact
owner-bound snapshot and validates canonical typed capture, including
the selected predecessor and terminal cursor, without restoring a
strategy or monitor. The caller must retain the acknowledged receipt
independently; an ambiguous commit or crash before receipt retention
does not authorize discovering and trusting the latest checkpoint.

## Qualification

One new unit test covers canonical capture round-trip, wrong cursor,
noncanonical bytes and invalid nested risk version. Extend the actual
coherent-stop PostgreSQL integration to reopen and compare the stored
capture with the returned records and actual registered checker;
alter and remove the attachment, require refusal, then restore exact
bytes and verify again. Existing injected deferred commit failure
proves the head and boundary (including attachment) roll back.
Existing checkpoint-only zero-fill integration verifies the absent
attachment receipt remains valid.

Rust 1.99.0, locked dependencies and serial tests: 206 OMS/EMS/live
tests (97/32/77) passed with isolated PostgreSQL; 813 core/risk tests
and 60 engine tests passed (1,079 total), zero failures, six external
tests ignored. Workspace all-target check and Clippy with warnings
denied, formatting and diff checks passed. Disposable database
stopped and removed. Full workspace build/test, independent review,
remote CI and real-service qualification pending. No real orders or
production database operations.

## Remaining work

Atomic database persistence is not atomic capture of externally shared
monitor handles, proof of complete monitor configuration/state, source
completeness or authoritative exchange evidence. Next qualify complete
monitor coverage and offline risk/ledger state restoration contracts.
Keep the one-time claim restriction; no repeatable post-execution
restart, callback replay or periodic checkpoint is enabled.
