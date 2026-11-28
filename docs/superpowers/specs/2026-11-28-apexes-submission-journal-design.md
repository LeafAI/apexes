# F04.04 — Durable historical submission journal

## Activation and scope

Historical run_backtest accepts registry SubmissionJournalPolicy { path }
from apexes_backtest::submission_journal. Requires deterministic replay
identity and its verified dataset, fast replay and no-control constraints.
Default is absent: no file writes or report schema/fingerprint changes.
The path must be new, in an existing caller-controlled directory. Use
a different destination from the optional completed-replay archive.

This slice covers strategy-originated direct submissions and EMS parent
submissions reaching the executor after existing local admission checks,
including actual transit arrivals. Local refusals and intents merely
queued or expired are not submission attempts. EMS children, automatic
risk exits, later fills, settlement and notifications are outside this
journal. The recorded request fields are a correlation summary, not
a full reconstructible order or algorithm request.

## Record protocol

A single writer creates an exclusive JSONL file (Unix 0600), writes
and syncs a schema-1 header containing a unique run UUID, deterministic
replay namespace and verified ordered dataset digest, then syncs the
directory. Each record carries its sequence number, prior record digest,
typed event and SHA-256 of the record. The first prior digest is empty.
A Begin record contains the session intent ordinal, observed decision
or arrival timestamp, route, instrument, side, order type, quantity
and limit/trigger prices. It must sync successfully before the existing
submission call is invoked. End records are synced immediately after
the executor/EMS response, before existing bookkeeping and callbacks.

An OrderReturned contains the actual order identity and returned status;
ParentReturned contains the actual parent identity. Neither establishes
durable accounting or complete execution. UncertainError deliberately
does not classify an executor error as a confirmed rejection. Raw error
strings and credentials are not recorded. A missing End is likewise
uncertain, whether the call never started, was interrupted or completed
without recording its response. Do not automatically retry either case.

Writer rejects a duplicate ordinal, concurrent unresolved attempt,
wrong route, or absent identity. Any append failure poisons the writer.
I/O/size failures abort the opted-in historical run rather than bypass
the journal. An error after submission can leave an uncertain result;
do not treat the returned error as proof of no side effects. Finish
records the canonical report digest after final report construction
and optional archive publication. A missing Finish is an incomplete
run, even if every attempt has a response.

## Inspection

RecoveredJournal::load(path) validates complete newline-terminated
records, header version and identity shape, sequence/hash chain,
unique ordinals, attempt/result pairing, route and finish placement.
Bound total encoded size to 64 MiB, records to 64 KiB including newline.
A complete-record prefix can be inspected even with a final unresolved
attempt. A torn line or malformed chain fails closed; no automatic
repair, truncation, resume or append. uncertain_attempts includes both
missing results and recorded uncertain errors, even in a finished run.
matches_report compares namespace, dataset digest and final report hash.
The random run UUID intentionally distinguishes separate executions;
journal bytes are not deterministic across runs.

The hash chain detects record inconsistency, not authenticity, malicious
recomputation or removal of a valid suffix. Missing Finish cannot prove
why a run is incomplete. File/directory sync relies on OS and filesystem
durability semantics; physical power-loss qualification is not claimed.
Read after the writer has stopped, in a trusted directory. No automatic
trading-state recovery or live activation. In particular, this is not
an exchange idempotency key or a transactional OMS outbox.

## Verification

Three new unit tests cover incomplete synced attempts, explicit uncertain
errors, exclusive destination and permissions, torn/tampered evidence,
writer state/route/duplicate checks, size-limit poisoning, and invalid
transitions with freshly recomputed chains. Extended direct replay checks
only actual arrival attempts (IDs 1 and 4, not locally rejected IDs 2 and
3), returned identities/status and unchanged report fingerprints.
Existing cancelled-order versus executor-error replay now verifies their
distinct journal outcomes. Extended ten-run EMS replay checks two actual
parent identities and finalized report binding alongside the existing
archive, and two uncertain errors in the unsupported-policy case.
Interruption is modeled by dropping the writer at a synced record boundary;
no physical crash, power-loss or real-service qualification performed.

Rust 1.99.0 locked offline serial: 637 tests passed (446 backtest,
123 strategy, 23 EMS, 15 paper, 25 indicators, 5 loader); twelve
external tests ignored. All-target Clippy with warnings denied passed
for strategy, loader, backtest, paper, live-trading, engine and EMS.
Formatting and diff checks passed. Workspace CI and independent review
pending. No actual orders, production accounts or databases accessed.

Next: authoritative live OMS identity reconciliation and durable
client-order correlation. Keep live activation, crash-consistent child
and fill evidence, ledger/strategy recovery and exactly-once delivery
open. F04.04 remains in progress.
