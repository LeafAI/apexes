# F04.05 — Verified live runner restoration

## Scope and safety boundary

PR #100 provides the owner-bound checkpoint head, exact publication receipt
and recovery selection. Connect that contract to the existing unified live
runner without guessing whether old market observations, OMS orders, fills
or lifecycle callbacks were represented in the strategy snapshot.

Until an atomic strategy/callback recovery boundary exists, explicitly
restrict restoration to a fresh managed Spot inventory with no execution
history. This includes no OMS orders (even zero-fill rejected orders),
fills, EMS parents/children or durable Spot parent allocations. Require the
existing startup reconciliation and recovered-wallet contract, an initialized
empty lifecycle baseline, a shared managed kill switch in Reconciling phase,
no recovered strategy positions, no open orders/reservations or recorded
faults, and an EMS URL exactly matching the inventory URL. The engine
performs its existing exchange startup reconciliation before entering this
runner; library callers remain responsible for supplying reconciled account
state. These checks are not a new proof of exchange-side reconciliation.

## Independent recovery selection

`RecoverySelection` stores a versioned canonical bounded JSON document
separate from the database head: SHA-256 of the independently established
stable inventory owner, exact acknowledged CheckpointReceipt, and sorted
original load-time parameter overrides. `write_new` writes a new explicit
file (no overwrite), requests mode 0600 on Unix, syncs the file and parent
directory, and returns success only after both syncs. A partial file is
invalid; an I/O error is not an acknowledgement. The trusted directory
and independently selected receipt are the operator's responsibility.
Read a specific regular file, bounded to 64 KiB, reject noncanonical,
truncated, oversized or invalid input. Never discover a latest file or
derive recovery selection from the current database head. Files and database
must be protected against malicious modification/rollback; hashes are not
authentication. The receipt writer is a library API, not an automatic
checkpoint exporter.

The engine accepts an optional local CLI flag:

```sh
apexes-engine --launch-config /trusted/launch.json \
  --strategy-recovery-receipt /trusted/selected-receipt.json
```

The flag is valid only for Live mode and a qualified scoped-only Drift
script. Load the exact original overrides into two isolated interpreter
instances inside the entered runtime. Their actual source, instrument
universe and load-time configuration are checked by the scoped codec.
The operator must qualify deterministic initialization and absence of
untracked risk-relevant globals/side effects before opting in. No receipt
or credentials are transported over the control network. No new automatic
UI/supervisor recovery configuration is enabled.

## Durable one-time startup claim

Under the existing owner lease connection, atomically check the exact
selected envelope and expected stable owner fingerprint, absence of any
prior startup claim and absence of durable execution history; insert a
singleton startup claim with synchronous commit. Do not replace or clear
a claim through this API. Refuse a second launch even if the earlier
process never placed an order, or a new head has been published. A crash,
ambiguous commit result or semantic restore failure after claiming can
consume the claim without a successful launch. This is deliberate
fail-closed behavior until atomic recovery is qualified, not a general
restart feature. Do not delete the claim to force recovery. An unbound
legacy schema cannot adopt orphaned checkpoint or claim data.

Restore only into the isolated candidate via the existing envelope
contract. On any failure, return before constructing EMS, subscribing
to market data or invoking the strategy; latch managed health. The
legacy managed runner requires an empty checkpoint store and no claim
instead of silently discarding saved state. Unbound standalone legacy
callers and managed schemas without checkpoints/claims keep their
previous behavior.

## Causal boundary and task lifecycle

Both startup paths use the same existing unified live loop. On verified
restoration, exclude market source timestamps at or before the selected
checkpoint observation timestamp (bars are available at close). Excluded
events do not advance startup readiness or strategy callbacks, but a
depth invalidation still engages the health gate. This is a conservative
lower-bound filter, not a causal history reconstruction or proof that
the gap before the first fresh event is complete. Do not infer
historical order/fill notification receipts from the restored state.

During qualification, the old runner dropped background halt-task
JoinHandles without aborting them; the detached cancel task retained the
inventory lease after runner exit. Own those tasks with an abort-on-drop
guard and abort/join them when the loop completes or errors. This prevents
a detached halt worker from retaining ownership indefinitely; do not
weaken the exclusive inventory lease. Existing OMS reconciliation remains
responsible for uncertain execution outcomes on process termination.

## Qualification

Real PostgreSQL disposable schemas, mock exchange and synthetic market
transport: independently write/read a receipt; restore a native strategy
into the actual live runner before its first fresh event; verify the
observed restored counter, pre-boundary filtering, zero fills, health
and persistent one-time claim; reopen and refuse repeated or legacy
startup even if the head is removed. Refuse missing, stale, wrong-owner
and wrong-database selections before callbacks. Reject rejected zero-fill
order history and durable parent allocations. A semantic candidate
failure consumes the claim and never invokes market callbacks. Unit
tests cover durable file publication, no overwrite, malformed/oversized
files and bar-close boundary. The engine test loads actual independent
scoped Drift interpreters and confirms distinct mutable parameter stores,
exact original overrides and legacy/no-live behavior.

Rust 1.99.0, locked dependencies and serial tests: 174 isolated
PostgreSQL-suite tests (91 OMS, 32 EMS, 51 live trading; includes two
new receipt/time unit tests and four new live-runner integration tests),
733 core tests and 60 engine library/binary tests passed; six external
core tests ignored. Workspace all-target Clippy with warnings denied,
formatting and diff checks passed. First qualification exposed the
halt-task lease retention defect; corrected and repeated the full
isolated database suite twice. All disposable clusters stopped and
removed. No production database or real orders were used. Full workspace
build/test, engine subprocess integration, independent review, remote CI
and real-service/crash-failover qualification remain pending.

## Remaining work

No automatic in-loop checkpoint publication or coherent durable
observation/fill/terminal/callback receipt. No restart of a strategy
with prior execution history or repeatable recovery from a consumed
claim. The next slice must establish a durable coherent execution and
observation boundary and atomic strategy-state/callback checkpoint
publication before general repeatable restore is allowed. Causal
no-order history warm-up and arbitrary interpreter globals remain open.
