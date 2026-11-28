# F04.04 — Lifecycle recovery qualification

## Recovery contract and observability

An exclusive bound OMS writer establishes its fill and terminal recovery
baselines once, during quiescent startup, after exchange reconciliation.
The reconstructed inventory includes historical executions, not proof of
historical strategy callback execution. Both callback-return cursors are
unknown in a newly opened process until new callbacks return.

Expose read-only TerminalDeliveryProgress alongside FillDeliveryProgress:
recovered terminal sequence, optional terminal callback-return watermark,
number of staged terminal records, latched terminal delivery failure,
and whether fill delivery/notification failure blocks terminal delivery.
These values are process-local and are not durable strategy receipts.
The existing ordered marker, fill checkpoint and receipt paths remain
unchanged. Repeated inventory initialization on the same manager is
refused and faults the attached health gate; it cannot discard pending
receipts or clear a latched delivery fault. Verify the bound inventory
lease before initialization.

## Restart qualification

Use real PostgreSQL order and fill persistence, the real managed OMS
and an isolated mock exchange, with a separate bound schema per
interruption. Drop and reopen the manager at each of these boundaries:

- Authoritative execution/terminal commit, before staging any fill.
- Fill staged, before ledger acknowledgement.
- Ledger acknowledged, before fill notification receipt.
- Fill notification returned, before terminal staging.
- Terminal staged, before its receipt.
- Terminal notification returned.

In every case verify original order, execution and scoped intent
identity; reconstructed history contains the execution exactly once;
the new baseline contains the persisted terminal marker but neither
callback cursor asserts historical delivery. A delayed duplicate
exchange update produces no new fill or terminal. A subsequent
execution with an identical intent ordinal in a new run scope is
distinct and delivers exactly one ordered fill/terminal batch. Also
verify attempted same-manager reinitialization cannot erase a staged
terminal or its failed receipt.

The live-loop integration test interrupts after the mock exchange fills
an accepted order but before local reconciliation. A fresh manager
performs the real recover_live_state sequence and reconstructs the
strategy-owned Spot position from journal fills with a native base fee.
Seed the live ledger with the independently recovered wallet (including
unowned base inventory), not a second application of historical fills.
The real unified loop observes the expected starting cost basis and
balances; a duplicate old exchange push has no callback or report trade;
the new scoped execution emits its fill and terminal in order. A
separate per-run health object represents the restarted process.

## Boundaries

These are deterministic drop/reopen fault-boundary drills, not a
forced operating-system crash, network/database failover or live
exchange qualification. No callback replay or exactly-once delivery
across process failures is promised. State that depends on callbacks
must be handled explicitly by F04.05; automatic dispatch repair and
atomic strategy-state/callback recovery remain open. Do not promote
F04.04 to fully complete while those qualifications remain unresolved.

## Verification

Rust 1.99.0 with locked dependencies and serial tests. All 164 OMS,
EMS and live PostgreSQL tests passed (87/32/45), using the exact CI
sequence on a verified disposable PostgreSQL 17 cluster, stopped and
removed after the run. The restart matrix includes six independent
schema/manager cases inside one test. All 717 nonignored core tests
passed; six external tests ignored. Workspace-wide all-target Clippy
with warnings denied, formatting and diff checks passed. Full workspace
build/test, remote CI, independent review and real-service qualification
remain pending. No real orders or production database operations.
