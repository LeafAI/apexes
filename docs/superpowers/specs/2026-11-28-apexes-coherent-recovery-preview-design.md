# F04.05 — Independent coherent receipt selection and recovery preview

## Scope

PR #102 publishes a coherent direct Spot checkpoint and current-run
callback-prefix witness at a quiescent stop. This slice independently
retains that acknowledged boundary and previews recovery after execution.
No startup authorization, claim reset, order mutation, lifecycle baseline
initialization, replay or automatic capture is introduced. The existing
one-time startup restriction stays in force.

## Independent selection

`CoherentRecoverySelection` combines the original independently selected
`RecoverySelection` (including owner fingerprint, original initial
parameters and predecessor receipt) with the acknowledged
`CoherentCheckpointReceipt`. Require the same identity and schema,
a strictly later observation ordinal, nonregressing observation time,
valid digest/scope/sequence/count shape and bounded audit size.
No discovery from the current database head; the caller independently
selects the exact file and protects its contents against rollback.

Share the original receipt-file I/O contract: canonical bounded JSON
(64 KiB maximum), strict version/field validation, create-new without
overwrite, Unix mode 0600, file and directory sync, regular-file and
bounded-read checks. Invalid/truncated/oversized/noncanonical files are
refused. Hashes detect inconsistency, not authentication. A write error
is not acknowledgement. Existing pre-execution receipt format and loader
remain unchanged; coherent receipts are not accepted by the old loader.

## Locked durable journal inspection

Under the existing owner lease, `OrderStore::inspect_coherent_recovery`
verifies the independent owner fingerprint and exact inventory/EMS
database URL, original startup claim, monotonic predecessor, selected
checkpoint, coherent boundary and full order/fill journal witness in
one transaction. The journal is locked using the existing nonblocking
lock protocol; no network call is made under those locks. Explicit
rollback preserves the lease on refused inspections.

The preview is deliberately bounded to 2,048 orders and 8,192 fills.
Refuse inconsistent quantity, leverage/margin, symbol/venue/side or
nonpositive price/negative fee evidence, and parse orders and raw fill
rows within the same verified transaction. Preserve original exchange
trade IDs and the existing Spot recovery order (fill timestamp, venue,
symbol, trade-ID length, trade ID, fill ID). Sequence maxima alone are
insufficient; verify the full prior witness. Later row mutation, even
with unchanged maxima, invalidates inspection.

## Recovery preview

`CoherentRecoverySelection::inspect` checks that historical orders
are in the supplied Binance Spot instrument scope, converts the exact
fills, and uses the existing fee-aware owned-position reconstruction.
Caller-provided wallet evidence remains separate from strategy-owned
positions; missing inventory, locked relevant assets or invalid amounts
fail closed. The caller must independently qualify the instrument
configuration and supply the original parameters when constructing two
isolated strategy instances. Validate semantic restore using the
selected checkpoint, then discard the restored candidate. No strategy
market or historical lifecycle callback is invoked by inspection.
Reverify the exact predecessor, owner, claim, boundary and journal
after semantic inspection before returning a point-in-time preview.
The returned cursor, owned positions, supplied wallet and historical
counts are inspection data, not a startup or submission token.

## Qualification

Two new unit tests cover independent file round trip, no overwrite,
legacy-format separation, malformed/oversized/noncanonical files,
original parameter preservation and invalid identity/cursor/prefix
shape. Extend the actual live runner direct-fill coherent-stop test:
after reopening the owner-bound store, persist/read the acknowledged
receipt independently, verify the original claim, preview restored
state with no market callbacks, and reconstruct owned BTC separately
from larger wallet holdings. Wrong owner, predecessor, database,
instrument scope, inventory shortfall, semantic restore failure and
later journal mutation all refuse preview. No claim transition.

Rust 1.99.0, locked dependencies, serial tests: 182 isolated
PostgreSQL-suite tests (91 OMS, 32 EMS, 59 live trading), 733 core
tests and 60 engine library/binary tests passed (975 total), zero
failures, six external core tests ignored. Workspace-wide all-target
Clippy with warnings denied, formatting and diff checks passed.
Disposable PostgreSQL cluster stopped and removed. No real exchange
orders or production database operations. Full workspace build/test,
remote CI, independent review and real-service qualification pending.

## Remaining work

This is database/strategy/owned-inventory inspection, not authoritative
exchange recovery. The wallet is caller-supplied and the historical
terminal execution evidence has not yet been rechecked against the
exchange. Next verify terminal client/exchange order identities,
cumulative execution, exact trade quantities/prices/fees and late
executions, plus open-order/foreign-order checks. Then establish fresh
wallet evidence and full ledger/risk/observation continuity, and a
durable claim transition without replaying completed callbacks.
No general repeatable live restart, periodic capture, unresolved or
EMS execution recovery, or causal no-order history warm-up yet.
