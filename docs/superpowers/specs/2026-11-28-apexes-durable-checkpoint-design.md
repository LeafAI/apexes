# F04.05 — Durable checkpoint publication and recovery selection

## Storage scope

Use the existing dedicated managed Spot PostgreSQL inventory schema and
its exclusive owner lease, rather than a second connection pool or a
filesystem checkpoint racing the OMS. Add a single bounded
`strategy_checkpoint_head` row containing owner, canonical receipt and
opaque canonical envelope bytes. The table is additive; existing schemas
are empty until explicitly published. An unbound inventory cannot
silently adopt orphaned checkpoint data. No production configuration or
runner behavior changes in this slice.

All checkpoint operations use the same dedicated lease connection,
verify the bound owner within their transaction, and never reconnect
that lease silently. Row locks serialize head access. A plain initial
insert refuses a racing insert instead of overwriting it. Every
successful publication commits with local synchronous_commit enabled;
durability assumes correctly configured durable PostgreSQL storage.
Explicit rollback on failed validation or write releases locks on the
long-lived lease connection immediately. A database error may represent
an ambiguous commit response; no success receipt is inferred.

## Receipt and publication contract

Factor canonical envelope validation into a strategy-independent API
shared with existing restore. A CheckpointReceipt identifies the exact
identity, observation cursor and SHA-256 of the full envelope bytes.
The storage receipt does not assert semantic payload validity or callback
execution. Store the receipt as bounded canonical JSON to preserve full
u64 ordinals and nanosecond observation timestamps, rather than losing
precision through PostgreSQL timestamp conversion.

Publish requires an independently established identity/cursor and the
expected exact predecessor receipt (None explicitly means initial
publication). Validate the proposed envelope before any database write.
The existing head must validate against its own receipt and bound owner,
then match the expected predecessor. Identity cannot change across a
head transition, observation ordinal strictly increases, and observation
time cannot regress. Equal observation times with increasing ordinals
are permitted. An exact retry with the same valid candidate bytes and
receipt is a no-op success if that candidate is already the current head;
a stale retry never rolls the head backwards. Return a receipt only
after successful transaction commit.

## Recovery selection

Load requires the independently selected exact expected receipt. None
requires an empty store, not the latest checkpoint. If a head is missing,
unexpected, corrupted, owned by a different owner, or differs from the
expected receipt, fail closed. Never fall back to another checkpoint or
return unverified bytes. A coherent old receipt/envelope replacement
does not satisfy a newer independently trusted receipt. No historical
checkpoint archive or automatic identity migration is introduced.

Restore still runs on an isolated candidate using the existing strategy
contract. The caller must establish and retain the trusted recovery
receipt outside this head; deriving the expected receipt from the head
would defeat rollback detection. The store does not authenticate data,
prove semantic strategy state validity, or make checkpoint publication
atomic with OMS orders, fills, or callbacks. Protect storage and receipt
metadata. A failed or cancelled operation must not permit trading to
resume on inferred state; integration must handle uncertain commit
results and reopen after lost lease/connection.

## Qualification

Use independent schemas in a disposable real PostgreSQL 17 cluster.
Four regression tests cover acknowledged publication across reopening,
exact idempotent retry, full ordinal and nanosecond precision, actual
native strategy restore, empty/missing recovery, stale predecessor,
same-cursor conflicts, regressing time, identity changes, invalid
envelopes, a deferred trigger failure at transaction commit, unchanged
prior head after the failure, corrupted stored envelope rejection,
exclusive owner binding and orphan adoption refusal. The initial
qualification exposed deferred transaction rollback retaining locks on
the lease connection; explicit rollback resolved this. The interrupted
test cluster and final test cluster were verified isolated and removed.

Rust 1.99.0 and locked dependencies: all 168 PostgreSQL-backed tests
(91 OMS, 32 EMS, 45 live trading) passed; 733 core tests passed with zero
failures and six external ignored tests. Workspace-wide all-target Clippy
with warnings denied, formatting and diff checks passed. Full workspace
build/test, remote CI, independent review and real-service qualification
remain pending. No production database or real orders were used.

## Remaining work

Verified runner integration, an independently durable recovery receipt
source, atomic strategy state and callback recovery, and causal no-order
history warm-up are separate slices. The head is managed-live storage,
not an automatic checkpoint facility for paper or historical runners.
General interpreter globals and arbitrary script side effects remain
outside the qualified scoped-only contract.
