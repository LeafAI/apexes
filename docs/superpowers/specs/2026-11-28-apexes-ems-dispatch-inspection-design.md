# F04.04 — Interrupted EMS dispatch inspection

An interrupted child dispatch prevents EMS construction. Expose
`apexes_ems::postgres::inspect_pending_dispatch(&PgPool, ParentOrderId)`
independently of a running scheduler. Caller supplies an already migrated,
correctly account/schema-bound pool. No migration, state changes, worker
startup, reservation release or gateway submission are performed.

Use one repeatable-read, read-only PostgreSQL transaction and a local
statement timeout. Read the parent marker, search the entire bound order
table by dispatch UUID (not by parent), and check the completed EMS
association by dispatch or child ID. Distinguish ParentMissing, NotPending,
LegacyPending, MissingOms, Conflict and Linked. A stale dispatch UUID without
a pending marker is a conflict. Legacy pending markers without an identity
are never inferred from symbol/time. A valid pending marker must refer to a
scheduling parent with remaining positive quantity. Reject nil identities,
duplicate dispatch records, already recorded child association, wrong owner,
inconsistent symbol/venue/side, non-Spot child, invalid or excessive quantity,
direct intent linkage on a child and unknown submission state. The result
preserves the exact order UUID, parent/dispatch identity and local submission
state. Missing order evidence does not establish nonexecution. Existing
pending markers do not persist exact intended slice quantity, so remaining
parent quantity is a bound, not proof of exact matching. Legacy schema must
be migrated separately. This is local evidence, not account attestation,
authoritative venue status, or repair authorization.

The caller may use the existing OMS identity inspection for the exact client
UUID and verify the returned child link. Remote evidence is a separate
observation outside the database snapshot. No negative exchange lookup or
local absence grants retry permission. No automatic repair or scheduling
restart; the existing refusal remains intact.

Verification: extended existing legacy pending test to check LegacyPending,
MissingOms and nil dispatch Conflict. Extended link write failure test to
use actual OrderManagerChildOrderSink and MockExecutionGateway, inject failure
only in the EMS child-link write after OMS accepts the child, confirm pending
marker and unchanged submitted quantity, exact durable OMS link and positive
gateway lookup, then reopen OMS and prove EMS startup still refuses and the
same evidence remains with exactly one mock exchange order. Deliberately
conflicting owner evidence fails closed. Also check missing parent and
not-pending states. All 152 OMS/EMS/live-trading tests (78/32/42), including
the entire database-backed CI step, passed twice on disposable PostgreSQL
17 clusters; final rerun includes reopened OMS. No real exchange trades or
production database operations. All-target Clippy for eight affected crates,
fmt and diff checks passed. Workspace CI and independent review pending.
No changes to historical replay or submission behavior.

Next: durable fill attribution and notification progress, then authoritative
terminal delivery and full recovery qualification.
