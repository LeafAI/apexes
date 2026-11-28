# F04.04 — Durable live EMS parent and child linkage

The shared strategy loop passes the live executor's existing run scope
and intent ordinal into Ems::submit_correlated_algo_order. Default
implementations call the existing uncorrelated method. PostgresEms
persists the scope and exact unsigned ordinal in nullable parent
columns, with a unique pair index and a pre-reservation duplicate
check. Parent admission and scheduling remain unchanged. The unique
index protects concurrent parent inserts; a racing failure after
capital reservation can still leave an orphan lock requiring review.
This index is per parent table, not a cross-route direct/parent
idempotency guarantee. Legacy parents remain unlinked.

For each scheduled child, generate a UUID before committing the
existing dispatch_pending marker, and store pending_dispatch_id in
that update. Pass it and the actual parent UUID through the new
ChildOrderSink::submit_dispatched_child hook. Existing custom sinks
default to the previous allocated-child method; the live OMS sink
explicitly implements this hook.

The OMS persists ChildLink { parent_id, dispatch_id } in its existing
pre-network transaction. Separate nullable ems_parent_id and
ems_dispatch_id fields are not aliases for parent_allocation_id.
Managed capital consumption and reservation rules remain unchanged.
A unique dispatch index refuses a second OMS row for the same
dispatch. Acknowledgement/status writes do not overwrite linkage.
The OMS does not require all callers to be EMS children, and direct
legacy orders retain no child link. A child has its own order ID,
not a duplicate of the strategy parent intent ordinal.

The EMS records the returned child ID and dispatch UUID together
with submitted quantity; the transaction clears the pending marker
only for the matching dispatch. If the child-link transaction fails,
the pending identity remains and the existing scheduling health
latch prevents recovery from silently resubmitting. The pending
marker does not prove whether a child reached OMS or the exchange.
Existing unresolved legacy markers remain unresolved without IDs.
No automatic repair, replay, release of capital or restart is added.
The EMS filled_quantity field remains the submitted/scheduled amount,
not authoritative execution quantity.

Read-only inspection: PostgresEms::parent_for_intent and
pending_dispatch, OrderManager/OrderStore::client_order_for_dispatch,
and IdentityEvidence.child_link. The first two are available on a
constructed EMS; if startup refuses an unresolved marker, inspection
requires read-only database access. Pending lookup returns None
for missing and legacy null identities, not proof of no dispatch.
Completed child association carries dispatch_id for audit. Custom
sinks that do not persist ChildLink cannot claim OMS evidence.

Verification: extended managed POV integration checks exact u64
parent intent, duplicate parent refusal, matching OMS child evidence
and EMS child dispatch ID, no pending marker after completion of
the dispatch, and parent lookup after EMS recreation. Existing
injected child-link write failure test verifies retained nonnil
pending dispatch identity, zero submitted quantity and refusal to
restart/resubmit. The latter uses a recording sink, not the live OMS
sink; end-to-end interrupted OMS/EMS link reconciliation remains
unqualified. Existing OMS and live tests preserve legacy paths.
All 152 OMS/EMS/live-trading tests including PostgreSQL tests passed
serially on a disposable PostgreSQL 17 cluster (78/32/42), verified
the endpoint's data directory and removed the cluster afterwards.
714 nonignored backtest/strategy/indicator/loader/paper/gateway
tests passed; 6 external tests ignored. All-target lint and fmt
checks passed. No real exchange order or production DB operation.
Workspace CI and independent review pending.

Next: qualify interrupted dispatch reconciliation, then persist live
fill attribution and notification progress. No automatic restart or
exactly-once delivery claim.
