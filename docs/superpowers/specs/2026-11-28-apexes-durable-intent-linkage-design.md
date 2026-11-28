# F04.04 — Durable direct strategy intent/client-order linkage

## Scope

The shared trading loop passes its session-local IntentId to a new
OrderExecutor::submit_correlated hook for direct strategy submissions
reaching the executor. Default implementations delegate to the original
submit method. Event-latency simulation and deterministic replay
identity remain unchanged. Automatic risk exits, flatten, legacy
direct callers and EMS children remain unlinked, never assigned a
fabricated strategy intent. EMS parents/children and fill recovery are
separate follow-ups.

The live OrderManagerExecutor creates a UUID scope per executor
instance (one per live runner invocation); the runner logs the scope
for inspection. A reused executor belongs to the same scope. A fresh
executor does not restore or reuse the scope. The live adapter sends
IntentLink { run_scope, intent_id } to OrderManager::submit_correlated_order
under the existing manager lock. Nil scopes and zero ordinals are
invalid. An absent ordinal retains the legacy submission path. This
is correlation, not an exchange idempotency key or cross-run identity.

## Durable boundary

Add nullable intent_scope UUID and intent_ordinal NUMERIC(20,0) to
orders, with a partial unique index on the pair. Existing orders
remain unlinked. Full unsigned 64-bit ordinals are represented exactly
as Decimal, not floats or signed 64-bit integers. Validate identity
before submitting; decode both columns together and fail closed on
partial/invalid stored values. Application writes both fields or neither.

The existing pre-network begin_submission transaction inserts the client
UUID, Submitting marker, optional reservation and scoped intent together.
The unique index refuses a second row for the same scoped intent; the
transaction rolls back and the gateway is not called. Existing
unknown-ID submission guards, risk checks and health handling remain.
Acknowledgement/status upserts never rewrite the two association
columns. Duplicate attempts are errors, not successful idempotent
retries. The legacy submit and flatten paths still use the same OMS
boundary without intent linkage.

## Inspection and limitations

OrderStore/OrderManager::client_order_for_intent resolves the scoped
pair to the persisted client UUID, including terminal rows.
IdentityEvidence now includes optional intent_link from the same row
as the durable Order and SubmissionState; remote identity lookup
retains its existing semantics. Neither API grants permission to retry.
A restart creates a fresh executor scope; use the original recorded
scope for inspection, not automatic re-submission. Correct account
and store binding remains caller responsibility. No pending-state,
strategy-state or ledger restoration is added. Direct order correlation
does not establish full execution or exactly-once delivery. No
migration tries to infer historical links.

## Verification

New unit test: full u64 ordinal round-trip and refusal of partial,
nil, zero, negative, fractional and out-of-range links.
New PostgreSQL regression: accepted order preserves link, full u64
ordinal, independent scopes, legacy absence, store/manager reopen,
duplicate rejection without an extra mock gateway order.
Extended interrupted-submission regression: scoped link exists while
Submitting before acknowledgement, survives manager reopen and normal
reconciliation, and existing unknown-outcome guard remains effective.
New live executor regression: correlation hook persists exact scope
and ordinal; separate executors get distinct scopes; uncorrelated
legacy submit remains unlinked.

All three database tests executed successfully against a disposable
PostgreSQL 17 instance on a dynamically selected loopback port with
three separate databases and mock gateways. Temporary cluster stopped
and removed. No production account or database used. Existing
service tests remain ignored unless explicitly selected.

Rust 1.99.0 locked offline serial: 770 ordinary tests passed across
OMS, Binance gateway, backtest, paper, strategy, indicators, loader,
and EMS; 60 ignored in that core suite, two of which were executed
separately as above. The additional live adapter database test passed.
All-target Clippy with warnings denied passed for OMS, gateway,
live-trading, engine, EMS, backtest, paper and strategy. Formatting
and diff checks passed. Initial compile errors were fixed. Combined
suite timed out at the existing live-trading flatten test requiring
its default PostgreSQL service; full live suite and workspace CI
remain unqualified locally. Independent review pending.

Next: durable live EMS parent/child ownership and original strategy
intent correlation, followed by in-flight fill evidence and
authoritative terminal callbacks. F04.04 remains in progress.


### CI follow-up

PR #91's first CI run passed lint and Docker but failed the new
correlated-submission restart regression in the shared PostgreSQL
suite. The fixture incorrectly assumed an empty database; the
preceding test left an unresolved order, and the OMS correctly
refused a new submission. Use the existing fresh-manager fixture
for the default shared CI database in both new correlation tests.
An explicitly configured disposable test URL still requires an
empty database and is not truncated. No production logic changed.

Reproduced the CI PostgreSQL step locally against a newly initialized
temporary PostgreSQL 17 cluster: OMS 78, EMS 32, live trading 42,
all 152 tests passed including previously ignored tests. Verified
the local endpoint's data directory before running destructive
test-only fixtures. Temporary cluster stopped and removed.
