# F04.05 — Receipt-bound successor continuity

## Contract

Select two independently preserved historical runs with exact checkpoint
selections, opening and closing collection receipts, independent tracked
allocation, independent non-wallet transition evidence, and separately
retained receipt-bound witness digests.

Require exact prior closing checkpoint equality to the successor
predecessor, equal owner and initial parameters, distinct run scopes,
advancing observation cursor, identical instruments, conversion roles,
pristine registered risk and market monitor configuration and margin
tiers. Independently requalify both receipt-bound transition witnesses
using the existing full isolated accounting, observation and risk
qualification. Use the validated historical evidence from those same
qualifications for cross-run checks, not caller-supplied wallets.

Require the complete next opening collection to start no earlier than
the prior closing inspection, with exact account-wide wallet equality
including explicit zero and untracked rows. Refuse overlapping client,
exchange order or exchange trade identities. Require independently
qualified flat closing positions, flat retained ledger and exact tracked
asset balance continuity into the successor allocation. Verify that the
first successor completed market observation follows the entire opening
collection and re-read both durable captures.

## Limits

This qualifies cold flat historical boundaries only. Independent
collection and protected selection are still required; equality at
endpoints does not establish absence of intervening activity, funding
ownership or risk state continuity. No live state, replay, startup
authorization, claim reset or execution watermark change. The fixture
uses two independent disposable PostgreSQL schemas, not repeatable
startup in one live store.

## Verification

The integration fixture collects separate opening and closing receipts
for two chronological flat runs, copying the acknowledged predecessor
envelope into a separate schema. It requalifies both witnesses,
rejects mismatched or substituted witness digests, and rejects a
separately valid but late prior closing collection that overlaps the
successor opening. Both claims and durable boundaries remain intact.

Rust 1.99.0, locked dependencies: 238 OMS/EMS/live tests, 893
core/risk/research/engine tests and 27 Binance wiremock REST tests
passed (1,158 total, zero failures, four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Qualify exact cross-boundary risk state continuity against the successor
opening configuration/state, including explicit refusal when opening
state or chronological risk evidence is unavailable. Do not infer reset
permissions or weaken the one-time claim or owned-inventory gates.
