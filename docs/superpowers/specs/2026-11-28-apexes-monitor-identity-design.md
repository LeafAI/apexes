# F04.05 — Shared built-in monitor identity and simultaneous inspection

## Scope

Strengthen read-only registered risk evidence without changing any
risk decision, execution logic, strategy restore, or startup permission.

## Identity and capture contract

Built-in split circuit-breaker, margin and market-condition adapters
expose opaque references to their actual shared mutexes. Resolve
identity only in memory using typed live mutex identity, not equality
of snapshot values. No addresses are serialized. Canonical local alias
numbers follow first encounter in pre-trade, in-trade, position and
market-condition registration order. Independent equal-state monitors
retain different numbers; repeated handles share one number.

The composite holds its registration locks throughout inspection.
Nonshared slots are inspected under the registration locks. Acquire
all distinct supported shared monitor locks with try-lock before
inspecting their state; retain all guards through the shared inspection
pass, capture each distinct instance once and distribute the captured
state to its alias slots. Busy or poisoned locks refuse capture and
release already acquired guards. This does not introduce waiting on
shared external handles or change normal runtime risk checks.

The optional alias DTO is validated for role lengths, counts,
first-encounter numbering, supported shared monitor types and identical
evidence within each group. Margin and market adapters require alias
identity when alias evidence is present. A direct nonshared circuit
checker may have no alias. Legacy receipts omit this field unchanged;
reading legacy evidence does not qualify identity retroactively.
Custom monitor hooks remain unsupported by default. The separately
constructed loop-owned market monitor remains separate from composite
registration. No topology is inferred from equal state.

## Limits

The simultaneous lock guarantee applies to the distinct actual
built-in shared monitors within this composite inspection. It is not
atomic capture of independently mutable custom monitors, all runtime
state, ledger state, or the subsequent database commit. Local alias
numbers do not identify instances across different processes; they
describe topology only. Persisted evidence is not restart authority
and does not prove source completeness or external provenance.

## Qualification

Two new tests verify equal-state independent monitors with reversed
position registration, shared IDs, canonical numbering, role lengths,
required margin alias identity and lock contention with guard release.
The coherent-stop PostgreSQL integration verifies actual shared margin
adapter alias topology and reopens the exact durable attachment.

Rust 1.99.0, locked dependencies, serial tests: 207 OMS/EMS/live tests
(97/32/78), 819 core/risk tests (77 risk), and 60 engine tests passed,
1,086 total, zero failures, six external tests ignored. Workspace
all-target check and Clippy with warnings denied, formatting and
diff checks passed. Disposable PostgreSQL stopped and removed.
Full workspace build/test, independent review, remote CI and
real-service qualification remain pending. No real orders or
production database operations.

## Remaining work

Next qualify offline risk/ledger restoration contracts, including
explicit topology and coverage validation. Full runtime/ledger
atomicity, custom monitor state, independently verified source
completeness and authoritative acquisition remain open. Preserve
one-time startup restriction; do not enable runnable restart,
callback replay or periodic checkpoint publication.
