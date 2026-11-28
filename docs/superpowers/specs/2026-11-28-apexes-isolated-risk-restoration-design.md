# F04.05 — Isolated built-in risk state restoration

## Contract

The offline restoration qualifier first checks the existing exact pristine
configuration, registration order, slot coverage and alias topology against
independently supplied candidate evidence. Legacy alias-less captures and
unsupported/custom slots are refused. It creates only private built-in
checker instances and never mutates a supplied candidate or live checker.

Restore exact accepted-order daily counters without resetting or clamping
them, circuit-breaker symbol/account trips, margin trips, allow/deny lists,
stateless self-trade prevention, market/equity volatility windows, last
prices and retained de-risk fractions. Every canonical shared alias
identifies one newly allocated monitor, reused by its actual adapters.
Separate identical instances remain separate. Reinspect the complete
registered topology and compare all retained state exactly before discarding
the reconstructed checker. The separate market monitor is qualified in the
same isolated manner. Invalid state fails closed before successful
qualification; no mutable handle is returned.

The captured-observation API retains the prior independent scope,
configuration, unsupported-monitor and margin-tier checks. Margin tier
tables are compared, not installed into a live ledger. Durable capture
selection remains the caller's obligation, and matching reconstruction
cannot establish authoritative provenance, freshness or cross-system
atomicity.

## Verification

Tests exercise nonzero daily counters above a tightened cap, latched trips,
all registered roles, rolling indicator and equity accumulators, shared
adapter state propagation on a subsequent event and refusal of changed
topology, legacy/unsupported and nonpristine evidence. The coherent-stop
fixture qualifies a reopened capture and independently configured candidate.
Rust 1.99.0, locked dependencies: 228 OMS/EMS/live tests (including
isolated PostgreSQL suites) and 893 core/risk/research/engine tests passed,
for 1,121 total, zero failures and four external tests ignored. Strict
workspace Clippy, formatting and diff checks passed. The disposable
PostgreSQL cluster was stopped and removed. Full workspace build/test,
independent review, remote CI and real-service qualification remain open.

## Remaining scope

No live risk state restoration, no submission authority and no change to
the existing one-time startup claim. Next qualify isolated ledger state
application against exact selected journal and independently retained
accounting evidence. Authoritative collection, risk/ledger/strategy
atomicity, post-execution restart, in-loop periodic publication and EMS
callback recovery remain open.
