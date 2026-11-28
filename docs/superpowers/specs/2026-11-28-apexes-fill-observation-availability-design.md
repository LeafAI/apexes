# F04.05 — Offline fill observation-prefix comparison

## Scope

Read-only comparison of retained per-fill valuation decisions with
independently supplied observation-prefix evidence. No runtime
accounting changes, callback replay, state application, instrument
filtering, market acquisition or recovery authorization.

## Contract

First qualify the selected exact journal, independently declared
instrument roles, durable capture and independently retained completed
observation receipt, including final retained price windows.
Require versioned evidence bound to the exact selected coherent receipt
and owner. Each retained fill decision has one prefix witness in
attribution order, specifying the number of market observations
processed through price maintenance before that decision. The prefix
may include the market iteration in progress when a fill was applied.
Do not infer prefix counts from exchange execution timestamps.

Require complete unique fill identity coverage, bounded records and
nondecreasing prefix counts no greater than the completed observation
count. Compare fill identity, symbol, venue, execution timestamp,
price and fee quantity against the exact selected journal. Reconstruct
only the declared prefix using positive trade observations, last
replacement at equal source time, and the existing 2,048 timestamp
window. For third-asset fees, compare the latest observation at or
before the fill source timestamp and within five minutes; refuse
incorrect source and value, including a claimed missing valuation
when a suitable observation exists. Compare zero, base and quote
fee decisions directly using checked exact arithmetic. Refuse
caller-provided unqualified valuation sources.

The comparison is conditional on the supplied prefix witness. It does
not prove actual callback interleaving, independently authenticate the
witness, or qualify external funding ownership.

## Qualification

Two offline tests verify late same-timestamp replacement, exclusion
of future source timestamps, missing conversion, unknown fill identity,
incomplete witnesses, regressing prefixes, duplicate journal IDs,
owner mismatch and caller-supplied valuation refusal. The isolated
coherent-stop integration checks a separately supplied one-observation
prefix for the reopened fee-free fixture; its receipt and witness are
fixture evidence, not independently collected production evidence.

Rust 1.99.0, locked dependencies, serial tests: 224 OMS/EMS/live tests
(97/32/95) with disposable PostgreSQL, 841 core/risk/research (17
research and 78 risk), and 60 engine tests passed: 1,125 total,
zero failures, six external tests ignored. Workspace all-target Clippy
with warnings denied, formatting and diff checks passed. Disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain pending.

## Remaining work

Next qualify actual retained observation-prefix evidence in an offline
diagnostic context. The supplied witness is not proof of actual
market/fill interleaving. Authoritative market and funding provenance,
full nonzero external-fee checkpoint qualification, complete runtime
continuity and state application remain open. Preserve the existing
owned-position gate and one-time startup restrictions.
