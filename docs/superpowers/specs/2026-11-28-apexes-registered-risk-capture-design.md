# F04.05 — Actual registered risk-monitor inspection

## Scope

Extend the explicit observation-capture stop path from PR #110 with
read-only inspection of the actual composite checker used by the unified
loop. Do not change risk decisions, counters, trips, execution or
startup authorization. No new restore API or repeatable restart.

## Capture contract

PreTradeCheck and InTradeMonitor expose an optional read-only
inspect_registered_state hook. Its default is explicit Unsupported,
not an inferred empty or stateless snapshot. LimitChecker and the
circuit-breaker implementations, including both shared adapters,
return the existing validated versioned snapshot types.

CompositeRiskChecker inspects registered pre-trade and in-trade
slots in registration order, and records the number of unsupported
position and market-condition monitor slots. Bound each role to 128
slots; refuse poisoned locks and supported snapshot errors.
An absent composite checker and a registered empty composite remain
distinct. Separate loop-owned market-condition monitoring and
configured margin tiers are marked unqualified rather than silently
omitted. Split circuit adapters are represented in their registered
roles; equality does not prove shared identity or aliasing.

When actual observation capture is requested, the unified loop
captures the registered state after the completed iteration history
and strategy envelope have been finalized and before the existing
coherent checkpoint publication. The capture result is returned only
after acknowledgement, together with the actual observation history.
Ordinary and checkpoint-only callers retain the existing behavior.

## Limits of evidence

Unsupported slots have no serialized configuration/state in this
slice. A partial snapshot cannot prove full risk continuity.
Composite locks protect inspection of registered lists, but do not
prove atomicity with external shared monitor handles or the durable
journal transaction. The evidence is in memory and may be lost
after commit before return. No runtime risk restoration, trip
clearing, callback replay, or recovery authorization is provided.

## Qualification

Two new composite tests exercise actual accepted-order budgets,
shared circuit trips, registration order, explicit unsupported
pre-trade and position slots, and distinguish empty coverage.
Extend the actual coherent-stop PostgreSQL integration to register
a real limit checker and compare its returned snapshot to the same
registered instance after one accepted order.

Rust 1.99.0, locked dependencies and serial tests: 206 OMS/EMS/live
tests (97/32/77) passed with disposable PostgreSQL; 812 core/risk
tests (including all 72 risk-engine tests) and 60 engine tests passed
(1,078 total), zero failures, six external tests ignored. Workspace
all-target Clippy with warnings denied, formatting and diff checks
passed. Disposable database stopped and removed. Full workspace
build/test, independent review, remote CI and real-service
qualification pending. No real orders or production DB operations.

## Remaining work

Next qualify atomic durable observation/risk evidence with explicit
coverage limitations. Complete monitor coverage, aliasing/atomicity,
full risk and ledger restoration, source completeness and authoritative
evidence collection remain unqualified. Maintain the one-time startup
restriction; no repeatable post-execution restart is enabled.
