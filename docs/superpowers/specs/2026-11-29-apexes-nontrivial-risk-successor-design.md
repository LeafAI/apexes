# F04.05 — Nontrivial registered risk-state successor qualification

## Contract and coverage

Extend the historical two-store PostgreSQL qualification fixture to
exercise actual registered risk state rather than only explicit absence
of risk checkers. Construct independent built-in checkers with identical
configuration and shared adapter topology. Seed test-only historical
state through actual callbacks: a nonzero daily order count and volume,
an account circuit breaker trip, and three retained market volatility
samples. Do not mutate checkpoint DTOs, restore live risk state or
insert fictional execution records into the durable journal.

Capture and retain opening and closing risk handoffs using the existing
explicit managed runner. Independently qualify each historical transition
against a freshly configured pristine candidate, then require exact
prior closing and successor opening risk equality in the full dual
handoff successor inspection. The matching nonzero state qualifies;
an independently valid successor with pristine opening state refuses
with RiskState. Verify both original claims and coherent boundaries.

## Limits

The test-only callback seeding is not independent production evidence
of earlier executions or a live restoration mechanism. The journal
for each selected run remains flat with no executions. The registered
composite market monitor currently does not receive market events from
the unified loop's separate market-condition path. Consequently the
three seeded samples test retention and equality, not live progression
of that registered monitor. Separate market monitor event coverage,
actual runtime risk carryover, funding provenance and repeatable
startup authority remain gated. Exchange I/O is mocked.

## Verification

Rust 1.99.0, locked dependencies: 243 OMS/EMS/live tests, 893
core/risk/research/engine tests and 27 Binance wiremock REST tests
passed (1,163 total, zero failures, four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Qualify registered market-condition event dispatch in the unified loop,
with explicit shared-monitor topology, no duplicate delivery and no
change to existing liquidation or submission authority. Test actual
progression of the retained registered state before extending
historical risk qualification. Preserve existing startup claims.
