# F04.05 — Offline risk candidate compatibility

## Scope

Qualify the read-only compatibility prerequisite for offline risk-state
restoration. Compare independently configured, pristine candidates with
retained evidence. Never apply retained counters, trips, or volatility
state to a checker, return a runnable recovered monitor, or authorize a
new execution session.

## Contract

Validate both retained and candidate registered risk evidence.
Require supported coverage and explicit alias topology on both sides,
exact role/count/order and canonical alias equality. Match the actual
configuration of each supported slot: limit and circuit parameters,
margin threshold, list mode and membership, stateless checker version,
and market-condition configuration. Require unused candidate daily
counters, no trips, empty price windows and no account volatility samples
or trip fractions. Do not require the retained state to be pristine.

The durable observation capture comparison also validates its own
receipt, independent instrument scope, absent/present registered and
separate market monitors, legacy unsupported coverage flags, and exact
ordered configured margin tables. It refuses a missing expected monitor,
an unexpected monitor, or different tables. The caller supplies the
independently configured evidence; this API does not independently
authenticate its provenance or prove candidate ownership. The caller
must select and verify the durable receipt before using the comparison.

No monitor or ledger state is changed, and no runtime call path is added.
A successful comparison is only evidence of compatibility, not state
application, complete historical coverage, runtime atomicity, or
authorization.

## Qualification

New risk test checks retained nonempty budgets/trips versus a pristine
independent candidate, mismatch of configuration and alias topology,
legacy and unsupported coverage, non-pristine rejection and unchanged
instances. Extend the existing isolated PostgreSQL coherent-stop fixture
to independently construct a candidate, compare the reopened capture,
and reject the active checker, missing separate monitor/table and wrong
instrument scope.

Rust 1.99.0, locked dependencies, serial tests: 207 OMS/EMS/live tests
(97/32/78), 820 core/risk (78 risk), and 60 engine tests passed,
1,087 total, zero failures, six external ignored. Workspace all-target
Clippy with warnings denied, formatting and diff checks passed.
Disposable PostgreSQL stopped and removed. Full workspace build/test,
independent review, remote CI and real-service qualification pending.
No real orders or production database operations.

## Remaining work

Next qualify offline ledger-state continuity and restoration contracts.
Actual risk-state application, custom state, full runtime atomicity,
source completeness and authoritative evidence remain unqualified.
Do not enable repeatable startup or callback replay.
