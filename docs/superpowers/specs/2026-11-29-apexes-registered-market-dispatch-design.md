# F04.05 — Registered market-condition event dispatch

## Contract

Dispatch each fresh event admitted by the unified loop to the registered
composite market-condition event monitors before strategy evaluation.
Aggregate and log their violations; preserve existing pre-trade checks,
liquidation policy and the separately configured loop-local monitor.
Do not use the registered event callback as an order or liquidation
authorization.

Within the composite, invoke each actual shared monitor once per event.
Deduplicate using the existing ephemeral shared-handle identity, not
configuration equality or a serialized alias number. Distinct actual
monitors with identical configuration remain distinct. Independently
registered custom nonshared slots each receive the event. The loop-local
monitor is constructed separately from configuration and cannot share
the registered handle. No durable receipt or alias wire format changes.

## Verification

A component test registers two event adapters referencing the same
built-in monitor, plus an independently constructed monitor with the
same configuration. Both distinct monitors receive exactly one sample;
their pre-trade slots see the same state as their event slots.

The historical two-store PostgreSQL successor test now seeds only two
prior samples for the first run, checks that the actual loop advances
that state to three samples and a new last price, and verifies the
separately constructed successor's opening equality and subsequent
progression. A pristine-reset successor receives its own first sample
but still refuses cross-boundary qualification.

## Limits

Only market-event dispatch is covered here. The existing loop-local
account equity and de-risking path remains separate. Custom monitors
cannot establish inspectable shared identity unless they expose the
supported built-in handle; unsupported custom recovery state remains
unqualified. Historical checker initialization is still test-only;
no production restoration or repeatable startup authority is introduced.

## Verification results

Rust 1.99.0, locked dependencies: 243 OMS/EMS/live tests, 894
core/risk/research/engine tests and 27 Binance wiremock REST tests
passed (1,164 total, zero failures, four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Qualify the registered market monitor's account-equity observation
coverage and exact event/equity ordering against independently retained
state, retaining the current de-risking authority boundary. Do not
infer exchange completeness, funding provenance or startup permission.
