# F04.05 — Market-condition and margin-tier evidence

## Scope

Extend read-only durable capture to the remaining built-in market-condition
monitor and actual margin-tier configuration. Preserve risk decisions,
execution, startup restrictions, and checkpoint-only behavior.

## Evidence

Capture the actual market monitor configuration, sorted symbol records,
last prices, rolling standard-deviation window and accumulators, symbol trip
fractions, optional equity volatility and account-wide trip fraction.
Use the existing bounded native indicator checkpoint representation and
validator (isolated temporary indicator only); no runnable risk monitor is
restored. Require positive bounded indicator samples, configured period
1–512, at most 128 symbols, canonical ordering, consistent price tail and
account configuration, and valid fractions. Unqualified or invalid state
refuses capture; it does not alter runtime checks. Zero or negative equity
samples are outside the existing positive-sample indicator contract.

Inspect the two registered market monitor adapters and loop-owned read
handle. Preserve optional ordered market slots; legacy nonzero count
without slots remains unsupported. Custom adapters still default to
Unsupported. Validate role and count consistency. Separately capture
configured margin tables sorted by symbol, with bounded counts and
increasing nonnegative notional floors, leverage >= 1, rates in [0,1],
and nonnegative deductions. These are actual configured tables, not
evidence of exchange provenance or complete margin accounting.

The opt-in loop captures this evidence before the existing durable
attachment publication. Optional fields are absent from legacy encodings;
previous unqualified flags remain interpretable and are not upgraded by
reading an old receipt. The existing exact digest and transaction bind
the new evidence. No risk decisions, trips, automatic execution policy,
or startup authorization are changed.

## Qualification

New unit tests exercise real retained price/equity windows, trip fractions,
shared adapter observations, inconsistent last-price refusal, and canonical
margin-table round-trip/duplicate-floor refusal. Extend the actual coherent
stop and reopening PostgreSQL integration with a configured market monitor
and margin table; compare exact durable attachment.

Rust 1.99.0, locked dependencies, serial tests: 207 OMS/EMS/live tests
(97/32/78), 817 core/risk (75 risk), and 60 engine tests passed, 1,084 total,
zero failures, six external tests ignored. Workspace all-target check and
Clippy with warnings denied, formatting and diff checks passed. Disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain pending. No real
orders or production DB operations.

## Remaining work

Shared monitor identity and atomic inspection across external handles,
custom monitor coverage, full ledger/risk restoration, gap-free source
qualification and authoritative evidence acquisition remain open. The
capture is not proof of the complete runtime policy, exchange state or
external provenance. No runnable risk restoration, repeatable startup,
callback replay or periodic checkpoint publication is authorized.
