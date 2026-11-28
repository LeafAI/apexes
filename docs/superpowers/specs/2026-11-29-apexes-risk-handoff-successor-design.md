# F04.05 — Opening risk handoff-bound successor qualification

## Contract

Select the actual pre-event opening risk handoff and its independently
retained capture-time digest, along with the two independently selected
receipt-bound transition witnesses and all existing independent evidence.

Reinspect the handoff against the successor's exact original checkpoint,
owner, run scope and acknowledged durable opening risk snapshot. Derive
opening risk evidence only from that validated handoff. Execute the
existing complete risk-bound successor qualification unchanged, including
exact journal, accounting, observation, risk state, chronology and
cold flat-boundary linkage checks.

Bind the complete risk-bound successor qualification receipt and
selected opening handoff digest into a distinct versioned receipt with
a separate checksum domain. Preserve the earlier receipt format;
a detached-snapshot receipt cannot substitute for the stronger type.
Use bounded canonical create-new persistence and independently selected
receipt digest. Reopen and rerun qualification before comparing.

## Limits

This is historical equality evidence, not proof of independent
production acquisition or durable external retention. The trusted
caller remains responsible for independently retaining the capture-time
digest, original handoff and all other inputs. No live state carryover,
continuous monitoring, funding ownership or repeatable startup
authorization is inferred. Existing claims and watermarks are unchanged.

## Verification

Extend the existing two-store PostgreSQL fixture to qualify the
capture-time successor handoff, persist and reopen the stronger
receipt, rerun and compare the exact result, and refuse wrong digests,
prior-run handoff substitution, modified handoff binding, overwrite,
and interchange with the earlier receipt format. Both acknowledged
boundaries and startup claims remain intact. Exchange I/O is mocked.

Rust 1.99.0, locked dependencies: 240 OMS/EMS/live tests, 893
core/risk/research/engine tests and 27 Binance wiremock REST tests
passed (1,160 total, zero failures, four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Qualify an independently retained closing risk snapshot handoff from the
actual acknowledged closing boundary, rather than relying only on a
retrospectively selected database snapshot. Preserve capture-time
chronology and the exact run scope, with no live restoration or
repeatable claim authority.
