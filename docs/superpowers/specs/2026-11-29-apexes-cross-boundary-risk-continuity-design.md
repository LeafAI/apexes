# F04.05 — Cross-boundary risk state continuity

## Contract

Record the actual registered risk and separately configured market monitor
snapshots at entry to the managed unified event loop, before consuming
any market or supplemental event. Preserve explicit absence of both
registrations. The timestamp is local consumer UTC, not exchange source
time. Commit the optional opening snapshot with the existing canonical
durable observation capture and coherent checkpoint. Legacy capture
bytes remain canonical and readable; missing opening evidence cannot
qualify risk continuity.

The stricter receipt-bound successor inspection additionally requires
independently selected opening risk evidence. First requalify both full
receipt-bound transitions and all existing successor checks. Then compare
the actual next opening snapshot to the independent selection, including
its timestamp; compare the complete registered risk state and market
monitor to the prior closing capture, including counters, latches,
indicator windows, alias topology and configuration. Margin tier tables
must remain identical. The prior closing inspection precedes the next
opening wallet collection; the actual pre-event risk capture timestamp
must lie between completion of that opening collection and the start
of the next closing terminal collection.

## Limits

No inference from pristine configuration or automatic reset permission.
The independent snapshot must be obtained and retained through a trusted
process, and exact comparison is not proof of continuous monitoring or
unseen intervening activity. Consumer wall-clock time is distinct from
source event availability time; the loop provides the local pre-event
ordering, not proof of exact exchange chronology. The new stricter API
returns only a historical qualification, not risk carryover, restored
live state or permission to restart. Existing historical successor
inspection remains available with its original weaker scope. No startup
claim, watermark or owned-position recovery gate is changed.

## Verification

Extend the nonempty managed callback fixture to verify actual pristine
opening risk versus nonzero closing counters and separate monitor
history. Extend the two-store flat receipt-bound successor test to
qualify explicit no-risk registration and reject a changed independent
opening timestamp. Component tests refuse missing legacy snapshots,
a reset of nonzero daily counters and incompatible observation timing.

Rust 1.99.0, locked dependencies: 239 OMS/EMS/live tests, 893
core/risk/research/engine tests and 27 Binance wiremock REST tests
passed (1,159 total, zero failures, four external ignored). Strict
workspace Clippy, formatting and diff checks passed; disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain outstanding.

## Next

Qualify durable, independently selected risk-bound successor evidence
for the exact pair of receipt-bound transition witnesses and opening
snapshot; retain explicit scope and chronology without granting startup
authority. Actual live carryover and authoritative provenance remain
gated.
