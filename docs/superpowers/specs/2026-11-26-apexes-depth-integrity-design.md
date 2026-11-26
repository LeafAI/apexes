# F03.02 — Depth integrity and freshness

Add an independent optional simulated depth policy: maximum event-time age
and either snapshot-only or contiguous sequence semantics. Absence preserves
the legacy replay benchmark. Binance update IDs can span ranges; the
DepthUpdate schema contains only a single final sequence. Do not infer
continuity for that feed: strict contiguous mode requires a source whose
sequence advances exactly one per delta. Snapshot-only mode accepts full
snapshots and invalidates on deltas. Neither mode promises loss detection
for arbitrary range-based feeds. Full snapshots rebase sequence.

Strict books require a valid snapshot before accepting deltas. An explicit
empty unsequenced invalidation, invalid level, missing/gapped sequence,
or unexpected delta invalidates the book; retain the invalidation timestamp
and require a nonolder fresh snapshot. Duplicate/older messages do not
replenish consumed liquidity. Validate positive prices, nonnegative
quantities, no duplicate prices per side and uncrossed reconstructed depth.
One-sided sequenced snapshots are valid (no liquidity on the missing side).
A stale book cannot fill or seed queue position; a delta cannot restore a
stale snapshot chain. Fresh snapshot is required. Strict book validity is
checked per venue/symbol using event time, not wall clock.

Reject new market/limit/stop submissions without fresh valid depth. On
trade events with invalid depth, discard existing simulated resting and
trigger orders for that venue/symbol rather than invent queue continuity;
on explicit invalidation or discontinuity discard them immediately.
An accepted snapshot rebase after an observed stale interval also discards
existing queue state. Adapter prunes its working view when depth changes.
No fills manufactured on invalid data. This conservative cancellation
policy is a simulation assumption, not real exchange cancellation.

Expose policy in launch JSON and programmatic registry, reject it for live
launch, record it in reports. Test delta-before-snapshot, duplicate,
sequence gap, invalidation/recovery, stale clock, crossed book, venue
isolation, queue cancellation and policy serialization. No live safety
changes. Causal arrivals and queue calibration remain separate.
