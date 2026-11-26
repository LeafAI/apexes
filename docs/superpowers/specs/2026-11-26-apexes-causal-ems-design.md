# F03.02 — Causal EMS scheduling

Enable event-time EMS when the existing event_latency policy is set.
Queue parent intent through the same admission delay/recheck as a direct
intent. InMemoryEms anchors admission to its latest accepted event,
not the host clock; before the first event, admission fails. Dispatch
starts on subsequent observed events. TWAP advances at most one slice
per parent per event and spaces from the actual submission time (no
catch-up bursts); VWAP advances cumulative targets scaled to the
requested duration; POV uses only positive matching-venue trades.
Ignore regressing event timestamps, use close time for bars, process
parents in admission order within each fixed algorithm class.
Pass the event timestamp explicitly to the simulated child sink,
including on bars, rather than fallback to wall clock.
Validate positive quantities and algorithm parameters. In causal mode
VWAP must use an as-of provider (no default fallback to current history);
query only bars from UTC dates before admission, reject incomplete or
invalid curves. Require fixed routing in causal mode, avoiding ambiguity
in existing cross-asset routing data. Legacy mode and PostgresEms unchanged.

This increment models latency on parent admission, not a separate network
delay for every child. Child scheduling and fills occur at the observed
event time. Completion still means all intended quantity submitted,
not actual quantity filled. No parent capital reservation or independent
child risk check in simulated EMS; existing simulated ledger and matching
policy remain authoritative. Independent per-child transit delay and
full data provenance are future work. No real exchange calls.

## Fill-preservation follow-up

For causal fixed-route simulated parents, collect each parent's result
independently. Mark a failed parent terminal Failed with its error
reason and continue collecting fills from other parents, including
across algorithm classes. Do not discard previously executed fills
when a later child is refused. This relies on the single fixed-route
allocation and synchronous simulated child contract; it does not
resolve ambiguous execution or legacy multi-allocation error handling.
Legacy and PostgreSQL EMS error propagation are unchanged.
