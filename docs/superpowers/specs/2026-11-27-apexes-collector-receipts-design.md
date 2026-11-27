# F03.03 — Durable raw depth collector receipts

A dedicated opt-in collector owns a bounded raw capture channel and an
explicit ClickHouse raw_depth_batches store. A fresh UUID identifies one
run, one market kind and one symbol; never write these records to the
legacy symbol/venue-only depth table. Preserve exact JSON event bytes
(including nanosecond publication time, decimals and native range IDs)
and order within each batch. Read by run ID and batch index; reject
duplicate rows, not silently deduplicate an ambiguous insert.

Create a new local JSONL receipt journal per run, sync its header and
parent directory before capture starts. Before each bounded insert,
append and sync an intent containing the batch index, event count,
SHA-256 of the exact payload and invalidation count. Only after
ClickHouse acknowledges the synchronous insert append and sync its
commit record. On write failure stop capture; do not drop or retry
ambiguous inserts. The journal is independent of the exported data.
Missing commit, missing terminal record, partial line or failed terminal
are not evidence of successful collection. A stopped terminal only
describes intentional shutdown and acknowledged observed batches, not
upstream coverage. Reconnect invalidations remain in the payload.
No claim of independently attested source completeness or contiguous
time windows is made. Range-aware segmentation and offline journal
reconciliation into coverage evidence remain follow-up work.

Stop by closing the receiver and draining queued events; capture error
or backpressure terminates the run after draining accepted events.
No default trading provider, existing ingestion path or production
database changes are activated by this API. Qualification uses only
local synthetic data; real ClickHouse tests are opt-in/CI.

## Qualification

Rust 1.99.0 locked targeted all-target check and Clippy with warnings
denied; 60 market-data and 34 storage library tests passed serially,
including seven new regressions. Formatting and diff checks passed.
An ignored real ClickHouse round-trip/duplicate detection test was
added; local Docker is unavailable, so it and full workspace CI remain
pending. No production exchange or database was contacted. Independent
review pending.
