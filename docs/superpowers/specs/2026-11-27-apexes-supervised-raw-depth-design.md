# F03.03 — Opt-in supervised raw depth capture

## Capture contract

The raw_depth_capture::capture API captures one explicit Binance market
(Spot or USD-M Perpetual) and canonical symbol on a dedicated bounded
channel. The WebSocket is connected before the REST snapshot request.
While REST is outstanding, buffer the original incremental updates.
Discard obsolete final IDs without refreshing the publication clock.
Require a valid advancing initial bridge and verify the whole buffered
chain before publishing the initial snapshot and increments.

For Spot, the advancing range must include the prior final ID plus
one. For USD-M, the initial advancing range must bridge the snapshot
ID and its previous-final ID cannot be later than the snapshot;
later updates must link exactly to the last final ID. Original U/u/pu
are preserved. The initial bridge is deliberately conservative:
if a valid advancing bridge cannot be established, retry from a new
snapshot rather than guess or manufacture IDs.

Maintain absolute level updates with zero-quantity deletions and
reject crossed or one-sided reconstructed books. The REST snapshot
is depth-limited (1,000 levels); no claim is made that it represents
the complete exchange book. Bound WebSocket frames, REST response
size, buffered events/levels and reconstructed book levels. Use
nondecreasing publication timestamps: buffered updates become
observable only after the bridge validates, and timestamps are
assigned when published into the bounded channel.

## Recovery and backpressure

Use bounded connection/initialization and data-idle deadlines,
ping/pong handling and capped reconnect backoff. Only advancing
published data refreshes the data-idle timer. A gap, malformed frame,
timeout, disconnect or invalid book ends the connection attempt.
Publish explicit depth invalidation before reconnect backoff; require
a new snapshot/bridge on every attempt.

Reserve capacity for the whole startup batch before sending any of
it. If the bounded channel is full, terminate capture with an error
rather than wait indefinitely, silently discard frames, or emit a
partial startup batch. If an invalidation cannot be queued, terminate
with an error too. The caller must observe the capture task result
and discard its book on termination, including if the channel was
full. The channel should be dedicated to capture and sized for the
startup batch. Closing the receiver stops capture.

## Integration boundary

This is an opt-in capture API and does not change the existing
BinanceMarketDataProvider or activate a replacement trading feed.
Use one source namespace per symbol and market kind. The API emits
normalized DepthUpdate events with native source range metadata, so
a subsequent storage consumer can preserve them. Durable collection
receipts, a dedicated persistence consumer, range-aware segmentation
across reconnects, and production qualification remain follow-up work.
No production network feed, schema migration or trading operation
was executed during qualification. The exact public endpoint behavior
has not yet been tested against Binance.

## Qualification

Rust 1.99.0 locked apexes-market-data all-target check and Clippy
with warnings denied passed. All 55 market-data library tests passed,
including seven new tests for Spot and USD-M bridge/link semantics,
buffer/book validation, bounded channel publication, endpoint
construction, and a local REST/WebSocket reconnect/invalidation
scenario. Formatting and diff checks passed. The development Mac
has approximately 4 GiB free after the targeted build; full local
workspace build/tests and real exchange qualification were not run.
Full workspace lint, tests and Docker qualification remain pending
remote CI. Independent review pending.
