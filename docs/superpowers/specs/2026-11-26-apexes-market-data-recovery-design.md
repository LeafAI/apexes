# F01.03 — Market-data recovery

## Objective

A healthy symbol must not conceal another symbol's stale market data. A
broken or lossy relay connection must not appear to be a continuous market
stream. Recover a coherent depth snapshot before applying incremental
updates, and engage the existing live trading kill switch on detected loss.

## Freshness and relay transport

Track usable market-event arrival with a monotonic clock for each configured
(venue, symbol). Trade, quote, bar and valid depth events refresh that
instrument; mark prices and explicit book invalidations do not. The
configured live staleness timeout checks the stalest instrument, including
immediately before processing an event following a long stall. The aggregate
idle metric reports this worst age. A connected but quiet symbol is distinct
from a failed heartbeat; either can independently prevent safe trading.

Engine gateway clients use a five-second connection/I/O timeout, ten-second
WebSocket ping interval and thirty-second inbound heartbeat deadline.
Reconnect with exponential backoff (0.5–30 seconds); reset on a valid event
or Pong rather than on handshake alone. The output queue is bounded to
1,024 events. Overflow, malformed frames and connection loss cause
reconnection, not silent dropping. Live clients engage the shared kill
switch on these failures, on network mismatch and on invalidated books.
They reject exchange timestamps older than the configured staleness limit
(or thirty seconds if not configured), and timestamps over five seconds in
the future. Paper clients reconnect without a live OMS kill switch.
The server reads client frames while forwarding data, including Ping/Pong,
and closes a lagged subscriber rather than forwarding a discontinuous suffix.

## Depth recovery

Binance upstream sequencing remains the responsibility of barter-data,
which checks exchange update ranges. The final sequence alone is not
a consecutive event counter. When the depth adapter encounters a stream
error, reconnection or replacement, invalidate affected books using an
empty snapshot with no exchange sequence. Re-establish the connection and
require a fresh snapshot before forwarding any deltas for a symbol.

The gateway reconstructs books from snapshots and validated deltas,
including zero-quantity deletion, and discards non-increasing sequences
and deltas without a base. On a new live connection it takes the cached
snapshot and creates the broadcast receiver within the same critical
section, preserving the original source timestamp and preventing an
update from slipping between snapshot and subscription. Normal updates
remain incremental. Historical replay connections retain the existing
replay protocol, rather than inserting a current snapshot ahead of history.
On upstream termination the cached books are cleared.

## Safety and limitations

The existing OMS kill switch blocks ordinary submissions and requests
cancellation of open orders. Reconnection does not automatically release
it. Operators must review data freshness, reconstructed depth, open orders
and positions before releasing a halt. The existing release grace window
and explicit flatten controls are preserved. Fully gated operator recovery
and persistent session-health states belong to F01.05; protective exits
belong to F02.04.

The existing MarketEvent identity uses venue and symbol, not asset class:
simultaneous Spot and Perpetual feeds with the same normalized symbol
are not independently distinguishable by this feature. Testnet depth is
not supported by the existing Binance provider. The upstream provider's
own idle supervisor and sequence validation remain necessary; the relay
cannot detect arbitrary missing exchange events solely from final update
IDs. The bounded engine queue does not bound all upstream channels.

## Qualification

Use local mock feeds, loopback WebSockets, virtual-time watchdog tests and
isolated PostgreSQL for the workspace suite. Regressions cover per-symbol
freshness, stale/future timestamps, malformed frames, bounded queue
overflow, silent socket heartbeat timeout, snapshot reconstruction,
snapshot/subscription continuity and Ping/Pong on a quiet gateway.
Full workspace build, fmt, Clippy and required CI remain merge gates.
No live exchange credentials or orders are needed.
