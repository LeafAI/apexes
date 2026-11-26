# F01.01 — Binance exchange clock synchronization

## Objective

Resolve timestamp failures observed in Binance signed requests without
enlarging the receive window by default or retrying uncertain order
submissions. Use the same mechanism in the gateway's REST requests and
Spot user-data subscription; public market-data requests stay unsigned.

## Clock state and synchronization

A REST client and its clones share independent Spot and USDS-M Futures
clock states. Before the first signed request, and on the first request
after a sample is 60 seconds old, query the corresponding unsigned
`/api/v3/time` or `/fapi/v1/time`. The sample anchors server milliseconds
at the midpoint of a monotonic RTT. Subsequent timestamps advance using
monotonic time, not local wall-clock time, so an OS clock adjustment
does not invalidate the estimate. Log family, measured RTT and local
clock offset (no credentials, signatures or response bodies).

Only one sampling request per family can run across clones at a time.
Reject invalid responses, nonpositive timestamps, and samples whose RTT
exceeds the smaller of 1 second or half the configured receive window.
The time HTTP request has a one-second deadline. Failed sampling
invalidates the sample and suppresses further attempts for one second.
A failed initial/expired synchronization means no signed request is sent;
the REST layer returns a clearly identified synthetic client rejection.
Existing 429/418 rate-limit gates also apply to clock preflight responses.

## Retry contract

For signed requests an explicit HTTP 400 containing a parseable Binance
error with code -1021 establishes that the request was rejected for its
timestamp. Invalidate the appropriate sample and retry at most once,
using the original parameter string and client order ID, with a freshly
computed timestamp and signature. A second rejection is surfaced and the
sample invalidated. Never retry an order-changing request for transport
errors, malformed responses, or 5xx responses (even with a -1021 body).
Existing idempotent retry rules remain in effect for signed HTTP attempts.
The next feature, F01.02, owns reconciliation of ambiguous submissions.

The actual gateway Spot WebSocket subscribe/reconnect path uses the
shared Spot clock after connecting. An explicit -1021 subscription
rejection triggers one fresh connection and synchronization; all other
startup and reconnect behavior is preserved. The standalone low-level
WebSocket helper retains its previous local timestamp behavior.

## Configuration

The default receive window remains 5,000 ms. The engine reads the optional
`APEXES_BINANCE_RECV_WINDOW_MS` environment setting (integer 1..60,000).
Invalid values fail startup; the supervisor allowlists the setting and
the compose engine forwards it. The same window is used for REST and
Spot subscription signing. The gateway's existing explicit
`BinanceGatewayConfig.recv_window_ms` remains supported.

## Qualification and limits

Use only local WireMock and WebSocket fixtures to exercise skew correction,
clock cache expiry, concurrent clones, Spot/Futures independence,
explicit rejection retry with identical client ID and valid HMAC,
ambiguous-response safety, preflight failures, cooldown/rate-limit
gates, WebSocket resubscription and invalid receive windows. No real
exchange orders or credentials are required. Workspace build, tests,
Clippy, formatting and CI are mandatory before merge. The feature
does not guarantee exchange availability or repair uncertain orders;
the session-health and reconciliation features will address those cases.
