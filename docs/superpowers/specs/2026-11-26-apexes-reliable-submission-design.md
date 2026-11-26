# F01.02 — Reliable order submission

## Objective

A timeout, interrupted future, or ambiguous exchange response must not
permit a replacement order merely because the original acknowledgement
was lost. Persist the stable client UUID before exchange I/O and recover
using that same identifier, never a freshly generated replacement.

## Durable state

PostgreSQL orders gain a submission_state column, independent of the
exchange order status and without changing the public Order shape.

- Submitting: persisted before dispatch, may have reached the exchange.
- Uncertain: execution is unconfirmed; no exchange order ID is known.
- Accepted: authoritative acknowledgement or reconciliation supplies the
  exchange order ID.
- Rejected: authoritative refusal without an accepted exchange order ID.

The migration is idempotent. Existing rows with known exchange IDs are
accepted, existing rejected rows without an ID are rejected, and other
existing rows without an ID are uncertain. Historical false rejections
from earlier versions are not automatically repaired.

The pre-dispatch insert cannot overwrite an existing client UUID.
The in-memory open order is installed after the durable insert and before
awaiting exchange I/O; cancellation of the caller or failure to persist an
acknowledgement leaves the unresolved order guarded. The returned exchange
ID and final submission state are persisted with the order in one write.

## Reconciliation and guard

The OMS blocks further submissions for the same venue, asset class and
symbol while it has an open order without an exchange ID. This also
applies to flatten requests: an unknown fill can make the proposed close
quantity unsafe. Other instruments are not blocked by this feature.

On restart the existing open-order recovery restores the guard. Reconcile
first checks exchange open orders, then looks up unresolved orders by the
original client UUID. A found order is applied via the shared exchange
update path, including fill recovery and existing fill deduplication.
A negative lookup, even after the legacy warning grace period, never
proves that the original order did not execute. Keep it unresolved.
Explicit exchange rejection releases the guard only after persistence.

This is intentionally conservative. A permanently unresolvable submission
may require manual exchange/account investigation; do not clear the guard
by editing status blindly. Session-level health and operator recovery,
cross-session account-wide reservation, and authoritative fill-to-ledger
delivery are separate roadmap features. The guard covers one OMS's
tracked/recovered orders, not simultaneous independent managers racing
against one exchange account.

## Binance response policy

Retain F01.01's bounded retry after explicit HTTP 400 / -1021 timestamp
rejection, with identical client UUID. Never retry ambiguous writes.
Only known explicit refusal codes are treated as rejected; unknown
execution (-1006/-1007), duplicate order messages and unrecognized codes
remain uncertain, even in a 4xx response. An order-not-found response
(-2013) is only a negative lookup when HTTP status is 400, not when a 5xx
carries the same body. No lookup result authorizes automatic replacement.

## Qualification

Use local WireMock and isolated PostgreSQL fixtures. Exercise state
persistence, interrupted submissions, late exchange acceptance/fills,
restart recovery, rejection, repeated negative lookups and ambiguous
response classification. Full workspace build, fmt, Clippy, tests and
green remote CI are required. Do not use live credentials or real orders.
