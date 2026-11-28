# F04.04 — Deterministic intent correlation

Each strategy decision allocates session-local IntentId ordinals
starting at one in returned intent vector order, before validation.
Distinct attempts, including identical intents or refused intents,
receive distinct IDs. The checked counter returns unavailable at
exhaustion rather than wrapping or changing execution admission.

The existing arrival queue stores the same optional ID alongside
the intent and deadline. Rule conformance, arrival, emergency transit
cancellation and end-of-session expiry retain this identity. No change
to queue order, capital reservation, validation or report counters.

The shared loop binds actual returned OrderId and ParentOrderId
values to intent IDs. Separate namespaces prevent cross-type matches.
Subsequent direct/resting/supplemental fill and terminal notifications
look up the actual order identity; parent phase/unavailable/cancellation
notifications look up the actual parent identity. Unknown or conflicting
bindings return unavailable rather than guessing from symbol, price,
quantity or timing. Conflicting bindings remain unavailable. Automatic
risk exits and unassociated EMS child fills have no inferred intent ID.
The maps grow with returned identities during a session, as notification
history does; they are not durable or evicted.

All four native lifecycle payloads gain optional intent_id. Drift
receives a decimal string (preserving the full u64) or null. Existing
callback functions remain optional and notification-only; native
callers constructing payload literals now specify intent_id.
No changes to OrderIntent, exchange IDs, report schema, replay identity
policy, admission, settlement or scheduling semantics. Same decision
stream yields the same ordinals; no cross-session uniqueness or guarantee
of equal decisions under different live observations.

## Verification

New unit regression covers checked allocation, independent parent/order
namespaces, absent mappings and permanently unavailable conflicts.
Extended sealed native/Drift direct-order replay verifies ordinal
sequence 1,2,3,1,4,4 across scheduling, refusals and arrivals, including
duplicate-looking intents. Extended fill replay verifies direct,
partial resting and complete terminal identity; transit expiry retains
the scheduled identity. Existing deterministic two-parent replay checks
both parent IDs and intent ordinals across ten runs. Report fingerprint
and execution assertions remain intact.

Rust 1.99.0 locked offline serial: 607 tests passed (439 backtest,
123 strategy, 15 paper, 25 indicators, 5 loader); three external tests
ignored. All-target Clippy with warnings denied passed for strategy,
loader, backtest, paper, live-trading and engine. Full workspace CI and
independent review pending. No live orders, accounts or databases used.

This first correlation slice is session-local, not durable storage.
Pre-EMS submission refusals still lack parent notifications; authoritative
child-to-parent execution attribution/progress, persistent recovery and
live-service qualification remain open. Next attach explicit EMS child
correlation before adding durable lifecycle storage.
