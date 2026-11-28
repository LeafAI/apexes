# F04.04 — Explicit simulated triggered-order terminal evidence

## Scope and contract

An admitted simulated stop can disappear from the working view for
multiple reasons. The matching engine must expose the actual result of
conversion rather than require the strategy adapter to infer cancellation
from that view. This slice covers direct simulated stop-market and
stop-limit orders in backtest and paper, using the existing unified
trading loop. No live exchange status normalization or submission logic
changes. Bounded Spot simulation continues to refuse unsupported stop
intents on admission.

The engine retains the original admitted order snapshot with the pending
trigger. A successful stop-market execution returning Cancelled supplies
IocRemainderExpired evidence: original requested quantity, actual filled
quantity (including zero), original order ID/type/stop price/creation
time and trigger event update time. A failed market or limit conversion
supplies ConversionRejected evidence with Rejected status and zero
executed quantity, because the conversion guard fails before consuming
depth. The fill path and ID allocation are unchanged. A successful full
execution keeps the existing Filled terminal path. A successfully
activated stop-limit that rests its remainder remains working and does
not emit an early terminal.

This evidence is event-local and consumed once. The matching adapter
matches it against the original tracked order and accumulated actual
fill quantity before pruning the working view. The unified loop
dispatches the callback only after accounting and notifying all resting
fills for that event. Scoped intent correlation remains unchanged.
Repeated market events cannot re-trigger a removed pending order. The
callback return value is ignored, as for existing lifecycle hooks.

Native strategies receive TerminalKind::Triggered with the original
order and typed TriggerTerminalReason. Drift strategies receive
kind=trigger_expired or trigger_rejected plus the existing order,
intent, venue, symbol, quantity and filled_quantity fields.
Existing Filled, emergency cancellation, transit and live payloads are
unchanged. Existing exhaustive Rust matches must handle the new variant.

## Fail-closed boundaries

Strict-depth invalidation, stale depth, an untriggered working order
at replay end, or absence from the working view does not prove an IOC
expiry or conversion rejection. Those paths emit neither new trigger
terminal. Reservation reconciliation, emergency preemption, capital
admission, depth consumption and causal execution ordering remain
unchanged. This is explicit local simulation evidence, not exchange
confirmation, a durable callback receipt or cross-restart replay.
Exchange-specific expiry/trigger motives remain unverified; lifecycle
recovery qualification and atomic strategy-state/callback recovery
are separate work.

## Qualification

Rust 1.99.0, locked dependencies, serial tests, warnings denied.
717 nonignored core tests passed, zero failures, six external tests
ignored. All 161 PostgreSQL OMS/EMS/live regression tests passed
(85/32/44) on a verified disposable PostgreSQL 17 cluster, removed
afterward. Workspace-wide all-target Clippy, formatting and diff
checks passed. Full workspace build/test, remote CI and independent
review remain pending.

Regressions cover partial and zero-fill IOC remainder expiry, original
identity and timestamps, duplicate suppression, successful full
stop-limit completion without duplicate trigger terminal, both rejected
conversion types with unchanged depth, depth-gap and stale-depth
disappearance without terminal inference, and the existing gapped-stop
capital reservation test with an explicit rejection callback. The
native/Drift sealed replay proves terminal delivery follows actual
fill callbacks, enables a later strategy decision, ignores returned
callback orders and repeats the economic fingerprint and exact
terminal evidence. No real orders or production database operations.
