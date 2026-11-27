# F04.02 — Causal strategy time and closed-bar gates

First slice: explicit Rust SDK and Drift callback API, using the
engine-provided StrategyContext.now, not wall clock or event count.
Bind the current event identity and observation time to the existing
instrument-scoped transaction. A named cooldown grants its first
request, then grants at elapsed duration >= requested milliseconds.
The duration is a positive integer <= 365 days and may be changed
without resetting the last granted timestamp. A grant is a strategy
decision, not an order/fill acknowledgment. Bound to 32 names per
instrument. No synthetic clock or tick generation.

accept_closed_bar() grants a closed OHLCV bar once per interval and
instrument; non-bar and exact duplicate/older overlapping bars do
not grant another update. Reject malformed intervals and observations
whose context time is earlier than their event time (bar close, not
open), as well as time regression relative to the last committed
time operation. Equal observation times are allowed. Use exact UTC
timestamps for decisions, including sub-millisecond precision.
The guard is opt-in, not a change to existing event execution.

Time operations participate in the same data/native indicator
transaction; failed callbacks or invalid operations discard all staged
changes and suppress intents. Expired context closures cannot mutate
current state. Time watermarks only cover callbacks using this API;
no guarantee of upstream completeness or actual source arrival time.
Historical causal mode can use its explicit event-time latency
configuration. Legacy bar-open replay/live context is refused by
these new gates; causal historical source sorting/explicit live
observation provenance, time-driven bar emission and quiet-market
timers are subsequent slices, not silently simulated here.

## Authoring

```javascript
function on_market_event(event, ctx) {
    if (event.type != "bar" || event.interval != "1m") { return []; }
    if (!ctx.time.accept_closed_bar()) { return []; }
    let mean = ctx.indicators.sma("minute", 20, event.close);
    if (mean == null) { return []; }
    if (!ctx.time.try_cooldown("entry", 60000)) { return []; }
    // Evaluate and return order intents.
    return [];
}
```

Filter the intended interval before consuming the bar, and apply
cooldown only where a strategy wants to consume a decision grant.
A valid callback with no intents still commits its state. The API
does not guarantee that a generated intent will be admitted or filled.
No timer event is fabricated; a cooldown becomes eligible at its
deadline but grants only on the next observed callback that requests it.
The existing context clock must be at or after event availability.
In particular, legacy open-time bar delivery cannot use the new closed
bar gate. No upstream closed-bar provenance is inferred.

## Verification

Six new regressions: exact nanosecond threshold and dense/sparse
cooldown requests, future/duplicate/overlapping bars and interval
separation, bounded named cooldowns, retained closure invalidation,
real Drift per-instrument/venue grant isolation and rollback, and
actual historical runner using Drift with duplicate closed bars.
Sparse/dense causal runs fill at the same times (2500ms and 6500ms
relative to fixture origin), legacy open-time runs produce no fills.
Rust 1.99.0 locked offline serial 556 nonignored tests passed
(395 backtest, 15 paper, 116 strategy, 5 loader, 25 indicators);
three external tests ignored. All-target Clippy with warnings denied
for strategy, loader, backtest, paper and live-trading.
Formatting/diff checks passed. Full workspace CI and independent
review pending. No exchange accounts, databases or real orders accessed.
