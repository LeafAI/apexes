# F04.01 — Explicit instrument-scoped strategy state

Provide a bounded Rust SDK SymbolState<T> keyed by exact event venue
and symbol. Drift callbacks get ctx.state_get(name, default) and
ctx.state_set(name, value) bound to the current event's key, not
a script-provided symbol. Independent session stores. Persistent
callback-local data is restricted to finite numbers, booleans, null,
bounded strings and bounded flat numeric arrays for indicator windows.
No native functions, mutable native indicator handles or arbitrary
objects may enter the store. Existing global variables/indicators
and portfolio context remain unchanged; migration is explicit.

Stage a detached snapshot per callback; commit only after a successful
script call and valid returned intents, with no rejected state
operations. A failed state operation suppresses intents and rolls back
the staged writes. Retained callback closures are invalidated after
the callback. No rollback of arbitrary script globals or execution
outcome implied. Bound memory by symbol count, key count and value
size; no silent eviction of risk-relevant state. No disk persistence
or automatic isolation of existing global indicator instances yet.

## Authoring and limits

In a trade callback, for example:

```javascript
let count = ctx.state_get("count", 0) + 1;
let old = ctx.state_get("prices", [0, 0]);
let window = [old[0], old[1], event.price];
ctx.state_set("count", count);
ctx.state_set("prices", [old[1], event.price]);
// Use math.mean(window) only once count >= 3.
```

The maximum is 128 populated venue-symbol keys per session, 32 named
values per key, 64-byte names and event symbols, 1024-byte strings,
and 512 finite numbers per array. Reads do not allocate persistent
entries. Rejected state operations abort the callback's intents and
staged writes; no execution acknowledgment is implied. A retained
old context cannot access or mutate current state. The limits apply
to this store only, not arbitrary interpreter globals. A new strategy
instance starts empty. The Rust helper's capacity is caller-selected.
The market event identity does not distinguish asset classes with
identical venue and symbol; the existing instrument identity model
must be extended before such a configuration can be isolated.

## Verification

Two binding-unit regressions cover rejected capacity and values,
rollback and expired callback closure invalidation. A real Drift
interpreter fixture interleaves Binance BTCUSDT, Binance ETHUSDT,
and OKX BTCUSDT, verifies independent three-sample moving averages
(20, 200, 2000), rejects invalid data and return values without
contaminating windows, and verifies a fresh session starts cold.
Existing Trend Guard V1, example and mean-reversion scripts are
unchanged. The explicit data API does not automatically isolate
pre-existing global state or stateful native indicator handles.

Rust 1.99.0 locked offline serial 521 nonignored tests passed
(394 backtest, 15 paper, 107 strategy, 5 strategy-loader), three
external tests ignored. All-target Clippy with warnings denied for
strategy, loader, backtest, paper and live-trading; formatting/diff
checks passed. Full workspace CI and independent review pending.
No exchange accounts, production databases or actual orders accessed.
