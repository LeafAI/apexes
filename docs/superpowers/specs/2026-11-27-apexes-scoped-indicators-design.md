# F04.01 — Instrument-scoped native indicators

Add explicit callback-bound ctx.indicators.sma(name, period, price),
ema, rsi, stddev, macd(name, fast, slow, signal, price),
bollinger(name, period, deviations, price), atr(name, period, bar).
Each name identifies a native instance in the current event's exact
venue/symbol state. Names cannot change kind or configuration after
first successful use. Warm-up and outputs match existing native
indicators; null means warming up, not failure. Repeated calls
advance the named instance explicitly; no implicit event deduplication.

Use the existing callback transaction for both data and indicator
state; clone native state, commit both together only on a valid
callback, invalidate retained closures. Bounds: 128 populated
instrument keys shared with data state, 16 indicator names per key,
64-byte names, periods 1..512. No silent reset or eviction. Legacy
global factories remain unchanged, including Trend Guard V1.
Migration requires explicitly choosing named scoped calls, not
rewriting strategy semantics. No disk persistence or bar scheduling.

## Migration and limits

Replace a legacy top-level factory and update:

```javascript
let sma = indicators.sma(20);
// In on_market_event:
let value = sma.update(event.price);
```

with the callback-scoped equivalent:

```javascript
let value = ctx.indicators.sma("trend", 20, event.price);
```

Use a distinct name for each logical indicator and stable configuration
within a session. Do not call on irrelevant event types; for ATR supply
an OHLC object and only update on the intended observations. Callback
scopes do not automatically schedule closed bars. Period changes are
explicitly refused, not silently reset (a different name or a new
session is required). The data store and indicator store share the
same transaction and instrument capacity. Scoped prices must be
positive and at most 1e12; ATR requires coherent positive OHLC;
Bollinger deviations must be in 0..=100. Legacy factories retain
their existing input contract. Extreme RSI gain/loss ratios use
an overflow-safe equivalent fraction only when the ordinary
calculation cannot represent its intermediate ratio.

## Verification

Five new regressions: all seven indicators compared update-for-update
with independent native reference instances, capacity and expired
closure rejection/rollback, real Drift interpreter multi-symbol and
multi-venue independent warm-up and rollback, legacy single-symbol
SMA migration equivalent intents, and extreme RSI ratio safety.
Existing native indicators and strategy scripts remain compatible.
Rust 1.99.0 locked offline serial 550 nonignored tests passed
(394 backtest, 15 paper, 111 strategy, 5 loader, 25 indicators);
three external tests ignored. All-target Clippy with warnings
denied for those five crates plus live-trading, formatting/diff
checks passed. Workspace CI and independent review pending.
No production orders, accounts or databases accessed.
