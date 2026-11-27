# F04.03 — Causal simulated order-book context

## Contract

Native StrategyContext gains optional BookObservation; Drift exposes
the detached ctx.book snapshot or null. The shared event loop resolves
the unique configured event instrument first, and asks the executor
for a read-only observation after processing the current event and due
simulated arrivals. The default executor implementation returns None.
MatchingEngineOrderExecutor queries the same MatchingEngine and strict
ReplayOrderBook used for direct and EMS execution, without maintaining
an independent book or altering orders, fills, clocks or depth policy.

Only a configured strict simulated depth policy permits a book
observation. The existing strict freshness and integrity checks and
matching engine's global causal watermark apply. Missing, stale,
invalid or future books, ambiguous instruments and wrong-venue
observations are unavailable. A gap needs a fresh snapshot under the
existing validation policy. No synthetic quote is constructed from a
trade or tick. Legacy simulation without strict validation and live
executors without validated book support return no book.

The native observation uses Decimal prices/quantities, exact u64
optional sequence, UTC source observation and decision timestamps,
configured max age, policy and optional best remaining bid/ask.
The exact freshness test uses time deltas; displayed age_ms truncates
sub-millisecond precision. The decision timestamp is the callback's
logical clock, not a new network-latency measurement. The returned
view is detached and does not consume liquidity; saved snapshots do
not refresh. Native callers constructing StrategyContext must specify
book (None if unavailable); all existing workspace literals are updated.

## Drift API

```drift
function on_market_event(event, ctx) {
    let book = ctx.book;
    if (book == null || book.best_bid == null || book.best_ask == null) {
        return [];
    }
    let spread = book.spread;
    let askPrice = book.best_ask.price;
    let askQuantity = book.best_ask.quantity;
    // Admission still belongs to the execution/risk engine.
    return [];
}
```

Fields: symbol, venue, source ("simulated_remaining_depth"),
observed_at, decision_at (RFC3339), age_ms, max_age_ms, integrity
("snapshot_only", "contiguous", "verified_ranges"), sequence (decimal
string or null), best_bid and best_ask (price and quantity or null),
spread (ask minus bid or null if either side absent). Sequence strings
preserve IDs above the exact integer range of f64. Numeric prices,
quantities and spread use Drift's f64 representation, with possible
precision loss. Engine Decimal checks remain authoritative.

Best levels reflect simulated remaining depth after consumption,
including due simulated arrivals. They do not represent raw source
quantities, total book liquidity, guaranteed executable quantity or
exchange-confirmed liquidity. Exhausted levels are skipped and an
empty side has no invented price. Integrity identifies the existing
policy, not an independent source-completeness attestation. No
assumption of complete live depth, no live exchange calls and no
change to execution admission. Actual validated live book sourcing
and costs, spendable capital and health remain future F04.03 slices.

## Verification

Two new regressions:
- Direct strict ReplayOrderBook observation: read-only idempotence,
  future/stale rejection (exact boundary and one nanosecond beyond),
  consumed top-level rollover, empty-side handling, detached snapshot.
- Real native/Drift-backed sealed replay: exact sequence
  9007199254740993; ask liquidity 10, 9, 7 after fills at 500 and
  1000 ms; exact freshness boundary; gap invalidation and snapshot
  recovery; legacy strict-policy absence; equal canonical report
  fingerprints across two strict runs.

Rust 1.99.0 locked offline serial 567 nonignored tests passed
(404 backtest, 15 paper, 118 strategy, 25 indicators, 5 loader);
three external tests ignored. All-target Clippy with warnings denied
for strategy, loader, backtest, paper, live-trading and engine passed.
Formatting and diff checks passed. Workspace CI and independent
review pending. No real orders, exchange accounts or production
databases accessed.
