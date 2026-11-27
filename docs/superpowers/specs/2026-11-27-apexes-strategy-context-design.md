# F04.03 — Strategy instrument and ledger context

## Contract

The shared trading loop attaches an optional InstrumentContext to
StrategyContext. Resolve exactly one configured instrument for the
event's symbol and require the event venue to match. Any duplicate
symbol is ambiguous: the existing ledger and rule map use symbol keys,
and MarketEvent lacks asset class. Do not guess which instrument owns
a balance or rule. Native Rust consumers receive borrowed Decimal
metadata and optional rules from the same session rule map used by
order conformance. Existing rule admission, matching, risk, capital
reservation, and clocks are unchanged.

Drift callbacks receive a detached snapshot:

```drift
function on_market_event(event, ctx) {
    if (ctx.market == null || ctx.market.rules == null) {
        return [];
    }
    let quote = ctx.ledger_balance(ctx.market.quote_currency);
    if (quote == null) { return []; }
    // Read quantity_step, price_tick, min_quantity, max_quantity,
    // and min_notional. Final admission is still the engine's job.
    return [];
}
```

Fields of ctx.market: symbol, venue, asset_class, base_currency,
quote_currency, tick_size, lot_size, multiplier, rules. Missing
currencies and unavailable rules are null. Configured tick_size and
lot_size are not exchange-verified trading limits. Session-supplied
rules retain zero-valued disabled constraints as supplied; there is no
fabricated min_notional or default max quantity. Current historical and
paper runners do not populate rule snapshots; live uses the supplied
rules without new exchange queries or freshness claims. Ambiguous,
missing or mismatched instrument context is null.

ctx.ledger_balance(asset) returns the actual recorded local ledger
value or null for absent entries; malformed argument lists return
undefined. This is not exchange free balance or spendable capital
after reservations. Rust retains exact Decimal; Drift uses f64 just
like the existing SDK, with possible precision loss. Final Decimal
rule enforcement remains authoritative. New and legacy balance lookups
share an owned snapshot; saving a context or lookup in script state
does not refresh it. Existing get_balance missing-as-zero behavior,
get_position, has_open_order and deployed V1 logic remain unchanged.

Rust callers constructing StrategyContext literals must now specify
market (None when no instrument context is available); every existing
workspace literal was updated. No new mandatory method on Strategy.

## Verification

Three new regression tests: Drift native lookup missing/zero/arity
contract, script integration for real rule fields and missing/ambiguous/
wrong-venue context, and native shared-loop execution proving the
exposed session rule is applied to the order (0.027 rounds to 0.02)
and the next callback sees the post-fill local ledger values.
Rust 1.99.0 locked offline serial: 565 nonignored tests passed
(402 backtest, 15 paper, 118 strategy, 25 indicators, 5 loader);
three external tests ignored. All-target Clippy for strategy, loader,
backtest, paper, live-trading and engine passed with warnings denied.
Formatting and diff checks passed. Full workspace CI and independent
review pending. No exchange accounts, production databases or real
orders accessed.

Next: causal book observations with explicit integrity and freshness;
then execution costs, available/spendable capital and session health.
Historical rules provenance and multi-asset ledger disambiguation
remain open; no claim of full F04.03 completion.
