# F03.05 — Historical funding attribution

Record the exact quote-currency movement produced by the existing
ledger settlement operation. Keep its rate, notional, valuation
price (mark or entry fallback), signed position quantity, symbol,
venue, scheduled boundary and observation timestamp. Positive balance
movement is income, negative is expense; aggregate in each symbol's
quote currency. The existing fixed-rate simulated funding model,
eight-hour observation-triggered cadence and pre-arrival versus
legacy ordering remain unchanged. A gap crossing several boundaries
still causes one pass, not fabricated intermediate settlements.

Historical runner alone opts into complete settlement history;
the shared loop retains one implementation and paper/live callers
do not accumulate additional unbounded histories. Old ledger and
loop entry points remain compatible. Summaries use checked arithmetic;
reconcile net realized execution P&L plus net funding only when
available in the same quote currency. This is a component sum, not
total account P&L (open inventory, unallocated fees and valuation
effects remain separate). Disabled funding is explicit, not missing
historical evidence. No assertion of exchange funding rates or
actual exchange settlements.

## Report semantics and verification

The optional historical `funding` field records configured fixed rate
(None = disabled), observed passes when enabled (including passes
with no held leveraged positions), all resulting movements, and
per-symbol totals for symbols with movements. Explicit zero rates
record zero movements for eligible positions. A missing per-symbol
entry does not claim actual exchange funding was zero. Quote units
are never combined. Older reports deserialize with absent funding.
No extra persistent or in-memory settlement history for paper/live.

The ledger regression verifies exact signed credits/debits for
opposite positions, stable symbol order, mark/entry fallback,
positive/negative/zero rates and unchanged combined balance.
The historical gap regression exercises both legacy and causal
loops across two eight-hour boundaries with no intervening
observations, and verifies one actual settlement with distinct
scheduled and observation timestamps. The existing repeated
perpetual fixture verifies 0.1 USDT expense, -1.199 USDT realized
execution net, -1.299 USDT realized-plus-funding and equal closed
account equity change across ten runs; disabled funding is explicit.

Rust 1.99.0 locked offline serial 406 nonignored tests passed
(391 backtest, 15 paper), three external tests ignored.
All-target Clippy (-D warnings) for backtest, paper and live-trading,
format/diff checks passed. Full workspace CI and independent review
pending. Actual variable rates, exchange funding and complete
mark-to-market reconciliation are outside this slice. No exchange
accounts, orders or production databases accessed.
