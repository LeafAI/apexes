# F03.04 — Stable portfolio and emergency execution ordering

HashMap iteration must not determine the order of automatic liquidation
or de-risking, funding debits/credits, or finite-precision portfolio
reductions. Use ascending symbol/asset keys for these operations.
Keep market event order and strategy intent order unchanged. Do not
sort fills after execution. This is a shared ledger invariant in all
modes: existing candidates, risk decisions, margin formulae and live
liquidation prohibitions stay unchanged; the previous arbitrary
ordering becomes explicit, not a new risk-priority algorithm.

Verify simultaneous cross-margin liquidation through the historical
runner with a sealed input and deterministic IDs, and de-risking with
multiple long/short positions inserted in opposite orders. Check
finite-precision equity reduction against an explicit ordered fold.
Arbitrary strategy map iteration, external providers and full replay
qualification remain separate concerns.

## Verification and limits

Use lexical symbol order for emergency candidates and funding
settlements and lexical asset order for equity. Cross-maintenance,
gross exposure and realized PnL reductions use ordered entries;
parent status queries use UUID order. No report fill reordering or
changes to liquidation thresholds, order eligibility or risk checks.

A sealed two-instrument synthetic cross-margin replay admits ETH
then BTC, liquidates BTC then ETH on the same observation, and
reproduces all four fills and final equity (18.5 USDT) and the report
fingerprint across ten runs with alternating instrument insertion
order. Additional regressions verify de-risking long/short candidate
order and non-associative Decimal equity reduction.

Rust 1.99.0 locked offline 393 nonignored backtest/paper tests passed
(378 backtest, 15 paper); three external tests ignored. All-target
Clippy passed for backtest, paper and live-trading; format and diff
checks passed. The wider live-trading test invocation had two
flattening failures and timed out while running PostgreSQL-dependent
fixtures; localhost:5432 was unreachable and the default Colima
profile was stopped. No test changes or database startup attempted.
Full workspace CI and independent review pending. No real orders,
accounts or production databases accessed. The data-dependent
F03.02/F03.03 work remains open.
