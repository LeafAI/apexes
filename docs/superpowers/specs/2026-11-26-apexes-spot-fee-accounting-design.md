# F02.03 — Spot fee and P&L attribution

## Scope of this increment

Separate the economic contribution of session executions from account
holdings and from exchange fee movements. Use the shared PortfolioLedger
in backtest, paper and live trading. Preserve existing balance, risk,
position recovery and equity calculations. The new spot_pnl fields are
session attribution, not lifetime cost-basis or tax accounting.

Each symbol has a fee-free shadow of its executed quantities and a
weighted average entry price. Its gross realized plus gross unrealized
P&L equals the marked trading cash-flow contribution before fees. This
shadow may be negative when the session sells starting holdings; it
does not authorize a short position. Actual position/balance accounting
remains independent.

## Fee valuation

Keep native amounts by fee asset, using quote when fee_currency is absent
(the same convention as the ledger). Quote fees have unit rate. Base
fees use execution price at the fill. Other assets require a same-venue
direct conversion pair to the instrument quote, with an observed
timestamp at or before the fill and age at most five minutes. Per-pair
price history is bounded to 2,048 observations. No future price, assumed
stablecoin parity or invented zero conversion is permitted.

At report time, value the fee quantities at the latest observed marks.
Gross P&L less marked fees gives the net marked contribution. For the
realized/unrealized split, expense fees at execution and attribute the
change in their marked value to unrealized. Thus:

- gross = gross_realized + gross_unrealized
- net_realized = gross_realized - fee_at_execution
- net_unrealized = gross_unrealized - (fee_at_mark - fee_at_execution)
- net = gross - fee_at_mark

The marked fee revaluation is necessary to reconcile the contribution
against the starting wallet held at current marks. It is not an
additional exchange charge. Existing execution prices already incorporate
slippage; do not subtract a second assumed slippage charge. Missing
conversion or mark produces an optional unknown amount, not zero.
Historical cost remains unknown if the bounded causal history could not
price a fill when it was applied. Net marked P&L can still be known when
current marks are available.

Tracked fee assets continue to be debited once by the existing ledger.
Untracked third-asset fees remain explicit external costs in attribution;
do not create fictitious wallet balances or subtract them again from
equity. The new attribution is distinct from legacy account-equity and
position P&L fields. Aggregation in the operator details groups by quote
currency; no addition across incompatible quote units.

## Integration and limitations

The shared event loop feeds observed trades to the accounting history
and records only fills successfully applied to the ledger. BacktestReport
and EvaluationSummary carry backward-compatible spot_pnl fields. The
operator details show gross, execution fee cost and net by quote, with
unpriced values explicitly labeled.

This increment covers spot execution attribution for configured direct
market pairs. A dedicated fee-only conversion feed, automatic historical
price retrieval for unconfigured fee assets (such as BNB in a BTCUSDT-only
session), persisted conversion provenance and lifetime attribution across
restarts remain to be implemented. Current marks are latest observed
marks, not a synchronized account valuation snapshot. Existing quote
accounting and leveraged P&L remain unchanged. F02.03 is in progress
until these remaining conversion/reconciliation requirements are addressed.
No real orders or live databases are used.

## Qualification

Unit tests cover base fees and fee revaluation, quote and third-asset
fees, causal versus future/stale/wrong-quote prices, actual wallet equity
reconciliation with held BNB, and unavailable external-fee valuation.
The real Drift baseline asserts gross/net/fee totals against its
equity curve. Operator detail tests preserve currency units and unknown
cost. Full workspace build, serial tests, PostgreSQL group, Clippy, fmt,
doc tests and remote CI are merge gates.
