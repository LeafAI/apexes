# F00.02 — Trading decision and execution diagnostics

## Objective and invariants

Explain what the shared trading loop decided, which pre-trade checks refused
an intent, what the executor or EMS returned, and whether fills reached the
ledger. Keep the same execution, risk, accounting, and report semantics for
backtest, paper and live trading. No new service, database schema, or strategy
API is required. The diagnostic identifier is deliberately not an order ID
or a persisted strategy checkpoint.

## Trace contract

Use tracing target `apexes::decision`. A run UUID, monotonically increasing
market event sequence and intent ordinal identify an intent. Intent records
contain source (strategy, liquidation, derisk), event time, symbol, venue,
side, type, requested quantity and optional price. Strategy decisions with
intents are INFO; no-intent and paused decisions are DEBUG to avoid INFO
market-tick noise. Rules-conformed values and stable typed rejection reason
codes are INFO. Pretrade success distinguishes unconfigured checks.
The returned order ID and status connect subsequent fill records, including
delayed resting-order and exchange fills. An EMS parent ID connects to the
child-order ID via the in-memory and PostgreSQL EMS submit paths. A fill
record reports source, order/fill identity, quantity, price, fee, currency and
ledger application success/failure. Supplemental fills have no invented
causal event sequence; join on order ID instead. Fill records do not claim
that a submitted or accepted order was filled.

## Security and operation

Use an allowlist of typed reason codes for diagnostics and adjacent shared
loop submission/ledger error logs; do not log arbitrary error payloads,
signed URLs, credentials, strategy source or request headers. Retain
locally constructed symbol-rule and risk explanations in the existing order
view. Existing lower-level gateway logs are outside this feature's redaction
boundary; treat session logs as private trading data. Full fill reconciliation,
durable attribution across restart, and gateway error taxonomy are later
roadmap features. Diagnostics do not change the current ledger discrepancy
handling. Log retention and access control use the existing session logging
infrastructure; no extra in-memory trace history is accumulated.

Set `APEXES_LOG_FORMAT=json` and `RUST_LOG=info,apexes::decision=debug`
in the engine environment to inspect every decision; omit the debug filter
for order-producing decisions only. The supervisor forwards both variables
and saves the engine output to its existing session log. Backtest callers
can install their own tracing subscriber. Join records by run/event/ordinal,
then order ID or parent-to-child mapping; order IDs may originate outside
the current run (startup recovery or manually placed orders).

## Acceptance

- Real Trend Guard replay has identical economic outputs with diagnostics on.
- Entry and exit intents correlate with returned orders and applied fills.
- Risk-refused intents have explicit codes and no resulting order/fill.
- Supplemental fill ledger success and failure are distinguished.
- Free-form error details containing a synthetic signature are not emitted by
  the diagnostic formatter. Existing adjacent shared-loop logs use codes.
- Workspace build, tests, fmt, Clippy and remote CI pass before merge.
