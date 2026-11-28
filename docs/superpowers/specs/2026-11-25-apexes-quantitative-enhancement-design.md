# Apexes quantitative trading enhancement — design

## Objective

Establish a reproducible, cost-aware research and trading system without
replacing the existing Rust workspace or shared event loop. Binance Spot is
the first integration; synthetic fixtures and testnet verify correctness,
not profitability. Production market data is required for research. Mainnet
activation is outside this plan until explicit operator approval and safety
qualification.

## Invariants

- One trading loop and consistent strategy semantics across modes.
- Never retry an order of uncertain outcome as a new order. Reconcile first.
- Exchange fills are authoritative; block new exposure on an unresolved ledger discrepancy.
- Separate inventory, strategy ownership, cost basis and reserved balances.
- Never let an entry throttle alone prevent a verified exposure-reducing action.
- Model fees, spreads, latency and liquidity; avoid double-counting slippage.
- Use causal, timestamp-ordered data; no lookahead or repeated tuning against the holdout.
- Scope strategy state by instrument unless explicitly portfolio-wide.
- Preserve original strategy and backward-compatible APIs where feasible.
- No credentials, signed URLs or secrets in diagnostics.
- Feature status lives exclusively in FEATURES.md; sequence in PLAN.md.

## Delivery

One feature per independently reviewable iteration. Regression tests precede
fixes; deterministic local tests precede any testnet qualification. Use
signed and signed-off commits when approved by the owner. Full workspace
build, serial tests, clippy and fmt are required before declaring a feature
complete; external integration tests are explicitly qualified or reported
unverified. The feature-level acceptance criteria are in the companion plan.

## Baseline design (F00.01)

Use the unchanged deploy/strategy.trend_guard.drift through the actual
DriftScriptStrategy, BacktestConfig and shared event loop. Synthetic
ETHUSDT trades and order-book snapshots have fixed UTC timestamps and
deterministic prices. Three cases: flat (no fills, unchanged equity),
trend (take-profit buy/sell, exact fee and final-equity identities), and
trend followed by a sharp decline (stop-loss exit after entry). Run the round trip twice at 10 bps and once at zero fees.
Compare all economic report fields except randomly generated order/fill UUIDs;
the report is economically reproducible, not byte-identical.

Keep simulation working-order views consistent with the live OMS. A terminal
order belongs in the bounded order history, not in the open-orders map.
Resting orders remain visible until terminal. This regression was discovered
by the baseline test: the original script bought once but could never sell
because has_open_order() saw a filled order. Incomplete immediate market
orders remain a separate execution-model issue (F03.02).

The fixture is intentionally synthetic and assumes executable depth at the
displayed bid/ask. It proves accounting and control flow only. Actual
production data, fees and execution assumptions are evaluated in Phase 3.


## Managed live lifecycle evidence (F04.04)

See [managed live fill attribution and notification progress](2026-11-28-apexes-live-fill-attribution-design.md). The existing durable execution journal remains authoritative; inventory replay is not a receipt for strategy callback execution or an atomic strategy-state checkpoint.


See also [authoritative managed live terminal callbacks](2026-11-28-apexes-live-terminal-callbacks-design.md): a terminal delivery marker shares the existing authoritative status/fill transaction; live terminal callbacks wait for the fill notification prefix and never infer terminal state from disappearance. Startup inventory and terminal baselines are not durable strategy receipts.

[Explicit simulated trigger terminal evidence](2026-11-28-apexes-trigger-terminal-evidence-design.md) separates an actual stop-market IOC remainder from a rejected stop conversion. Both preserve original order identity and notify after committed fills; disappearance due to invalid depth is not terminal evidence. Live exchange terminal normalization remains unchanged.

[Lifecycle recovery qualification](2026-11-28-apexes-lifecycle-recovery-qualification-design.md) verifies the restart boundaries of managed fill and terminal delivery and the actual live loop after inventory recovery. Recovered baselines are not callback receipts; a running manager may not reset them. Durable strategy-state restore and callback atomicity remain separate work.

## Strategy state and warm-up (F04.05)

[Versioned strategy state contract](2026-11-28-apexes-strategy-state-contract-design.md) validates a bounded checkpoint against an independently selected identity, schema and observation cursor before invoking opt-in strategy restore. It does not provide durable publication, automatic runner integration or atomic callback recovery.
