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

[Scoped Drift checkpoint codecs](2026-11-28-apexes-drift-state-codec-design.md) extend the versioned checkpoint contract with explicit scoped-only opt-in, exact bounded native indicator accumulators, state values, causal time gates and parameter state. No arbitrary interpreter globals or automatic runner restoration are claimed. Durable publication and warm-up remain separate slices.

[Durable checkpoint publication and recovery selection](2026-11-28-apexes-durable-checkpoint-design.md) uses the existing exclusive managed inventory lease to publish a validated head transactionally and select only an independently supplied exact receipt. A checkpoint receipt does not establish an atomic strategy/OMS boundary; runner integration and independently durable receipt selection remain separate work.

[Verified live runner restoration](2026-11-28-apexes-verified-runner-restore-design.md) connects independent receipt files to the existing live runner using a durable one-time claim restricted to an execution-history-free managed schema. This is a conservative first launch from a qualified checkpoint, not repeatable post-execution crash recovery. Atomic observation/execution/callback checkpoint publication and causal no-order warm-up remain open.

[Coherent stop checkpoint publication](2026-11-28-apexes-coherent-stop-checkpoint-design.md) adds atomic strategy-envelope and completed direct-Spot lifecycle-prefix evidence at an explicit quiescent stop, plus read-only exact journal verification. It does not yet authorize repeatable post-execution restart. Independently durable coherent receipt selection, authoritative reconciliation, ledger/risk/observation continuity and EMS callback recovery remain separate qualification work.

[Independent coherent receipt selection and recovery preview](2026-11-28-apexes-coherent-recovery-preview-design.md) preserves the independently selected original startup receipt and acknowledged coherent stop receipt in a durable file, verifies the exact bound journal, reconstructs owned Spot inventory against supplied wallet evidence, and validates isolated strategy restore without authorizing startup. Authoritative terminal execution re-verification and complete risk/ledger/observation continuity remain mandatory before any repeatable restart.

[Offline terminal execution evidence validation](2026-11-28-apexes-terminal-evidence-validation-design.md) compares independently supplied complete execution evidence against the selected coherent journal. Evidence acquisition and freshness are not established by this comparison, and restart remains prohibited.

[Supplied recovery evidence window](2026-11-28-apexes-recovery-evidence-window-design.md) validates asserted observation intervals and complete account-wide open-order coverage against a selected durable journal. It does not establish authoritative evidence acquisition or permit repeatable startup.

[Offline wallet continuity preview](2026-11-28-apexes-wallet-continuity-design.md) qualifies supplied wallet balances within the existing terminal/open-order evidence window and previews fee-aware owned Spot inventory. It does not prove actual exchange provenance or authorize a restart.

[Offline closed-account ledger continuity](2026-11-28-apexes-ledger-continuity-design.md) checks independently supplied opening and closing wallet evidence against exact durable Spot cash flows and fees without authorizing a restart. Full valuation, risk state, observation continuity and authoritative evidence provenance remain unqualified.

[Offline risk-state inspection contract](2026-11-28-apexes-risk-state-inspection-design.md) retains versioned accepted-order budget and circuit-breaker state evidence independently for comparison with the selected coherent checkpoint. Atomic risk capture, runnable restoration, observation continuity and authoritative evidence collection remain open.

[Offline observation receipt inspection](2026-11-28-apexes-observation-continuity-design.md) compares independently supplied ordered completed market records against retained cursor/scope/clock-bound digests and the exact selected checkpoint journal; actual runtime capture and gap-free source provenance remain unqualified.

[Actual completed observation capture](2026-11-28-apexes-observation-capture-design.md) retains bounded loop-local observation records and returns a validated receipt only after the existing coherent stop publication acknowledges success. Atomic durable observation/risk persistence and actual risk-monitor capture remain open.

[Actual registered risk-monitor inspection](2026-11-28-apexes-registered-risk-capture-design.md) captures supported registered slots with explicit unsupported coverage before coherent checkpoint publication. The acknowledged in-memory result is not an atomic durable or complete risk witness.

[Atomic durable observation and partial-risk evidence](2026-11-28-apexes-durable-capture-design.md) persists the actual opt-in capture in the same coherent checkpoint transaction, binds its digest to the independently selected receipt, and exposes read-only validation after reopening. Complete monitor coverage, external shared-state atomicity and restoration remain open.

[Additional registered monitor evidence](2026-11-28-apexes-monitor-coverage-design.md) retains list configuration, self-trade prevention version and margin-monitor threshold/trip state with explicit legacy and unsupported position coverage. Market-condition and margin-tier evidence remain open.

[Market-condition and margin-tier evidence](2026-11-28-apexes-market-risk-evidence-design.md) retains bounded actual volatility state and configured margin tables in the existing durable observation/risk attachment. Shared identity, atomicity and restoration remain unqualified.

[Shared built-in monitor identity and simultaneous inspection](2026-11-28-apexes-monitor-identity-design.md) captures actual alias topology with all distinct registered built-in shared monitor locks held together. Full runtime/ledger atomicity and restoration remain unqualified.

[Offline risk candidate compatibility](2026-11-28-apexes-offline-risk-contract-design.md) validates independently configured pristine candidates against retained evidence without applying state or changing startup authorization.

[Offline closing position evidence](2026-11-28-apexes-offline-ledger-contract-design.md) compares independently supplied strategy-owned Spot quantities and cost basis with the selected journal and verified cash-flow continuity preview, without ledger mutation or startup permission.

[Actual basic Spot ledger evidence](2026-11-28-apexes-ledger-capture-design.md) retains the loop-owned ledger scope, balances, positions and realized P&L in the existing durable attachment; no state is applied.

[Captured basic ledger reconciliation](2026-11-28-apexes-ledger-reconciliation-design.md) compares actual retained basic ledger balances and positions against qualified independently supplied evidence without state application or startup authority.

[Independent fee-free gross realized P&L reconciliation](2026-11-28-apexes-realized-pnl-evidence-design.md) checks the actual captured gross realized map against the exact selected restricted Spot journal, without modifying accounting state.

[Retained Spot fee-valuation evidence](2026-11-28-apexes-fee-valuation-evidence-design.md) captures actual bounded attribution aggregates and retained conversion windows, preserving missing-valuation counts without applying state.

[Independent direct Spot fee reconciliation](2026-11-28-apexes-direct-fee-reconciliation-design.md) verifies direct base/quote fee quantities and historical valuation subtotals against the exact selected journal; third-asset valuations remain unqualified.

[Independent per-fill third-asset valuation evidence](2026-11-28-apexes-third-asset-fee-evidence-design.md) compares the exact selected journal and supplied causal per-fill rates against retained fee totals and valuation subtotals; actual runtime conversion decisions and provenance remain unqualified.

[Actual per-fill valuation decision evidence](2026-11-28-apexes-per-fill-valuation-evidence-design.md) retains bounded actual attribution decisions and validates local aggregate consistency; independent per-fill reconciliation and external provenance remain open.
