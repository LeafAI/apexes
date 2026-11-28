# F04.05 — Combined read-only fee evidence

## Scope

Combine the existing offline local fee classification, exact historical
fee valuation decisions, realized attribution and closing mark checks.
No trading or accounting behavior, recovery gate, state application,
or startup authorization changes.

## Contract

Require the exact selected account journal and independent wallet and
closing owned-position evidence before reopening the durable attachment.
First compare independent predecessor-bound opening local allocation,
per-fill fee classification and tracked closing balances. Only then
compare exact historical fee totals, complete retained decisions,
independent causal per-fill rates and metadata, permitting external
classification already verified by that separate comparison. The
existing direct, aggregate and exact-decision APIs retain their
non-external restrictions. Compare basic realized entries and owned
cost basis, gross shadow attribution, historical expense and independent
closing marked fee totals using the existing exact arithmetic checks.
The full-account cash-flow evidence still includes every fee, while
external fee expense is counted once in shadow net attribution.

## Qualification and blocker

A new offline component test uses actual in-memory ledger evidence
for a mixed quote and external third-asset fee history. Independently
expected gross realized P&L is 10, historical expense 4, derived net
realized P&L 6, closing fee mark 5 and revaluation 1 (quote units).
Verify the stricter old comparison still refuses external fees; reject
changed historical rate, incorrect opening allocation, altered realized
P&L and mismatched closing mark. Extend the isolated coherent-stop
integration with the combined API on the reopened fee-free attachment.

The nonzero external-fee test explicitly also verifies that the existing
owned-position recovery gate rejects a fee in another scoped instrument's
base. Actual conversion-price capture currently relies on instrument
metadata; a conversion-only instrument can therefore conflict with
the owned-base restriction. Component equality does not qualify this
history through the full checkpoint API. Do not bypass that restriction
or infer ownership or funding provenance from the comparison.

Rust 1.99.0, locked dependencies, serial tests: 218 OMS/EMS/live tests
(97/32/89) with disposable PostgreSQL, 841 core/risk/research (including
17 research and 78 risk), and 60 engine tests passed: 1,119 total,
zero failures, six external tests ignored. Workspace all-target Clippy
with warnings denied, formatting and diff checks passed. Disposable
PostgreSQL stopped and removed. Full workspace build/test, independent
review, remote CI and real-service qualification remain pending.

The merged parallel F05.01 research implementation is preserved; remove
the duplicate stale active-work note in FEATURES.md and retain the
research progress note. Do not change its authoritative feature status.

## Remaining work

Next qualify offline conversion-only evidence scope without changing
the existing owned-position gate. Nonzero external-fee end-to-end
qualification, authoritative funding and market provenance, complete
historical valuation, actual ledger/risk state application, full runtime
atomicity and repeatable restart remain open. Preserve startup
restrictions and never replay completed callbacks.
