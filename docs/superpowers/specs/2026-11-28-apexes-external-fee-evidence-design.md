# F04.05 — Independent local external-fee classification

## Scope

Read-only comparison of local fee classification and tracked asset
coverage. This is not proof of external funding provenance, a general
allocation model, or authority to restore execution state.

## Contract

First require independent opening and closing account wallet
continuity, exact selected terminal journal, and closing owned
position evidence. Independently supplied opening tracked asset
evidence binds the predecessor receipt, account owner and opening
wallet observation interval. Its canonical, unique asset entries
must equal the corresponding full free account balances; partial
allocation of an asset is unsupported. An explicitly tracked zero
balance is distinct from an omitted asset.

For a bounded exact journal, require a corresponding retained fee
decision for every fill in strictly increasing global execution
timestamp order. Refuse ambiguous order, missing decisions, duplicate
fill IDs and mismatched fill identity, amount, fee currency, or
classification. A fee asset is local if it is the execution
instrument base or quote, or a balance key tracked before the fill.
Otherwise it is external to the local ledger. This classification
is relative to local tracked coverage, not legal ownership of assets.

Independently calculate local closing balance entries from the supplied
opening allocation using exact checked decimal arithmetic and the
historical fill cash flows. Add base and quote entries on execution;
debit only fees classified as local. Refuse negative balances. Compare
the exact resulting local balance map to the captured ledger, and
each tracked closing amount to the independently qualified full account
wallet. Other account assets can remain outside the local ledger.
The separate full account continuity check still accounts for every fee.

## Qualification

The new offline unit test uses actual in-memory captured ledger evidence
from synthetic base, quote and third-asset fee fills. It verifies both
tracked and untracked third-asset classifications and closing balances.
Refuse a self-consistent alteration of the retained classification,
absent decisions, wrong owner, out-of-window opening evidence,
incorrect opening balance and tied fill timestamps. Extend the
isolated coherent-stop integration to compare the reopened fee-free
attachment with independently retained opening allocation.

Rust 1.99.0, locked dependencies, serial tests: 215 OMS/EMS/live tests
(97/32/86) against disposable PostgreSQL, 824 core/risk (78 risk),
and 60 engine tests passed: 1,099 total, zero failures, six external
tests ignored. Workspace all-target Clippy with warnings denied,
formatting and diff checks passed. Disposable PostgreSQL stopped
and removed. Full workspace build/test, independent review, remote
CI and real-service qualification remain pending.

## Remaining work

Next qualify independent mark-to-market fee revaluation. Nonzero
external-fee scenarios are offline unit-tested, not end-to-end database
qualified. Combined external-fee valuation/P&L comparison and
authoritative funding provenance remain open; the earlier stricter
non-external fee comparison APIs are unchanged. No fee or execution
decisions, runtime state application, production data or startup
authorization are changed.
