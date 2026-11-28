# F04.05 — Offline wallet continuity preview

## Scope

The authoritative evidence collector remains blocked. Complete the independent
offline wallet validation layer without fetching balances, modifying the
account, resetting claims, or permitting another trading session. The
previous offline preview and one-time startup contract remain unchanged.

## Supplied evidence contract

WalletEvidence carries a caller-supplied observation interval, stable owner
fingerprint, venue and asset class, asserted complete account-wide coverage,
and explicit free/locked amounts by asset. Require matching selected owner
and Binance Spot scope. Require the wallet interval to follow the terminal
and open-order intervals and end no later than the independently supplied
inspection time. The entire sequence shares the existing inclusive 60-second
freshness ceiling; do not start a second independent freshness budget.

Limit the wallet map to 1,024 asset entries. Asset names must be nonempty,
uppercase ASCII alphanumeric and at most 24 bytes. Reject negative amounts
and any nonzero locked funds, including unrelated assets, conservatively:
an account asserted to be quiescent must not carry unexplained locks.
Require explicit balance entries (zero allowed) for every configured
base/quote asset and every historical nonzero fee asset. A nonzero fee
without valid asset attribution is refused. A historical fee asset may
have zero current holdings; this does not establish future fee funding.

## Integrated inspection

CoherentRecoverySelection::inspect_wallet_continuity verifies the exact
independent selection and durable journal, validates the existing supplied
terminal/account evidence window and complete terminal comparison, and
validates supplied wallet evidence. It then uses the existing isolated
strategy restore and fee-aware owned-inventory preview against the same
selected journal, including re-verification before returning.

Wallet balances are account-level amounts, not strategy ownership.
Reject inventory shortfall; do not debit or credit historical executions
again or infer a position from larger unrelated wallet holdings. The
preview does not establish historical wallet cash-flow continuity,
valuation, risk counters, quote/third-asset fee funding, or actual exchange
snapshot provenance. Returned data is not a startup or submission token.

## Qualification

Five unit tests cover relevant and nonzero fee asset coverage, explicit
zero balances, matching owner/scope, completeness, interval ordering and
freshness ceiling, invalid assets, negative amounts, unrelated locked
funds and unattributed fees. Extend the actual coherent-stop integration
test to validate the complete synthetic terminal/open-order/wallet
sequence after reopening PostgreSQL, reconstruct the owned BTC position
separately from larger wallet holdings, and refuse a missing quote
balance or owned-inventory shortfall.

Rust 1.99.0, locked dependencies, serial tests: 198 OMS/EMS/live-trading
tests (97/32/69) passed using isolated PostgreSQL; 733 core and 60 engine
tests passed (991 total), zero failures, six external core tests ignored.
Workspace all-target Clippy with warnings denied, formatting and diff
checks passed. Disposable cluster stopped and removed. No live exchange
requests, production database operations or real orders. Full workspace
build/test, independent review, remote CI and real-service qualification
remain pending.

## Remaining work

Authoritative read-only collection and independently verified provenance
and freshness remain outstanding. Supplied evidence cannot prove a
current exchange snapshot. Full ledger/risk/observation continuity and
durable run transition are unqualified. Maintain the one-time claim
restriction. Periodic checkpoint publication, unresolved and EMS
recovery, and causal no-order warm-up remain open.
