# F04.05 — Offline closed-account ledger continuity

## Scope

Authoritative acquisition remains blocked. Advance the independent
offline accounting comparison, not the live ledger or trading path.
The one-time startup claim and all existing execution safeguards remain
unchanged. This preview does not establish evidence provenance.

## Opening and closing evidence

OpeningWalletEvidence binds a separately supplied wallet observation
to the original exact predecessor receipt, owner fingerprint and
complete Binance Spot account coverage. The caller must retain that
evidence independently; deriving it backward from the closing balance
would invalidate the comparison. The opening interval starts no earlier
than the predecessor observation cursor and ends no later than the
earliest durable order creation, fill timestamp or terminal collection
start. Equal timestamps are permitted; they do not establish causal
ordering independently. The opening snapshot need not be within the
closing 60-second inspection window.

Require the same explicit asset set at both endpoints, at most 1,024
assets, nonnegative opening holdings and zero opening locked funds.
The existing closing wallet validator checks relevant and historical
fee assets, account coverage, scope, names, amounts, timestamps and
locked balances. Zero balances must be explicit rather than inferred.

## Accounting comparison

Use only the selected exact durable journal, in the established Spot
recovery order, and supplied instrument scope. For each fill, a buy
credits base and debits quantity times price in quote; a sell debits
base and credits quote. Debit each nonzero commission from its actual
asset, whether base, quote or third asset. Check nonnegative holdings
after all legs of each fill. Refuse unsupported scope, invalid execution
amounts and missing asset attribution.

Use normalized decimal mantissas with checked i128 arithmetic and
exact scale conversion. Reject overflow or unrepresentable precision
rather than approximate, including conservative refusal when an
intermediate cannot be represented. Compare every closing asset
balance exactly. No deposits, withdrawals, transfers, rebates or
unexplained movements are inferred. Equal endpoints cannot exclude
offsetting external movements; this is only a closed-account
consistency check against supplied evidence.

CoherentRecoverySelection::inspect_ledger_continuity reads the
independently selected owner-bound journal, runs the comparison,
then delegates to the existing terminal/open-order/wallet and isolated
strategy-state preview, including final journal re-verification.
No historical callbacks, order mutations, account requests, risk
state changes or claim transition occur. No startup authorization
or live balance seeding is produced. Historical cost basis, fee
valuation, account cash-flow provenance, full ledger statistics,
risk counters and observation continuity are separate requirements.

## Qualification

Five unit tests cover both trade sides and all three fee asset
categories, unexplained balance changes, missing assets, predecessor
and interval mismatches, negative intermediate holdings, precision
and overflow. Extend the coherent-stop PostgreSQL integration to
compare independent synthetic opening/closing evidence and reject
a one-unit unexplained quote balance change.

Rust 1.99.0, locked dependencies and serial tests: 203 OMS/EMS/live
tests (97/32/74) passed with isolated PostgreSQL, 733 core and 60
engine tests passed (996 total), zero failures, six external tests
ignored. Workspace all-target Clippy with warnings denied,
formatting and diff checks passed. Disposable database stopped
and removed. Full workspace build/test, independent review,
remote CI and real-service qualification pending. No real orders,
live exchange calls or production database operations.

## Remaining work

Authoritative collection remains blocked; supplied observations
cannot prove actual provenance or absence of offsetting movements.
Next independent task is offline risk/observation continuity.
Full ledger/risk/observation continuity, authoritative acquisition
and durable run transition must all be qualified before repeatable
post-execution restart. Periodic capture, unresolved/EMS recovery
and causal no-order warm-up remain open.
