# F04.05 — Independent per-fill third-asset valuation evidence

## Scope

Offline diagnostic reconciliation of retained fee aggregates with the
exact selected journal and independently supplied per-fill historical
conversion observations. No runtime accounting, execution, recovery
authorization or state application changes.

## Contract

First qualify existing independent wallet and position evidence and
the captured basic ledger. Reopen the exact selected journal and durable
attachment. Versioned third-asset evidence binds the exact coherent
boundary and owner. Require at most 8,192 rows, unique and sorted by
fill ID, and exactly one row for each nonzero third-asset fee. Reject
unused rows, including rates supplied for direct or zero fees.

Require exact fill, venue, symbol, fee asset and quote identity, positive
rates, observation time no later than the fill and no more than five
minutes earlier. Retain the existing direct base/quote valuation
semantics. Compare independently calculated per-asset quantities and
historical valuation subtotal using exact checked decimal arithmetic.
Require all traded symbols and exact instrument metadata, zero missing
valuations and no external fee entries, consistent with the restricted
complete-wallet qualification. The old direct-fee API remains restricted
to direct fees.

## Limits

The caller supplies observations; this API does not authenticate their
provenance, prove observation completeness, prove they were available to
the live process, or establish the individual conversion decisions the
runtime made. Equal aggregate subtotals do not establish per-fill
equality. External fee ownership, mark valuation, actual per-fill
retained decisions, fee-bearing realized P&L, ledger restoration and
risk restoration remain unqualified. No live orders or authorization
to resume execution.

## Qualification

New offline unit test checks mixed base, quote and third-asset fees
with independently expected subtotal and refuses altered rate, stale or
future observations, wrong quote, duplicate/unused/missing rows, wrong
boundary/owner and external fee entries. Existing direct-fee unit tests
remain intact. Extend isolated coherent-stop integration with the new
API against the actual reopened fee-free attachment and explicit empty
third-asset evidence. Nonzero third-asset evidence is tested offline,
not in an end-to-end database fixture.

Rust 1.99.0, locked dependencies, serial tests: 211 OMS/EMS/live tests
(97/32/82) against disposable PostgreSQL, 822 core/risk (78 risk), and
60 engine tests passed: 1,093 total, zero failures, six external tests
ignored. Workspace all-target Clippy with warnings denied, formatting
and diff checks passed. Disposable database stopped and removed.
Full workspace build/test, independent review, remote CI and real-service
qualification remain pending. No real orders or production DB operations.

## Remaining work

Next qualify actual retained per-fill valuation decisions. Complete
valuation history, fee-bearing realized P&L, external fee ownership,
actual risk/ledger state application, full runtime atomicity, source
completeness and authoritative acquisition remain open. Preserve the
one-time startup restriction and do not replay completed callbacks.
