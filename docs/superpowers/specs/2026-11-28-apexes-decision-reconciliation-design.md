# F04.05 — Exact retained per-fill valuation reconciliation

## Scope

Read-only independent comparison of actual retained Spot valuation
decisions against the exact selected journal and caller-supplied causal
conversion evidence. No changes to accounting, execution, recovery
authorization, or state application.

## Contract

Preserve the existing aggregate-only third-asset reconciliation API for
legacy evidence. The stronger inspection method first qualifies wallet,
position, basic ledger and independent fee aggregates using the exact
selected coherent receipt and journal. It requires actual retained
decision evidence, with exactly one record per unique journal fill,
including zero-fee fills. Refuse missing, extra or duplicate identities.

Match venue, symbol, effective fee asset, execution timestamp, execution
price and fee quantity. Require the restricted non-external fee contract.
Compare exact source and valuation: zero, direct base at execution price,
direct quote at fee quantity, or the exact independently supplied
third-asset observation time and rate. Use checked exact arithmetic.
The earlier causal window and complete rate coverage checks remain
mandatory. Missing or caller-provided source classifications are not
qualified by this stricter comparison.

Match membership by fill identity, not an assumed timestamp ordering.
This does not prove the runtime processing order or reconstruct
complete historical market availability. No actual state is applied.

## Qualification

New offline unit test uses mixed direct and third-asset fees and a
zero-fee fill. Swapping two third-asset rates preserves the aggregate
subtotal and passes aggregate reconciliation but fails exact decision
reconciliation. Refuse altered timestamps, caller-provided sources,
unknown fill IDs, omitted zero-fee decisions and missing legacy
decision evidence. Extend the isolated coherent-stop integration with
the stricter comparison on the actual reopened fee-free attachment.
Nonzero third-asset cases remain offline unit-tested, not end-to-end
database qualified.

Rust 1.99.0, locked dependencies, serial tests: 212 OMS/EMS/live tests
(97/32/83) against disposable PostgreSQL, 824 core/risk (78 risk), and
60 engine tests passed: 1,096 total, zero failures, six external tests
ignored. Workspace all-target Clippy with warnings denied, formatting
and diff checks passed. Disposable database stopped and removed.
Full workspace build/test, independent review, remote CI and real-service
qualification remain pending.

## Remaining work

Next qualify independent fee-bearing realized P&L. Complete historical
valuation coverage, external fee ownership, actual risk/ledger
application, runtime atomicity and authoritative acquisition remain
open. The supplied evidence is not authenticated by this comparison.
Preserve existing startup restrictions; do not replay completed
callbacks or grant execution authority.
