# F04.05 — Isolated cold-start Spot ledger application

## Contract

Qualify exact independent opening allocation, closing wallet and position
evidence, terminal journal, fee decisions and revaluation, and retained
observation availability before constructing a private PortfolioLedger.
The existing coherent selection binds the exact persisted journal and
capture. Require complete actual fill observation prefixes; legacy
captures without them are refused. Start with the explicitly selected
opening balances, no owned positions or attribution state, and no
pre-start price history. Apply each durable fill once in retained
application order, advancing only the observed Trade prices at its
recorded prefix. Consume remaining completed observations after the
last fill. Never substitute the final price window for earlier availability.

Compare the full resulting ledger evidence, including explicit zero
balances, flat position entries, realized P&L, fee attribution accounts,
individual decisions and retained conversion prices, with the exact
captured evidence. Return success only, not the ledger or an order
executor. Discard on any mismatch or failed application. The existing
independent exact arithmetic and ownership checks are mandatory; an
additional conservative input magnitude/precision envelope restricts the
offline application to bounded numeric inputs. This is not an extension
of live ledger arithmetic or a proof of unrestricted Decimal safety.

## Qualification boundary

This is offline cold-start reconstruction, not automatic continuation
of an arbitrary previously funded ledger. No change to startup claims,
risk controls or live submission. Authoritative collection and
provenance, nonzero external-fee end-to-end qualification with conversion
instruments, actual live application, repeatable post-execution restart
and callback/EMS recovery remain open.

## Verification

The coherent-stop PostgreSQL fixture qualifies the reopened durable
capture and independent evidence, refusing a changed opening allocation.
A separate focused test reconstructs actual fee-bearing ledger evidence,
including external fee classification, and rejects changed closing state,
duplicate application identifiers and missing prefixes. The fee-bearing
unit fixture is component coverage only, not an independent end-to-end
external funding proof.

Rust 1.99.0, locked dependencies: 228 OMS/EMS/live tests, 893 core/risk/
research/engine tests, and one additional focused test passed (1,122
total, zero failures, four external ignored). Strict workspace Clippy,
formatting and diff checks passed; disposable PostgreSQL removed. Full
workspace build/test, independent review, remote CI and real-service
qualification remain outstanding.

## Next

Qualify a durable post-execution transition witness binding the exact
selected journal, accounting and risk/observation evidence without
consuming or resetting a startup claim; authoritative collection and
operational live restart remain prerequisites.
