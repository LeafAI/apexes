# F02.01 — Persistent spot position recovery

## Objective

Preserve strategy-owned spot inventory and weighted average entry price
across a process restart. An account wallet is not a strategy position:
pre-existing holdings and deposits must never be assigned to a strategy
merely because it trades the corresponding symbol.

## Ownership boundary

The live engine binds a dedicated PostgreSQL schema to the Binance network,
a SHA-256 REST API-key fingerprint (never the raw key or secret), the full
strategy reference, and the sorted symbol/base/quote instrument identities.
The ephemeral session ID is not part of the binding. A different credential
or strategy, including a change to the instrument set, cannot silently
reuse the existing history. Credential rotation requires an audited
migration of the owner binding. The fingerprint identifies credentials,
not a permanent exchange account ID; independent schemas/accounts are not
coordinated by this feature.

A schema-scoped PostgreSQL advisory lock on a detached connection prevents
a second managed engine from claiming the same schema. The OMS checks
that connection on submission and reconciliation and latches the session
if the lease is unavailable. This is a single-owner deployment guard,
not distributed exchange-account fencing. An exchange request already
in flight may finish during a database failure. Keep external writers and
other engines off the dedicated live schema and account.

The singleton owner record may be created automatically only when the
order and EMS parent history is empty. Existing unbound records remain
intact and startup refuses to guess their owner. Legacy adoption requires
an audited migration against the actual exchange order and trade history;
no automatic migration/import or owner reassignment is provided here.

## Startup sequence

1. Bind the schema before OMS reconciliation of its historical client IDs.
2. Reconcile local open orders against exchange truth; reject unknown,
   foreign and unresolved exchange orders using existing recovery.
3. Obtain a Spot wallet snapshot retaining free and locked amounts.
4. Read persisted executions in exchange-time order (numeric Binance trade
   IDs break same-symbol timestamp ties). Validate symbol/venue/side against
   parent orders, Spot asset class, and recorded versus summed executed
   quantities. Existing exchange-trade deduplication remains in force.
5. Rebuild per-symbol quantity and weighted average entry price. Base fees
   reduce acquired inventory on buys and consume inventory on sells.
   Never infer an opening position from account balances; reject a sell
   without sufficient attributable inventory.
6. Validate owned quantity against the free base balance and seed the
   existing PortfolioLedger with recovered positions and authoritative
   wallet balances, without applying historical fills to balances a
   second time. Readiness still requires F01.05's market-data gate.

Until F02.02 provides stronger fill delivery/checkpoints and F02.05
provides continuous reservation accounting, the initial recovery path
requires a quiescent account: no remaining working orders and no locked
balance in any configured base or quote asset. Cancel/reconcile explicitly
before startup; the engine does not submit cancellation or trading
requests solely to make this check pass. Do not treat locked funds as
spendable. Multiple pairs sharing a base or using another traded base as
quote are rejected by this initial ownership model. Cross-symbol base
fees are not silently attributed. Accurate quote/third-asset fee and
P&L attribution, durable online discrepancy detection, pending-order
reservations and portfolio-wide coordination are subsequent features.

## Verification

Use unit tests for ownership versus account balances, weighted basis
and base fees (compared with the existing live ledger), invalid sells,
inventory shortfall, working orders and locked funds. Use isolated
PostgreSQL schemas for restart and persisted fill ordering, duplicate
execution deduplication, conflicting owner/lease, legacy refusal and
inconsistent history. Verify the gateway retains separate free and
locked amounts using WireMock. No live database or exchange order is
needed. Full workspace build, serial tests, formatting, Clippy and
remote CI remain merge gates.
