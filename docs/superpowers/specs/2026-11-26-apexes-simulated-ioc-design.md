# F03.02 — Bounded Spot IOC simulation

Add a serializable simulation execution policy: unrestricted market
(default, legacy benchmark) or bounded Binance Spot buy IOC with explicit
per-symbol historical SymbolRules and maximum trade-reference age.
No use of Instrument placeholder ticks, future trades, or live metadata.
Require complete rules for configured Binance Spot instruments; refuse
unsupported venues, asset classes and SOR in bounded mode. Register the
policy in the runner's existing dependency registry, so programmatic
callers and JSON launch files use the same engine. Live launch rejects
simulation policy. Reports record the selected policy and rules.

In the shared matching engine, keep latest causal positive trade per
venue/symbol. A bounded buy requires a nonfuture reference within the
configured age. Conform quantity downward, then derive a passive
tick-rounded limit price from the reference using SymbolRules. Check
the resulting quantity and notional; never increase quantity or price.
Walk only available asks at or below the cap, charge taker fees, and
expire the remainder. Preserve original market intent type and expose
the cap in the returned order price. Shared EMS children follow the
same policy; reject a child whose quantity would change (scheduler
accounts requested quantity). Sell market orders retain existing
behavior. Stop orders are unsupported in bounded mode, matching
managed live scope. Legacy unrestricted runs retain existing behavior.

This models price/quantity bounds, not live signed commission retrieval,
every exchange filter, account-wide inventory, strict book freshness,
or causal order-arrival latency. Historical rule provenance is supplied
by caller; no claim that rules reflect a particular historical date.
Test capped partial/zero/full execution, shared liquidity, fee math,
missing/stale/future trade, wrong venue, rule validation, launch and report
round-trip. No real exchange calls.
