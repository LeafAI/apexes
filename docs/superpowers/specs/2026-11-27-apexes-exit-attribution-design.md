# F03.05 — Closing execution origin attribution

Historical replay records accepted fills' execution paths in occurrence
order at the authoritative shared ledger application site. Distinguish
direct strategy (including delayed arrivals), resting execution, EMS
child execution, margin liquidation and extreme-market de-risking.
Resting execution identifies the delivery path, not a strategy's
subjective exit motive (stop-loss vs take-profit is not inferred).
Record only accepted fills; no allocation or rejection is a closure.

Use the existing weighted-average signed session inventory and
proportional fee allocation to derive each reducing slice, including
partial closes and flips. Attach fill ID, timestamp, reason, reducing
quantity, allocated entry/exit fees and gross/net realized P&L.
Summarize by origin in per-symbol quote units. Unsupported economics
remain unavailable as in existing execution performance. No separate
inventory model, no modification to order scheduling or risk policies.
Historical runner opts in; live/paper retain the original path with
no additional history. Old reports deserialize without attribution.

## Verification

Three new regressions exercise an attributed flip, extreme-market
de-risking, and resting limit plus event-time TWAP child closes.
Existing ordinary perpetual close and margin liquidation regressions
assert exact fill linkage, reason, fees and realized P&L. The
legacy Trend Guard economic-report normalization maps random fill
identities to occurrence indices in the new closing records, retaining
the referential integrity and economic comparisons rather than
dropping the new attribution. Deterministic report fingerprint
regressions still pass without normalization. An initial legacy TWAP
fixture did not produce child fills under wall-clock scheduling;
the causal fixture uses explicit existing event-time latency instead.
No scheduler or risk policy changed.

Rust 1.99.0 locked offline serial 409 nonignored tests passed
(394 backtest, 15 paper); three external tests ignored. All-target
Clippy with warnings denied for backtest, paper and live-trading,
formatting/diff checks passed. Full workspace CI and independent
review pending. Delivery-path attribution does not infer stop-loss,
take-profit or other user-defined motives. Actual exchange orders,
accounts and production databases were not accessed.
