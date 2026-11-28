# F04.04 — Locally applied fill notifications

Strategy gains optional on_fill(&FillOutcome), default no-op and
forwarded through Box<dyn Strategy>. Drift supports an optional
top-level on_fill(fill) function. A defined noncallable entry point
fails construction. Returned values are ignored and script errors
logged; global changes are not rolled back. The callback is synchronous,
notification-only, without new submission or scoped-state capabilities.

The shared loop delivers successful existing fill batches after local
accounting, in their existing occurrence order. Direct fills follow
on_order_outcome(Returned); resting, EMS, liquidation and de-risking
paths report their actual delivery source. Supplemental delivery uses
the newly applied suffix of all_fills after the existing acknowledgement
call and equity calculation. Refused/duplicate supplemental executions
are not added to that suffix. A successful callback does not attest to
durable acknowledgement, exchange completeness, or order terminality.
A failed batch or loop termination may prevent delivery.

A session-local set suppresses repeated notification IDs across all
paths, with a warning. It grows with distinct executions, as the report
history does; it is not persisted. Deduplication is only for callbacks,
not a change to ledger, reconciliation or risk accounting. No claims of
exactly-once processing across restart or replay of missed notifications.
The existing pause semantics and order admission remain unchanged.

Drift fields: source (direct, resting, ems, liquidation, derisk,
supplemental), at, filled_at (RFC3339 timestamps), fill_id, order_id,
symbol, venue, side, price, quantity, fee, fee_currency, leverage,
margin_mode. at is the existing market-event logical timestamp or the
supplemental arm's current UTC observation after acknowledgement.
filled_at is preserved from the actual execution. Numeric values use
the existing f64 representation; native fills retain Decimal. IDs are
strings. Unspecified fee currency remains null, not inferred from the
instrument. A fill is an individual execution, not proof that the
order is fully filled; order terminality requires separate evidence.
No fabricated cumulative order quantity or EMS parent association.

Verification: two new regressions, including a sealed native/Drift
replay with direct, two resting partial executions sharing an order ID,
and subsequent fill-driven exit. Fill IDs and occurrence order match
the report, callback return intents are ignored, and repeated runs
match notification records and canonical report fingerprints. A
supplemental accounting regression confirms refused and duplicate
executions do not notify, including repeat delivery from another path.
Existing EMS and de-risking replay tests now assert callback provenance
against report fill IDs. The initial fixture incorrectly assumed
simulated fee_currency was USDT; corrected to assert the actual null
and exact modeled fee rather than fabricating currency metadata.

Rust 1.99.0 locked offline serial: 603 tests passed (436 backtest,
122 strategy, 15 paper, 25 indicators, 5 loader); three external
tests ignored. All-target Clippy for strategy, loader, backtest, paper,
live-trading and engine passed. Formatting and diff checks passed.
Workspace CI and independent review pending. Live supplemental
acknowledgement/service qualification, durable delivery, explicit
cancellation/expiry, terminal status and EMS parent lifecycle remain
open. No live orders or production databases used.
