# F01.05 — Live session health

## Objective and scope

Expose and enforce session health independently of market-event cadence.
Build on the existing OMS kill switch, durable submission guard, EMS
outage latch, and market-data recovery. Keep the unified event loop and
legacy standalone OMS, paper and backtest behavior unchanged.

## Managed health lifecycle

The live engine opts into a managed KillSwitch. Its process-local
health state is protected by a short lock that never spans I/O.

- Initializing: configure dependencies and establish user-data stream.
- Reconciling: recover exchange orders, balances and positions, initialize
  EMS and subscribe to market data.
- Running: all configured venue/symbol pairs have produced usable market
  events. Only the startup path can make this transition. PID readiness
  is published afterward.
- Degraded: a fault or operator halt engages the shared kill switch,
  blocks ordinary OMS/EMS submissions and requests cancellation.
- Halted: the cancellation sweep has completed. This is not proof that
  all positions are flat; an explicitly requested flatten has its own
  outcome and safety checks.

The readiness gate is separate from the emergency cancellation flag:
starting a session must not request cancellation solely because
initialization is incomplete. Recovered EMS background schedulers wait
for Running before dispatch. Ordinary OMS submissions recheck readiness
after persistence and before calling the exchange; an already-dispatched
request may still finish and must be reconciled. Existing explicit
flatten behavior is retained; its unresolved-order checks remain active.

Managed live engines use a 30-second staleness limit if no other limit
is configured. A market-data event from one symbol cannot establish
readiness for all instruments. The existing timestamp, gap and depth
invalidation safeguards remain in effect.

## Fault handling and recovery

Existing market-data and EMS fault signals engage the same gate.
Managed Binance user-data stream disconnections trigger the halt before
reconnection, not only upon resubscription. OMS submission uncertainty,
persistence failures, missing/untracked orders observed by reconciliation,
order/fill query failures and unresolved submissions trigger the same
conservative gate. Known terminal duplicates in the push stream are not
interpreted as a new fault merely because an order is no longer open.
The first halt reason is retained; reconnecting or a successful subsequent
query cannot erase it.

For this iteration there is no in-process recovery authorization after
any managed halt, including operator-requested halt. ResumeTrading is
refused and audited by the server; the console explains that reconciliation
and restart are required. Pause/Resume remains distinct from trading
halt. Before restart operators must reconcile the exchange, outstanding
orders, fills, balances and EMS records. Startup recovery and the market
readiness checks run again on restart. This health latch is process-local,
not a persistent recovery approval or a substitute for F02.01/F02.02
ledger and inventory work. Do not restart blindly. Full durable operator
recovery approval and broader fault drills belong to later enhancements.

## Status and compatibility

The authenticated /session/ws Status envelope adds optional health_state
and health_reason fields and resume_blocked (default false when absent).
Status changes are polled independently of market snapshots. /healthz
remains liveness, and /readyz returns 503 when startup is incomplete or
trading is halted. The console displays all five health states and
suppresses the misleading resume button for managed halted sessions.
Legacy clients can ignore the added fields. Legacy non-managed callers
retain release semantics.

## Verification

Tests use loopback WebSockets, mock exchange and isolated PostgreSQL.
Cover startup gating across all symbols, health transitions, fault latch,
recovered scheduling readiness, uncertain submission, disconnect and
resubscription, and server denial of resume without market ticks.
Run the workspace build, formatting, Clippy, serial tests including
the CI PostgreSQL group, and remote CI before merge. No live exchange
credentials or orders are required.
