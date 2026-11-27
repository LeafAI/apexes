# Apexes — Execution Plan

> **Purpose.** This file tracks *what we are working on now, what is next, and what is blocked*. It is the execution-level companion to [`FEATURES.md`](FEATURES.md).
>
> | Question | Authoritative file |
> |---|---|
> | Is feature X done? | [`FEATURES.md`](FEATURES.md) (single source of truth for feature status) |
> | What are we doing now / next, and in what order? | **this file** |
> | Why was it designed this way? | `docs/superpowers/specs/` |
> | Step-by-step tasks for one sub-project | `docs/superpowers/plans/` |
> | Long-term vision and roadmap (#0–#15) | [`docs/superpowers/specs/2026-07-28-apexes-vision-and-roadmap.md`](superpowers/specs/2026-07-28-apexes-vision-and-roadmap.md) |
>
> **Maintenance rules**
> 1. Never record feature status here — link to the `FEATURES.md` row ID instead (e.g. "row 9.3").
> 2. Update this file whenever a task finishes, a new plan starts, or priorities change.
> 3. Keep "Now" to one active plan. Finished plans move to "Completed" with their commit range.
> 4. Every plan-level item links to its plan file; per-task detail lives in that plan, not here.

_Last updated: 2026-11-27 (supervised raw depth capture qualification)_

---

## Now — Quantitative trading enhancement

Feature F00.01: preserve the original Trend Guard script and add deterministic
offline replay through the real Drift runtime and matching engine. See the
[feature plan](superpowers/plans/2026-11-25-apexes-quantitative-enhancement.md)
and [design](superpowers/specs/2026-11-25-apexes-quantitative-enhancement-design.md).
PR #11 passed all three CI jobs and PR #12 promoted the baseline to master.
F00.02 adds structured decision diagnostics (see [design](superpowers/specs/2026-11-26-apexes-trading-diagnostics-design.md)); PR #13 merged to integration after green CI, and PR #14 promoted it to master with green CI.
F01.01 adds exchange clock synchronization (see [design](superpowers/specs/2026-11-26-apexes-exchange-clock-design.md)); PR #15 merged to master after green CI.
F01.02 reliable submission (see [design](superpowers/specs/2026-11-26-apexes-reliable-submission-design.md)) passed CI and merged in PR #16. Corrected master 33e6be3 passed CI. F01.03 market-data recovery (see [design](superpowers/specs/2026-11-26-apexes-market-data-recovery-design.md)) passed CI and merged in PR #17. F01.04 EMS database decoupling (see [design](superpowers/specs/2026-11-26-apexes-ems-database-decoupling-design.md)) passed CI and merged in PR #18. F01.05 session health management (see [design](superpowers/specs/2026-11-26-apexes-session-health-design.md)) passed CI and merged in PR #19. F02.01 persistent spot position recovery (see [design](superpowers/specs/2026-11-26-apexes-spot-position-recovery-design.md)) passed CI and merged in PR #20. F02.02 authoritative fill reconciliation (see [design](superpowers/specs/2026-11-26-apexes-authoritative-fill-reconciliation-design.md)) passed CI and merged in PR #21. F02.03 spot fee and P&L attribution (see [design](superpowers/specs/2026-11-26-apexes-spot-fee-accounting-design.md)) passed CI and merged in PR #22. The durable conversion and lifetime journal follow-up passed CI and merged in PR #23. F02.04 protective exits (see [design](superpowers/specs/2026-11-26-apexes-protective-exits-design.md)) passed CI and merged in PR #24. F02.05 dynamic Spot rules and reservations passed CI and merged in PR #25 (see [design](superpowers/specs/2026-11-26-apexes-dynamic-spot-rules-design.md)). The durable reservation follow-up passed CI and merged in PR #26. Bounded market-buy execution passed CI and merged in PR #27. Fee-aware reservation follow-up passed CI and merged in PR #28. EMS parent-level capital allocation passed CI and merged in PR #29. F02.05 exact external fee-token funding and real-service qualification remain open. F03.01 [configurable execution fees](superpowers/specs/2026-11-26-apexes-configurable-costs-design.md) passed CI and merged in PR #30. F03.02 [causal liquidity and lifecycle](superpowers/specs/2026-11-26-apexes-causal-execution-design.md) passed CI and merged in PR #31. [Bounded Spot IOC simulation](superpowers/specs/2026-11-26-apexes-simulated-ioc-design.md) passed CI and merged in PR #32. [Strict depth integrity](superpowers/specs/2026-11-26-apexes-depth-integrity-design.md) passed CI and merged in PR #33. [Direct-order event-time latency](superpowers/specs/2026-11-26-apexes-event-time-latency-design.md) passed CI and merged in PR #34. [Causal EMS scheduling](superpowers/specs/2026-11-26-apexes-causal-ems-design.md) passed CI and merged in PR #35. Causal EMS fill preservation passed CI and merged in PR #36. [Direct marketable limit execution](superpowers/specs/2026-11-26-apexes-crossing-limit-design.md) passed CI and merged in PR #37. [Shared maker-queue accounting](superpowers/specs/2026-11-26-apexes-shared-queue-design.md) passed CI and merged in PR #38. [Triggered stop-limit crossing](superpowers/specs/2026-11-26-apexes-stop-limit-crossing-design.md) merged in PR #39. [Simulated EMS child transit](superpowers/specs/2026-11-27-apexes-ems-child-transit-design.md) merged in PR #40. [Independent simulated child-arrival risk](superpowers/specs/2026-11-27-apexes-ems-child-arrival-risk-design.md) merged in PR #41. [Spot transit capital reservations](superpowers/specs/2026-11-27-apexes-transit-capital-reservation-design.md) merged in PR #42. [Spot EMS parent capital ownership](superpowers/specs/2026-11-27-apexes-parent-capital-design.md) merged in PR #43. [Spot resting capital ownership](superpowers/specs/2026-11-27-apexes-resting-capital-design.md) merged in PR #44. [Leveraged capital reservations](superpowers/specs/2026-11-27-apexes-leveraged-capital-design.md) merged in PR #45. [Emergency reservation preemption](superpowers/specs/2026-11-27-apexes-emergency-reservation-design.md) merged in PR #46. [Position-aware margin reservations](superpowers/specs/2026-11-27-apexes-reducing-margin-design.md) merged in PR #47. [Offline queue-factor calibration](superpowers/specs/2026-11-27-apexes-queue-calibration-design.md) merged in PR #48 with synthetic qualification only. [Historical replay provenance](superpowers/specs/2026-11-27-apexes-historical-provenance-design.md) merged in PR #49. [Historical source coverage](superpowers/specs/2026-11-27-apexes-source-coverage-design.md) merged in PR #50. [Source-specific identity normalization](superpowers/specs/2026-11-27-apexes-source-identity-design.md) merged in PR #51. [Historical depth range verification](superpowers/specs/2026-11-27-apexes-depth-range-design.md) merged in PR #52. [Native depth range metadata](superpowers/specs/2026-11-27-apexes-native-depth-range-design.md) passed CI and merged in PR #53. [Opt-in supervised raw depth capture](superpowers/specs/2026-11-27-apexes-supervised-raw-depth-design.md) merged in PR #54 after centralizing Spot endpoints. [Durable collector receipts and isolated raw depth ingestion](superpowers/specs/2026-11-27-apexes-collector-receipts-design.md) merged in PR #55 with synced intent/commit journal, explicit run namespace, acknowledged ClickHouse batch writes, bounded channel and fail-closed shutdown. [Offline collector receipt reconciliation](superpowers/specs/2026-11-27-apexes-collector-reconciliation-design.md) implemented with strict journal/batch verification and receipt-bound replay before strategy execution. All 359 nonignored backtest tests and all-target Clippy passed; PR #56 merged after fixing the supervisor loopback allocation regression test. [Reconnect-aware segmentation](superpowers/specs/2026-11-27-apexes-reconnect-segments-design.md) implemented on the receipt-bound path: explicit invalidation gaps, independent native range chains, no cross-gap continuity claim, and strict replay policy promotion only after verification. All 365 nonignored backtest tests and all-target Clippy passed; PR #56 merged at 9953794; PR #57 merged at 700447f with the supervisor test fix. [Recorded-source qualification](superpowers/specs/2026-11-27-apexes-recorded-source-qualification-design.md) implemented: bounded read-only entire-run ClickHouse reconciliation, explicit qualification provenance and derived manifest/journal/events bundle. 402 targeted backtest/storage tests passed; PR #58 merged at dc7294e. [Operational capture CLI](superpowers/specs/2026-11-27-apexes-operational-capture-cli-design.md) implemented with explicit public market selection, bounded setup/capture/drain and durable completion reporting. 102 targeted market-data/storage tests passed; PR #59 merged at 56fc5b4. [Public capture and replay qualification](superpowers/specs/2026-11-27-apexes-public-capture-replay-qualification-design.md): real public Spot/USD-M transport probe passed after changing only raw Spot capture to TLS port 443 (190/186 advancing deltas); no-order replay CLI and storage-to-replay CI regression implemented. 519 targeted tests passed; PR #60 merged at f430366. Actual durable public capture remains blocked by unavailable isolated ClickHouse (temporary VM and native binary downloads timed out). Default trading provider unchanged. Production completeness and recorded maker-fill empirical calibration remain open. Advance independently implementable F03.04 while those evidence-dependent tasks are blocked: [Replay identity](superpowers/specs/2026-11-27-apexes-replay-identity-design.md) implemented opt-in deterministic matching order/fill IDs and canonical report fingerprint; 389 backtest/paper tests passed including ten repeated sealed-input runs with identical report digests; PR #61 merged at 5307474. [Deterministic EMS](superpowers/specs/2026-11-27-apexes-deterministic-ems-design.md) adds run-local parent IDs and requires explicit event-time latency for deterministic algorithmic admission; ten repeated two-instrument TWAP runs verified parent identities, child arrival order and report digests. 413 targeted tests passed; PR #62 merged at f9ecd62. [Stable portfolio ordering](superpowers/specs/2026-11-27-apexes-stable-portfolio-order-design.md) implements lexical symbol/asset ordering for liquidation, de-risking, funding and portfolio reductions; ten sealed simultaneous cross-margin liquidation replays reproduced exact fill order and report fingerprint. 393 backtest/paper tests passed; Clippy also passed for live-trading. Local live-trading tests blocked by absent test PostgreSQL (two flattening failures before suite timeout); PR #63 merged at 7744c85. [Mixed-event replay qualification](superpowers/specs/2026-11-27-apexes-replay-qualification-design.md) verifies bounded Spot and leveraged perpetual policies across all five event types, latency, maker/taker fees, risk refusal and funding; ten repetitions per profile reproduced the exact report fingerprint. The opt-in deterministic runner rejects bars without event-time latency or observation-time regressions before strategy execution. 419 targeted tests passed; PR #64 merged at 0848125. [Execution performance](superpowers/specs/2026-11-27-apexes-execution-performance-design.md) adds historical per-symbol realized gross/net closing-fill attribution, quote-denominated turnover and fees, entry-fee allocation, closing-count expectancy/win rate/profit factor, and explicit unavailable evidence for unsupported economics. 400 targeted tests passed; PR #65 merged at 6b3b594. [Benchmark and drawdown evidence](superpowers/specs/2026-11-27-apexes-benchmark-drawdown-design.md) adds opt-in verified single-Spot cash/buy-and-hold reference returns, excess returns, observed high-water episodes, recovery and censored drawdown duration; 404 targeted tests passed; PR #66 merged at 76aa5b2. [Historical funding attribution](superpowers/specs/2026-11-27-apexes-funding-attribution-design.md) records actual simulated ledger movements with scheduled/observed timestamps, mark/fallback price, rate and signed quantity; per-symbol quote income/expenses and net realized execution plus funding reconciled. 406 targeted tests passed; PR #67 merged at 7672ebb. [Closing execution origin attribution](superpowers/specs/2026-11-27-apexes-exit-attribution-design.md) records accepted fill paths and uses the existing realized accounting to classify reducing slices, allocate fees and reconcile per-origin P&L. 409 targeted tests passed; PR #68 merged at b97ccc5. [Explicit instrument-scoped strategy state](superpowers/specs/2026-11-27-apexes-symbol-state-design.md) introduces bounded Rust SymbolState and transactional event-scoped Drift data state keyed by venue and symbol. 521 targeted tests passed; PR #69 merged at 18720f6. [Scoped native indicators](superpowers/specs/2026-11-27-apexes-scoped-indicators-design.md) adds seven named event-instrument-scoped indicators, transactional data/indicator rollback, bounded configuration and single-symbol migration equivalence. 550 targeted tests passed; PR #70 merged at d4dfaeb. [Causal strategy time gates](superpowers/specs/2026-11-27-apexes-event-time-strategy-design.md) provides explicit Rust and Drift event-time closed-bar and cooldown gates, instrument-scoped transactional state, and real sparse/dense historical replay equivalence. 556 targeted tests passed; PR #71 merged at 1157775. [Historical observation-order manifests](superpowers/specs/2026-11-27-apexes-observation-order-design.md) seal explicit close-time observation sequences under schema 2, verify original v1 inputs before stable migration to a distinct version, retain source-day partitions and identity contracts, and require event-time latency. 560 targeted tests passed; PR #72 merged at dd3a1b5. [Live closed-bar observation](superpowers/specs/2026-11-27-apexes-live-bar-observation-design.md) corrects subsecond UTC buckets and rejects regressing trades from aggregation; opt-in live consumer UTC execution clock separates decision/submission times from source timestamps. 624 targeted tests passed; workspace CI, independent review and live-service qualification pending. Next implement quiet-market timing and nonfabricated closed-bar delivery when no subsequent trade arrives. Schema-2 bar receipt coverage and storage/engine export integration remain open. Retain legacy global scripts unchanged and explicitly migrate multi-instrument strategies when authoring V2, while recorded-source/empirical evidence and remaining F03.05 scope (multi-asset/leveraged benchmarks, observed rates, explicit strategy exit motives) stay open; do not mark F03.02–04 complete. Feature statuses remain in FEATURES.md.

Baseline discovered a simulator defect: completed orders remained in
open_orders and blocked all later strategy orders. The regression now checks
the real script's round trip and removes terminal orders from that view.

Local qualification (Rust 1.99.0, locked dependencies): workspace build,
formatting, Clippy with warnings denied, and all 78 non-doc test binaries
passed in split runs (1,295 passed, 0 failed, 75 ignored); 30 doc-test
suites passed. PostgreSQL-dependent default tests ran against a temporary
isolated PostgreSQL 17 instance, stopped afterwards. No live databases or
exchange orders were used. The prior failing flatten test was caused by
the unavailable database, and all 22 live-trading unit tests passed with
the isolated database. Docker/ClickHouse and ignored external integration
tests were not available/qualified. The 20 existing local baseline commits were approved, passed CI, and merged in PR #10. PR #11 passed remote CI and PR #12 promoted it to master.

F00.02 local qualification (Rust 1.99.0, locked): full workspace build,
formatting, Clippy with warnings denied, all 1,299 default tests across
serial split runs (0 failures, 75 ignored), and all 30 doc-test suites passed.
A new temporary loopback PostgreSQL 17 database was used for the default
database-dependent tests and stopped afterwards. New tests cover real
Drift order/fill correlation, pretrade refusal, synthetic-secret omission,
and supplemental fill ledger success/failure. No live exchange or database
was used. PR #13 passed database/workspace CI and Docker builds; PR #14 promotion to master passed CI and merged. Live-network tests remain unverified.
The log target, correlation contract and known redaction boundary are
documented in the design and RUNBOOK. The log does not repair ledger
discrepancies; F02.02 addresses reconciliation.

F01.01 local qualification (Rust 1.99.0, locked): workspace build, formatting,
Clippy with warnings denied, all 78 default test binaries in serial batches
(1,308 passed, 0 failed, 75 ignored), and 30 doc-test suites passed.
Temporary isolated PostgreSQL 17 was stopped after tests. Nine new tests
cover explicit timestamp retry and same client order ID, bounded/ambiguous
responses, family separation and clone sharing, stale samples, invalid/slow
preflight, rate limits, failure cooldown, synchronized WebSocket subscription,
and receive-window configuration. No real exchange requests or orders were
used for qualification. PR #15 passed remote lint, test and Docker checks and merged; live-exchange tests remain unverified.

F01.02 local qualification (Rust 1.99.0, locked): full workspace build,
formatting, Clippy with warnings denied, 78 default test binaries (1,310
passed, 0 failed, 78 ignored), and 30 doc-test suites passed. All 53 OMS
tests passed with ignored database tests enabled, including 26 not run
by the default suite. All 91 default Binance gateway tests passed. Both
database runs used temporary isolated loopback PostgreSQL instances,
stopped afterwards. Five new tests cover interrupted submission, durable
explicit rejection, late fill recovery after restart, ambiguous Binance
errors, and false negative lookup responses. An existing age-expiry
regression now tests conservative unknown-outcome retention. No real
exchange orders were placed. PR #16 passed CI and merged; remaining live-network qualification is pending. Same-instrument guarding is scoped to one OMS and its recovered
database; account-wide capital reservations, durable ledger delivery and
operator health controls are later features.

F01.03 local qualification (Rust 1.99.0, locked): workspace build,
formatting, Clippy with warnings denied, 78 test binaries (1,318 passed,
0 failed, 78 ignored), and 30 doc-test suites passed. The default
database-dependent tests ran against a temporary isolated PostgreSQL 17
instance, stopped afterwards. Eight new tests cover per-symbol freshness,
old/future timestamps, malformed frames, queue overflow, silent sockets,
book reconstruction, atomic snapshot/subscription continuity and Ping/Pong.
PR #17 passed CI and merged; remaining live-exchange qualification is pending. No real exchange
orders were placed. Operator-controlled release is retained; stronger
session-health recovery gates belong to F01.05.

F01.04 local qualification (Rust 1.99.0, locked): full workspace build,
formatting, Clippy with warnings denied, 78 default test binaries (1,318
passed, 0 failed; 83 ignored in the workspace run), and 30 doc-test suites
passed. A sixth new, ignored PostgreSQL regression was added afterward;
the final EMS suite with ignored tests enabled passed all 26 tests
(18 default plus 8 database-dependent). Both database qualification runs
used temporary isolated PostgreSQL 17, stopped afterward. The six new
regressions cover no SQL on the inactive event path, active POV recovery
and venue/symbol routing, cancellation, unresolved dispatch at restart,
child-link persistence failure, and slow sink/queue overflow. PR #18 passed CI and merged; remaining live-network qualification is pending. No live database or exchange
orders were used. The EMS fault latch requires reconciliation/restart;
operator health and recovery controls are the next feature.

PR #18 CI follow-up: the initial PostgreSQL job failed six live-trading
integration tests because an EMS failure-injection fixture left an
unresolved dispatch marker in the shared public schema. The startup guard
correctly refused recovery. All eight EMS database tests now use isolated
UUID-named schemas and remove them on success. The original safety
assertions and production scheduling code are unchanged. The exact CI
PostgreSQL sequence passed locally on one fresh temporary PostgreSQL 17
database: OMS 53, EMS 26, live-trading 30 (109 total, zero failures).
Confirmed no EMS test schemas or public parent_orders table remained
after the EMS suite. Workspace Clippy and fmt passed; PR #18 passed all remote CI checks and merged.

F01.05 local qualification (Rust 1.99.0, locked): workspace build,
formatting, Clippy with warnings denied, all 78 test binaries and 30
doc-test suites passed. Executed 1,364 tests (113 OMS/EMS/live-trading
including their ignored PostgreSQL cases, 927 other core, 63 supervisor,
261 UI); zero failures and 44 remaining ignored external/manual tests.
The PostgreSQL group ran serially in an isolated temporary PostgreSQL 17
instance, stopped afterward. Six new regressions cover managed transitions,
startup wait, per-symbol readiness, unresolved submission, user-stream
disconnect, and tick-independent status/resume denial. No live database
or real exchange orders were used. The initial managed policy requires
operator reconciliation and restart after any halt; it does not persist
an independent health latch across process restarts. Remote CI is pending.

F02.01 local qualification (Rust 1.99.0, locked): full workspace build,
fmt, Clippy with warnings denied, all 79 test binaries, and 30 doc-test
suites passed. Executed 1,372 tests (121 OMS/EMS/live-trading including
isolated PostgreSQL cases, 927 other core, 63 supervisor, 261 UI);
zero failures, 44 remaining ignored external/manual tests. Eight new
regressions verify restart inventory/basis, deduplication, wallet
ownership, conflicting bindings, legacy refusal and incomplete history.
The recovery path requires no working orders or relevant locked funds,
a dedicated bound schema, and distinct base assets. Existing legacy
history requires an audited migration, not automatic adoption; online
reservation accounting and full fee attribution remain F02.05/F02.03.
No live database or real exchange orders were used. Remote CI pending.

F02.02 local qualification (Rust 1.99.0, locked): full workspace build,
fmt, Clippy with warnings denied, all 80 test binaries, and 30 doc-test
suites passed. Executed 1,379 tests (126 OMS/EMS/live-trading including
isolated PostgreSQL cases, 929 other core, 63 supervisor, 261 UI);
zero failures, 44 remaining ignored external/manual tests. Seven new
regressions cover repeated partial fills and cancellation delivery,
atomic rollback on conflicting executions, incomplete terminal evidence,
late fills, failed acknowledgement, cumulative quantity response, and
duplicate/refused ledger fill reporting. Managed delivery uses a
process-local acknowledgement cursor over durable fill history and
rebuilds on restart. Late or inconsistent executions require reconciliation
and restart; incomplete trade pages halt rather than silently advance.
No live database or exchange orders used. Remote CI pending.

F02.03 spot attribution increment local qualification (Rust 1.99.0,
locked): workspace build, fmt, Clippy with warnings denied, all 80 test
binaries, and 30 doc-test suites passed. Executed 1,385 tests (126
OMS/EMS/live-trading including isolated PostgreSQL cases, 934 other core,
63 supervisor, 262 UI); zero failures and 44 remaining ignored
external/manual tests. Six new regressions verify causal conversion,
base/quote/third-asset fee economics, held-asset equity reconciliation,
unknown external fee pricing and quote-specific operator display.
The unchanged Drift round-trip also asserts explicit gross/net/fee
attribution. This is an incremental implementation: automatic historical
conversion retrieval for fee assets outside configured direct pairs,
durable conversion provenance and lifetime attribution across restarts
remain open. Existing balance/position and leveraged accounting semantics
are preserved. No live database or real exchange orders used. Remote CI
and independent review pending.

F02.03 durable conversion follow-up local qualification (Rust 1.99.0,
locked): workspace build, formatting, Clippy with warnings denied,
80 test binaries and 30 doc-test suites passed. 1,389 executed tests
(127 OMS/EMS/live-trading including isolated PostgreSQL, 937 other core,
63 supervisor, 262 UI); zero failures, 44 remaining ignored external/manual
tests. Four new regressions cover immutable causal pricing evidence after
restart, public pair identity and closed candle selection, and lifetime
reconstruction. Live worker queries direct Binance Spot pairs independently
of the OMS, using bounded lookups and immutable persisted rates. Lifetime
journal attribution is separate from session equity. Direct pair absence,
offline conversion datasets, historical external wallet funding and
large-journal performance remain limits; current session-only attribution
can still be unpriced. No real orders or live database used. Remote CI,
independent review and real-service pricing qualification remain pending.

F02.04 local qualification (Rust 1.99.0, locked): workspace build,
formatting, Clippy with warnings denied, 80 test binaries and 30
doc-test suites passed. 1,393 executed tests (128 OMS/EMS/live-trading
including isolated PostgreSQL cases, 940 other core, 63 supervisor,
262 UI); zero failures and 44 remaining ignored external/manual tests.
Four new regressions cover the narrow entry-budget exemption, durable
and free inventory checks, a real Drift exit with its daily budget
exhausted, and a recovered position's first-trade stop before warm-up.
The existing deterministic economic baseline remains green. Managed
health, unresolved submissions, ledger acknowledgement and hard risk
limits remain enforced. The scope is direct Spot reductions, not
derivative reduce-only orders or continuous fee-aware reservations.
Trend Guard's safety-corrected script is distinct from its historical
V1 Git revision for later research comparisons. No real exchange orders
or live database used. Remote CI and real-service qualification pending.

F02.05 initial increment qualification (Rust 1.99.0, locked): 1,397
executed tests passed (130 OMS/EMS/live-trading including isolated
PostgreSQL, 942 other core, 63 supervisor, 262 UI); zero failures,
44 remaining ignored external/manual tests. Workspace build, Clippy
with warnings denied, formatting and all 80 test binaries passed.
Four new regressions cover changing filters, market lot/averaging
windows, shared quote reservation through partial fill/cancellation,
and durable position plus notional checks without caller snapshots.
Reservations are exclusive per spend asset, retained until terminal
reconciliation, and are not exact fee or market-buy price guarantees.
EMS child dispatches must conform exactly to the already-journaled
quantity. Parent-level allocation, working-order restart recovery,
other exchange filter types, exact fee reserves and real-service
qualification remain open. No real orders or live database used.
Remote CI pending.

F02.05 durable reservation follow-up (Rust 1.99.0, locked): workspace
build, formatting, Clippy with warnings denied, all 80 test binaries
and 30 doc-test suites passed. 1,399 tests executed (132 OMS/EMS/live
trading, 942 other core, 63 supervisor, 262 UI); zero failures,
44 external/manual tests remain ignored. The order and reservation
evidence are persisted atomically before dispatch; working-order
reservations are restored by the OMS and kept through reconciliation.
A cumulative spend overrun, including fees in the spend asset, is
durably latched and blocks new submissions and managed restart
without discarding actual fills. Existing quiescent-startup, ownership,
health and fill-acknowledgement gates remain. This detects an overrun
after execution; it does not guarantee an exchange-side spending cap
or exact pre-trade fees. Parent-level allocation and real-service
qualification remain open. No real orders or live database used.
Remote CI pending.

F02.05 bounded market-buy follow-up (Rust 1.99.0, locked): workspace build,
formatting, Clippy with warnings denied, all 80 test binaries and 30
doc-test suites passed. 1,402 tests executed (134 OMS/EMS/live trading,
943 other core, 63 supervisor, 262 UI); zero failures, 44 external/manual
tests remain ignored. Managed market buys now dispatch LIMIT IOC with
the original client ID and conformed quantity, at a durable tick-aligned
price cap no higher than the observed last price. Partial fills/expiry
reconcile before capital release. A reported above-cap fill durably
latches health even when total spend is below the reservation.
The 1% quote-fee headroom remains an estimate. Parent-level allocation,
real-service qualification and backtest execution-model parity remain
open. No real exchange orders or live database used. Remote CI pending.

F02.05 fee-aware reservation follow-up (Rust 1.99.0, locked):
workspace build, formatting, Clippy with warnings denied, 80 test
binaries and 30 doc-test suites passed. 1,405 executed tests (137
OMS/EMS/live-trading, 943 other core, 63 supervisor, 262 UI); zero
failures, 44 external/manual tests remain ignored. Three new
PostgreSQL regressions cover rate-driven funding, discount-asset
exclusivity across restart, and base-denominated discount fee
inventory. Extended signed Binance mock tests cover side-specific
standard/special/tax rates, undiscounted maker/taker maximum and
missing evidence. The fee snapshot is persisted with the order;
older working orders lacking evidence block new submissions.
Fee rates are observed, not frozen by the exchange. Exact third-asset
fee quantities and future rate changes are not guaranteed; discounts
are never assumed in the budget. No real orders or live database
used. Remote CI and parent-level capital allocation remain pending.

F02.05 EMS parent capital follow-up (Rust 1.99.0, locked):
workspace build, formatting, Clippy with warnings denied, 80 test
binaries and 30 doc-test suites passed. 1,408 executed tests (140
OMS/EMS/live-trading, 943 other core, 63 supervisor, 262 UI), zero
failures; 44 external/manual tests remain ignored. Three new
PostgreSQL regressions cover competing parent/direct orders, fee-rate
budget exhaustion, durable child allocation, orphan locks, restart
verification and actual EMS/OMS POV integration. Parent admission
reserves the whole conformed quantity against fresh Spot evidence;
children consume their own reservations and quantities atomically
with pre-dispatch order records. Release waits for terminal scheduling,
terminal children and the existing fill-acknowledgement gate.
No budget recycling, flexible cross-parent sharing or native
exchange-side parent reservation is provided. Admission is
conservative (full parent must satisfy single-order filters); a crash
between allocation and scheduling insert leaves an orphan lock for
audited recovery. Exact external fee-token funding, real-service
qualification and execution-model parity remain open. No real orders
or live database used. Remote CI pending.

F03.01 configurable fees (Rust 1.99.0, locked): workspace build,
formatting, Clippy with warnings denied, all 80 test binaries and 30
doc-test suites passed. 1,412 executed tests (140 OMS/EMS/live trading,
946 core, 63 supervisor, 263 UI), zero failures; 44 external/manual
tests remain ignored. Four new tests plus extended Drift baseline
verify JSON policy, live refusal, modeled paper fees and explicit
zero/unknown labels. Configured venue rates are frozen and serialized
in reports. Legacy custom schedules retain fallback behavior for
unconfigured routes; report assumptions cover configured venues only.
Launch JSON is configurable; no new launcher controls. Existing
zero-rate default is retained and labeled. No change to real exchange
fee accounting or funding behavior. No real orders or live DB used.
Independent reviewer and rendered UI inspection not completed.
Remote CI pending. F03.02 owns causal depth/spread/latency and bounded
IOC simulation parity. F02.05 remaining qualification is not waived.

F03.02 causal liquidity/lifecycle first increment (Rust 1.99.0, locked):
workspace build, formatting, Clippy with warnings denied, all 80 test
binaries and 30 doc-test suites passed. 1,419 executed tests (140
OMS/EMS/live trading, 953 core, 63 supervisor, 263 UI), zero failures;
44 external/manual tests remain ignored. Seven new regressions cover
consumed liquidity, duplicate depth, venue isolation and future-book
refusal, terminal partial market orders, stop/stop-limit identity,
shared EMS liquidity and replay timestamps, and terminal order logging.
Book-walk spread/impact uses displayed depth; historical absolute
updates may replenish it. Direct and EMS market orders expire any
unfilled remainder. EMS consumes the book after the current event is
processed and stamps fills with the latest observed matching clock
when available. Existing EMS scheduling, paper wall-clock latency,
strict stale/gap handling, queue calibration, crossing-limit behavior
and bounded managed Spot IOC remain open. The original Trend Guard
baseline is unchanged. No real exchange orders or live database used.
Independent review and remote CI remain pending.

F03.02 bounded Spot IOC increment (Rust 1.99.0, locked): workspace
build, formatting, Clippy with warnings denied, all 80 test binaries
and 30 doc-test suites passed. 1,424 executed tests (140 database
group, 958 core, 63 supervisor, 263 UI), zero failures; 44 external/manual
tests remain ignored. Five new regressions cover capped depth/fees,
invalid trade references, launch validation, Drift replay comparison
and paper execution. The opt-in policy requires explicit historical
rules and a positive trade-reference maximum age. Backtest and paper
record the policy in their reports; legacy runs remain unrestricted.
Market buys consume only depth priced at or below the causal
tick-rounded cap; remainder expires. EMS children require exact
quantity conformance. The existing unrestricted Trend Guard fixture
is unchanged; bounded mode produces no fills in that fixture because
asks exceed the trade reference. This is not a profitability forecast.
Rule provenance, full exchange filter/fee/account parity, strict book
freshness, causal arrival and EMS scheduling, and queue calibration
remain open. No independent reviewer, real orders or live database
used. Remote CI pending.

F03.02 strict depth integrity increment (Rust 1.99.0, locked):
workspace build, formatting, Clippy with warnings denied, all 80 test
binaries and 30 doc-test suites passed. 1,430 executed tests (140
database group, 964 core, 63 supervisor, 263 UI), zero failures; 44
external/manual tests remain ignored. Six new regressions cover
snapshot-chain recovery, duplicate and delayed updates, explicit
invalidation, malformed/crossed depth, working-order expiry, launch
validation and deterministic replay. An optional event-time maximum
age and source-specific contiguous or snapshot-only policy prevents
matching against missing, invalid or stale depth. Strict expiry
discards resting and trigger orders rather than assuming queue
continuity. Recovery requires a fresh snapshot. The policy is
persisted in reports; absence preserves the unrestricted benchmark.
A source with range-based update IDs cannot be safely gap-checked
from its final sequence alone; contiguous mode requires appropriate
source semantics. No real exchange orders or live database used.
No separate reviewer; remote CI pending. Causal order arrivals and
EMS scheduling and queue calibration remain open.

F03.02 direct-order event-time latency increment (Rust 1.99.0, locked):
workspace build, formatting, Clippy with warnings denied, all 80 test
binaries and 30 doc-test suites passed. 1,435 executed tests (140
database group, 969 core, 63 supervisor, 263 UI), zero failures; 44
external/manual tests remain ignored. Five new regressions cover
arrival-book matching, unfinished arrivals, risk recheck, paper
execution, launch validation and queue ordering. The opt-in fixed
delay queues direct strategy intents after pretrade checks and
rechecks at the first observed event at or after the deadline, after
processing that event. Arrival uses the current observed price,
not an interpolated price or exact exchange timestamp. One in-flight
intent per venue/symbol; FIFO capacity 1024; regressing events are
ignored and counted. End-of-stream intents expire without fills.
Immediate automatic liquidation/de-risking and legacy behavior are
preserved. EMS parents are refused in event-time mode pending causal
child scheduling; no wall-clock EMS execution is silently mixed in.
Reports record policy and scheduled/arrived/refused/expired counts.
No independent reviewer, real orders or live database used. Remote
CI pending. Crossing-limit and queue calibration remain open.

F03.02 causal EMS scheduling increment (Rust 1.99.0, locked):
workspace build, formatting, Clippy with warnings denied, all 80 test
binaries and 30 doc-test suites passed. 1,439 executed tests (143
database group, 970 core, 63 supervisor, 263 UI), zero failures; 44
external/manual tests remain ignored. Four new regressions cover
historical TWAP timing/order, matching-venue POV, as-of VWAP duration
and end-to-end delayed parent admission/fills. Event-time policy now
admits algorithmic parents, advances schedules on observed events,
and passes explicit event timestamps to the simulated child sink.
TWAP spaces slices from actual dispatch; VWAP scales historical
fractions to duration and requires prior UTC-day history, with no
wall-clock provider fallback. Competing parents are ordered within
algorithm class by admission order. Legacy timing remains unchanged
without event-time policy. Separate child transit delays, independent
child risk and capital reservations, complete source provenance and
crossing-limit/queue calibration remain open. The ClickHouse as-of
query has not been qualified against a real database locally.
No independent reviewer, real exchange orders or live database used.
Remote CI pending.

F03.02 causal EMS fill preservation (Rust 1.99.0, locked): workspace
build, formatting, Clippy with warnings denied, all 80 test binaries
and 30 doc-test suites passed. 1,440 executed tests (144 database
group, 970 core, 63 supervisor, 263 UI), zero failures; 44 external/manual
tests remain ignored. One new regression injects a refusal in the
middle of three causal TWAP parents, verifies successful fills before
and after the failure survive, and verifies only the failed parent
stops scheduling. Error isolation covers TWAP, VWAP, POV and Iceberg
fixed-route causal simulation; legacy and PostgreSQL EMS retain
their original error handling. Does not resolve multi-allocation
legacy error propagation, ambiguous execution failures, child risk
or child transit latency. No real orders or live database used.
Independent review and remote CI remain pending.

## Prior work — Binance Spot testnet MVP: built; remaining verification and polish

**Goal.** One operator can start a Binance **Spot testnet** live session from a UI, watch it, tune it, halt it, and judge the strategy and its risk from a live timeline. Mainnet is an explicit opt-in (typed `MAINNET`) and is not part of this MVP's acceptance. Design: [`specs/2026-11-15-apexes-binance-spot-testnet-mvp-design.md`](superpowers/specs/2026-11-15-apexes-binance-spot-testnet-mvp-design.md). Operating it: [`RUNBOOK.md`](RUNBOOK.md).

**Scope note.** "Binance" means trading **Spot on the Binance exchange**, not BNB Smart Chain or the BNB token. The Spot Test Network (`testnet.binance.vision`) is a separate system from a Binance account: own keys (GitHub login), virtual funds, periodic resets.

### Built and verified

| Task | What | Evidence |
|---|---|---|
| A | `BinanceNetwork` is the one switch; every Spot endpoint comes from one function (a test fails if a hostname appears elsewhere); `network` and `risk_limits` are **required** in live configs; per-network credential variables; the gateway announces its network and an engine refuses a mismatch | unit + integration tests; real binaries refuse bad configs |
| A2 | Testnet market data via a `barter-data` testnet server type (trades, ticks, bars; **no depth** on testnet) | ran the real gateway against the testnet and read live trades |
| A3 | Live sessions carry mandatory pre-trade limits; previously a live engine registered no risk checker at all | tests prove an over-limit order is rejected |
| B | Environment banner from the engine's own report | headless render tests |
| C | Bounded engine-side evaluation tracker, `egui_plot` timeline, limit-utilisation bars | unit + render tests |
| D | `apexes-supervisor` crate (detached spawn, discovery, stop, secrets by env and 0600 files, allow-listed child env, port-race retry) and the launcher/console shell | 20 real-process tests with test doubles, stable over 25 runs under CPU load (it flaked once at the 9th run before the port-race retry); real-binary tests below |
| E | Demo strategy `deploy/strategy.example.drift`: holds a fixed VALUE (default 20 USDT) in whole lot steps, so one script works for BTC and ETH | tests, and **real testnet trades**: buys, a sell, and re-entries filled on ETHUSDT (the first version used a fixed 0.001 base quantity, which is only ~3 USDT of ETH and was refused by Binance's 5 USDT minimum 25 times) |
| — | **Binance removed the listenKey user-data stream (HTTP 410, 2026-02-20).** Replaced the Spot stream with the WebSocket-API `userDataStream.subscribe.signature` (HMAC), unwrapping the new `{subscriptionId, event}` envelope; a cancel report's order id is in `C`; reconnect on `serverShutdown`/`eventStreamTerminated`. Signing is behind a `ParamSigner` trait | fake-server tests + a live testnet test that places and cancels a real order and sees both updates keyed by our id |
| — | A launch counts as started only after live startup recovery (the PID file is written then; the control port still opens early so an operator can always Halt) | a bogus-key launch now fails with Binance's own error instead of looking started |
| — | Live equity: only the session's base/quote assets seed the ledger, and the evaluation starts only once holdings are priced (the ledger values an unpriced asset at 1.0; a pre-filled testnet account has 500+ assets) | unit tests + real run shows equity 75 210 (10 000 USDT + 1 BTC), not 10 500+ |
| — | Binance REST client: 4 s connect timeout per address (one of the testnet's four IPs black-holes connections; a default client stalled ~30 s and then failed signed requests with `-1021`) | live test went from failing to passing |

### Theme and window management (2026-11-16)

| What | Evidence |
|---|---|
| Light / Dark / System theme in every window (`theme.rs`, `chrome.rs`), saved in `~/.apexes/ui.json` (`preferences.rs`); one palette for every status colour and banner, checked for WCAG contrast on both themes; Cmd/Ctrl+Shift+L cycles | 13 palette tests, headless tests drive the shortcut and read the saved file |
| One window, many sessions: a tab per console (`tabs.rs`, `shell_app.rs`), each with its own connection; a launch opens its console; background tabs keep reading and show `(offline)`/`HALTED`; Cmd/Ctrl+1-9, [ ], W; window size and position remembered, minimum size enforced | 9 `ShellApp` tests through the real app, 9 pure tab-model tests |
| Found by looking at rendered PNGs, not by tests: eframe clears to near-black and gives `ui()` no background, so the light theme drew white-on-black text on a dark window (and dark-theme text on white) until every app drew a themed page; the `●` glyph is missing from egui's fonts and drew as an empty box (now a painted circle); limit-bar text was unreadable on its own fill | `cargo test -p apexes-ui --test themes -- --include-ignored` writes the PNGs |

### Silent feed death (2026-11-16, reported from a real run)

An overnight session halted with `market data stale: no events for 108.9s`; the feed then never came back. The gateway's two upstream sockets were `ESTABLISHED` but frozen (byte counters flat for 10+ minutes) while a fresh connection from the same machine delivered at once: a half-open connection, which `barter-data` never reconnects because it saw no error and no close. Fix: `supervised.rs` wraps trades, ticker, depth and mark-price in an idle-timeout/backoff loop on a fresh runtime per attempt. Verified: 9 supervisor tests incl. a real socket that stays open and silent (replaced, data resumes), 15 repeated and 5 CPU-loaded runs, and the real testnet gateway serving 150 live events in 25 s. **Not verified:** a real mid-run network cut (can't be induced without root); the engine's halt is deliberately sticky, so after a feed outage you still press Resume.

### Two symbols at once (2026-11-16, reported from the launcher)

Launching ETHUSDT while a BTCUSDT session ran failed: *"a testnet market-data gateway is already running with instruments [BTCUSDT]... Stop it"*. The supervisor kept one gateway per network and refused anything it did not carry. Now: a gateway per symbol set (smallest covering one is reused, otherwise a new one for exactly the requested symbols), each with its own PID file/token/log under `~/.apexes/gateways/`, an older build's gateway still found and stopped, long-dead gateways' files tidied. Fixed alongside a latent bug the sharing exposed: an engine on a shared gateway received every symbol the gateway streams, so it now filters to its own. Verified with real processes on the testnet: a BTC and an ETH session ran side by side, each on its own gateway, each showing sane equity (BTC ~74 874, ETH ~11 784 USDT) with no cross-contamination, and each real gateway streamed only its own symbol (58 BTCUSDT events, 60 ETHUSDT in 12 s). **Not verified:** the old gateway still running on your machine is the legacy kind; it is stopped with the other gateways once no session uses it.

### Equity chart on a clock-time axis (2026-11-16)

The x-axis was "seconds since first point". It is now the operator's **local clock time** (`time_axis.rs`): x is Unix seconds, ticks fall on round *local* times (a +05:30 zone's hours are :30 in UTC), labels show seconds only when ticks are seconds apart and the date only at a local midnight, the axis title names the UTC offset, and hovering a point shows `YYYY-MM-DD HH:MM:SS +09:00`. 13 pure tests include a half-hour-offset zone, panning stability, an absurd zoom-out, and a hand-built daylight-saving zone through the spring-forward gap. Looked at as rendered images at 12 min, 5 h and 30 h spans. **Not verified:** the hover text and the look in a real window; the zone is the machine's at draw time.

### Fees on mainnet-style fills (2026-11-16, fixed)

The Spot testnet charges 0% (account `commissionRates` all zero; Binance's `myTrades` shows `commission 0` for every fill), so "Fees paid 0" there is correct and nothing in a testnet run could show how fees are booked. Feeding mainnet-style fees to the session ledger found **two** bugs, both of which made the live loop log and *skip* a real fill, leaving the session's position, balances and PnL out of step with the exchange: (1) Binance's default fee is a slice of the asset *received*, and the ledger checked the fee against the balance *before* crediting that asset, so a first buy on a flat account was refused (`required 0.0000277 ETH, available 0`); (2) with "pay fees in BNB" the fee asset is one a one-symbol session does not hold. Now the ledger validates the **net** change per asset; a fee in an asset the session does not track is left out of the ledger (the trade still happened) and stays in the fee total under its own currency; a fee in a held asset is still debited and still must be affordable (atomic, unchanged); a base-asset fee leaves the position net of it, raises the average entry price (the fee is a cost) and, on a sell, is booked as a realised loss. 10 ledger tests and 3 live-loop tests, plus a check that a genuinely unaffordable fill is still refused. **Not verified against a real fee:** the testnet cannot produce one, so a first small mainnet fill (with a tiny order) should be checked against the exchange's own trade history.

### Defects found in a screenshot of a running session (2026-11-16, fixed)

Read against Binance's own balances and trade history (which matched exactly), the console was right about *what* but misleading about *why*:

1. **"Total PnL -271.99" was 99.97% the market, not the strategy.** The testnet account already held 1.0006 BTC; the session's two trades made -0.07, and BTC's fall made the rest. The same swing was the 1.16% "max drawdown", and "gross exposure 0%" ignored an ~87%-of-equity holding. Now the tracker records what the account held when the session started and splits the result: **Account PnL** (as before), **from holdings** (starting quantity x price change), **Strategy PnL** (the rest, includes fees) with its Realized/Unrealized, a **strategy-only drawdown**, and exposure that counts non-cash holdings. An account that started flat sees the original three labels. Older engines' snapshots still parse (`serde(default)`).
2. **y-axis had one label.** `egui_plot` steps by powers of ten, so 73,930-74,765 got `74000`. Now a 1-2-5 step chosen from the range (about 6-8 lines), thousands separators, and only the decimals the step needs (`value_axis.rs`).
3. **A closed position showed as a row** (`0.00000000` at its old entry price). Closed positions are now "flat".
4. **Balances showed the decimal's internal scale** (`9940.9058680000000000`); all amounts/prices now go through `number_format.rs`.
5. **Zero fees read `0.00000000 BTC, 0.00000000 USDT`**; now `0`, and only currencies that charged are listed.

Found by running the real engine against the testnet, which no unit test did: prices arrive as `f64`, so an idle strategy's `total - holdings` was `3.3e-25`, not 0, and showed a strategy PnL and drawdown; the split is now rounded to 8 places. Verified: 5 tracker tests + 3 live-loop tests + 7 console tests + axis/format unit tests; a real engine on the testnet with 1.0006 BTC and an idle strategy reports total -8.475 = holdings -8.475 + strategy 0, exposure 86.6%, strategy drawdown 0 (account 0.045%); the issue-screenshot's layout rendered and looked at. **Not verified:** the new panel in your real window, and a holding that is sold or bought mid-session (covered by tests, not seen live).

### Recent orders panel (2026-11-17)

The console only listed *open* orders. A market order is working for well under a second, so that list was always empty and nothing in the window showed what the strategy had done (the orders existed only in Postgres and the log). The engine now keeps a bounded log of every order the loop sends (`order_log.rs`: newest first, 100 kept, volume-weighted fill price, partial fills, risk/exchange refusals **with the reason**, and orders that disappear without filling marked *Closed* after a 10 s grace, since the live open-orders view can lag) and sends it in the snapshot (`recent_orders`, `serde(default)` so older engines still parse). The console shows it as a **panel, not a modal**: in the whitespace to the right of the tables (resizable, 380-640 px) when the window is at least 1100 px wide, under the tables when narrower. A modal would block the window under it and HALT must always stay reachable. Local times, dates only on rows from another day. 

Found by looking and by testing, not by reading: a first version counted a simulated fill twice (0.02 filled on a 0.01 order: the simulator returns an order that already carries its fills *and* the same fills separately); the first layout shrank cells to their text, and the next grew the panel a few pixels every frame (a test now catches that, and was shown to fail when the bug is put back). Verified: 11 log tests, 10 view tests, 6 console tests, 2 loop tests; a real engine on the testnet sent a buy and a sell and listed both, newest first, **with fill prices equal to Binance's own trade history** (64989.12 / 64989.11). **Not verified:** the panel in your real window; the *Closed* state against a real cancelled order; an order that fills much later than it was sent.

### Console layout redesign (2026-11-17)

A screenshot of the real window showed one long single column: thirteen metric rows down the left edge of a ~1,500 px window, a chart that was the smallest thing on screen, a thin list of small labelled rows, and every heading the same weight as body text. Rebuilt as: a slim top (banner, one status line with a TRADING/PAUSED/HALTED pill and a red HALT), **key-figure tiles** (what an operator reads first; the strategy's PnL leads and the market's move on held assets is a tile of its own), a taller chart with a soft fill and a quiet grid, and **cards** that go side by side when wide (details / risk limits, positions+orders / balances) and stack when narrow, parameters in up to three columns with the box and Set button as one control, tabs as filled rounded buttons with an accent underline, and one shared style (spacing, corner radius, type scale, button edge). Building blocks are in `widgets.rs` and the width decisions in `console_layout.rs`, both pure and tested; palette tokens (card, stroke, label, grid, track) have contrast tests in both themes.

Found by looking at the renders, one defect at a time (each invisible to every assertion): a card laid its contents out sideways because it inherited its row's direction; a limit bar collapsed to a dot (`ProgressBar` ignores a card's width); a two-column row grew wider than its column and slid under the orders panel (a `Ui` offered a width takes what its content asks for, so rows and cells are now exact rectangles); painted text (`painter.text`) draws fine but creates no accessibility node, so a screen reader and the tests could not see the numbers (rows are real labels in exact rects); the heaviest grid lines read as axes. And one real regression the old tests caught: the parameter **Set** button had been pushed below the test window, because the harness default is 800x600, smaller than the app's minimum; tests now use a real window size and two new tests pin that **HALT is on screen without scrolling at the smallest window** and parameters stay reachable by scrolling. **Not verified:** the layout in your real window and on a high-DPI display other than via the renders; the launcher tab was restyled by the shared style only.

### Several symbols in one session (2026-11-17)

The launcher accepts `BTCUSDT, ETHUSDT` and the platform handles it: a real session on both launched on one gateway carrying both, held BTC, ETH and USDT in its ledger (and no BNB), and its equity (76,748.55) matched Binance's own balances at Binance's own prices (76,748.56). **But neither shipped strategy is multi-symbol.** `trend_guard.drift` says so in its header ("assumes a single symbol per session: the indicators are shared") and `strategy.example.drift` has the same shape: one `indicators.sma(..)` is fed every symbol's price, so BTC (~65,000) and ETH (~1,820) are averaged together. In a replay of a quiet synthetic market the demo made 1,140 orders on BTC alone and 1 on BTC+ETH (ETH never entered: its price is always below the blended average). Use one symbol per session until a strategy keeps its indicators per symbol; sessions are cheap, and several run side by side. Open: the launcher does not warn when given more than one symbol.

### Priced from the exchange at start, per-symbol charts, two-column cards (2026-11-17)

A real 3-symbol session (BTC, ETH, BNB) showed no chart and a wrong headline equity (76,773 against about 77,314). BNBUSDT is very quiet on the testnet (its last 1,000 trades span about 11 hours); the evaluation waits until every held asset has a price, so the chart waited for BNB's first trade (about 178 s here), and in the meantime the ledger counted the unpriced BNB at 1.0 USDT. **Fixed:** the engine asks the exchange for each session symbol's last price (`BinanceExecutionGateway::last_prices`, an unsigned public `GET /api/v3/ticker/price`, a symbol that fails is left out and logged) and passes it as `UnifiedLoopConfig::initial_prices`, so held assets are valued from the first event. If a price is still missing the snapshot says so (`equity_priced: false`) and the console shows **pricing…** instead of a number that is wrong by thousands, and says why the details card is empty. Verified on the real testnet: a quiet-BNB 3-symbol session was priced and measured **0.0 s** after connecting, start equity 76,933.99, holdings 66,993.38, a price series for each symbol (`a_quiet_symbol_is_priced_from_the_exchange_at_start_not_from_its_first_trade`).

**Charts:** the snapshot now carries `price_history` (each symbol's trade prices, bounded by the same halving as the equity curve; `history.rs`). The chart card has a selector: **Equity**, each symbol (its price), and **All** (percent change since each symbol's first price, the only fair way to draw BTC at 65,000 and BNB at 590 together; a colour key sits beside the caption because a legend inside the plot covered the data). A symbol with no price yet is not offered and a stale selection falls back to Equity.

**Layout:** first built as two body columns (details and positions/open orders on the left, risk limits and balances on the right); then, as asked, **Risk limits and Balances moved under Recent orders**, and then **Positions (with Open orders) between Recent orders and Risk limits**: the right-hand column of a wide window is Recent orders, Positions, Risk limits, Balances, and it scrolls (bar hidden) when they are taller than the window. The main column keeps Details and the parameters at full width. The side panel's default width went from 500 to 560 px because the six-column positions table needs about 540 and a panel widens to fit its content, which made the column's width depend on what was in it (`the_side_panel_keeps_its_width_with_a_wide_table_and_does_not_grow`); at the narrowest window that gets a side panel (1,100 px) the main column is left about 540 px. In a narrow window (under 1,100 px, where the orders panel goes below) there is no right-hand column, so the body keeps its own two columns (details and positions on the left, limits and balances on the right), split from 640 px. Two lines that read as a stray vertical rule were also removed (the orders panel's separator and the page's scroll-bar handle); the resize handle shows only on hover.

**Recent orders list height:** the list is now ten orders tall and scrolls (it replaced a "Show N older" button that stopped at 30; all 100 recorded orders are now reachable). Its height is summed from each row's own height, because a refused order has a reason line under it and is taller than a plain one, so ten refusals get a taller box and the tenth is never cut in half. The column headings sit above the scroll box.

Not verified: the new chart and layout in your real window (renders only), hovering the chart, and the selector with a session running for hours.

### Order rejected: `Filter failure: LOT_SIZE (code -1013)` (2026-11-17, fixed)

A BNBUSDT buy of 0.0846 was refused. Binance requires a quantity that is a multiple of the symbol's step, and the step differs per symbol (BTC 0.00001, ETH 0.0001, BNB 0.001); `trend_guard` sizes for ETH (`step` 0.0001), so on BNB it asked for a quantity off BNB's step. The engine had only placeholder lot sizes ("nothing in the live path reads them") and sent whatever the strategy asked. **Fixed in the engine, not the strategy** (a script cannot know every symbol's rules): at startup the engine reads each symbol's real rules from the public `exchangeInfo` (`BinanceExecutionGateway::symbol_rules`: quantity step/min/max, price tick, minimum order value) and the loop conforms every order before the risk check (`apexes_core::SymbolRules::conform`): the quantity is **rounded down** to the step (never up, so it never spends more than asked), a limit price moves to the passive side of the tick, and an order that cannot be made valid (rounds to nothing, under the minimum quantity or the 5 USDT minimum value, over the maximum) is **refused with the reason** in Recent orders and never sent. The risk checks therefore judge the order that is really sent. A symbol whose rules cannot be fetched is sent as asked and the exchange decides, as before. Verified on the real testnet: the same 0.0846 BNB order went out as 0.084 and filled at 589.93 (`an_off_step_bnb_order_is_rounded_to_the_exchanges_step_and_fills`).

Not covered: `MARKET_LOT_SIZE` (the testnet's is unrestricted), `PERCENT_PRICE`, and `ICEBERG` and `MAX_NUM_ORDERS` filters; a rounded-down order is smaller than the strategy intended, which the Recent orders list shows (quantity as sent). A strategy that sizes its own sells from what it holds still needs its own epsilon for float residue.

### Strategy trade-off: trend_guard trades rarely, a mean-reversion one was built (2026-11-17)

Asked why a `trend_guard` ETH session had made no orders: it had made **4** (Binance, the engine's snapshot and a replay of the real script in the real interpreter all agree; the window simply did not show them). It is selective by design, ETH moved in a ~1% band for hours and ended flat, and every faster setting traded more and earned less. So the request became a strategy that suits a ranging market: `deploy/strategy.mean_reversion.drift`.

**Method.** Two weeks of real ETHUSDT testnet prints (11-03 to 11-17, 284,075 trades), split by date: parameters were chosen on the first week only and judged on the second. The shipped script was run in the real Drift interpreter over all of it and produced the **same 720 orders as an independent harness, order for order** (side, print index and price), which is what the numbers below rest on. Eight committed tests cover the script's behaviour and each of the guard, the stop and the entry cap was broken on purpose to confirm a test fails.

**Finding that matters most: the testnet feed prints garbage.** About 0.4% of prints (11 episodes in two weeks) sit hundreds of dollars from the market (e.g. 1,490 while ETH was 1,814). `trend_guard` on the raw feed lost 13.97 USD on $50 in the two weeks, and 11.13 of that came from 5 trips that entered or exited on a garbage print. The new script ignores any print more than 1% from a slow average of accepted prints, and re-anchors after 30 in a row so a real crash is not missed.

**Result, stated plainly: no edge was shown.** With the shipped defaults, zero cost, fills on the next print, it made +23.47 USD on a $50 position over 360 round trips (62% winners; first week +5.5, second +17.9; t-statistic about 1.2, which is not significant), against -9.35 for buying and holding and -13.97 for `trend_guard`. But the sign flips with small changes: fills 10 prints (about 20 s) later give -5.87, **filling at the market's real price instead of the raw print gives -9.07**, and a 0.1% fee per side (mainnet) gives -12.47; 0.05% gives +5.50. Five trades out of 360 account for +36.63. It trades often enough to exercise the platform and it is measurably better than the trend follower on this data, but it should not be read as a profitable strategy and I would not run it on mainnet.

Not tested: other symbols, another period, partial fills, the engine's real latency, and a second random split. Two weeks of one asset in a falling market (ETH 2,246 to 1,826) is one sample.

### Found by hand-testing a real session (2026-11-16) and fixed

| Symptom (from a screenshot and the session log) | Root cause | Fix |
|---|---|---|
| Equity chart spanned ~54 000 s and folded back on itself | The evaluation recorded Bar events; a daily bar is stamped with its open time (00:00 UTC), so the timeline jumped back by hours | Bars are ignored by the evaluation and out-of-order points are dropped |
| `DailyTradeCountExceeded 51/50` after only 25 orders, all rejected by the exchange | `LimitChecker::check` incremented its counters, and both the loop and `OrderManager` ask about the same order, so each order counted twice and orders Binance then refused still used up the cap | `check` is pure; a new `on_order_accepted` advances the counters, called once by the loop after an order is accepted |
| `KILL SWITCH ENGAGED: market data stale ... 37.4s` while the feed was healthy | A real **deadlock**: reconcile held the order-manager lock across exchange calls inside a stream the loop only polls on its own turns; the loop's next `open_orders()` then waited on that lock forever, so no event was processed and the watchdog fired (the engine then never logged again) | Lock-holding work runs as its own task; `open_orders()` waits at most 50 ms and serves its last known view otherwise. A regression test with a hanging gateway reproduced the freeze (0 events) and now passes (116/120) |
| Orders rejected by the exchange with `-1100` for `quantity=0.0000000000000000017...` | Floating-point residue (`target - held`) reached the order | The strategy works in whole steps; the Binance gateway sends quantities and prices as plain decimals of at most 8 places and refuses a value that rounds to zero |
| `max_order_notional` never applied to market orders | A market intent has no price, so the check was skipped | The loop values a priceless order at the last trade price for the check only |
| A phantom open order (`BTCUSDT Buy Market 1.0`) in the console, plus a `-1100` warning every 30 s | The test suites TRUNCATE the shared `apexes` database and leave mock-gateway orders in it; a live engine adopted one as its own | Live sessions use their own database `apexes_live` (the script creates it) |
| The binary the screenshot came from had no launcher | When splitting commits I reverted `apexes-operator.rs` to get a compiling tree and then saved that as "the full version" | Restored; `tests/operator_binary.rs` now fails if the binary stops advertising the launcher |
| `scripts/run-testnet.sh` failed with "cannot reach testnet" | One of the testnet's four DNS addresses black-holes connections and plain HTTP waits ~30 s on it | The script tries each address with a short timeout |
| Stale-data default | The testnet feed is quiet and flaky (silences over a minute, abrupt disconnects, measured) | Launcher defaults: 90 s on testnet, 30 s on mainnet |

**End-to-end, real:** through the supervisor, the real `apexes-engine` and `apexes-market-data-gateway` with Postgres and testnet keys launched, the console's authenticated client saw a snapshot with `network: testnet`, sane equity and the strategy's parameters, a `Halt` was acknowledged, and a clean stop ended the process (`crates/apexes-supervisor/tests/real_binaries.rs`, `#[ignore]`; needs keys).

### Not yet verified or built (be honest about these)

- **The GUI window driven by a human.** Only headless `egui_kittest` render tests exist; nobody has clicked through the real launcher and console. Specifically unverified for the theme/window work: that **System** follows the real OS setting (headless tests can only force Light or Dark), that the window position restores on a multi-monitor desk, and the look of the market dashboard (`apexes-ui`, wgpu candlesticks keep fixed colours).
- **The sample strategy trading on testnet:** now seen (buys and sells filled on ETHUSDT, positions and PnL moving in the snapshot). Not seen: a losing streak, a partial fill, or a session left running for hours.
- **`SetParam` taking effect on a running session** (sent and the parameter shown, but its effect on orders not observed), a **genuine** stale-feed halt (the one seen was the deadlock above, now fixed), **restart-and-reconcile**, an **unclean engine kill**, and behaviour across a **testnet reset**.
- **Screenshots of the GUI from an automated session are not possible here** (no screen-recording permission), so the chart fix is verified from the snapshot data the console renders (155 points over 287 s, chronological), not by eye.
- **Reconciliation vs. unresolvable local orders:** a row whose exchange id is not a Binance id still makes reconcile log `-1100` forever (live sessions no longer meet one, since they use their own database). Decide whether to mark such orders failed.
- **Testnet `GET /api/v3/order` can answer "Order does not exist" for an order it just accepted** (seen once). Reconcile logs and retries, which is right; the live test now polls.
- **Ed25519 / agent-held signing** (for APIs that require it): the `ParamSigner` trait is the seam; only HMAC is implemented. Two smoke scripts under `scripts/` prove both key types work against the testnet.
- Remaining plan items: the #13c plan's docs and final-review tasks below, a pass over `README`/`AGENTS`, and committing.

**Decisions made (2026-11-15, delegated by the owner):** one GUI crate (`apexes-ui`) over an `apexes-supervisor` library; engines outlive the GUI; secrets only by environment and 0600 files; `network` required; testnet data for testnet sessions; `egui_plot` **0.36** (0.35 targets egui 0.34, so it would have pulled in a second egui); standard evaluation metrics; mandatory risk limits; HMAC is the default signer and Ed25519 is for APIs that require it.

**Testing strategy (owner's instruction):** build everything first, then run one comprehensive end-to-end verification instead of a full pass per task. Applied here: the end-to-end run found a problem none of the earlier mock-based tests could (the dead listenKey API), which is the reason to run real binaries against the real service before calling anything done.

---

## Reshaped — remainder of #13c Strategy Monitoring & Live Tuning

- **Plan:** [`plans/2026-08-05-apexes-13c-strategy-monitoring-tuning.md`](superpowers/plans/2026-08-05-apexes-13c-strategy-monitoring-tuning.md) (14 tasks)
- **Spec:** [`specs/2026-08-05-apexes-13c-strategy-monitoring-tuning-design.md`](superpowers/specs/2026-08-05-apexes-13c-strategy-monitoring-tuning-design.md)
- **FEATURES rows:** 9.3 (strategy monitoring & live tuning panel; now 🚧 In Progress), plus the minimum viable slice of 8.3 (cross-process market-data gateway; still Not Started in the table)
- **Reconciliation after PR #9 (`e09cadd`):** a separate effort landed on `master` after Task 8. It delivered a standalone **`apexes-operator`** console (a second binary in the `apexes-ui` crate) covering most of the monitoring/tuning/control half of Task 11 and part of row 9.4, plus engine/gateway authentication, a kill switch, telemetry, CI, Docker and a runbook. The 13c plan was written before this and **has not been revised**; Tasks 9, 10 and 11 below must be re-scoped before dispatch (see "Decisions before resuming").
- **Architecture:** four OS processes — shared market-data gateway, one `apexes-engine` per session (backtest / paper / live), and `apexes-ui` as a thin supervisor and renderer. Diagram: [`specs/assets/2026-08-05-apexes-13c-communication-model-v2-multiprocess.svg`](superpowers/specs/assets/2026-08-05-apexes-13c-communication-model-v2-multiprocess.svg). The superseded in-process v1 diagram is kept for history.

| Task | Scope | State |
|---|---|---|
| 1 | `Strategy::current_params` / `set_param`; Drift Script `param()` hot-reload | ✅ done (`ea32bff`) |
| 2 | `ControlHandle` / `SessionSnapshot` / `SessionCommand` in the unified loop | ✅ done (`cf3022a`) |
| 3 | `core_affinity` dependency; gateway crate scaffold and broadcast core | ✅ done (`0caccd6`) |
| 4 | Gateway replay and async persistence (reuses `apexes-storage`) | ✅ done (`2b2d0e0`) |
| 5 | Gateway `main.rs`: CLI, CPU affinity, PID file, `/market` WebSocket | ✅ done (`ae4b2a9`) |
| 6 | `apexes-engine` scaffold: `LaunchConfig`; `apexes-strategy-loader` | ✅ done (`2c9a0ed`) |
| 7 | Engine `main.rs`: 3-mode dispatch, control wiring, credentials | ✅ done (`7250feb`, patch `dc11783`) |
| 8 | Engine `/session/ws` protocol and server | ✅ done (`f2d9ec2`, patch `f459d00`) |
| 9 | `apexes-ui` process supervisor (spawn/discover gateway, spawn engines, reconnect → respawn) | ⏭ next — brief written, not dispatched; needs re-scoping |
| 10 | `apexes-ui` session launcher tab | ⬜ not built (the `9.3` Notes in `FEATURES.md` list it as not done) |
| 11 | `apexes-ui` monitoring/tuning tab; `/session/ws` client extension | 🟡 mostly superseded: `apexes-operator` already does monitoring, `SetParam`, pause/resume/stop and a `/session/ws` client with auth. Automatic engine respawn is not built. |
| 12 | Docs: Drift Script `param()` hot-reload authoring convention | ⬜ |
| 13 | Full-workspace verification checkpoint | ⬜ |
| 14 | Final whole-branch review; `FEATURES.md` update (9.3, 8.3, recomputed counts); refresh `PLAN.md`, `README.md` status and `AGENTS.md` | ⬜ |

**Rough remaining effort (Tasks 9–14):** the earlier 4–6 hour estimate assumed the original plan. Task 11 has shrunk because of the operator console, but re-scoping Tasks 9–11 adds design work. Re-estimate once the decisions below are made.

**Decisions before resuming (Tasks 9-11)**
- **Where does the supervisor and launcher live?** The operator console was deliberately made a *separate binary* so it survives market-data failure, and `apexes-ui` (the dashboard) exits when its market-data subscription fails. Options: extend `apexes-operator` with the launcher and supervisor, keep them in `apexes-ui`, or add a third binary. This also decides what Task 11 still needs to build.
- **Engine invocation changed.** Engines now require control tokens for live (`APEXES_CONTROL_TOKENS` / `APEXES_CONTROL_TOKEN`) and the gateway takes `APEXES_GATEWAY_TOKEN`. A supervisor that spawns them must generate or pass these without leaking them on the command line. The Task 9 brief predates this and must be updated.
- `apexes-ui` depends on `apexes-strategy-loader`, not on the `apexes-engine` binary crate. Where `LaunchConfig` lives (engine lib vs. a shared crate) must be settled before dispatch. Note `LaunchConfig` gained `stale_data_timeout_secs` in PR #9.
- A cross-platform PID-liveness check needs a new dependency (e.g. `sysinfo`) or a Unix-only syscall. macOS/Linux is the de facto platform scope; document it rather than silently skipping Windows.
- **Task 14's final review checklist** must also cover PR #9's changes (the plan's "credentials never leave the engine" check now includes the token and audit-log paths).

---

## Next (not yet planned)

Ordered by the roadmap's "vertical slice first" principle. None of these has a spec yet; each starts with its own brainstorming → spec → plan cycle.

| Priority | Item | FEATURES rows | Notes |
|---|---|---|---|
| 1 | Remaining UI panels (#13) | 9.4, 9.5, 9.6 | 9.4 has an MVP in `apexes-operator`; still missing order entry/cancel, risk-limit editing and a multi-session view. 9.5 (backtest report) and 9.6 (monitoring wall) are not started. |
| 2 | Full message bus and relational storage (#12) | 8.2, 8.3, 8.4 | 13c delivers only a minimal gateway slice of 8.3. NATS JetStream or a lock-free ring buffer is still open. |
| 3 | Monitoring and ops (#14) | 10.1–10.4 | **Table lags the code:** PR #9 added Prometheus metrics, alert rules, a Grafana dashboard, CI and container images, which touch row 10.2 (CI, images and the runbook are deployment groundwork, not row 10.4's canary release and rollback). Audit row 10.2 and update it. OpenTelemetry tracing (10.1) and HA/DR (10.3) are not started. |
| 4 | Security and compliance (#15) | 6.3, 11.1–11.4 | **Table lags the code:** PR #9 added named operator/viewer bearer tokens and an audit log (touches 11.2). Credentials still come from environment variables; KMS/HSM, TLS/mTLS and compliance reporting are not started. |
| 5 | Market gateways beyond crypto (#10) | 1.1.2, 1.2.x, 1.3.x, 1.4.x, 5.3.2, 5.3.3 | Rows are *In Progress* in `FEATURES.md` with explicit scope boundaries in their Notes. |

---

## Carry-forward technical debt

- Simulated immediate market orders with insufficient visible depth currently remain New/PartiallyFilled despite no resting market-order mechanism; resolve under F03.02 with explicit terminal remainder semantics and tests.
- F00.01 local and remote CI qualification passed (including database integration tests), and PR #12 merged to master; live-exchange qualification remains pending. Synthetic data is not a profitability result.

Found during reviews and deliberately deferred. Close them in the 13c final review (Task 14) or schedule them as their own tasks.

- **Flaky test:** `apexes-live-trading::order_executor_adapter::tests::order_manager_executor_open_orders_reflects_submitted_orders` fails intermittently under parallel test threads; passes with `--test-threads=1`.
- `StrategyError::UnknownParam` is currently unused.
- `apexes-market-data-gateway`: `PersistenceBuffer::BatchVariant` duplicates a private type in `apexes-storage`.
- `Gateway::attach_persistence` can silently replace an existing buffer.
- `apexes-backtest/src/unified_loop.rs` is over the 800-line guideline (the evaluation hooks added more); consider extracting the snapshot/evaluation assembly.
- PyO3 bindings (part of row 2.1's full scope) remain deferred and unscheduled.
- **`FEATURES.md` rows 8.3, 9.3, 9.4, 10.2 and 11.2 were reconciled on 2026-11-15** against the code (PR #9 had updated only 9.3 and 9.4). Rows are claims about verified behaviour: 9.3 stays 🚧 until the unverified items under "Now" are checked. No `docs/superpowers/` spec or plan documents PR #9's design.
- **`apexes-oms` reconciliation vs. unresolvable local orders:** an order row whose exchange id is not a Binance numeric id makes every reconcile cycle log a `-1100` warning forever. Decide whether to mark such orders failed.
- **`apexes-ui` shell:** supervisor calls run on a worker thread, but `ShellApp` has no test that drives the real worker; only the state machine and headless renders are tested.
- **Gateway-binance `account_balances` over-reports for a Spot-only session** if the account holds assets nobody trades: the engine filters them for the ledger now, but the raw call still returns all of them.

---

## Completed plans

| Plan | Scope | Commits |
|---|---|---|
| Trading-loop unification | `run_backtest`, `run_paper_trading` and `run_live_trading` share one `run_unified_event_loop` via the `OrderExecutor`, `LiquidationPolicy` and `SupplementalFillSource` traits | `d413455`..`4fcf081` |
| #13c design and diagrams | Spec, 14-task plan, v1 and v2 architecture diagrams | `638f53a` |
| Production-readiness work (PR #9, merged separately) | Kill switch, startup recovery, crash-safe OMS persistence, resilient Binance streams, bearer-token auth and audit log, telemetry, CI, Docker, runbook, `apexes-operator` console | `e30d4a6`..`64c4360`, merge `e09cadd` (no plan file under `docs/superpowers/plans/`) |

Earlier sub-projects (#0–#12 partial and the plans under `docs/superpowers/plans/`) are recorded in [`FEATURES.md`](FEATURES.md); their plan files are the historical record.

---

## How work is executed

Each plan is run with subagent-driven development: one implementer subagent per task, then an independent reviewer subagent. The controller verifies claims against the real code before dispatch, and any gap found is fixed immediately rather than deferred. Commits are signed (`git commit -S -s`) and use conventional-commit messages. A per-task ledger is kept under `.superpowers/sdd/` (local working state, not part of the published docs).
