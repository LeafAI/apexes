# Apexes quantitative trading enhancement — feature plan

Design: [quantitative enhancement design](../specs/2026-11-25-apexes-quantitative-enhancement-design.md).
Authoritative status: [FEATURES.md](../../FEATURES.md). Each feature gets a
focused design, regression tests and a reviewable change; no parallel status
checklist is maintained here.

| ID | Feature | Acceptance criterion | Prerequisites |
|---|---|---|---|
| F00.01 | Reproducible baseline | Real Drift replay deterministic economically; isolated fixtures; documented test commands and limits | — |
| F00.02 | Decision/execution diagnostics | Correlate decision, refusal, order, fill and ledger; redact secrets | F00.01 |
| F01.01 | Exchange clock sync | Clock skew and -1021 handled without duplicate orders | F00.01 |
| F01.02 | Reliable submission | Lost response reconciled by persistent client ID | F01.01 |
| F01.03 | Market-data recovery | Per-symbol freshness and book recovery prevent unsafe new exposure | F00.01 |
| F01.04 | EMS database decoupling | No inactive POV queries on hot path; outage safely restricts trading | F00.01 |
| F01.05 | Session health | Explicit degraded/reconciling/halted states; safe recovery and operator visibility | F01.02–04 |
| F02.01 | Spot position recovery | Strategy inventory and cost basis survive restart | F00.01 |
| F02.02 | Fill reconciliation | Duplicate/delayed/out-of-order fills cannot corrupt the ledger; discrepancy blocks new exposure | F02.01, F01.02 |
| F02.03 | Fee and P&L accounting | Base, quote and third-party fees reconcile to equity | F02.02 |
| F02.04 | Protective exits | Entry caps and warm-up alone never block validated reduction; cannot increase exposure | F02.01–02 |
| F02.05 | Dynamic trading rules | Valid quantities, notionals and reserved balances across symbols | F02.01 |
| F03.01 | Configurable costs | Explicit fees change P&L by expected amount; zero fees labeled | F00.01 |
| F03.02 | Execution simulation | Causal depth/spread/latency and partial-fill semantics, no phantom working market orders | F03.01 |
| F03.03 | Historical datasets | Versioned production data, completeness checks and deduplication | F00.01 |
| F03.04 | Deterministic replay | Causal stable replay; reproducible execution/equity | F03.02–03 |
| F03.05 | Performance evaluation | Net/gross P&L, benchmarks, turnover, expectancy, drawdown and exit attribution | F02.03, F03.04 |
| F04.01 | Per-symbol state | Interleaved instruments do not contaminate indicators or counters | F00.01 |
| F04.02 | Time-based events | Closed bars and elapsed-time cooldowns independent of trade rate | F04.01 |
| F04.03 | Context API | Trading rules, book, costs, balances and health available to scripts | F02.05, F03.01–02 |
| F04.04 | Lifecycle callbacks | Accepted, rejected and partial-fill outcomes delivered deterministically | F02.02, F04.01 |
| F04.05 | Persistent state/warm-up | Restart restores risk-relevant state; causal history warm-up | F04.01–04 |
| F05.01 | Research framework | Versioned strategies, datasets, hypotheses and configurations reproducible | F03.05 |
| F05.02 | Trend Guard V2 | Compare cost-aware, time-based V2 to unchanged V1 on same data | F04.01–05, F05.01 |
| F05.03 | Alternative strategies | Reference trend, mean-reversion, breakout and relative-strength candidates | F05.01, F04.03 |
| F05.04 | Parameter optimization | Reproducible constrained multi-objective experiments | F05.01 |
| F05.05 | Walk-forward validation | Unseen periods, final holdout, cost and regime stress tests; explicit verdict | F05.02–04 |
| F06.01 | Capital allocation | Concurrent strategies cannot double-spend available balance | F02.05, F04.01 |
| F06.02 | Portfolio risk | Filled and pending exposure constrained account-wide | F06.01 |
| F06.03 | Multi-strategy orchestration | Independent positions, orders, fills and P&L attribution | F06.01–02 |
| F07.01 | Promotion lifecycle | Only approved version/config/dataset verdict can be promoted | F05.05 |
| F07.02 | Operator monitoring | Reasons for blocked orders, health and P&L changes visible | F00.02, F01.05, F03.05 |
| F07.03 | Production validation | Fault/restart/kill/flatten drills and no unresolved critical defects | F01–F06, F07.01–02 |

## F00.01 verification

From repository root:

```sh
cargo +1.99.0 test --locked -p apexes-backtest --test trend_guard_baseline -- --test-threads=1
cargo +1.99.0 test --locked -p apexes-backtest -- --test-threads=1
cargo +1.99.0 test --locked -p apexes-paper -- --test-threads=1
cargo +1.99.0 clippy --locked -p apexes-backtest -p apexes-paper --all-targets -- -D warnings
cargo +1.99.0 fmt --all -- --check
```

The new baseline requires no credentials, PostgreSQL, ClickHouse or network.
The pre-existing compose-based database and mock exchange tests are reused
for subsequent integration qualification, not replaced. The original
strategy SHA-256 is
da989984f2d0bdfc3bf5a2d39c960034815b1be5fb9689b225ed3ab6c9c44a06.
The synthetic test uses 1,000 USDT and explicit 0/10 bps taker costs, not the
engine launch default or actual Binance fees. UUIDs are excluded from
economic report comparison because matching currently generates random IDs.
Production dataset replay, database qualification and testnet recovery are
not established by this fixture.

### Local qualification evidence (2026-11-26)

Using Rust 1.99.0, the full workspace build, formatting, diff check and
Clippy with warnings denied passed. All 78 compiled test binaries were
exercised across split runs: 1,295 passed, 0 failed, 75 ignored; all 30
doc-test suites passed. For PostgreSQL-dependent default tests, a new
temporary PostgreSQL 17 cluster (loopback port 5432, database and role
apexes) was created only after verifying the port was unused. The cluster
was stopped after each run. No existing database was started or truncated.
The interrupted supervisor suite was resumed for its remaining 18 tests,
and the final UI suites were run directly from Cargo's compiled binary
list. No exchange orders were placed. ClickHouse and ignored external
integration tests, remote CI and merge remain unverified. The 20 existing
local-only commits must not be implicitly included in this feature's push.

## Sequencing

Begin with F00.01 and F00.02. Prioritize safety-critical F01/F02 work and
F03.01 cost modeling in parallel where dependencies permit. Phase 1/2
completion requires fault-injection and ledger reconciliation; Phase 3
requires realistic historical replay. Phase 4 requires backwards-compatible
Drift tests; Phase 5 requires reproducible out-of-sample evidence, not a
promised profit. Phase 6 is portfolio-safe concurrency. Phase 7 requires
explicit approval, operational recovery and risk gates before mainnet.
