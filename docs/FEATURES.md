# Apexes — Feature Tracking

> This is the **single source of truth** for feature-level status across the Apexes project. It is derived from the functional breakdown in [`docs/superpowers/specs/2026-07-28-apexes-vision-and-roadmap.md`](superpowers/specs/2026-07-28-apexes-vision-and-roadmap.md) (§2.2) and mapped to the sub-project roadmap (§3) in that same document.
>
> **Maintenance rule:** every feature row must map to exactly one roadmap sub-project (`#0`–`#15`). When a sub-project's design/plan/implementation cycle starts or finishes, update the status of every feature row it owns. Do not track feature status anywhere else (no parallel TODO lists) — this file is authoritative.

## Status legend

| Symbol | Meaning |
|---|---|
| 🔲 Not Started | No design work has begun |
| 📝 Designing | Brainstorming/spec in progress for the owning sub-project |
| 📋 Planned | Spec approved, implementation plan written, not yet started |
| 🚧 In Progress | Implementation underway |
| ✅ Done | Implemented, tested, merged |
| ⏸️ Deferred | Explicitly out of scope for now (see Notes) |

## Roadmap phase key

Sub-project numbers refer to the table in §3 of the vision/roadmap doc: `#0` Foundation skeleton, `#1` Market data ingestion, `#2` Historical storage, `#3` Backtest engine, `#4` Strategy SDK, `#5` Risk engine, `#6` Paper trading, `#7` OMS, `#8` First live gateway (crypto), `#9` EMS/execution algos, `#10` Additional market gateways, `#11` Account/fund management, `#12` Message bus & full storage, `#13` UI, `#14` Monitoring/ops, `#15` Security/compliance.

---

## 1. Market Data & Data Ingestion

| ID | Feature | Sub-project | Status | Notes |
|---|---|---|---|---|
| 1.1.1 | CEX REST/WebSocket market data (Binance, OKX, Coinbase, etc.) | #1 | ✅ Done | Binance Spot implemented via apexes-market-data (BinanceMarketDataProvider); OKX/Coinbase/other CEX deferred |
| 1.1.2 | DEX on-chain data (The Graph, DEX pool event parsing) | #10 | 🔲 Not Started | |
| 1.1.3 | Public chain RPC access & node management (free: Infura/Alchemy; paid: self-hosted full node/QuickNode) | #10 | 🔲 Not Started | |
| 1.1.4 | Mempool monitoring & block event subscription | #10 | 🔲 Not Started | |
| 1.2.1 | A-share free data sources (AKShare, Tushare, Sina/Eastmoney) | #10 | 🔲 Not Started | |
| 1.2.2 | A-share paid Level-2 tick data (Hundsun UF2.0, ZTP XTP, Huaxin Qidian) | #10 | 🔲 Not Started | |
| 1.2.3 | Call auction, order-by-order commit/fill parsing | #10 | 🔲 Not Started | |
| 1.3.1 | H-share free data (Futu OpenAPI, Longbridge OpenAPI) | #10 | 🔲 Not Started | |
| 1.3.2 | H-share paid HKEX direct connect (OMD-C) | #10 | 🔲 Not Started | |
| 1.4.1 | US equity free/low-cost data (IEX Cloud, Alpaca, Polygon free tier) | #10 | 🔲 Not Started | |
| 1.4.2 | US equity paid full-depth data (Polygon.io paid, Databento, Nasdaq TotalView) | #10 | 🔲 Not Started | |
| 1.5 | Market data normalization engine (unified Tick/Depth/Bar model) | #0 | ✅ Done | `MarketEvent` enum defined in foundation skeleton design |
| 1.6 | Historical data storage & backfill (full tick-level, multi-timeframe bars) | #2 | ✅ Done | ClickHouse-backed via apexes-storage (StorageWriter/StorageReader); backfill from external sources deferred |
| 1.7 | Market data distribution bus (multi-subscriber broadcast, on-demand downsampling) | #12 | 🔲 Not Started | |

## 2. Strategy & Algorithm Research Platform

| ID | Feature | Sub-project | Status | Notes |
|---|---|---|---|---|
| 2.1 | Strategy development SDK (Rust native trait API + PyO3 bindings for research) | #4 | ✅ Done | Strategy trait/StrategyContext/OrderIntent/Position migrated to new apexes-strategy crate (was in apexes-backtest); DriftScriptStrategy runs .drift scripts via drift-dsl. PyO3 bindings deferred to a future pass. |
| 2.2 | Factor library & technical indicator library | #4 | 🔲 Not Started | |
| 2.3 | Multi-signal combination & position generation engine | #4 | 🔲 Not Started | |
| 2.4 | Parameter optimization interface (grid/Bayesian, pluggable external ML service) | #4 | 🔲 Not Started | |
| 2.5 | Strategy version management & canary release | #4 | 🔲 Not Started | |

## 3. Backtest System

| ID | Feature | Sub-project | Status | Notes |
|---|---|---|---|---|
| 3.1 | Historical data replay engine (tick-level exact replay, variable speed) | #3 | ✅ Done | Implemented in apexes-backtest via HistoricalMarketDataSource + BacktestClock (AsFastAsPossible/Multiplier/RealTime) |
| 3.2 | Matching simulator (order book reconstruction, probabilistic fill model, Level-2 depth consumption) | #3 | ✅ Done | MatchingEngine: ReplayOrderBook (reuses barter-data's OrderBook), book-walk VWAP fills, TradeEvent-driven limit-order fills with configurable queue_position_factor, Stop/StopLimit trigger simulation |
| 3.3 | Cost model (slippage, fees, market impact, funding rate) | #3 | ✅ Done | CostModel: venue-aware FeeSchedule trait (BinanceSpotVipFeeSchedule with real confirmed VIP0-9 rates, FlatFeeSchedule fallback); slippage/market-impact emerges from book-walk fills. NOTE: funding_rate_bps calculation exists on CostModel but is not yet wired into run_backtest's event loop (no periodic funding-period pass) -- spot-only backtests unaffected (set funding_rate_bps: None); perpetual funding application deferred to a future iteration |
| 3.4 | Performance attribution & risk metrics (Sharpe, max drawdown, turnover, etc.) | #3 | ✅ Done | PerformanceTracker via barter::statistic::metric (Sharpe/Sortino/Calmar/drawdown/win-rate/profit-factor), overall + per-symbol + per-venue breakdown |
| 3.5 | Backtest report generation & visualization | #3 | ✅ Done | BacktestReport: structured, JSON-serializable data only; no rendering/charting (deferred to sub-project #13 per design spec §8) |

## 4. Paper Trading System

| ID | Feature | Sub-project | Status | Notes |
|---|---|---|---|---|
| 4.1 | Virtual account & virtual fund management | #6 | 🔲 Not Started | |
| 4.2 | Real-time-feed-driven paper matching engine (reuses backtest matching kernel) | #6 | 🔲 Not Started | |
| 4.3 | Order lifecycle state machine consistent with live trading | #0 / #6 | ✅ Done | `OrderStatus` state machine defined in foundation skeleton; reused as-is by paper trading |

## 5. Live Trading System

| ID | Feature | Sub-project | Status | Notes |
|---|---|---|---|---|
| 5.1.1 | Order generation & pre-trade validation | #7 | 🔲 Not Started | |
| 5.1.2 | Order state machine (New→PartiallyFilled→Filled/Cancelled/Rejected) | #0 | ✅ Done | Core type-level definition; OMS (#7) builds the runtime around it |
| 5.1.3 | Persistence & reconnect state recovery (reconciliation) | #7 | 🔲 Not Started | Flagged as non-negotiable quality bar (see vision doc §1) |
| 5.2.1 | Smart order routing (SOR) — multi-exchange/multi-market-maker price comparison | #9 | 🔲 Not Started | |
| 5.2.2 | Execution algorithms (TWAP/VWAP/Iceberg/POV) | #9 | 🔲 Not Started | |
| 5.2.3 | Cross-market order splitting & consolidated execution | #9 | 🔲 Not Started | |
| 5.3.1 | Crypto gateway (CEX private API + DEX smart-contract interaction/signing) | #8 | 🔲 Not Started | First live gateway target |
| 5.3.2 | A-share broker gateway (CTP, QMT, Hundsun O32, ZTP XTP) | #10 | 🔲 Not Started | |
| 5.3.3 | H-share/US equity broker gateway (IBKR TWS API, Futu, Longbridge) | #10 | 🔲 Not Started | |
| 5.4 | FIX protocol engine (institutional counterparties, optional module) | #10 | ⏸️ Deferred | Optional module per original design; revisit if institutional connectivity needed |

## 6. Risk Management System

| ID | Feature | Sub-project | Status | Notes |
|---|---|---|---|---|
| 6.1 | Pre-trade risk controls (limits, black/white lists, self-trade/abnormal-price prevention) | #5 | 🔲 Not Started | Flagged as non-negotiable quality bar |
| 6.2 | In-trade risk controls (real-time position monitoring, anomaly circuit breaker) | #5 | 🔲 Not Started | Flagged as non-negotiable quality bar |
| 6.3 | Post-trade risk & compliance reporting | #15 | 🔲 Not Started | |
| 6.4 | Margin & leverage monitoring (crypto derivatives focus) | #5 | 🔲 Not Started | |
| 6.5 | Extreme-market circuit breaker & automatic strategy de-risking | #5 | 🔲 Not Started | |

## 7. Account & Fund Management

| ID | Feature | Sub-project | Status | Notes |
|---|---|---|---|---|
| 7.1 | Unified multi-market, multi-account management | #11 | 🔲 Not Started | |
| 7.2 | Fund transfer, settlement & reconciliation | #11 | 🔲 Not Started | |
| 7.3 | Real-time position & P&L calculation (incl. cross-currency/FX conversion) | #11 | 🔲 Not Started | |

## 8. Data Storage & Messaging Middleware

| ID | Feature | Sub-project | Status | Notes |
|---|---|---|---|---|
| 8.1 | Time-series database (ClickHouse/QuestDB — tick & K-line storage) | #2 | ✅ Done | Implemented in apexes-storage: 4 MergeTree tables (trades/ticks/depth_updates/bars), Decimal(38,10) precision |
| 8.2 | Relational database (PostgreSQL — config/account/order storage) | #7 / #12 | 🔲 Not Started | Introduced when OMS needs durable order storage |
| 8.3 | Message bus (NATS JetStream or self-built lock-free ring buffer) | #12 | 🔲 Not Started | |
| 8.4 | In-memory cache (Redis or self-built shared-memory cache) | #12 | 🔲 Not Started | |

## 9. High-Performance UI Module

| ID | Feature | Sub-project | Status | Notes |
|---|---|---|---|---|
| 9.1 | Real-time market dashboard (Level-2 depth, tick-by-tick trade heatmap) | #13 | 🔲 Not Started | |
| 9.2 | High-performance K-line/tick chart engine (GPU rendering, wgpu) | #13 | 🔲 Not Started | |
| 9.3 | Strategy monitoring & live tuning panel | #13 | 🔲 Not Started | |
| 9.4 | Order/position/risk management UI | #13 | 🔲 Not Started | |
| 9.5 | Interactive backtest report visualization | #13 | 🔲 Not Started | |
| 9.6 | Multi-screen system monitoring wall (latency, throughput, error rate) | #13 | 🔲 Not Started | |

## 10. System Monitoring & Operations

| ID | Feature | Sub-project | Status | Notes |
|---|---|---|---|---|
| 10.1 | Distributed logging & full-chain tracing (tracing + OpenTelemetry) | #14 | 🔲 Not Started | |
| 10.2 | Metrics collection & alerting (Prometheus + Grafana) | #14 | 🔲 Not Started | |
| 10.3 | High availability & disaster recovery (multi-active, Raft state replication, sub-second failover) | #14 | 🔲 Not Started | |
| 10.4 | Canary release & one-click rollback | #14 | 🔲 Not Started | |

## 11. Security & Compliance

| ID | Feature | Sub-project | Status | Notes |
|---|---|---|---|---|
| 11.1 | Key/private key management (KMS/HSM, on-chain wallet private key protection priority) | #15 | 🔲 Not Started | |
| 11.2 | RBAC permission system & operation audit | #15 | 🔲 Not Started | |
| 11.3 | Network security (leased line, VPN, DDoS protection, mutual TLS) | #15 | 🔲 Not Started | |
| 11.4 | Regulatory compliance (A-share penetrating supervision reporting, offshore market compliance) | #15 | 🔲 Not Started | |

## 0. Foundation (cross-cutting, underpins all of the above)

| ID | Feature | Sub-project | Status | Notes |
|---|---|---|---|---|
| 0.1 | Cargo workspace layout (`apexes-core`, `apexes-cli`) | #0 | ✅ Done | |
| 0.2 | `Instrument` type (unified identity across crypto/A-share/H-share/US equity) | #0 | ✅ Done | Two-level `MarketType`/`Venue` + `AssetClass` enums, decided |
| 0.3 | `Order` / `Fill` types + `OrderStatus` state machine | #0 | ✅ Done | Full state machine with validated transitions, decided |
| 0.4 | `MarketDataProvider` trait | #0 | ✅ Done | Async streaming trait signature, decided |
| 0.5 | `MarketEvent` model (Tick/Depth/Bar/Trade) | #0 | ✅ Done | Full enum with real payload fields, decided |
| 0.6 | `ApexesError` unified error type | #0 | ✅ Done | Single `thiserror` enum, decided |
| 0.7 | `apexes-cli` skeleton (`--version`/`--help` only) | #0 | ✅ Done | |

---

## Summary

| Status | Count |
|---|---|
| 🔲 Not Started | 50 |
| 📝 Designing | 0 |
| 📋 Planned | 0 |
| 🚧 In Progress | 0 |
| ✅ Done | 19 |
| ⏸️ Deferred | 1 |
| **Total** | **70** |

**Current active work:** Sub-projects `#0` (Foundation skeleton), `#1` (Binance Spot market data ingestion), `#2` (historical data storage — ClickHouse via `apexes-storage`), and `#3` (backtest engine via `apexes-backtest`) — all implementation complete (all plan tasks done, workspace builds/tests/clippy/fmt clean, live ClickHouse integration tests passing). Sub-project `#4`'s first phase (Rust-native strategy trait API + Drift Script integration, row `2.1`) is now also complete via the new `apexes-strategy` crate. Remaining `#4` work (`2.2` factor/indicator library, `2.3` multi-signal combination, `2.4` parameter optimization, `2.5` strategy versioning, plus PyO3 bindings) and sub-project `#5` (risk engine) are both unblocked and available next; which to pursue is not yet decided.
