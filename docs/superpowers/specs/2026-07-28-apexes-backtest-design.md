# Apexes Backtest Engine (Sub-project #3) — Design Spec

Status: Approved (pending final Binance fee-schedule numbers from user)
Depends on: sub-project #0 (`apexes-core`), sub-project #2 (`apexes-storage`)
Crate: `apexes-backtest`

## 1. Scope

Per the vision/roadmap doc (`docs/superpowers/specs/2026-07-28-apexes-vision-and-roadmap.md`,
row #3), this sub-project implements:

- Tick-level exact replay of historical market data (variable speed).
- A matching simulator: order book reconstruction from `DepthUpdate` snapshots/diffs,
  book-walk fills for market orders, deterministic depth-consumption fills for limit orders
  with an optional probabilistic queue-position adjustment, and Stop/StopLimit trigger
  simulation.
- A cost model: venue-aware maker/taker fee schedules, funding rate for perpetuals, and
  market-impact/slippage naturally produced by book-walk fills.
- Performance attribution & risk metrics: Sharpe, Sortino, Calmar, max drawdown, win rate,
  profit factor, P&L/turnover, broken down overall and per-symbol/per-venue.
- Backtest report generation as structured, serializable data (no charting/rendering — that
  is sub-project #13's responsibility).

## 2. Non-negotiable constraint: backtest/live logic parity

Per the project's final objective, backtest and live trading must never diverge in strategy
logic (the "profitable in backtest, losing in live" failure mode is explicitly called out as
unacceptable). This sub-project therefore defines the **`Strategy` trait** as the shared
interface that later live-trading sub-projects (#6 Paper Trading, #7 OMS, #8 live crypto
gateway) will implement against unchanged. Only what sits behind `StrategyContext` and order
execution differs between backtest (simulated ledger + matching engine) and live (real
broker connection) — the strategy's own code never needs to know which mode it's running in.

## 3. OSS reuse decision (barter-rs) and why

`apexes-market-data` (#1) already depends on `barter-data`/`barter-instrument` as isolated
connector implementations. For this sub-project, the `barter` meta-crate (a distinct crate
from `barter-data`, published separately, v0.12.5) was evaluated as a candidate for the whole
backtest runtime — it ships an `Engine`/`EngineState` orchestration layer, `AlgoStrategy`/
`RiskManager`/`ExecutionClient` traits, a `MockExchange`/`MockExecution` backtest fill
simulator, a `backtest::run_backtests()` convenience function, and a `statistic` module with
Sharpe/Sortino/Calmar/drawdown/win-rate/profit-factor calculations and `TradingSummary`/
`TearSheet` reporting.

**Decision: apexes-native backtest engine, narrow reuse of `barter::statistic::metric` only.**

Investigation findings that drove this decision:

1. **`barter-execution`'s `MockExchange` is too simplistic for this sub-project's own
   requirements.** It only supports `OrderKind::Market` (rejects everything else),
   fills instantly and completely at the exact requested price (no slippage, no
   market-impact, no depth consumption, no probabilistic fill model), and its
   `cancel_order()` is `unimplemented!()` (panics if called). This directly contradicts the
   "matching simulator (order book reconstruction, probabilistic fill model, Level-2 depth
   consumption)" and "cost model (slippage, fees, market impact)" requirements from the
   roadmap — adopting it as-is would produce a backtest that always fills perfectly, the
   opposite of what this sub-project exists to build, and a real source of backtest/live
   logic drift.
2. **A "hybrid" approach (write a custom `ExecutionClient` implementing barter-execution's
   trait, plug it into barter's `Engine`) was evaluated and rejected.** Barter's convenience
   functions `backtest()`/`run_backtests()` are hardcoded to only accept
   `ExecutionConfig::Mock(...)` — there is no way to plug a custom `ExecutionClient` into
   them. The lower-level `ExecutionBuilder::add_live<Client: ExecutionClient>()` is generic
   enough to accept a custom client, but using it requires bypassing `backtest()`/
   `run_backtests()` entirely and hand-assembling `Engine` + `SystemBuilder` +
   `ExecutionBuilder` + `HistoricalClock` wiring ourselves — essentially reimplementing
   barter's own internal orchestration. Furthermore, `ExecutionClient` is a broker-shaped
   trait (`account_snapshot`, `account_stream`, `fetch_balances`, `fetch_open_orders`,
   `fetch_trades`, `cancel_order`, `open_order`) designed for talking to a real exchange's
   REST/WS API — a pure matching-engine simulator doesn't naturally fit this shape, and
   every fill/trade produced would have to be shaped as barter's own `Trade`/`AccountEvent`/
   `Order<ExchangeId, InstrumentNameExchange, ...>` types rather than `apexes_core::Fill`/
   `Order`, reintroducing the type-authority problem one layer down.
3. **`barter::statistic::metric` (Sharpe/Sortino/Calmar/drawdown/win-rate/profit-factor) has
   zero dependency on barter's `engine`/`execution`/`strategy`/`system` modules** (confirmed
   by inspecting `statistic/metric/*.rs`'s imports directly) — it is cleanly reusable as a
   narrow, internal-only utility dependency, exactly like `fixnum` in `apexes-storage`: extra
   transitive compile weight, zero public-API leakage risk, since we only call its pure
   calculation functions/types and never implement or expose any of its traits.

`apexes-core::{Order, Fill, OrderStatus, MarketEvent, DepthUpdate, PriceLevel, TradeEvent}`
remain the single authoritative shared vocabulary across backtest and all future
live-trading sub-projects (§6 below adds Stop/StopLimit support to `Order`/`OrderType` as a
backward-compatible extension, but no existing field or the `OrderStatus` state machine
changes).

## 4. Architecture / Pipeline

```
HistoricalMarketDataSource (trait)
  ├─ StorageMarketDataSource   (wraps apexes_storage::StorageReader — real usage)
  └─ InMemoryMarketDataSource  (Vec<MarketEvent> — fast, network-free unit tests)
        │
        ▼
BacktestClock (variable-speed replay pacing; logical time always from event timestamps)
        │
        ▼
Strategy::on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent>
        │
        ▼
MatchingEngine (order book reconstruction, depth-consumption fills, cost model)
        │
        ▼
Fill (apexes_core::Fill)
        │
        ▼
PortfolioLedger (multi-asset balances + per-symbol positions)
        │
        ▼
PerformanceTracker (equity curve + metrics via barter::statistic::metric)
        │
        ▼
BacktestReport (JSON-serializable; no rendering)
```

## 5. Strategy-facing trait (shared with future live trading)

```rust
pub trait Strategy: Send {
    /// Called once per incoming MarketEvent, in timestamp order.
    fn on_market_event(
        &mut self,
        event: &MarketEvent,
        ctx: &StrategyContext,
    ) -> Vec<OrderIntent>;
}

pub struct StrategyContext<'a> {
    pub positions: &'a HashMap<String, Position>,   // keyed by symbol
    pub balances: &'a HashMap<String, Decimal>,     // keyed by asset (e.g. "USDT", "BTC")
    pub open_orders: &'a HashMap<OrderId, Order>,
    pub now: DateTime<Utc>,
}

pub struct OrderIntent {
    pub symbol: String,
    pub venue: Venue,
    pub side: Side,
    pub order_type: OrderType,        // Market | Limit | Stop | StopLimit
    pub quantity: Decimal,
    pub price: Option<Decimal>,       // limit price (Limit, StopLimit)
    pub stop_price: Option<Decimal>,  // trigger price (Stop, StopLimit)
}

pub struct Position {
    pub symbol: String,
    pub quantity: Decimal,            // signed: positive = long, negative = short
    pub average_entry_price: Decimal,
}
```

`Strategy` has no knowledge of backtest vs. live — it only ever sees `apexes_core::MarketEvent`
and emits `OrderIntent`s. Later sub-projects implement this exact trait; only the mechanism
behind `StrategyContext`/order routing changes.

## 6. apexes-core changes: Stop/StopLimit order types

`apexes-core::OrderType` (currently `Market`/`Limit` only) gains two new variants:

```rust
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
}
```

`apexes-core::Order` gains a new field:

```rust
pub struct Order {
    // ...existing fields unchanged...
    pub price: Option<Decimal>,       // limit price (Limit, StopLimit); None for Market/Stop
    pub stop_price: Option<Decimal>,  // NEW: trigger price (Stop, StopLimit); None otherwise
    // ...
}
```

- `Stop`: `stop_price` set, `price` is `None` — triggers a Market order once the trigger price
  is touched.
- `StopLimit`: both `stop_price` and `price` set — triggers a Limit order (at `price`) once
  the trigger price is touched.
- `Market`/`Limit`: `stop_price` is always `None` (unchanged behavior).

This is a backward-compatible, additive change to `apexes-core` (new enum variants + new
`Option` field defaulting to `None`); no existing `OrderStatus` state-machine logic changes.

## 7. Matching Engine

### 7.1 Order book reconstruction

Maintained per `(symbol, venue)` from `apexes_core::DepthUpdate` snapshots (`is_snapshot:
true`) and incremental diffs, reusing the sequence/gap-detection convention already
established for live Binance depth handling in `apexes-market-data`, adapted for replay
(sequential historical events rather than a live WebSocket) rather than reconnect-driven
resync.

### 7.2 Market order fills — book-walk

Consume price levels from best to worst (bids for sell orders, asks for buy orders) until the
order's quantity is filled, producing a volume-weighted average fill price. If total visible
depth is insufficient to fill the full requested quantity, fill whatever is available and
leave the remainder unfilled — logged via `tracing::warn` as a partial-liquidity event, not
treated as an error.

### 7.3 Limit order fills — deterministic depth-consumption + optional probabilistic layer

Limit orders rest in a simulated order queue at their price. On each subsequent
`DepthUpdate`/`TradeEvent`, the engine checks whether the market has traded through the
order's limit price; once triggered, it consumes visible volume at-or-better exactly like a
market order (deterministic by default).

A configurable `queue_position_factor: Decimal` (range `0.0`–`1.0`) discounts how much of the
visible resting volume at a price level must be consumed by other market participants before
our simulated order is considered "next in queue" and can fill:
- `0.0` — optimistic: our order is always assumed to be at the front of the queue.
- `1.0` — pessimistic/conservative: our order must wait for all visible resting volume at that
  price to trade through first.

Recommended default: a conservative non-zero value (e.g. `0.5`), configurable per
`BacktestConfig`.

### 7.4 Stop / StopLimit simulation

Stop/StopLimit orders do not rest in the order book. The matching engine tracks their trigger
condition against incoming `TradeEvent`/`Tick` prices; once the trigger price is touched, the
order converts to a Market order (`Stop`) or Limit order (`StopLimit`) and is processed via
§7.2/§7.3 respectively.

### 7.5 Cost model

```rust
pub trait FeeSchedule: Send + Sync {
    fn maker_fee_bps(&self, venue: Venue) -> Decimal;
    fn taker_fee_bps(&self, venue: Venue) -> Decimal;
}

/// Binance Spot's real VIP tier fee table (VIP0/Regular..VIP9), standard
/// (non-BNB-discounted) rates, confirmed by the user directly from
/// binance.com/en/fee/schedule on 2026-07-28:
///
/// | Tier | Maker (bps) | Taker (bps) |
/// |------|-------------|-------------|
/// | 0 (Regular) | 10.0 | 10.0 |
/// | 1    | 9.0  | 10.0 |
/// | 2    | 8.0  | 10.0 |
/// | 3    | 4.0  | 6.0  |
/// | 4    | 4.0  | 5.2  |
/// | 5    | 2.5  | 3.1  |
/// | 6    | 2.0  | 2.9  |
/// | 7    | 1.9  | 2.8  |
/// | 8    | 1.6  | 2.5  |
/// | 9    | 1.1  | 2.3  |
///
/// BNB-fee-discount (25% off) and USDC-pair-specific rates are out of scope
/// for this sub-project; only standard spot rates are modeled.
pub struct BinanceSpotVipFeeSchedule {
    pub tier: u8,  // 0..=9
}

/// Fallback for any venue without a dedicated fee-schedule implementation yet.
pub struct FlatFeeSchedule {
    pub maker_bps: Decimal,
    pub taker_bps: Decimal,
}

pub struct CostModel {
    pub fee_schedule: Box<dyn FeeSchedule>,
    /// Applied periodically (every 8h simulated-time) to open positions on
    /// `AssetClass::Perpetual` instruments only; `None` for spot/equity.
    pub funding_rate_bps: Option<Decimal>,
}
```

Market-impact/slippage is produced naturally by the book-walk fill logic (§7.2/§7.3) — no
separate slippage parameter is needed. Fees apply per fill: maker rate for resting-limit-order
fills, taker rate for market-order/aggressive fills. Funding applies periodically to open
perpetual positions only.

## 8. Backtest Runner, Clock, Performance Tracking, Report

```rust
pub struct BacktestConfig {
    pub instruments: Vec<Instrument>,
    pub date_range: std::ops::Range<chrono::NaiveDate>,
    /// Required — no implicit default. Keyed by asset symbol (e.g. "USDT", "BTC").
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub speed: ReplaySpeed,
    /// 0.0 (optimistic) .. 1.0 (pessimistic), see §7.3.
    pub queue_position_factor: Decimal,
}

pub enum ReplaySpeed {
    AsFastAsPossible,     // default: no delay between events
    Multiplier(f64),      // e.g. 10.0 = 10x real-time
    RealTime,             // 1:1 wall-clock pacing
}

pub async fn run_backtest(
    config: BacktestConfig,
    market_data: impl HistoricalMarketDataSource,
    strategy: impl Strategy,
) -> Result<BacktestReport, ApexesError>;
```

**Clock semantics:** logical time for all strategy/risk/funding-period calculations is always
derived from event timestamps, regardless of `ReplaySpeed`. `AsFastAsPossible` (default)
processes events back-to-back with zero artificial delay. `Multiplier`/`RealTime` insert real
`tokio::time::sleep` calls between events purely for demo/visual-inspection pacing — they
never affect calculated results, and this same replay mechanism is expected to be reusable by
sub-project #6 (Paper Trading) later.

**Performance tracking:** an equity-curve time series (mark-to-market portfolio value sampled
at each processed event) feeds `barter::statistic::metric::{sharpe, sortino, calmar, drawdown,
win_rate, profit_factor}`. `PerformanceSummary` is computed overall and broken down
per-symbol and per-venue.

```rust
pub struct PerformanceSummary {
    pub total_pnl: Decimal,
    pub sharpe: Decimal,
    pub sortino: Decimal,
    pub calmar: Decimal,
    pub max_drawdown: Decimal,
    pub win_rate: Decimal,
    pub profit_factor: Decimal,
    pub turnover: Decimal,
    pub trade_count: u64,
}

pub struct BacktestReport {
    pub overall: PerformanceSummary,
    pub per_symbol: HashMap<String, PerformanceSummary>,
    pub per_venue: HashMap<Venue, PerformanceSummary>,
    pub equity_curve: Vec<(DateTime<Utc>, Decimal)>,
    pub trades: Vec<Fill>,
}
```

`BacktestReport` and all nested types are `Serialize`/`Deserialize` (JSON). No charting or
rendering code is part of this sub-project — visualization is sub-project #13's
responsibility, consuming this structured data directly.

## 9. Error Handling

`apexes_core::ApexesError` gains new variants for backtest/trading-domain errors (rather than
a new crate-local error type, since these are conceptually the same errors future live-trading
sub-projects will also need to represent identically):

```rust
pub enum ApexesError {
    // ...existing variants unchanged...

    #[error("insufficient balance: required {required} {asset}, available {available}")]
    InsufficientBalance { asset: String, required: Decimal, available: Decimal },

    #[error("invalid backtest config: {reason}")]
    InvalidBacktestConfig { reason: String },

    #[error("order rejected: {reason}")]
    OrderRejected { reason: String },
}
```

Partial fills due to insufficient visible liquidity (§7.2) are **not** an error — they are a
valid, expected `Fill` for less than the requested quantity, logged via `tracing::warn`, not
propagated as `Err`.

## 10. Testing Strategy

- **Pure-logic unit tests** (network-free, always run by default `cargo test`): order book
  reconstruction from depth diffs; book-walk fill/VWAP calculation; queue-position-factor fill
  gating; fee calculation (maker/taker/funding); balance-ledger debit/credit correctness;
  `barter::statistic::metric` wrapper correctness; `OrderStatus` transitions (reused from #0,
  no new tests needed there); new Stop/StopLimit trigger-detection logic.
- **`InMemoryMarketDataSource`-driven integration tests** (also network-free, always run): a
  full `run_backtest()` call end-to-end against a small synthetic scripted `MarketEvent`
  sequence with a trivial deterministic test strategy, asserting the resulting
  `BacktestReport`'s P&L/fills/equity-curve match hand-calculated expected values.
- **`#[ignore]`d live tests**: one full backtest run against real ClickHouse-stored historical
  data via `StorageMarketDataSource`, following the exact `docker-compose up -d` /
  `cargo test -- --ignored` convention established in sub-project #2.

## 11. Resolved: Binance fee schedule

The Binance Spot VIP0–VIP9 maker/taker fee table (§7.5) was confirmed by the user directly
from `binance.com/en/fee/schedule` on 2026-07-28 (automated fetches of that page were blocked
during spec-writing — bot-detected/empty responses — so the user supplied the authoritative
values directly). The exact table is embedded in §7.5's `BinanceSpotVipFeeSchedule` doc
comment above. No open items remain.
