# Apexes — 子项目 #6：模拟交易（`apexes-paper`）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#6`（模拟交易 Paper Trading）的详细设计。对应 `docs/FEATURES.md` §4 "Paper Trading System"：行 `4.1`（虚拟账户与资金管理）、`4.2`（实时行情驱动的模拟撮合引擎，复用回测撮合内核）。行 `4.3`（订单生命周期状态机与实盘一致）已在子项目 `#0` 完成（`apexes_core::OrderStatus`），本文档不重复设计，只在集成处复用。
>
> 前置依赖：`apexes-market-data`（`MarketDataProvider` trait + `BinanceMarketDataProvider` 已完成）、`apexes-backtest`（`MatchingEngine`/`PortfolioLedger`/`CostModel`/`Strategy` trait 已完成）、`apexes-strategy`（`OrderIntent`/`Position` 已完成）、`apexes-risk`（`PreTradeCheck`/`InTradeMonitor`/`CompositeRiskChecker` 已完成）。

---

## 一、范围

### 核心架构问题与决策

路线图对 `#6` 的描述是"实时行情 + 复用回测撮合内核，与实盘一致的 `OrderStatus` 生命周期"。但 `apexes-backtest::run_backtest` 当前的事件循环签名是：

```rust
pub async fn run_backtest(
    mut config: BacktestConfig,
    market_data: impl HistoricalMarketDataSource,  // .events() 返回预加载的 Vec<MarketEvent>
    mut strategy: impl Strategy,
) -> Result<BacktestReport, ApexesError>
```

`HistoricalMarketDataSource::events()` 返回**一次性预加载好的 `Vec<MarketEvent>`**，而 `apexes_core::MarketDataProvider::subscribe()`（模拟盘的行情来源）返回的是**实时异步流** `BoxStream<'static, MarketEvent>`。两者在数据消费方式上有本质差异，`run_backtest` 无法直接复用给模拟盘调用。

**决策（本设计的 Task 1，是 `#6` 能成立的前提）**：重构 `apexes-backtest`，把事件循环的核心逻辑抽取成一个内部共享函数，统一接受 `impl Stream<Item = MarketEvent> + Unpin`驱动——回测侧把 `HistoricalMarketDataSource::events()` 返回的 `Vec` 用 `futures::stream::iter(...)` 包装成 Stream 后传入；模拟盘侧直接把 `MarketDataProvider::subscribe()` 返回的 `BoxStream` 传入。`run_backtest`对外签名/行为完全不变（向后兼容，现有调用方无感知），内部实现改为委托给这个共享核心。新的 `apexes-paper::run_paper_trading` 则是该共享核心面向实时场景的另一个薄封装。

这不是范围蔓延——路线图明确要求"复用回测撮合内核"，而当前 `run_backtest` 的结构做不到这一点，必须先重构才能满足路线图对 `#6` 的字面要求。

### 包含

1. **`apexes-backtest` 内部重构**（不改变任何对外公开的 API 签名/行为）：
   - 新增内部函数 `run_event_loop`（不对外公开，或标记 `pub(crate)`），接受 `impl Stream<Item = MarketEvent> + Unpin`、`BacktestConfig`（或未来更通用的配置类型）、`impl Strategy`，执行当前 `run_backtest` 函数体里从"构造 ledger/matching_engine/trackers"到"逐事件处理"的全部逻辑，返回 `BacktestReport`。
   - `run_backtest` 改为：读取 `market_data.events().await?`，用 `futures::stream::iter(events)` 包装成 Stream，调用 `run_event_loop`。
   - 新增一个可选的"停止信号"参数（`Option<tokio::sync::oneshot::Receiver<()>>`）：`run_backtest` 传 `None`（回测永远跑到 Stream 耗尽为止，行为不变）；`apexes-paper`调用时传 `Some(receiver)`，循环每次迭代前检查该 receiver 是否已收到信号或已被 drop，收到则优雅退出并返回目前为止累积的 `BacktestReport`。
2. **新增 workspace 成员 crate：`apexes-paper`**（依赖 `apexes-backtest` + `apexes-market-data` + `apexes-risk` + `apexes-strategy` + `apexes-core`），提供：
   - **`PaperAccount`**：`PortfolioLedger` 之上的轻量只读查询封装（对应 `4.1`），提供 `equity_snapshot()`/`positions_snapshot()`/`balances_snapshot()` 等方法，供未来监控/UI（子项目 `#13`）调用，不改变 `PortfolioLedger`本身的写入路径。
   - **`LatencyModel`**：`Fixed(Duration)` 和 `Uniform(min, max)` 两种订单提交延迟模型（对应 `4.2`的"实时场景调整"）。
   - **`run_paper_trading`**：主入口，消费 `Box<dyn MarketDataProvider>` 的实时流，在订单意图提交给 `MatchingEngine` 之前应用 `LatencyModel`（真实 `tokio::time::sleep`），驱动 `apexes-backtest`重构后暴露的共享事件循环核心，接受一个 `tokio::sync::oneshot::Receiver<()>` 作为停止信号，返回与 `BacktestReport`同型的报告。
   - 风控复用：`run_paper_trading`接受可选的 `apexes_risk::CompositeRiskChecker`，行为与 `apexes-backtest`集成完全一致（同一套 `PreTradeCheck`/`InTradeMonitor`语义）。

### 不包含（明确超出范围，留给对应后续子项目/后续轮次）

- **不做真实资金/真实下单**——`apexes-paper`产生的所有 `Fill`都是模拟撮合结果，不与任何真实交易所账户交互。真实下单是子项目 `#7`（OMS）+ `#8`（首个实盘网关）的范围。
- **不做订单持久化/断线重连对账**——`run_paper_trading`的所有状态（`PortfolioLedger`/`open_orders`）都在内存中，进程重启即丢失。持久化是子项目 `#7`（OMS）的范围（"真实订单生命周期、持久化、断线重连对账"）。
- **不做多账户/多用户支持**——`PaperAccount`是单账户封装，多账户管理是子项目 `#11`（账户与资金管理）的范围。
- **不做除延迟模拟外的其他实时特有因素**——不模拟丢包、乱序到达、交易所限流等更复杂的网络异常场景，`LatencyModel`只处理"下单到交易所收到"这一段的固定/均匀延迟。
- **不修改 `apexes-core::OrderStatus`状态机本身**——`4.3`已在子项目 `#0`完成，本设计只是复用现有状态机，不新增状态或转换规则。
- **不做 UI/监控面板**——`PaperAccount`的查询方法是为子项目 `#13`（UI）预留的接口，本轮不实现任何可视化。
- **不引入 `tokio-util`依赖**——停止信号用标准库/`tokio`已有的 `oneshot::Receiver<()>`实现，不为此单独引入 `CancellationToken`（工作区当前未依赖 `tokio-util`，没有必要为一个简单的一次性停止信号新增依赖）。

---

## 二、`apexes-backtest`重构（Task 1）

### 2.1 共享事件循环核心

新增 `crates/apexes-backtest/src/event_loop.rs`：

```rust
//! Shared event-loop core driving both `run_backtest` (historical replay,
//! `Vec<MarketEvent>` wrapped as a `Stream`) and `apexes-paper`'s
//! `run_paper_trading` (live `MarketDataProvider::subscribe()` stream) --
//! see design spec `2026-07-31-apexes-paper-design.md` §二 for why this
//! extraction was necessary (the two callers' event sources are
//! fundamentally different: pre-loaded `Vec` vs. live async `Stream`, and
//! the roadmap explicitly requires `apexes-paper` to "reuse the backtest
//! matching-engine kernel").

use crate::clock::BacktestClock;
use crate::cost_model::CostModel;
use crate::ledger::PortfolioLedger;
use crate::matching_engine::MatchingEngine;
use crate::performance::PerformanceTracker;
use crate::report::BacktestReport;
use apexes_core::{ApexesError, Fill, Instrument, MarketEvent, Order, OrderId, OrderType};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{Strategy, StrategyContext};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::ops::Range;
use chrono::NaiveDate;

/// Configuration shared by `run_backtest` and `run_paper_trading`'s event
/// loop core. `BacktestConfig` (in `runner.rs`) is the historical-replay
/// caller's public config type; `apexes-paper` has its own public config
/// type with the fields relevant to live trading, and both construct this
/// shared internal struct before calling `run_event_loop`.
pub(crate) struct EventLoopConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub queue_position_factor: Decimal,
    pub risk_free_return: Decimal,
    pub risk_checker: Option<CompositeRiskChecker>,
}

/// Runs the shared event loop: replays/consumes `events` in order through
/// `strategy`, simulating fills via a `MatchingEngine`, applying
/// `apexes-risk` checks if configured, tracking portfolio state via a
/// `PortfolioLedger`, and producing a `BacktestReport`. `clock` paces
/// dispatch (an already-configured `BacktestClock` -- `ReplaySpeed::
/// AsFastAsPossible` for historical replay callers that don't want
/// artificial pacing, since real event source pacing already comes from
/// wall-clock-timed live events for `apexes-paper`'s use). `stop_signal`,
/// if `Some`, causes the loop to exit early (returning whatever
/// `BacktestReport` has accumulated so far, not an error) once it fires
/// or its sender is dropped -- `run_backtest` always passes `None`
/// (historical replay runs to completion by construction, since `events`
/// is a finite pre-loaded stream).
pub(crate) async fn run_event_loop(
    config: EventLoopConfig,
    mut events: impl Stream<Item = MarketEvent> + Unpin,
    mut clock: BacktestClock,
    mut strategy: impl Strategy,
    mut stop_signal: Option<tokio::sync::oneshot::Receiver<()>>,
) -> Result<BacktestReport, ApexesError> {
    let instruments_by_symbol: HashMap<String, Instrument> = config
        .instruments
        .iter()
        .map(|instrument| (instrument.symbol.clone(), instrument.clone()))
        .collect();

    let mut ledger = PortfolioLedger::new(instruments_by_symbol, config.initial_balances);
    let mut matching_engine = MatchingEngine::new(config.cost_model, config.queue_position_factor);
    let mut overall_tracker = PerformanceTracker::new(config.risk_free_return);
    let mut per_symbol_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut per_venue_trackers: HashMap<String, PerformanceTracker> = HashMap::new();
    let mut open_orders: HashMap<OrderId, Order> = HashMap::new();
    let mut last_prices: HashMap<String, Decimal> = HashMap::new();
    let mut all_fills: Vec<Fill> = Vec::new();
    let mut recorded_initial_equity = false;

    loop {
        if let Some(signal) = stop_signal.as_mut() {
            match signal.try_recv() {
                Ok(()) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => break,
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
            }
        }

        let event = match events.next().await {
            Some(event) => event,
            None => break,
        };

        if !recorded_initial_equity {
            let initial_equity = ledger.equity(&last_prices)?;
            overall_tracker.record_equity_point(event_timestamp(&event), initial_equity);
            recorded_initial_equity = true;
        }

        let timestamp = event_timestamp(&event);
        clock.wait_for_next_event(timestamp).await;

        // ... existing match on MarketEvent::Depth/Trade/_ (unchanged
        // from run_backtest's current body -- moved here verbatim,
        // including both InTradeMonitor::on_fill call sites), then the
        // strategy.on_market_event -> PreTradeCheck -> matching-engine
        // submission block (also moved here verbatim, unchanged) ...

        let equity = ledger.equity(&last_prices)?;
        overall_tracker.record_equity_point(timestamp, equity);
    }

    Ok(BacktestReport {
        overall: overall_tracker.summary().into(),
        per_symbol: per_symbol_trackers
            .into_iter()
            .map(|(k, v)| (k, v.summary().into()))
            .collect(),
        per_venue: per_venue_trackers
            .into_iter()
            .map(|(k, v)| (k, v.summary().into()))
            .collect(),
        equity_curve: overall_tracker.equity_curve().to_vec(),
        trades: all_fills,
    })
}

fn event_timestamp(event: &MarketEvent) -> chrono::DateTime<chrono::Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
    }
}
```

（`apply_fills`辅助函数、`MarketEvent::Depth`/`Trade`分支、订单提交循环等实现细节与当前 `runner.rs`里的代码完全一致，只是搬到新文件——设计文档不重复贴出未改变的逻辑，实现计划阶段会给出逐行 diff。）

**为什么用"预加载事件是否耗尽"而非"显式最大事件数"来判断回测何时结束**：历史行情天然是有限的 `Vec`，包装成 `Stream`后自然在最后一个元素被消费后产生 `None`，`events.next().await`返回 `None`直接触发 `break`，无需额外的计数逻辑，`run_backtest`一侧因此不需要任何新参数即可保持完全向后兼容。

### 2.2 `run_backtest`重构后的实现

```rust
pub async fn run_backtest(
    config: BacktestConfig,
    market_data: impl HistoricalMarketDataSource,
    strategy: impl Strategy,
) -> Result<BacktestReport, ApexesError> {
    let events = market_data.events().await?;
    let event_stream = futures::stream::iter(events);
    let clock = BacktestClock::new(config.speed);

    let event_loop_config = EventLoopConfig {
        instruments: config.instruments,
        initial_balances: config.initial_balances,
        cost_model: config.cost_model,
        queue_position_factor: config.queue_position_factor,
        risk_free_return: config.risk_free_return,
        risk_checker: config.risk_checker,
    };

    crate::event_loop::run_event_loop(event_loop_config, event_stream, clock, strategy, None).await
}
```

`BacktestConfig`的公开字段/`date_range`语义完全不变（`date_range`本来就只是"提供给调用方查询历史数据用的信息字段"，`run_event_loop`不需要它）。`run_backtest`函数签名（含参数顺序）保持不变；`config`/`strategy`参数不再需要声明为 `mut`绑定（原先的 `mut`是因为 `config.risk_checker`要在循环内被 `&mut`借用，重构后 `EventLoopConfig`按值移入 `run_event_loop`，其内部自行声明所需的可变绑定），这是函数体内部实现细节，不影响任何调用方——调用方传参方式不变。

`CostModel`包含 `fee_schedule: Box<dyn FeeSchedule>`（trait object），**不能**派生 `Clone`（也没有必要）——`EventLoopConfig`和 `run_event_loop`都是按值（move）传递 `CostModel`，不需要克隆，与当前 `run_backtest`的写法完全一致，不对 `CostModel`做任何改动。

---

## 三、`apexes-paper` crate 结构

```
crates/apexes-paper/
├── Cargo.toml
├── src/
│   ├── lib.rs           // pub mod 声明 + re-exports
│   ├── account.rs       // PaperAccount
│   ├── latency.rs       // LatencyModel
│   └── runner.rs        // PaperTradingConfig + run_paper_trading
└── tests/
    └── live_binance.rs  // #[ignore] 真实 Binance 连接集成测试
```

`Cargo.toml`依赖：`apexes-core`、`apexes-strategy`、`apexes-backtest`（复用 `MatchingEngine`/`PortfolioLedger`/`event_loop`内部函数——需要 `apexes-backtest`把 `event_loop`模块标记为 `pub(crate)`可见性调整为跨 crate 可见，见下方说明）、`apexes-market-data`（`MarketDataProvider`）、`apexes-risk`（`CompositeRiskChecker`）、`tokio`、`futures`、`chrono`、`rust_decimal`、`tracing`、`thiserror`。

**`event_loop`模块的可见性问题**：`run_event_loop`需要被 `apexes-paper`调用，但它不应该是 `apexes-backtest`对外稳定 API 的一部分（它是"回测与模拟盘共享内核"这一实现细节，不是 `apexes-backtest`用户直接该调用的接口）。解决方式：`event_loop`模块声明为 `pub mod event_loop;`（跨 crate 可见），但模块内的类型/函数不通过 `apexes-backtest`的 `lib.rs`顶层 `pub use`重新导出——调用方必须写 `apexes_backtest::event_loop::run_event_loop(...)`这样的完整路径，这个"不方便"是有意的信号：提醒使用者这是内部共享机制而非公开门面 API，`run_backtest`仍然是 `apexes-backtest`对外的主要入口。

---

## 四、`PaperAccount`

```rust
//! [`PaperAccount`]: a thin read-only query layer over `PortfolioLedger`,
//! for future monitoring/UI (sub-project #13) consumers that need to
//! inspect current equity/positions without holding a mutable reference
//! to the ledger the event loop itself owns.

use apexes_backtest::PortfolioLedger;
use apexes_core::ApexesError;
use apexes_strategy::Position;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// A point-in-time snapshot of account state.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountSnapshot {
    pub balances: HashMap<String, Decimal>,
    pub positions: HashMap<String, Position>,
    pub equity: Decimal,
}

pub struct PaperAccount<'a> {
    ledger: &'a PortfolioLedger,
}

impl<'a> PaperAccount<'a> {
    pub fn new(ledger: &'a PortfolioLedger) -> Self {
        Self { ledger }
    }

    pub fn balances_snapshot(&self) -> HashMap<String, Decimal> {
        self.ledger.balances().clone()
    }

    pub fn positions_snapshot(&self) -> HashMap<String, Position> {
        self.ledger.positions().clone()
    }

    /// Computes current equity using `mark_prices` (asset symbol -> price
    /// in quote terms), the same convention as
    /// `PortfolioLedger::equity`.
    pub fn equity_snapshot(&self, mark_prices: &HashMap<String, Decimal>) -> Result<Decimal, ApexesError> {
        self.ledger.equity(mark_prices)
    }

    /// Convenience: all three snapshots in one call, for callers that
    /// want a single consistent point-in-time view (all three fields
    /// read from the same `&PortfolioLedger` borrow, so no other
    /// mutation can happen between them).
    pub fn full_snapshot(&self, mark_prices: &HashMap<String, Decimal>) -> Result<AccountSnapshot, ApexesError> {
        Ok(AccountSnapshot {
            balances: self.balances_snapshot(),
            positions: self.positions_snapshot(),
            equity: self.equity_snapshot(mark_prices)?,
        })
    }
}
```

`PaperAccount`本身不持有/拥有 `PortfolioLedger`（借用 `&'a PortfolioLedger`）——因为 `run_paper_trading`内部循环本身拥有并持续修改 `PortfolioLedger`，`PaperAccount`是给"外部只读观察者"用的临时借用视图，不是运行时状态的一部分。本轮不设计"如何把 `PaperAccount`暴露给一个正在运行的 `run_paper_trading`任务之外的进程/线程"——那属于未来 `#13`（UI）需要解决的"如何跨任务共享状态"问题（大概率是 `Arc<Mutex<PortfolioLedger>>`或消息通道，取决于 `#13`到时的技术选型），本设计不预先假设。

---

## 五、`LatencyModel`

```rust
//! [`LatencyModel`]: simulates network round-trip latency between a
//! strategy deciding to submit an order and the (simulated) exchange
//! receiving it, for realism in live-market-data-driven paper trading
//! (design spec §一 -- `apexes-backtest`'s historical replay has no
//! equivalent concept, since replay timestamps are already fixed by the
//! recorded data).

use rand::Rng;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub enum LatencyModel {
    /// No artificial delay -- the order reaches the matching engine as
    /// soon as the strategy returns it.
    None,
    Fixed(Duration),
    /// Uniformly distributed random delay in `[min, max]` (inclusive).
    /// Panics at construction time (via `Uniform::new`) if `min > max`.
    Uniform { min: Duration, max: Duration },
}

impl LatencyModel {
    /// Returns the delay to apply for one order submission. `Uniform`
    /// draws a new random value on every call (not cached), so
    /// consecutive orders in the same run get independent delays.
    pub fn sample(&self) -> Duration {
        match self {
            LatencyModel::None => Duration::ZERO,
            LatencyModel::Fixed(duration) => *duration,
            LatencyModel::Uniform { min, max } => {
                let min_nanos = min.as_nanos() as u64;
                let max_nanos = max.as_nanos() as u64;
                let sampled_nanos = rand::rng().random_range(min_nanos..=max_nanos);
                Duration::from_nanos(sampled_nanos)
            }
        }
    }

    /// Actually delays execution by `self.sample()`'s result via
    /// `tokio::time::sleep` -- a real wall-clock delay, not just a
    /// timestamp adjustment, per the design decision to most faithfully
    /// simulate real network latency's effect on timing.
    pub async fn apply(&self) {
        let delay = self.sample();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}
```

`rand`是新的 workspace 依赖（当前 `apexes`workspace 未依赖 `rand`）——实现计划需要把 `rand = "0.9"`（或当时最新稳定版本，实现时以 `cargo add rand`解析到的版本为准）加入 `[workspace.dependencies]`。

**为什么 `Uniform`不缓存采样结果**：每次订单提交都应该独立采样一个新的延迟值（模拟真实网络延迟的抖动/jitter），而不是整个运行期间固定用同一个随机延迟——固定延迟已经由 `LatencyModel::Fixed`覆盖了那个用例。

---

## 六、`run_paper_trading`

```rust
//! [`run_paper_trading`]: the `apexes-paper` counterpart to
//! `apexes_backtest::run_backtest` -- drives the same shared event-loop
//! core (`apexes_backtest::event_loop::run_event_loop`) against a live
//! `MarketDataProvider::subscribe()` stream instead of pre-loaded
//! historical data, applying `LatencyModel`-simulated order-submission
//! delay and accepting an explicit stop signal since a live feed has no
//! natural end.

use std::collections::HashMap;

use apexes_backtest::event_loop::{EventLoopConfig, run_event_loop};
use apexes_backtest::{BacktestReport, CostModel, PortfolioLedger, ReplaySpeed};
use apexes_backtest::clock::BacktestClock;
use apexes_core::{ApexesError, Instrument, MarketDataProvider};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::Strategy;
use rust_decimal::Decimal;
use tokio::sync::oneshot;

use crate::latency::LatencyModel;

pub struct PaperTradingConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub queue_position_factor: Decimal,
    pub risk_free_return: Decimal,
    pub risk_checker: Option<CompositeRiskChecker>,
    pub latency_model: LatencyModel,
}

/// Runs paper trading: subscribes to `provider` for `config.instruments`,
/// drives the shared event-loop core against the resulting live stream,
/// applying `config.latency_model`'s delay before each `OrderIntent`
/// reaches the matching engine (see §六's note on WHERE this hook lives
/// -- inside the shared event loop itself, gated by an
/// `Option<LatencyModel>` parameter, not wrapped around the whole
/// stream). Exits gracefully (returning the `BacktestReport` accumulated
/// so far) once `stop_signal` fires or its sender is dropped.
pub async fn run_paper_trading(
    config: PaperTradingConfig,
    provider: &dyn MarketDataProvider,
    strategy: impl Strategy,
    stop_signal: oneshot::Receiver<()>,
) -> Result<BacktestReport, ApexesError> {
    let event_stream = provider.subscribe(&config.instruments).await?;
    let clock = BacktestClock::new(ReplaySpeed::AsFastAsPossible); // live events already arrive at real-time pace; no additional artificial pacing needed.

    let event_loop_config = EventLoopConfig {
        instruments: config.instruments,
        initial_balances: config.initial_balances,
        cost_model: config.cost_model,
        queue_position_factor: config.queue_position_factor,
        risk_free_return: config.risk_free_return,
        risk_checker: config.risk_checker,
    };

    run_event_loop(
        event_loop_config,
        event_stream,
        clock,
        strategy,
        Some(stop_signal),
    )
    .await
}
```

**延迟应该插在哪一步**：`run_event_loop`本身不知道"延迟模型"这个概念（它是共享核心，`run_backtest`调用它时没有延迟概念）。因此 `LatencyModel`不能是 `EventLoopConfig`的字段——那样会把一个仅模拟盘需要的概念泄漏进回测的共享类型里。**修正方案**：`run_event_loop`签名新增一个 `Option<LatencyModel>`参数（`apexes-backtest`的 `event_loop.rs`需要依赖 `apexes-paper`的 `LatencyModel`类型？——这会造成循环依赖，`apexes-paper`依赖 `apexes-backtest`，`apexes-backtest`不能反过来依赖 `apexes-paper`）。

**因此实际决策**：`LatencyModel`类型定义**必须放在 `apexes-backtest`里**（作为 `event_loop`模块的一部分，或独立的 `crates/apexes-backtest/src/latency.rs`），而不是 `apexes-paper`——这样 `run_event_loop`可以直接接受 `Option<LatencyModel>`参数，`run_backtest`调用时永远传 `None`（历史回放没有"提交延迟"概念，直接沿用当前的零延迟提交路径），`apexes-paper::run_paper_trading`则传 `Some(config.latency_model)`。`apexes-paper`从 `apexes-backtest`重新导出 `LatencyModel`（`pub use apexes_backtest::LatencyModel;`），对 `apexes-paper`的使用者而言这个类型仍然看起来"属于"paper trading 场景，但物理上定义在 `apexes-backtest`以避免循环依赖。这是本设计过程中发现的一个必要修正，记录在此，实现计划以此处（而非上文§五最初的位置）为准。

`run_event_loop`签名修正为：

```rust
pub(crate) async fn run_event_loop(
    config: EventLoopConfig,
    events: impl Stream<Item = MarketEvent> + Unpin,
    clock: BacktestClock,
    strategy: impl Strategy,
    stop_signal: Option<tokio::sync::oneshot::Receiver<()>>,
    latency_model: Option<LatencyModel>,  // 新增：Some 时在订单提交前调用 latency_model.apply().await
) -> Result<BacktestReport, ApexesError>
```

在订单提交循环里，`PreTradeCheck`通过后、实际调用 `matching_engine.submit_*_order`之前插入：

```rust
if let Some(latency_model) = &latency_model {
    latency_model.apply().await;
}
```

`run_backtest`调用 `run_event_loop`时始终传 `None`（历史回放场景零延迟，保持现有行为完全不变）。

---

## 七、测试策略

1. **`apexes-backtest`重构回归测试**（Task 1）：重构前记录 `cargo test -p apexes-backtest`的完整通过数（当前 70 单元 + 3 集成 + 3 risk_integration），重构后必须逐一对照数字完全一致，且不新增/删除任何既有测试用例的内容（只是内部实现搬家，不改变可观察行为）。
2. **`PaperAccount`单元测试**：`balances_snapshot`/`positions_snapshot`/`equity_snapshot`/`full_snapshot`分别验证返回内容与底层 `PortfolioLedger`当前状态一致；`full_snapshot`验证三个字段来自同一次调用（用一个会在两次独立调用间返回不同结果的测试场景区分"一致快照"与"多次读取"的差异——如通过在两次调用之间人为修改 ledger 来验证 `full_snapshot`内部只读取一次)。
3. **`LatencyModel`单元测试**：`None`的 `sample()`返回 `Duration::ZERO`；`Fixed`的 `sample()`始终返回配置的固定值；`Uniform`的 `sample()`多次调用结果落在 `[min, max]`区间内（用多次采样 + 断言全部在区间内的方式验证，不做统计分布检验）；`apply()`对 `None`/`Fixed(Duration::ZERO)`几乎立即返回（用 `tokio::time::Instant`前后测量，断言耗时远小于一个可观测的下限，避免测试因为 `Fixed`需要真实 sleep 而变慢）。
4. **`run_event_loop`共享核心单元测试**（在 `apexes-backtest`里）：用 `futures::stream::iter`包装的有限事件序列，验证行为与重构前 `run_backtest`完全一致（复用重构前已有的测试场景，只是调用路径换成新的内部函数）；新增一个验证 `stop_signal`提前触发时循环在耗尽事件流之前退出、返回目前为止的 `BacktestReport`（而非错误）的测试；新增一个验证 `stop_signal`为 `None`时行为与 `run_backtest`现有测试完全相同的测试（防止误引入的 `stop_signal`检查逻辑影响历史回放路径）。
5. **`run_paper_trading`集成测试（mock provider，无需真实网络）**：定义一个测试专用的 `MockMarketDataProvider`（`subscribe()`返回 `futures::stream::iter(vec![...]).boxed()`包装的预定义 `MarketEvent`序列），验证：一个简单策略在收到预定义事件后正确提交订单、经过 `MatchingEngine`撮合、`PortfolioLedger`正确更新；配置了 `LatencyModel::Fixed`后，验证从"策略返回意图"到"该笔 `Fill`出现在结果里"之间确实经过了配置的延迟（用 `tokio::time::Instant`测量总运行时长的下限）；配置了 `apexes_risk::CompositeRiskChecker`后行为与 `apexes-backtest`集成一致（复用一个会拒绝特定 symbol 的黑名单场景）；`stop_signal`提前触发后循环优雅退出并返回部分结果。
6. **真实 Binance 集成测试**（`crates/apexes-paper/tests/live_binance.rs`，`#[ignore]`，沿用 `apexes-market-data`既有的 `#[ignore]`真实网络测试模式）：用真实 `BinanceMarketDataProvider`跑几秒钟的 `run_paper_trading`（配置一个短暂运行后自行触发 `stop_signal`的简单策略或外部定时器），验证不 panic、返回的 `BacktestReport`结构合法（即使可能没有任何实际成交，因为真实市场行情不可控）。
7. **全工作区验证**：`cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --check`。

---

## 八、非目标（Non-goals，本次明确不做）

- 不做真实资金/真实下单（留给 `#7`/`#8`）
- 不做订单持久化/断线重连对账（留给 `#7`）
- 不做多账户/多用户支持（留给 `#11`）
- 不做延迟模拟之外的其他网络异常场景（丢包/乱序/限流等）
- 不修改 `apexes_core::OrderStatus`状态机本身（`4.3`已完成，只复用）
- 不做 UI/监控面板（`PaperAccount`只是为 `#13`预留的查询接口，本轮不实现可视化）
- 不引入 `tokio-util`/`CancellationToken`（用已有的 `tokio::sync::oneshot`足够）
- 不解决"如何把运行中的 `PaperAccount`/`PortfolioLedger`状态暴露给外部观察者/多任务共享"的架构问题（留给未来 `#13`根据实际技术选型决定，如 `Arc<Mutex<_>>`或消息通道）
