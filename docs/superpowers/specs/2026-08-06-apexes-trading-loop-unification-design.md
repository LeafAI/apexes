# Apexes — 13c 前置子项目：统一回测/模拟盘/实盘交易循环设计

**日期：** 2026-08-06
**关联子项目：** 13c（策略监控与实时调参面板）的前置重构，不属于 13c 本身范围。
**状态：** 待审查

## 一、动机与范围

`apexes-backtest::event_loop::run_event_loop`（`run_backtest`/`run_paper_trading`
共用）与 `apexes-live-trading::run_live_trading` 目前是两份手写的、结构高度相似但
完全独立维护的事件循环实现。两者在过去几个子项目（保证金监控 6.4、极端行情熔断
6.5、消息总线 #12）中每次新增能力都需要在两处分别实现、分别测试、分别踩坑——例如
"降风险平仓单必须携带 `position.leverage`/`.margin_mode` 才能走 `close_leveraged`
路径"这个教训在两份代码里各自出现过一次注释说明。

13c（策略监控与实时调参面板）需要在事件循环内部插入监控/调参钩子；如果不先统一，
这些钩子需要在两份循环体里分别插入、分别测试，重复上述历史模式。因此在开始 13c 之
前，先完成本设计描述的统一重构：让 `run_backtest`/`run_paper_trading`/
`run_live_trading` 都成为同一个统一循环核心的薄调用方，差异全部通过下述三个新增
trait 的不同实现来表达。

**本设计明确不包含**：13c 本身的监控/调参面板功能——那是后续独立 spec 的范围，本
设计只负责让被监控的对象（事件循环）从两份变成一份。

### 现状代码结构（写入本设计前的基线）

- `apexes-backtest::event_loop::run_event_loop`（`event_loop.rs`，约 1149
  行）：消费 `Stream<Item = MarketEvent> + Unpin`，通过共享的
  `Arc<Mutex<MatchingEngine>>` 模拟成交（`submit_market_order`/
  `submit_limit_order`/`submit_stop_order` 三个方法分别对应三种
  `OrderType`，成交可能立即通过返回值拿到，也可能通过后续
  `on_trade_event`/`on_depth_update` 打到本地挂单簿产生）。循环自己维护
  `open_orders: HashMap<OrderId, Order>`，手动更新 `filled_quantity`/
  `status`。`run_backtest`（历史重放）与 `apexes-paper::run_paper_trading`
  （实时模拟盘）都调用这同一个函数，只是喂入不同的 `Stream`/`BacktestClock`
  配置。
- `apexes-live-trading::run_live_trading`（`runner.rs`，约 826 行）：
  `tokio::select!` 三路事件源（主行情流、`exchange_updates` 推送流、
  `reconcile_interval` 定时轮询），通过 `Arc<tokio::sync::Mutex<OrderManager>>`
  的 `submit_order`（异步，成交总是稍后通过另外两路到达）提交订单，
  `open_orders` 直接读 `order_manager.open_orders()`（该状态由 `OrderManager`
  自己持久化在 Postgres、在 `apply_exchange_update`/`reconcile` 中更新）。
- 两者都：从同一个 `strategy.on_market_event(&event, &ctx)` 拿
  `Vec<OrderIntent>`；都对每个 intent 先过 `risk_checker.check`
  再过 `MarketConditionMonitor` 的 `PreTradeAdapter.check`；都对
  `intent.execution_algo.is_some()` 的 intent 转发给 `Ems::submit_algo_order`
  （EMS 路径完全共用，不在本设计改动范围）；都维护
  `margin_status`/`check_liquidations`/`check_de_risking` 同一套调用顺序，
  只是"强平/降风险单最终提交给谁、是否自动提交"不同。

## 二、三个新增 trait（定义于 `apexes-strategy`）

放置位置理由：与 `OrderIntent`/`Strategy`/`StrategyContext` 同一个 crate，
因为三个 trait 的方法签名都直接引用这些类型；这也是 `apexes-ems::ChildOrderSink`
既有的放置先例（定义在被驱动方 `apexes-ems`，而不是驱动方）的对称做法——这里
`apexes-strategy` 是"被驱动方类型的家"，三个 trait 是驱动 `Strategy` 输出的
"下游执行/风控"接口，与 `Strategy` 本身概念上配对。

### 2.1 `OrderExecutor`

```rust
#[async_trait::async_trait]
pub trait OrderExecutor: Send + Sync {
    /// 提交一个非算法路由的 OrderIntent（Market/Limit/Stop/StopLimit）。
    /// 返回的 `Vec<Fill>` 可能为空（回测的 Limit/Stop 挂单未立即成交，或
    /// 实盘——实盘总是空，真实成交总是通过 `exchange_updates` 或
    /// `on_market_event`/`reconcile` 路径稍后到达，不是这个方法的返回值）。
    ///
    /// `positions`/`balances`：`OrderManager::submit_order`（实盘）需要
    /// 这两者做自己的 `risk_checker` 前置校验（`OrderManager` 自身也持有
    /// 一份 `Option<Arc<CompositeRiskChecker>>`，与统一循环体里已经做的
    /// `risk_checker.check` 是两次独立校验，语义与今天完全一致，不是
    /// 本次重构引入的新校验）；`MatchingEngine` 侧实现直接忽略这两个
    /// 参数（它不做自己的风控校验，这层校验只在循环体里发生一次）。
    async fn submit(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        now: DateTime<Utc>,
    ) -> Result<(Order, Vec<Fill>), ApexesError>;

    /// 喂入一个新的 MarketEvent，让本地挂单簿/模拟撮合引擎有机会对其
    /// 作出反应，返回因此产生的成交（按原始 OrderId 分组）。这是同步的
    /// ——回测的挂单被新行情"打成交"是跟随同一条主行情流走的，不是独立
    /// 异步来源；不同于 `SupplementalFillSource`（见 2.3），后者专指
    /// 完全独立的异步成交来源（只有实盘用得上）。
    ///
    /// 回测实现：转发给 `MatchingEngine::on_trade_event`/
    /// `on_depth_update`（按事件类型分派），返回被打成交的挂单列表。
    /// 实盘实现：永远返回空 `Vec`（实盘不在本地模拟撮合，真实挂单
    /// 成交只通过 `SupplementalFillSource` 到达）。
    fn on_market_event(&self, event: &MarketEvent) -> Vec<(OrderId, Vec<Fill>)>;

    /// 当前所有未完全成交的订单，keyed by `OrderId`——供
    /// `StrategyContext::open_orders` 使用。统一循环不再自己维护本地
    /// `open_orders` map，总是通过这个方法读取。
    ///
    /// 回测实现：`MatchingEngine` 包装器新增自己跟踪 open orders 的职责
    /// （此前这份记账是 `run_event_loop` 自己做的，现在挪进 executor，
    /// 与 `submit`/`on_market_event` 产生的成交/状态变化保持在同一处
    /// 更新，不再有循环体和 executor 各记一半状态的风险）。
    /// 实盘实现：直接委托给 `OrderManager::open_orders()`（该状态已经
    /// 由 `OrderManager` 自己持久化/维护，零新增代码）。
    ///
    /// 返回值是拥有所有权的 `HashMap`（不是 `&HashMap`）：
    /// `OrderManager::open_orders()` 今天返回 `&HashMap`，但两个后端
    /// 需要在同一个 trait 方法签名下统一（`MatchingEngine` 侧被
    /// `Arc<Mutex<_>>` 包裹，跨 `.await` 持有借用不可行），因此这里
    /// 每次调用产生一次克隆——已知的、可接受的权衡（订单簿规模通常是
    /// 几十到几百个未完成订单量级，每个 tick 一次克隆不构成性能瓶颈），
    /// 不是遗漏。
    ///
    /// 这个方法是 `async fn`（不是同步方法）：`OrderManager::open_orders()`
    /// 需要先 `.await` 拿 `tokio::sync::Mutex` 的锁才能读取；
    /// `MatchingEngine` 侧实现用的是 `std::sync::Mutex`，在 `async fn`
    /// 内部直接同步加锁、读取、返回即可，不需要真正跨越 `.await` 点。
    async fn open_orders(&self) -> HashMap<OrderId, Order>;
}
```

**已确认的实现映射**：
- `apexes-backtest`：新增 `MatchingEngineOrderExecutor`（包装既有
  `Arc<Mutex<MatchingEngine>>`），实现上述三个方法；`submit` 内部按
  `intent.order_type` 分派到 `submit_market_order`/`submit_limit_order`/
  `submit_stop_order` 三者之一，`Limit`/`Stop`/`StopLimit` 情形返回
  `(order, vec![])`（无立即成交）。
- `apexes-oms`（或 `apexes-live-trading`）：新增 `OrderManagerExecutor`
  包装 `Arc<tokio::sync::Mutex<OrderManager>>`，`submit` 转发给
  `OrderManager::submit_order`，永远返回 `(order, vec![])`；
  `on_market_event` 永远返回空 `Vec`；`open_orders` 转发给
  `OrderManager::open_orders()`。

### 2.2 `LiquidationPolicy`

```rust
pub enum LiquidationReason {
    /// 单个杠杆仓位保证金率突破维持保证金阈值（行 6.4）。
    MarginBreach,
    /// 极端行情触发的降风险部分平仓（行 6.5）。
    ExtremeMarketCondition,
}

pub trait LiquidationPolicy: Send + Sync {
    /// 该 `reason` 触发的平仓/降风险单是否应该自动提交执行。
    fn should_auto_execute(&self, reason: LiquidationReason) -> bool;
}
```

**已确认的实现映射**：
- `apexes-backtest`（回测/模拟盘共用）：两个 reason 都返回 `true`
  ——与今天的行为完全一致（自动全部强平 + 自动按比例降风险）。
- `apexes-live-trading`：`MarginBreach` 永远返回 `false`（硬编码，不受
  任何配置影响——真实强平是交易所自己的职责，这个策略维度不允许调用方
  更改，与行 6.4 设计"live 永不自动强平"的既有措辞完全一致）；
  `ExtremeMarketCondition` 返回 `config.auto_derisk_live`（默认
  `false`，可显式设 `true` 开启）。

统一循环拿到 `should_auto_execute(reason) == false` 时的行为与今天
`run_live_trading` 完全一致：只 `tracing::error!` 记录，不提交订单，
依赖对应的 `PreTradeCheck`（`MarginMonitor`/`MarketConditionMonitor`）
阻止该 symbol 的新单。

### 2.3 `SupplementalFillSource`

```rust
pub trait SupplementalFillSource: Send {
    /// 完全独立于主行情流的异步成交来源。回测/模拟盘：一个永不产出的
    /// 空流（所有成交要么来自 `OrderExecutor::submit` 的同步返回值，
    /// 要么来自 `OrderExecutor::on_market_event`——都不需要这第三条
    /// 独立流）。实盘：包装真实的 `exchange_updates` 推送流 +
    /// `reconcile_interval` 定时轮询，两者合并为一个统一的 `Stream`。
    fn stream(&mut self) -> impl Stream<Item = Vec<Fill>> + Unpin + '_;
}
```

**已确认的实现映射**：
- `apexes-backtest`：一个恒久 `Pending`（`futures::stream::pending()`）
  的包装类型，`stream()` 直接返回它——与 `run_live_trading` 现有的
  "`exchange_updates` 流结束后 `chain(futures::stream::pending())`
  防止 busy-loop"这个既有教训保持同样的处理方式，不是本设计新引入的
  边界情况。
- `apexes-live-trading`：包装既有的 `exchange_updates.chain(pending())`
  流 + `reconcile_interval` 定时器，两者通过内部 `select!`/`merge`
  合并为一个统一 `Stream<Item = Vec<Fill>>`（`reconcile()` 返回的
  `Vec<(OrderId, Vec<Fill>)>` 展平为 `Vec<Fill>`，`OrderId` 信息通过
  每个 `Fill.order_id` 字段保留，与既有 `apply_new_fills` 的处理方式
  一致，不丢信息）。

## 三、统一循环的整体结构

> **实现落地后更新（Task 4/7 whole-branch review）**：下方伪代码是设计
> 阶段的原始提案，与最终交付的 `run_unified_event_loop` 签名有两处
> 已批准的偏差，均记录在实现计划 Task 4 的报告里：
> 1. `UnifiedLoopConfig` 实际多了一个 `funding_rate_bps: Option<Decimal>`
>    字段（不是完整 `CostModel`）——`PortfolioLedger::apply_funding_payments`
>    需要一个 `&CostModel`，但完整 `CostModel`（含手续费模型）已经被
>    隐藏在调用方自己构造的 `Arc<dyn OrderExecutor>` 背后不可达，循环
>    内部用这一个字段现构造一个零手续费的最小 `CostModel` 仅用于资金
>    费结算这一次调用，与直接持有完整 `CostModel` 在结果上可证明等价
>    （`CostModel::funding_payment` 只读 `funding_rate_bps`，从不读
>    `fee_schedule`）。
> 2. `venue_scorer`/`volume_curve_provider` 两个参数被替换为单一
>    `ems: Arc<dyn Ems>` 参数——具体 `ChildOrderSink`/`Ems` 后端是
>    调用方特定的（回测/模拟盘用 `MatchingEngineChildOrderSink`+
>    `InMemoryEms`；实盘用 `OrderManagerChildOrderSink`+`PostgresEms`），
>    循环核心本身不需要知道路由细节，只需要一个已经构造好的 `Ems`
>    handle；`venue_scorer`/`volume_curve_provider` 的 registry 查询
>    留在各调用方自己内部完成。
>
> 下方伪代码保留原始设计意图不做修改（仍是理解"为什么长这个形状"的
> 参考），实际签名以 `crates/apexes-backtest/src/unified_loop.rs`
> 源码为准。

原设计提案的 `run_unified_event_loop` 大致形状：

```rust
pub struct UnifiedLoopConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub risk_free_return: Decimal,
    pub risk_checker: Option<Arc<CompositeRiskChecker>>,
    pub margin_tiers: HashMap<String, MarginTierTable>,
    pub market_condition_config: Option<MarketConditionConfig>,
    // cost_model/queue_position_factor 等回测特有字段保留在
    // `EventLoopConfig`（回测侧的具体 config），不提升到这里——
    // 见下方"不统一的部分"。
}

pub async fn run_unified_event_loop(
    config: UnifiedLoopConfig,
    mut market_events: impl Stream<Item = MarketEvent> + Unpin,
    mut clock: BacktestClock,
    executor: Arc<dyn OrderExecutor>,
    liquidation_policy: Arc<dyn LiquidationPolicy>,
    mut supplemental_fills: impl SupplementalFillSource,
    venue_scorer: Arc<dyn VenueScorer>,
    volume_curve_provider: Option<Arc<dyn VolumeCurveProvider>>,
    mut strategy: impl Strategy,
    mut stop_signal: Option<oneshot::Receiver<()>>,
    latency_model: Option<LatencyModel>,
) -> Result<UnifiedLoopReport, ApexesError> {
    loop {
        tokio::select! {
            _ = stop_signal_check() => break,
            Some(event) = market_events.next() => {
                // 1. clock.wait_for_next_event（回测重放调用方传入真实
                //    速率限制配置；实时调用方——模拟盘/实盘——传入
                //    AsFastAsPossible，等同于今天的既有约定）。
                // 2. executor.on_market_event(&event) 处理本地挂单簿
                //    反应产生的成交 -> apply_fills -> risk_checker.on_fill。
                // 3. MarketConditionMonitor.on_market_event 更新状态。
                // 4. ems.on_market_event -> 算法单产生的成交，同上处理。
                // 5. 按事件类型更新 last_prices/mark_prices，推进
                //    matching-engine 本地簿状态（Depth 分支）。
                // 6. 资金费结算（沿用既有 last_funding_boundary 逻辑）。
                // 7. margin_status -> risk_checker.on_tick。
                // 8. check_liquidations -> 对每个 symbol：
                //    if liquidation_policy.should_auto_execute(MarginBreach)
                //      { executor.submit(强平intent) -> apply_fills }
                //    else { tracing::error! 仅记录 }
                // 9. check_de_risking（同上模式，reason =
                //    ExtremeMarketCondition）。
                // 10. strategy.on_market_event -> 对每个 intent：
                //     risk_checker.check -> market_condition.pre_trade.check
                //     -> (algo 路径: ems.submit_algo_order 不变)
                //     -> (非算法路径: executor.submit -> apply_fills)。
                // 11. 记录 equity point。
            }
            Some(fills) = supplemental_fills.stream().next() => {
                // apply_fills -> risk_checker.on_fill -> 记录 equity
                // point（对应今天 run_live_trading 的
                // exchange_updates/reconcile 两个分支，现在合并为一个）。
            }
        }
    }
    Ok(report)
}
```

`StrategyContext::open_orders` 由 `executor.open_orders()` 现取现用
（不再有循环体自己的 `HashMap<OrderId, Order>`）。

### 不统一的部分（明确保留差异，不勉强合并）

- **`cost_model`/`queue_position_factor`**：只有回测/模拟盘的
  `MatchingEngine` 需要（成交模拟参数），实盘没有对应概念（真实成交
  没有"排队位置模型"）。保留在各自的公开 `BacktestConfig`/
  `PaperTradingConfig`/`LiveTradingConfig`，不提升到
  `UnifiedLoopConfig`。
- **`reconcile_interval`/`ems_postgres_url`**：只有实盘需要，保留在
  `LiveTradingConfig`。
- **EMS 的具体后端**（`InMemoryEms` vs `PostgresEms`）：两者已经通过
  既有的 `Arc<dyn Ems>` trait object 统一，本设计不改动这部分。
- **风控检查器调用位置**：`CompositeRiskChecker`/
  `MarketConditionMonitor` 的 `check`/`on_fill`/`on_tick`/
  `on_market_event` 调用保持在统一循环本身里（不下沉进
  `OrderExecutor`），与现有代码结构一致——`OrderExecutor` 只负责
  "提交并可能立即拿到成交"和"挂单对行情的反应"，不做风控判断。

## 四、代码组织与放置

- 三个 trait 定义：`apexes-strategy/src/order_executor.rs`（新文件，
  约 100-150 行，含 trait 定义 + `LiquidationReason` 枚举 +
  文档注释，不与 `strategy.rs`/`margin_tier.rs` 混在一起，遵循
  "many small files"惯例）。
- 统一循环实现：留在 `apexes-backtest`（`event_loop.rs` 改名或新增
  `unified_loop.rs`，具体由实现任务拆解阶段决定），不迁移到
  `apexes-core`（`apexes-core` 的零依赖约束禁止它依赖
  `apexes-ems`/`apexes-risk`/`tokio` 等）。`run_backtest`/
  `run_paper_trading` 保持是这个统一循环的两个入口保持不变的公开
  API 承诺（外部调用方无感知）。
- `MatchingEngineOrderExecutor`/`MatchingEngineLiquidationPolicy`：
  `apexes-backtest`（包装 `MatchingEngine`，与其他 `apexes-backtest`
  内部适配器如 `MatchingEngineChildOrderSink` 同等待遇）。
- `OrderManagerExecutor`/`LiveLiquidationPolicy`/
  `LiveSupplementalFillSource`：`apexes-live-trading`（包装
  `OrderManager`/`LiveTradingConfig`，与该 crate 现有的
  `handle_market_event`/`apply_new_fills` 等私有辅助函数同等待遇）。
- `run_live_trading`/`run_backtest`/`run_paper_trading` 三者的公开
  签名（`LiveTradingConfig`/`BacktestConfig`/`PaperTradingConfig`
  及其字段）保持不变——本设计是纯内部重构，不改变任何既有对外行为
  或公开类型；已有测试套件（三个 crate 各自的单测 + 集成测试）必须
  在改动后原样通过，不允许因本次重构而改变任何测试的期望输出。

## 五、测试策略

- 三个新 trait 各自的实现（`MatchingEngineOrderExecutor`/
  `OrderManagerExecutor` 等）在其所在 crate 内新增独立单元测试，
  隔离验证（不经过完整循环）。
- 新增一个跨 crate 集成测试，证明 `run_live_trading`/
  `run_paper_trading`（或至少两者之一 + `run_backtest`）现在共用
  同一个 `run_unified_event_loop` 函数（而非"看起来像但其实是复制
  粘贴的两份循环体"）——例如通过在两个不同 executor 实现下运行同一
  策略脚本，断言两者在等价市场条件下产生结构一致的成交序列。
- 现有 `apexes-backtest`/`apexes-live-trading`/`apexes-paper` 全部
  既有测试必须原样通过，不修改任何既有测试的断言（本设计是行为不变
  的重构）。

## 六、与既有设计的关系

| | 本设计前 | 本设计后 |
|---|---|---|
| 循环实现数量 | 2（`run_event_loop` + `run_live_trading` 手写） | 1（`run_unified_event_loop`），两者都是薄调用方 |
| 订单提交 | `MatchingEngine` 三方法 vs `OrderManager::submit_order` 分别调用 | 统一 `OrderExecutor::submit` |
| 挂单成交（行情驱动） | `MatchingEngine::on_trade_event`/`on_depth_update` 直接调用 vs 实盘无此概念 | 统一 `OrderExecutor::on_market_event` |
| 挂单簿记 | 循环自己维护 `HashMap<OrderId, Order>` vs `OrderManager::open_orders()` | 统一 `OrderExecutor::open_orders()`，循环不再自己记账 |
| 强平/降风险自动执行策略 | 分散在两处 `if auto_derisk_live`/硬编码判断 | 统一 `LiquidationPolicy::should_auto_execute(reason)` |
| 独立异步成交来源 | 仅实盘有（`exchange_updates`/`reconcile`），回测无对应概念 | 统一 `SupplementalFillSource`，回测侧为恒久 pending 空实现 |
| 风控检查调用位置 | 两处循环体内分别调用 | 保持不变，仍在统一循环体内调用（不下沉进新 trait） |
