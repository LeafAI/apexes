# Apexes — 子项目 #5 补完：保证金与杠杆监控（`docs/FEATURES.md` 行 `6.4`）设计

> 本文档是路线图子项目 `#5`（风控引擎）剩余范围的详细设计，覆盖 `docs/FEATURES.md` 行 `6.4`（保证金杠杆监控，crypto derivatives focus）。行 `6.5`（极端行情熔断与策略自动降级）不在本文档范围内，作为独立的后续子项目单独立项。
>
> **前置依赖（已完成）**：mark-price 数据管道（`docs/superpowers/plans/2026-08-02-apexes-mark-price-pipeline.md`）——`apexes_core::MarketEvent::MarkPrice`/`MarkPriceUpdate`、`apexes-market-data` 对 Binance USDS-M 永续合约的真实 mark price 实时抓取、`apexes-storage` 的 `mark_prices` ClickHouse 表（历史回放数据）。本设计正是消费这条管道产出的数据。
>
> **前置依赖（已完成）**：`apexes-risk`（`PreTradeCheck`/`InTradeMonitor` trait + `CircuitBreaker`/`LimitChecker`/`SymbolVenueList`/`SelfTradePreventer`/`CompositeRiskChecker`，见 `docs/superpowers/specs/2026-07-31-apexes-risk-design.md`）、`apexes-backtest::PortfolioLedger`（当前只支持现货净仓位）、`apexes-backtest::CostModel`（`funding_rate_bps` 字段已存在但标注"NOT YET WIRED UP，等杠杆持仓模型出现后再接入"——本设计正是接入这个字段的时机）。

---

## 一、范围

### 包含

1. **杠杆持仓模型**：在现有 `apexes_strategy::Position`/`OrderIntent` 上原地扩展杠杆/保证金模式字段（而不是新建平行类型），现货仓位（`leverage: None`）行为完全不变。
2. **`MarginMode`（逐仓/全仓）基础类型**：定义在 `apexes-core`（避免循环依赖，见 §二），供 `apexes-core::Order`、`apexes-strategy::{Position, OrderIntent}` 共同引用。
3. **分档维持保证金表**（`MarginTierTable`）及**保证金状态类型**（`PositionMarginStatus`），均定义在 `apexes-strategy`（与 `Position` 作伴，`apexes-risk`/`apexes-backtest` 都已依赖 `apexes-strategy`），供 `apexes-risk`/`apexes-backtest` 共同消费。
4. **`MarkPriceProvider` trait**（`apexes-strategy`）：回测场景由 `apexes-backtest`（同时依赖 `apexes-storage` 和 `apexes-strategy`）提供的适配器驱动，读取真实历史 `mark_prices` 表；模拟盘/实盘场景不需要单独实现——事件循环本身已经能直接收到 `MarketEvent::MarkPrice`，维护一份最新价格表即可（与现有 `last_prices`/`equity()` 用法完全一致）。
5. **`apexes-backtest::PortfolioLedger` 扩展**：
   - `apply_funding_payments`（真实币安 8 小时对齐周期：00:00/08:00/16:00 UTC）——正式接入 `CostModel::funding_rate_bps`。
   - `margin_status`（每个持仓的保证金率 + 逐仓精确强平价 / 全仓账户级保证金率，逐仓 `liquidation_price` 为 `None`）。
   - `check_liquidations`（判定哪些仓位已触发强平阈值）。
6. **`StrategyContext` 新增字段** `margin_status`，由 `PortfolioLedger` 的新方法计算后填入，回测/模拟盘/实盘三种模式复用同一套计算逻辑，保证一致性。策略可以据此在真正触发强平前主动减仓。
7. **`apexes-risk::MarginMonitor`**：新增的风控检查器，同时实现 `PreTradeCheck`（保证金率过低时阻止该 symbol 新订单，仿照 `CircuitBreaker` 的熔断/自动解除模式）和一个新的 `PositionMonitor` trait（每个事件循环 tick 都调用一次，用最新 `margin_status` 更新熔断状态——不同于 `on_fill` 只在成交后触发，保证金率也会因为 mark price 变化而单独恶化，因此需要独立于成交事件的驱动方式）。`CompositeRiskChecker` 新增第三类注册列表管理 `PositionMonitor`。
8. **回测/模拟盘自动强平，实盘只监控不下单**：`run_event_loop`（回测/模拟盘共用）每次循环调用 `check_liquidations`，对触发强平的仓位向 `MatchingEngine` 提交平仓市价单，按普通成交处理；`run_live_trading` 调用同样的判定逻辑，但只记录 `tracing::error!` 并依赖 `MarginMonitor` 的熔断阻止新单，**绝不自动下平仓单**。
9. **`apexes-gateway-binance` 实盘杠杆/保证金模式设置**：`BinanceExecutionGateway` 内部维护每个 symbol 已设置的杠杆/保证金模式缓存，`submit_order` 前若发现需要变更，先调用币安 `POST /fapi/v1/leverage` / `POST /fapi/v1/marginType`；设置失败则整个下单请求返回 `GatewayOutcome::Rejected`（不提交订单）。

### 不包含（明确超出范围）

- **行 `6.5`（极端行情熔断、策略自动降级）**——独立后续子项目。
- **强平清算的精确撮合模拟**（真实交易所强平通常走特殊的强平引擎/保险基金机制，可能有独立的强平价格曲线）——本设计的回测强平只是提交一个普通市价单，按 `MatchingEngine` 现有的正常撮合逻辑成交，允许有滑点，不追求逐笔精确复刻真实强平引擎行为。
- **跨保证金资产的精确汇率转换以外的更复杂全仓保证金计算**（如部分抵押率、非 1:1 抵押折算系数）——直接复用 `PortfolioLedger::equity()` 现有的汇率折算净值作为全仓保证金池，不做额外折算系数。
- **修改已有逐仓仓位的杠杆倍数**——如果同一 symbol 已有逐仓仓位，新订单若指定了不同的 `leverage`，直接拒绝（返回错误），要求先平仓或使用相同杠杆加仓。修改现有仓位杠杆（不改变仓位数量）是另一个功能，本设计不做。
- **保证金相关的历史持久化/审计报表**（行 `6.3`，归属子项目 `#15`）。

---

## 二、类型系统改动

### 2.1 `MarginMode`（`apexes-core`，新枚举）

放在 `apexes-core` 而非 `apexes-strategy`，是因为 `apexes_core::Order`（`ExecutionGateway`/`OrderManager` 操作的核心类型）需要携带这个字段，而 `apexes-strategy` 依赖 `apexes-core`——若定义在 `apexes-strategy` 会形成循环依赖。

```rust
// crates/apexes-core/src/order.rs -- 与 Side/OrderType 同级的基础枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarginMode {
    /// 逐仓：该仓位有独立分配的保证金，强平只影响这一个仓位。
    Isolated,
    /// 全仓：该账户所有全仓仓位共享账户总净值（`PortfolioLedger::equity()`
    /// 的折算净值）作为保证金池。
    Cross,
}
```

`apexes-strategy` 通过 `pub use apexes_core::MarginMode;` 重导出，供 `Position`/`OrderIntent` 直接使用，调用方无需关心它实际定义在哪个 crate。

### 2.2 `apexes_core::Order` 扩展

```rust
// crates/apexes-core/src/order.rs -- Order 结构体新增两个字段
pub struct Order {
    // ...现有字段不变...
    /// `None`：现货订单（无杠杆）。`Some`：该订单使用的杠杆倍数，供
    /// `ExecutionGateway` 实现（如 `BinanceExecutionGateway`）在提交前
    /// 判断是否需要先调用交易所的杠杆设置接口。
    pub leverage: Option<Decimal>,
    /// 仅当 `leverage.is_some()` 时有意义；`None` 视为 `Isolated`
    /// （与 `OrderIntent.margin_mode` 的默认值语义一致）。
    pub margin_mode: Option<MarginMode>,
}
```

`OrderManager::submit_order`（`apexes-oms`）构造 `Order` 时从 `OrderIntent.leverage`/`.margin_mode` 直接复制过去（详见 §六）。

### 2.3 `apexes_strategy::Position` 扩展

```rust
// crates/apexes-strategy/src/strategy.rs -- Position 结构体新增两个字段
pub struct Position {
    pub symbol: String,
    pub quantity: Decimal,
    pub average_entry_price: Decimal,
    /// `None`：现货净仓位（今天的行为完全不变）。`Some(leverage)`：
    /// 该仓位是用这个杠杆倍数建立的永续合约仓位。
    pub leverage: Option<Decimal>,
    /// 仅当 `leverage.is_some()` 时有意义。
    pub margin_mode: Option<MarginMode>,
}
```

### 2.4 `apexes_strategy::OrderIntent` 扩展

```rust
// crates/apexes-strategy/src/strategy.rs -- OrderIntent 结构体新增两个字段
pub struct OrderIntent {
    // ...现有字段不变...
    /// `None`：现货订单（今天的行为）。`Some(leverage)`：以该杠杆
    /// 倍数开/加仓永续合约仓位。
    pub leverage: Option<Decimal>,
    /// 仅当 `leverage.is_some()` 时读取；为 `None` 时默认视为
    /// `MarginMode::Isolated`（更安全的保守默认值——逐仓仓位的强平
    /// 不会连锁影响其他仓位）。
    pub margin_mode: Option<MarginMode>,
}
```

**同一 symbol 逐仓加仓时杠杆不一致的处理**：`PortfolioLedger::apply_fill`（现有方法，详见 §三 对 `PortfolioLedger` 的扩展）在处理一笔会导致仓位数量变化（而非首次开仓）的成交前，检查该 symbol 现有 `Position.leverage`（若为 `Isolated` 且已有非零仓位）与本次 `OrderIntent.leverage` 是否一致；不一致时返回新的 `ApexesError::InconsistentLeverageForIsolatedPosition { symbol, existing, requested }` 错误，交易被拒绝、账本不做任何改动（原子性，与 `apply_fill` 现有的"检查全部通过才应用变更"约定一致）。全仓仓位不受此限制（全仓杠杆本质上是订单级参数，不要求同一账户下所有全仓订单杠杆一致——但账户共享保证金池的机制仍然适用）。

### 2.5 `MarginTierTable`（`apexes-strategy`，新类型）

```rust
// crates/apexes-strategy/src/margin_tier.rs（新文件）
/// 单个保证金阶梯档位——名义价值越高，维持保证金率越高、允许的最大
/// 杠杆越低，仿真实币安阶梯保证金表设计。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarginTier {
    /// 该档位生效的名义价值下限（含）。
    pub notional_floor: Decimal,
    pub max_leverage: Decimal,
    pub maintenance_margin_rate: Decimal,
    /// 固定扣减额（币安阶梯表里每个档位都有的常数项，用于让维持保证金
    /// 曲线在档位边界处连续）。
    pub maintenance_amount: Decimal,
}

/// 一个 symbol 的完整阶梯保证金表，按 `notional_floor` 升序排列。
#[derive(Debug, Clone, PartialEq)]
pub struct MarginTierTable {
    pub tiers: Vec<MarginTier>,
}

impl MarginTierTable {
    /// 返回适用于给定名义价值的档位（表中最后一个 `notional_floor <=
    /// notional` 的档位）；`notional` 小于第一档下限时返回第一档。
    pub fn tier_for_notional(&self, notional: Decimal) -> &MarginTier { ... }
}
```

由调用方以 `HashMap<String, MarginTierTable>`（键为 symbol）的形式构造并同时传给 `apexes-risk::MarginMonitor` 和 `apexes-backtest::PortfolioLedger` 的新方法——保证两边用的是同一份配置，不会出现回测和风控计算不一致的情况。

### 2.6 `MarkPriceProvider`（`apexes-strategy`）

```rust
// crates/apexes-strategy/src/mark_price_provider.rs（新文件）
#[async_trait]
pub trait MarkPriceProvider: Send + Sync {
    /// 返回 `symbol`/`venue` 当前（或调用方语境下"当下"）的 mark
    /// price。回测场景下，"当下"是当前回放到的事件时间戳。
    async fn mark_price(&self, symbol: &str, venue: Venue) -> Result<Decimal, ApexesError>;
}
```

- **回测**：`apexes-backtest`（不是 `apexes-storage`——`apexes-storage` 目前只依赖 `apexes-core`，并不依赖 `apexes-strategy`，因此无法实现定义在 `apexes-strategy` 里的 `MarkPriceProvider` trait；`apexes-backtest` 已经同时依赖 `apexes-storage` 和 `apexes-strategy`，是实现这个适配器的正确位置，不需要给 `apexes-storage` 新增额外的依赖边）提供 `StorageReaderMarkPriceProvider`（新类型），内部持有 `StorageReader` + 当前回放时间戳（由 `run_event_loop` 每次循环更新），查询 `mark_prices` 表里 `timestamp` 最接近但不晚于当前回放时间戳的一条记录。
- **模拟盘/实盘**：不需要单独实现这个 trait——`run_event_loop`/`run_live_trading` 已经会收到 `MarketEvent::MarkPrice` 事件（mark-price 管道产出），维护一个 `HashMap<String, Decimal>`（与现有 `last_prices` 完全同构的模式）即可，直接把这份 map 传给 `PortfolioLedger` 的新方法，无需经过 `MarkPriceProvider` trait 这层抽象。

---

## 三、`apexes-backtest::PortfolioLedger` 扩展

### 3.1 `apply_funding_payments`

```rust
/// 对每个杠杆持仓（`leverage.is_some()`）按 `funding_rate_bps` 结算一次
/// 资金费率——`notional * funding_rate_bps / 10000`，多头支付、空头
/// 收取（或反之，取决于 `funding_rate_bps` 符号，与现有
/// `CostModel::funding_payment` 的符号约定一致）。调用方
/// （`run_event_loop`）负责判断"距离上次结算是否已跨过一个真实币安
/// 8 小时边界"（00:00/08:00/16:00 UTC）——本方法本身不感知时间调度，
/// 只负责在被调用时对当前持仓做一次性结算。
pub fn apply_funding_payments(
    &mut self,
    mark_prices: &HashMap<String, Decimal>,
    cost_model: &CostModel,
) -> Result<(), ApexesError>
```

`run_event_loop` 维护一个 `last_funding_settlement: DateTime<Utc>`（初始化为回测起始时间归一化到上一个 8 小时边界），每次循环用当前事件时间戳与之比较：若跨过了至少一个 00:00/08:00/16:00 UTC 边界，调用一次 `apply_funding_payments`（多次跨越边界之间只结算一次，不做逐个边界的精确逐次结算——这是一个可接受的简化，记录在本设计的验收标准里）。

### 3.2 `margin_status`

**`PositionMarginStatus` 定义位置的修正（自查发现的真实依赖问题）**：这个类型被 `apexes-strategy::StrategyContext` 和 `apexes-risk::PositionMonitor`/`CompositeRiskChecker` 同时引用，但 `apexes-strategy`/`apexes-risk` 都不依赖 `apexes-backtest`（只有 `apexes-backtest` 反过来依赖它们两个）。因此 `PositionMarginStatus` 必须定义在 `apexes-strategy`（与 `Position`/`MarginTierTable` 作伴，`apexes-risk`/`apexes-backtest` 都已依赖 `apexes-strategy`），而不是定义在 `apexes-backtest` 里——`PortfolioLedger::margin_status` 方法本身仍然定义在 `apexes-backtest`（计算逻辑的唯一实现位置），只是返回值类型来自 `apexes-strategy`。

```rust
// crates/apexes-strategy/src/margin_tier.rs（与 MarginTierTable 同一个文件，或新建
// crates/apexes-strategy/src/margin_status.rs——具体归档留给实现阶段决定）
/// 每个持仓（含现货，现货返回一个"永远健康"的哨兵状态）的实时保证金
/// 状态。
#[derive(Debug, Clone, PartialEq)]
pub struct PositionMarginStatus {
    /// `None`：现货仓位，不适用保证金概念。
    pub margin_ratio: Option<Decimal>,
    /// 仅逐仓仓位有精确值；全仓仓位（保证金池是账户共享的）返回
    /// `None`——全仓仓位"单独" 强平价这个概念本身不精确。
    pub liquidation_price: Option<Decimal>,
}
```

```rust
// crates/apexes-backtest/src/ledger.rs -- PortfolioLedger 新增方法，
// 返回类型来自 apexes_strategy::PositionMarginStatus
pub fn margin_status(
    &self,
    mark_prices: &HashMap<String, Decimal>,
    margin_tiers: &HashMap<String, MarginTierTable>,
) -> HashMap<String, PositionMarginStatus>
```

- **逐仓**：`margin_ratio = 仓位自身保证金 / (名义价值 * maintenance_margin_rate - maintenance_amount)`；`liquidation_price` 用标准的"保证金耗尽时的标记价格"公式反解（多头：入场价 - 保证金/仓位数量 沿维持保证金方向调整；空头相反）。
- **全仓**：`margin_ratio = ledger.equity(mark_prices) / 该账户全部全仓仓位的维持保证金需求总和`；`liquidation_price` 恒为 `None`。

### 3.3 `check_liquidations`

```rust
/// 返回保证金率已跌破强平阈值（`margin_ratio <= 1.0`，即维持保证金
/// 需求已经无法被现有保证金满足）的 symbol 列表。
pub fn check_liquidations(
    &self,
    mark_prices: &HashMap<String, Decimal>,
    margin_tiers: &HashMap<String, MarginTierTable>,
) -> Vec<String>
```

`run_event_loop`（回测/模拟盘）对每个返回的 symbol，构造一个市价平仓单提交给 `MatchingEngine`，产生的成交按 `apply_fills` 现有逻辑处理（与策略主动下单产生的成交完全同源，账本/绩效统计不做区分）。`run_live_trading` 调用同样的方法，但只 `tracing::error!` 记录，不下单。

---

## 四、`StrategyContext` 扩展

```rust
// crates/apexes-strategy/src/strategy.rs -- StrategyContext 新增字段
pub struct StrategyContext<'a> {
    // ...现有字段不变...
    /// 每个持仓的实时保证金状态，由 `PortfolioLedger::margin_status`
    /// 计算后由调用方（`run_event_loop`/`run_live_trading`）填入。
    /// 策略可以据此在真正触发强平前主动减仓/平仓。现货仓位不出现在
    /// 这个 map 里（`margin_status` 只为杠杆仓位产出条目）。
    pub margin_status: &'a HashMap<String, PositionMarginStatus>,
}
```

回测（`run_event_loop`）、模拟盘（`run_event_loop` 复用同一份代码）、实盘（`run_live_trading`）三处调用点在构造 `StrategyContext` 前都调用同一个 `PortfolioLedger::margin_status` 方法，保证三种模式下策略观察到的保证金状态计算逻辑完全一致（这是整个项目"回测/模拟/实盘逻辑同源"这一不可动摇原则的直接延伸）。

---

## 五、`apexes-risk::MarginMonitor`

```rust
// crates/apexes-risk/src/margin_monitor.rs（新文件）
//! [`MarginMonitor`]: 保证金率过低时阻止该 symbol 新订单的熔断器，
//! 架构上仿照 [`crate::circuit_breaker::CircuitBreaker`]（同一份共享
//! 状态、`split()` 拆成 PreTradeCheck/PositionMonitor 两个适配器的模式）。
//! 与 `CircuitBreaker` 不同：`CircuitBreaker` 只在 `InTradeMonitor::
//! on_fill`（成交后）更新状态；`MarginMonitor` 的保证金率会因为 mark
//! price 变化（不一定伴随新成交）而单独恶化，因此改用新增的
//! `PositionMonitor::on_tick`，由事件循环每次迭代都调用一次。

pub struct MarginMonitor {
    warning_threshold: Decimal, // 例如 1.5 -- margin_ratio 低于此值即熔断
    tripped_symbols: HashSet<String>,
}

impl MarginMonitor {
    pub fn split(self) -> (MarginMonitorPreTradeAdapter, MarginMonitorPositionAdapter) { ... }
}
```

### 5.1 新增 `PositionMonitor` trait（`apexes-risk`）

```rust
// crates/apexes-risk/src/position_monitor.rs（新文件）
/// 每个事件循环 tick 都调用一次（不依赖是否有新成交），用最新的
/// `PositionMarginStatus` 更新内部熔断状态。区别于 `InTradeMonitor`
/// （只在成交后触发）——保证金率会因为纯粹的价格变化而单独恶化。
pub trait PositionMonitor: Send + Sync {
    fn on_tick(
        &mut self,
        margin_status: &HashMap<String, PositionMarginStatus>,
    ) -> Result<(), Vec<RiskViolation>>;
}
```

### 5.2 `CompositeRiskChecker` 新增第三类注册列表

```rust
// crates/apexes-risk/src/composite.rs -- 新增字段与方法
pub struct CompositeRiskChecker {
    pre_trade_checks: Mutex<Vec<Box<dyn PreTradeCheck>>>,
    in_trade_monitors: Mutex<Vec<Box<dyn InTradeMonitor>>>,
    position_monitors: Mutex<Vec<Box<dyn PositionMonitor>>>, // 新增
}

impl CompositeRiskChecker {
    pub fn with_position_monitor(self, monitor: Box<dyn PositionMonitor>) -> Self { ... }
    /// 依次调用每个已注册 `PositionMonitor::on_tick`，聚合所有违规。
    pub fn on_tick(&self, margin_status: &HashMap<String, PositionMarginStatus>) -> Result<(), Vec<RiskViolation>> { ... }
}
```

`run_event_loop`/`run_live_trading` 在每次循环调用 `ledger.margin_status(...)` 之后、构造 `StrategyContext` 之前，调用一次 `risk_checker.on_tick(&margin_status)`（若配置了 `CompositeRiskChecker`），日志记录任何违规（与现有 `on_fill` 违规日志处理方式一致，不中断事件循环）。

---

## 六、`apexes-oms`/`apexes-gateway-binance` 改动

### 6.1 `OrderManager::submit_order`（`apexes-oms`）

构造 `Order` 时补充两个新字段的复制：

```rust
let mut order = Order {
    // ...现有字段构造不变...
    leverage: intent.leverage,
    margin_mode: intent.margin_mode,
};
```

### 6.2 `BinanceExecutionGateway`（`apexes-gateway-binance`）

- 新增内部状态：`leverage_cache: Mutex<HashMap<String, (Decimal, MarginMode)>>`（symbol -> 已在交易所设置生效的杠杆/保证金模式）。
- `submit_order`（`ExecutionGateway` trait 方法本身签名不变）内部逻辑：若 `order.leverage.is_some()` 且与缓存中该 symbol 当前值不一致（或缓存为空），先调用 `POST /fapi/v1/marginType`（若 `margin_mode` 有变化）再调用 `POST /fapi/v1/leverage`，成功后更新缓存再继续原有下单逻辑；任一调用失败，直接返回 `GatewayOutcome::Rejected { reason: "杠杆/保证金模式设置失败: ..." }`，不提交订单。
- `ExecutionGateway` trait 本身不变（不新增 `set_leverage`/`set_margin_mode` 方法）——这两个动作完全是 `BinanceExecutionGateway` 的内部实现细节，其它未来的网关实现（如 A 股/美股）不需要关心。

---

## 七、测试策略

- **`apexes-core`**：`MarginMode` 序列化往返测试；`Order`/`OrderIntent` 新字段的默认值/序列化测试。
- **`apexes-strategy`**：`MarginTierTable::tier_for_notional` 的边界测试（档位边界、超出最高档、低于最低档）；`Position`/`OrderIntent` 新字段的构造测试。
- **`apexes-backtest`**：
  - `apply_funding_payments`：多头/空头资金费率符号正确性、跨越多个 8 小时边界只结算一次的行为、现货仓位不受影响。
  - `margin_status`：逐仓精确保证金率/强平价计算（用手工验证过的数值），全仓账户级保证金率计算，现货仓位返回哨兵状态。
  - `check_liquidations`：触发阈值的仓位被正确识别；未触发的不被误判。
  - 端到端测试：一个开了杠杆仓位的策略，价格朝不利方向移动触及强平阈值后，`run_backtest` 自动提交平仓单并在 `report.trades` 中体现。
  - 同一 symbol 逐仓杠杆不一致加仓被拒绝的测试。
- **`apexes-risk`**：`MarginMonitor`（`split()` 后两个适配器共享状态）的熔断触发/自动解除测试，仿照 `CircuitBreaker` 现有测试的结构。
- **`apexes-gateway-binance`**：`wiremock` 测试验证杠杆/保证金模式设置的调用时机（首次下单时调用、同杠杆重复下单不重复调用、设置失败时不提交订单）。
- 全流程 `cargo build --workspace`/`cargo test --workspace -- --test-threads=1`/`cargo clippy --workspace --all-targets -- -D warnings`/`cargo fmt --check` 保持干净，遵循本项目一贯的全局约束。

---

## 八、非目标回顾

本设计明确不做：`6.5`（极端行情熔断/策略自动降级）、精确复刻真实交易所强平引擎、跨资产更复杂的抵押率折算、修改已有仓位杠杆（不改变仓位数量）、保证金相关的持久化审计报表（行 `6.3`，子项目 `#15`）。
