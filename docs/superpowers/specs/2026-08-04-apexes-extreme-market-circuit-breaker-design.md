# Apexes — 子项目 #5 补完：极端行情熔断与策略自动降风险（`docs/FEATURES.md` 行 `6.5`）设计

> 本文档是路线图子项目 `#5`（风控引擎）剩余范围的详细设计，覆盖 `docs/FEATURES.md` 行 `6.5`（极端行情熔断与策略自动降级）。这是行 `6.4`（保证金杠杆监控，已完成）的独立后续子项目——`6.4` 关注单个杠杆仓位因保证金不足而被强平；`6.5` 关注整个市场/账户层面因价格剧烈波动而需要主动降低风险敞口，两者触发条件、覆盖范围（现货 vs 杠杆）、以及"谁来执行平仓"的责任边界都不同，因此不合并到同一个检查器。
>
> **前置依赖（已完成）**：
> - `apexes-risk`：`PreTradeCheck`/`InTradeMonitor`/`PositionMonitor` 三类 trait + `CircuitBreaker`/`MarginMonitor`/`LimitChecker`/`SymbolVenueList`/`SelfTradePreventer`/`CompositeRiskChecker`（`docs/superpowers/specs/2026-07-31-apexes-risk-design.md`、`docs/superpowers/specs/2026-08-03-apexes-margin-leverage-monitoring-design.md`）。`MarginMonitor::split()` 的 `Arc<Mutex<_>>`+两个适配器模式是本设计直接复用的架构范式。
> - `apexes-indicators`：`RollingStdDev`（滚动标准差，O(1) 增量更新）——本设计直接复用作为波动率计算的核心组件，不重新实现。
> - `apexes-backtest::PortfolioLedger::{margin_status, check_liquidations, apply_funding_payments}`（行 `6.4`）——`check_de_risking`（本设计新增）在方法签名和调用方式上直接模仿 `check_liquidations`。
> - `apexes_core::MarketEvent`（`Tick`/`Depth`/`Bar`/`Trade`/`MarkPrice`）——本设计消费 `Trade`/`MarkPrice` 事件计算价格波动信号。

---

## 一、范围

### 包含

1. **`apexes-risk::MarketConditionMonitor`**：新增的风控检查器，按 symbol 监测价格波动率突增与价格跳变（闪崩/闪涨），可选按账户级权益波动监测。架构上镜像 `MarginMonitor`（`Arc<Mutex<_>>` 共享状态 + `split()` 拆分为多个 trait 适配器），但驱动信号是原始行情事件而非保证金状态，因此引入一个新 trait `MarketConditionMonitor`（trait 名与实现类型同名，通过模块路径区分，见 §二.2）。
2. **新 trait `apexes_risk::MarketConditionEventMonitor`**：`fn on_market_event(&mut self, event: &MarketEvent) -> Result<(), Vec<RiskViolation>>`，每个事件循环收到一个 `MarketEvent` 就调用一次——不同于 `PositionMonitor::on_tick`（只携带保证金状态，没有原始价格），这个新 trait 直接消费行情事件本身。`CompositeRiskChecker` 新增第四类注册列表管理它。
3. **可配置的检测信号**（`MarketConditionConfig`）：
   - 价格波动率突增：基于 `apexes-indicators::RollingStdDev`，绝对阈值触发（默认），可选相对阈值模式。
   - 价格跳变（闪崩/闪涨）：相邻两次价格更新之间的变化百分比超过阈值触发。
   - 两种信号任一触发即视为该 symbol 处于极端行情。
   - 价格来源可配置：默认用成交价（`MarketEvent::Trade`，适用现货），永续合约场景可选用中价（`MarketEvent::MarkPrice`）——只处理与配置项对应的事件类型，其余忽略。
   - 可选的账户级权益波动检测（同样用 `RollingStdDev`，输入是 `PortfolioLedger::equity()` 的时间序列）。
4. **线性插值减仓比例**：`reduce_fraction = min(1.0, sensitivity × (actual/threshold − 1.0))`，`actual` 取波动率和跳变百分比中相对各自阈值超出程度更严重的那个。账户级触发时，对所有当前持仓 symbol 统一应用同一比例。
5. **`apexes-backtest::PortfolioLedger::check_de_risking`**：新方法，输入 `reduce_fractions: &HashMap<String, Decimal>`（由 `MarketConditionMonitor` 计算后由调用方传入）与 `include_spot: bool`，输出每个需要部分平仓的 `(symbol, side, quantity)`。方法签名与调用约定直接模仿 `check_liquidations`，但支持部分（而非全部）平仓。
6. **`run_event_loop`/`run_paper_trading`（回测/模拟盘共用）自动执行降风险平仓**：每个 tick 调用 `check_de_risking`，对返回的每个 `(symbol, side, quantity)` 向 `MatchingEngine` 提交对应数量的市价单，按普通成交处理——这是策略层面的主动风控决策，不同于交易所强平，允许自动执行。
7. **`run_live_trading` 默认保守，可显式开启自动执行**：`LiveTradingConfig` 新增 `auto_derisk_live: bool`（默认 `false`）。为 `false` 时，只记录 `tracing::error!` 并依赖 `MarketConditionMonitor` 的 `PreTradeCheck` 阻止新单（与行 `6.4` 的实盘保守策略完全一致）；显式设为 `true` 时，与回测/模拟盘一样自动提交降风险平仓单。
8. **`BacktestConfig`/`PaperTradingConfig`/`LiveTradingConfig` 都从一开始就暴露公开字段**——直接吸取行 `6.4` 审查中发现的教训（`margin_tiers` 最初只加在内部 `EventLoopConfig` 上，未暴露到公开配置，导致整个保证金监控系统对真实调用方形同虚设）：`market_condition_config: Option<MarketConditionConfig>` 从任务拆解的第一个任务就加到所有三个公开配置类型上，不留到后续任务补。

### 不包含（明确超出范围）

- **精确复刻真实交易所的熔断/停牌机制**（如 A 股涨跌停、美股 LULD、期货熔断停盘）——那些是交易所对手方的规则，属于未来"A股/H股/美股网关"子项目（`#10`）在协议适配层需要遵守的外部约束，不是本系统自己的风控逻辑；本设计的"熔断"专指系统自身对策略仓位的主动降风险，不是对交易所停牌规则的模拟。
- **多 symbol 之间的波动率相关性/传染效应建模**（如"BTC 暴跌导致其他币种流动性枯竭"的联动检测）——每个 symbol 独立判定，不做跨 symbol 的相关性分析。YAGNI，未来如有需要可再扩展。
- **行情数据中断/延迟本身作为触发信号**（如 WebSocket 断线、数据源延迟超过阈值）——这是数据管道健壮性问题，不是"极端行情"信号，超出本设计范围；如需要，属于运维监控（子项目 `#14`）或市场数据网关自身的重连逻辑范畴。
- **减仓平仓的精确撮合模拟**——与行 `6.4` 的强平一致，本设计的降风险平仓只是提交一个普通市价单，按 `MatchingEngine` 现有正常撮合逻辑成交，允许滑点，不追求特殊的"紧急平仓撮合曲线"。
- **阶梯式（离散分级）减仓比例表**——已确认采用线性插值而非离散分级表，不引入类似 `MarginTierTable` 的分级配置类型。

---

## 二、类型系统改动

### 2.1 `MarketConditionConfig`（`apexes-risk`，新结构体）

```rust
pub struct MarketConditionConfig {
    /// 波动率突增的绝对阈值（`RollingStdDev` 的输出值，与价格同量纲）。
    pub volatility_threshold: Decimal,
    /// 价格跳变阈值，百分比形式（如 `0.05` 代表 5%）。
    pub jump_threshold_pct: Decimal,
    /// 线性插值减仓比例公式中的敏感度系数。
    pub sensitivity: Decimal,
    /// 波动率计算使用的滚动窗口大小（传给 `RollingStdDev::new`）。
    pub volatility_window: usize,
    /// 用哪种行情事件类型驱动波动率/跳变计算。
    pub price_source: PriceSource,
    /// 若为 `Some`，额外启用账户级权益波动检测，使用同样的
    /// 阈值/敏感度语义，但输入是 `PortfolioLedger::equity()` 的时间序列
    /// 而非单个 symbol 的价格。触发时对所有当前持仓 symbol 统一应用
    /// 同一 `reduce_fraction`。
    pub account_equity_volatility_threshold: Option<Decimal>,
    /// 降风险平仓是否也适用于现货仓位（`Position.leverage: None`）。
    /// 默认 `false`（仅杠杆仓位）。
    pub include_spot: bool,
}

pub enum PriceSource {
    /// 用 `MarketEvent::Trade.price`（默认，适用现货）。
    Trade,
    /// 用 `MarketEvent::MarkPrice.mark_price`（永续合约场景）。
    MarkPrice,
}
```

### 2.2 `apexes_risk::MarketConditionEventMonitor`（新 trait）

```rust
/// 每个事件循环收到一个 `MarketEvent` 就调用一次（不同于
/// `PositionMonitor::on_tick`，后者只携带 `PositionMarginStatus`，没有
/// 原始价格数据）。实现者据此增量更新自己的波动率/跳变检测状态。
pub trait MarketConditionEventMonitor: Send + Sync {
    fn on_market_event(&mut self, event: &MarketEvent) -> Result<(), Vec<RiskViolation>>;
}
```

`RiskViolation` 新增两个变体，明确区分 symbol 级与账户级触发（不使用哨兵值，保持类型安全）：

```rust
MarketConditionBreached { symbol: String, reduce_fraction: Decimal },
AccountWideMarketConditionBreached { reduce_fraction: Decimal },
```

### 2.3 `apexes_risk::MarketConditionMonitor`（新结构体）

```rust
pub struct MarketConditionMonitor {
    config: MarketConditionConfig,
    /// 每个 symbol 独立的滚动标准差状态。
    volatility: HashMap<String, RollingStdDev>,
    /// 每个 symbol 最近一次价格，用于计算跳变百分比。
    last_price: HashMap<String, Decimal>,
    /// 账户级权益的滚动标准差状态（仅当
    /// `config.account_equity_volatility_threshold.is_some()` 时使用）。
    equity_volatility: Option<RollingStdDev>,
    /// 当前每个 symbol 应用的减仓比例（0 表示未触发/已恢复）。
    reduce_fractions: HashMap<String, Decimal>,
}

impl MarketConditionMonitor {
    pub fn new(config: MarketConditionConfig) -> Self { .. }

    /// 拆分为三个适配器，分别实现 `PreTradeCheck`（阻止已触发 symbol 的
    /// 新单）、`MarketConditionEventMonitor`（消费行情事件更新内部状态）、
    /// 以及一个供调用方读取当前减仓比例的只读句柄。镜像
    /// `MarginMonitor::split()` 的 `Arc<Mutex<_>>` 共享状态模式。
    pub fn split(self) -> (
        MarketConditionPreTradeAdapter,
        MarketConditionEventAdapter,
        MarketConditionReadHandle,
    ) { .. }

    /// 供 `run_event_loop`/`run_live_trading` 每个 tick 读取——当前每个
    /// symbol（或账户级触发展开后的每个持仓 symbol）应该减仓的比例。
    /// 只包含非零值。
    pub fn reduce_fractions(&self) -> HashMap<String, Decimal> { .. }

    pub fn reset(&mut self, symbol: &str) { .. }
}
```

`account_equity_volatility_threshold` 触发时，`reduce_fractions()` 的实现需要把计算出的账户级 `reduce_fraction` 展开写入**当前持有仓位的每一个 symbol**（而不是用哨兵 key）——这样 `PortfolioLedger::check_de_risking` 不需要知道"账户级触发"这个概念，只看到一个已经展开好的 `HashMap<String, Decimal>`，保持 `check_de_risking` 接口简单。

`CompositeRiskChecker` 新增第四个 `Mutex<Vec<Box<dyn MarketConditionEventMonitor>>>` 字段 + `with_market_condition_monitor` builder + `on_market_event`（聚合调用所有已注册的实现,风格与现有 `on_fill`/`on_tick` 完全一致）。

### 2.4 `apexes-backtest::PortfolioLedger::check_de_risking`（新方法）

```rust
/// 返回每个需要部分平仓的 `(symbol, side, quantity)`。`reduce_fractions`
/// 由调用方从 `MarketConditionMonitor::reduce_fractions()` 读取后传入
/// （账户级触发已经在那里展开为逐 symbol 的比例，这里不需要区分来源）。
/// 仅当 symbol 出现在 `reduce_fractions` 且该 symbol 当前有非零持仓时才
/// 返回；`include_spot=false` 时跳过 `Position.leverage.is_none()` 的
/// 现货仓位。`quantity = position.quantity.abs() * reduce_fraction`，
/// `side` 与 `check_liquidations`/`run_event_loop` 现有的强平平仓方向
/// 推导逻辑一致（多头减仓=Sell，空头减仓=Buy）。
pub fn check_de_risking(
    &self,
    reduce_fractions: &HashMap<String, Decimal>,
    include_spot: bool,
) -> Vec<(String, Side, Decimal)> { .. }
```

---

## 三、`run_event_loop`/`run_live_trading` 接入

### 3.1 每个事件循环的调用点

- 每收到一个 `MarketEvent`：调用 `checker.on_market_event(&event)`（若已注册 `MarketConditionEventMonitor`），与现有 `on_fill` 调用点风格一致。
- 每个 tick（与行 `6.4` 的 `apply_funding_payments`/`margin_status`/`check_liquidations` 同一位置，紧邻它们之后）：
  1. 若配置了账户级权益波动检测，先用最新 `mark_prices` 调 `ledger.equity(&mark_prices)`，通过 `MarketConditionMonitor` 新增的 `pub fn on_equity_update(&mut self, equity: Decimal) -> Result<(), Vec<RiskViolation>>` 方法喂给账户级 `RollingStdDev`（与 `on_market_event` 平级的另一个更新入口，同样通过 `split()` 产生的适配器暴露给调用方，不归入 `MarketConditionEventMonitor` trait——因为它的输入是 `Decimal` 而非 `MarketEvent`，语义不同）。
  2. 读取 `monitor.reduce_fractions()`。
  3. 调用 `ledger.check_de_risking(&reduce_fractions, config.market_condition_config.map(|c| c.include_spot).unwrap_or(false))`。
  4. 回测/模拟盘：对每个返回的 `(symbol, side, quantity)` 向 `MatchingEngine` 提交市价单，按 `apply_fills` 现有路径处理（与强平平仓的提交方式完全一致，包括杠杆仓位需要携带 `position.leverage`/`.margin_mode` 以走 `close_leveraged` 路径而不是误入现货路径——直接复用行 `6.4` 强平平仓时已经踩过并修复的这个坑，不需要重新发现）。
  5. 实盘：`config.auto_derisk_live == false`（默认）时只 `tracing::error!` 记录；`true` 时与回测/模拟盘一样提交平仓单。

### 3.2 公开配置字段（从任务拆解第一步就加上，不留到后续任务）

- `BacktestConfig`/`PaperTradingConfig` 新增 `pub market_condition_config: Option<MarketConditionConfig>`（`None` = 完全不启用，行为与今天完全一致——静默 no-op，同 `margin_tiers` 的空表约定）。
- `LiveTradingConfig` 新增同名字段 + `pub auto_derisk_live: bool`。
- 三者都直接、立即地贯穿到内部构造的 `EventLoopConfig`/等价结构，不经过任何中间"暂时只加在内部类型上，以后再暴露"的过渡状态——这是本设计从行 `6.4` 的最终全分支审查中吸取的最重要教训。

---

## 四、与行 `6.4` 的关系与边界

| | 行 6.4（保证金杠杆监控） | 行 6.5（本设计） |
|---|---|---|
| 触发信号 | 单个杠杆仓位的保证金率（`allocated_margin + unrealized_pnl` vs 维持保证金） | 单个 symbol 的价格波动率/跳变，或账户级权益波动 |
| 覆盖仓位 | 仅杠杆仓位（现货无保证金概念） | 默认仅杠杆仓位，可配置扩展到现货 |
| 平仓比例 | 全部平仓（`check_liquidations` 返回的 symbol 全部强平） | 线性插值的部分比例（`check_de_risking` 返回按比例平仓） |
| 回测/模拟盘行为 | 自动全部强平 | 自动按比例部分平仓 |
| 实盘行为 | 永不自动强平（依赖交易所自己的强平引擎），只监控+阻止新单 | 默认同 6.4（监控+阻止新单），但允许调用方显式开启自动执行 |
| 检查器类型 | `MarginMonitor`（`PreTradeCheck` + `PositionMonitor`） | `MarketConditionMonitor`（`PreTradeCheck` + 新 `MarketConditionEventMonitor`） |

两者都遵循 `CompositeRiskChecker` 的聚合模式，在同一个 `run_event_loop`/`run_live_trading` tick 中先后（顺序不敏感，两者读写的是不同的 `PortfolioLedger` 状态，互不冲突）各自计算、各自提交平仓单——一个 symbol 理论上可能同时触发保证金强平和极端行情降风险，此时两个平仓单会先后提交，第二个提交时读取的仓位数量已经反映第一个平仓单的结果（`PortfolioLedger` 是顺序更新的，不存在竞态）。
