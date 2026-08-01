# Apexes — 子项目 #9：EMS / 执行算法（`apexes-ems`）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#9`（EMS / 执行算法）的详细设计。对应 `docs/FEATURES.md` 行 `5.2.1`（智能路由 SOR）、`5.2.2`（执行算法 TWAP/VWAP/冰山/POV）、`5.2.3`（跨市场订单拆分与统一执行）。
>
> 前置依赖：`apexes-core`（`Order`/`OrderIntent`/`Instrument` 等）、`apexes-oms`（`ExecutionGateway`/`OrderManager`，`#7` 已完成）、`apexes-gateway-binance`（真实 Binance 现货+永续合约网关，`#8` 已完成）、`apexes-live-trading`（`run_live_trading`，`#8` 已完成）、`apexes-storage`（ClickHouse 历史 bar 数据，`#2` 已完成）。

---

## 一、范围

### 决策背景：为什么拆分为三个阶段

`#9` 的完整目标（SOR + 执行算法 + 跨市场拆分）与本项目现有架构存在两处结构性缺口，brainstorming 阶段与用户逐一确认：

1. **多实例组件的显式依赖注入已不够用**：`OrderIntent::routing`（见 §三）允许策略请求"帮我在现货/永续合约之间选一个更好的"，这需要 EMS 能拿到"当前注册了哪些 `MarketDataProvider`"这类**运行时可能存在多个实例**的信息；而 `run_live_trading`/`OrderManager::new` 目前用的是"调用方显式构造一个具体实例，作为函数参数传入"的模式（`ExecutionGateway`/`PreTradeCheck`/`MarketDataProvider` 皆如此）。用户明确要求引入一个轻量级的**核心对象注册表**（详见 §二），并要求把它统一应用到全部既有可插拔组件上，而不仅仅是给 EMS 单独开一个特例。
2. **EMS 需要在回测/模拟盘/实盘三种运行模式下行为完全一致**（本项目"策略永远不知道自己在回测还是实盘"这一硬性要求的自然延伸）。用户经过多轮确认后明确：**只有一个真实的、PostgreSQL 持久化的 `Ems` 实现**用于实际运行（含真实回测运行——不是单元测试），另有一个纯内存实现仅用于 `apexes-ems` 自身的单元测试；但为避免每次网格搜索/灰度对比都拖慢（`apexes-optimize`/`apexes-strategy-registry` 目前零 I/O、每次跑数百次回测），`run_backtest` 及其下游调用者（网格搜索、灰度对比）被排除在"必须使用 PostgreSQL 版 EMS"之外，改用内存版；`apexes-paper`（模拟盘）同理，继续保持零 I/O，用内存版；只有 `apexes-live-trading`（本来就已经通过 `OrderManager`/`OrderStore` 依赖 PostgreSQL）使用真实的 PostgreSQL 版 `Ems`。

因此本子项目按依赖顺序拆分为三个阶段，均在 `#9` 这一个子项目/一份实现计划下顺序推进：

- **阶段一**：`apexes-core` 新增核心对象注册表（`Registry`）。
- **阶段二**：把既有的三个可插拔组件（`ExecutionGateway`、`MarketDataProvider`、`PreTradeCheck`）**彻底改为**通过注册表获取，移除原有的构造函数参数注入方式（不保留向后兼容）。
- **阶段三**：新增 `apexes-ems` crate，实现 SOR、执行算法（TWAP/VWAP/冰山/POV）、跨市场拆分，其 `Ems` trait 本身也通过注册表暴露。

### 包含

1. **`apexes-core::registry`**：进程级线程安全的核心对象注册表（详见 §二）。
2. **既有组件迁移**（详见 §三）：`ExecutionGateway`（来自 `apexes-oms`）、`MarketDataProvider`（来自 `apexes-core`/`apexes-market-data`）、`PreTradeCheck`（来自 `apexes-risk`）全部改为注册表查找方式获取，`OrderManager::new`/`run_backtest`/`run_paper_trading`/`run_live_trading` 等构造函数签名相应变更。
3. **`apexes-storage` 新增能力**：`intraday_volume_curve`——从历史 bar 数据构建日内成交量分布曲线，供 VWAP 执行算法使用（详见 §四）。
4. **新增 workspace 成员 crate：`apexes-ems`**（详见 §五）：
   - `Ems` trait：提交/查询/撤销"父订单"（算法执行订单）。
   - `InMemoryEms`：纯内存实现，仅用于 `apexes-ems` 自身单元测试。
   - `PostgresEms`：PostgreSQL 持久化实现，供 `apexes-live-trading` 使用，支持进程重启后恢复未完成的算法执行状态。
   - 四种执行算法：TWAP（时间切片）、VWAP（依据历史成交量曲线配速）、冰山（限价挂单，仅显示部分数量）、POV（按实时成交量比例参与）。
   - `VenueScorer` trait：可插拔的"给定候选交易场所，打分"接口；本轮提供一个诚实标注局限性的默认实现 `PriceAndFeeScorer`（仅考虑最优报价与手续费，明确不考虑合规/成交概率等因素），供智能路由（选最优）与跨市场拆分（按分数归一化为拆分比例）共用。
5. **`apexes-strategy` 扩展**：`OrderIntent` 新增 `execution_algo: Option<ExecutionAlgoSpec>` 与 `routing: OrderRouting` 两个字段。与 `#8` 给 `Order`/`OrderIntent` 新增 `asset_class` 字段时的经验一致——这是一次破坏性变更，工作区内全部现有穷举式 `OrderIntent { .. }` 构造调用点（`apexes-strategy` 自身测试、`apexes-backtest`、`apexes-risk`、`apexes-paper`、`apexes-oms`、`.drift` 脚本 fixture 等）都需要补上这两个新字段（默认 `execution_algo: None, routing: OrderRouting::Fixed`，与现有行为完全等价）。实现计划阶段需要重复 `#8` Task 2-7 那样的"逐 crate 修复构造点 + 全工作区 checkpoint 编译验证"流程，并特别注意两类容易被常规 `grep "OrderIntent {"` 遗漏的隐藏调用点：(a) `.drift` 脚本 fixture 文件（`#8` 的 Task 7 checkpoint 曾在 `apexes-optimize`/`apexes-strategy-registry` 的 fixture 中发现过此类真实回归）；(b) `apexes-strategy/src/drift_dsl/signals.rs` 的 `signals.rebalance_order` 原生函数绑定——它在 Rust 代码里手工构造一个"`OrderIntent`-shaped"的 Drift Script 对象（`convert.rs`/`bindings.rs` 负责把这个对象转换回真正的 `OrderIntent`），需要同步补上新字段对应的 Drift Script 端默认值。

### 不包含（明确超出范围）

- **将注册表推广到 `Strategy`/其他尚未存在多实例场景的组件**：`Strategy` 本身在 `run_*` 系列函数中始终是单个具体类型参数（泛型 `impl Strategy`），没有"运行时查找哪个实现"的需求，不纳入本轮迁移。
- **VenueScorer 的高级实现**（订单簿深度衍生的成交概率、合规约束建模）：仅提供可插拔接口与一个诚实的基础实现，高级评分逻辑留给后续迭代或 `#15`（安全与合规）。
- **杠杆/保证金相关的路由决策**：沿用 `#8` 的既有假设（本网关不管理保证金模式配置），SOR/拆分只在现货与永续合约的已配置状态之间选择/分配，不涉及杠杆倍数调整。
- **除 Binance 现货/永续合约外的路由场景**：跨交易所路由需要 `#10`（其他市场网关）提供更多网关实现后才有意义，但 §三的注册表迁移与 §五的 `VenueScorer`/`Ems` 接口本身是交易所无关的通用设计，为 `#10` 到来时的扩展预留了空间。
- **父订单的取消/暂停之外的运维操作**（如手动调整执行速度、动态改变 `ExecutionAlgoSpec` 参数）：`Ems::cancel_parent_order` 是本轮唯一的运行时控制接口。

---

## 二、`apexes-core::registry`：核心对象注册表

### 设计目标

Rust 没有运行时反射，无法按类型名字符串查找实例；本注册表按 **`TypeId` + 业务 Key** 的二元组索引，支持"同一类型可能存在多个实例"（例如未来每个 `Venue` 一个 `MarketDataProvider`）与"同一类型必须只有一个实例"（例如 `ExecutionGateway`）两类场景，且对误用（重复注册、期望唯一但实际有多个）**快速失败**，而不是静默选择其中之一。

### API

```rust
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

/// 可作为注册表 Key 的类型的标记 trait；任何满足 `Eq + Hash + Clone +
/// Send + Sync + 'static` 的类型自动实现（blanket impl），包括 `()`
/// （用于"无需业务 Key，只有一个实例"的场景）。
pub trait RegistryKey: Any + Eq + Hash + Clone + Send + Sync + 'static {}
impl<K: Any + Eq + Hash + Clone + Send + Sync + 'static> RegistryKey for K {}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("an instance of {type_name} is already registered under this key")]
    AlreadyRegistered { type_name: &'static str },
    #[error("no instance of {type_name} is registered")]
    NotRegistered { type_name: &'static str },
    #[error("expected exactly one instance of {type_name}, found {count}")]
    AmbiguousRegistration { type_name: &'static str, count: usize },
}

pub struct Registry {
    // TypeId of Arc<dyn Trait> (or any T) -> TypeId of K -> (key -> value)
    // 具体存储结构见实现说明。
    entries: RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
}

impl Registry {
    pub fn new() -> Self;

    /// 注册 `value`，Key 为 `key`。若同一 `(T, key)` 已存在实例，返回
    /// `Err(AlreadyRegistered)`——不静默覆盖。生产代码路径统一使用此方法。
    pub fn register<T, K>(&self, key: K, value: Arc<T>) -> Result<(), RegistryError>
    where
        T: Any + Send + Sync,
        K: RegistryKey;

    /// 仅供测试辅助代码使用：若 `(T, key)` 已存在则静默跳过（不返回
    /// 错误、不覆盖），否则正常注册。用于并行测试场景下多个测试用例
    /// 各自尝试注册同一 mock 实现时避免误报"重复注册"错误。生产代码
    /// 路径不得调用此方法。
    pub fn register_or_skip<T, K>(&self, key: K, value: Arc<T>)
    where
        T: Any + Send + Sync,
        K: RegistryKey;

    /// 返回当前已注册的全部 `T` 实例，按 Key 索引。
    pub fn get<T, K>(&self) -> HashMap<K, Arc<T>>
    where
        T: Any + Send + Sync,
        K: RegistryKey;

    /// 断言当前恰好注册了一个 `T` 实例（不论其 Key 是什么）并返回它；
    /// 0 个或 2+ 个均返回 `Err`。用于"必须且只能有一个"的组件
    /// （`ExecutionGateway`、`Ems`）。
    pub fn one<T>(&self) -> Result<Arc<T>, RegistryError>
    where
        T: Any + Send + Sync;

    /// 类似 `one`，但 0 个实例时返回 `Ok(None)` 而非 `Err`（仅 2+ 个
    /// 仍是 `Err`）。用于"可选、允许完全不配置"的组件
    /// （`PreTradeCheck`：不配置即代表"不做事前风控"）。
    pub fn try_one<T>(&self) -> Result<Option<Arc<T>>, RegistryError>
    where
        T: Any + Send + Sync;
}

/// 进程级全局单例（`OnceLock` 实现），生产代码默认使用；测试代码可
/// 选择构造独立的 `Registry::new()` 实例传入，避免与其他测试共享
/// 全局状态（但见 §三"测试策略"一节——本轮实际选择的是共享全局
/// 注册表 + `--test-threads=1` + `register_or_skip`，而非每测试独立
/// 实例，记录两种方案以供后续参考）。
pub fn global() -> &'static Registry;
```

### `T` 与 Key 的选型约定

- **必须唯一的组件**（`ExecutionGateway`、`PreTradeCheck`、`Ems`）：`T = Arc<dyn Trait>`，`K = ()`（无业务含义的 Key，仅为满足 API 形状）。查找用 `one()`/`try_one()`。
- **可能存在多实例的组件**（`MarketDataProvider`）：`T = Arc<dyn MarketDataProvider>`，`K = Venue`（已存在的 `apexes_core::instrument::Venue` 枚举，天然的业务 Key）。查找用 `get::<_, Venue>()`。

### 为什么不做成对已有代码透明无感的自动装配

用户明确要求的是"线程安全哈希表 + 显式 `register`/查找"，而非运行时自动扫描/依赖图解析式的完整 DI 容器——后者是明显更大的工程（且 Rust 生态里通常需要过程宏或运行时类型注册表配合才能做到"自动发现"，与本项目当前规模不匹配）。本设计选择了用户认可的最小实现。

---

## 三、既有组件迁移到注册表

### 迁移范围与 Key 选型

| 组件 | 原构造方式 | 新方式 | `K` |
|---|---|---|---|
| `ExecutionGateway`（`apexes-oms`） | `OrderManager::new(store, gateway: Arc<dyn ExecutionGateway>, risk_checker)` | `OrderManager::new(store, registry: &Registry)`，内部调用 `registry.one::<Arc<dyn ExecutionGateway>>()` | `()` |
| `PreTradeCheck`（`apexes-risk`） | 同上，`risk_checker: Option<Box<dyn PreTradeCheck>>` 参数 | 同上，内部调用 `registry.try_one::<Arc<dyn PreTradeCheck>>()`（`None` 语义不变：未注册即"不做事前风控"） | `()` |
| `MarketDataProvider`（`apexes-core` trait，`apexes-market-data` 实现） | `run_backtest`/`run_paper_trading`/`run_live_trading` 显式接收 `&dyn MarketDataProvider`/`provider: &dyn MarketDataProvider` 参数 | 改为接收 `registry: &Registry`，内部调用 `registry.get::<Arc<dyn MarketDataProvider>, Venue>()` 按 `config.instruments` 涉及的 `Venue` 集合选取所需 provider（若某个 `Venue` 缺失注册，返回 `ApexesError`） | `Venue` |

**彻底移除旧签名**（用户明确表示不需要向后兼容）：`OrderManager::new`、`apexes_backtest::runner::run_backtest`、`apexes_backtest::event_loop::run_event_loop`、`apexes_paper::runner::run_paper_trading`、`apexes_live_trading::runner::run_live_trading` 的原有构造参数（`gateway`/`risk_checker`/`provider`/`market_data_provider`）全部替换为 `registry: &Registry`（或直接使用 `apexes_core::registry::global()`，视调用惯例统一为显式传参，以保持可测试性——测试可以传入独立构造的 `Registry` 或共享全局注册表，两种都支持，因为 `Registry` 本身不是单例类型，只有 `global()` 是单例访问入口）。

### 测试策略：全局注册表 + `--test-threads=1` + `register_or_skip`

由于 Rust 测试默认多线程并行执行，且本轮选择让测试代码同样使用 `apexes_core::registry::global()`（而非每测试构造独立 `Registry`），会出现多个测试并行调用 `register()` 竞争同一 `(T, ())` Key 的问题。解决方式：

1. 受影响 crate（`apexes-oms`、`apexes-backtest`、`apexes-paper`、`apexes-live-trading`）的测试统一改用 `cargo test -- --test-threads=1` 运行（本项目已有先例：`apexes-oms` 的 PostgreSQL 集成测试因共享数据库已采用此约定）。
2. 测试辅助函数（如 `fresh_manager`/`base_config` 一类的测试 setup helper）使用 `registry.register_or_skip(...)` 而非 `register(...)`，避免"这个 mock 网关之前某个测试已经注册过了"触发误报的 `AlreadyRegistered` 错误。
3. 生产代码路径（`main.rs`、真实部署入口）始终使用严格的 `register()`，重复注册仍是硬错误——`register_or_skip` 只出现在 `#[cfg(test)]` 代码里。

---

## 四、`apexes-storage`：日内成交量曲线（`intraday_volume_curve`）

### 动机

真实的 VWAP 执行算法需要"历史上这个时段通常占全天成交量的百分之多少"这一先验分布，用于将当前时刻应完成的累计执行比例与历史正常水平对比、动态调整切片大小——而不是简单假设成交量在一天内均匀分布（那样命名为"VWAP"是误导性的近似，讨论后明确放弃这种简化方案，改为构建真实能力）。

### API（`apexes-storage`）

```rust
pub struct VolumeCurve {
    /// 按 `(距离曲线起点的时间偏移量, 累计成交量占比 0.0..=1.0)` 排列，
    /// 单调递增，最后一个元素的占比恒为 1.0。
    pub cumulative_fractions: Vec<(Duration, Decimal)>,
}

impl StorageReader {
    /// 从 ClickHouse 的 `bars` 表查询过去 `lookback_days`个自然日
    /// 同一 symbol/venue 的历史日内成交量分布，按 bar 的
    /// 时间偏移量聚合出平均累计占比曲线。
    ///
    /// 若历史数据不足 `lookback_days` 天（例如新上市 symbol、或
    /// venue 刚接入不久），返回
    /// `Err(StorageError::InsufficientHistory { available_days, requested_days })`
    /// ——不做"数据不够就退化为均匀分布"的静默兜底，调用方（EMS 的
    /// VWAP 调度器）据此拒绝该笔 VWAP 父订单。
    pub async fn intraday_volume_curve(
        &self,
        symbol: &str,
        venue: Venue,
        lookback_days: u32,
    ) -> Result<VolumeCurve, StorageError>;
}
```

`StorageError` 新增 `InsufficientHistory { available_days: u32, requested_days: u32 }` 变体。

---

## 五、`apexes-ems`：EMS 核心

### `OrderIntent` 扩展（`apexes-strategy`）

```rust
pub struct OrderIntent {
    pub symbol: String,
    pub venue: Venue,
    pub asset_class: AssetClass,   // 不变：routing == Fixed 时的唯一依据
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
    // 新增字段：
    pub execution_algo: Option<ExecutionAlgoSpec>,
    pub routing: OrderRouting,
}

pub enum ExecutionAlgoSpec {
    /// 时间切片：把 `quantity` 均分为 `slices` 份，每隔
    /// `duration / slices` 提交一份子订单。调度依据"时间流逝"而非
    /// 行情内容（不需要 `MarketEvent::Depth`/`Trade` 的具体数值），
    /// 但"时间"本身在回测/模拟盘路径下是虚拟时间（见下文"调度时钟
    /// 来源"一节），在实盘路径下是真实挂钟时间——并非任何路径都用
    /// 真实定时器。
    Twap { duration: Duration, slices: u32 },
    /// 依据历史日内成交量曲线配速：调度开始时调用
    /// `StorageReader::intraday_volume_curve(symbol, venue, lookback_days)`，
    /// 若返回 `InsufficientHistory` 则该父订单直接以 `Failed` 状态
    /// 结束（不提交任何子订单）。
    Vwap { duration: Duration, lookback_days: u32 },
    /// 冰山：任意时刻最多在场上挂 `visible_qty` 数量的可见子订单，
    /// 每当无挂单或挂单刚成交完毕（`MarketEvent::Depth` 驱动检查），
    /// 补充下一份 `min(visible_qty, 剩余数量)` 子订单，直至耗尽。
    Iceberg { visible_qty: Decimal },
    /// 按实时成交量参与率下单：每次收到该 symbol 的
    /// `MarketEvent::Trade`，提交 `trade.quantity * participation_rate`
    /// （不超过剩余数量）的子订单。
    Pov { participation_rate: Decimal },
}

pub enum OrderRouting {
    /// 现有行为：严格按 `asset_class` 下单，不做路由决策。
    Fixed,
    /// 智能路由：`VenueScorer` 对候选场所（现货/永续合约）打分，
    /// 选分数最高的一个作为整笔订单的下单场所。
    RouteToBest,
    /// 跨市场拆分：`VenueScorer` 对候选场所打分并归一化为拆分比例，
    /// 按比例把 `quantity` 分配到多个场所并行下单。
    Split(Vec<AssetClass>),
}
```

### `VenueScorer`：可插拔场所评分接口

```rust
pub struct VenueMarketState {
    /// 每个候选场所当前的最优买卖价（来自 `MarketEvent::Depth`）。
    pub best_bid_ask: HashMap<AssetClass, (Decimal, Decimal)>,
    /// 每个候选场所的预估有效手续费率（basis points，maker/taker 视
    /// 订单类型由调用方预先解析好，`VenueScorer` 不关心其来源）。
    /// 调用方（`apexes-live-trading`/`apexes-paper`/`apexes-backtest`
    /// 各自持有的 `apexes_backtest::CostModel`）负责把自己的
    /// `FeeSchedule` 结果转换填入此处——`apexes-ems` 不直接依赖
    /// `apexes-backtest`，避免与"`apexes-backtest` 依赖 `apexes-ems`
    /// 获取 `InMemoryEms`"（见下）形成循环依赖。
    pub estimated_fee_bps: HashMap<AssetClass, Decimal>,
}

pub trait VenueScorer: Send + Sync {
    /// 分数越高越优。返回值只用于比较相对高低（`RouteToBest` 取最大值）
    /// 或归一化为拆分权重（`Split`），无绝对量纲要求。
    fn score(
        &self,
        intent: &OrderIntent,
        candidates: &[AssetClass],
        market_state: &VenueMarketState,
    ) -> HashMap<AssetClass, Decimal>;
}

/// 本轮提供的唯一具体实现：仅按"最优报价 - 预估手续费"打分。
/// **明确局限性**（写入文档注释，供后续读者/评审确认这是已知的、
/// 刻意的简化，而非疏漏）：不考虑订单簿深度衍生的实际成交概率、
/// 不考虑合规/监管约束、不考虑跨场所延迟差异。后续若需要更精细的
/// 评分逻辑（如接入 `#15` 的合规约束，或基于深度数据估算冲击成本），
/// 实现一个新的 `VenueScorer` 并通过注册表替换即可，无需改动 EMS
/// 路由/拆分的核心调度逻辑。
pub struct PriceAndFeeScorer;
impl VenueScorer for PriceAndFeeScorer { /* ... */ }
```

`VenueScorer` 本身也通过注册表暴露：`registry.one::<Arc<dyn VenueScorer>>()`，`K = ()`。

### `Ems` trait

`ParentOrderId`/`ParentOrderStatus` 定义在 `apexes-strategy`（与 `OrderIntent`/`ExecutionAlgoSpec`/`OrderRouting` 同一 crate，理由相同）：`StrategyContext`（见下）新增字段需要引用这两个类型，而 `StrategyContext` 定义在 `apexes-strategy`；若这两个类型定义在 `apexes-ems`，会导致 `apexes-strategy` 反向依赖 `apexes-ems`，与 `apexes-ems` 依赖 `apexes-strategy` 形成循环。`apexes-ems` 只使用、不重新定义它们。

```rust
// 定义于 apexes-strategy：
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ParentOrderId(pub Uuid);

#[derive(Debug, Clone, PartialEq)]
pub enum ParentOrderStatus {
    Scheduling,   // 仍在按算法节奏提交子订单
    Completed,    // 全部数量已提交完毕且子订单均已到达终态
    Cancelled,    // 用户主动撤销，未提交的部分不再提交
    Failed { reason: String },  // 如 VWAP 历史数据不足
}

// 定义于 apexes-ems：
pub struct AlgoOrderRequest {
    pub symbol: String,
    pub venue: Venue,
    pub side: Side,
    pub quantity: Decimal,
    pub execution_algo: ExecutionAlgoSpec,
    pub routing: OrderRouting,
}

#[async_trait]
pub trait Ems: Send + Sync {
    /// 提交一个父订单，立即返回 `ParentOrderId`；实际的子订单调度在
    /// 后台异步进行（具体调度机制随实现而异：`PostgresEms` 用
    /// `tokio::spawn` 的后台任务持久化每一步状态；`InMemoryEms`
    /// 同样用 `tokio::spawn` 但状态只在内存）。
    async fn submit_algo_order(&self, request: AlgoOrderRequest) -> Result<ParentOrderId, EmsError>;

    /// 查询父订单当前的聚合状态（跨其全部子订单）。
    async fn parent_order_status(&self, id: ParentOrderId) -> Result<Option<ParentOrderStatus>, EmsError>;

    /// 撤销父订单：停止调度尚未提交的切片；已经提交且仍处于活跃
    /// 状态的子订单会通过 `OrderManager::cancel_order` 逐一撤销。
    async fn cancel_parent_order(&self, id: ParentOrderId) -> Result<(), EmsError>;
}
```

### 集成点：`run_live_trading`（`apexes-live-trading`）与共享事件循环核心（`apexes_backtest::event_loop::run_event_loop`）

`StrategyContext` 的构造点在整个工作区里只有两处：`apexes_backtest::event_loop::run_event_loop`（`run_backtest`/`run_paper_trading` 共用的核心，见 `#6` 设计）与 `apexes_live_trading::runner::run_live_trading`。二者都需要同样的集成逻辑，因此下述改动需要同时应用到两处（而非只改 `run_live_trading`）：

处理 `strategy.on_market_event` 返回的每个 `OrderIntent` 时：

```rust
if intent.execution_algo.is_some() {
    let ems = registry.one::<Arc<dyn Ems>>()?;
    ems.submit_algo_order(AlgoOrderRequest::from(intent)).await?;
} else {
    order_manager.submit_order(intent, ...).await?;  // 现有路径不变
}
```

`StrategyContext.open_orders`（策略可见的订单视图）需要能反映"这是一个父订单"而非暴露一堆离散子订单——具体做法：`StrategyContext` 新增一个只读字段 `open_parent_orders: &HashMap<ParentOrderId, ParentOrderStatus>`（来自 `ems.parent_order_status` 批量查询的快照），策略据此感知算法执行进度，同时子订单本身仍然存在于 `order_manager.open_orders()` 中（供 `PortfolioLedger`/`PerformanceTracker` 正常统计持仓与成交），只是策略在决策时主要参考父订单视图而非逐个子订单。

`run_backtest`/`run_paper_trading` 走的是 `run_event_loop` 这条共享路径，其内部构造的 `MatchingEngine`/`OrderManager` 等价物是纯内存的（`run_backtest` 甚至没有 `OrderManager` 概念，直接用 `MatchingEngine`）——因此这两者注册进注册表的是 `InMemoryEms`，其 `submit_algo_order` 内部把子订单交给 `run_event_loop` 已有的撮合路径（`matching_engine.submit_market_order` 等），而非 `OrderManager::submit_order`（后者是 `apexes-oms`/`apexes-live-trading` 专属，`apexes-backtest`/`apexes-paper` 从不依赖 `apexes-oms`）。换言之，`InMemoryEms`/`PostgresEms` 不仅在"是否持久化"上不同，在"子订单最终提交给谁"上也不同（前者提交给 `MatchingEngine`，后者提交给 `OrderManager`）——`Ems` trait 的 `submit_algo_order` 只关心"提交并调度子订单"这一行为契约，不假设具体的子订单提交对象是什么，具体实现各自持有自己需要的引用（`InMemoryEms` 持有 `&mut MatchingEngine` 或等价物，`PostgresEms` 持有 `OrderManager`）。

**调度时钟来源的关键差异**：`apexes-backtest` 用 `BacktestClock`（`ReplaySpeed::AsFastAsPossible` 等）驱动虚拟/事件时间，一整天的历史行情可能几秒内重放完毕——若 TWAP/VWAP/POV 调度器内部用真实的 `tokio::time::sleep`/`tokio::time::interval` 计时，回测中的算法执行会实际耗费真实挂钟时间（例如一个 30 分钟窗口的 TWAP 会让整个回测卡住等待 30 分钟），彻底违背回测"快速重放"的存在意义。因此：

- **`InMemoryEms`（回测/模拟盘路径）**：调度节奏依据 `run_event_loop` 已知的"当前事件虚拟时间"推进，而非真实挂钟时间——具体机制是 `run_event_loop` 每处理一个 `MarketEvent` 后，把该事件的时间戳（虚拟时间）通过一个新方法（如 `Ems::advance_virtual_clock(timestamp)`，仅 `InMemoryEms` 有意义的实现，`PostgresEms` 该方法为空操作）告知 EMS，`InMemoryEms` 内部据此判断"虚拟时间是否已到下一个切片应提交的时刻"，而不依赖任何真实定时器。
- **`PostgresEms`（实盘路径）**：使用真实 `tokio::time::sleep`/`tokio::time::interval`，因为实盘中真实时间流逝就是调度依据本身，与 `run_live_trading` 已有的 `reconcile_timer` 是同一性质的真实定时器。

`Ems` trait 因此新增：

```rust
#[async_trait]
pub trait Ems: Send + Sync {
    async fn submit_algo_order(&self, request: AlgoOrderRequest) -> Result<ParentOrderId, EmsError>;
    async fn parent_order_status(&self, id: ParentOrderId) -> Result<Option<ParentOrderStatus>, EmsError>;
    async fn cancel_parent_order(&self, id: ParentOrderId) -> Result<(), EmsError>;
    /// 告知当前虚拟/事件时间已推进到 `timestamp`，供依赖虚拟时钟调度
    /// 的实现（`InMemoryEms`）据此触发到期的切片提交。`PostgresEms`
    /// （依赖真实挂钟时间的独立后台任务调度）对此方法是空操作。
    async fn advance_virtual_clock(&self, timestamp: DateTime<Utc>) -> Result<(), EmsError>;
}
```

### `PostgresEms` 的持久化 schema

```sql
CREATE TABLE IF NOT EXISTS parent_orders (
    id                  UUID PRIMARY KEY,
    symbol              TEXT NOT NULL,
    venue               TEXT NOT NULL,
    side                TEXT NOT NULL,
    total_quantity      NUMERIC(38, 10) NOT NULL,
    filled_quantity     NUMERIC(38, 10) NOT NULL,
    algo_spec           JSONB NOT NULL,
    routing             JSONB NOT NULL,
    status              TEXT NOT NULL,
    failure_reason      TEXT,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS parent_order_children (
    parent_order_id     UUID NOT NULL REFERENCES parent_orders(id),
    child_order_id      UUID NOT NULL,
    sequence_number     INT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (parent_order_id, child_order_id)
);
```

子订单本身**不是新实体**——它们是通过 `OrderManager::submit_order` 正常提交的 `apexes_core::Order`，完整生命周期（状态机、成交、对账）沿用 `apexes-oms` 现有机制不变；`parent_order_children` 只记录"哪些 `OrderId` 属于哪个父订单及提交顺序"，供 `Ems` 内部聚合子订单状态为父订单状态、以及进程重启后恢复调度进度使用。

`PostgresEms::new(pool)` 构造时，比照 `OrderManager::new`，重新加载全部 `status = 'Scheduling'` 的父订单行并根据已提交的子订单数量恢复调度循环（从 `filled_quantity`/已提交切片数继续，而非从头重来）。

### `apexes-ems` 依赖关系

`apexes-ems` 依赖 `apexes-oms`（直接调用 `OrderManager::submit_order`/`cancel_order` 提交/撤销子订单）、`apexes-core`、`apexes-strategy`（`OrderIntent`/`ExecutionAlgoSpec`/`OrderRouting` 定义位置）、`apexes-storage`（`intraday_volume_curve`）。**不依赖 `apexes-backtest`**——`apexes-backtest` 需要依赖 `apexes-ems`（获取 `InMemoryEms`，见下），若 `apexes-ems` 反过来依赖 `apexes-backtest`（例如为了复用 `CostModel`）将形成循环依赖；因此 `VenueScorer::score` 的手续费输入改为调用方预先解析好、以纯数据形式（`VenueMarketState::estimated_fee_bps`）传入，而非 `apexes-ems` 直接持有/调用 `apexes_backtest::CostModel`。

`ExecutionAlgoSpec`/`OrderRouting` 定义在 `apexes-strategy`（与 `OrderIntent` 同一 crate），而非 `apexes-ems`：`OrderIntent` 直接以字段形式内嵌这两个类型，若定义在 `apexes-ems` 会导致 `apexes-strategy` 反向依赖 `apexes-ems`——而 `apexes-ems` 本身需要依赖 `apexes-strategy`（获取 `OrderIntent`/`Strategy`/`StrategyContext`）——形成循环依赖。`apexes-ems` 只依赖并使用这两个类型，不定义它们（与 `apexes-oms` 依赖 `apexes-strategy` 的 `OrderIntent`/`Position` 而不重新定义是同样的既有模式）。

`apexes-ems` **不是** `apexes-backtest`/`apexes-paper`/`apexes-live-trading` 的依赖倒置对象——而是三者都直接依赖 `apexes-ems`（获取 `InMemoryEms`/`PostgresEms` 构造函数），并各自决定注册哪个实现进注册表：`run_backtest`/`run_paper_trading` 注册 `InMemoryEms`，`run_live_trading` 注册 `PostgresEms`。

---

## 六、测试策略

- **`apexes-core::registry`**：纯单元测试，覆盖 `register`/`register_or_skip`/`get`/`one`/`try_one` 的全部分支（含重复注册报错、0/1/2+ 实例的 `one`/`try_one` 行为）。
- **既有组件迁移**：既有测试套件（`apexes-oms`/`apexes-backtest`/`apexes-paper`/`apexes-live-trading`）改造为通过注册表构造依赖，验证行为与迁移前一致（无回归）；受影响测试统一加入 `--test-threads=1` 运行要求。
- **`apexes-storage::intraday_volume_curve`**：需要真实 ClickHouse（`#[ignore]`，沿用既有 `live_clickhouse.rs` 集成测试模式），覆盖"历史数据充足"与"历史数据不足报错"两条路径。
- **`apexes-ems`**：
  - `InMemoryEms` 单元测试覆盖四种算法的调度逻辑（TWAP 切片节奏、VWAP 历史曲线配速与数据不足报错路径、冰山补单、POV 按成交量参与）与 `VenueScorer`/`PriceAndFeeScorer` 的路由/拆分决策。
  - `PostgresEms` 的持久化与重启恢复需要真实 PostgreSQL，`#[ignore]`，沿用 `apexes-oms` 既有集成测试模式。
  - 端到端集成测试：`run_live_trading` + `MockExecutionGateway` + `PostgresEms`，验证一个标记 `execution_algo` 的 `OrderIntent` 真的被拆分为多个子订单提交、`StrategyContext.open_parent_orders` 正确反映聚合状态。
