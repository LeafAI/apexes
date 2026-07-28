# Apexes — 子项目 #0：基础骨架（Foundation Skeleton）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#0` 的详细设计。子项目 #0 的唯一目标是搭建 Cargo workspace 与所有下游子项目共用的核心类型/trait，**不包含任何真实业务逻辑、网络连接或交易所实现**。

---

## 一、范围

### 包含
- Cargo workspace 结构（`crates/apexes-core`, `crates/apexes-cli`）
- `Instrument` / `MarketType` / `Venue` / `AssetClass` 类型
- `Order` / `Fill` / `OrderStatus` 状态机类型
- `MarketEvent`（`Tick`/`Depth`/`Bar`/`Trade`）与 `MarketDataProvider` trait
- `ApexesError` 统一错误类型
- `apexes-cli` 空壳（仅 `--version`/`--help`）
- 单元测试：`OrderStatus` 状态转移 + 各类型构造/序列化往返测试

### 不包含（明确超出范围，留给对应子项目）
- 任何真实网关/连接器实现（Binance、CTP、IBKR 等）——`MarketDataProvider` 只是 trait，无任何实现者
- OMS 运行时逻辑（持久化、断线重连对账）——留给子项目 #7
- 风控引擎、策略引擎、回测引擎——分别留给子项目 #5、#4、#3
- 消息总线、ClickHouse/PostgreSQL 集成——留给子项目 #12
- UI、监控运维——留给子项目 #13、#14

---

## 二、开源库复用决策

调研了 `barter-rs`（`barter-instrument`/`barter-data`/`barter-execution`）生态后确认：其 `ExchangeId` 是不可扩展的封闭枚举，仅覆盖约 40 家加密货币交易所，无法表达 A股/H股/美股概念；且 `barter-data` 内部存在 `f64`（`PublicTrade`/`Candle`）与 `Decimal`（`OrderBookL1`）混用的精度不一致问题。

**决策**：`apexes-core` 的核心类型（`Instrument`/`Order`/`Fill`/`MarketEvent`/`MarketDataProvider`）保持完全自研，覆盖四类市场、全程使用 `Decimal`。`barter-data`/`barter-execution` 作为加密货币网关的**底层连接器实现**候选，留给子项目 #1（行情接入）与 #8（首个实盘网关）在网关适配层内部评估使用，与 `apexes-core` 类型的转换发生在该适配层边界，不侵入核心抽象。

（该决策已同步记录到顶层路线图文档 §2.6。）

---

## 三、Workspace 与 Crate 布局

```
~/src/oss/apexes/
  Cargo.toml                 # workspace root
  crates/
    apexes-core/
      Cargo.toml
      src/
        lib.rs
        error.rs             # ApexesError
        instrument.rs        # Instrument, MarketType, Venue, AssetClass
        order.rs             # Order, OrderStatus, Side, OrderType, OrderId
        fill.rs               # Fill, FillId
        market_data.rs       # MarketEvent, Tick, DepthUpdate, Bar, TradeEvent,
                              # PriceLevel, BarInterval, MarketDataProvider
    apexes-cli/
      Cargo.toml
      src/
        main.rs               # clap skeleton, --version/--help only
```

### `apexes-core` 依赖

| Crate | 用途 |
|---|---|
| `rust_decimal`（`serde` feature） | 价格/数量精确计算，替代浮点数 |
| `serde`（`derive` feature） | 所有核心类型的序列化 |
| `thiserror` | `ApexesError` |
| `chrono`（`serde` feature） | 时间戳（`DateTime<Utc>`） |
| `tokio` | 异步运行时基础 |
| `futures` | `Stream`/`BoxStream` |
| `async-trait` | 使 `MarketDataProvider` 保持对象安全（可 `Box<dyn MarketDataProvider>`） |
| `uuid`（`v4` feature） | `OrderId`/`FillId` 生成 |

### `apexes-cli` 依赖

`apexes-core`（path 依赖）+ `clap`（`derive` feature）。

---

## 四、类型设计

### 4.1 `Instrument`

**关键决策**：`Market` 采用两级设计（区别于最初讨论的单一扁平枚举）——`market_type`（监管市场：AShare/HShare/UsEquity/Crypto）与 `venue`（具体交易所：Sse/Szse/Hkex/Nasdaq/Nyse/Binance/Okx/Coinbase）分离，便于风控/合规代码按监管市场分支判断，且新增交易所（如 Bybit）不影响 `MarketType`。

为防止构造出不一致的实例（如 `market_type: AShare` 搭配 `venue: Binance`），提供校验构造函数 `Instrument::new(...) -> Result<Instrument, ApexesError>`；字段保持 `pub`，便于模式匹配和字段更新，但推荐通过 `new()` 构造。

`tick_size`/`lot_size`/`multiplier` 为**必填** `Decimal`（非 `Option`）——强制调用方在构造前就已获取真实交易所元数据，避免下游任何代码需要处理"未知 tick size"的情况，这类精度值直接影响订单有效性。

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Instrument {
    pub symbol: String,
    pub market_type: MarketType,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub base_currency: Option<String>,
    pub quote_currency: Option<String>,
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub multiplier: Decimal,
}

impl Instrument {
    /// 校验 venue 属于 market_type 后再构造。
    pub fn new(
        symbol: impl Into<String>,
        market_type: MarketType,
        venue: Venue,
        asset_class: AssetClass,
        base_currency: Option<String>,
        quote_currency: Option<String>,
        tick_size: Decimal,
        lot_size: Decimal,
        multiplier: Decimal,
    ) -> Result<Self, ApexesError> {
        if venue.market_type() != market_type {
            return Err(ApexesError::InvalidInstrument {
                reason: format!(
                    "venue {venue:?} does not belong to market_type {market_type:?}"
                ),
            });
        }
        Ok(Self {
            symbol: symbol.into(),
            market_type,
            venue,
            asset_class,
            base_currency,
            quote_currency,
            tick_size,
            lot_size,
            multiplier,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketType {
    AShare,
    HShare,
    UsEquity,
    Crypto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Venue {
    Sse,
    Szse,
    Hkex,
    Nasdaq,
    Nyse,
    Binance,
    Okx,
    Coinbase,
}

impl Venue {
    pub fn market_type(&self) -> MarketType {
        match self {
            Venue::Sse | Venue::Szse => MarketType::AShare,
            Venue::Hkex => MarketType::HShare,
            Venue::Nasdaq | Venue::Nyse => MarketType::UsEquity,
            Venue::Binance | Venue::Okx | Venue::Coinbase => MarketType::Crypto,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AssetClass {
    Spot,
    Perpetual,
    Future,
    Option,
    Equity,
}
```

### 4.2 `Order` / `Fill` / `OrderStatus`

**关键决策**：
- `OrderId`/`FillId` 采用 Apexes 本地生成的 UUID newtype（而非交易所分配 ID），因为回测/模拟环境没有真实交易所来分配 ID；交易所确认后的 ID 存入独立的可选字段 `exchange_order_id: Option<String>`。
- `Order`/`Fill` 不内嵌完整 `Instrument`，而是引用 `symbol: String` + `venue: Venue` 组合——自描述、无需查表，这一阶段还没有 instrument 注册表。
- `OrderStatus` 状态机：`Rejected` 只能从 `New` 到达（代表"交易所完全拒绝接受该订单"，一旦部分成交就不可能再被拒绝，只能走向 `Filled` 或 `Cancelled`）。

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

impl OrderStatus {
    /// 判断 self -> next 是否为合法转移。
    pub fn can_transition_to(&self, next: OrderStatus) -> bool {
        use OrderStatus::*;
        matches!(
            (self, next),
            (New, PartiallyFilled)
                | (New, Filled)
                | (New, Cancelled)
                | (New, Rejected)
                | (PartiallyFilled, PartiallyFilled)
                | (PartiallyFilled, Filled)
                | (PartiallyFilled, Cancelled)
        )
        // Filled / Cancelled / Rejected 为终态，无后续转移
    }

    pub fn transition_to(&mut self, next: OrderStatus) -> Result<(), ApexesError> {
        if self.can_transition_to(next) {
            *self = next;
            Ok(())
        } else {
            Err(ApexesError::InvalidStateTransition { from: *self, to: next })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FillId(pub Uuid);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub venue: Venue,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    pub price: Option<Decimal>,          // None = Market order, Some = Limit order
    pub filled_quantity: Decimal,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub id: FillId,
    pub order_id: OrderId,
    pub symbol: String,
    pub venue: Venue,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_currency: Option<String>,
    pub filled_at: DateTime<Utc>,
}
```

### 4.3 `MarketEvent` / `MarketDataProvider`

**关键决策**：
- `MarketEvent` 为完整枚举（`Tick`/`Depth`/`Bar`/`Trade`），每个 variant 携带真实字段，而非占位符——符合顶层设计 §2.2 第 1.5 条"统一 Tick/Depth/Bar 模型"的要求。
- `MarketDataProvider` 使用 `#[async_trait]`（而非原生 RPITIT）以保持**对象安全**，从而支持 `Vec<Box<dyn MarketDataProvider>>`——这是顶层设计"免费优先、付费可插拔"原则（§2.1 第4条）的前提：数据源需要能在运行时通过配置动态切换，而非仅能编译期泛型选择。代价是每次调用有少量 boxing 开销，在这一阶段可接受。

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketEvent {
    Tick(Tick),
    Depth(DepthUpdate),
    Bar(Bar),
    Trade(TradeEvent),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tick {
    pub symbol: String,
    pub venue: Venue,
    pub bid_price: Decimal,
    pub bid_size: Decimal,
    pub ask_price: Decimal,
    pub ask_size: Decimal,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DepthUpdate {
    pub symbol: String,
    pub venue: Venue,
    pub bids: Vec<PriceLevel>,   // 按价格从优到劣排序
    pub asks: Vec<PriceLevel>,   // 按价格从优到劣排序
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bar {
    pub symbol: String,
    pub venue: Venue,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub interval: BarInterval,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BarInterval {
    OneSecond,
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
    OneDay,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeEvent {
    pub symbol: String,
    pub venue: Venue,
    pub price: Decimal,
    pub quantity: Decimal,
    pub side: Option<Side>,      // 主动成交方向，非所有数据源都能提供
    pub trade_id: Option<String>,
    pub timestamp: DateTime<Utc>,
}

#[async_trait]
pub trait MarketDataProvider: Send + Sync {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError>;
}
```

### 4.4 `ApexesError`

**关键决策**：为可能携带底层错误的变体预留 `source_detail: Option<String>` 字段（而非 `#[source] Box<dyn Error>`），使 `ApexesError` 能够 `derive(Clone)`——因为 `Box<dyn Error>` 不支持 `Clone`。代价是放弃标准 `std::error::Error::source()` 错误链能力，改用格式化字符串保留上下文；若后续子项目确实需要结构化错误链，需再单独决策。

**字段命名说明**：字段名为 `source_detail` 而非 `source`——`thiserror` 的 derive 宏会将任何名为 `source` 的字段特殊处理为错误链的 `std::error::Error::source()`，强制要求其类型实现 `std::error::Error`，这与 `Option<String>`（为保持 `Clone` 而选择的类型）冲突，编译期即报错（`E0599`，已用 `thiserror 2.0.19` 独立验证复现）。重命名为 `source_detail` 可在保留 `#[derive(thiserror::Error)]` 的前提下规避此冲突。

```rust
#[derive(Debug, Clone, Error)]
pub enum ApexesError {
    #[error("invalid instrument: {reason}")]
    InvalidInstrument { reason: String },

    #[error("invalid order state transition: {from:?} -> {to:?}")]
    InvalidStateTransition { from: OrderStatus, to: OrderStatus },

    #[error("market data connection failed: {reason}")]
    ConnectionFailed {
        reason: String,
        source_detail: Option<String>,   // 格式化后的底层错误信息，保持 ApexesError 可 Clone
    },

    #[error("subscription failed for {symbol} on {venue:?}: {reason}")]
    SubscriptionFailed {
        symbol: String,
        venue: Venue,
        reason: String,
        source_detail: Option<String>,
    },
}
```

---

## 五、`apexes-cli`

本阶段 `apexes-cli` 仅证明二进制 crate 能正确依赖并链接 `apexes-core`，不提供任何真实子命令：

```rust
use clap::Parser;

#[derive(Parser)]
#[command(name = "apexes", version, about = "Apexes multi-market trading system")]
struct Cli;

fn main() {
    Cli::parse();
}
```

`apexes --version` / `apexes --help` 可用即视为本阶段完成，后续子项目落地时再逐步添加真实子命令。

---

## 六、测试计划

由于本阶段唯一的真实逻辑是 `OrderStatus` 状态机，测试范围聚焦于：

1. **`OrderStatus` 状态转移测试**（`crates/apexes-core/src/order.rs` 内 `#[cfg(test)] mod tests`）：
   - 覆盖所有合法转移路径（`New→PartiallyFilled`、`New→Filled`、`New→Cancelled`、`New→Rejected`、`PartiallyFilled→PartiallyFilled`、`PartiallyFilled→Filled`、`PartiallyFilled→Cancelled`）
   - 覆盖非法转移（如 `Filled→PartiallyFilled`、`Rejected→New`、`PartiallyFilled→Rejected`），断言返回 `Err(ApexesError::InvalidStateTransition)`
2. **构造/序列化往返测试**：
   - `Instrument::new(...)` 的合法构造 + venue/market_type 不匹配时返回 `Err`
   - `Order`/`Fill`/`MarketEvent`（每个 variant 至少一例）的 `serde_json` 序列化 → 反序列化往返，断言相等

不做穷尽性属性测试（proptest）——按讨论结论，这一阶段的测试范围止于"覆盖唯一的真实逻辑（状态机）+ 数据形状健全性检查"，不追求最大化严谨度。

---

## 七、验收标准（Definition of Done）

- `cargo build --workspace` 成功
- `cargo test --workspace` 全部通过
- `cargo clippy --workspace -- -D warnings` 无警告
- `cargo fmt --check` 无格式问题
- `apexes-cli` 可执行 `--version`/`--help`
- 本文档中列出的所有类型/trait 均已实现且带有基本 doc comment
