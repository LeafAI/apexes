# Apexes — 子项目 #7：OMS（订单管理系统，`apexes-oms`）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#7`（OMS）的详细设计。对应 `docs/FEATURES.md` §5 "Live Trading System"：行 `5.1.1`（订单生成与事前校验）、`5.1.3`（持久化与断线重连状态恢复/对账）。行 `5.1.2`（订单状态机）已在子项目 `#0` 完成（`apexes_core::OrderStatus`），本文档只复用，不重新设计。
>
> **本模块是 vision 文档"关键推论"一节明确标注的三大不可降质模块之一**："OMS 订单生命周期状态机与断线重连对账"——正确性是"系统能否被信任无人值守运行真实资金"的决定性因素，任何简化都必须显式记录并评估风险，不能为赶进度默默略过。
>
> 前置依赖：`apexes-core`（`Order`/`OrderId`/`OrderStatus`/`Fill`/`Side`/`Venue` 已完成）、`apexes-risk`（`PreTradeCheck`/`CompositeRiskChecker` 已完成）。

---

## 一、范围

### 关键架构决策：网关无关设计

`#7` 是本项目第一个触及"真实订单管理"的子项目——此前的 `#3`（回测）、`#6`（模拟盘）都是纯模拟/内存态，没有真实交易所连接的概念；而真正的实盘网关是 `#8`（依赖 `#7`）才开始做。

**决策**：`apexes-oms` 设计为**网关无关**——定义一个 `ExecutionGateway` trait（本 crate 私有，不下沉到 `apexes-core`，因为目前只有 OMS 需要消费它），本轮只提供一个用于测试的 mock 实现；OMS 自身实现订单生命周期状态机的运行时管理、持久化、断线重连对账逻辑，这些逻辑与"如何真正跟交易所通信"完全解耦。`#8` 后续只需要实现 `ExecutionGateway` trait 对接真实交易所 API，不需要改动 OMS 内部逻辑。

**OMS 不驱动交易循环**：与 `apexes-backtest`/`apexes-paper` 的 `event_loop` 共享核心不同，`apexes-oms` 的职责是订单管理本身（`OrderIntent → 风控检查 → 创建 Order → 提交网关 → 状态跟踪/持久化`），不负责消费行情事件、调用 `Strategy::on_market_event`。`Strategy`/`MatchingEngine` 与 OMS 的集成（把策略产生的意图接入 OMS）留给 `#8`。

### 数据库选型：引入 PostgreSQL（新增技术栈组件）

路线图 §八（数据存储与消息中间件）明确规划了"ClickHouse（时序）+ PostgreSQL（关系型，配置/账户/订单存储）"两套存储后端并存,`docs/FEATURES.md` 行 `8.2` 也标注"引入时机：OMS 需要持久化订单存储时"——现在正是这个时机。

**为什么不用 ClickHouse（沿用项目已有唯一存储后端）**：ClickHouse 是列存 OLAP 数据库，为海量追加写入 + 大范围分析查询优化，但存在三个与 OMS 场景直接冲突的局限：
1. **没有真正的 UPDATE**——项目里 `apexes-strategy-registry` 用 `ReplacingMergeTree` + 查询时 `FINAL` 去重模拟"更新"，但 `FINAL` 在数据量大时查询变慢，且官方文档本身不建议用它做高频点查。
2. **没有唯一约束/事务保证**——无法在数据库层面阻止同一订单状态被并发写入两次（如网络重试导致的重复提交）。
3. **点查（按 `OrderId` 查询单条最新状态）不是其强项**——`MergeTree` 的 `ORDER BY` 索引面向范围扫描，不是为"给我这一个 OrderId 现在的状态"这种低延迟点查设计的。

而 OMS 恰恰需要"强一致、低延迟点查、幂等写入"——这些正是 PostgreSQL 的强项，也是 ClickHouse 的已知弱项。继续用 ClickHouse 会让"不可降质"的模块建立在错误的技术选型之上。

**决策**：`apexes-oms` 使用 **PostgreSQL**（新引入，通过 `sqlx` 异步驱动，`postgres` feature）持久化 `Order` 与 `Fill`。ClickHouse 继续专职负责行情历史数据（`apexes-storage`，不变）；`apexes-strategy-registry`（`#4.5`）保持其现有的 ClickHouse 实现不变，本轮不迁移——它的场景（策略版本指针）不涉及真实资金、不在断线重连对账的关键路径上，没有已知问题驱动这次改动，迁移它会稀释本轮 OMS 的设计焦点，留给未来如有需要再单独立项评估。

`docker-compose.yml` 新增一个 `postgres` 服务，与现有 `clickhouse` 服务并存。

### 包含

1. **新增 workspace 成员 crate：`apexes-oms`**（依赖 `apexes-core` + `apexes-risk` + `sqlx` + `tokio` + `chrono` + `rust_decimal` + `tracing` + `thiserror`），提供：
   - **`ExecutionGateway` trait**：`submit_order`/`cancel_order`/`query_order_status`/`query_all_open_orders` 四个方法，返回值区分"成功"/"明确拒绝"/"结果不确定"三种结果（见 §五）。
   - **`OrderManager`**：内部持有 `HashMap<OrderId, Order>`（当前订单快照）+ PostgreSQL 持久化，提供 `submit_order`（风控检查 → 创建订单 → 持久化 → 提交网关 → 更新状态）、`cancel_order`（状态检查 → 提交撤单 → 更新状态）、`reconcile`（断线重连对账：拉取交易所侧全量未结束订单，以其为权威真相覆盖本地状态）。
   - **PostgreSQL schema**：`orders` 表、`fills` 表（结构见 §四），沿用项目现有的"表结构 DDL 常量 + Row 结构体 + `TryFrom` 双向转换"模式（参考 `apexes-storage::schema`）。
   - **`MockExecutionGateway`**：供本轮单元测试使用的内存态网关实现，支持人为注入"成功"/"明确拒绝"/"结果不确定"/断线期间状态变化等场景，验证 `OrderManager` 的对账逻辑。
2. **`docker-compose.yml` 增量**：新增 `postgres` 服务定义。

### 不包含（明确超出范围，留给对应后续子项目/后续轮次）

- **不做真实交易所网关实现**——`ExecutionGateway` 的真实实现（对接 Binance 等交易所私有 API）是 `#8`（首个实盘网关）的范围，本轮只有 mock 实现。
- **不做 `Strategy`/`MatchingEngine` 与 OMS 的集成**——OMS 如何接入策略产生的 `OrderIntent` 流（类似 `run_backtest`/`run_paper_trading` 的事件循环）留给 `#8`；本轮 `OrderManager::submit_order` 是一个独立可调用的方法，调用方（未来的 `#8`）自行决定何时调用它。
- **不做智能路由/执行算法（TWAP/VWAP等）**——`docs/FEATURES.md` 行 `5.2.1`/`5.2.2`/`5.2.3`，归属 `#9`（EMS/执行算法）。
- **不做多账户/多市场统一管理**——归属 `#11`（账户与资金管理）。
- **不做自动重试机制**——"结果不确定"的订单提交/撤单，本轮只是保留原状态等待对账确认，不自动重新提交（自动重试在真实资金场景下有产生重复订单的风险，需要更谨慎的幂等键设计，留给未来评估）。
- **不迁移 `apexes-strategy-registry` 到 PostgreSQL**——保持其现有 ClickHouse 实现不变（见上文"数据库选型"决策说明）。
- **不做 PostgreSQL 之外的关系型数据库支持**——不做可插拔的数据库后端抽象，直接绑定 PostgreSQL（与 `apexes-storage` 直接绑定 ClickHouse 的既有风格一致）。

---

## 二、Crate 结构

```
crates/apexes-oms/
├── Cargo.toml
├── src/
│   ├── lib.rs             // pub mod 声明 + re-exports
│   ├── error.rs           // OmsError
│   ├── gateway.rs         // ExecutionGateway trait + SubmitOutcome/CancelOutcome
│   ├── mock_gateway.rs    // MockExecutionGateway（测试用）
│   ├── schema.rs          // PostgreSQL 表 DDL + OrderRow/FillRow + TryFrom 转换
│   ├── store.rs           // OrderStore（纯 PostgreSQL 读写，无业务逻辑）
│   └── manager.rs         // OrderManager（业务逻辑：submit_order/cancel_order/reconcile）
└── tests/
    └── live_postgres.rs   // #[ignore] 真实 PostgreSQL 集成测试
```

`Cargo.toml` 依赖：`apexes-core`、`apexes-risk`、`sqlx`（`runtime-tokio`、`postgres`、`chrono`、`rust_decimal` features）、`tokio`、`chrono`、`rust_decimal`、`tracing`、`thiserror`、`async-trait`（`ExecutionGateway` 是 async trait）。

---

## 三、`ExecutionGateway` trait

```rust
//! [`ExecutionGateway`]: the abstraction `OrderManager` submits/cancels/
//! queries orders through, decoupling order-lifecycle management from
//! any specific exchange's API. Sub-project #8 (first live gateway)
//! implements this trait against a real exchange; this crate ships only
//! [`crate::mock_gateway::MockExecutionGateway`] for testing.

use apexes_core::{Order, OrderId, OrderStatus};
use async_trait::async_trait;

/// The outcome of a `submit_order`/`cancel_order` call, distinguishing
/// three cases that `OrderManager` must handle differently:
/// - `Accepted`: the exchange confirmed the action.
/// - `Rejected`: the exchange explicitly refused (e.g. insufficient
///   balance, invalid parameters) -- authoritative, no ambiguity.
/// - `Uncertain`: the call failed in a way that does NOT confirm
///   whether the exchange actually processed it (e.g. network timeout,
///   connection dropped mid-request) -- the order may have gone through
///   despite the local failure. `OrderManager` must NOT assume failure
///   in this case; it leaves the order's status unchanged and defers to
///   a later `reconcile()` call to discover the true state.
#[derive(Debug, Clone, PartialEq)]
pub enum GatewayOutcome {
    Accepted { exchange_order_id: String },
    Rejected { reason: String },
    Uncertain { reason: String },
}

#[async_trait]
pub trait ExecutionGateway: Send + Sync {
    /// Submits `order` (already persisted locally with status `New` by
    /// the caller before this is invoked -- see `OrderManager::submit_order`)
    /// to the exchange.
    async fn submit_order(&self, order: &Order) -> GatewayOutcome;

    /// Cancels an already-submitted order, identified by its
    /// `exchange_order_id` (the caller -- `OrderManager` -- guarantees
    /// this is `Some` before calling, since only orders that have
    /// completed submission can be cancelled).
    async fn cancel_order(&self, exchange_order_id: &str) -> GatewayOutcome;

    /// Queries the exchange's current authoritative status for a single
    /// order. Returns `None` if the exchange has no record of this
    /// order ID at all (distinct from "order exists but query failed" --
    /// query failures are reported via the `Result`'s `Err`).
    async fn query_order_status(
        &self,
        exchange_order_id: &str,
    ) -> Result<Option<OrderStatus>, String>;

    /// Queries the exchange for every currently open (not Filled/
    /// Cancelled/Rejected) order across the whole account -- used by
    /// `OrderManager::reconcile` after a reconnect, matching real
    /// exchange APIs' typical "list open orders" endpoint shape (more
    /// efficient than querying one order ID at a time when many orders
    /// may have changed state during a disconnect).
    ///
    /// Returns `(client_order_id, exchange_order_id, status)` triples.
    /// `client_order_id` is OUR locally-assigned `OrderId` (`Order.id`),
    /// not the exchange's own identifier -- real exchange APIs
    /// universally support attaching and later querying by a
    /// caller-supplied "client order ID" alongside their own order ID
    /// specifically to support this exact correlation problem: without
    /// it, an order whose `submit_order` call returned `Uncertain` (so
    /// `Order.exchange_order_id` is still `None`) could never be
    /// matched back to its exchange-side record by
    /// `exchange_order_id`, since we never learned one. `submit_order`
    /// is therefore understood to pass `order.id` to the exchange as
    /// this client order ID at submission time (an implementation
    /// requirement for any real `ExecutionGateway`, noted here since
    /// `MockExecutionGateway`'s test double must honor it too --
    /// see §七).
    async fn query_all_open_orders(
        &self,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String>;
}
```

**为什么 `GatewayOutcome` 是三态枚举而不是 `Result<String, String>`**：如果只用 `Result`，`Err` 天然会被调用方当作"失败=应该做失败处理"，但网络超时这种"结果不确定"的情况恰恰不能这样处理——`OrderManager` 需要能区分"交易所明确说不行"（可以安心标记 `Rejected`）与"我不知道交易所是否处理了"（必须保守地保持原状态，等待对账），三态枚举把这个区分显式编码进类型系统，调用方不可能遗漏处理某一种情况（`match` 强制穷举）。

---

## 四、PostgreSQL Schema

```rust
//! PostgreSQL table schemas for `apexes-oms`. Uses `sqlx`'s
//! compile-time-unchecked query API (`sqlx::query`/`query_as`, not the
//! `query!` macro family) to avoid requiring a live database connection
//! at `cargo build` time -- matching the project's existing preference
//! for keeping builds independent of external service availability
//! (see `apexes-storage`'s use of the plain `clickhouse` client for the
//! same reason).

pub const ORDERS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS orders
(
    id                  UUID PRIMARY KEY,
    exchange_order_id   TEXT,
    symbol              TEXT NOT NULL,
    venue               TEXT NOT NULL,
    side                TEXT NOT NULL,
    order_type          TEXT NOT NULL,
    quantity            NUMERIC(38, 10) NOT NULL,
    price               NUMERIC(38, 10),
    stop_price          NUMERIC(38, 10),
    filled_quantity     NUMERIC(38, 10) NOT NULL,
    status              TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL
)
";

pub const ORDERS_EXCHANGE_ID_INDEX_DDL: &str = "
CREATE INDEX IF NOT EXISTS idx_orders_exchange_order_id
    ON orders (exchange_order_id)
    WHERE exchange_order_id IS NOT NULL
";

pub const FILLS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS fills
(
    id              UUID PRIMARY KEY,
    order_id        UUID NOT NULL REFERENCES orders(id),
    symbol          TEXT NOT NULL,
    venue           TEXT NOT NULL,
    side            TEXT NOT NULL,
    price           NUMERIC(38, 10) NOT NULL,
    quantity        NUMERIC(38, 10) NOT NULL,
    fee             NUMERIC(38, 10) NOT NULL,
    fee_currency    TEXT,
    filled_at       TIMESTAMPTZ NOT NULL
)
";

pub const FILLS_ORDER_ID_INDEX_DDL: &str = "
CREATE INDEX IF NOT EXISTS idx_fills_order_id ON fills (order_id)
";
```

`Order.id`/`Fill.id`/`Fill.order_id` 用 PostgreSQL 原生 `UUID` 类型存储 `apexes_core::{OrderId, FillId}`（两者都是 `Uuid` 的 newtype 包装，`sqlx` 对 `uuid::Uuid` 有原生支持）。`side`/`order_type`/`status`/`venue` 用 `TEXT` 存储 `Debug` 格式字符串（`format!("{side:?}")`），与 `apexes-storage::schema` 对 `Venue`/`Side` 的既有序列化约定一致，读取时反向解析（解析失败返回 `OmsError`）。`quantity`/`price`/`stop_price`/`fee` 用 `NUMERIC(38, 10)`（PostgreSQL 原生精确小数类型，与 `rust_decimal::Decimal` 直接对应，`sqlx` 的 `rust_decimal` feature 提供原生转换，不需要像 ClickHouse 那样手写 `CkDecimal` 转换层）。

**`OrderRow`/`FillRow` 结构体**（对应 `apexes-storage::schema` 的 Row 模式）：

```rust
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrderRow {
    pub id: uuid::Uuid,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub venue: String,
    pub side: String,
    pub order_type: String,
    pub quantity: rust_decimal::Decimal,
    pub price: Option<rust_decimal::Decimal>,
    pub stop_price: Option<rust_decimal::Decimal>,
    pub filled_quantity: rust_decimal::Decimal,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FillRow {
    pub id: uuid::Uuid,
    pub order_id: uuid::Uuid,
    pub symbol: String,
    pub venue: String,
    pub side: String,
    pub price: rust_decimal::Decimal,
    pub quantity: rust_decimal::Decimal,
    pub fee: rust_decimal::Decimal,
    pub fee_currency: Option<String>,
    pub filled_at: chrono::DateTime<chrono::Utc>,
}
```

`impl TryFrom<&Order> for OrderRow` / `impl TryFrom<OrderRow> for Order`（及 `Fill`/`FillRow` 对应版本）沿用 `apexes-storage::schema` 的双向转换模式，转换失败（如 `status` 字符串无法解析回 `OrderStatus`）返回 `OmsError::InvalidRow { reason: String }`。

---

## 五、`OrderStore`（纯持久化，无业务逻辑）

```rust
//! [`OrderStore`]: pure PostgreSQL read/write for `Order`/`Fill` --
//! no business logic (no risk checks, no gateway calls, no state-machine
//! validation). [`crate::manager::OrderManager`] is the business-logic
//! layer built on top of this.

use apexes_core::{Fill, Order, OrderId};
use sqlx::PgPool;

use crate::error::OmsError;

pub struct OrderStore {
    pool: PgPool,
}

impl OrderStore {
    /// Connects to PostgreSQL at `url` and ensures both tables (plus
    /// their indexes) exist.
    pub async fn new(url: &str) -> Result<Self, OmsError> {
        let pool = PgPool::connect(url).await.map_err(|error| OmsError::ConnectionFailed {
            reason: error.to_string(),
        })?;

        sqlx::query(crate::schema::ORDERS_TABLE_DDL).execute(&pool).await?;
        sqlx::query(crate::schema::ORDERS_EXCHANGE_ID_INDEX_DDL).execute(&pool).await?;
        sqlx::query(crate::schema::FILLS_TABLE_DDL).execute(&pool).await?;
        sqlx::query(crate::schema::FILLS_ORDER_ID_INDEX_DDL).execute(&pool).await?;

        Ok(Self { pool })
    }

    /// Inserts a new order, or updates it if `order.id` already exists
    /// (`ON CONFLICT (id) DO UPDATE` -- an idempotent upsert, since
    /// `OrderManager` writes the same order multiple times across its
    /// lifecycle: once at creation, again after gateway submission
    /// updates `exchange_order_id`, again on every status change).
    pub async fn upsert_order(&self, order: &Order) -> Result<(), OmsError>;

    pub async fn get_order(&self, id: OrderId) -> Result<Option<Order>, OmsError>;

    /// Returns every order currently NOT in a terminal state (`New` or
    /// `PartiallyFilled`) -- used by `OrderManager::reconcile` to know
    /// which local orders need to be checked against the exchange.
    pub async fn get_open_orders(&self) -> Result<Vec<Order>, OmsError>;

    pub async fn insert_fill(&self, fill: &Fill) -> Result<(), OmsError>;

    pub async fn get_fills_for_order(&self, order_id: OrderId) -> Result<Vec<Fill>, OmsError>;
}
```

---

## 六、`OrderManager`（业务逻辑层）

```rust
//! [`OrderManager`]: the business-logic layer -- risk checks, gateway
//! submission, status tracking, and reconnect reconciliation. Holds an
//! in-memory `HashMap<OrderId, Order>` snapshot of open orders (mirroring
//! `apexes_backtest::PortfolioLedger`'s in-memory-plus-persistence
//! pattern), rebuilt from `OrderStore::get_open_orders` on construction
//! so a process restart recovers its view of in-flight orders without
//! waiting for the next `reconcile()` call.

use std::collections::HashMap;
use std::sync::Arc;

use apexes_core::{Order, OrderId, OrderStatus};
use apexes_risk::PreTradeCheck;
use apexes_strategy::OrderIntent;

use crate::error::OmsError;
use crate::gateway::{ExecutionGateway, GatewayOutcome};
use crate::store::OrderStore;

pub struct OrderManager {
    store: OrderStore,
    gateway: Arc<dyn ExecutionGateway>,
    risk_checker: Option<Box<dyn PreTradeCheck>>,
    open_orders: HashMap<OrderId, Order>,
}

/// `PreTradeCheck::check`'s full signature requires `positions`,
/// `balances`, `open_orders`, and `now` (see `apexes-risk`'s
/// `pre_trade.rs`). `OrderManager` supplies `open_orders` from its own
/// state and `now` from `chrono::Utc::now()`, but has NO position/
/// balance tracking of its own -- unlike `apexes_backtest::PortfolioLedger`,
/// which `apexes-backtest`/`apexes-paper` already own. Account
/// position/balance management is explicitly sub-project `#11`'s scope
/// (not yet built), so `OrderManager::submit_order` accepts them as
/// caller-supplied parameters (`positions: &HashMap<String, Position>`,
/// `balances: &HashMap<String, Decimal>`) rather than owning them --
/// the caller (in this sub-project's scope, whatever test harness or
/// future `#8` integration calls `submit_order`) is responsible for
/// supplying an accurate snapshot. This is a deliberate, explicit scope
/// boundary, not an oversight: it keeps `OrderManager` focused on order
/// lifecycle/persistence/reconciliation only, deferring "where do
/// current positions and balances come from in live trading" to `#11`.

impl OrderManager {
    /// Constructs a new manager, rebuilding `open_orders` from
    /// `store.get_open_orders()` -- this is the "recover in-flight
    /// order state after a process restart" half of reconnect
    /// reconciliation; `reconcile()` (below) is the other half
    /// (comparing that recovered state against the exchange's current
    /// truth).
    pub async fn new(
        store: OrderStore,
        gateway: Arc<dyn ExecutionGateway>,
        risk_checker: Option<Box<dyn PreTradeCheck>>,
    ) -> Result<Self, OmsError>;

    /// Submits a new order:
    /// 1. Runs `risk_checker` (if configured) against `intent`; returns
    ///    `Err(OmsError::RiskCheckFailed)` without creating any order if
    ///    it fails.
    /// 2. Constructs an `Order` (status `New`, `exchange_order_id: None`),
    ///    persists it via `store.upsert_order` BEFORE calling the
    ///    gateway (see design spec §一's ordering rationale: a local
    ///    record that the gateway never accepted is recoverable via
    ///    reconciliation or manual review; a gateway-accepted order with
    ///    no local record is not).
    /// 3. Calls `gateway.submit_order`:
    ///    - `Accepted { exchange_order_id }`: updates the order's
    ///      `exchange_order_id` field, re-persists, inserts into
    ///      `open_orders`, returns `Ok(order_id)`.
    ///    - `Rejected { reason }`: transitions the order to `Rejected`,
    ///      re-persists, does NOT insert into `open_orders`, returns
    ///      `Err(OmsError::OrderRejected { reason })`.
    ///    - `Uncertain { reason }`: leaves the order's status as `New`
    ///      (already persisted), inserts into `open_orders` anyway
    ///      (it's genuinely uncertain whether it's live), logs a
    ///      `tracing::warn!`, and returns
    ///      `Ok(order_id)` -- the caller gets an order ID back either
    ///      way; only `reconcile()` will later resolve the true status.
    ///
    /// `positions`/`balances` are caller-supplied snapshots passed
    /// straight through to `risk_checker.check(..)` -- see the note on
    /// `OrderManager`'s fields above for why this crate does not own
    /// position/balance tracking itself.
    pub async fn submit_order(
        &mut self,
        intent: OrderIntent,
        positions: &HashMap<String, apexes_strategy::Position>,
        balances: &HashMap<String, rust_decimal::Decimal>,
    ) -> Result<OrderId, OmsError>;

    /// Cancels an existing order:
    /// 1. Looks up `order_id` in `open_orders`; returns
    ///    `Err(OmsError::OrderNotFound)` if absent, or
    ///    `Err(OmsError::OrderNotCancellable { current_status })` if its
    ///    status isn't `New`/`PartiallyFilled`.
    /// 2. Checks `order.exchange_order_id`: if `None` (this order's
    ///    original `submit_order` call returned `Uncertain` and has not
    ///    yet been resolved by a `reconcile()` call), returns
    ///    `Err(OmsError::ExchangeOrderIdUnknown { order_id })` rather
    ///    than calling the gateway -- there is no exchange-side
    ///    identifier to cancel by yet, and guessing one is not safe.
    ///    The caller must wait for `reconcile()` to resolve the order's
    ///    true state (which populates `exchange_order_id` once the
    ///    exchange's own records confirm it) before cancellation is
    ///    possible.
    /// 3. Otherwise calls `gateway.cancel_order` using the order's
    ///    `exchange_order_id`. Same three-way `GatewayOutcome` handling
    ///    as `submit_order`:
    ///    `Accepted` -> mark `Cancelled`, remove from `open_orders`;
    ///    `Rejected` -> return `Err` with the reason, order status
    ///    unchanged; `Uncertain` -> log and leave status unchanged,
    ///    defer to `reconcile()`.
    pub async fn cancel_order(&mut self, order_id: OrderId) -> Result<(), OmsError>;

    /// Reconnect reconciliation: calls
    /// `gateway.query_all_open_orders()`, and for every
    /// `(client_order_id, exchange_order_id, exchange_status)` triple
    /// returned, looks up the matching local order BY `client_order_id`
    /// (our own `OrderId` -- see `ExecutionGateway::query_all_open_orders`'s
    /// docs for why matching happens on this field rather than
    /// `exchange_order_id`, which may still be unknown locally for
    /// `Uncertain`-outcome orders). If found: sets
    /// `order.exchange_order_id` (populating it if it was `None`) and
    /// OVERWRITES the local status with the exchange's reported status
    /// UNCONDITIONALLY -- bypassing `OrderStatus::can_transition_to`'s
    /// validation entirely, per this design's explicit decision that
    /// the exchange is the authoritative source of truth during
    /// reconciliation (a transition the local state machine would
    /// normally reject, e.g. `New -> Filled` skipping `PartiallyFilled`,
    /// is not a bug here -- it's simply what actually happened while
    /// disconnected). If NOT found (a `client_order_id` the exchange
    /// reports that we have no local record of at all -- e.g. our
    /// database was reset, or an order placed by a different process
    /// sharing the account), logs a `tracing::error!` and skips it (out
    /// of scope to fabricate a local record for an order we never
    /// created). Local orders NOT present in the exchange's open-order
    /// list are left unchanged if already terminal, or logged as a
    /// discrepancy (`tracing::warn!`) if the local status is still
    /// `New`/`PartiallyFilled` (meaning the exchange no longer
    /// considers it open -- it must have reached a terminal state the
    /// exchange isn't reporting via this endpoint; a full investigation
    /// mechanism for this edge case is out of scope, see §十一).
    /// Every status change made during reconciliation is persisted via
    /// `store.upsert_order` and reflected in `open_orders`. Returns the
    /// list of `OrderId`s whose status actually changed, so a caller can
    /// log/alert on what reconciliation found.
    pub async fn reconcile(&mut self) -> Result<Vec<OrderId>, OmsError>;

    pub fn open_orders(&self) -> &HashMap<OrderId, Order>;
}
```

**为什么 `reconcile()` 用"覆盖本地状态"而非"校验后拒绝不合法转换"**：`OrderStatus::can_transition_to`（`apexes-core`）是为**本地状态机在正常运行时**的自洽性设计的（比如程序自己不应该把一个 `Filled` 订单改回 `New`）。但断线重连场景下，"不合法转换"很可能恰恰反映了断线期间真实发生、只是本地没来得及观察到的事件序列（比如断线前是 `New`，断线期间迅速经历部分成交又完全成交，重连后交易所直接报告 `Filled`——这不是状态机被违反，是本地视角"跳过"了中间状态）。强行用 `can_transition_to` 校验交易所回报，只会导致 OMS 拒绝更新真实状态，制造"本地记录与交易所事实不符"这种正是断线重连对账要解决的问题本身。

---

## 七、`MockExecutionGateway`

```rust
//! [`MockExecutionGateway`]: an in-memory `ExecutionGateway` for tests.
//! Lets a test pre-program specific `GatewayOutcome`s per call (keyed by
//! order ID or call sequence) and simulate exchange-side state changes
//! that occur "during a disconnect" for `OrderManager::reconcile` tests.

use std::collections::HashMap;
use std::sync::Mutex;

use apexes_core::{Order, OrderId, OrderStatus};
use async_trait::async_trait;

use crate::gateway::{ExecutionGateway, GatewayOutcome};

pub struct MockExecutionGateway {
    /// Programmed outcome for the next `submit_order` call, consumed
    /// (removed) on use. `None` means "accept with a freshly generated
    /// exchange_order_id" (the convenient default for tests that don't
    /// care about submission failure paths).
    next_submit_outcome: Mutex<Option<GatewayOutcome>>,
    next_cancel_outcome: Mutex<Option<GatewayOutcome>>,
    /// Exchange-side authoritative state, keyed by OUR client order ID
    /// (`OrderId`) -- tests mutate this directly (simulating "the
    /// exchange processed something while we were disconnected")
    /// before calling `OrderManager::reconcile`. Each entry also stores
    /// the exchange-assigned ID string this mock generated at
    /// `submit_order` time, so `query_all_open_orders` can return both
    /// per its corrected signature.
    exchange_state: Mutex<HashMap<OrderId, (String, OrderStatus)>>,
}

impl MockExecutionGateway {
    pub fn new() -> Self;
    pub fn program_next_submit_outcome(&self, outcome: GatewayOutcome);
    pub fn program_next_cancel_outcome(&self, outcome: GatewayOutcome);
    /// Directly sets the exchange-side status for the order previously
    /// submitted as `client_order_id`, simulating a state change a test
    /// wants `reconcile()` to discover. Panics if `client_order_id` was
    /// never submitted through this mock (a test setup error, not a
    /// runtime condition to handle gracefully).
    pub fn set_exchange_state(&self, client_order_id: OrderId, status: OrderStatus);
}

#[async_trait]
impl ExecutionGateway for MockExecutionGateway {
    async fn submit_order(&self, order: &Order) -> GatewayOutcome { /* ... */ }
    async fn cancel_order(&self, exchange_order_id: &str) -> GatewayOutcome { /* ... */ }
    async fn query_order_status(&self, exchange_order_id: &str) -> Result<Option<OrderStatus>, String> { /* ... */ }
    async fn query_all_open_orders(&self) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> { /* ... */ }
}
```

(具体方法体实现细节留给实现计划阶段——本设计只锁定接口形状和可编程行为，即"测试能控制网关在每次调用时返回什么、能直接篡改交易所侧状态来模拟断线期间的变化"这两个核心能力。)

---

## 八、错误类型

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OmsError {
    #[error("failed to connect to PostgreSQL: {reason}")]
    ConnectionFailed { reason: String },

    #[error("database query failed: {reason}")]
    QueryFailed { reason: String },

    #[error("failed to convert database row: {reason}")]
    InvalidRow { reason: String },

    #[error("order rejected by risk checker: {violations:?}")]
    RiskCheckFailed { violations: Vec<apexes_risk::RiskViolation> },

    #[error("order rejected by exchange: {reason}")]
    OrderRejected { reason: String },

    #[error("order {order_id:?} not found")]
    OrderNotFound { order_id: apexes_core::OrderId },

    #[error("order {order_id:?} cannot be cancelled in status {current_status:?}")]
    OrderNotCancellable {
        order_id: apexes_core::OrderId,
        current_status: apexes_core::OrderStatus,
    },

    #[error(
        "order {order_id:?} has no known exchange_order_id yet (submission outcome was Uncertain) -- wait for reconcile() to resolve it before cancelling"
    )]
    ExchangeOrderIdUnknown { order_id: apexes_core::OrderId },
}

impl From<sqlx::Error> for OmsError {
    fn from(error: sqlx::Error) -> Self {
        OmsError::QueryFailed { reason: error.to_string() }
    }
}
```

---

## 九、`docker-compose.yml` 增量

```yaml
services:
  clickhouse:
    # ...unchanged...

  postgres:
    image: postgres:17
    container_name: apexes-postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: apexes
      POSTGRES_USER: apexes
      POSTGRES_PASSWORD: apexes
    volumes:
      - apexes-postgres-data:/var/lib/postgresql/data

volumes:
  apexes-clickhouse-data:
  apexes-postgres-data:
```

---

## 十、测试策略

1. **`schema.rs`/`OrderRow`/`FillRow` 单元测试**：`TryFrom` 双向转换往返验证（`Order`/`Fill` → Row → 转回，字段完全一致），非法 `status`/`side` 字符串转换失败返回 `OmsError::InvalidRow`。
2. **`MockExecutionGateway` 自身单元测试**：验证可编程的 outcome 被正确消费（用一次即清空，未编程时默认 `Accepted`）、`set_exchange_state`/`query_all_open_orders` 正确反映测试设置的状态。
3. **`OrderManager::submit_order` 单元测试**（用 `MockExecutionGateway`，`OrderStore` 指向一个真实但每测试独立的 PostgreSQL schema/database，或用 `#[ignore]` 处理——见下方测试隔离说明）：风控检查失败时不创建任何订单；`GatewayOutcome::Accepted` 时订单进入 `open_orders`且 `exchange_order_id` 被正确设置；`Rejected` 时订单标记 `Rejected`且不进入 `open_orders`；`Uncertain` 时订单保持 `New`且**仍然**进入 `open_orders`（后续等待 reconcile）。
4. **`OrderManager::cancel_order` 单元测试**：撤销不存在的订单返回 `OrderNotFound`；撤销已终结状态订单（如 `Filled`）返回 `OrderNotCancellable`；三种 `GatewayOutcome` 分支行为与 `submit_order`对应逻辑一致地验证。
5. **`OrderManager::reconcile` 单元测试**（本设计的核心验证点）：模拟"断线前提交一个订单（`Accepted`，本地 `New`）→ 测试直接用 `set_exchange_state(order_id, ...)`把交易所侧状态改为 `Filled`（模拟断线期间成交）→ 调用 `reconcile()`→ 验证本地订单状态被更新为 `Filled`，即使这个转换按 `can_transition_to`规则是不允许跳过 `PartiallyFilled`的"；验证 reconcile 返回的"状态变化订单列表"准确；验证交易所未报告的本地非终态订单被记录警告（不 panic，不静默丢弃）；**新增关键场景**：验证一个 `submit_order`返回 `Uncertain`（本地 `exchange_order_id`仍为 `None`）的订单，在 `reconcile()`按 `client_order_id`（而非 `exchange_order_id`）匹配到交易所记录后，`exchange_order_id`被正确回填、状态被正确更新——这是验证"客户端订单号"关联机制本身工作正常的测试，也是本设计对 §三 提出的"`Uncertain`订单如何最终被对账解决"问题的具体回答。
6. **端到端集成测试**（`crates/apexes-oms/tests/live_postgres.rs`，`#[ignore]`，需要真实 PostgreSQL，沿用 `apexes-storage`既有的 `#[ignore]`模式）：完整走一遍"构造 `OrderManager` → 提交订单 → 模拟进程重启（新建一个指向同一 PostgreSQL 的 `OrderManager` 实例）→ 验证 `open_orders`从数据库正确恢复 → 调用 `reconcile` → 验证最终状态"的全流程。
7. **全工作区验证**：`cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --check`。

**测试隔离说明**：`OrderManager`/`OrderStore`层面的单元测试（第3-5类）本设计倾向于要求真实 PostgreSQL 连接（因为 `OrderStore`没有内存态实现，`OrderManager`持有真实 `OrderStore`），因此这些测试也标记 `#[ignore]`，与端到端集成测试统一处理；真正不需要数据库的只有 `schema.rs`的 `TryFrom`单元测试和 `MockExecutionGateway`自身的单元测试。这与 `apexes-strategy-registry`（`VersionStore`无内存态实现，其 `CanaryComparator`测试因此也标记 `#[ignore]`）的既有先例一致——具体是否要为 `OrderStore`额外设计一个内存态测试替身，留给实现计划阶段根据开发效率需要决定。

---

## 十一、非目标（Non-goals，本次明确不做）

- 不做真实交易所网关实现（留给 `#8`）
- 不做 `Strategy`/`MatchingEngine`与 OMS 的集成（留给 `#8`）
- 不做智能路由/执行算法（留给 `#9`）
- 不做多账户/多市场管理（留给 `#11`）
- 不做订单提交/撤单的自动重试机制（"结果不确定"时只保留原状态等待对账，不自动重发）
- 不迁移 `apexes-strategy-registry`到 PostgreSQL（保持现有 ClickHouse 实现不变）
- 不做可插拔的关系型数据库后端抽象（直接绑定 PostgreSQL）
- 不解决"本地非终态订单、但交易所未在 `query_all_open_orders`结果中报告"这一边界情况的自动化处理——本轮只记录警告，深入调查机制留给未来评估
- 不做持仓/余额跟踪——`OrderManager::submit_order`所需的 `positions`/`balances`快照由调用方提供（类似 `apexes_backtest::PortfolioLedger`的能力留给 `#11`账户与资金管理去补齐，本轮 OMS 不重新实现）
