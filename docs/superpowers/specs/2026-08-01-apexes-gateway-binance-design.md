# Apexes — 子项目 #8：首个实盘网关（加密货币 CEX，`apexes-gateway-binance` + `apexes-live-trading`）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#8`（首个实盘网关）的详细设计。对应 `docs/FEATURES.md` 行 `5.3.1`（加密货币网关：CEX 私有 API）。目标：在 Binance 上真实下单，打通"回测 → 模拟盘 → 实盘"闭环。
>
> 前置依赖：`apexes-core`（`Order`/`Instrument`/`OrderStatus` 等）、`apexes-oms`（`ExecutionGateway` trait、`OrderManager`，`#7` 已完成）、`apexes-market-data`（`BinanceMarketDataProvider`，`#1` 已完成）、`apexes-backtest`（`PortfolioLedger`/`Strategy` trait，`#3` 已完成）、`apexes-risk`（`CompositeRiskChecker`，`#5` 已完成）。

---

## 一、范围

### 关键前置发现：`barter-execution` 不可用

路线图（§2.6、`#8` 行）原计划"底层可复用 `barter-execution`"承担 Binance 私有 API（下单/撤单/查询）的协议细节，类比 `#1` 复用 `barter-data` 承担公开行情协议的成功经验。**设计阶段实际检出 `barter-execution` 0.7.0 源码（并核对了其 GitHub `main` 分支，结论一致）后发现这一假设不成立**：

- `barter-execution::client` 模块下 `mod binance;` 声明为**私有模块**（非 `pub mod`），且该目录下的 `mod.rs` 文件是**空文件（0 字节）**——即便未来某个版本补齐了实现，只要声明仍是私有的，外部 crate 也无法调用。
- 唯一公开可用的 `ExecutionClient` 实现是 `client::mock`（供该库自身的回测/模拟盘场景使用），没有任何真实交易所的私有 API 客户端随库发布。

**决策**：`apexes-gateway-binance` 不依赖 `barter-execution`，改为直接基于 `reqwest`（REST）+ `tokio-tungstenite`（WebSocket User Data Stream）+ `hmac`/`sha2`（请求签名）手写 Binance 私有 API 客户端。这是本子项目相对路线图原始设想的**已知偏离**，记录于此供后续子项目（`#10` 其他市场网关）参考："`barter-rs` 生态目前只解决了公开行情接入，不解决私有下单 API"这一结论应作为评估其他交易所网关时的默认假设，而非个例。

### 范围边界：现货 + 永续合约，仅下单/撤单/查询机制本身

- **现货（Spot）**：与 `#1`（`BinanceMarketDataProvider`，仅 Binance 现货）行情范围保持一致。
- **永续合约（USDS-M Perpetual Futures）**：应用户要求纳入范围，但**仅支持下单/撤单/查询订单状态/查询成交明细这些机制本身**——不做杠杆倍数设置、保证金模式（逐仓/全仓）切换等账户配置管理。假定使用本网关下单的账户已经在交易所网页/App 上预先配置好期望的杠杆倍数与保证金模式；`apexes-risk` 的 `6.4`（保证金杠杆监控）已被用户明确推迟，依赖"尚不存在的杠杆持仓模型"，本轮不构建该模型。
- **不做**：DEX/链上交易（`#10` 范围）、其他 CEX（OKX/Coinbase 等，未来若需要则新增对称的 `apexes-gateway-<exchange>` crate）、智能路由/执行算法（`#9` EMS 范围）、多账户资金管理（`#11` 范围）、`Order`/`OrderIntent` 未涉及的其他资产类别（A股/H股/美股，`#10` 范围）。

### 包含

1. **新增 workspace 成员 crate：`apexes-gateway-binance`**——纯粹的"与 Binance 通信"实现，不含业务编排逻辑：
   - `BinanceExecutionGateway`：实现 `apexes_oms::ExecutionGateway`（含新增的 `query_fills_for_order` 方法，见 §四）的真实客户端，覆盖现货 + USDS-M 永续合约两族 REST 端点。
   - 请求签名（HMAC-SHA256）、`GatewayOutcome`/成交明细到 Apexes 类型的转换。
   - User Data Stream（WebSocket 实时推送订单/成交事件）客户端，含 `listenKey` 生命周期管理（获取 + 定期续期）。
2. **新增 workspace 成员 crate：`apexes-live-trading`**——交易所无关的实盘事件循环编排，不依赖任何交易所特定类型：
   - `run_live_trading`：类比 `apexes-paper::run_paper_trading`，但驱动源是 `OrderManager`/`ExecutionGateway`（真实交易所状态）而非本地 `MatchingEngine`（模拟撮合）。
   - 复用 `apexes_backtest::PortfolioLedger` 从真实 `Fill` 重建持仓/余额快照，作为 `OrderManager::submit_order` 所需 `positions`/`balances` 参数的来源（`#11` 账户与资金管理完成后可替换为更完整的实现——这是本轮的临时职责代理，不是最终方案）。
3. **`apexes-core` 破坏性变更**：`Order`/`OrderIntent` 新增 `asset_class: AssetClass` 字段（详见 §二），需要修复全部现有穷举式构造调用点。
4. **`apexes-oms` 扩展**：`ExecutionGateway` trait 新增 `query_fills_for_order` 方法；`fills` 表新增 `exchange_trade_id` 列 + 去重逻辑；`OrderManager` 新增公开方法 `apply_exchange_update`（从 `reconcile()` 中抽取的共享状态更新+成交拉取逻辑，供 User Data Stream 推送复用）。

### 不包含（明确超出范围）

- 杠杆/保证金模式设置管理（见上）。
- 除 Binance 外的其他交易所。
- DEX/链上交易。
- 智能路由/执行算法（`#9`）。
- 真正的多账户/资金管理（`#11`）——`run_live_trading` 内部的 `PortfolioLedger` 使用只是本轮的临时职责代理。
- 配置文件加载（如从 `~/.apexes/config.yaml` 读取 API Key）——`BinanceGatewayConfig` 只定义字段结构，由调用方构造并传入；加载机制留给专门的后续子项目（如 `#13` UI 或未来的 CLI 工具化工作）。
- 任何真实 Binance API Key 的集成测试自动化——所有需要真实签名请求的测试标记 `#[ignore]`，用户自行在 Binance Testnet（`testnet.binance.vision`）上手动运行验证。

---

## 二、`apexes-core` 变更：`AssetClass` 字段

### 为什么需要

`ExecutionGateway::submit_order(&self, order: &Order)` 只接收 `Order`，其字段目前是 `symbol`/`venue`/`side`/`order_type`/`quantity`/`price`/`stop_price`/`filled_quantity`/`status`/时间戳——**没有任何字段能告诉 `BinanceExecutionGateway` 这笔订单该路由到现货 API（`api.binance.com`）还是合约 API（`fapi.binance.com`)**。这两族端点的路径、参数形状、部分语义均不同（例如合约订单的 `positionSide`、逐仓/全仓保证金模式相关参数），网关必须在构造请求前就知道资产类别。

`apexes_core::instrument::AssetClass`（`Spot`/`Perpetual`/`Future`/`Option`/`Equity`）已经存在且承担了完全相同语义，直接复用即可。

### 变更内容

```rust
// crates/apexes-core/src/order.rs
pub struct Order {
    pub id: OrderId,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub venue: Venue,
    pub asset_class: AssetClass,   // 新增
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
    pub filled_quantity: Decimal,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

```rust
// crates/apexes-strategy/src/strategy.rs
pub struct OrderIntent {
    pub symbol: String,
    pub venue: Venue,
    pub asset_class: AssetClass,   // 新增
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
}
```

`OrderManager::submit_order`（`apexes-oms`）构造 `Order` 时从 `intent.asset_class` 直接带过（与其他字段的处理方式一致，无需新逻辑）。

### 现有调用点修复清单（穷举式字面量构造，均需新增 `asset_class` 字段）

**`Order { .. }` 构造点**（8 处）：
| 文件 | 行号（约） | 修复方式 |
|---|---|---|
| `apexes-core/src/order.rs` | 188, 211, 233, 258（测试） | 按测试语境补 `AssetClass::Spot`（BTCUSDT/BTCUSDT测试）或 `AssetClass::Equity`（AAPL 测试，line 211） |
| `apexes-backtest/src/matching_engine.rs` | 259, 308, 396（测试） | 补 `AssetClass::Spot`（现有测试均用 Binance/BTCUSDT） |
| `apexes-risk/src/self_trade.rs` | 122（测试） | 补 `AssetClass::Spot` |
| `apexes-strategy/src/drift_dsl/bindings.rs` | 174（生产代码，`OrderIntent`→`Order`转换路径的一部分） | 从对应 `OrderIntent.asset_class` 透传 |
| `apexes-oms/src/mock_gateway.rs` | 163（测试 helper `sample_order`） | 补 `AssetClass::Spot` |
| `apexes-oms/src/manager.rs` | 94（生产代码，`submit_order`内部构造） | 从 `intent.asset_class` 透传 |
| `apexes-oms/src/schema.rs` | 99（`TryFrom<OrderRow> for Order`，生产代码）, 224（测试 helper） | 99：从新增的 `OrderRow.asset_class` 列透传（见下方 schema 变更）；224：补 `AssetClass::Spot` |

**`OrderIntent { .. }` 构造点**（约 20+ 处，均为测试代码 + 1 处生产代码）：
| 文件 | 修复方式 |
|---|---|
| `apexes-backtest/tests/risk_integration.rs`, `tests/end_to_end.rs`, `src/matching_engine.rs`（23处测试）, `src/runner.rs`（1处测试） | 全部补 `AssetClass::Spot`（现有测试均为 Binance/BTCUSDT 场景） |
| `apexes-risk/src/self_trade.rs`, `lists.rs`, `limits.rs`, `composite.rs`, `circuit_breaker.rs`（测试 helper，共6处） | 补 `AssetClass::Spot` |
| `apexes-strategy/src/strategy.rs`（1处测试） | 补 `AssetClass::Spot` |
| `apexes-strategy/src/drift_dsl/convert.rs`（**生产代码**，Drift Script `OrderIntent`转换） | 新增一个 `asset_class` 字符串字段解析（复用 `parse_venue`/`parse_side` 同款模式，新增 `parse_asset_class` 辅助函数），Drift Script 策略脚本需要新增 `asset_class: "spot"` / `"perpetual"` 字段——**这是本轮对 Drift Script 绑定 API 的一个小的、必要的破坏性变更**，需要更新其自测试用例 |
| `apexes-oms/tests/live_postgres.rs`, `src/manager.rs`（测试 helper，共2处） | 补 `AssetClass::Spot` |
| `apexes-paper/src/runner.rs`（1处测试） | 补 `AssetClass::Spot` |

**`schema.rs`（`apexes-oms`）DDL/Row 变更**：`orders` 表新增 `asset_class TEXT NOT NULL` 列，`OrderRow` 新增对应字段 + `TryFrom` 双向转换分支（`"Spot"`/`"Perpetual"`/`"Future"`/`"Option"`/`"Equity"` 字符串序列化，与现有 `side`/`order_type`/`status` 字段的字符串编码模式一致）。

**实施计划阶段的强制要求**：写实施计划时必须把上述表格中的每一处调用点列为独立的、可勾选的 checklist 项，不能笼统写"修复所有调用点"——这是此前 `apexes-risk` plan Task 8 踩过的坑，本设计明确要求不再重复。

---

## 三、Crate 结构

```
crates/apexes-gateway-binance/
├── Cargo.toml
├── src/
│   ├── lib.rs                  // pub mod 声明 + re-exports
│   ├── config.rs                // BinanceGatewayConfig
│   ├── signing.rs               // sign_query_string (纯函数，无 HTTP 依赖)
│   ├── error.rs                 // BinanceGatewayError（内部用，最终转换为 GatewayOutcome/String）
│   ├── rest/
│   │   ├── mod.rs
│   │   ├── spot.rs               // 现货 REST 端点路径/参数构造
│   │   └── futures.rs            // 永续合约 REST 端点路径/参数构造
│   ├── gateway.rs                // BinanceExecutionGateway（实现 ExecutionGateway）
│   └── user_data_stream.rs      // listenKey 获取/续期 + WebSocket 订阅 + 事件解析
└── tests/
    ├── signing.rs                // 签名算法纯函数测试（不依赖网络）
    ├── wiremock_rest.rs          // wiremock 验证真实请求形状（URL/headers/query string）
    └── live_testnet.rs           // #[ignore] 真实 Binance Testnet 集成测试

crates/apexes-live-trading/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── config.rs                 // LiveTradingConfig
│   ├── exchange_update.rs        // ExchangeUpdate（网关无关的推送事件类型）
│   └── runner.rs                 // run_live_trading
└── tests/
    └── live_trading_integration.rs   // 用 MockExecutionGateway 驱动的端到端测试（不需要 #[ignore]）
```

`apexes-gateway-binance/Cargo.toml` 依赖：`apexes-core`、`apexes-oms`（`ExecutionGateway`/`GatewayOutcome`）、`reqwest`（`json` feature）、`tokio-tungstenite`、`hmac`、`sha2`、`hex`、`serde`/`serde_json`、`chrono`、`rust_decimal`、`tracing`、`async-trait`、`tokio`。dev-dependencies 新增 `wiremock`。

`apexes-live-trading/Cargo.toml` 依赖：`apexes-core`、`apexes-oms`、`apexes-backtest`（`PortfolioLedger`/`Strategy`/`StrategyContext`）、`apexes-risk`、`apexes-strategy`、`tokio`、`futures`、`chrono`、`rust_decimal`、`tracing`。

---

## 四、`ExecutionGateway` trait 扩展：`query_fills_for_order`

```rust
// crates/apexes-oms/src/gateway.rs

/// A single trade execution as reported by the exchange, keyed by the
/// exchange's own per-trade identifier (`exchange_trade_id`) so
/// `OrderManager` can deduplicate against fills it has already recorded
/// (the exchange's "my trades"-style endpoints return the FULL trade
/// history for an order every time, not just new trades since the last
/// call -- naively inserting every returned entry would create
/// duplicate `Fill` rows across repeated calls). Not `apexes_core::Fill`
/// directly: the gateway has no knowledge of our locally-assigned
/// `FillId`, which `OrderManager` assigns only for genuinely-new trades.
#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeFill {
    pub exchange_trade_id: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_currency: Option<String>,
    pub filled_at: DateTime<Utc>,
}

#[async_trait]
pub trait ExecutionGateway: Send + Sync {
    // ... 现有四个方法不变 ...

    /// Returns every trade execution the exchange has recorded for
    /// `order` (identified by `order.exchange_order_id`, which the
    /// caller guarantees is `Some`). Returns the FULL trade history for
    /// this order every call (matching real exchanges' typical "my
    /// trades" endpoint shape) -- deduplication against previously
    /// recorded fills is `OrderManager`'s responsibility (via
    /// `exchange_trade_id`), not this method's.
    async fn query_fills_for_order(
        &self,
        order: &Order,
    ) -> Result<Vec<ExchangeFill>, String>;
}
```

`MockExecutionGateway`（`apexes-oms`）新增对应实现：内存态注入 `Vec<ExchangeFill>`，支持测试用例编排"订单部分成交产生2笔交易记录，之后再次查询返回同样2笔（验证去重）"这类场景。

### `fills` 表 + `OrderStore` 变更

```sql
-- ALTER：fills 表新增列
exchange_trade_id   TEXT NOT NULL,

-- 新增唯一约束，是去重的实际执行者
UNIQUE (order_id, exchange_trade_id)
```

`OrderStore::insert_fill` 改为 `ON CONFLICT (order_id, exchange_trade_id) DO NOTHING`（幂等，与 `orders` 表 `upsert_order` 的 `ON CONFLICT (id) DO UPDATE` 幂等风格一致，但语义是"跳过重复"而非"覆盖更新"，因为一笔真实成交一旦发生就不会再变化）。`insert_fill` 返回值改为 `Result<bool, OmsError>`（`true` = 确实插入了新记录，`false` = 因冲突被跳过），供 `OrderManager` 判断该笔 `ExchangeFill` 是否是"新的"、需要喂给调用方的 `PortfolioLedger`。

---

## 五、`OrderManager::apply_exchange_update`（从 `reconcile()` 抽取的共享逻辑）

### 为什么需要抽取

`reconcile()` 目前的"用交易所权威状态无条件覆盖本地状态 + 持久化 + 维护 `open_orders`"逻辑，写死在其两趟循环内部（`apexes-oms/src/manager.rs` 现有代码）。User Data Stream 推送需要对**单个**订单应用完全相同的处理规则，但触发路径是异步 WebSocket 消息，不是 `reconcile()` 的批量轮询。为避免在两处重复"覆盖状态 + 判断变化 + 持久化 + 更新 `open_orders` 归属 + 检测终态转换以拉取成交"这一整套逻辑，抽取为一个共享方法。

```rust
impl OrderManager {
    /// Applies one authoritative exchange-reported status update to a
    /// single local order -- the shared core both `reconcile()`'s two
    /// passes and live push-based updates (Binance User Data Stream via
    /// `apexes-gateway-binance`) call, so this overwrite/persist/
    /// fill-fetch logic exists in exactly one place.
    ///
    /// Unconditionally overwrites local status with `status` (same
    /// authoritative-exchange rationale as `reconcile()` -- see its
    /// docs). If `exchange_order_id` is `Some`, populates it on the
    /// local order if not already known. If the status transition
    /// crosses into `PartiallyFilled` or `Filled` (i.e. `status` is one
    /// of those AND differs from the order's previous status), calls
    /// `gateway.query_fills_for_order`, deduplicates against
    /// `OrderStore` via `exchange_trade_id` (see §四), persists any
    /// genuinely-new fills, and returns them.
    ///
    /// Returns `Ok(new_fills)` -- empty if the order wasn't found
    /// locally (logs `tracing::error!`, matching `reconcile()`'s
    /// existing "no local record" handling), if the status was
    /// unchanged, or if no new fills were found beyond what was already
    /// recorded.
    pub async fn apply_exchange_update(
        &mut self,
        client_order_id: OrderId,
        exchange_order_id: Option<String>,
        status: OrderStatus,
    ) -> Result<Vec<Fill>, OmsError>;
}
```

`reconcile()` 的两趟循环体（现有 `manager.rs` 中直接内联的覆盖/持久化代码）均改写为调用 `self.apply_exchange_update(...)`，其返回的 `changed`/`new_fills` 分别累积进 `reconcile()` 自己的返回值——**`reconcile()` 的公开签名本身不变**（仍返回 `Vec<OrderId>`，代表状态变化的订单），但内部实现改为委托，同时額外收集所有 `new_fills` 通过一个新的重载或者第二个返回值暴露给调用方（`run_live_trading` 需要这些 `Fill` 来更新 `PortfolioLedger`）。

**签名调整**：`reconcile()` 返回值从 `Vec<OrderId>` 改为 `Vec<(OrderId, Vec<Fill>)>`（每个状态变化的订单，及其伴随产生的新成交，可能为空 `Vec`）——这是对 `apexes-oms` 现有公开 API 的又一处破坏性变更，需要更新 `apexes-oms` 自身的所有 `reconcile()` 调用点（`manager.rs` 测试 + `tests/live_postgres.rs`）。

---

## 六、`apexes-gateway-binance`：签名、端点路由、`GatewayOutcome` 映射

### 配置

```rust
// crates/apexes-gateway-binance/src/config.rs
#[derive(Debug, Clone)]
pub struct BinanceGatewayConfig {
    pub api_key: String,
    pub api_secret: String,
    /// e.g. "https://api.binance.com" (production) or
    /// "https://testnet.binance.vision" (testnet). Overridable so
    /// `wiremock` tests can point it at a local mock server.
    pub spot_base_url: String,
    /// e.g. "https://fapi.binance.com" (production) or
    /// "https://testnet.binancefuture.com" (testnet).
    pub futures_base_url: String,
    pub recv_window_ms: u64,
}
```

不提供 `Default`、不从环境变量/配置文件加载——调用方（测试代码、未来的 CLI/UI 工具化工作）自行构造。

### 签名（纯函数，独立单元测试）

```rust
// crates/apexes-gateway-binance/src/signing.rs
/// Computes Binance's required HMAC-SHA256 signature over a request's
/// query string, using `secret` as the HMAC key. Pure function --
/// testable without any HTTP dependency (per design decision: keep the
/// signing algorithm itself covered by fast, network-free unit tests,
/// while `wiremock` tests separately verify the resulting HTTP request
/// shape end-to-end).
pub fn sign_query_string(secret: &str, query: &str) -> String;
```

### 端点路由

```rust
enum ApiFamily { Spot, Futures }

impl From<AssetClass> for ApiFamily {
    fn from(asset_class: AssetClass) -> Self {
        match asset_class {
            AssetClass::Spot => ApiFamily::Spot,
            AssetClass::Perpetual => ApiFamily::Futures,
            _ => unreachable!("BinanceExecutionGateway only routes Spot/Perpetual orders"),
        }
    }
}
```

`BinanceExecutionGateway::submit_order`/`cancel_order`/`query_order_status`/`query_fills_for_order` 均先从 `order.asset_class`（或 `query_order_status`/`query_all_open_orders`场景下调用方提供的 asset_class 参数——见下方接口签名调整）算出 `ApiFamily`，再选择 `config.spot_base_url`/`config.futures_base_url` + 对应端点路径常量（现货 `/api/v3/order`、`/api/v3/myTrades`；合约 `/fapi/v1/order`、`/fapi/v1/userTrades`）。

**接口签名调整说明**：`ExecutionGateway::query_order_status`/`query_all_open_orders` 目前只接受 `exchange_order_id: &str`，不携带 `asset_class` 信息，导致 `BinanceExecutionGateway` 无法判断该查哪个 API 族。**决策**：这两个方法新增 `asset_class: AssetClass` 参数（`OrderManager` 调用时从其本地持有的 `Order`/`open_orders` 中取值传入）——这是对 `ExecutionGateway` trait 的又一处必要扩展，随 §四 的 `query_fills_for_order` 一并在同一轮实施计划里处理，避免分两次改同一个 trait。

### `GatewayOutcome` 映射规则

| Binance 响应 | 映射 |
|---|---|
| HTTP 200 + 合法订单 JSON | `Accepted { exchange_order_id: <orderId 字符串化> }` |
| HTTP 4xx + `{"code": <负数>, "msg": "..."}` | `Rejected { reason: msg }`（明确的业务拒绝，如余额不足/参数非法/精度错误） |
| 网络错误（连接失败/超时）、HTTP 5xx、响应体无法解析为预期 JSON 形状 | `Uncertain { reason: <错误描述> }`（无法确认交易所是否已处理） |

单元测试（`wiremock`）分别覆盖三种映射路径，验证请求本身携带了正确的 `signature`/`timestamp`/`newClientOrderId`（= `order.id`）等参数。

---

## 七、User Data Stream

### listenKey 生命周期

```rust
// crates/apexes-gateway-binance/src/user_data_stream.rs
pub struct UserDataStreamHandle {
    listen_key: String,
    // 内部持有一个后台 tokio task 的 JoinHandle，负责每 30 分钟 PUT 续期一次
    // （Binance 要求 60 分钟内续期，30 分钟给出充分安全余量）
}
```

`BinanceExecutionGateway` 提供 `start_user_data_stream(&self) -> Result<(UserDataStreamHandle, impl Stream<Item = ExchangeUpdate>), String>`：
1. `POST /api/v3/userDataStream`（现货）与 `POST /fapi/v1/listenKey`（合约）——**两条独立的流**，因为 Binance 现货和合约账户的 User Data Stream 是分开的两个 WebSocket 端点，各自有独立的 `listenKey`。`start_user_data_stream` 内部同时发起两个订阅，把两路事件合并进同一个输出 `Stream`。
2. 连接 `wss://stream.binance.com:9443/ws/<listenKey>`（现货）/ `wss://fstream.binance.com/ws/<listenKey>`（合约）。
3. 解析 `executionReport`（现货）/ `ORDER_TRADE_UPDATE`（合约）事件，转换为网关无关的 `ExchangeUpdate`：

```rust
// crates/apexes-live-trading/src/exchange_update.rs
/// Gateway-agnostic representation of a push-based order status update
/// -- `apexes-live-trading` depends on this, not any Binance-specific
/// type, keeping the event loop exchange-agnostic per this crate's
/// stated purpose.
#[derive(Debug, Clone)]
pub struct ExchangeUpdate {
    pub client_order_id: OrderId,
    pub exchange_order_id: Option<String>,
    pub status: OrderStatus,
}
```

后台续期任务与 WebSocket 重连均在 `apexes-gateway-binance` 内部处理，对 `apexes-live-trading` 透明——`run_live_trading` 只消费 `impl Stream<Item = ExchangeUpdate>`。

---

## 八、`apexes-live-trading::run_live_trading`

```rust
pub struct LiveTradingConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    /// Fallback polling interval for `OrderManager::reconcile()`,
    /// running alongside (not instead of) the push-based
    /// `exchange_updates` stream -- catches anything a WS disconnect
    /// window might have missed.
    pub reconcile_interval: Duration,
}

pub struct LiveTradingReport {
    pub overall: PerformanceSummary,
    pub trades: Vec<Fill>,
}

pub async fn run_live_trading(
    config: LiveTradingConfig,
    market_data_provider: &dyn MarketDataProvider,
    strategy: impl Strategy,
    mut order_manager: OrderManager,
    risk_checker: Option<CompositeRiskChecker>,
    mut exchange_updates: impl Stream<Item = ExchangeUpdate> + Unpin,
    mut stop_signal: oneshot::Receiver<()>,
) -> Result<LiveTradingReport, ApexesError>
```

### 循环结构

`tokio::select!` 三路，外加 stop_signal 检查（与 `run_event_loop` 的 stop_signal 处理方式一致）：

1. **行情事件**（`market_data_provider.subscribe(...)` 产生的 `MarketEvent` 流）→ `Strategy::on_market_event(&event, &ctx)`（`ctx.positions`/`ctx.balances` 来自 `PortfolioLedger`，`ctx.open_orders` 来自 `order_manager.open_orders()`）→ 对每个 `OrderIntent`：`risk_checker.check(...)` 通过后调用 `order_manager.submit_order(intent, ledger.positions(), ledger.balances())`。
2. **交易所推送**（`exchange_updates` 流）→ `order_manager.apply_exchange_update(update.client_order_id, update.exchange_order_id, update.status)` → 返回的 `Vec<Fill>` 依次 `ledger.apply_fill(&fill)` + 累积进 `LiveTradingReport.trades`。
3. **定时器**（`tokio::time::interval(config.reconcile_interval)`）→ `order_manager.reconcile()` → 返回的 `Vec<(OrderId, Vec<Fill>)>` 中每个 `Fill` 同样喂给 `ledger.apply_fill`。

退出（`stop_signal` 触发或其发送端被丢弃）时返回目前累积的 `LiveTradingReport`（不是错误，与 `run_event_loop`/`run_paper_trading` 的优雅退出语义一致）。

### 测试策略

`apexes-live-trading` 的端到端测试使用 `MockExecutionGateway`（`apexes-oms` 已有）+ 一个测试用的内存态 `MarketDataProvider`（`apexes-paper` 测试里已有先例）+ 一个可编程的 `ExchangeUpdate` mock 流,**不需要 `#[ignore]`**——因为不涉及真实网络/数据库依赖之外的东西（`OrderManager`/`OrderStore` 层面已有的 PostgreSQL 依赖决定了这些测试仍然间接需要真实 PostgreSQL,继承 `apexes-oms` 现有的测试隔离约定,标记 `#[ignore]`)。

`apexes-gateway-binance` 自身分三层测试：
1. `signing.rs`——纯函数签名测试，不依赖网络。
2. `wiremock_rest.rs`——用 `wiremock` 起本地 HTTP mock server，验证 `BinanceExecutionGateway` 发出的真实请求形状(URL/query string/`signature`参数存在且格式正确/`newClientOrderId`等于传入`Order.id`)，同时验证三种`GatewayOutcome`映射路径。
3. `tests/live_testnet.rs`——`#[ignore]`,需要真实 Binance Testnet API Key（用户自行配置环境变量或本地文件运行，不由本轮提供加载机制,直接在测试代码里用`std::env::var(...).expect(...)`读取，因为这是测试代码而非生产代码,不违反"网关本身不做环境变量假设"的决策)。

---

## 九、不做自动重试、`Uncertain` 的处理与 `#7` 保持一致

`BinanceExecutionGateway`不引入任何超出`GatewayOutcome`三态语义之外的重试机制——网络超时等`Uncertain`结果的解决完全依赖`OrderManager::reconcile()`与 User Data Stream的组合(§五-§八已覆盖),与`#7`"不做自动重试机制"的既有决策保持一致,不重新引入这个已经被明确推迟的话题。

---

## 十、总结:本设计相对路线图/`#7`设计的偏离与新增决策记录

供未来子项目(尤其`#10`)参考:

1. **`barter-execution`不可用于任何真实交易所私有API**——已核实其Binance客户端模块是空的私有模块,`#10`评估其他交易所网关时不应默认这条路径可行。
2. **`Order`/`OrderIntent`新增了`asset_class`字段**——这是自`#0`基础骨架以来对这两个核心类型的首次破坏性变更,`#10`如果需要类似的"同一交易所多资产类别路由"需求,应复用这个字段而非发明新的机制。
3. **`ExecutionGateway` trait 本轮新增/修改了3处**:`query_fills_for_order`(新方法)、`query_order_status`/`query_all_open_orders`新增`asset_class`参数、`reconcile()`返回值签名从`Vec<OrderId>`改为`Vec<(OrderId, Vec<Fill>)>`——`#7`当时的"网关无关设计只需`#8`实现trait,不需要改动OMS内部逻辑"这一断言被证明**部分不成立**:确实不需要改动`OrderManager`的业务逻辑主体,但trait本身及`reconcile()`的对外签名仍需扩展,因为`#7`设计时未曾预见"如何获取成交明细"与"如何区分现货/合约路由"这两个真实交易所场景下必然出现的需求。记录于此,提醒未来"网关无关"设计不应过度自信trait在首个真实实现到来前就已经完备。
