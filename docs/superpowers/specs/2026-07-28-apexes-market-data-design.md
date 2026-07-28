# Apexes — 子项目 #1：行情接入（单一免费加密货币源）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#1` 的详细设计。目标：在 `apexes-core` 定义的 `MarketDataProvider` trait 之上，为 Binance 提供一个真实的、端到端可用的实现，验证 trait 设计在真实交易所协议下的可行性。

---

## 一、范围

### 包含
- 新增 workspace 成员 crate：`apexes-market-data`
- `BinanceMarketDataProvider`：`apexes-core::MarketDataProvider` 的真实实现
- 同时支持多个交易对（`subscribe(&[Instrument])` 的 slice 语义）
- 产出全部 4 种 `MarketEvent` variant：`Trade`、`Tick`（bookTicker）、`Depth`（L2 订单簿）、`Bar`（自研 OHLCV 聚合，覆盖全部 6 个 `BarInterval`）
- 断线自动重连（指数退避 + 抖动），对下游消费者透明
- 单元测试覆盖纯逻辑部分 + 显式标记为 `#[ignore]` 的真实 Binance 集成测试

### 不包含（明确超出范围）
- 除 Binance 外的其他交易所（DEX、A股/H股/美股）——留给子项目 #10
- 历史数据回补/存储——留给子项目 #2
- 付费 Level-2 数据源——留给未来按需评估
- 任何策略/回测对本数据的消费——留给子项目 #3/#4
- 认证私有 API（下单、账户查询）——留给子项目 #8（首个实盘网关）

---

## 二、开源库复用决策：`barter-data`

在设计阶段实际检出并阅读了 `barter-rs` 源码（而非仅凭 crates.io 描述），关键发现：

1. **`barter-data` 已经完整实现了 Binance 官方文档规定的订单簿同步算法**——REST 快照获取 + WebSocket 增量更新的 `U`/`u`/`pu` 序列号校验 + 缺口检测（`BinanceSpotOrderBookL2Sequencer::validate_sequence`，逻辑与 [Binance 官方"如何正确维护本地订单簿"文档](https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly) 完全一致），并通过 `OrderBookEvent::Snapshot`/`Update` 变体向外暴露结果。这正是本子项目原本计划自行实现的、风险最高的部分。
2. `barter_data::books::OrderBook` 提供 `sequence() -> u64`、`bids() -> &OrderBookSide<Bids>`、`asks() -> &OrderBookSide<Asks>`、`time_engine() -> Option<DateTime<Utc>>` 等公开访问器，足以映射到 Apexes 自己的 `DepthUpdate`。

**决策**：`apexes-market-data` 使用 `barter-data` 承担 WebSocket 连接、Binance 消息解析、**以及订单簿快照/增量同步与缺口检测**的全部工作；`apexes-market-data` 自身只负责把 `barter-data` 的规范化类型（`PublicTrade`、`OrderBookL1`、`OrderBookEvent`）转换为 Apexes 自己的 `MarketEvent`，以及在此之上聚合出 `Bar`（`barter-data` 不提供 K线聚合，且不同交易所的原生 kline 粒度不一致，自行聚合可在未来跨交易所时保持行为一致）。

这一发现将本子项目原本预估的"自行实现订单簿重建"这一最复杂、最高风险的自研逻辑，替换为对已被广泛使用（132K+ 下载量）、经过验证的开源实现的封装，显著降低了本子项目的实现与正确性风险。

---

## 三、架构

```
BinanceMarketDataProvider::subscribe(&self, instruments: &[Instrument])
  1. 过滤 instruments：venue != Venue::Binance 的实例被跳过并记录 tracing::warn!
     （若过滤后为空，返回 Err(ApexesError::SubscriptionFailed { .. })）
  2. 为每个通过过滤的 instrument 各自 spawn 4 个 tokio task：
       ├─ trade_task     → barter-data PublicTrades 流 → MarketEvent::Trade
       ├─ ticker_task    → barter-data OrderBooksL1（bookTicker）流 → MarketEvent::Tick
       ├─ depth_task     → barter-data OrderBooksL2 流（快照+增量，序列号校验由 barter-data
       │                    内部完成）→ DepthConverter → MarketEvent::Depth
       └─ bar_task       → 复用 trade_task 的原始成交流 → BarAggregator（每个 symbol 6 个
                            并行 interval 桶）→ MarketEvent::Bar（区间边界关闭时产出）
  3. 全部 4×N 个 task 将产出的 MarketEvent 推入同一个共享的 tokio::sync::mpsc 通道
  4. subscribe() 在 spawn 完成后立即返回 Ok(该通道包装为 BoxStream<'static, MarketEvent>)
     ——不等待任何一个 WebSocket 握手完成（见 §5 "初始连接失败处理"）
```

**从 tokio 胶水代码中抽取出的纯逻辑、可独立单元测试的类型**：
- `BarAggregator`（`bar_aggregator.rs`）——每个 symbol 维护 6 个并行的 interval 累积桶，`on_trade(price, qty, time) -> Vec<Bar>`（一次成交可能触发 0 个或多个 bar 关闭，取决于跨越了多少个区间边界）
- `DepthConverter`（`depth_converter.rs`）——薄转换层，把 `barter_data::books::OrderBook` + `OrderBookEvent::Snapshot`/`Update` 标签转换为 Apexes 的 `DepthUpdate`（映射 `sequence()`/`bids()`/`asks()`/`is_snapshot`）

### 文件结构

```
crates/apexes-market-data/
  Cargo.toml
  src/
    lib.rs
    provider.rs          # BinanceMarketDataProvider、subscribe()、task 编排
    bar_aggregator.rs     # BarAggregator（纯逻辑，单元测试）
    depth_converter.rs    # DepthConverter（纯逻辑，单元测试）
    trade_task.rs         # tokio task：barter-data 成交流 -> MarketEvent::Trade
    ticker_task.rs        # tokio task：barter-data bookTicker -> MarketEvent::Tick
    depth_task.rs         # tokio task：barter-data L2 -> DepthConverter -> MarketEvent::Depth
    bar_task.rs           # tokio task：成交流 -> BarAggregator -> MarketEvent::Bar
```

---

## 四、`Instrument` 过滤与错误处理

- **venue 不匹配**：`subscribe()` 静默跳过 `venue != Venue::Binance` 的 `Instrument`，通过 `tracing::warn!` 记录，不中断整个调用——允许调用方传入混合 venue 的列表而不需要提前过滤。
- **过滤后为空**：若所有 instrument 都被跳过（结果为空列表），返回 `Err(ApexesError::SubscriptionFailed { .. })`——因为一个空订阅永远不会产出任何数据，这本身就是一个真实错误，而非可以静默忽略的边界情况。
- **初始连接失败处理**：`subscribe()` 在 spawn 完 4×N 个 task 后立即返回 `Ok`，**不等待**任何 WebSocket 握手完成。某个 symbol 的初始连接失败与运行中掉线走同一条重连路径（见下），不存在单独的"初始连接错误"分支——这样可以避免 `subscribe()` 因为等待 N 个握手而变慢，也避免一个坏 symbol 阻塞整个调用。

---

## 五、断线重连

**行为**：每个 task 独立拥有一个"重连 + 指数退避"循环，包裹其 `barter-data` 订阅。无论是初始连接失败还是运行中断线，都走同一套重连逻辑；重连对下游消费者透明——返回的 `BoxStream` 永远不会因为某个 symbol 掉线而结束，只是暂时停止产出该 symbol 的事件，重连成功后自动恢复。

**退避参数**：
- 初始延迟：1 秒
- 倍增系数：2x（每次失败后延迟翻倍）
- 上限：30 秒
- 抖动：对每次计算出的延迟施加 ±25% 的随机抖动，避免多个 symbol 同时掉线时产生同步重连风暴

**Depth 重连语义**：`depth_task` 重连后，`barter-data` 会重新获取订单簿快照；这在 Apexes 层面自然表现为下一条 `DepthUpdate` 的 `is_snapshot: true`——下游消费者据此判断"必须丢弃之前维护的本地状态，从此更新重新开始"，无需额外的错误信号或专门的重连事件类型。这一约定同样适用于 `barter-data` 内部检测到序列号缺口（`DataError::InvalidSequence`）而触发的重新同步：无论重连原因是网络中断还是序列号缺口，都统一表现为下一条 `DepthUpdate.is_snapshot == true`。

---

## 六、`BarAggregator` 设计细节

**核心决策：跳过空区间，不合成空 Bar**——如果某个 interval 内一笔成交都没有，`BarAggregator` 不会为该区间产出任何 `Bar`（不合成 OHLC 全部等于上一收盘价、volume=0 的"平 K"）。理由：保持聚合逻辑简单（无需跨静默期追踪"最后已知价格"），下游消费者（图表、策略）自行处理 bar 序列中的空隙。

**乱序成交处理**：`BarAggregator` 具备防御性——如果一笔成交的时间戳落入一个已经关闭（已产出 `Bar`）的区间，该笔成交会被丢弃并通过 `tracing::warn!` 记录，绝不会修改一个已经产出的 `Bar`。假设前提：Binance WebSocket 连接内的成交总体有序（这是 Binance 的保证），乱序处理只是防御性兜底，而非常态路径。

**跨区间跳跃（gap）行为**：一笔成交到达时，如果与上一笔成交之间跨越了多个区间边界（例如 3 分钟没有成交，`OneMinute` 区间下应该跨越 2-3 个边界），`on_trade()` 应正确关闭所有已完整跨越的区间（各自产出一个 `Bar`，如果该区间内有成交的话——按上述"跳过空区间"规则，中间的空区间不产出任何内容），并为新成交所在的区间开启新的累积桶。

**覆盖区间**：同时对全部 6 个 `BarInterval` 变体（`OneSecond`/`OneMinute`/`FiveMinutes`/`FifteenMinutes`/`OneHour`/`OneDay`）维护独立的累积桶，每个 symbol 各自 6 份状态，互不影响。

---

## 七、测试计划

### 单元测试（纯逻辑，无网络依赖，默认随 `cargo test` 运行）

**`BarAggregator`**：
- 单笔成交、单区间内累积后关闭产出 `Bar`（OHLC/volume 正确性）
- 跨单个区间边界的关闭与新桶开启
- 跨多个区间边界（gap）：中间空区间不产出 `Bar`，两端有成交的区间各自正确产出
- 乱序/迟到成交：时间戳落入已关闭区间时被丢弃，不影响已产出的 `Bar`
- 至少覆盖 `OneMinute`，并对全部 6 个 `BarInterval` 做代表性抽查

**`DepthConverter`**：
- 构造合成的 `barter_data::books::OrderBook` + `OrderBookEvent::Snapshot`，验证转换出的 `DepthUpdate` 的 `sequence`/`is_snapshot: true`/`bids`/`asks` 正确
- 同上，针对 `OrderBookEvent::Update`（`is_snapshot: false`）

### 集成测试（连接真实 Binance WebSocket，标记 `#[ignore]`，默认不随 `cargo test`/`cargo test --workspace` 运行）

- 订阅至少一个真实 symbol（如 `BTCUSDT`），验证端到端产出全部 4 种 `MarketEvent` variant 中的至少一例
- 显式通过 `cargo test -p apexes-market-data -- --ignored` 运行，或在专门的 CI 步骤中运行；保持默认 `cargo test --workspace` 快速、无网络依赖，与 `apexes-core` 现有测试套件的约定一致

---

## 八、依赖与版本

```toml
[package]
name = "apexes-market-data"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
barter-data = "0.11"
barter-instrument = "0.3"
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time"] }
futures = { workspace = true }
async-trait = { workspace = true }
rust_decimal = { workspace = true }
chrono = { workspace = true }
serde = { workspace = true }
tracing = "0.1"
rand = "0.8"

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
```

日志统一使用 `tracing`（`warn!`/`debug!`），不使用 `println!`——与项目 Rust 规范一致。

---

## 九、验收标准（Definition of Done）

- `cargo build --workspace` 成功（新增 `apexes-market-data` 成员）
- `cargo test --workspace`（默认，不含 `--ignored`）全部通过，且不产生任何网络访问
- `cargo test -p apexes-market-data -- --ignored` 在有网络的环境下手动运行通过，验证端到端可以从真实 Binance 拿到全部 4 种 `MarketEvent`
- `cargo clippy --workspace -- -D warnings` 无警告
- `cargo fmt --check` 无格式问题
- `BinanceMarketDataProvider` 实现 `apexes_core::MarketDataProvider` trait，可通过 `Box<dyn MarketDataProvider>` 使用
