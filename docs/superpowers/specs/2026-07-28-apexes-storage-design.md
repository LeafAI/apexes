# Apexes — 子项目 #2：历史数据存储（ClickHouse）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#2` 的详细设计。目标：为 `apexes-core` 的 `MarketEvent` 提供一个真实、可用的持久化存储层，使用 ClickHouse 作为唯一后端。

---

## 一、范围

### 包含
- 新增 workspace 成员 crate：`apexes-storage`
- `StorageWriter`：将 `MarketEvent`（全部 4 种 variant：`Trade`/`Tick`/`Depth`/`Bar`）批量写入 ClickHouse
- `StorageReader`：按 symbol + venue + 日期范围 + 事件类型查询回 `Vec<MarketEvent>`，证明写入-读取往返正确
- ClickHouse 表结构设计（4 张表，`MergeTree` 引擎，按日分区）
- 仓库根目录 `docker-compose.yml`，供本地开发/测试启动 ClickHouse
- `rust_decimal::Decimal` ↔ ClickHouse `Decimal128`（通过 `fixnum` 库的 `FixedPoint` 类型）显式转换层
- 单元测试（纯逻辑，无需数据库）+ 显式标记 `#[ignore]` 的真实 ClickHouse 集成测试

### 不包含（明确超出范围）
- 消息总线集成——留给子项目 #12
- 面向回测引擎的流式读取 API——留给子项目 #3（该子项目更清楚自己的读取模式需求）
- 历史数据从外部数据源的批量回补——留给未来按需评估
- ClickHouse 多节点集群拓扑、副本、分片——本阶段单节点足够
- Compaction/TTL 策略调优——本阶段使用 ClickHouse 默认行为

---

## 二、路线图调整说明

原顶层路线图（子项目 #2）计划"先用本地文件/Parquet 或 sqlite，暂缓 ClickHouse（留给子项目 #12）"。在本子项目 brainstorming 阶段，深入设计本地 Parquet 方案的增量写入语义时，发现 **Parquet 是一次性写入、footer 终结的格式**——已关闭（调用 `close()`）的 `.parquet` 文件无法重新打开并追加新的 row group（`parquet` crate 的 `SerializedFileWriter::close()` 消费 `self` 并写入最终 footer，无"重新打开追加"的 API）。这意味着"当天多次写入同一个 symbol+day 文件"这一原本设想的最小可用方案，实际上做不到"追加"，只能退化为"当天多个小文件 + 读取时拼接"。

同时评估发现 `clickhouse` crate（官方 Rust 客户端）已经成熟稳定（1000万+ 下载量，持续维护）。综合两点，决定：**将 ClickHouse 引入的时间点从子项目 #12 提前到本子项目**，直接使用 ClickHouse 作为历史数据存储的唯一后端，跳过"本地文件→ClickHouse"的中间迁移阶段。该调整已同步更新到顶层路线图文档。

---

## 三、部署与测试策略

### ClickHouse 本地部署

仓库根目录新增 `docker-compose.yml`，包含一个 `clickhouse/clickhouse-server` 服务，暴露 HTTP 接口（`localhost:8123`）与原生协议接口（`localhost:9000`）。开发者在处理/测试 `apexes-storage` 前需执行 `docker compose up -d` 启动本地 ClickHouse 实例。

### 测试分层（与子项目 #1 的模式一致）

- **纯逻辑单元测试**（无数据库依赖，默认随 `cargo test` 运行）：`MarketEvent` ↔ ClickHouse 行结构的映射/转换逻辑（含 `Decimal` ↔ `FixedPoint` 转换），SQL/建表语句构造逻辑。
- **真实 ClickHouse 集成测试**（标记 `#[ignore]`，默认不随 `cargo test`/`cargo test --workspace` 运行）：`write_batch`/`read_batch` 的完整写入-读取往返，需要连接真实运行中的 ClickHouse 实例；显式通过 `cargo test -p apexes-storage -- --ignored` 运行（要求本地已 `docker compose up`）。

---

## 四、ClickHouse 表结构

四张表，一一对应 `MarketEvent` 的四种 variant：`trades`、`ticks`、`depth_updates`、`bars`。

### 通用设计决策

- **引擎**：`MergeTree`——标准时序数据引擎，支持高效的分区裁剪与范围扫描。
- **分区键**：`PARTITION BY toYYYYMMDD(timestamp)`——按天分区，便于按日期范围高效查询，也便于未来按分区批量删除旧数据。
- **排序键**：`ORDER BY (venue, symbol, timestamp)`——匹配未来最主要的查询模式（"给我某个 symbol 在某个日期范围内的数据"），venue+symbol 前缀保证同一交易对的数据物理相邻。
- **数值精度**：所有价格/数量/成交量/手续费字段统一使用 `Decimal(38, 10)`——对应 ClickHouse 的 `Decimal128`/`Decimal256` 范围，10 位小数覆盖加密货币典型的 8 位精度并留有余量，28 位整数部分远超任何真实价格/数量/成交量的量级需求，一套精度适配全部未来市场（加密货币、A股、H股、美股），无需按市场单独调参。

### 4.1 `trades` 表（对应 `TradeEvent`）

```sql
CREATE TABLE trades
(
    symbol      String,
    venue       String,
    price       Decimal(38, 10),
    quantity    Decimal(38, 10),
    side        Nullable(String),   -- "Buy" / "Sell" / NULL（并非所有数据源都提供）
    trade_id    Nullable(String),
    timestamp   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (venue, symbol, timestamp);
```

### 4.2 `ticks` 表（对应 `Tick`）

```sql
CREATE TABLE ticks
(
    symbol      String,
    venue       String,
    bid_price   Decimal(38, 10),
    bid_size    Decimal(38, 10),
    ask_price   Decimal(38, 10),
    ask_size    Decimal(38, 10),
    timestamp   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (venue, symbol, timestamp);
```

### 4.3 `depth_updates` 表（对应 `DepthUpdate`）

```sql
CREATE TABLE depth_updates
(
    symbol      String,
    venue       String,
    bids        Array(Tuple(Decimal(38, 10), Decimal(38, 10))),  -- (price, quantity)
    asks        Array(Tuple(Decimal(38, 10), Decimal(38, 10))),  -- (price, quantity)
    sequence    Nullable(UInt64),
    is_snapshot Bool,
    timestamp   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (venue, symbol, timestamp);
```

`bids`/`asks` 使用 ClickHouse 原生的 `Array(Tuple(...))` 类型，一行对应一次完整的 `DepthUpdate`（与内存中的 `apexes_core::DepthUpdate` 形状完全一致），而非像 Parquet 方案中因 `parquet_derive` 不支持嵌套结构而被迫拆分成"每个价位一行"。已通过 `clickhouse` crate 的官方示例（`examples/data_types_derive_containers.rs`）验证：`Vec<(A, B)>` 字段可通过 `#[derive(clickhouse::Row, Serialize, Deserialize)]` 正确映射到 `Array(Tuple(A, B))` 列。

### 4.4 `bars` 表（对应 `Bar`）

```sql
CREATE TABLE bars
(
    symbol      String,
    venue       String,
    open        Decimal(38, 10),
    high        Decimal(38, 10),
    low         Decimal(38, 10),
    close       Decimal(38, 10),
    volume      Decimal(38, 10),
    interval    String,   -- "OneSecond" / "OneMinute" / "FiveMinutes" / "FifteenMinutes" / "OneHour" / "OneDay"
    open_time   DateTime64(3, 'UTC'),
    close_time  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(open_time)
ORDER BY (venue, symbol, interval, open_time);
```

`bars` 表的排序键额外包含 `interval`（因为同一 symbol 在同一时刻可能有 6 个不同 `BarInterval` 的 bar 同时存在），分区键使用 `open_time` 而非独立的 `timestamp` 字段。

---

## 五、`rust_decimal::Decimal` ↔ ClickHouse `Decimal128` 转换

**关键决策**：`clickhouse` crate 自身的 `Decimal128` 类型底层是 `fixnum::FixedPoint<i128, U12>`（`fixnum` crate 提供），并非 `rust_decimal::Decimal`，两者之间没有现成的桥接实现。调研确认这一点后（`clickhouse` crate 源码检索未发现任何 `rust_decimal` 依赖或 feature），决定：`apexes-storage` 内部依赖 `fixnum` 作为 ClickHouse 线协议的数值类型，并编写显式转换函数：

```rust
fn decimal_to_fixed(value: rust_decimal::Decimal) -> FixedPoint<i128, U12> { ... }
fn fixed_to_decimal(value: FixedPoint<i128, U12>) -> rust_decimal::Decimal { ... }
```

`fixnum` 依赖**仅限于 `apexes-storage` crate 内部**，不通过公开 API 泄漏给调用方——`StorageWriter`/`StorageReader` 对外的输入输出类型始终是 `apexes_core::MarketEvent`（内部字段为 `rust_decimal::Decimal`），转换发生在 crate 边界内部，调用方无需感知 `fixnum` 的存在。

---

## 六、API 设计

### `StorageWriter`

```rust
pub struct StorageWriter {
    client: clickhouse::Client,
}

impl StorageWriter {
    pub async fn new(url: &str) -> Result<Self, ApexesError> { ... }

    /// 写入一批事件。要求 events 中所有元素属于同一 MarketEvent variant
    /// （即同一张目标表）——若批次中混合了不同事件类型，返回错误，
    /// 不做内部拆分/分组。
    pub async fn write_batch(&self, events: &[MarketEvent]) -> Result<(), ApexesError> { ... }
}
```

**批次同质性要求**：`write_batch` 要求传入的 `events` 全部为同一 `MarketEvent` variant（全 `Trade`、全 `Tick`、全 `Depth` 或全 `Bar`），否则返回 `Err(ApexesError::...)`。调用方负责按类型预先分组——ClickHouse 的 `INSERT` 本身是廉价且可重复调用的（不像 Parquet 存在"追加已关闭文件"的限制），因此本设计**不需要**内部缓冲/批处理逻辑：调用方攒够一批直接调用 `write_batch` 即可。

### `StorageReader`

```rust
pub struct StorageReader {
    client: clickhouse::Client,
}

impl StorageReader {
    pub async fn new(url: &str) -> Result<Self, ApexesError> { ... }

    /// 查询某个 symbol/venue 在指定日期范围内、指定事件类型的全部数据，
    /// 一次性读入内存返回。
    pub async fn read_batch(
        &self,
        symbol: &str,
        venue: Venue,
        date_range: std::ops::Range<chrono::NaiveDate>,
        event_type: EventType,
    ) -> Result<Vec<MarketEvent>, ApexesError> { ... }
}

pub enum EventType {
    Trade,
    Tick,
    Depth,
    Bar,
}
```

`read_batch` 内部根据 `event_type` 选择目标表，构造 `WHERE venue = ? AND symbol = ? AND timestamp >= ? AND timestamp < ?` 查询（`bars` 表用 `open_time` 代替 `timestamp`），依赖 `ORDER BY (venue, symbol, ...)` 排序键实现高效范围扫描。

---

## 七、依赖与版本

```toml
[package]
name = "apexes-storage"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
clickhouse = { version = "0.15", features = ["chrono"] }
fixnum = { version = "0.9", features = ["i128"] }
tokio = { workspace = true }
rust_decimal = { workspace = true }
chrono = { workspace = true }
serde = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
```

---

## 八、测试计划

### 单元测试（纯逻辑，无数据库依赖）

- `rust_decimal::Decimal` ↔ `fixnum::FixedPoint` 转换的往返正确性（含边界值：0、负数、8 位小数精度）
- 各表对应的 Rust row 结构体与 `MarketEvent` variant 之间的映射构造（如 `TradeEvent` → trades 表 row 结构体）
- `write_batch` 对混合事件类型批次的拒绝逻辑（构造一个混合 `Trade`+`Tick` 的 slice，断言返回错误）

### 集成测试（标记 `#[ignore]`，需要真实 ClickHouse）

- 建表（若不存在）+ 写入一批 `Trade` 事件 + 用 `read_batch` 读回，断言与写入内容一致
- 对 `Tick`/`Depth`/`Bar` 各重复上述往返测试，`Depth` 测试需验证嵌套 `Array(Tuple(...))` 的 bids/asks 正确往返
- 跨天日期范围查询：写入两天的数据，用横跨两天的 `date_range` 查询，验证按分区正确聚合返回

---

## 九、验收标准（Definition of Done）

- `cargo build --workspace` 成功（新增 `apexes-storage` 成员）
- `cargo test --workspace`（默认，不含 `--ignored`）全部通过，且不产生任何数据库网络访问
- 在本地 `docker compose up -d` 启动 ClickHouse 后，`cargo test -p apexes-storage -- --ignored` 全部通过，验证写入-读取往返对全部 4 种 `MarketEvent` variant 均正确
- `cargo clippy --workspace -- -D warnings` 无警告
- `cargo fmt --check` 无格式问题
- `docker-compose.yml` 可通过 `docker compose up -d` 成功启动可用的 ClickHouse 实例
