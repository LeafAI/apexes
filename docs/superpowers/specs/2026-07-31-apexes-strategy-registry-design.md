# Apexes — 子项目 #4.5：策略版本管理与灰度发布（`apexes-strategy-registry`）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#4`（策略 SDK）的第五阶段、也是最后一个已排期阶段的详细设计。`#4` 按路线图 §2.2 包含五块能力：(1) Rust 策略 trait API + PyO3 绑定、(2) 因子/技术指标库、(3) 多信号组合与仓位生成、(4) 参数优化接口、(5) 策略版本管理与灰度发布。本文档覆盖 **(5)**（对应 `docs/FEATURES.md` 行 `2.5`）。(1)/(2)/(3)/(4) 均已完成（分别见 [`2026-07-30-apexes-strategy-drift-integration-design.md`](2026-07-30-apexes-strategy-drift-integration-design.md)、[`2026-07-30-apexes-indicators-design.md`](2026-07-30-apexes-indicators-design.md)、[`2026-07-30-apexes-signals-design.md`](2026-07-30-apexes-signals-design.md)、[`2026-07-30-apexes-optimize-design.md`](2026-07-30-apexes-optimize-design.md)）。(1) 的 PyO3 部分仍留给未来独立评估，不在本文档范围内。
>
> 前置依赖：`apexes-strategy`（`DriftScriptStrategy::from_file`/`from_file_with_params` 已完成）、`apexes-storage`（ClickHouse 客户端封装模式已建立：`clickhouse` crate + `CkDecimal` 转换 + `StorageError` 风格）、`apexes-backtest`（`run_backtest`/`BacktestConfig`/`BacktestReport`/`InMemoryMarketDataSource` 已完成）、`apexes-optimize`（`Evaluator` trait 已完成）。

---

## 一、范围

### 背景与关键 scope 决策

路线图原文把这一阶段描述为"策略版本管理**与灰度发布**"（version management & canary release）。"灰度发布"在完整语义下通常指把新版本投入部分真实/模拟资金流量、与旧版本并行运行对比——但这依赖子项目 `#6`（模拟交易）/`#7`（OMS）/`#8`（实盘网关）提供的执行环境，而这三者均未开始。

**决策**：本轮范围拆分为两部分都做，但"灰度发布"部分是**回测语境下的简化版**——用同一份历史行情数据，对多个策略版本分别跑回测、产出并列对比结果，不涉及真实资金/流量分配、不等待真实执行环境。真正意义上的"生产流量灰度"留给 `#6`/`#7` 完成后再重新评估。

### 包含

1. **新增 workspace 成员 crate：`apexes-strategy-registry`**（依赖 `apexes-strategy` + `apexes-storage` + `apexes-optimize` + `apexes-backtest`），提供：
   - **`VersionStore`**：纯存储 CRUD，把 `.drift` 脚本源码连同元数据以版本形式持久化到 ClickHouse，并维护每个策略名下的"当前版本指针"。
   - **`CanaryComparator`**：对比逻辑，给定一个策略名和若干版本号，对同一份历史行情数据分别跑回测评分，返回并列结果；可选地自动采纳最优版本并更新指针。
2. **ClickHouse 两张新表**（DDL 由 `VersionStore::new` 负责 `CREATE TABLE IF NOT EXISTS`，与 `apexes-storage::StorageWriter::new` 的既有模式一致）：
   - `strategy_versions`（`MergeTree`，追加型，永不改写/删除）：每个策略每次 `publish` 产生一行，记录完整脚本源码 + 元数据。
   - `strategy_current_version`（`ReplacingMergeTree`，按写入时间去重取最新）：每个策略名对应"当前生效版本号"这一可变状态，通过插入新行 + 查询时自动去重取最新实现"更新指针"语义，不引入 ClickHouse 之外的新存储后端。
3. **版本号语义**：策略以用户自定义的字符串名字标识（如 `"threshold_strategy"`）；版本号在同名策略下从 1 开始自动递增，由 `VersionStore::publish` 内部查询当前最大版本号 + 1 决定，调用方不能手动指定版本号。
4. **每个版本的持久化内容**：完整 `.drift` 脚本源码文本（自包含，回滚时直接读出写入临时文件即可执行，不依赖外部文件系统/git 状态）、内容哈希（SHA-256，用于完整性校验，不用于去重强制约束）、可选的参数快照（`HashMap<String, Decimal>`，JSON 编码存储，供 `apexes-optimize` 调优产出的版本记录其 `threshold` 等参数取值）、用户自由文本描述/备注、创建时间。
5. **回滚**：`VersionStore::set_current(strategy_name, version)` 把"当前指针"写一条新记录指向历史版本号——底层机制与"发布新版本"完全相同（都是 `strategy_current_version` 表的一次插入），语义上的区别只在于指向的 `version` 是否小于当前值。
6. **灰度对比（简化版）**：`CanaryComparator::compare` 接受一个策略名、一组要对比的版本号列表、一份历史行情数据（`InMemoryMarketDataSource`）、以及一个 Evaluator 工厂闭包（`Fn(&str) -> Box<dyn Evaluator>`，`&str` 是该版本的脚本源码），对每个版本现场构造一个新 `Evaluator` 实例并调用其 `evaluate`，返回 `Vec<(u32, Result<Decimal, String>)>`（版本号 + 该版本的评分结果或错误），不自动选择、不修改指针。`compare_and_promote` 是 `compare` 的一层薄封装：额外找出评分最高的版本并调用 `set_current` 更新指针，调用方通过选择调这个方法还是 `compare` 来决定是否要自动采纳。

### 不包含（明确超出范围，留给对应后续子项目/后续轮次）

- **不做真正的生产流量灰度发布**（把一部分真实/模拟订单流量分配给新版本、观察真实执行表现）——依赖 `#6`（模拟交易）/`#7`（OMS）/`#8`（实盘网关）提供的执行环境，均未开始；本轮的"灰度对比"完全在回测语境下进行。
- **不做自动化的版本晋升策略/调度**（如"新版本连续 N 天回测跑赢就自动晋升"）——`compare_and_promote` 只是一次性、由调用方显式触发的对比+晋升，不是持续运行的策略。
- **不做版本 diff / 可视化**——`list_versions` 返回的元数据里不包含脚本内容的 diff 计算，调用方如需对比源码差异需自行读取两个版本的 `script_source` 字段并处理。
- **不做权限控制/审批流程**（谁可以 publish/rollback）——留给未来子项目 `#15`（安全与合规，RBAC）。
- **不做删除历史版本的能力**——`strategy_versions` 是纯追加表，版本历史永久保留（符合"审计/回溯不可篡改"的项目原则，见 vision 文档 §2.7 全操作审计日志不可篡改）。
- **不做 Rust 原生 `Strategy` trait 的版本管理**——本次范围明确限定为 `.drift` 脚本策略（`DriftScriptStrategy`），与 `apexes-optimize` 的既有范围限定一致。
- **不引入 ClickHouse 之外的新存储后端**——不提前引入 PostgreSQL（原计划留给 `#7` OMS 需要持久化订单/账户状态时再引入）。

---

## 二、Crate 结构

```
crates/apexes-strategy-registry/
├── Cargo.toml
├── src/
│   ├── lib.rs           // pub mod 声明 + re-exports
│   ├── error.rs         // RegistryError
│   ├── schema.rs        // 两张表的 DDL + Row 结构体（沿用 apexes-storage::schema 风格）
│   ├── version_store.rs // VersionStore
│   └── comparator.rs    // CanaryComparator
└── tests/
    └── live_clickhouse.rs  // #[ignore] 集成测试，沿用 apexes-storage 既有模式
```

`Cargo.toml` 依赖：`apexes-strategy`、`apexes-storage`（复用 `CkDecimal`/`decimal_to_fixed`/`fixed_to_decimal` 转换函数，避免重复实现）、`apexes-optimize`（复用 `Evaluator` trait）、`apexes-backtest`（`InMemoryMarketDataSource`）、`clickhouse`、`chrono`、`serde`、`serde_json`（参数快照 JSON 编码）、`rust_decimal`、`sha2`（内容哈希）、`tokio`、`tracing`、`thiserror`、`async-trait`。

---

## 三、`schema.rs`：ClickHouse 表结构

```rust
pub const STRATEGY_VERSIONS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS strategy_versions
(
    strategy_name   String,
    version         UInt32,
    script_source   String,
    content_hash    String,
    params          Nullable(String),
    description     Nullable(String),
    created_at      DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY strategy_name
ORDER BY (strategy_name, version)
";

pub const STRATEGY_CURRENT_VERSION_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS strategy_current_version
(
    strategy_name    String,
    current_version  UInt32,
    updated_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY strategy_name
";
```

**`params` 字段**：`Nullable(String)`，存 `serde_json::to_string(&HashMap<String, Decimal>)` 的结果；`None`（SQL `NULL`）表示该版本未附带参数快照（例如手工编写的脚本，而非 `apexes-optimize` 调优产出）。`Decimal` 在 JSON 里以字符串形式序列化（`rust_decimal` 的 `serde` feature 默认行为），避免浮点精度问题。

**`content_hash` 字段**：`sha2::Sha256` 对 `script_source` 的 UTF-8 字节计算摘要，十六进制字符串存储。用途是让调用方能快速判断"这次要发布的脚本内容是否与已有某个版本完全相同"（`VersionStore` 不会自动拒绝重复内容——是否要去重是调用方的决定，`content_hash` 只是暴露判断依据）。

**`strategy_current_version` 用 `ReplacingMergeTree(updated_at)`**：每次 `set_current` 调用都是一次 `INSERT`（新增一行，`updated_at = now()`），从不 `UPDATE`/`DELETE`。查询"当前版本"时用 `SELECT current_version FROM strategy_current_version FINAL WHERE strategy_name = ? ORDER BY updated_at DESC LIMIT 1`——`FINAL` 关键字强制 ClickHouse 在查询时立即合并去重（而不是等待后台异步合并），保证读到的一定是最新一次 `set_current`/首次 `publish` 时设置的指针值,即使还没有触发过后台 merge。

**对应的 Row 结构体**（`#[derive(Row, Serialize, Deserialize)]`，沿用 `apexes-storage::schema` 的模式）：

```rust
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct VersionRow {
    pub strategy_name: String,
    pub version: u32,
    pub script_source: String,
    pub content_hash: String,
    pub params: Option<String>,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct CurrentVersionRow {
    pub strategy_name: String,
    pub current_version: u32,
    pub updated_at: DateTime<Utc>,
}
```

---

## 四、`VersionStore`

```rust
pub struct VersionStore {
    client: clickhouse::Client,
}

impl VersionStore {
    /// Connects to ClickHouse at `url` and ensures both tables exist
    /// (CREATE TABLE IF NOT EXISTS), matching
    /// `apexes_storage::StorageWriter::new`'s established pattern.
    pub async fn new(url: &str) -> Result<Self, RegistryError>;

    /// Publishes a new version of `strategy_name`. Queries the current
    /// max version for this strategy name (`SELECT max(version) FROM
    /// strategy_versions WHERE strategy_name = ?`, treating "no rows"
    /// as 0), inserts a new row with `version = max + 1`, computes
    /// `content_hash` internally via SHA-256. If this is the strategy's
    /// first-ever version (max was 0 / no prior rows), ALSO inserts a
    /// `strategy_current_version` row pointing at the new version 1 --
    /// a strategy is never left with zero current-version rows once at
    /// least one version has been published. Returns the newly assigned
    /// version number.
    pub async fn publish(
        &self,
        strategy_name: &str,
        script_source: &str,
        params: Option<HashMap<String, Decimal>>,
        description: Option<&str>,
    ) -> Result<u32, RegistryError>;

    /// Reads back a single version's full record. Returns
    /// `Err(RegistryError::VersionNotFound { strategy_name, version })`
    /// if no such row exists.
    pub async fn get_version(
        &self,
        strategy_name: &str,
        version: u32,
    ) -> Result<VersionRecord, RegistryError>;

    /// Lists every version of `strategy_name`, ordered by version
    /// ascending. Returns an empty `Vec` (not an error) if the strategy
    /// name has never been published -- an empty list is a valid,
    /// unambiguous answer to "what versions exist", unlike
    /// `get_version`'s single-record lookup where a specific requested
    /// version not existing IS an error condition.
    pub async fn list_versions(
        &self,
        strategy_name: &str,
    ) -> Result<Vec<VersionRecord>, RegistryError>;

    /// Returns the current version pointer for `strategy_name`, or
    /// `None` if the strategy has never been published (no
    /// `strategy_current_version` row exists yet).
    pub async fn current_version(
        &self,
        strategy_name: &str,
    ) -> Result<Option<u32>, RegistryError>;

    /// Points `strategy_name`'s current-version pointer at `version`
    /// (rollback when `version` < the existing current version, or a
    /// forward move otherwise -- the operation is identical either
    /// way). Returns
    /// `Err(RegistryError::VersionNotFound { strategy_name, version })`
    /// if `version` does not exist in `strategy_versions` -- the
    /// pointer is never allowed to reference a non-existent version.
    pub async fn set_current(
        &self,
        strategy_name: &str,
        version: u32,
    ) -> Result<(), RegistryError>;
}

/// One version's full record, as returned by `get_version`/`list_versions`.
#[derive(Debug, Clone, PartialEq)]
pub struct VersionRecord {
    pub strategy_name: String,
    pub version: u32,
    pub script_source: String,
    pub content_hash: String,
    pub params: Option<HashMap<String, Decimal>>,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
}
```

**`publish` 的版本号分配是否存在竞态**：两个并发 `publish` 调用可能读到相同的 `max(version)`，都算出同一个"新版本号"，各自插入一行——ClickHouse 不提供跨行的唯一约束/事务隔离保证来阻止这种情况。这与项目当前的单进程/单调用方使用场景相符（`VersionStore` 目前没有多进程并发发布同一策略的需求），本设计**明确不处理这一竞态**，留给未来如果真的出现多写者场景时再引入协调机制（如借助 `strategy_current_version` 表的插入时间做乐观锁，或前置一个真正支持事务的存储）。这一限制记录在 `VersionStore::publish` 的文档注释中，不是遗漏。

---

## 五、`CanaryComparator`

```rust
pub struct CanaryComparator<'a> {
    version_store: &'a VersionStore,
}

impl<'a> CanaryComparator<'a> {
    pub fn new(version_store: &'a VersionStore) -> Self;

    /// Evaluates each of `versions` against the same `market_data`,
    /// constructing a fresh `Evaluator` per version via
    /// `evaluator_factory(script_source)`. Reads each version's
    /// `script_source` from `self.version_store` first (returns
    /// `Err(RegistryError::VersionNotFound { .. })` immediately, before
    /// evaluating anything, if any requested version doesn't exist --
    /// fails closed rather than silently skipping missing versions).
    /// Each version's `Evaluator::evaluate` call uses that version's
    /// own `params` (from its stored `VersionRecord`, defaulting to an
    /// empty `HashMap` if `None`) as the `params` argument -- NOT a
    /// caller-supplied shared params map, since different versions may
    /// have been published with different parameter snapshots.
    /// Per-version evaluation failures (the `Evaluator::evaluate`
    /// `Err` case) are recorded as `Err(message)` in the returned Vec's
    /// corresponding slot, not propagated as a whole-comparison
    /// failure -- one broken version must not prevent seeing the other
    /// versions' scores, mirroring `GridSearchOptimizer`'s
    /// per-combination-failure semantics.
    pub async fn compare(
        &self,
        strategy_name: &str,
        versions: &[u32],
        market_data: &InMemoryMarketDataSource,
        evaluator_factory: impl Fn(&str) -> Box<dyn Evaluator>,
    ) -> Result<Vec<(u32, Result<Decimal, String>)>, RegistryError>;

    /// Runs `compare`, then finds the version with the highest `Ok`
    /// score (versions with `Err` results are excluded from
    /// selection, same exclusion semantics as
    /// `apexes_optimize::GridSearchOptimizer`'s failed-combination
    /// handling) and calls `self.version_store.set_current(strategy_name,
    /// winning_version)`. Returns the full comparison Vec (so the
    /// caller can see every version's result, not just the winner) plus
    /// the promoted version number. Returns
    /// `Err(RegistryError::NoSuccessfulVersion { strategy_name })` if
    /// every version in `versions` failed to evaluate -- the pointer is
    /// left unchanged in this case (never promotes a version that
    /// itself errored).
    pub async fn compare_and_promote(
        &self,
        strategy_name: &str,
        versions: &[u32],
        market_data: &InMemoryMarketDataSource,
        evaluator_factory: impl Fn(&str) -> Box<dyn Evaluator>,
    ) -> Result<(Vec<(u32, Result<Decimal, String>)>, u32), RegistryError>;
}
```

**为什么 `evaluator_factory` 是 `Fn(&str) -> Box<dyn Evaluator>` 而不是复用 `apexes_optimize::Evaluator` 的调用约定原样传入一个 Evaluator 实例**：`apexes-optimize::Evaluator::evaluate(&mut self, params, market_data)` 假设脚本路径/内容在 `Evaluator` 实现内部固定不变（同一脚本、不同参数是它的设计前提，见 `apexes-optimize` 设计文档 §一）。但版本对比场景下每个版本的脚本**内容本身**不同，`Evaluator` 实现内部通常需要先把 `script_source` 写入一个临时文件再调用 `DriftScriptStrategy::from_file_with_params`（因为 `from_file_with_params` 接受路径而非源码字符串）。让调用方提供一个工厂闭包，由 `CanaryComparator` 对每个版本现场调用 `evaluator_factory(&version.script_source)` 构造一个新 `Evaluator` 实例，把"如何把源码字符串变成可执行 Evaluator"这一步的实现细节（临时文件命名、清理时机等）留给调用方——`CanaryComparator` 本身不假设脚本必须先落盘。

**`Box<dyn Evaluator>` 而非 `&mut dyn Evaluator`**：`apexes-optimize` 的 `ParameterOptimizer::optimize` 接受调用方已经构造好的单个 `&mut dyn Evaluator`（一次优化跑内所有组合共享同一个 Evaluator 实例）。这里语义不同：每个版本需要一个**新的** Evaluator 实例（因为每个版本的脚本内容不同，不能像"同一脚本不同参数"那样复用同一个 Evaluator 内部状态），所以工厂闭包返回一个拥有所有权的新实例，`compare` 内部用完即丢弃。

---

## 六、错误类型

```rust
#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("failed to connect to ClickHouse: {reason}")]
    ConnectionFailed { reason: String },

    #[error("ClickHouse query failed: {reason}")]
    QueryFailed { reason: String },

    #[error("strategy '{strategy_name}' has no version {version}")]
    VersionNotFound { strategy_name: String, version: u32 },

    #[error("failed to serialize/deserialize params snapshot: {reason}")]
    ParamsSerializationFailed { reason: String },

    #[error("failed to convert Decimal to ClickHouse FixedPoint: {reason}")]
    DecimalConversionFailed { reason: String },

    #[error("no version among {versions:?} for strategy '{strategy_name}' evaluated successfully")]
    NoSuccessfulVersion { strategy_name: String, versions: Vec<u32> },
}

impl From<clickhouse::error::Error> for RegistryError {
    fn from(error: clickhouse::error::Error) -> Self {
        RegistryError::QueryFailed { reason: error.to_string() }
    }
}
```

沿用 `apexes-storage::StorageError` 的错误分类风格（连接失败 / 查询失败 / 特定领域校验失败），新增本 crate 特有的 `VersionNotFound`/`ParamsSerializationFailed`/`NoSuccessfulVersion`。`params` 字段的 `Decimal` 转换复用 `apexes_storage::conversion::{decimal_to_fixed, fixed_to_decimal}`（该函数本身与 ClickHouse 列类型转换相关，但这里 `params` 走的是 JSON 字符串序列化而非 ClickHouse 原生 `Decimal(38,10)` 列——`DecimalConversionFailed` 变体保留是为了未来若改为原生列类型存储做准备，本轮实际触发路径是 `serde_json::to_string`/`from_str` 失败时的 `ParamsSerializationFailed`）。

---

## 七、测试策略

1. **`schema.rs`／`VersionRow`／`CurrentVersionRow` 序列化单元测试**：验证 `params: Option<String>` 字段在 `None`/`Some(json)` 两种情况下的往返正确性。
2. **`VersionStore` 单元测试**（不依赖真实 ClickHouse——具体如何隔离数据库依赖留给实现计划阶段决定：可以是一个内存 fake 实现相同方法签名的 trait,或直接把 `VersionStore` 的纯逻辑部分（版本号分配算法、SQL 拼装）拆成不依赖真实连接即可测试的辅助函数）：
   - `publish` 首次发布返回版本号 1，且自动设置当前指针为 1。
   - `publish` 同一策略名连续多次，版本号依次递增（2, 3, 4...）。
   - 不同策略名的版本号互不影响（各自从 1 开始）。
   - `get_version` 请求不存在的版本返回 `VersionNotFound`。
   - `list_versions` 对未发布过的策略名返回空 `Vec`（不是错误）。
   - `set_current` 指向不存在的版本返回 `VersionNotFound`，指针不变。
   - `set_current` 指向一个版本号小于当前值的历史版本（模拟回滚）成功后，`current_version` 返回该历史版本号。
3. **`CanaryComparator` 单元测试**（用一个不依赖真实回测的 mock `Evaluator`，模式与 `apexes-optimize::GridSearchOptimizer` 单元测试的 `MockEvaluator` 一致：分数从参数或脚本内容确定性派生）：
   - `compare` 对多个版本分别返回对应分数，顺序与传入的 `versions` 参数一致。
   - `compare` 中某个版本的 `Evaluator::evaluate` 返回 `Err` 时，该版本在结果 `Vec` 中对应 `Err(message)`，其余版本正常返回 `Ok`，不中断整体比较。
   - `compare` 请求一个不存在的版本号时，在开始评估任何版本之前就返回 `VersionNotFound`（fail-closed，不做部分评估）。
   - `compare_and_promote` 正确选出最高分版本并调用 `set_current`，返回值包含完整对比结果 + 晋升的版本号。
   - `compare_and_promote` 所有版本都失败时返回 `NoSuccessfulVersion`，指针保持不变（用 `current_version` 查询验证晋升前后指针值相同）。
   - `compare`/`compare_and_promote` 使用的 `params` 是每个版本自己存储的参数快照,而非跨版本共享的同一份——用两个存了不同 `params` 的版本 + 一个读取 `params["threshold"]` 来决定行为的 mock Evaluator 验证。
4. **端到端集成测试**（`#[ignore]`，需要真实 ClickHouse，沿用 `apexes-storage::tests::live_clickhouse` 模式）：`publish` 两个版本的真实 `.drift` 脚本（可复用 `apexes-optimize` 测试 fixture 里的 `threshold_strategy.drift` 思路）、`list_versions` 验证顺序与内容、`compare` 用真实 `DriftScriptStrategy::from_file_with_params` + `run_backtest` 构造的 `Evaluator`（脚本源码先写入临时文件）对比两个版本、验证 `compare_and_promote` 后 `current_version` 确实更新为预期版本、`set_current` 回滚后再次查询验证指针正确切回。
5. **全工作区验证**：`cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --check`。

---

## 八、非目标（Non-goals，本次明确不做）

- 不做真正的生产流量灰度发布（依赖 `#6`/`#7`/`#8`，见 §一背景说明）
- 不做自动化/持续运行的版本晋升调度
- 不做版本 diff / 可视化
- 不做权限控制/审批流程（留给 `#15`）
- 不做删除历史版本的能力（`strategy_versions` 永久追加保留）
- 不做 Rust 原生 `Strategy` trait 的版本管理（仅限 `DriftScriptStrategy`）
- 不引入 ClickHouse 之外的新存储后端（不提前引入 PostgreSQL）
- 不处理 `publish` 并发调用下的版本号分配竞态（见 §四说明，记录为已知限制而非遗漏）
