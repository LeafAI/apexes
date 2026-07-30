# Apexes — 子项目 #4.4：参数优化接口（`apexes-optimize`，Grid Search + 可插拔优化器接口）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#4`（策略 SDK）的第四阶段详细设计。`#4` 按路线图 §2.2 包含五块能力：(1) Rust 策略 trait API + PyO3 绑定、(2) 因子/技术指标库、(3) 多信号组合与仓位生成、(4) 参数优化接口、(5) 策略版本管理与灰度发布。本文档覆盖 **(4)**（对应 `docs/FEATURES.md` 行 `2.4`）。(1)/(2)/(3) 均已完成（分别见 [`2026-07-30-apexes-strategy-drift-integration-design.md`](2026-07-30-apexes-strategy-drift-integration-design.md)、[`2026-07-30-apexes-indicators-design.md`](2026-07-30-apexes-indicators-design.md)、[`2026-07-30-apexes-signals-design.md`](2026-07-30-apexes-signals-design.md)）。(5) 及 (1) 的 PyO3 部分留给后续独立的 brainstorming → spec → plan 周期。
>
> 前置依赖：`apexes-strategy`（`DriftScriptStrategy` 已完成）、`apexes-backtest`（`run_backtest`/`BacktestConfig`/`BacktestReport`/`HistoricalMarketDataSource`/`InMemoryMarketDataSource` 已完成）。

---

## 一、范围

### 包含

1. **`apexes-strategy` 的小增量**：`DriftScriptStrategy` 新增 `from_file_with_params(path, overrides: HashMap<String, Decimal>)` 构造函数，配合脚本侧新增的 `param(name, default)` 原生函数绑定——脚本用 `let sma_period = param("sma_period", 20);` 声明并读取一个可调参数：有对应的 `overrides` 条目时返回该值，否则返回 `default`。
2. **新增 workspace 成员 crate：`apexes-optimize`**（依赖 `apexes-strategy` + `apexes-backtest`），提供：
   - `SearchSpace`：每个参数名到其候选值列表的映射，用于网格搜索的笛卡尔积展开。
   - `ParameterOptimizer` trait（`#[async_trait]`）：**可插拔优化算法接口**，本次只实现 `GridSearchOptimizer` 一个具体算法，但接口本身为未来的贝叶斯优化/外部 ML 服务对接（路线图 §2.2 提到的"pluggable external ML service"）预留扩展点。
   - `Evaluator` trait（`#[async_trait]`）：调用方实现的"给定一组参数，跑一次回测并返回目标分数"的回调，解耦优化算法与"如何构造策略/跑回测/计算目标函数"的具体逻辑。
   - `GridSearchOptimizer`：`ParameterOptimizer` 的具体实现，对 `SearchSpace` 做笛卡尔积展开，依次调用 `Evaluator::evaluate`，记录每组参数的分数，返回最优组合与完整结果列表。
3. **`apexes-backtest` 的小增量**：为 `HistoricalMarketDataSource` 增加一个 blanket `impl<T: HistoricalMarketDataSource> HistoricalMarketDataSource for &T`，使 `GridSearchOptimizer` 可以对同一份预加载的行情数据取引用、逐组合传给 `Evaluator`，无需每次网格点都克隆整个事件 `Vec`。

### 不包含（明确超出范围，留给对应后续子项目/后续轮次）

- 不实现贝叶斯优化（Bayesian）本身——本次只做 `ParameterOptimizer` 接口 + `GridSearchOptimizer` 一个实现；贝叶斯优化器（无论是纯 Rust 实现还是对接外部 ML 服务的适配器）留给未来单独一轮，届时只需新增一个 `ParameterOptimizer` 实现，不改动本次接口
- 不做外部 ML 服务的具体网络协议/客户端（路线图提到的"pluggable external ML service"——本次只保证 `ParameterOptimizer` trait 本身与具体优化算法解耦，足以让未来的外部服务适配器接入，不在本次实现任何网络调用代码）
- 不做参数搜索空间的自动化描述/UI（如从脚本静态分析出所有 `param()` 调用生成搜索空间）——`SearchSpace` 完全由调用方手工构造
- 不做并行/并发跑多组回测（本次网格搜索是纯顺序执行；并行化留给未来性能优化轮次）
- 不做 Rust 原生 `Strategy` trait 的参数化——本次范围明确限定为 Drift Script 策略（`DriftScriptStrategy`），Rust 原生策略的参数化留给未来评估

---

## 二、`param(name, default)` Drift Script 绑定 + `from_file_with_params`

### 脚本侧用法

```javascript
let sma_period = param("sma_period", 20);
let threshold = param("threshold", 0.2);

let sma = indicators.sma(sma_period);

function on_market_event(event, ctx) {
  // ... 使用 sma_period/threshold ...
}
```

### 语义

- `param(name, default)` 是一个原生函数：`name` 必须是字符串，`default` 必须是数字。查找 `DriftScriptStrategy` 构造时传入的 `overrides: HashMap<String, Decimal>` 是否有 `name` 对应的条目——有则返回该值（转换为 `DriftValue::Number`），否则返回 `default` 本身（原样返回，不做任何转换/校验，因为 `default` 已经是脚本字面量数字）。
- `param(...)` **不是**持久化状态的绑定（不同于 `indicators.*` 的工厂模式）——它只是构造期间的一次性查找，脚本每次调用 `param("sma_period", 20)` 都会重新查一次 `overrides`（尽管脚本通常只在顶层调用一次并存入 `let` 变量）。
- 参数覆盖仅在**脚本顶层语句求值期间**生效（`DriftScriptStrategy::from_file_with_params` 内部，`interpreter.evaluate(&stmts)` 这一步）——这与现有 `indicators.*`/`signals.*`/`ctx.*` 绑定的注册时机一致，都是在 `Interpreter::new()` 之后、脚本顶层语句求值之前完成绑定注册。

### API

```rust
impl DriftScriptStrategy {
    /// Existing constructor, unchanged: equivalent to
    /// `from_file_with_params(path, HashMap::new())` -- no parameter
    /// overrides, every `param(name, default)` call in the script
    /// resolves to its own literal `default`.
    pub fn from_file(path: &Path) -> Result<Self, StrategyError> { ... }

    /// Like `from_file`, but registers a `param(name, default)` native
    /// function that returns `overrides.get(name)` when present,
    /// otherwise `default`. Intended for parameter-optimization callers
    /// (`apexes-optimize`) that need to re-construct the same script
    /// with different parameter values across many backtest runs.
    pub fn from_file_with_params(
        path: &Path,
        overrides: HashMap<String, Decimal>,
    ) -> Result<Self, StrategyError> { ... }
}
```

### 校验失败（脚本级 `param()` 调用本身的参数错误，返回 `{error: "..."}` 对象，与既有 `indicators.*`/`signals.*` 绑定的错误约定一致）

- `name` 不是字符串 → error
- `default` 不是数字 → error
- （`overrides` 里的值本身在构造 `DriftScriptStrategy` 时由 Rust 调用方保证是合法 `Decimal`，脚本层不需要对 `overrides` 的内容做二次校验）

---

## 三、`apexes-optimize` crate 结构

```
apexes-optimize/
  Cargo.toml
  src/
    lib.rs               # pub mod {search_space, optimizer, evaluator, grid_search};
    search_space.rs       # SearchSpace + 笛卡尔积展开
    optimizer.rs          # ParameterOptimizer trait + OptimizationResult
    evaluator.rs          # Evaluator trait
    grid_search.rs        # GridSearchOptimizer（唯一具体实现）
```

`Cargo.toml` 依赖：
```toml
[package]
name = "apexes-optimize"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-strategy = { path = "../apexes-strategy" }
apexes-backtest = { path = "../apexes-backtest" }
async-trait = { workspace = true }
rust_decimal = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

---

## 四、`SearchSpace`（`search_space.rs`）

```rust
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Maps each tunable parameter's name to its list of candidate values.
/// `GridSearchOptimizer` expands this into the Cartesian product of all
/// combinations.
pub struct SearchSpace {
    parameters: HashMap<String, Vec<Decimal>>,
}

impl SearchSpace {
    pub fn new() -> Self { ... }

    /// Adds a parameter and its candidate values. Calling this again
    /// with the same `name` replaces the previous candidate list
    /// (last-write-wins, matching `HashMap::insert`'s own semantics).
    pub fn with_parameter(mut self, name: impl Into<String>, values: Vec<Decimal>) -> Self { ... }

    /// Returns every combination as a `HashMap<String, Decimal>`, one
    /// per grid point (`Vec::new()` if any parameter's candidate list is
    /// empty, or if the `SearchSpace` itself has no parameters --
    /// design decision: empty search space or empty candidate list both
    /// produce zero combinations, not one combination with fewer keys).
    pub fn combinations(&self) -> Vec<HashMap<String, Decimal>> { ... }
}
```

**笛卡尔积展开的边界情况**：若 `parameters` 为空（`SearchSpace::new()` 后没调用任何 `with_parameter`），`combinations()` 返回空 `Vec`（而不是一个空 `HashMap` 的单元素列表）——因为"没有参数可搜索"等价于"没有要评估的组合"，这个决定避免了"是否要跑一次无参数回测"的歧义。若某个参数的候选值列表为空（`with_parameter("x", vec![])`），整个笛卡尔积也是空的（标准笛卡尔积语义：任一维度为空则整体为空）。

---

## 五、`ParameterOptimizer` + `Evaluator` trait（`optimizer.rs` + `evaluator.rs`）

```rust
// evaluator.rs
use apexes_backtest::InMemoryMarketDataSource;
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Evaluates a single parameter combination and returns its objective
/// score. Implementations decide how to construct the strategy (e.g.
/// `DriftScriptStrategy::from_file_with_params`), run the backtest
/// (`apexes_backtest::run_backtest`), and compute the score from the
/// resulting `BacktestReport` (e.g. via a caller-supplied
/// `Fn(&BacktestReport) -> Decimal` objective closure) -- this trait
/// only fixes the input (`params`, `market_data`) and output (`Decimal`
/// score) shape, not any of that internal wiring.
#[async_trait]
pub trait Evaluator {
    /// Returns `Err(message)` if constructing the strategy or running
    /// the backtest failed for this parameter combination (e.g. a
    /// malformed `.drift` script, or a backtest configuration error).
    /// `GridSearchOptimizer` treats this as the worst possible score for
    /// ranking purposes, logs the error, and continues with the
    /// remaining combinations -- one bad combination must not abort the
    /// whole search.
    async fn evaluate(
        &mut self,
        params: &HashMap<String, Decimal>,
        market_data: &InMemoryMarketDataSource,
    ) -> Result<Decimal, String>;
}
```

```rust
// optimizer.rs
use crate::evaluator::Evaluator;
use crate::search_space::SearchSpace;
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// One parameter combination's evaluated result, whether it succeeded
/// or failed.
#[derive(Debug, Clone)]
pub struct EvaluatedCombination {
    pub params: HashMap<String, Decimal>,
    /// `None` if this combination's `Evaluator::evaluate` call returned
    /// `Err` -- excluded from "best" selection, but still recorded here
    /// so callers can inspect which combinations failed and why (the
    /// error message itself is logged via `tracing::error!` at the time
    /// of failure, not stored on this struct, since `EvaluatedCombination`
    /// is meant to summarize numeric results, not carry error strings
    /// through the whole result set).
    pub score: Option<Decimal>,
}

/// The full result of an optimization run: the best-scoring combination
/// (if at least one combination succeeded) plus every combination's
/// result, in the order they were evaluated.
#[derive(Debug, Clone)]
pub struct OptimizationResult {
    /// `None` only if every single combination failed (or the search
    /// space was empty) -- callers must handle this case explicitly
    /// rather than assuming a best combination always exists.
    pub best: Option<EvaluatedCombination>,
    pub all_results: Vec<EvaluatedCombination>,
}

/// A pluggable parameter-search algorithm. `GridSearchOptimizer` is the
/// only implementation in this pass; this trait boundary exists so a
/// future Bayesian optimizer (in-process or backed by an external ML
/// service, per the project roadmap's "pluggable external ML service"
/// note) can be added later without changing `Evaluator` or any
/// existing caller code -- only a new `ParameterOptimizer` impl.
#[async_trait]
pub trait ParameterOptimizer {
    async fn optimize(
        &self,
        search_space: &SearchSpace,
        evaluator: &mut dyn Evaluator,
    ) -> OptimizationResult;
}
```

---

## 六、`GridSearchOptimizer`（`grid_search.rs`）

```rust
use apexes_backtest::InMemoryMarketDataSource;
use apexes_core::MarketEvent;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::evaluator::Evaluator;
use crate::optimizer::{EvaluatedCombination, OptimizationResult, ParameterOptimizer};
use crate::search_space::SearchSpace;

/// Exhaustively evaluates every combination in a `SearchSpace`'s
/// Cartesian product, sequentially, against a single pre-loaded set of
/// market events shared across all combinations (loaded once by the
/// caller, not re-fetched per combination).
pub struct GridSearchOptimizer {
    market_data: InMemoryMarketDataSource,
}

impl GridSearchOptimizer {
    pub fn new(events: Vec<MarketEvent>) -> Self {
        Self { market_data: InMemoryMarketDataSource::new(events) }
    }
}

#[async_trait]
impl ParameterOptimizer for GridSearchOptimizer {
    async fn optimize(
        &self,
        search_space: &SearchSpace,
        evaluator: &mut dyn Evaluator,
    ) -> OptimizationResult {
        let mut all_results = Vec::new();
        let mut best: Option<EvaluatedCombination> = None;

        for params in search_space.combinations() {
            let score = match evaluator.evaluate(&params, &self.market_data).await {
                Ok(score) => Some(score),
                Err(message) => {
                    tracing::error!("grid search combination {params:?} failed: {message}");
                    None
                }
            };

            let is_new_best = match (score, best.as_ref().and_then(|b| b.score)) {
                (Some(candidate), Some(current_best)) => candidate > current_best,
                (Some(_), None) => true,
                (None, _) => false,
            };

            let combination = EvaluatedCombination { params, score };
            if is_new_best {
                best = Some(combination.clone());
            }
            all_results.push(combination);
        }

        OptimizationResult { best, all_results }
    }
}
```

**为什么 `GridSearchOptimizer` 拥有 `InMemoryMarketDataSource`（而不是每次调用 `evaluate` 时构造一次）**：用户已明确决定——同一份行情数据在整个网格搜索期间不变，只有策略参数在变，提前一次性加载并在 `GridSearchOptimizer` 内部持有，避免每个网格点重复触发 ClickHouse 读取或重复克隆整个事件 `Vec`。`Evaluator::evaluate` 接收 `&InMemoryMarketDataSource`（借用），配合 §七的 blanket `impl<T: HistoricalMarketDataSource> HistoricalMarketDataSource for &T`，`Evaluator` 的实现可以直接把这个引用传给 `apexes_backtest::run_backtest`，无需克隆。

---

## 七、`apexes-backtest` 的 blanket 引用实现

在 `crates/apexes-backtest/src/market_data_source.rs` 新增：

```rust
impl<T: HistoricalMarketDataSource + Sync> HistoricalMarketDataSource for &T {
    async fn events(&self) -> Result<Vec<MarketEvent>, ApexesError> {
        (**self).events().await
    }
}
```

这样 `run_backtest(config, &market_data, strategy)`（`market_data: &InMemoryMarketDataSource`）可以直接编译通过，`run_backtest` 内部对 `market_data.events()` 的调用透明地转发到被引用对象的 `events()` 实现，不引入任何克隆。

---

## 八、测试策略

1. **`param()` 绑定单元测试**（`apexes-strategy`）：`from_file_with_params` 传入的 override 生效（脚本读到 override 值而非 default）、未提供 override 时脚本读到 default 值、`param()` 参数类型错误（`name` 非字符串/`default` 非数字）返回 error 对象。
2. **`SearchSpace` 单元测试**（`apexes-optimize`）：单参数笛卡尔积、多参数笛卡尔积（验证组合数 = 各参数候选值数量之积，且每个组合内容正确）、空 `SearchSpace` 返回空组合列表、某参数候选值为空列表时整体返回空组合列表。
3. **`GridSearchOptimizer` 单元测试**（用一个不依赖真实回测的 mock `Evaluator`，直接返回参数派生的确定性分数，如 `params["x"]`）：验证网格搜索正确找到最高分组合、验证 `all_results` 包含每个组合且顺序与 `SearchSpace::combinations()` 一致、验证某个组合 `Evaluator::evaluate` 返回 `Err` 时不影响其余组合的评估、不被误选为 best、且该组合的 `score` 字段为 `None`、验证全部组合都失败时 `best` 为 `None`。
4. **端到端集成测试**（真实 `.drift` 脚本 + `run_backtest` + `GridSearchOptimizer`）：一个使用 `param("threshold", ...)` 的 `.drift` fixture，`Evaluator` 实现内部调用 `DriftScriptStrategy::from_file_with_params` + `run_backtest` + 一个简单目标函数（如 `report.overall.trade_count` 转 `Decimal`，避免依赖 Sharpe 等需要足够数据点的复杂指标），验证网格搜索能跑通全流程并选出预期的最优参数组合（用手工构造的确定性行情数据，使某个特定参数组合产生可预测的最多/最少交易次数）。
5. **全工作区验证**：`cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --check`。

---

## 九、非目标（Non-goals，本次明确不做）

- 不实现贝叶斯优化本身（只做接口预留）
- 不做外部 ML 服务网络协议/客户端
- 不做参数搜索空间自动化描述/UI
- 不做并行/并发回测执行
- 不做 Rust 原生 `Strategy` trait 的参数化（仅限 `DriftScriptStrategy`）
