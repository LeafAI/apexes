# Apexes — 子项目 #4.4：参数优化接口（`apexes-optimize`，Grid Search + 可插拔优化器接口）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#4`（策略 SDK）的第四阶段详细设计。`#4` 按路线图 §2.2 包含五块能力：(1) Rust 策略 trait API + PyO3 绑定、(2) 因子/技术指标库、(3) 多信号组合与仓位生成、(4) 参数优化接口、(5) 策略版本管理与灰度发布。本文档覆盖 **(4)**（对应 `docs/FEATURES.md` 行 `2.4`）。(1)/(2)/(3) 均已完成（分别见 [`2026-07-30-apexes-strategy-drift-integration-design.md`](2026-07-30-apexes-strategy-drift-integration-design.md)、[`2026-07-30-apexes-indicators-design.md`](2026-07-30-apexes-indicators-design.md)、[`2026-07-30-apexes-signals-design.md`](2026-07-30-apexes-signals-design.md)）。(5) 及 (1) 的 PyO3 部分留给后续独立的 brainstorming → spec → plan 周期。
>
> 前置依赖：`apexes-strategy`（`DriftScriptStrategy` 已完成）、`apexes-backtest`（`run_backtest`/`BacktestConfig`/`BacktestReport`/`HistoricalMarketDataSource`/`InMemoryMarketDataSource` 已完成）。

---

## 一、范围

### 包含

1. **`apexes-strategy` 的小增量**：`DriftScriptStrategy` 新增 `from_file_with_params(path, overrides: HashMap<String, Decimal>)` 构造函数，配合脚本侧新增的 `param(name, default)` 原生函数绑定——脚本用 `let sma_period = param("sma_period", 20);` 声明并读取一个可调参数：有对应的 `overrides` 条目时返回该值，否则返回 `default`。
2. **新增 workspace 成员 crate：`apexes-optimize`**（依赖 `apexes-strategy` + `apexes-backtest`），提供：
   - `SearchSpace`：每个参数名到其候选值列表的映射，用于网格搜索的笛卡尔积展开，同时也是传给外部优化器进程的初始搜索空间描述。
   - `ParameterOptimizer` trait（`#[async_trait]`）：**可插拔优化算法接口**，本次提供两个具体实现——`GridSearchOptimizer`（进程内穷举）和 `ExternalProcessOptimizer`（通过子进程 + JSON-over-stdio 协议对接外部优化算法，见 §六），对应路线图 §2.2 "pluggable external ML service" 的字面要求。
   - `Evaluator` trait（`#[async_trait]`）：调用方实现的"给定一组参数，跑一次回测并返回目标分数"的回调，解耦优化算法与"如何构造策略/跑回测/计算目标函数"的具体逻辑，两个 `ParameterOptimizer` 实现共用同一个 `Evaluator` 接口。
   - `GridSearchOptimizer`：对 `SearchSpace` 做笛卡尔积展开，依次调用 `Evaluator::evaluate`，记录每组参数的分数，返回最优组合与完整结果列表。
   - `ExternalProcessOptimizer`：启动一个外部子进程（任意语言实现，典型场景是 Python + scikit-optimize 等贝叶斯优化库），通过换行分隔 JSON（newline-delimited JSON）双向通信——宿主发送搜索空间，外部进程自主决定评估顺序/次数，宿主逐一调用 `Evaluator::evaluate` 并把分数传回，外部进程最终宣布最优结果（见 §六）。
3. **`apexes-backtest` 的小增量**：为 `HistoricalMarketDataSource` 增加一个 blanket `impl<T: HistoricalMarketDataSource> HistoricalMarketDataSource for &T`，使两个 `ParameterOptimizer` 实现都可以对同一份预加载的行情数据取引用、逐组合传给 `Evaluator`，无需每次网格点/评估请求都克隆整个事件 `Vec`。

### 不包含（明确超出范围，留给对应后续子项目/后续轮次）

- **不做原生动态库（`.so`/`.dylib`）插件加载**——已明确排除：Rust 没有稳定 ABI，`dyn ParameterOptimizer`/`dyn Evaluator` trait object 无法安全跨动态库边界传递，宿主与插件必须用完全相同的 rustc 版本编译，且 async 方法（`optimize`/`evaluate`）本身也无法以 FFI-safe 的方式跨边界——这需要引入 `unsafe` 代码并违反项目"unsafe 需要明确批准"的原则。改用 §六 的进程 + JSON-over-stdio 方案，零 `unsafe`，且与路线图"external ML service"的字面表述吻合（外部服务本来就该是独立进程，不该是原地加载进宿主进程地址空间的动态库）。
- 不实现贝叶斯优化算法本身（无论是 Rust 内部实现还是 `ExternalProcessOptimizer` 协议另一端的外部进程要跑什么算法）——本次只提供 `ExternalProcessOptimizer` 这个协议适配器 + 一个供集成测试使用的 mock 外部进程（用简单确定性逻辑模拟"外部优化器"的行为，不是真正的贝叶斯优化实现）
- 不做参数搜索空间的自动化描述/UI（如从脚本静态分析出所有 `param()` 调用生成搜索空间）——`SearchSpace` 完全由调用方手工构造
- 不做并行/并发跑多组回测（`GridSearchOptimizer` 是纯顺序执行；`ExternalProcessOptimizer` 的评估节奏由外部进程决定，但宿主侧对每个 `evaluate` 请求仍是逐一顺序处理，不并发跑多个回测）——并行化留给未来性能优化轮次
- 不做 Rust 原生 `Strategy` trait 的参数化——本次范围明确限定为 Drift Script 策略（`DriftScriptStrategy`），Rust 原生策略的参数化留给未来评估
- 不做除 stdin/stdout 以外的进程间通信方式（如共享内存、Unix domain socket）——newline-delimited JSON over stdio 足够满足"外部优化器进程"这个场景，且实现最简单

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
    lib.rs               # pub mod {search_space, optimizer, evaluator, grid_search, external_process};
    search_space.rs       # SearchSpace + 笛卡尔积展开
    optimizer.rs          # ParameterOptimizer trait + OptimizationResult + EvaluatedCombination
    evaluator.rs          # Evaluator trait
    grid_search.rs        # GridSearchOptimizer
    external_process.rs   # ExternalProcessOptimizer + newline-delimited JSON 协议消息类型
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
serde = { workspace = true }
serde_json = { workspace = true }
tokio = { workspace = true, features = ["process", "io-util", "macros", "rt-multi-thread"] }
tracing = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
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
    /// `Some(message)` only for a run-level (not per-combination)
    /// failure -- e.g. `ExternalProcessOptimizer`'s subprocess crashed,
    /// exited unexpectedly, or sent malformed/unrecognized protocol
    /// messages. `GridSearchOptimizer` never sets this field (it has no
    /// such failure mode: individual combination failures are recorded
    /// per-`EvaluatedCombination` via `score: None`, not here). Distinct
    /// from per-combination failures: this field describes something
    /// that invalidates the ENTIRE run, not just one parameter
    /// combination's result.
    pub error: Option<String>,
}

/// A pluggable parameter-search algorithm. Two implementations exist:
/// `GridSearchOptimizer` (in-process exhaustive search) and
/// `ExternalProcessOptimizer` (delegates to an external subprocess over
/// a JSON-over-stdio protocol, per the project roadmap's "pluggable
/// external ML service" requirement -- see `external_process.rs`, §六).
/// This trait boundary is what lets both share the same `Evaluator` and
/// caller code; a future third implementation (e.g. a different search
/// algorithm) only needs to implement this trait, no other code changes.
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

        OptimizationResult { best, all_results, error: None }
    }
}
```

**为什么 `GridSearchOptimizer` 拥有 `InMemoryMarketDataSource`（而不是每次调用 `evaluate` 时构造一次）**：用户已明确决定——同一份行情数据在整个网格搜索期间不变，只有策略参数在变，提前一次性加载并在 `GridSearchOptimizer` 内部持有，避免每个网格点重复触发 ClickHouse 读取或重复克隆整个事件 `Vec`。`Evaluator::evaluate` 接收 `&InMemoryMarketDataSource`（借用），配合 §八的 blanket `impl<T: HistoricalMarketDataSource> HistoricalMarketDataSource for &T`，`Evaluator` 的实现可以直接把这个引用传给 `apexes_backtest::run_backtest`，无需克隆。

---

## 七、`ExternalProcessOptimizer`（`external_process.rs`）——外部进程优化器插件接口

对应用户明确要求的"可插拔外部优化服务"（路线图 §2.2 "pluggable external ML service" 的字面实现）。设计原则见 §一"不包含"条目：明确排除原生动态库加载（无稳定 ABI、需要 `unsafe`），改用子进程 + newline-delimited JSON over stdio——零 `unsafe`，外部优化器可以用任何语言实现（典型场景：Python + scikit-optimize/Optuna 等成熟贝叶斯优化库）。

### 协议：外部进程主导（plugin-in-control）

宿主（`ExternalProcessOptimizer`）启动子进程后：

1. **宿主 → 外部进程**（一次性，启动后立即发送）：
   ```json
   {"search_space": {"sma_period": [10, 20, 50], "threshold": [0.1, 0.2, 0.3]}}
   ```
   `search_space` 的形状与 `SearchSpace::combinations()` 展开前的原始候选值列表一致（每个参数名到其候选值数组），而不是展开后的笛卡尔积——把"如何从候选值域里选点"这个决策完全交给外部进程（贝叶斯优化器需要自己决定探索顺序，不是遍历笛卡尔积）。

2. **外部进程 → 宿主**（重复零到多次，外部进程自主决定节奏/顺序/次数）：
   ```json
   {"evaluate": {"sma_period": 20, "threshold": 0.2}}
   ```
   宿主收到后，用这组参数调用 `Evaluator::evaluate(&params, &market_data)`，把结果回传：

3. **宿主 → 外部进程**（对每个 `evaluate` 请求的响应）：
   ```json
   {"score": 1.42}
   ```
   或（若该组合评估失败）：
   ```json
   {"error": "strategy construction failed: missing entry point"}
   ```

4. **外部进程 → 宿主**（终止信号，标志外部进程已经决定不再继续）：
   ```json
   {"done": {"best_params": {"sma_period": 20, "threshold": 0.2}, "best_score": 1.42}}
   ```
   或（外部进程自己判断没有找到任何可用结果，如所有组合都评估失败）：
   ```json
   {"done": null}
   ```

每条消息都是单独一行 JSON（newline-delimited JSON，即 JSON Lines / NDJSON），双向都是如此——宿主逐行读取子进程 stdout，子进程逐行读取宿主写入其 stdin 的内容。

### `apexes-optimize` 侧的消息类型（`external_process.rs`）

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Sent once, immediately after spawning the subprocess.
#[derive(Serialize)]
struct SearchSpaceMessage {
    search_space: HashMap<String, Vec<rust_decimal::Decimal>>,
}

/// Messages the subprocess may send, in any order/quantity it chooses.
#[derive(Deserialize)]
#[serde(untagged)]
enum PluginMessage {
    Evaluate { evaluate: HashMap<String, rust_decimal::Decimal> },
    Done { done: Option<DoneResult> },
}

#[derive(Deserialize)]
struct DoneResult {
    best_params: HashMap<String, rust_decimal::Decimal>,
    best_score: rust_decimal::Decimal,
}

/// Sent by the host in response to each `Evaluate` message.
#[derive(Serialize)]
#[serde(untagged)]
enum HostResponse {
    Score { score: rust_decimal::Decimal },
    Error { error: String },
}
```

### `ExternalProcessOptimizer`

```rust
use apexes_backtest::InMemoryMarketDataSource;
use apexes_core::MarketEvent;
use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

use crate::evaluator::Evaluator;
use crate::optimizer::{EvaluatedCombination, OptimizationResult, ParameterOptimizer};
use crate::search_space::SearchSpace;

/// Delegates parameter search to an external subprocess (any language)
/// speaking the newline-delimited-JSON protocol documented above. The
/// external process is fully in control of evaluation order/count; this
/// struct only spawns it, feeds it the search space, relays
/// `evaluate`/`score` messages back and forth via `Evaluator`, and
/// parses its final `done` message.
pub struct ExternalProcessOptimizer {
    command: String,
    args: Vec<String>,
    market_data: InMemoryMarketDataSource,
}

impl ExternalProcessOptimizer {
    pub fn new(command: impl Into<String>, args: Vec<String>, events: Vec<MarketEvent>) -> Self {
        Self {
            command: command.into(),
            args,
            market_data: InMemoryMarketDataSource::new(events),
        }
    }
}

#[async_trait]
impl ParameterOptimizer for ExternalProcessOptimizer {
    async fn optimize(
        &self,
        search_space: &SearchSpace,
        evaluator: &mut dyn Evaluator,
    ) -> OptimizationResult {
        // 1. Spawn the subprocess with piped stdin/stdout.
        // 2. Write the SearchSpaceMessage as one JSON line, flush.
        // 3. Loop: read one line from the subprocess's stdout.
        //    - Malformed JSON / unrecognized shape -> return
        //      OptimizationResult { best: None, all_results: vec![],
        //      error: Some(message) } immediately, kill the subprocess.
        //    - Evaluate { evaluate: params } -> call
        //      evaluator.evaluate(&params, &self.market_data).await,
        //      write back a Score or Error HostResponse line, record an
        //      EvaluatedCombination in all_results (score: None on
        //      Evaluator error, same per-combination-failure semantics
        //      as GridSearchOptimizer), continue looping.
        //    - Done { done: Some(result) } -> construct `best` from
        //      `result.best_params`/`result.best_score` (looked up
        //      against all_results by matching params, or constructed
        //      directly from the message if not found in all_results --
        //      the external process's own reported best is authoritative
        //      even if the host's own bookkeeping disagrees, since the
        //      external optimizer may have internal reasoning the host
        //      doesn't replicate), stop looping, return normally
        //      (error: None).
        //    - Done { done: None } -> stop looping, return with
        //      best: None, error: None (a normal, non-error empty
        //      result -- the plugin explicitly decided no usable result
        //      existed, distinct from a protocol failure).
        // 4. If the subprocess exits unexpectedly before sending a Done
        //    message (stdout EOF without a prior Done line), that is
        //    also a run-level failure: return with error: Some(...).
        unimplemented!("state machine described above; exact tokio::process::Command / BufReader / serde_json call sequence is the implementation plan's responsibility")
    }
}
```

**Note（本设计文档只锁定协议状态机，不是遗留占位）**：上面 `optimize` 方法体用注释描述了控制流分支而不是逐行可执行代码——具体的 `tokio::process::Command` 生成、`BufReader`/逐行读取、JSON 序列化/反序列化的确切调用序列，属于实现计划（plan）阶段该给出的逐步可执行代码；本设计文档的职责是把协议状态机的每个分支、每个消息的字段形状、每种失败模式的处理方式说清楚（已在上面注释里逐条列出），供计划编写者据此展开成完整实现，不存在语义模糊或遗留决策。

**为什么外部进程宣布的 `best_params`/`best_score` 是权威结果**：宿主自己也在 `all_results` 里记录了每次 `evaluate` 调用的分数，理论上可以自己算出"目前见过的最高分"，但外部优化器（尤其贝叶斯优化）内部可能有自己的排序/置信区间逻辑，不一定"分数最高的那次调用"就是它认为的最优解——所以宿主直接信任 `done` 消息里外部进程自己报告的最优结果，而不是重新在 `all_results` 里找最大值。

---

## 八、`apexes-backtest` 的 blanket 引用实现

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

## 九、测试策略

1. **`param()` 绑定单元测试**（`apexes-strategy`）：`from_file_with_params` 传入的 override 生效（脚本读到 override 值而非 default）、未提供 override 时脚本读到 default 值、`param()` 参数类型错误（`name` 非字符串/`default` 非数字）返回 error 对象。
2. **`SearchSpace` 单元测试**（`apexes-optimize`）：单参数笛卡尔积、多参数笛卡尔积（验证组合数 = 各参数候选值数量之积，且每个组合内容正确）、空 `SearchSpace` 返回空组合列表、某参数候选值为空列表时整体返回空组合列表。
3. **`GridSearchOptimizer` 单元测试**（用一个不依赖真实回测的 mock `Evaluator`，直接返回参数派生的确定性分数，如 `params["x"]`）：验证网格搜索正确找到最高分组合、验证 `all_results` 包含每个组合且顺序与 `SearchSpace::combinations()` 一致、验证某个组合 `Evaluator::evaluate` 返回 `Err` 时不影响其余组合的评估、不被误选为 best、且该组合的 `score` 字段为 `None`、验证全部组合都失败时 `best` 为 `None`。
4. **`ExternalProcessOptimizer` 单元/集成测试**（用一个专门为测试编写的最小"mock 外部优化器"脚本，而不是真正的贝叶斯优化实现——非目标里已明确排除实现真实算法）：
   - mock 外部进程可以是一个极简 shell 脚本或小型 Rust 测试二进制（用 `assert_cmd`/纯手写子进程均可，具体选择留给实现计划），读一行 `search_space` JSON，写几行确定性的 `evaluate` 请求，读回 `score` 响应，最后写一行 `done`。
   - 验证正常路径：`ExternalProcessOptimizer` 正确发送 `search_space`、正确转发多个 `evaluate`→`Evaluator::evaluate`→`score` 往返、正确解析最终 `done` 消息，`OptimizationResult.best` 与 mock 进程报告的 `best_params`/`best_score` 完全一致，`error` 字段为 `None`。
   - 验证 `evaluate` 请求对应的 `Evaluator::evaluate` 返回 `Err` 时，宿主正确回传 `{"error": "..."}` 而不是崩溃，且该次的 `EvaluatedCombination.score` 为 `None`（与 `GridSearchOptimizer` 一致的每组合失败语义）。
   - 验证 `{"done": null}`（外部进程主动声明无可用结果）时，`OptimizationResult.best` 为 `None`，`error` 也为 `None`（区分"正常但无结果"与"运行级故障"）。
   - 验证子进程发送格式错误的 JSON 时，`OptimizationResult.error` 为 `Some(...)`，`best` 为 `None`。
   - 验证子进程在发送 `done` 之前提前退出（stdout EOF）时，同样产生 `error: Some(...)` 而不是挂起等待更多输入。
5. **端到端集成测试**（真实 `.drift` 脚本 + `run_backtest` + `GridSearchOptimizer`）：一个使用 `param("threshold", ...)` 的 `.drift` fixture，`Evaluator` 实现内部调用 `DriftScriptStrategy::from_file_with_params` + `run_backtest` + 一个简单目标函数（如 `report.overall.trade_count` 转 `Decimal`，避免依赖 Sharpe 等需要足够数据点的复杂指标），验证网格搜索能跑通全流程并选出预期的最优参数组合（用手工构造的确定性行情数据，使某个特定参数组合产生可预测的最多/最少交易次数）。
6. **全工作区验证**：`cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --check`。

---

## 十、非目标（Non-goals，本次明确不做）

- 不做原生动态库（`.so`/`.dylib`）插件加载（见 §一"不包含"的详细理由——无稳定 ABI，需要 `unsafe`）
- 不实现任何真实的贝叶斯优化算法（无论是 Rust 内部实现还是 `ExternalProcessOptimizer` 协议另一端要跑的具体算法）——`ExternalProcessOptimizer` 只是协议适配器，测试里的"外部优化器"只是行为确定的 mock，不是生产可用的优化算法实现
- 不做参数搜索空间自动化描述/UI
- 不做并行/并发回测执行（`GridSearchOptimizer` 顺序执行；`ExternalProcessOptimizer` 对每个 `evaluate` 请求也是逐一顺序处理，不并发跑多个回测）
- 不做 Rust 原生 `Strategy` trait 的参数化（仅限 `DriftScriptStrategy`）
- 不做 stdin/stdout 以外的进程间通信方式（如共享内存、Unix domain socket、TCP）
