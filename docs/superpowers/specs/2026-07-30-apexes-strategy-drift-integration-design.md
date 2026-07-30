# Apexes — 子项目 #4.1：策略 SDK 第一阶段（Strategy trait 搬家 + Drift Script 集成）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#4`（策略 SDK）的第一阶段详细设计。`#4` 按路线图 §2.2 包含五块能力：(1) Rust 策略 trait API + PyO3 绑定、(2) 因子/技术指标库、(3) 多信号组合与仓位生成、(4) 参数优化接口、(5) 策略版本管理与灰度发布。这五块耦合度不高，本文档只覆盖 **(1) 的 Rust 部分**（不含 PyO3），其余四块留给后续独立的 brainstorming → spec → plan 周期。
>
> 前置依赖：[`2026-07-29-drift-dsl-extraction-design.md`](2026-07-29-drift-dsl-extraction-design.md)（已完成并推送，`drift-script`/`drift-script-stdlib`/`drift-script-math` 三个 crate 已在 `github.com/LeafAI/drift-dsl` 可用）。

---

## 一、范围

### 包含

1. **新增 workspace 成员 crate：`apexes-strategy`**，成为 `Strategy`/`StrategyContext`/`OrderIntent`/`Position` 的**权威定义**（从 `apexes-backtest` 迁移，行为不变）。
2. **`DriftScriptStrategy`**：`Strategy` trait 的一个实现，加载 `.drift` 脚本文件，把每次 `on_market_event` 调用转发给脚本里的 `on_market_event(event, ctx)` 函数，脚本返回值解析回 `Vec<OrderIntent>`。
3. **`apexes-backtest` 迁移改动**：删除其 `src/strategy.rs`，改为依赖 `apexes-strategy` 并 `pub use` 重新导出，确保整个 workspace 保持可编译、可测试、行为不变。
4. **`drift-dsl` 仓库的一处小增量**：给 `drift_script::Interpreter` 增加一个公开的 `call()` 方法（转发到现有私有的 `call_function`），供 `apexes-strategy` 调用脚本里的具名函数。这是本次 plan 范围内、对 `drift-dsl` 仓库的独立小提交。

### 不包含（明确超出范围，留给对应后续子项目）

- PyO3 绑定（`#4` 的其余部分，留给单独一轮）
- 因子库/技术指标库（`#4.2`）
- 多信号组合与仓位生成引擎（`#4.3`）
- 参数优化接口（`#4.4`）
- 策略版本管理与灰度发布（`#4.5`）
- 任何交易特定的技术指标（SMA/EMA/RSI 等）——`drift-script-math` 明确只提供通用算术/统计，交易指标属于 `apexes-strategy` 自己后续的绑定 crate（`#4.2`），不属于 `drift-dsl`

---

## 二、Crate 结构

```
apexes-strategy/
  Cargo.toml
  src/
    lib.rs
    strategy.rs          # Strategy trait, StrategyContext, OrderIntent, Position（从 apexes-backtest 原样迁移）
    error.rs             # StrategyError
    drift_dsl/
      mod.rs
      convert.rs          # event/ctx -> DriftValue，DriftValue -> Vec<OrderIntent>
      bindings.rs         # ctx.get_position / get_balance / now / has_open_order 的 native fn 注册
      strategy.rs         # DriftScriptStrategy 本体
  tests/
    fixtures/
      always_buy.drift
      uses_math.drift
      uses_ctx_position.drift
      syntax_error.drift
      runtime_panic.drift
```

`Cargo.toml` 依赖：
```toml
[dependencies]
apexes-core = { path = "../apexes-core" }
drift-script = { git = "https://github.com/LeafAI/drift-dsl.git", rev = "<sha>" }
drift-script-math = { git = "https://github.com/LeafAI/drift-dsl.git", rev = "<sha>" }
tokio = { workspace = true }
thiserror = { workspace = true }
rust_decimal = { workspace = true }
chrono = { workspace = true }
tracing = { workspace = true }
```

`apexes-backtest` 改动：
```toml
# Cargo.toml 新增
apexes-strategy = { path = "../apexes-strategy" }
```
```rust
// lib.rs：删除 pub mod strategy; 改为
pub use apexes_strategy::{OrderIntent, Position, Strategy, StrategyContext};
```
`src/strategy.rs` 整个文件删除（其内容原样迁移到 `apexes-strategy/src/strategy.rs`，字段/文档注释/既有单元测试保持不变）。

---

## 三、`Strategy`/`StrategyContext`/`OrderIntent`/`Position`（迁移，行为不变）

与当前 `apexes-backtest::strategy` 模块完全一致，仅移动 crate 位置：

```rust
pub trait Strategy: Send {
    fn on_market_event(&mut self, event: &apexes_core::MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent>;
}

pub struct StrategyContext<'a> {
    pub positions: &'a HashMap<String, Position>,
    pub balances: &'a HashMap<String, Decimal>,
    pub open_orders: &'a HashMap<OrderId, Order>,
    pub now: DateTime<Utc>,
}

pub struct OrderIntent {
    pub symbol: String,
    pub venue: Venue,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
}

pub struct Position {
    pub symbol: String,
    pub quantity: Decimal,
    pub average_entry_price: Decimal,
}
```

---

## 四、`drift-dsl` 增量：公开的 `Interpreter::call()`

`drift_script::Interpreter` 当前的函数调用逻辑是私有方法 `fn call_function(&mut self, func: DriftValue, args: Vec<DriftValue>) -> Pin<Box<dyn Future<...>>>`（`interpreter.rs` 第 993 行）。新增一个公开的薄包装：

```rust
impl Interpreter {
    /// Calls a previously-resolved `DriftValue::Function`/`NativeFunction` with the
    /// given positional arguments, returning its result (or the error it raised).
    /// Intended for host code that needs to invoke a specific script-defined function
    /// by value (e.g. a resolved export), as opposed to `evaluate()` which runs a
    /// whole statement list.
    pub async fn call(&mut self, func: DriftValue, args: Vec<DriftValue>) -> InterpretResult {
        self.call_function(func, args).await
    }
}
```

这是对 `drift-dsl` 仓库的独立小提交（`feat: expose Interpreter::call for invoking resolved functions by value`），签名提交并推送，本次 plan 的一个前置任务。

---

## 五、类型转换层（`convert.rs`）

手写转换函数，不依赖 serde，精确控制字段名/类型：

### Rust → DriftValue

- `market_event_to_drift_value(&MarketEvent) -> DriftValue`：`Object`，含 `"type"` 标签字段（`"tick"`/`"depth"`/`"bar"`/`"trade"`）+ 各 variant 专属字段。`Decimal` 经 `to_f64()` 转 `DriftValue::Number`；`DateTime<Utc>` 转 ISO8601 字符串。
- `position_to_drift_value(&Position) -> DriftValue`：`{ symbol, quantity, average_entry_price }`。
- 无持仓/无对应资产/无未成交单时的默认值：
  - `get_position(symbol)` 未找到时返回 `{ symbol, quantity: 0, average_entry_price: 0 }`（不是 null）
  - `get_balance(asset)` 未找到时返回 `0`
  - `has_open_order(symbol)` 未找到任何未成交单时返回 `false`

### DriftValue → Rust

- `order_intents_from_drift_value(DriftValue) -> Result<Vec<OrderIntent>, String>`：
  - 期望顶层是 `DriftValue::Array`；非 Array 返回 `Err`
  - 每个元素期望是 `Object`，校验必填字段 `symbol`(String)、`venue`(String)、`side`(String)、`order_type`(String)、`quantity`(Number)
  - `side`: `"buy"`/`"sell"`（大小写不敏感）→ `Side::Buy`/`Side::Sell`，其他值报错
  - `order_type`: `"market"`/`"limit"`/`"stop"`/`"stop_limit"`（大小写不敏感）→ 对应 `OrderType`，其他值报错
  - `venue`: 大小写不敏感匹配 `Venue` 的所有已知变体（`"binance"`/`"okx"`/`"coinbase"`/`"sse"`/`"szse"`/`"hkex"`/`"nasdaq"`/`"nyse"`），其他值报错
  - `price`/`stop_price`：可选字段，存在则要求是 Number，`Limit`/`StopLimit` 缺少 `price`、`Stop`/`StopLimit` 缺少 `stop_price` 时报错（类型级要求，脚本传错等价于策略逻辑错误，视为运行时错误，走"记录日志+返回空 Vec"路径，不 panic）
  - `f64` → `Decimal`：用 `Decimal::from_f64_retain`，若 `None`（如 NaN/Infinity）则报错

---

## 六、`ctx` 便利方法（`bindings.rs`）

脚本里 `ctx` 是一个 `DriftValue::Object`，包含四个字段：

| 脚本调用 | 签名 | 语义 |
|---|---|---|
| `ctx.get_position(symbol)` | `(string) -> Object` | 无持仓返回 `quantity=0` 的默认 Position（见上） |
| `ctx.get_balance(asset)` | `(string) -> number` | 无该资产余额返回 `0` |
| `ctx.now()` | `() -> string` | 当前回测/实盘时间，ISO8601 |
| `ctx.has_open_order(symbol)` | `(string) -> bool` | 该 symbol 上是否存在任意未成交单（不区分买卖方向），无则 `false` |

**生命周期约束与解决方案**：`StrategyContext<'a>` 的 `positions`/`balances`/`open_orders` 是借用的 `HashMap` 引用，而 `DriftValue::NativeFunction` 要求闭包 `'static`。`DriftScriptStrategy::on_market_event` 每次调用时，先把这三个 map **clone 成 owned `HashMap`**（因为已经是引用背后的数据，一次 clone 成本可控），再用 `move` 闭包捕获这些 owned 数据注册为四个 native fn，随本次调用的 `Interpreter` 环境一起构造、一起丢弃——不做 unsafe 生命周期延长，不做全局可变状态。

---

## 七、`DriftScriptStrategy`

```rust
pub struct DriftScriptStrategy {
    interpreter: drift_script::Interpreter,
    on_market_event_fn: drift_script::DriftValue,  // 构造时解析出的具名函数值
}

impl DriftScriptStrategy {
    /// 读取并 parse 脚本文件，执行顶层语句（使 `function on_market_event(...)`
    /// 声明进入解释器环境），再通过 `interpreter.env().get("on_market_event")`
    /// 取出该函数值（不要求脚本写 `export`——`FunctionDecl` 本身就会把函数登记
    /// 进环境，`get()` 而非 `get_exported()` 即可取到；`get_exported()` 只对显式
    /// `export function ...` 语句生效，二者是 drift-script 里两条独立路径，见
    /// `interpreter.rs` 的 `FunctionDecl`/`ExportDecl` 分支）。脚本读取失败/语法
    /// 错误/取不到该函数值/取到的值不是 Function，均返回 `Err`（构造失败，不
    /// 产生半初始化实例）。
    pub fn from_file(path: &Path) -> Result<Self, StrategyError> { ... }
}

impl Strategy for DriftScriptStrategy {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent> {
        // 1. clone ctx 的三个 map，注册 ctx.* native fn，构造本次调用用的 ctx DriftValue
        // 2. event_to_drift_value(event)
        // 3. tokio::runtime::Handle::current().block_on(
        //      self.interpreter.call(self.on_market_event_fn.clone(), vec![event_val, ctx_val])
        //    )
        // 4. Ok(v) => match order_intents_from_drift_value(v) { Ok(intents) => intents, Err(msg) => { tracing::error!(...); vec![] } }
        //    Err(e) => { tracing::error!("on_market_event script error: {e}"); vec![] }
    }
}
```

**运行时错误处理**（脚本抛异常、返回值格式不对）：记录 `tracing::error!` 日志，返回空 `Vec<OrderIntent>`，不中断整个回测/实盘循环——单次 tick 的策略失败不应该导致整个交易系统停摆，这与项目"无人值守运行"的最终目标一致。

**`block_on` 的调用环境**：`DriftScriptStrategy::on_market_event` 是同步方法，通过 `tokio::runtime::Handle::current().block_on(...)` 同步等待脚本执行完毕。调用方（`apexes-backtest::runner::run_backtest`、未来的实盘/模拟 runner）必须在 tokio runtime 线程上下文中调用 `Strategy::on_market_event`（已经是当前 `apexes-backtest` runner 的实际运行环境，因为整个 crate 依赖 tokio）。若未来某个调用方在真正的单线程无 runtime 环境下调用，会在运行时 panic（`block_on` 的已知限制）——这是本设计接受的已知约束，不在本次范围内额外处理（比如切换到 `block_in_place`），后续如遇到真实场景再评估。

---

## 八、`StrategyError`

```rust
#[derive(Debug, thiserror::Error)]
pub enum StrategyError {
    #[error("failed to read script file {path}: {source}")]
    Io { path: PathBuf, source: std::io::Error },

    #[error("failed to parse script: {0}")]
    Parse(String),

    #[error("script has no top-level `on_market_event` function")]
    MissingEntryPoint,

    #[error("`on_market_event` is defined but is not a function")]
    EntryPointNotCallable,
}
```

仅在 `DriftScriptStrategy::from_file`（构造期）使用；`on_market_event` 方法本身不返回 `Result`（match trait 签名），运行时错误走日志+空 Vec 路径（见上）。

---

## 九、测试策略

1. **`convert.rs` 单元测试**：
   - 每个 `MarketEvent` variant（Tick/Depth/Bar/Trade）转换正确
   - `get_position`/`get_balance`/`has_open_order` 的默认值路径（无持仓/无余额/无未成交单）
   - `order_intents_from_drift_value`：合法输入 + 每种校验失败场景（缺字段、非法枚举字符串、非法数值、顶层非 Array、元素非 Object）各至少一个测试

2. **`DriftScriptStrategy` 端到端测试**（`tests/fixtures/*.drift` + driver 测试）：
   - `always_buy.drift`：固定返回一个 Buy Market OrderIntent，验证基本调用链路
   - `uses_math.drift`：调用 `math.mean()`/`math.stddev()` 等，验证 `drift-script-math` 绑定生效
   - `uses_ctx_position.drift`：调用 `ctx.get_position()`/`ctx.get_balance()`/`ctx.has_open_order()`，验证 ctx 便利方法与默认值语义
   - `syntax_error.drift`：`from_file` 返回 `Err(StrategyError::Parse(_))`
   - `runtime_panic.drift`：脚本运行时抛异常（如访问未定义变量），验证 `on_market_event` 记录日志后返回空 `Vec`，不 panic

3. **`apexes-backtest` 迁移验证**：删除 `strategy.rs`、改为 re-export 后，`cargo test -p apexes-backtest` 全部既有测试原样通过（证明搬迁行为不变）。

4. **全工作区验证**：`cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --check`。

---

## 十、非目标（Non-goals，本次明确不做）

- 不做 PyO3 绑定
- 不做交易特定技术指标（SMA/EMA/RSI 等）——`drift-script-math` 及本次 `ctx` 绑定均保持通用，交易指标属于后续 `#4.2`
- 不做脚本热重载（`DriftScriptStrategy::from_file` 只在构造时加载一次；运行中修改 `.drift` 文件不会生效，需要重新构造实例）
- 不改变 `block_on` 的调用环境假设（见 §七 最后一段）
