# Apexes — 子项目 #4.2：因子/技术指标库（`apexes-indicators`）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#4`（策略 SDK）的第二阶段详细设计。`#4` 按路线图 §2.2 包含五块能力：(1) Rust 策略 trait API + PyO3 绑定、(2) 因子/技术指标库、(3) 多信号组合与仓位生成、(4) 参数优化接口、(5) 策略版本管理与灰度发布。本文档覆盖 **(2)**。(1) 的 Rust 部分（`apexes-strategy` + `DriftScriptStrategy`）已完成（见 [`2026-07-30-apexes-strategy-drift-integration-design.md`](2026-07-30-apexes-strategy-drift-integration-design.md)）；(1) 的 PyO3 绑定部分明确推迟到 `#4` 的最后一轮再做（已有 Drift Script 集成降低了脚本化策略对 Python 绑定的迫切性）。其余三块（(3)/(4)/(5)）留给后续独立的 brainstorming → spec → plan 周期。
>
> 前置依赖：`apexes-strategy`（`Strategy`/`StrategyContext`/`DriftScriptStrategy` 已完成并推送）、`apexes-core`（`Bar`/`MarketEvent` 等类型已定义）。

---

## 一、范围

### 包含

1. **新增 workspace 成员 crate：`apexes-indicators`** —— 纯 Rust、无 `drift-script` 依赖，定义统一的流式（增量）指标 trait 与 7 个内置指标的实现：SMA、EMA、RSI、MACD、Bollinger Bands、ATR、滚动标准差（rolling stddev）。
2. **`apexes-strategy` 的新增绑定模块 `drift_dsl::indicators`**：向 Drift Script 环境注册一个全局 `indicators` 对象，暴露 7 个工厂函数（`indicators.sma(period)` 等），返回的对象持有底层 Rust 指标实例并暴露 `.update(...)` 方法，供 `.drift` 脚本在 `on_market_event` 里增量调用。
3. **`apexes-strategy` 的 `DriftScriptStrategy` 微调**：注册 `indicators` 绑定（与已有的 `drift-script-math`、`ctx.*` 绑定并列），使脚本可以在顶层 `let sma = indicators.sma(20);` 一次性创建指标实例，并在每次 `on_market_event` 调用中通过闭包持有的共享状态复用同一实例。

### 不包含（明确超出范围，留给对应后续子项目/后续轮次）

- PyO3 绑定（`#4` 最后一轮）
- 多信号组合与仓位生成引擎（`#4.3`）
- 参数优化接口（`#4.4`）
- 策略版本管理与灰度发布（`#4.5`）
- 本次未覆盖的其他技术指标（如 KDJ、OBV、Ichimoku 等）——v1 只做 SMA/EMA/RSI/MACD/Bollinger/ATR/stddev 七个，后续按需扩展
- 批量（batch，slice-in-slice-out）计算接口——v1 只做流式增量接口，回测场景下 `run_backtest` 按 tick 顺序调用 `Strategy::on_market_event` 天然是流式驱动，与实盘完全同源；如未来研究场景确实需要批量计算的便利封装，留给后续单独评估
- 指标热重载/参数动态调整（指标一旦通过 `indicators.sma(20)` 构造，`period=20` 在其生命周期内不可变，需要新参数则构造新实例）

---

## 二、Crate 结构

```
apexes-indicators/
  Cargo.toml
  src/
    lib.rs              # pub mod indicator; pub use indicator::Indicator; pub mod {sma,ema,rsi,macd,bollinger,atr,stddev};
    indicator.rs         # Indicator trait 定义
    sma.rs                # Sma
    ema.rs                # Ema
    rsi.rs                # Rsi
    macd.rs               # Macd, MacdOutput
    bollinger.rs           # Bollinger, BollingerOutput
    atr.rs                 # Atr
    stddev.rs              # RollingStdDev
```

`Cargo.toml` 依赖：
```toml
[package]
name = "apexes-indicators"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
rust_decimal = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
```

`apexes-strategy/Cargo.toml` 新增：
```toml
apexes-indicators = { path = "../apexes-indicators" }
```

---

## 三、`Indicator` trait（统一流式接口）

```rust
/// A streaming (incremental) technical indicator: internal state is
/// updated one data point at a time (`update`), never recomputed from
/// scratch over a historical window. This matches the project's
/// non-negotiable backtest/live logic-parity requirement -- the exact
/// same `update` call sequence drives both a historical replay
/// (`apexes-backtest`) and a live/paper trading loop.
pub trait Indicator {
    /// The data point type this indicator consumes per `update` call.
    /// Price-only indicators (SMA/EMA/RSI/MACD/Bollinger/stddev) use
    /// `rust_decimal::Decimal` (typically a bar's close price, but the
    /// indicator itself has no opinion on which price a caller feeds it).
    /// Indicators that structurally need more than one price field per
    /// bar (ATR needs high/low/close) use `apexes_core::Bar` instead.
    type Input;

    /// The value(s) produced by one `update` call. Single-value
    /// indicators use `Option<Decimal>` (`None` during warm-up, before
    /// enough data points have arrived to produce a meaningful value).
    /// Multi-value indicators (MACD, Bollinger Bands) use their own
    /// dedicated `Option<Struct>` output type, following the same
    /// warm-up convention.
    type Output;

    /// Feeds one new data point into the indicator's internal state and
    /// returns the updated value. O(1) amortized per call for every
    /// indicator in this crate (ring buffers / running sums, never an
    /// O(n) recompute-from-scratch over history).
    fn update(&mut self, input: Self::Input) -> Self::Output;
}
```

**设计理由**：
- `type Input`/`type Output` 关联类型（而非泛型参数）使每个指标的输入输出形状在类型系统里精确表达，调用方 `sma.update(price)` 和 `atr.update(&bar)` 都是类型安全的，不需要用一个万能的 `MarketEvent`/枚举做运行时判断。
- 流式而非批量：`update` 只接受一个新数据点、返回一个新值，内部状态（如 SMA 的滑动窗口和、EMA 的上一个值、RSI 的平均涨跌）自行维护。这是本设计唯一支持的计算模型（batch 封装明确排除在 v1 范围外，见 §一）。
- `Option<Output>` 表达"预热期"（warm-up）：例如 `Sma::new(20)` 在收到第 1~19 个数据点时 `update` 返回 `None`，第 20 个及之后返回 `Some(value)`。调用方（无论是 Rust 策略还是 Drift Script 脚本）必须显式处理 `None`，不存在"预热期间返回错误占位值"的隐式行为。

---

## 四、七个内置指标

### 4.1 `Sma`（简单移动平均）

```rust
pub struct Sma {
    period: usize,
    window: std::collections::VecDeque<Decimal>,
    sum: Decimal,
}

impl Sma {
    pub fn new(period: usize) -> Self { ... }  // period == 0 panics: 构造期契约错误，不是运行时数据错误
}

impl Indicator for Sma {
    type Input = Decimal;
    type Output = Option<Decimal>;
    fn update(&mut self, price: Decimal) -> Option<Decimal> {
        // 窗口未满 period 个元素时返回 None；满了之后每次 O(1) 更新（滑动窗口和）
    }
}
```

### 4.2 `Ema`（指数移动平均）

```rust
pub struct Ema {
    period: usize,
    alpha: Decimal,           // 2 / (period + 1)
    value: Option<Decimal>,   // None 直到第一个数据点
}
```
第一个数据点：`value = Some(price)`（作为 EMA 的初始种子），返回 `None`（尚未形成有意义的"平均"，与 SMA 的预热语义保持一致：至少要看到 `period` 个点才返回 `Some`——实现上第 1~`period-1` 个点更新内部种子但输出 `None`，第 `period` 个点开始输出 `Some`）。之后每个点：`value = alpha * price + (1 - alpha) * value`。

### 4.3 `Rsi`（相对强弱指数，Wilder's smoothing）

```rust
pub struct Rsi {
    period: usize,
    prev_price: Option<Decimal>,
    avg_gain: Decimal,
    avg_loss: Decimal,
    count: usize,
}
```
标准 Wilder 平滑：前 `period` 次涨跌用简单平均初始化 `avg_gain`/`avg_loss`，之后每次用 `avg = (avg * (period - 1) + current) / period` 递推更新。`update` 在拿到第一个价格时无法计算涨跌（无前值），以及预热期内（少于 `period+1` 个价格点）均返回 `None`。`avg_loss == 0` 时（历史区间内价格只涨不跌）RSI 定义为 `100`，避免除零。

### 4.4 `Macd`（含 `MacdOutput`）

```rust
pub struct Macd {
    fast_ema: Ema,
    slow_ema: Ema,
    signal_ema: Ema,
}

pub struct MacdOutput {
    pub macd: Decimal,       // fast_ema - slow_ema
    pub signal: Decimal,     // EMA(macd, signal_period)
    pub histogram: Decimal,  // macd - signal
}

impl Indicator for Macd {
    type Input = Decimal;
    type Output = Option<MacdOutput>;
}
```
`Macd::new(fast_period, slow_period, signal_period)`。每次 `update(price)` 先分别推进 `fast_ema`/`slow_ema`；只要两者都已给出 `Some` 值，计算 `macd = fast - slow`，再把这个 `macd` 值喂给 `signal_ema`；只有当 `signal_ema` 也给出 `Some` 值时，才返回 `Some(MacdOutput { .. })`，否则返回 `None`。

### 4.5 `Bollinger`（布林带，含 `BollingerOutput`）

```rust
pub struct Bollinger {
    sma: Sma,
    stddev: RollingStdDev,
    num_std: Decimal,
}

pub struct BollingerOutput {
    pub upper: Decimal,
    pub middle: Decimal,  // == SMA
    pub lower: Decimal,
}

impl Indicator for Bollinger {
    type Input = Decimal;
    type Output = Option<BollingerOutput>;
}
```
`Bollinger::new(period, num_std)`。内部持有一个 `Sma`（同一个 `period`）和一个 `RollingStdDev`（同一个 `period`），每次 `update(price)` 同步推进两者；两者都给出 `Some` 时才返回 `Some(BollingerOutput { upper: middle + num_std * std, middle, lower: middle - num_std * std })`。

### 4.6 `Atr`（平均真实波幅，唯一需要 high/low/close 三个价格字段的指标）

```rust
pub struct Atr {
    period: usize,
    prev_close: Option<Decimal>,
    avg_tr: Option<Decimal>,
    count: usize,
}

impl Indicator for Atr {
    type Input = AtrInput;   // 见 §六·决策 2 —— 不是 apexes_core::Bar 本身
    type Output = Option<Decimal>;
    fn update(&mut self, input: AtrInput) -> Option<Decimal> {
        // true_range = max(high-low, |high-prev_close|, |low-prev_close|)
        // 第一个数据点无 prev_close，true_range = high-low
        // Wilder 平滑：avg_tr = (avg_tr * (period-1) + true_range) / period（预热期用简单平均初始化，同 RSI）
    }
}
```
`Input` 类型是本 crate 定义的 `AtrInput`（只含 `high`/`low`/`close` 三个 `Decimal` 字段），而不是完整的 `apexes_core::Bar`——理由见 §六·决策 2。`AtrInput` 实现 `From<&apexes_core::Bar>`，Rust 端调用方（策略/回测/未来实盘）持有真实 `Bar` 时用 `atr.update((&bar).into())` 即可，不需要手动拆字段。

### 4.7 `RollingStdDev`（滚动标准差/波动率）

```rust
pub struct RollingStdDev {
    period: usize,
    window: std::collections::VecDeque<Decimal>,
    sum: Decimal,
    sum_sq: Decimal,
}

impl Indicator for RollingStdDev {
    type Input = Decimal;
    type Output = Option<Decimal>;
    // population stddev over the current window: sqrt(E[x^2] - E[x]^2)，用 sum/sum_sq 的滑动窗口 O(1) 更新
}
```
`rust_decimal::Decimal::sqrt()`（workspace 已启用 `rust_decimal` 的 `maths` feature，见 `apexes-backtest` Task 13 的既有先例）。

---

## 五、`apexes-strategy` 的 Drift Script 绑定（`drift_dsl/indicators.rs`）

### 脚本侧用法

```javascript
// 顶层：脚本解析+执行一次时创建，随 Interpreter 环境持久化
let sma = indicators.sma(20);
let atr = indicators.atr(14);
let macd = indicators.macd(12, 26, 9);

function on_market_event(event, ctx) {
  // 价格类指标：显式传入价格（不是整个 event），脚本自己决定用哪个价格字段。
  // 注意：drift-script 目前不支持 `??`（nullish coalescing，parser 层面能解析，
  // 但 lowering 阶段显式报 "nullish coalescing not supported" 错误，见
  // drift-dsl `crates/drift-script/src/parser.rs` 的 `lower_logical_expression`），
  // 所以用三元表达式 + `event.type` 标签字段（`convert.rs` 对每种 MarketEvent
  // variant 都会写入这个字段）判断走哪个价格字段：
  let price = event.type == "bar" ? event.close : event.price;
  let sma_value = sma.update(price);

  // ATR：唯一需要完整 Bar 的指标，直接传整个 event（须是 bar 类型事件；
  // 结构上要求 event.high/low/close 都存在，与 apexes_core::Bar 的字段一一对应）
  let atr_value = atr.update(event);

  let macd_value = macd.update(price);
  if (macd_value != null) {
    // macd_value.macd / macd_value.signal / macd_value.histogram
  }

  return [];
}
```

### 绑定实现要点（`indicators.rs`，与 `bindings.rs` 并列的新文件）

```rust
/// Installs the `indicators` global object into `interp`'s environment:
/// factory functions (`sma`/`ema`/`rsi`/`macd`/`bollinger`/`atr`/`stddev`)
/// that construct an `apexes_indicators::*` instance wrapped in
/// `Arc<Mutex<...>>` and return a `DriftValue::Object` exposing a single
/// `update` method closing over that shared state. Because the returned
/// object (and its captured `Arc<Mutex<_>>`) lives in the script's
/// top-level environment -- not reconstructed per `on_market_event` call
/// like `ctx.*` -- indicator state correctly persists across ticks.
pub fn register_to(interp: &Interpreter) { ... }
```

- 每个工厂函数（如 `sma`）签名：`(period: number) -> Object { update: NativeFunction }`。
- `update` 闭包：`Arc<Mutex<apexes_indicators::Sma>>`，锁内调用 `.update(price)`，把 `Option<Decimal>` 转成 `DriftValue::Number` 或 `DriftValue::Null`（与既有 `default_position_drift_value` 等惯例一致：预热期返回 `null`，不是错误）。
- **MACD/Bollinger 的 `Option<Struct>` → `DriftValue`**：`Some(MacdOutput{macd,signal,histogram})` 转成 `DriftValue::Object({macd, signal, histogram})`（均为 `Number`）；`None` 转成 `DriftValue::Null`。Bollinger 同理（`upper`/`middle`/`lower`）。
- **ATR 的入参**：`update` 闭包从 `DriftValue::Object`（脚本传入的 `event`）里读取 `high`/`low`/`close`（复用 `convert.rs` 已有的 `num_field` 校验辅助函数模式），直接构造一个 `apexes_indicators::AtrInput { high, low, close }`（见 §六·决策 2），不涉及 `apexes_core::Bar` 的其余无关字段。
- **参数校验**：`period`/`fast`/`slow`/`signal` 等必须是正整数（`DriftValue::Number` 转 `usize`，非正数或非整数报错——通过 `NativeFn` 返回一个 `error_object`，遵循 `drift-script-math` 已有的错误约定，而不是让 Rust 端 `panic`）。

---

## 六、关键设计决策

### 决策 1：为什么绑定层用 `Arc<Mutex<T>>` 而不是复用 `bindings.rs` 的"每次调用重建"模式

`ctx.*` 绑定（`bindings.rs`）之所以每次 `on_market_event` 调用都重新构造 `NativeFunction`，是因为 `StrategyContext<'a>` 本身是每次调用传入的新借用数据，没有跨 tick 保留状态的语义。指标则相反：**状态必须跨 tick 累积**（SMA 的滑动窗口、EMA 的上一个值等），如果每次 `on_market_event` 调用都重新构造指标实例，预热期永远无法结束，指标永远输出 `None`——这是指标绑定与 `ctx` 绑定在生命周期设计上的本质区别，因此指标绑定必须依附在脚本的**顶层持久环境**（`let sma = indicators.sma(20);` 只求值一次），而不是每次调用时重建的 `ctx` 对象上，`Arc<Mutex<T>>` 是让"跨调用共享可变状态"在 `'static` 闭包约束下工作的标准做法。

### 决策 2：ATR 绑定不直接复用 `apexes_core::Bar`，改用专用小 struct

Rust 原生 `Atr::update` 的 `Input` 类型规范上是 `apexes_core::Bar`（§四·4.6），因为 Rust 调用方（策略/回测/未来实盘）天然持有真实的 `Bar` 值。但 Drift Script 绑定层从脚本传入的 `event`（`DriftValue::Object`）重建一个完整 `Bar` 需要编造 `symbol`/`venue`/`interval`/`open_time`/`close_time` 等 ATR 计算根本用不到的字段，纯属噪音。**解决方案**：`apexes-indicators` 额外导出一个只含 ATR 真正需要的三个字段的小类型：

```rust
/// The subset of OHLC fields `Atr` actually needs. `apexes_core::Bar`
/// implements `From<&Bar> for AtrInput` (defined in this crate, not
/// `apexes-core`, to avoid a reverse dependency) so Rust callers can pass
/// a real `Bar` directly; the Drift Script binding layer constructs an
/// `AtrInput` straight from the script's `high`/`low`/`close` fields
/// without fabricating an entire fake `Bar`.
pub struct AtrInput {
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
}

impl From<&apexes_core::Bar> for AtrInput {
    fn from(bar: &apexes_core::Bar) -> Self {
        Self { high: bar.high, low: bar.low, close: bar.close }
    }
}

impl Indicator for Atr {
    type Input = AtrInput;
    // ...
}
```
这修正了 §四·4.6 草稿中"`Input = apexes_core::Bar`"的说法——最终以 `Input = AtrInput` 为准，`apexes_core::Bar` 通过 `From` 转换提供人机友好的 Rust 端调用方式（`atr.update((&bar).into())`），Drift Script 绑定层则直接从三个已校验的 `Decimal` 字段构造 `AtrInput`，不涉及 `apexes_core::Bar` 的其余字段。

### 决策 3：`period`/`num_std` 等构造期参数校验放在哪一层

- **Rust 端**（`Sma::new(0)` 等）：非法参数（`period == 0`）是调用方的编程错误（契约违反），`new()` 直接 `panic!`——与项目里其他"构造期契约"惯例一致（例如 `rust_decimal::Decimal::from_f64_retain` 之外的很多内部辅助函数对非法输入的处理方式）。不返回 `Result`，因为这类错误在 Rust 调用方那里应该在编译期/代码审查期就被发现，不是需要优雅降级的运行时数据错误。
- **Drift Script 绑定层**（`indicators.sma(0)`）：**不能** panic——脚本参数来自脚本作者，是"运行时数据"意义上的输入，任何 `.drift` 脚本的错误不应该 panic 整个宿主进程。绑定层在调用 `Sma::new(period)` 之前先校验 `period` 是正整数，非法时返回 `error_object`（复用 `drift-script-math` 的既有 `error_object` 约定），不进入 Rust 端的 `new()` 调用。

---

## 七、测试策略

1. **`apexes-indicators` 单元测试**（每个指标一个文件内的 `#[cfg(test)] mod tests`）：
   - 预热期行为：`period` 个点之前全部返回 `None`（或 MACD/Bollinger 对应的 `Option<Struct>::None`）
   - 已知输入序列 + 手算/第三方工具（如 Python `pandas`/`ta-lib`）交叉验证的期望输出值，至少覆盖每个指标 1~2 个"黄金数据"用例
   - RSI 的 `avg_loss == 0` 边界（只涨不跌）→ 期望 `100`
   - ATR 的 `AtrInput::from(&Bar)` 转换正确性
   - `Sma::new(0)` 等非法构造参数 panic（用 `#[should_panic]`）

2. **`apexes-strategy` 的 Drift Script 绑定测试**：
   - `drift_dsl/indicators.rs` 自身的 Rust 单元测试（类似 `bindings.rs` 现有模式）：直接调用绑定层构造出的 `NativeFn`，验证预热期 `null`、正常值、MACD/Bollinger 的 `Object` 输出结构、ATR 从脚本 `event` 对象正确提取 high/low/close
   - 新增 `.drift` fixture（`tests/fixtures/uses_indicators.drift`）+ 对应 `DriftScriptStrategy` 端到端测试：脚本顶层创建一个 `indicators.sma(3)`，连续 3 次调用 `on_market_event`（价格分别为已知值），验证第 1、2 次调用返回值里看不到 SMA 影响（预热期），第 3 次调用开始 SMA 生效并影响返回的 `OrderIntent`（例如"SMA 就绪且当前价高于 SMA 才买入"的简单逻辑）——验证跨调用状态持久化这一核心行为正确工作，而不仅仅是绑定函数本身被正确注册。

3. **全工作区验证**：`cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --check`。

---

## 八、非目标（Non-goals，本次明确不做）

- 不做 PyO3 绑定（`#4` 最后一轮）
- 不做批量（batch）计算接口封装
- 不做 v1 七个指标之外的其他技术指标
- 不做指标参数的运行时动态调整/热重载
- 不做指标结果的持久化/缓存（每次 `Strategy`/脚本实例的生命周期内，指标状态只存在于内存中）
