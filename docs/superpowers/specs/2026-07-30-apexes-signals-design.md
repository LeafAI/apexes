# Apexes — 子项目 #4.3：多信号组合与仓位生成引擎（`signals.*` Drift Script 绑定）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#4`（策略 SDK）的第三阶段详细设计。`#4` 按路线图 §2.2 包含五块能力：(1) Rust 策略 trait API + PyO3 绑定、(2) 因子/技术指标库、(3) 多信号组合与仓位生成、(4) 参数优化接口、(5) 策略版本管理与灰度发布。本文档覆盖 **(3)**（对应 `docs/FEATURES.md` 行 `2.3`）。(1)/(2) 均已完成（分别见 [`2026-07-30-apexes-strategy-drift-integration-design.md`](2026-07-30-apexes-strategy-drift-integration-design.md)、[`2026-07-30-apexes-indicators-design.md`](2026-07-30-apexes-indicators-design.md)）。(4)/(5) 及 (1) 的 PyO3 部分留给后续独立的 brainstorming → spec → plan 周期。
>
> 前置依赖：`apexes-strategy`（`DriftScriptStrategy`、`drift_dsl::indicators` 绑定已完成并推送）、`apexes-indicators`（流式 `Indicator` trait 与 7 个内置指标已完成）。

---

## 一、范围

### 包含

**纯 Drift Script 绑定扩展，不新增 Rust trait 或 crate。** 用户已明确决定：#4.3 的范围就是给 `apexes-strategy` 的 `DriftScriptStrategy` 新增一组 `signals.*` 原生函数绑定，供 `.drift` 脚本在 `on_market_event` 里把多个信号分数组合成一个仓位调整订单。不存在 Rust 端的 "Signal" trait、"SignalCombiner" 结构体或独立的 `apexes-signals` crate——这与 `apexes-indicators`（Rust 原生 trait + crate + 绑定层）的架构故意不同，因为用户的意图就是"信号组合逻辑只活在脚本层面，Rust 侧不需要额外抽象"。

新增 `crates/apexes-strategy/src/drift_dsl/signals.rs`，与已有的 `drift_dsl/indicators.rs`、`drift_dsl/bindings.rs` 并列，注册一个全局 `signals` 对象，暴露三个函数：

1. **`signals.combine(items)`**：把多个 `{score, weight}` 信号加权平均成一个复合分数。
2. **`signals.target_position(score, max_position_size, threshold)`**：把复合分数（连同死区阈值）线性映射成目标仓位数量。
3. **`signals.rebalance_order(target_qty, current_qty, symbol, venue, min_order_size)`**：把目标仓位与当前仓位的差值转换成一个（或零个）市价单，直接以 `on_market_event` 期望的数组形状返回。

### 不包含（明确超出范围，留给对应后续子项目/后续轮次）

- 不新增任何 Rust 端 trait/struct/crate（如 `Signal`、`SignalCombiner`、`apexes-signals`）——纯 Drift Script 绑定
- 不做参数优化接口（`#4.4`）
- 不做策略版本管理与灰度发布（`#4.5`）
- 不做 PyO3 绑定（`#4` 最后一轮）
- 不做除市价单以外的订单类型支持（`rebalance_order` 只产生 `Market` 订单；Limit/Stop 场景留给脚本自己用 `OrderIntent` 字面量手写，不属于本次范围）
- 不做跨 symbol 的仓位再平衡/组合优化（每次 `rebalance_order` 调用只处理单个 symbol；跨 symbol 的资金分配逻辑留给脚本自己编排多次调用）

---

## 二、文件改动

```
crates/apexes-strategy/
  src/
    drift_dsl/
      mod.rs          # 新增 `mod signals;`（无需 pub use，signals::register_to 只在 strategy.rs 内部调用，与 indicators 模式一致）
      signals.rs      # 新增：signals.combine / target_position / rebalance_order 三个绑定 + 各自单元测试
      strategy.rs     # 修改：DriftScriptStrategy::from_file 里紧跟 indicators::register_to 之后新增一行 signals::register_to(&interpreter);
  tests/
    fixtures/
      uses_signals.drift   # 新增：端到端 fixture，组合一个基于指标的信号分数 + ctx 持仓，产出 rebalance 订单
    drift_script_strategy.rs  # 修改：追加 1-2 个新 #[test]，驱动 uses_signals.drift
```

`Cargo.toml` 无需改动——`signals.rs` 只用到已有依赖（`drift_script`、`rust_decimal`，都已是 `apexes-strategy` 的依赖）。

---

## 三、`signals.combine(items)`：加权平均组合信号

### 脚本侧签名

```javascript
let composite = signals.combine([
  { score: sma_signal, weight: 2 },
  { score: rsi_signal, weight: 1 },
]);
// composite 是一个 number（复合分数，范围 [-1, 1]）
```

### 语义

- `items` 必须是 `DriftValue::Array`，每个元素必须是 `DriftValue::Object`，含 `score`（number）、`weight`（number）两个必填字段。
- 每个 `score` 在参与计算前**静默 clamp 到 `[-1.0, 1.0]`**（分数轻微越界是正常现象，不视为错误——例如指标短暂产生略超范围的中间值）。
- 复合分数 = `Σ(score_i * weight_i) / Σ(weight_i)`（加权平均，权重不要求归一化到 1）。
- **权重全为 0（或 `Σweight_i == 0`）时**：返回 `0.0`（避免除零；语义上"没有任何权重"等价于"没有意见"，返回中性分数）。
- **空数组**：返回 `0.0`（同样视为"没有意见"）。

### 校验失败（返回 `{error: "..."}`，不是 panic，不是静默降级）

- `items` 不是数组 → error
- 数组元素不是对象 → error（含索引，如 `signals.combine: element at index 1 must be an object, got number`）
- 元素缺少 `score` 或 `weight` 字段，或字段类型不是 number → error（含索引与字段名）
- `weight` 为负数 → error（负权重没有合理语义，视为脚本逻辑错误；`weight == 0` 合法，代表"这个信号暂时不参与"）

---

## 四、`signals.target_position(score, max_position_size, threshold)`：分数到目标仓位的线性映射（含死区）

### 脚本侧签名

```javascript
let target_qty = signals.target_position(composite, 10, 0.2);
// composite in [-1, 1]，max_position_size = 10（symbol 的最大仓位数量），threshold = 0.2（死区阈值）
```

### 语义（死区 + 线性缩放）

- `score` 先 clamp 到 `[-1.0, 1.0]`。
- 若 `|score| <= threshold`：返回 `0.0`（死区内，不产生任何仓位意图——避免噪声分数导致的频繁换仓）。
- 若 `score > threshold`：线性缩放，`threshold` 处为 `0`，`score == 1.0` 处为 `max_position_size`：
  ```
  target = max_position_size * (score - threshold) / (1.0 - threshold)
  ```
- 若 `score < -threshold`：对称地，`-threshold` 处为 `0`，`score == -1.0` 处为 `-max_position_size`：
  ```
  target = -max_position_size * (-score - threshold) / (1.0 - threshold)
  ```
- **边界情况 `threshold >= 1.0`**：死区覆盖了整个 `[-1,1]` 值域，任何输入分数都返回 `0.0`（数学上一致，不需要特殊分支——`|score| <= threshold` 恒真）。

### 校验失败（返回 `{error: "..."}`）

- 任一参数不是 number，或缺少参数 → error
- `max_position_size < 0` → error（"max_position_size must be non-negative"）
- `threshold < 0` → error（"threshold must be in [0, 1]"；`threshold` 上界不做硬校验，因为 `threshold >= 1.0` 有良好定义的行为——见上文边界情况，不需要拒绝）

---

## 五、`signals.rebalance_order(target_qty, current_qty, symbol, venue, min_order_size)`：目标仓位与当前仓位的差值转订单

### 脚本侧签名

```javascript
let position = ctx.get_position(event.symbol);
let orders = signals.rebalance_order(target_qty, position.quantity, event.symbol, event.venue, 0.001);
return orders; // 直接返回，形状已经是 on_market_event 期望的数组
```

### 语义

- `diff = target_qty - current_qty`。
- 若 `|diff| < min_order_size`：返回 `DriftValue::Array([])`（空数组——差值太小，不值得下单）。
- 否则返回单元素数组：`[{ symbol, venue, side: diff > 0 ? "buy" : "sell", order_type: "market", quantity: |diff| }]`。
  - 这个返回对象的字段形状**必须**与 `convert.rs::order_intents_from_drift_value` 已经识别的 `OrderIntent` 字段一致（`symbol`/`venue`/`side`/`order_type`/`quantity`，`price`/`stop_price` 对 Market 订单省略——`convert.rs` 里 `optional_num_field` 缺省时返回 `None`，Market 订单不校验 `price`/`stop_price`，无需在此填充）。
  - `venue` 直接透传脚本传入的字符串（通常是 `event.venue`，已经是 `convert.rs` 输出的小写 venue 字符串，如 `"binance"`），不在这一层做 venue 合法性校验——留给下游 `order_intents_from_drift_value` 统一校验（该函数已有完整的 `parse_venue` 逻辑）。

### 校验失败（返回 `{error: "..."}`）

- `target_qty`/`current_qty`/`min_order_size` 不是 number，或缺少参数 → error
- `symbol` 不是非空字符串 → error
- `venue` 不是字符串 → error（此处只校验类型是字符串，不校验是否是已知 venue——合法性交给下游）
- `min_order_size < 0` → error（"min_order_size must be non-negative"）

---

## 六、绑定实现要点

与 `drift_dsl/indicators.rs` 完全一致的实现惯例（复用同样的辅助函数模式）：

```rust
pub fn register_to(interp: &Interpreter) {
    let mut signals_map: HashMap<String, DriftValue> = HashMap::new();
    signals_map.insert("combine".to_string(), DriftValue::NativeFunction(combine_fn()));
    signals_map.insert("target_position".to_string(), DriftValue::NativeFunction(target_position_fn()));
    signals_map.insert("rebalance_order".to_string(), DriftValue::NativeFunction(rebalance_order_fn()));
    interp.env().define_const("signals".to_string(), DriftValue::Object(signals_map));
}
```

- **无状态**：与 `indicators.*`（需要 `Arc<Mutex<_>>` 持久状态）不同，`signals.*` 的三个函数都是纯函数（无内部状态），每次调用独立计算，不需要脚本在顶层预先构造任何对象——`signals.combine(...)`/`signals.target_position(...)`/`signals.rebalance_order(...)` 可以直接在 `on_market_event` 内联调用，不需要在脚本顶层 `let x = signals.foo();` 预先创建实例（这是与 `indicators.*` 工厂函数模式的关键区别，必须在实现里体现——`signals_map` 直接注册三个可调用的 `NativeFunction`，不是"工厂返回带 update 方法的对象"）。
- **错误对象格式**：复用 `indicators.rs` 已有的 `error_object(message: String) -> DriftValue` 辅助函数的等价实现（`{error: message}` 的 `DriftValue::Object`）。
- **Clamp 辅助**：新增一个 `clamp_score(n: f64) -> f64 { n.clamp(-1.0, 1.0) }` 辅助函数，`combine`/`target_position` 都调用它处理输入分数。
- **参数提取辅助**：复用/新增与 `indicators.rs` 里 `require_number`/`require_positive_usize` 同构的辅助函数（如 `require_object_field_number`、`require_string`），保持代码风格一致。

---

## 七、测试策略

1. **`signals.rs` 自身单元测试**（与 `indicators.rs`、`bindings.rs` 同构风格，直接调用绑定层构造出的 `NativeFn`）：
   - `combine`：加权平均正确性（至少一个非平凡权重组合的手算用例）、空数组返回 `0.0`、全零权重返回 `0.0`、越界分数被 clamp（如传入 `1.5` 应等价于传入 `1.0`）、非数组参数报错、元素非对象报错、缺字段报错、负权重报错
   - `target_position`：死区内返回 `0`（`|score| == threshold` 边界，以及 `|score| < threshold`）、死区外线性缩放正确性（至少一个手算用例，如 `threshold=0.2, max=10, score=0.6` → `10*(0.6-0.2)/(1-0.2) = 5`）、`threshold >= 1.0` 时任何分数都返回 `0`、越界分数先 clamp 再计算、`max_position_size < 0` 报错、`threshold < 0` 报错
   - `rebalance_order`：`diff` 超过 `min_order_size` 时产生正确方向（buy/sell）与数量的单元素数组、`diff` 低于 `min_order_size` 时返回空数组、`diff` 恰好等于 `min_order_size` 边界（视为"未达到"，即 `|diff| < min_order_size` 是严格小于，等于时仍然不下单——与 `apexes-indicators` 设计里"预热期恰好在 period 时首次产生 Some"的边界处理风格一致，这里改为"清晰的严格小于"以避免浮点相等判断的脆弱性）、非法参数报错（各字段类型错误、`min_order_size < 0`）

2. **端到端 `.drift` fixture 测试**（新增 `tests/fixtures/uses_signals.drift` + 对应 driver 测试，追加到 `tests/drift_script_strategy.rs`）：
   - 脚本顶层创建一个 `indicators.sma(3)`（复用已有的指标绑定），`on_market_event` 内部：用当前价格相对 SMA 的偏离方向构造一个信号分数（如 `price > sma_value ? 0.8 : -0.8`），调用 `signals.combine([{score: ..., weight: 1}])` 得到复合分数，再 `signals.target_position(...)` 得到目标仓位，再用 `ctx.get_position(event.symbol).quantity` 作为当前仓位调用 `signals.rebalance_order(...)`，直接 `return` 结果。
   - 驱动测试验证：SMA 预热期（前几次调用）复合分数计算路径仍能正常工作但 SMA 部分为 `null`（脚本需要处理 `sma_value == null` 的情况，与 `apexes-indicators` 集成时一致的处理模式）；SMA 预热完成后，根据价格相对位置产生正确方向的 rebalance 订单；当目标仓位与当前仓位（初始为 0）的差值低于 `min_order_size` 时不产生订单。

3. **全工作区验证**：`cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --check`。

---

## 八、非目标（Non-goals，本次明确不做）

- 不新增 Rust 端 Signal trait/struct/crate
- 不做参数优化接口（`#4.4`）
- 不做策略版本管理/灰度发布（`#4.5`）
- 不做 PyO3 绑定
- `rebalance_order` 不支持 Market 以外的订单类型
- 不做跨 symbol 的仓位再平衡/资金分配优化
