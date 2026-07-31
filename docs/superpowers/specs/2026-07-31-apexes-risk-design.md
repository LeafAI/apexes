# Apexes — 子项目 #5：风控引擎（`apexes-risk`，事前+事中风控）设计

> 本文档是顶层路线图（[`2026-07-28-apexes-vision-and-roadmap.md`](2026-07-28-apexes-vision-and-roadmap.md)）中子项目 `#5`（风控引擎）的第一阶段详细设计，覆盖 `docs/FEATURES.md` 行 `6.1`（事前风控）+ `6.2`（事中风控）。`6.3`（事后风控与合规报表）归属子项目 `#15`（安全与合规），本文档不涉及。`6.4`（保证金杠杆监控，crypto derivatives focus）依赖尚不存在的杠杆持仓模型（`apexes-core`/`apexes-backtest` 目前只有 spot 仓位），留待未来有了该模型后再单独立项；`6.5`（极端行情熔断与策略自动降级）同样留待后续。
>
> 前置依赖：`apexes-core`（`Order`/`OrderStatus`/`Fill`/`Venue` 已完成）、`apexes-strategy`（`OrderIntent`/`Position`/`Strategy` trait 已完成）、`apexes-backtest`（`run_backtest` 事件循环、`PortfolioLedger` 已完成）。

**依据 vision 文档"关键推论"一节**：风控引擎的正确性不是"重要模块"而是"系统能否被信任无人值守运行"的决定性因素——本设计的每一项检查逻辑、每一个边界条件都必须有对应测试覆盖，不允许为了赶进度降低标准。

---

## 一、范围

### 依赖假设的偏离（需要明确记录的架构决策）

路线图原文对子项目 #5 的依赖标注是"0"（仅依赖 `apexes-core`）。但事前/事中风控检查天然需要消费 `OrderIntent`（策略提交的下单意图）和 `Position`（当前持仓），这两个类型都定义在 `apexes-strategy` crate，而不是 `apexes-core`。

**决策**：`apexes-risk` 依赖 `apexes-core` + `apexes-strategy`（而非路线图原始设想的仅 `apexes-core`）。这不违反项目"回测/模拟/实盘逻辑同源"的核心原则——`apexes-strategy` 本身就是三种运行模式共用的策略接口层，`apexes-risk` 依赖它，意味着风控检查天然对三种模式一视同仁，这与 `apexes-backtest` 依赖 `apexes-strategy` 是完全类比的架构选择（参见 `crates/apexes-backtest/Cargo.toml`）。

### 包含

1. **新增 workspace 成员 crate：`apexes-risk`**（依赖 `apexes-core` + `apexes-strategy`），提供两个独立、可自由组合的 trait：
   - **`PreTradeCheck`**（事前风控，对应 `6.1`）：在 `OrderIntent` 提交给撮合引擎/OMS 之前调用，返回 `Result<(), Vec<RiskViolation>>`——一次检查可能同时命中多条规则，全部收集返回，不是命中第一条就短路返回。
   - **`InTradeMonitor`**（事中风控，对应 `6.2`）：每次成交（`Fill`）产生后调用，用于检测"事前检查通过时未超限、但多笔部分成交累加后实际超限"的情况，同样返回 `Result<(), Vec<RiskViolation>>`。
2. **四个具体实现**，均可独立使用、也可通过一个组合器（`CompositeRiskChecker`，见 §五）合并成一次调用：
   - **`LimitChecker`**（`PreTradeCheck`）：限额检查——单笔订单数量/金额上限、单 symbol 持仓上限、账户总权益下限、单日交易频率/成交量上限。限额值通过 `RiskLimits` 结构体配置，支持运行时动态更新（`update_limits()`）。
   - **`SymbolVenueList`**（`PreTradeCheck`）：symbol 级 + venue 级黑名单/白名单。
   - **`SelfTradePreventer`**（`PreTradeCheck`）：自成交防范——检查新 `OrderIntent` 是否会与调用方已持有的反向 `open_orders` 产生价格区间重叠（Limit/StopLimit 订单按价格区间判断；Market 订单只要方向相反就直接拒绝，因为无法判断价格区间，保守起见直接拦截）。
   - **`CircuitBreaker`**（同时实现 `PreTradeCheck` + `InTradeMonitor`，对应 `6.2` 的持仓超限熔断）：`on_fill` 每次成交后重新检查持仓类限额（单 symbol 持仓、账户总权益）是否被突破（多笔部分成交累加导致的事后超限，事前检查无法拦截这种情况），超限则将对应粒度（symbol 级或账户级）置为"熔断中"状态；熔断期间 `check`（`PreTradeCheck`）直接拒绝该粒度下的所有新订单；持仓回落到限额以内时自动解除熔断，同时提供 `reset(scope)` 方法供人工/未来监控系统提前显式解除。
3. **`apexes-backtest` 的小增量**：在 `run_backtest` 的事件循环中，`strategy.on_market_event()` 产生 `OrderIntent` 之后、提交给 `MatchingEngine` 之前，插入 `PreTradeCheck` 检查；每次 `apply_fills` 完成后调用 `InTradeMonitor::on_fill`。这是本设计范围内 `apexes-backtest` 唯一需要修改的地方——`BacktestConfig` 新增一个可选字段承载风控检查器集合（见 §六）。

### 不包含（明确超出范围，留给对应后续子项目/后续轮次）

- **不做 `6.3`（事后风控与合规报表）**——归属子项目 `#15`。
- **不做 `6.4`（保证金杠杆监控）**——依赖尚不存在的杠杆持仓模型（`Position`/`PortfolioLedger` 目前只表达 spot 净仓位，没有杠杆倍数/保证金率/强平价等概念），留待未来有了该模型的子项目再做。
- **不做 `6.5`（极端行情熔断与策略自动降级）**——`CircuitBreaker` 本设计范围内只响应"持仓超限"这一种触发条件，不监控价格异常波动/极端行情，也不做"策略自动降级"（如切换到更保守的参数）这类更高层的自适应逻辑。
- **不做自动平仓**——`CircuitBreaker` 触发后只拒绝后续新订单，不会自动生成反向订单强制平掉已超限的仓位（避免首轮引入"风控系统自己下单"这个更复杂的反馈环路，见 brainstorming 阶段的明确决策）。
- **不做 `apexes-paper`（#6）/`apexes-oms`（#7）的集成**——本设计只把 `PreTradeCheck`/`InTradeMonitor` 接入 `apexes-backtest::run_backtest`，因为 #6/#7 尚未开始；但 trait 本身的设计（不依赖任何 backtest-only 的类型）保证了未来 #6/#7 集成时无需修改 `apexes-risk` 本身，只需在各自的下单/成交处理路径中调用同样的 trait 方法。
- **不做风控规则的持久化**——`RiskLimits`/黑白名单配置目前只存在于内存中（构造时传入 + 运行时 `update_limits()`），不落地到 ClickHouse 或任何数据库；持久化留给未来需要跨进程重启保留配置时再评估。

---

## 二、Crate 结构

```
crates/apexes-risk/
├── Cargo.toml
├── src/
│   ├── lib.rs              // pub mod 声明 + re-exports
│   ├── error.rs            // RiskViolation 枚举
│   ├── pre_trade.rs        // PreTradeCheck trait 定义
│   ├── in_trade.rs         // InTradeMonitor trait 定义
│   ├── limits.rs           // RiskLimits + LimitChecker
│   ├── lists.rs            // SymbolVenueList
│   ├── self_trade.rs       // SelfTradePreventer
│   ├── circuit_breaker.rs  // CircuitBreaker（PreTradeCheck + InTradeMonitor 双实现）
│   └── composite.rs        // CompositeRiskChecker（组合多个 checker，见 §五）
```

`Cargo.toml` 依赖：`apexes-core`、`apexes-strategy`、`rust_decimal`、`chrono`、`thiserror`（`RiskViolation` 用它派生 `Error`）。不依赖 `tokio`/`async-trait`——`PreTradeCheck`/`InTradeMonitor` 都是同步方法（风控检查是纯内存计算，不涉及网络 I/O，无需 async）。

---

## 三、`RiskViolation`（错误类型）

```rust
//! [`RiskViolation`]: describes exactly which risk rule was violated and
//! why, so callers can log/debug precisely which limit/list/self-trade/
//! circuit-breaker condition rejected an order or fill.

use apexes_core::Venue;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Error)]
pub enum RiskViolation {
    #[error("order quantity {actual} exceeds per-order limit {limit}")]
    OrderQuantityExceeded { actual: Decimal, limit: Decimal },

    #[error("order notional {actual} exceeds per-order notional limit {limit}")]
    OrderNotionalExceeded { actual: Decimal, limit: Decimal },

    #[error("position for {symbol} would reach {projected}, exceeding limit {limit}")]
    PositionLimitExceeded {
        symbol: String,
        projected: Decimal,
        limit: Decimal,
    },

    #[error("account equity {actual} is below the configured floor {floor}")]
    EquityFloorBreached { actual: Decimal, floor: Decimal },

    #[error("daily trade count {actual} would exceed limit {limit} (day: {day})")]
    DailyTradeCountExceeded {
        actual: u32,
        limit: u32,
        day: DateTime<Utc>,
    },

    #[error("daily traded volume {actual} would exceed limit {limit} (day: {day})")]
    DailyVolumeExceeded {
        actual: Decimal,
        limit: Decimal,
        day: DateTime<Utc>,
    },

    #[error("symbol {symbol} is blacklisted")]
    SymbolBlacklisted { symbol: String },

    #[error("symbol {symbol} is not in the whitelist")]
    SymbolNotWhitelisted { symbol: String },

    #[error("venue {venue:?} is blacklisted")]
    VenueBlacklisted { venue: Venue },

    #[error("venue {venue:?} is not in the whitelist")]
    VenueNotWhitelisted { venue: Venue },

    #[error(
        "order for {symbol} would self-trade against an existing opposite-side open order"
    )]
    SelfTradeRisk { symbol: String },

    #[error("circuit breaker is open for {scope:?}; new orders are rejected until it resets")]
    CircuitBreakerOpen { scope: CircuitBreakerScope },
}

/// Which granularity a [`crate::circuit_breaker::CircuitBreaker`] trip
/// applies to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CircuitBreakerScope {
    Symbol(String),
    Account,
}
```

---

## 四、`PreTradeCheck` / `InTradeMonitor` trait 定义

```rust
//! [`PreTradeCheck`]: called after a `Strategy::on_market_event` call
//! produces an `OrderIntent`, before that intent is submitted to a
//! matching engine (backtest) or OMS (future live trading). Every
//! implementation must be side-effect-free with respect to the
//! *decision* it returns -- i.e. calling `check` twice with identical
//! inputs must return identical results -- even though implementations
//! like `CircuitBreaker` do carry internal mutable state that `on_fill`
//! (via `InTradeMonitor`) updates between calls.

use apexes_core::{Order, OrderId};
use apexes_strategy::{OrderIntent, Position};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::error::RiskViolation;

pub trait PreTradeCheck: Send {
    /// Returns `Ok(())` if `intent` passes this check, or
    /// `Err(violations)` with every rule this single check instance
    /// found violated (a single `PreTradeCheck` implementation may
    /// itself detect more than one violation, e.g. `LimitChecker`
    /// checking both quantity AND notional in one call).
    fn check(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        open_orders: &HashMap<OrderId, Order>,
        now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>>;
}
```

```rust
//! [`InTradeMonitor`]: called after a `Fill` is applied to the ledger,
//! to catch violations that only become visible from *actual* fills
//! rather than a single proposed order (e.g. several partial fills
//! cumulatively pushing a position over its limit, which no single
//! `PreTradeCheck` call could have seen in advance since each
//! individual partial fill's originating order may have looked fine in
//! isolation).

use apexes_core::Fill;
use apexes_strategy::Position;
use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::error::RiskViolation;

pub trait InTradeMonitor: Send {
    /// Returns `Ok(())` if the post-fill state passes this monitor, or
    /// `Err(violations)` describing what was breached. Implementations
    /// that carry mutable state (e.g. `CircuitBreaker`) update that
    /// state as part of this call -- `on_fill` takes `&mut self`.
    fn on_fill(
        &mut self,
        fill: &Fill,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Result<(), Vec<RiskViolation>>;
}
```

**为什么 `PreTradeCheck::check` 是 `&self` 而 `InTradeMonitor::on_fill` 是 `&mut self`**：`LimitChecker`/`SymbolVenueList`/`SelfTradePreventer` 的判断逻辑纯粹基于传入的快照（`positions`/`balances`/`open_orders`），不需要维护跨调用的内部状态，因此 `check` 用 `&self` 足够，也方便多个调用方并发只读共享同一个 checker 实例。`CircuitBreaker` 的 `on_fill` 需要更新"当前是否处于熔断状态"这一内部可变字段，因此 `InTradeMonitor::on_fill` 统一定义为 `&mut self`——但 `CircuitBreaker` 实现的 `PreTradeCheck::check` 部分仍然是 `&self`（只读判断"当前是否在熔断中"，不在 `check` 里修改状态；状态只在 `on_fill` 里更新）。

---

## 五、四个具体实现

### 5.1 `RiskLimits` + `LimitChecker`

```rust
//! [`RiskLimits`]: the configurable threshold values `LimitChecker`
//! enforces. Constructed once, then optionally updated at runtime via
//! [`LimitChecker::update_limits`] -- e.g. a future live-trading
//! monitoring system tightening limits in response to detected market
//! stress, without requiring a process restart.

use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct RiskLimits {
    /// `None` means "no limit configured for this dimension" -- every
    /// field is optional so callers can enable only the dimensions they
    /// care about.
    pub max_order_quantity: Option<Decimal>,
    pub max_order_notional: Option<Decimal>,
    pub max_position_per_symbol: Option<Decimal>,
    /// Minimum acceptable total account equity (computed the same way
    /// `PortfolioLedger::equity` does) -- an order is rejected if
    /// executing it would be projected to drop equity below this floor.
    /// `LimitChecker` does not itself compute equity (it doesn't have
    /// access to `PortfolioLedger`); the caller passes current equity
    /// in via `balances`'s aggregate, per §5.1's `check` implementation
    /// notes below.
    pub min_account_equity: Option<Decimal>,
    pub max_daily_trade_count: Option<u32>,
    pub max_daily_volume: Option<Decimal>,
}

impl Default for RiskLimits {
    /// All limits unset (`None`) -- a `LimitChecker` built from
    /// `RiskLimits::default()` accepts every order (no-op checker),
    /// which is the correct "opt-in per dimension" default per §一.
    fn default() -> Self {
        Self {
            max_order_quantity: None,
            max_order_notional: None,
            max_position_per_symbol: None,
            min_account_equity: None,
            max_daily_trade_count: None,
            max_daily_volume: None,
        }
    }
}
```

```rust
//! [`LimitChecker`]: enforces [`RiskLimits`] as a [`PreTradeCheck`].
//! Tracks its own per-day trade count/volume state internally (reset
//! when `now`'s UTC calendar day changes from the previously observed
//! day), since neither figure is derivable from the `positions`/
//! `balances` snapshot `check` receives.

use std::cell::RefCell;
use std::collections::HashMap;

use apexes_core::{Order, OrderId};
use apexes_strategy::{OrderIntent, Position};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;

use crate::error::RiskViolation;
use crate::limits::RiskLimits;
use crate::pre_trade::PreTradeCheck;

pub struct LimitChecker {
    limits: RiskLimits,
    /// Interior-mutable daily counters: `check` takes `&self` per the
    /// `PreTradeCheck` trait signature, but tracking "how many trades
    /// happened today" inherently requires mutation on every check
    /// call. `RefCell` keeps the public API `&self` (matching every
    /// other stateless-looking `PreTradeCheck` impl) while still
    /// allowing this one field to mutate internally -- single-threaded
    /// backtest/paper/live event loops call `check` sequentially, never
    /// concurrently, so this is sound without needing `Mutex`.
    daily_state: RefCell<DailyState>,
}

struct DailyState {
    day: Option<NaiveDate>,
    trade_count: u32,
    traded_volume: Decimal,
}

impl LimitChecker {
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            daily_state: RefCell::new(DailyState {
                day: None,
                trade_count: 0,
                traded_volume: Decimal::ZERO,
            }),
        }
    }

    /// Replaces the currently enforced limits. Does not reset the
    /// internal daily counters -- a limit tightened mid-day applies
    /// against whatever count/volume has already accumulated that day.
    pub fn update_limits(&mut self, limits: RiskLimits) {
        self.limits = limits;
    }
}

impl PreTradeCheck for LimitChecker {
    fn check(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        _open_orders: &HashMap<OrderId, Order>,
        now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut violations = Vec::new();
        let today = now.date_naive();

        {
            let mut state = self.daily_state.borrow_mut();
            if state.day != Some(today) {
                state.day = Some(today);
                state.trade_count = 0;
                state.traded_volume = Decimal::ZERO;
            }
        }

        if let Some(max_qty) = self.limits.max_order_quantity
            && intent.quantity > max_qty
        {
            violations.push(RiskViolation::OrderQuantityExceeded {
                actual: intent.quantity,
                limit: max_qty,
            });
        }

        if let Some(max_notional) = self.limits.max_order_notional
            && let Some(price) = intent.price
        {
            let notional = intent.quantity * price;
            if notional > max_notional {
                violations.push(RiskViolation::OrderNotionalExceeded {
                    actual: notional,
                    limit: max_notional,
                });
            }
        }
        // Market orders (intent.price == None) have no notional to
        // check against here -- their actual fill price is unknown
        // until the matching engine executes them. This is an accepted
        // limitation: max_order_notional only applies to Limit/
        // StopLimit orders where a price is known in advance.

        if let Some(max_position) = self.limits.max_position_per_symbol {
            let current = positions
                .get(&intent.symbol)
                .map(|p| p.quantity)
                .unwrap_or(Decimal::ZERO);
            let signed_delta = match intent.side {
                apexes_core::Side::Buy => intent.quantity,
                apexes_core::Side::Sell => -intent.quantity,
            };
            let projected = (current + signed_delta).abs();
            if projected > max_position {
                violations.push(RiskViolation::PositionLimitExceeded {
                    symbol: intent.symbol.clone(),
                    projected,
                    limit: max_position,
                });
            }
        }

        if let Some(floor) = self.limits.min_account_equity {
            let total_balance: Decimal = balances.values().copied().sum();
            if total_balance < floor {
                violations.push(RiskViolation::EquityFloorBreached {
                    actual: total_balance,
                    floor,
                });
            }
        }

        {
            let state = self.daily_state.borrow();
            if let Some(max_count) = self.limits.max_daily_trade_count
                && state.trade_count + 1 > max_count
            {
                violations.push(RiskViolation::DailyTradeCountExceeded {
                    actual: state.trade_count + 1,
                    limit: max_count,
                    day: now,
                });
            }
            if let Some(max_volume) = self.limits.max_daily_volume
                && state.traded_volume + intent.quantity > max_volume
            {
                violations.push(RiskViolation::DailyVolumeExceeded {
                    actual: state.traded_volume + intent.quantity,
                    limit: max_volume,
                    day: now,
                });
            }
        }

        if violations.is_empty() {
            let mut state = self.daily_state.borrow_mut();
            state.trade_count += 1;
            state.traded_volume += intent.quantity;
            Ok(())
        } else {
            Err(violations)
        }
    }
}
```

**关于账户权益（equity）的计算口径**：`min_account_equity` 的检查用的是 `balances` 各资产余额的算术求和，而不是 `PortfolioLedger::equity`（后者是 mark-to-market，需要每个资产的最新价格换算成统一计价单位）。这是一个刻意的简化：`LimitChecker` 不依赖 `apexes-backtest`（避免循环依赖——`apexes-backtest` 反过来要依赖 `apexes-risk`），因此拿不到 `PortfolioLedger` 或最新价格表。调用方如果需要严格的 mark-to-market equity 下限检查，应在调用 `LimitChecker::check` 之前自行算好 equity，通过一个专门的机制传入（本设计范围内不做——`balances` 求和是"总资产名义价值下限"的一个可用近似，精确 mark-to-market 版本留给未来评估）。

### 5.2 `SymbolVenueList`

```rust
//! [`SymbolVenueList`]: symbol-level and venue-level allow/deny lists.
//! Each dimension (symbol, venue) is independently either a blacklist
//! (reject only listed values, allow everything else) or a whitelist
//! (allow only listed values, reject everything else) -- configured via
//! [`ListMode`]. A dimension with `ListMode::Disabled` performs no
//! check on that dimension at all.

use std::collections::HashMap;
use std::collections::HashSet;

use apexes_core::{Order, OrderId, Venue};
use apexes_strategy::{OrderIntent, Position};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::error::RiskViolation;
use crate::pre_trade::PreTradeCheck;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListMode<T: std::hash::Hash + Eq> {
    Disabled,
    Blacklist(HashSet<T>),
    Whitelist(HashSet<T>),
}

pub struct SymbolVenueList {
    symbols: ListMode<String>,
    venues: ListMode<Venue>,
}

impl SymbolVenueList {
    pub fn new(symbols: ListMode<String>, venues: ListMode<Venue>) -> Self {
        Self { symbols, venues }
    }
}

impl PreTradeCheck for SymbolVenueList {
    fn check(
        &self,
        intent: &OrderIntent,
        _positions: &HashMap<String, Position>,
        _balances: &HashMap<String, Decimal>,
        _open_orders: &HashMap<OrderId, Order>,
        _now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut violations = Vec::new();

        match &self.symbols {
            ListMode::Disabled => {}
            ListMode::Blacklist(set) if set.contains(&intent.symbol) => {
                violations.push(RiskViolation::SymbolBlacklisted {
                    symbol: intent.symbol.clone(),
                });
            }
            ListMode::Whitelist(set) if !set.contains(&intent.symbol) => {
                violations.push(RiskViolation::SymbolNotWhitelisted {
                    symbol: intent.symbol.clone(),
                });
            }
            _ => {}
        }

        match &self.venues {
            ListMode::Disabled => {}
            ListMode::Blacklist(set) if set.contains(&intent.venue) => {
                violations.push(RiskViolation::VenueBlacklisted {
                    venue: intent.venue,
                });
            }
            ListMode::Whitelist(set) if !set.contains(&intent.venue) => {
                violations.push(RiskViolation::VenueNotWhitelisted {
                    venue: intent.venue,
                });
            }
            _ => {}
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }
}
```

`Venue` 需要实现 `Hash`/`Eq` 才能放进 `HashSet` —— 已确认 `apexes_core::Venue` 的既有 derive 满足此要求（`Venue` 是 `#[derive(..., Hash, Eq, PartialEq, ...)]` 的枚举，参见 `crates/apexes-core/src/instrument.rs`）。

### 5.3 `SelfTradePreventer`

```rust
//! [`SelfTradePreventer`]: rejects an `OrderIntent` that would risk
//! trading against the caller's own resting opposite-side orders.
//! Limit/StopLimit orders are checked for actual price-range overlap;
//! Market orders are rejected outright against any opposite-side open
//! order for the same symbol, since a Market order's execution price
//! isn't known in advance and this check errs conservative.

use std::collections::HashMap;

use apexes_core::{Order, OrderId, OrderType, Side};
use apexes_strategy::{OrderIntent, Position};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::error::RiskViolation;
use crate::pre_trade::PreTradeCheck;

pub struct SelfTradePreventer;

impl PreTradeCheck for SelfTradePreventer {
    fn check(
        &self,
        intent: &OrderIntent,
        _positions: &HashMap<String, Position>,
        _balances: &HashMap<String, Decimal>,
        open_orders: &HashMap<OrderId, Order>,
        _now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let opposite_side = match intent.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let risky = open_orders.values().any(|order| {
            if order.symbol != intent.symbol || order.side != opposite_side {
                return false;
            }

            match (intent.order_type, intent.price, order.price) {
                // Market intent against any resting opposite-side order
                // for this symbol: reject outright (no price to compare).
                (OrderType::Market, _, _) => true,
                // Limit/StopLimit intent against a resting order that
                // also has a price: overlap check. A Buy overlaps a
                // resting Sell if buy_price >= sell_price (the buy would
                // be willing to pay at least as much as the resting sell
                // wants); a Sell overlaps a resting Buy if
                // sell_price <= buy_price (symmetric case).
                (_, Some(intent_price), Some(order_price)) => match intent.side {
                    Side::Buy => intent_price >= order_price,
                    Side::Sell => intent_price <= order_price,
                },
                // Intent has a price but the resting order doesn't (the
                // resting order is itself a Market order, which
                // shouldn't normally still be "open" since Markets fill
                // immediately -- but if it somehow is, treat as risky
                // conservatively rather than assuming safe).
                (_, Some(_), None) => true,
                // Intent has no price (shouldn't happen for non-Market
                // order_type per apexes_strategy::OrderIntent's field
                // semantics, but handled defensively): treat as risky.
                (_, None, _) => true,
            }
        });

        if risky {
            Err(vec![RiskViolation::SelfTradeRisk {
                symbol: intent.symbol.clone(),
            }])
        } else {
            Ok(())
        }
    }
}
```

### 5.4 `CircuitBreaker`

```rust
//! [`CircuitBreaker`]: trips (opens) when a fill causes a position-type
//! limit (per-symbol position or account equity) to be breached --
//! catching violations that accumulate across multiple partial fills,
//! which no single `PreTradeCheck` call could see coming since each
//! individual originating order may have looked fine in isolation.
//! While open, `PreTradeCheck::check` rejects every new order in that
//! scope. Auto-resets once the breaching condition clears (position
//! back within limits); `reset` allows early manual override.

use std::collections::{HashMap, HashSet};

use apexes_core::{Fill, Order, OrderId};
use apexes_strategy::{OrderIntent, Position};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::error::{CircuitBreakerScope, RiskViolation};
use crate::in_trade::InTradeMonitor;
use crate::limits::RiskLimits;
use crate::pre_trade::PreTradeCheck;

pub struct CircuitBreaker {
    limits: RiskLimits,
    /// Symbols currently tripped (per-symbol position limit breached).
    tripped_symbols: HashSet<String>,
    /// Whether the account-wide equity floor is currently breached.
    account_tripped: bool,
}

impl CircuitBreaker {
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            tripped_symbols: HashSet::new(),
            account_tripped: false,
        }
    }

    /// Manually clears a trip before its auto-reset condition (position
    /// back within limits) would otherwise clear it -- e.g. a human
    /// operator confirming the position is intentional and safe to
    /// resume trading against.
    pub fn reset(&mut self, scope: &CircuitBreakerScope) {
        match scope {
            CircuitBreakerScope::Symbol(symbol) => {
                self.tripped_symbols.remove(symbol);
            }
            CircuitBreakerScope::Account => {
                self.account_tripped = false;
            }
        }
    }

    pub fn is_tripped(&self, scope: &CircuitBreakerScope) -> bool {
        match scope {
            CircuitBreakerScope::Symbol(symbol) => self.tripped_symbols.contains(symbol),
            CircuitBreakerScope::Account => self.account_tripped,
        }
    }
}

impl InTradeMonitor for CircuitBreaker {
    fn on_fill(
        &mut self,
        fill: &Fill,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut violations = Vec::new();

        if let Some(max_position) = self.limits.max_position_per_symbol {
            let current = positions
                .get(&fill.symbol)
                .map(|p| p.quantity.abs())
                .unwrap_or(Decimal::ZERO);
            if current > max_position {
                self.tripped_symbols.insert(fill.symbol.clone());
                violations.push(RiskViolation::PositionLimitExceeded {
                    symbol: fill.symbol.clone(),
                    projected: current,
                    limit: max_position,
                });
            } else {
                // Auto-reset: position back within limits.
                self.tripped_symbols.remove(&fill.symbol);
            }
        }

        if let Some(floor) = self.limits.min_account_equity {
            let total_balance: Decimal = balances.values().copied().sum();
            if total_balance < floor {
                self.account_tripped = true;
                violations.push(RiskViolation::EquityFloorBreached {
                    actual: total_balance,
                    floor,
                });
            } else {
                self.account_tripped = false;
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }
}

impl PreTradeCheck for CircuitBreaker {
    fn check(
        &self,
        intent: &OrderIntent,
        _positions: &HashMap<String, Position>,
        _balances: &HashMap<String, Decimal>,
        _open_orders: &HashMap<OrderId, Order>,
        _now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut violations = Vec::new();

        if self.tripped_symbols.contains(&intent.symbol) {
            violations.push(RiskViolation::CircuitBreakerOpen {
                scope: CircuitBreakerScope::Symbol(intent.symbol.clone()),
            });
        }
        if self.account_tripped {
            violations.push(RiskViolation::CircuitBreakerOpen {
                scope: CircuitBreakerScope::Account,
            });
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }
}
```

**`CircuitBreaker` 与 `LimitChecker` 都持有 `RiskLimits` 里的 `max_position_per_symbol`/`min_account_equity` 字段，是否重复**：是刻意的、非冗余的重复——两者检查的时机和目的完全不同。`LimitChecker::check`（事前）用当前快照 + 本次意图 **预测** 成交后是否会超限，在超限**之前**拒绝；`CircuitBreaker::on_fill`（事中）在成交**之后**用实际持仓验证是否**已经**超限（因为多笔部分成交的净效应，事前检查无法精确预测每一笔的最终结果），一旦确认超限就熔断后续所有新单，直到持仓回落。两者是同一份限额配置在两个不同检查阶段的应用，都需要各自独立持有配置副本（否则 `LimitChecker` 和 `CircuitBreaker` 必须共享同一个 `RiskLimits` 实例，这会在 `update_limits` 语义上引入额外的同步复杂度，本设计不采用共享方案）。

---

## 六、`CompositeRiskChecker`（组合器）+ `apexes-backtest` 集成

```rust
//! [`CompositeRiskChecker`]: runs multiple `PreTradeCheck`/
//! `InTradeMonitor` implementations in sequence, aggregating every
//! violation from every checker into one combined result -- so a caller
//! (e.g. `run_backtest`) only needs to hold and invoke one object
//! regardless of how many individual risk rules are configured.

use std::collections::HashMap;

use apexes_core::{Fill, Order, OrderId};
use apexes_strategy::{OrderIntent, Position};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::error::RiskViolation;
use crate::in_trade::InTradeMonitor;
use crate::pre_trade::PreTradeCheck;

#[derive(Default)]
pub struct CompositeRiskChecker {
    pre_trade_checks: Vec<Box<dyn PreTradeCheck>>,
    in_trade_monitors: Vec<Box<dyn InTradeMonitor>>,
}

impl CompositeRiskChecker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_pre_trade_check(mut self, check: Box<dyn PreTradeCheck>) -> Self {
        self.pre_trade_checks.push(check);
        self
    }

    pub fn with_in_trade_monitor(mut self, monitor: Box<dyn InTradeMonitor>) -> Self {
        self.in_trade_monitors.push(monitor);
        self
    }

    /// Runs every registered `PreTradeCheck`, collecting every
    /// violation from every checker (not stopping at the first
    /// checker that fails) into one combined `Vec`.
    pub fn check(
        &self,
        intent: &OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
        open_orders: &HashMap<OrderId, Order>,
        now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let violations: Vec<RiskViolation> = self
            .pre_trade_checks
            .iter()
            .filter_map(|checker| {
                checker
                    .check(intent, positions, balances, open_orders, now)
                    .err()
            })
            .flatten()
            .collect();

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    /// Runs every registered `InTradeMonitor`'s `on_fill`, collecting
    /// every violation from every monitor into one combined `Vec`.
    pub fn on_fill(
        &mut self,
        fill: &Fill,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Result<(), Vec<RiskViolation>> {
        let violations: Vec<RiskViolation> = self
            .in_trade_monitors
            .iter_mut()
            .filter_map(|monitor| monitor.on_fill(fill, positions, balances).err())
            .flatten()
            .collect();

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }
}
```

**`CircuitBreaker` 同时实现两个 trait，如何注册进 `CompositeRiskChecker`**：由于 `CompositeRiskChecker` 持有 `Box<dyn PreTradeCheck>` 和 `Box<dyn InTradeMonitor>` 两个独立的 trait-object 列表，同一个 `CircuitBreaker` 实例无法同时以两种 trait object 形式存在于两个 `Vec` 里（`Box<dyn PreTradeCheck>` 和 `Box<dyn InTradeMonitor>` 是不同类型，且 `CircuitBreaker` 的状态在两处需要是同一份，不能各自 `Box` 一份独立拷贝）。解决方式：用 `Arc<Mutex<CircuitBreaker>>` 包一层，分别注册两个转发适配器（`CircuitBreakerPreTradeAdapter(Arc<Mutex<CircuitBreaker>>)` 实现 `PreTradeCheck`、`CircuitBreakerInTradeAdapter(Arc<Mutex<CircuitBreaker>>)` 实现 `InTradeMonitor`，两者共享同一个 `Arc`），这样两处调用最终都作用于同一个底层状态。这两个适配器结构体定义在 `circuit_breaker.rs` 里，作为 `CircuitBreaker` 自身注册进 `CompositeRiskChecker` 的标准方式（不要求调用方自己手写适配器）。

### `apexes-backtest` 集成

`crates/apexes-backtest/src/runner.rs` 的 `BacktestConfig` 新增一个字段：

```rust
pub struct BacktestConfig {
    // ...既有字段不变...
    /// Optional risk checks run before submitting each `OrderIntent` and
    /// after each `Fill`. `None` (the default via a new `with_risk_checker`
    /// builder or explicit `None`) means no risk checking -- existing
    /// callers that construct `BacktestConfig` without this field
    /// continue to compile and behave exactly as before (backward
    /// compatible; see Global Constraints in the implementation plan for
    /// the exact mechanism, e.g. `#[derive(Default)]` on a newtype
    /// wrapper or an `Option` field with `None` as the struct-update
    /// baseline).
    pub risk_checker: Option<apexes_risk::CompositeRiskChecker>,
}
```

在 `run_backtest`'s 事件循环里：

```rust
for intent in intents {
    if let Some(risk_checker) = &config.risk_checker
        && let Err(violations) = risk_checker.check(
            &intent,
            ledger.positions(),
            ledger.balances(),
            &open_orders,
            timestamp,
        )
    {
        tracing::warn!(?violations, symbol = %intent.symbol, "order rejected by risk checker");
        continue; // Skip submission entirely -- same "log and move on"
                   // pattern the existing `Err(error)` branch below
                   // already uses for matching-engine rejections.
    }

    let submit_result = match intent.order_type {
        // ...unchanged...
    };
    // ...
}
```

`apply_fills` 之后（每批 fills 处理完）：

```rust
if let Some(risk_checker) = &mut config.risk_checker {
    for fill in &fills {
        if let Err(violations) = risk_checker.on_fill(fill, ledger.positions(), ledger.balances()) {
            tracing::warn!(?violations, symbol = %fill.symbol, "in-trade risk monitor flagged a violation");
        }
        // Note: on_fill violations are logged, not propagated as an
        // error -- per §一's explicit "no auto-liquidation" decision,
        // an in-trade violation (e.g. CircuitBreaker tripping) doesn't
        // undo the fill that already happened; it only affects FUTURE
        // PreTradeCheck::check calls for that scope.
    }
}
```

`config` 需要在 `run_backtest`'s 函数签名里从 `config: BacktestConfig` 改为 `mut config: BacktestConfig`（因为 `risk_checker.on_fill` 需要 `&mut`）——这是本设计范围内 `run_backtest` 函数签名的唯一改动。

---

## 七、测试策略

1. **`RiskViolation`/`CircuitBreakerScope` 单元测试**：`Display` 输出格式验证（沿用项目既有错误类型测试风格）。
2. **`LimitChecker` 单元测试**：单笔数量超限拒绝且返回正确的 `OrderQuantityExceeded`；单笔金额超限（仅 Limit/StopLimit，验证 Market 订单不做 notional 检查）；单 symbol 持仓预测超限（分别验证 Buy 增加多头/ Sell 增加空头两个方向）；账户权益低于下限拒绝；单日交易次数/成交量超限（验证跨自然日后计数器重置——用两个不同日期的 `now` 调用验证重置发生）；一次调用同时命中多条规则时 `Vec` 包含全部违规项；`update_limits` 后新配置立即生效于下一次 `check` 调用。
3. **`SymbolVenueList` 单元测试**：symbol 黑名单命中拒绝；symbol 白名单未命中拒绝；venue 黑/白名单同理；`ListMode::Disabled` 时该维度不做任何检查；symbol 和 venue 同时命中时两个违规项都出现在返回的 `Vec` 里。
4. **`SelfTradePreventer` 单元测试**：Limit Buy intent 价格 >= 已有反向 Sell open order 价格时拒绝；Limit Buy intent 价格 < 已有反向 Sell open order 价格时通过（不重叠，安全）；对称的 Sell 场景；Market intent 只要有任何反向 open order（不论价格）就拒绝；同方向 open order 存在时不触发（不是自成交风险）；不同 symbol 的反向 open order 不触发。
5. **`CircuitBreaker` 单元测试**：`on_fill` 后持仓超过 `max_position_per_symbol` 触发该 symbol 的熔断，之后 `check` 对该 symbol 的新意图返回 `CircuitBreakerOpen`，对其他 symbol 的意图不受影响；持仓通过后续 `on_fill` 回落到限额以内后自动解除熔断（后续 `check` 恢复通过）；`reset` 显式提前解除熔断验证；账户权益熔断同理（`Account` 粒度，验证熔断后**所有** symbol 的 `check` 都被拒绝，因为账户级熔断范围更广）。
6. **`CompositeRiskChecker` 单元测试**：注册多个 `PreTradeCheck` 时，多个 checker 各自的违规项被合并进同一个 `Vec`（用两个都会拒绝同一个 intent 的 mock checker 验证）；只要有一个 checker 通过、其余全部拒绝时整体仍返回 `Err`（不是"多数通过就算通过"）；全部 checker 都通过时返回 `Ok`；`CircuitBreaker` 通过共享 `Arc<Mutex<..>>` 适配器同时注册为 `PreTradeCheck` 和 `InTradeMonitor` 后，`on_fill` 触发的熔断状态确实反映在后续 `check` 调用里（验证两个适配器共享同一份底层状态，而非各自独立）。
7. **`apexes-backtest` 集成测试**（新增 `crates/apexes-backtest/tests/risk_integration.rs` 或类似文件）：一个手工构造的确定性行情+策略场景，验证 `BacktestConfig.risk_checker` 设置了会拒绝所有订单的 checker 时，`BacktestReport.trades` 为空（所有意图都被拒绝，从未到达撮合引擎）；`risk_checker` 为 `None` 时行为与未设置该字段前完全一致（回归测试，确认新增字段不破坏现有行为）；一个会在特定条件下熔断的场景，验证熔断前的订单正常成交、熔断触发后的订单被拒绝、持仓回落后恢复正常。
8. **全工作区验证**：`cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --check`。

---

## 八、非目标（Non-goals，本次明确不做）

- 不做 `6.3`（事后风控与合规报表，归属 `#15`）
- 不做 `6.4`（保证金杠杆监控，依赖尚不存在的杠杆持仓模型）
- 不做 `6.5`（极端行情熔断与策略自动降级）
- 不做自动平仓/自动生成反向订单
- 不做 `apexes-paper`（#6）/`apexes-oms`（#7）的实际集成代码（接口设计保证未来可无痛复用，但本次不写集成代码，因为这两个子项目尚未开始）
- 不做风控规则的持久化存储（配置只在内存中，跨进程重启不保留）
- 不做精确的 mark-to-market equity 计算（`min_account_equity` 检查用 `balances` 简单求和作为近似，见 §5.1 说明）
