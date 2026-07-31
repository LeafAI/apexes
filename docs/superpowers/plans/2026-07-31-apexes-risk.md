# Apexes-Risk: Pre-Trade & In-Trade Risk Controls Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `apexes-risk` crate providing pluggable `PreTradeCheck`/`InTradeMonitor` traits with four concrete implementations (`LimitChecker`, `SymbolVenueList`, `SelfTradePreventer`, `CircuitBreaker`) plus a `CompositeRiskChecker` aggregator, then wire it into `apexes-backtest::run_backtest`.

**Architecture:** `apexes-risk` depends on `apexes-core` + `apexes-strategy` (a deliberate deviation from the roadmap's original "depends on apexes-core only" assumption, since risk checks consume `OrderIntent`/`Position` which live in `apexes-strategy`). `PreTradeCheck::check` is called after `Strategy::on_market_event` produces an `OrderIntent`, before matching-engine submission; `InTradeMonitor::on_fill` is called after each `Fill` is applied to the ledger. `BacktestConfig` gains an optional `risk_checker: Option<apexes_risk::CompositeRiskChecker>` field.

**Tech Stack:** Rust edition 2024, `apexes-core`, `apexes-strategy`, `rust_decimal`, `chrono`, `thiserror` — no `tokio`/`async-trait` (risk checks are synchronous, in-memory only).

## Global Constraints

- Design spec of record: `docs/superpowers/specs/2026-07-31-apexes-risk-design.md`. If you find a mismatch between this plan and that spec while implementing, the spec wins -- stop and flag it rather than improvising.
- Rust edition `2024`, matching `apexes`' workspace (`Cargo.toml` `[workspace.package] edition = "2024"`).
- **This sub-project covers ONLY `docs/FEATURES.md` rows `6.1` (pre-trade) and `6.2` (in-trade)** -- row `6.3` belongs to sub-project `#15`, rows `6.4`/`6.5` are explicitly out of scope for this plan (no leverage/margin model exists yet; no extreme-market circuit breaker; no automatic strategy de-risking).
- **No automatic liquidation** -- `CircuitBreaker` only rejects future new orders when tripped; it never generates a reverse order to force-close an existing position.
- **No new storage backend** -- all risk configuration (`RiskLimits`, black/white lists) lives in memory only, constructed at startup and optionally updated at runtime via explicit methods; nothing is persisted to ClickHouse or any database.
- Every commit in the `apexes` repo uses `git commit -S -s` (signed + signed-off), matching the project's established convention. Direct commits to `master` (no branches/PRs).
- `cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --check` must all pass clean after every task that touches compiled code.
- `apexes-risk`'s `PreTradeCheck`/`InTradeMonitor` traits and their four implementations are fully synchronous (no `async fn`, no `#[async_trait]`) -- risk checks are pure in-memory computation with no I/O.
- `BacktestConfig` has NO `Default` impl and is constructed via exhaustive struct literals at 7 existing call sites across 3 crates (`apexes-backtest/src/runner.rs` tests, `apexes-backtest/tests/end_to_end.rs` x3, `apexes-backtest/tests/live_storage_backtest.rs`, `apexes-optimize/tests/grid_search_end_to_end.rs`, `apexes-strategy-registry/tests/registry_end_to_end.rs`). Adding a new field to `BacktestConfig` will fail to compile at ALL of these call sites until each one is updated -- Task 7 of this plan updates every single one; do not skip any.

---

### Task 1: Crate scaffold + `error.rs` (`RiskViolation`, `CircuitBreakerScope`)

**Files:**
- Modify: `Cargo.toml` (add `"crates/apexes-risk"` to `members`)
- Create: `crates/apexes-risk/Cargo.toml`
- Create: `crates/apexes-risk/src/error.rs`
- Create: `crates/apexes-risk/src/lib.rs`

**Interfaces:**
- Produces: `pub enum RiskViolation`, `pub enum CircuitBreakerScope` (both in `crate::error`). Consumed by every later task in this plan.

- [x] **Step 1: Register the new crate in the workspace**

```bash
cd ~/src/oss/apexes
cat Cargo.toml
```

Edit `Cargo.toml`'s `members` list:

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage", "crates/apexes-backtest", "crates/apexes-strategy", "crates/apexes-indicators", "crates/apexes-optimize", "crates/apexes-strategy-registry", "crates/apexes-risk"]
```

- [x] **Step 2: Create `crates/apexes-risk/Cargo.toml`**

```toml
[package]
name = "apexes-risk"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-strategy = { path = "../apexes-strategy" }
rust_decimal = { workspace = true }
chrono = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
uuid = { workspace = true, features = ["v4"] }
```

- [x] **Step 3: Write `crates/apexes-risk/src/error.rs`**

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

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn order_quantity_exceeded_display() {
        let err = RiskViolation::OrderQuantityExceeded {
            actual: dec!(10),
            limit: dec!(5),
        };
        assert_eq!(
            err.to_string(),
            "order quantity 10 exceeds per-order limit 5"
        );
    }

    #[test]
    fn symbol_blacklisted_display() {
        let err = RiskViolation::SymbolBlacklisted {
            symbol: "BTCUSDT".to_string(),
        };
        assert_eq!(err.to_string(), "symbol BTCUSDT is blacklisted");
    }

    #[test]
    fn circuit_breaker_open_display_for_symbol_scope() {
        let err = RiskViolation::CircuitBreakerOpen {
            scope: CircuitBreakerScope::Symbol("BTCUSDT".to_string()),
        };
        assert_eq!(
            err.to_string(),
            "circuit breaker is open for Symbol(\"BTCUSDT\"); new orders are rejected until it resets"
        );
    }

    #[test]
    fn circuit_breaker_open_display_for_account_scope() {
        let err = RiskViolation::CircuitBreakerOpen {
            scope: CircuitBreakerScope::Account,
        };
        assert_eq!(
            err.to_string(),
            "circuit breaker is open for Account; new orders are rejected until it resets"
        );
    }

    #[test]
    fn circuit_breaker_scope_equality() {
        assert_eq!(
            CircuitBreakerScope::Symbol("BTCUSDT".to_string()),
            CircuitBreakerScope::Symbol("BTCUSDT".to_string())
        );
        assert_ne!(CircuitBreakerScope::Account, CircuitBreakerScope::Symbol("BTCUSDT".to_string()));
    }
}
```

- [x] **Step 4: Write `crates/apexes-risk/src/lib.rs`**

```rust
//! Apexes risk engine: pluggable pre-trade (`PreTradeCheck`) and
//! in-trade (`InTradeMonitor`) risk controls, shared across backtest,
//! paper, and live trading per the project's non-negotiable
//! backtest/live logic-parity requirement. Covers `docs/FEATURES.md`
//! rows 6.1 (pre-trade: limits, black/white lists, self-trade
//! prevention) and 6.2 (in-trade: position-limit circuit breaker).

pub mod error;

pub use error::{CircuitBreakerScope, RiskViolation};
```

- [x] **Step 5: Run the error module's tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-risk error:: -- --nocapture
```
Expected: 5 passed, 0 failed.

- [x] **Step 6: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-risk --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run both commands).

- [x] **Step 7: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [x] **Step 8: Commit**

```bash
cd ~/src/oss/apexes
git add Cargo.toml crates/apexes-risk
git commit -S -s -m "feat: add apexes-risk crate scaffold with RiskViolation error type"
git push origin master
```

---

### Task 2: `PreTradeCheck` / `InTradeMonitor` trait definitions

**Files:**
- Create: `crates/apexes-risk/src/pre_trade.rs`
- Create: `crates/apexes-risk/src/in_trade.rs`
- Modify: `crates/apexes-risk/src/lib.rs`

**Interfaces:**
- Consumes: `crate::error::RiskViolation` (Task 1), `apexes_core::{Order, OrderId, Fill}`, `apexes_strategy::{OrderIntent, Position}` (existing).
- Produces: `pub trait PreTradeCheck`, `pub trait InTradeMonitor`. Consumed by Tasks 3-6's concrete implementations and Task 7's `CompositeRiskChecker`/backtest integration.

- [x] **Step 1: Write `crates/apexes-risk/src/pre_trade.rs`**

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

- [x] **Step 2: Write `crates/apexes-risk/src/in_trade.rs`**

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

- [x] **Step 3: Update `crates/apexes-risk/src/lib.rs`**

```rust
//! Apexes risk engine: pluggable pre-trade (`PreTradeCheck`) and
//! in-trade (`InTradeMonitor`) risk controls, shared across backtest,
//! paper, and live trading per the project's non-negotiable
//! backtest/live logic-parity requirement. Covers `docs/FEATURES.md`
//! rows 6.1 (pre-trade: limits, black/white lists, self-trade
//! prevention) and 6.2 (in-trade: position-limit circuit breaker).

pub mod error;
pub mod in_trade;
pub mod pre_trade;

pub use error::{CircuitBreakerScope, RiskViolation};
pub use in_trade::InTradeMonitor;
pub use pre_trade::PreTradeCheck;
```

- [x] **Step 4: Verify it compiles**

```bash
cd ~/src/oss/apexes
cargo check -p apexes-risk
```
Expected: succeeds, 0 errors. Traits with no implementations yet compile fine on their own.

- [x] **Step 5: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-risk --all-targets -- -D warnings
cargo fmt --check
```
Expected clean.

- [x] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-risk/src
git commit -S -s -m "feat: add PreTradeCheck and InTradeMonitor traits to apexes-risk"
git push origin master
```

---

### Task 3: `RiskLimits` + `LimitChecker`

**Files:**
- Create: `crates/apexes-risk/src/limits.rs`
- Modify: `crates/apexes-risk/src/lib.rs`

**Interfaces:**
- Consumes: `crate::pre_trade::PreTradeCheck`, `crate::error::RiskViolation` (Tasks 1-2), `apexes_strategy::{OrderIntent, Position}`, `apexes_core::{Order, OrderId, Side}` (existing).
- Produces: `pub struct RiskLimits`, `pub struct LimitChecker` with `new`/`update_limits`. Consumed by Task 6's `CircuitBreaker` (shares the `RiskLimits` type) and Task 7's `CompositeRiskChecker`/backtest integration tests.

- [x] **Step 1: Write `crates/apexes-risk/src/limits.rs`**

```rust
//! [`RiskLimits`]: the configurable threshold values [`LimitChecker`]
//! enforces. Constructed once, then optionally updated at runtime via
//! [`LimitChecker::update_limits`] -- e.g. a future live-trading
//! monitoring system tightening limits in response to detected market
//! stress, without requiring a process restart.
//!
//! [`LimitChecker`]: enforces [`RiskLimits`] as a [`crate::pre_trade::PreTradeCheck`].
//! Tracks its own per-day trade count/volume state internally (reset
//! when `now`'s UTC calendar day changes from the previously observed
//! day), since neither figure is derivable from the `positions`/
//! `balances` snapshot `check` receives.

use std::cell::RefCell;
use std::collections::HashMap;

use apexes_core::{Order, OrderId, Side};
use apexes_strategy::{OrderIntent, Position};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;

use crate::error::RiskViolation;
use crate::pre_trade::PreTradeCheck;

#[derive(Debug, Clone)]
pub struct RiskLimits {
    /// `None` means "no limit configured for this dimension" -- every
    /// field is optional so callers can enable only the dimensions they
    /// care about.
    pub max_order_quantity: Option<Decimal>,
    pub max_order_notional: Option<Decimal>,
    pub max_position_per_symbol: Option<Decimal>,
    /// Minimum acceptable total account balance (summed across all
    /// assets in `balances`, NOT a mark-to-market equity calculation --
    /// see this crate's design spec §5.1 for why: `LimitChecker` has no
    /// access to `PortfolioLedger` or current mark prices without
    /// introducing a circular dependency on `apexes-backtest`). An
    /// order is rejected if the CURRENT balance sum (before the order
    /// executes) is already below this floor.
    pub min_account_equity: Option<Decimal>,
    pub max_daily_trade_count: Option<u32>,
    pub max_daily_volume: Option<Decimal>,
}

impl Default for RiskLimits {
    /// All limits unset (`None`) -- a `LimitChecker` built from
    /// `RiskLimits::default()` accepts every order (no-op checker),
    /// which is the correct "opt-in per dimension" default.
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
                Side::Buy => intent.quantity,
                Side::Sell => -intent.quantity,
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

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::Venue;
    use rust_decimal_macros::dec;

    fn sample_intent(symbol: &str, side: Side, quantity: Decimal, price: Option<Decimal>) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            side,
            order_type: if price.is_some() {
                apexes_core::OrderType::Limit
            } else {
                apexes_core::OrderType::Market
            },
            quantity,
            price,
            stop_price: None,
        }
    }

    fn empty_positions() -> HashMap<String, Position> {
        HashMap::new()
    }

    fn empty_balances() -> HashMap<String, Decimal> {
        HashMap::new()
    }

    fn empty_open_orders() -> HashMap<OrderId, Order> {
        HashMap::new()
    }

    fn now() -> DateTime<Utc> {
        chrono::Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap()
    }

    use chrono::TimeZone;

    #[test]
    fn order_quantity_within_limit_passes() {
        let checker = LimitChecker::new(RiskLimits {
            max_order_quantity: Some(dec!(10)),
            ..Default::default()
        });
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(5), None);
        let result = checker.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert!(result.is_ok());
    }

    #[test]
    fn order_quantity_exceeding_limit_is_rejected() {
        let checker = LimitChecker::new(RiskLimits {
            max_order_quantity: Some(dec!(10)),
            ..Default::default()
        });
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(15), None);
        let result = checker.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert_eq!(
            result,
            Err(vec![RiskViolation::OrderQuantityExceeded {
                actual: dec!(15),
                limit: dec!(10),
            }])
        );
    }

    #[test]
    fn order_notional_exceeding_limit_is_rejected_for_limit_orders() {
        let checker = LimitChecker::new(RiskLimits {
            max_order_notional: Some(dec!(1000)),
            ..Default::default()
        });
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(1), Some(dec!(2000)));
        let result = checker.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert_eq!(
            result,
            Err(vec![RiskViolation::OrderNotionalExceeded {
                actual: dec!(2000),
                limit: dec!(1000),
            }])
        );
    }

    #[test]
    fn market_orders_are_not_checked_against_notional_limit() {
        let checker = LimitChecker::new(RiskLimits {
            max_order_notional: Some(dec!(1)),
            ..Default::default()
        });
        // Market order has no price -> notional cannot be computed ->
        // this dimension is skipped entirely, not treated as a failure.
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(1000), None);
        let result = checker.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert!(result.is_ok());
    }

    #[test]
    fn position_limit_exceeded_on_buy_increasing_long_position() {
        let checker = LimitChecker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let mut positions = HashMap::new();
        positions.insert(
            "BTCUSDT".to_string(),
            Position {
                symbol: "BTCUSDT".to_string(),
                quantity: dec!(8),
                average_entry_price: dec!(100),
            },
        );
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(5), None);
        let result = checker.check(&intent, &positions, &empty_balances(), &empty_open_orders(), now());
        assert_eq!(
            result,
            Err(vec![RiskViolation::PositionLimitExceeded {
                symbol: "BTCUSDT".to_string(),
                projected: dec!(13),
                limit: dec!(10),
            }])
        );
    }

    #[test]
    fn position_limit_exceeded_on_sell_increasing_short_position() {
        let checker = LimitChecker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let mut positions = HashMap::new();
        positions.insert(
            "BTCUSDT".to_string(),
            Position {
                symbol: "BTCUSDT".to_string(),
                quantity: dec!(-8),
                average_entry_price: dec!(100),
            },
        );
        let intent = sample_intent("BTCUSDT", Side::Sell, dec!(5), None);
        let result = checker.check(&intent, &positions, &empty_balances(), &empty_open_orders(), now());
        assert_eq!(
            result,
            Err(vec![RiskViolation::PositionLimitExceeded {
                symbol: "BTCUSDT".to_string(),
                projected: dec!(13),
                limit: dec!(10),
            }])
        );
    }

    #[test]
    fn account_equity_below_floor_is_rejected() {
        let checker = LimitChecker::new(RiskLimits {
            min_account_equity: Some(dec!(1000)),
            ..Default::default()
        });
        let mut balances = HashMap::new();
        balances.insert("USDT".to_string(), dec!(500));
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(1), None);
        let result = checker.check(&intent, &empty_positions(), &balances, &empty_open_orders(), now());
        assert_eq!(
            result,
            Err(vec![RiskViolation::EquityFloorBreached {
                actual: dec!(500),
                floor: dec!(1000),
            }])
        );
    }

    #[test]
    fn daily_trade_count_resets_across_calendar_days() {
        let checker = LimitChecker::new(RiskLimits {
            max_daily_trade_count: Some(1),
            ..Default::default()
        });
        let day1 = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let day2 = chrono::Utc.with_ymd_and_hms(2026, 1, 2, 12, 0, 0).unwrap();
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(1), None);

        // First trade on day 1: passes (count 0 -> 1, within limit 1).
        assert!(checker
            .check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), day1)
            .is_ok());
        // Second trade on day 1: rejected (would be count 2, exceeds limit 1).
        assert!(checker
            .check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), day1)
            .is_err());
        // First trade on day 2: passes again (counter reset for new day).
        assert!(checker
            .check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), day2)
            .is_ok());
    }

    #[test]
    fn daily_volume_limit_is_enforced_cumulatively() {
        let checker = LimitChecker::new(RiskLimits {
            max_daily_volume: Some(dec!(10)),
            ..Default::default()
        });
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(6), None);
        assert!(checker
            .check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now())
            .is_ok());
        // Second call: 6 (already traded) + 6 (this intent) = 12 > 10 -> rejected.
        let result = checker.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert!(result.is_err());
    }

    #[test]
    fn multiple_violated_rules_are_all_returned() {
        let checker = LimitChecker::new(RiskLimits {
            max_order_quantity: Some(dec!(1)),
            max_order_notional: Some(dec!(1)),
            ..Default::default()
        });
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(100), Some(dec!(100)));
        let result = checker.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        let violations = result.expect_err("expected both rules to be violated");
        assert_eq!(violations.len(), 2);
        assert!(violations.contains(&RiskViolation::OrderQuantityExceeded {
            actual: dec!(100),
            limit: dec!(1),
        }));
        assert!(violations.contains(&RiskViolation::OrderNotionalExceeded {
            actual: dec!(10000),
            limit: dec!(1),
        }));
    }

    #[test]
    fn update_limits_takes_effect_on_next_check() {
        let mut checker = LimitChecker::new(RiskLimits {
            max_order_quantity: Some(dec!(100)),
            ..Default::default()
        });
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(50), None);
        assert!(checker
            .check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now())
            .is_ok());

        checker.update_limits(RiskLimits {
            max_order_quantity: Some(dec!(10)),
            ..Default::default()
        });
        let result = checker.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert!(result.is_err());
    }

    #[test]
    fn default_limits_accept_every_order() {
        let checker = LimitChecker::new(RiskLimits::default());
        let intent = sample_intent("BTCUSDT", Side::Buy, dec!(1_000_000), Some(dec!(1_000_000)));
        let result = checker.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert!(result.is_ok());
    }
}
```

- [x] **Step 2: Update `crates/apexes-risk/src/lib.rs`**

```rust
//! Apexes risk engine: pluggable pre-trade (`PreTradeCheck`) and
//! in-trade (`InTradeMonitor`) risk controls, shared across backtest,
//! paper, and live trading per the project's non-negotiable
//! backtest/live logic-parity requirement. Covers `docs/FEATURES.md`
//! rows 6.1 (pre-trade: limits, black/white lists, self-trade
//! prevention) and 6.2 (in-trade: position-limit circuit breaker).

pub mod error;
pub mod in_trade;
pub mod limits;
pub mod pre_trade;

pub use error::{CircuitBreakerScope, RiskViolation};
pub use in_trade::InTradeMonitor;
pub use limits::{LimitChecker, RiskLimits};
pub use pre_trade::PreTradeCheck;
```

- [x] **Step 3: Run the tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-risk limits:: -- --nocapture
```
Expected: 12 passed, 0 failed.

- [x] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-risk --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run both commands plus the test command to confirm nothing regressed).

- [x] **Step 5: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [x] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-risk/src
git commit -S -s -m "feat: add RiskLimits and LimitChecker to apexes-risk"
git push origin master
```

---

### Task 4: `SymbolVenueList`

**Files:**
- Create: `crates/apexes-risk/src/lists.rs`
- Modify: `crates/apexes-risk/src/lib.rs`

**Interfaces:**
- Consumes: `crate::pre_trade::PreTradeCheck`, `crate::error::RiskViolation` (Tasks 1-2), `apexes_core::Venue` (existing, already derives `Hash`/`Eq`).
- Produces: `pub enum ListMode<T>`, `pub struct SymbolVenueList`. Consumed by Task 7's `CompositeRiskChecker`/backtest integration tests.

- [x] **Step 1: Write `crates/apexes-risk/src/lists.rs`**

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

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{OrderType, Side};
    use chrono::TimeZone;

    fn sample_intent(symbol: &str, venue: Venue) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: rust_decimal_macros::dec!(1),
            price: None,
            stop_price: None,
        }
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap()
    }

    fn empty_positions() -> HashMap<String, Position> {
        HashMap::new()
    }

    fn empty_balances() -> HashMap<String, Decimal> {
        HashMap::new()
    }

    fn empty_open_orders() -> HashMap<OrderId, Order> {
        HashMap::new()
    }

    #[test]
    fn symbol_blacklist_rejects_listed_symbol() {
        let mut blacklist = HashSet::new();
        blacklist.insert("BTCUSDT".to_string());
        let list = SymbolVenueList::new(ListMode::Blacklist(blacklist), ListMode::Disabled);
        let intent = sample_intent("BTCUSDT", Venue::Binance);
        let result = list.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert_eq!(
            result,
            Err(vec![RiskViolation::SymbolBlacklisted {
                symbol: "BTCUSDT".to_string(),
            }])
        );
    }

    #[test]
    fn symbol_blacklist_allows_unlisted_symbol() {
        let mut blacklist = HashSet::new();
        blacklist.insert("BTCUSDT".to_string());
        let list = SymbolVenueList::new(ListMode::Blacklist(blacklist), ListMode::Disabled);
        let intent = sample_intent("ETHUSDT", Venue::Binance);
        let result = list.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert!(result.is_ok());
    }

    #[test]
    fn symbol_whitelist_rejects_unlisted_symbol() {
        let mut whitelist = HashSet::new();
        whitelist.insert("BTCUSDT".to_string());
        let list = SymbolVenueList::new(ListMode::Whitelist(whitelist), ListMode::Disabled);
        let intent = sample_intent("ETHUSDT", Venue::Binance);
        let result = list.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert_eq!(
            result,
            Err(vec![RiskViolation::SymbolNotWhitelisted {
                symbol: "ETHUSDT".to_string(),
            }])
        );
    }

    #[test]
    fn symbol_whitelist_allows_listed_symbol() {
        let mut whitelist = HashSet::new();
        whitelist.insert("BTCUSDT".to_string());
        let list = SymbolVenueList::new(ListMode::Whitelist(whitelist), ListMode::Disabled);
        let intent = sample_intent("BTCUSDT", Venue::Binance);
        let result = list.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert!(result.is_ok());
    }

    #[test]
    fn disabled_symbol_dimension_never_rejects() {
        let list = SymbolVenueList::new(ListMode::Disabled, ListMode::Disabled);
        let intent = sample_intent("ANYTHING", Venue::Binance);
        let result = list.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert!(result.is_ok());
    }

    #[test]
    fn venue_blacklist_rejects_listed_venue() {
        let mut blacklist = HashSet::new();
        blacklist.insert(Venue::Okx);
        let list = SymbolVenueList::new(ListMode::Disabled, ListMode::Blacklist(blacklist));
        let intent = sample_intent("BTCUSDT", Venue::Okx);
        let result = list.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert_eq!(
            result,
            Err(vec![RiskViolation::VenueBlacklisted { venue: Venue::Okx }])
        );
    }

    #[test]
    fn venue_whitelist_rejects_unlisted_venue() {
        let mut whitelist = HashSet::new();
        whitelist.insert(Venue::Binance);
        let list = SymbolVenueList::new(ListMode::Disabled, ListMode::Whitelist(whitelist));
        let intent = sample_intent("BTCUSDT", Venue::Okx);
        let result = list.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        assert_eq!(
            result,
            Err(vec![RiskViolation::VenueNotWhitelisted { venue: Venue::Okx }])
        );
    }

    #[test]
    fn symbol_and_venue_violations_both_returned_when_both_hit() {
        let mut symbol_blacklist = HashSet::new();
        symbol_blacklist.insert("BTCUSDT".to_string());
        let mut venue_blacklist = HashSet::new();
        venue_blacklist.insert(Venue::Okx);
        let list = SymbolVenueList::new(
            ListMode::Blacklist(symbol_blacklist),
            ListMode::Blacklist(venue_blacklist),
        );
        let intent = sample_intent("BTCUSDT", Venue::Okx);
        let result = list.check(&intent, &empty_positions(), &empty_balances(), &empty_open_orders(), now());
        let violations = result.expect_err("expected both symbol and venue violations");
        assert_eq!(violations.len(), 2);
        assert!(violations.contains(&RiskViolation::SymbolBlacklisted {
            symbol: "BTCUSDT".to_string(),
        }));
        assert!(violations.contains(&RiskViolation::VenueBlacklisted { venue: Venue::Okx }));
    }
}
```

- [x] **Step 2: Update `crates/apexes-risk/src/lib.rs`**

```rust
//! Apexes risk engine: pluggable pre-trade (`PreTradeCheck`) and
//! in-trade (`InTradeMonitor`) risk controls, shared across backtest,
//! paper, and live trading per the project's non-negotiable
//! backtest/live logic-parity requirement. Covers `docs/FEATURES.md`
//! rows 6.1 (pre-trade: limits, black/white lists, self-trade
//! prevention) and 6.2 (in-trade: position-limit circuit breaker).

pub mod error;
pub mod in_trade;
pub mod limits;
pub mod lists;
pub mod pre_trade;

pub use error::{CircuitBreakerScope, RiskViolation};
pub use in_trade::InTradeMonitor;
pub use limits::{LimitChecker, RiskLimits};
pub use lists::{ListMode, SymbolVenueList};
pub use pre_trade::PreTradeCheck;
```

- [x] **Step 3: Run the tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-risk lists:: -- --nocapture
```
Expected: 8 passed, 0 failed.

- [x] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-risk --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run both commands plus the test command).

- [x] **Step 5: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [x] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-risk/src
git commit -S -s -m "feat: add SymbolVenueList to apexes-risk"
git push origin master
```

---

### Task 5: `SelfTradePreventer`

**Files:**
- Create: `crates/apexes-risk/src/self_trade.rs`
- Modify: `crates/apexes-risk/src/lib.rs`

**Interfaces:**
- Consumes: `crate::pre_trade::PreTradeCheck`, `crate::error::RiskViolation` (Tasks 1-2), `apexes_core::{Order, OrderId, OrderType, Side}` (existing).
- Produces: `pub struct SelfTradePreventer`. Consumed by Task 7's `CompositeRiskChecker`/backtest integration tests.

- [x] **Step 1: Write `crates/apexes-risk/src/self_trade.rs`**

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

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{OrderStatus, Venue};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap()
    }

    fn empty_positions() -> HashMap<String, Position> {
        HashMap::new()
    }

    fn empty_balances() -> HashMap<String, Decimal> {
        HashMap::new()
    }

    fn limit_intent(symbol: &str, side: Side, price: Decimal) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            side,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(price),
            stop_price: None,
        }
    }

    fn market_intent(symbol: &str, side: Side) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            side,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
        }
    }

    fn resting_limit_order(symbol: &str, side: Side, price: Decimal) -> (OrderId, Order) {
        let id = OrderId(Uuid::new_v4());
        let order = Order {
            id,
            exchange_order_id: None,
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            side,
            order_type: OrderType::Limit,
            quantity: dec!(1),
            price: Some(price),
            stop_price: None,
            filled_quantity: dec!(0),
            status: OrderStatus::New,
            created_at: now(),
            updated_at: now(),
        };
        (id, order)
    }

    fn open_orders_with(order: (OrderId, Order)) -> HashMap<OrderId, Order> {
        let mut map = HashMap::new();
        map.insert(order.0, order.1);
        map
    }

    #[test]
    fn buy_overlapping_resting_sell_price_is_rejected() {
        let preventer = SelfTradePreventer;
        let open_orders = open_orders_with(resting_limit_order("BTCUSDT", Side::Sell, dec!(100)));
        let intent = limit_intent("BTCUSDT", Side::Buy, dec!(100));
        let result = preventer.check(&intent, &empty_positions(), &empty_balances(), &open_orders, now());
        assert_eq!(
            result,
            Err(vec![RiskViolation::SelfTradeRisk {
                symbol: "BTCUSDT".to_string(),
            }])
        );
    }

    #[test]
    fn buy_below_resting_sell_price_does_not_overlap() {
        let preventer = SelfTradePreventer;
        let open_orders = open_orders_with(resting_limit_order("BTCUSDT", Side::Sell, dec!(100)));
        let intent = limit_intent("BTCUSDT", Side::Buy, dec!(90));
        let result = preventer.check(&intent, &empty_positions(), &empty_balances(), &open_orders, now());
        assert!(result.is_ok());
    }

    #[test]
    fn sell_overlapping_resting_buy_price_is_rejected() {
        let preventer = SelfTradePreventer;
        let open_orders = open_orders_with(resting_limit_order("BTCUSDT", Side::Buy, dec!(100)));
        let intent = limit_intent("BTCUSDT", Side::Sell, dec!(100));
        let result = preventer.check(&intent, &empty_positions(), &empty_balances(), &open_orders, now());
        assert!(result.is_err());
    }

    #[test]
    fn sell_above_resting_buy_price_does_not_overlap() {
        let preventer = SelfTradePreventer;
        let open_orders = open_orders_with(resting_limit_order("BTCUSDT", Side::Buy, dec!(100)));
        let intent = limit_intent("BTCUSDT", Side::Sell, dec!(110));
        let result = preventer.check(&intent, &empty_positions(), &empty_balances(), &open_orders, now());
        assert!(result.is_ok());
    }

    #[test]
    fn market_intent_rejected_against_any_opposite_side_open_order() {
        let preventer = SelfTradePreventer;
        let open_orders = open_orders_with(resting_limit_order("BTCUSDT", Side::Sell, dec!(999999)));
        let intent = market_intent("BTCUSDT", Side::Buy);
        let result = preventer.check(&intent, &empty_positions(), &empty_balances(), &open_orders, now());
        assert!(result.is_err());
    }

    #[test]
    fn same_side_open_order_never_triggers_self_trade_risk() {
        let preventer = SelfTradePreventer;
        let open_orders = open_orders_with(resting_limit_order("BTCUSDT", Side::Buy, dec!(100)));
        let intent = limit_intent("BTCUSDT", Side::Buy, dec!(100));
        let result = preventer.check(&intent, &empty_positions(), &empty_balances(), &open_orders, now());
        assert!(result.is_ok());
    }

    #[test]
    fn different_symbol_open_order_never_triggers_self_trade_risk() {
        let preventer = SelfTradePreventer;
        let open_orders = open_orders_with(resting_limit_order("ETHUSDT", Side::Sell, dec!(1)));
        let intent = limit_intent("BTCUSDT", Side::Buy, dec!(999999));
        let result = preventer.check(&intent, &empty_positions(), &empty_balances(), &open_orders, now());
        assert!(result.is_ok());
    }

    #[test]
    fn no_open_orders_never_triggers_self_trade_risk() {
        let preventer = SelfTradePreventer;
        let intent = limit_intent("BTCUSDT", Side::Buy, dec!(100));
        let result = preventer.check(&intent, &empty_positions(), &empty_balances(), &HashMap::new(), now());
        assert!(result.is_ok());
    }
}
```

- [x] **Step 2: Update `crates/apexes-risk/src/lib.rs`**

```rust
//! Apexes risk engine: pluggable pre-trade (`PreTradeCheck`) and
//! in-trade (`InTradeMonitor`) risk controls, shared across backtest,
//! paper, and live trading per the project's non-negotiable
//! backtest/live logic-parity requirement. Covers `docs/FEATURES.md`
//! rows 6.1 (pre-trade: limits, black/white lists, self-trade
//! prevention) and 6.2 (in-trade: position-limit circuit breaker).

pub mod error;
pub mod in_trade;
pub mod limits;
pub mod lists;
pub mod pre_trade;
pub mod self_trade;

pub use error::{CircuitBreakerScope, RiskViolation};
pub use in_trade::InTradeMonitor;
pub use limits::{LimitChecker, RiskLimits};
pub use lists::{ListMode, SymbolVenueList};
pub use pre_trade::PreTradeCheck;
pub use self_trade::SelfTradePreventer;
```

- [x] **Step 3: Run the tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-risk self_trade:: -- --nocapture
```
Expected: 8 passed, 0 failed.

- [x] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-risk --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run both commands plus the test command).

- [x] **Step 5: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [x] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-risk/src
git commit -S -s -m "feat: add SelfTradePreventer to apexes-risk"
git push origin master
```

---

### Task 6: `CircuitBreaker`

**Files:**
- Create: `crates/apexes-risk/src/circuit_breaker.rs`
- Modify: `crates/apexes-risk/src/lib.rs`

**Interfaces:**
- Consumes: `crate::pre_trade::PreTradeCheck`, `crate::in_trade::InTradeMonitor`, `crate::error::{RiskViolation, CircuitBreakerScope}`, `crate::limits::RiskLimits` (Tasks 1-3), `apexes_core::Fill`, `apexes_strategy::Position` (existing).
- Produces: `pub struct CircuitBreaker` with `new`/`reset`/`is_tripped`, plus two adapter structs `pub struct CircuitBreakerPreTradeAdapter` and `pub struct CircuitBreakerInTradeAdapter` (both wrapping `Arc<Mutex<CircuitBreaker>>`) and a `CircuitBreaker::split()` constructor helper. Consumed by Task 7's `CompositeRiskChecker`/backtest integration.

- [x] **Step 1: Write `crates/apexes-risk/src/circuit_breaker.rs`**

```rust
//! [`CircuitBreaker`]: trips (opens) when a fill causes a position-type
//! limit (per-symbol position or account equity) to be breached --
//! catching violations that accumulate across multiple partial fills,
//! which no single `PreTradeCheck` call could see coming since each
//! individual originating order may have looked fine in isolation.
//! While open, `PreTradeCheck::check` rejects every new order in that
//! scope. Auto-resets once the breaching condition clears (position
//! back within limits); `reset` allows early manual override.
//!
//! Because `CircuitBreaker` must be registered as BOTH a
//! `PreTradeCheck` (§`CircuitBreakerPreTradeAdapter`) and an
//! `InTradeMonitor` (§`CircuitBreakerInTradeAdapter`) sharing the exact
//! same underlying state, [`CircuitBreaker::split`] wraps one
//! `CircuitBreaker` in an `Arc<Mutex<_>>` and returns two thin adapter
//! structs, each implementing one trait by locking the shared mutex.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

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

    /// Wraps `self` in `Arc<Mutex<_>>` and returns two adapters sharing
    /// that same instance: the first implements `PreTradeCheck`, the
    /// second implements `InTradeMonitor`. Register both with a
    /// `CompositeRiskChecker` (Task 7) to get a circuit breaker that
    /// both rejects new orders while tripped AND updates its own
    /// tripped state from fills.
    pub fn split(self) -> (CircuitBreakerPreTradeAdapter, CircuitBreakerInTradeAdapter) {
        let shared = Arc::new(Mutex::new(self));
        (
            CircuitBreakerPreTradeAdapter(Arc::clone(&shared)),
            CircuitBreakerInTradeAdapter(shared),
        )
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

    fn on_fill_impl(
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

    fn check_impl(&self, intent: &OrderIntent) -> Result<(), Vec<RiskViolation>> {
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

impl InTradeMonitor for CircuitBreaker {
    fn on_fill(
        &mut self,
        fill: &Fill,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Result<(), Vec<RiskViolation>> {
        self.on_fill_impl(fill, positions, balances)
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
        self.check_impl(intent)
    }
}

/// Shares a [`CircuitBreaker`]'s state as a [`PreTradeCheck`]. See
/// [`CircuitBreaker::split`].
pub struct CircuitBreakerPreTradeAdapter(Arc<Mutex<CircuitBreaker>>);

impl PreTradeCheck for CircuitBreakerPreTradeAdapter {
    fn check(
        &self,
        intent: &OrderIntent,
        _positions: &HashMap<String, Position>,
        _balances: &HashMap<String, Decimal>,
        _open_orders: &HashMap<OrderId, Order>,
        _now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let guard = self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.check_impl(intent)
    }
}

/// Shares a [`CircuitBreaker`]'s state as an [`InTradeMonitor`]. See
/// [`CircuitBreaker::split`].
pub struct CircuitBreakerInTradeAdapter(Arc<Mutex<CircuitBreaker>>);

impl InTradeMonitor for CircuitBreakerInTradeAdapter {
    fn on_fill(
        &mut self,
        fill: &Fill,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut guard = self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.on_fill_impl(fill, positions, balances)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{OrderType, Side, Venue};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap()
    }

    fn sample_intent(symbol: &str) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
        }
    }

    fn sample_fill(symbol: &str) -> Fill {
        Fill {
            id: apexes_core::FillId(Uuid::new_v4()),
            order_id: apexes_core::OrderId(Uuid::new_v4()),
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            price: dec!(100),
            quantity: dec!(1),
            fee: dec!(0),
            fee_currency: None,
            filled_at: now(),
        }
    }

    fn positions_with(symbol: &str, quantity: Decimal) -> HashMap<String, Position> {
        let mut map = HashMap::new();
        map.insert(
            symbol.to_string(),
            Position {
                symbol: symbol.to_string(),
                quantity,
                average_entry_price: dec!(100),
            },
        );
        map
    }

    fn empty_balances() -> HashMap<String, Decimal> {
        HashMap::new()
    }

    fn empty_open_orders() -> HashMap<OrderId, Order> {
        HashMap::new()
    }

    #[test]
    fn position_over_limit_trips_that_symbol_only() {
        let mut breaker = CircuitBreaker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let fill = sample_fill("BTCUSDT");
        let result = breaker.on_fill(&fill, &positions_with("BTCUSDT", dec!(15)), &empty_balances());
        assert!(result.is_err());

        // BTCUSDT is tripped.
        let btc_intent = sample_intent("BTCUSDT");
        assert_eq!(
            breaker.check(&btc_intent, &HashMap::new(), &empty_balances(), &empty_open_orders(), now()),
            Err(vec![RiskViolation::CircuitBreakerOpen {
                scope: CircuitBreakerScope::Symbol("BTCUSDT".to_string()),
            }])
        );

        // ETHUSDT is unaffected.
        let eth_intent = sample_intent("ETHUSDT");
        assert!(breaker
            .check(&eth_intent, &HashMap::new(), &empty_balances(), &empty_open_orders(), now())
            .is_ok());
    }

    #[test]
    fn position_back_within_limit_auto_resets() {
        let mut breaker = CircuitBreaker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let fill = sample_fill("BTCUSDT");
        breaker
            .on_fill(&fill, &positions_with("BTCUSDT", dec!(15)), &empty_balances())
            .unwrap_err();
        assert!(breaker.is_tripped(&CircuitBreakerScope::Symbol("BTCUSDT".to_string())));

        // A subsequent fill brings the position back within the limit.
        breaker
            .on_fill(&fill, &positions_with("BTCUSDT", dec!(5)), &empty_balances())
            .unwrap();
        assert!(!breaker.is_tripped(&CircuitBreakerScope::Symbol("BTCUSDT".to_string())));

        let intent = sample_intent("BTCUSDT");
        assert!(breaker
            .check(&intent, &HashMap::new(), &empty_balances(), &empty_open_orders(), now())
            .is_ok());
    }

    #[test]
    fn manual_reset_clears_a_trip_early() {
        let mut breaker = CircuitBreaker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let fill = sample_fill("BTCUSDT");
        breaker
            .on_fill(&fill, &positions_with("BTCUSDT", dec!(15)), &empty_balances())
            .unwrap_err();
        assert!(breaker.is_tripped(&CircuitBreakerScope::Symbol("BTCUSDT".to_string())));

        breaker.reset(&CircuitBreakerScope::Symbol("BTCUSDT".to_string()));
        assert!(!breaker.is_tripped(&CircuitBreakerScope::Symbol("BTCUSDT".to_string())));
    }

    #[test]
    fn account_equity_trip_rejects_every_symbol() {
        let mut breaker = CircuitBreaker::new(RiskLimits {
            min_account_equity: Some(dec!(1000)),
            ..Default::default()
        });
        let fill = sample_fill("BTCUSDT");
        let mut low_balance = HashMap::new();
        low_balance.insert("USDT".to_string(), dec!(500));
        breaker.on_fill(&fill, &HashMap::new(), &low_balance).unwrap_err();
        assert!(breaker.is_tripped(&CircuitBreakerScope::Account));

        let btc_intent = sample_intent("BTCUSDT");
        let eth_intent = sample_intent("ETHUSDT");
        assert!(breaker
            .check(&btc_intent, &HashMap::new(), &empty_balances(), &empty_open_orders(), now())
            .is_err());
        assert!(breaker
            .check(&eth_intent, &HashMap::new(), &empty_balances(), &empty_open_orders(), now())
            .is_err());
    }

    #[test]
    fn split_adapters_share_the_same_underlying_state() {
        let breaker = CircuitBreaker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let (pre_trade, mut in_trade) = breaker.split();

        let intent = sample_intent("BTCUSDT");
        assert!(pre_trade
            .check(&intent, &HashMap::new(), &empty_balances(), &empty_open_orders(), now())
            .is_ok());

        let fill = sample_fill("BTCUSDT");
        in_trade
            .on_fill(&fill, &positions_with("BTCUSDT", dec!(15)), &empty_balances())
            .unwrap_err();

        // The trip recorded via the in-trade adapter must be visible
        // through the pre-trade adapter, since both wrap the same
        // underlying CircuitBreaker via Arc<Mutex<_>>.
        assert!(pre_trade
            .check(&intent, &HashMap::new(), &empty_balances(), &empty_open_orders(), now())
            .is_err());
    }
}
```

- [x] **Step 2: Update `crates/apexes-risk/src/lib.rs`**

```rust
//! Apexes risk engine: pluggable pre-trade (`PreTradeCheck`) and
//! in-trade (`InTradeMonitor`) risk controls, shared across backtest,
//! paper, and live trading per the project's non-negotiable
//! backtest/live logic-parity requirement. Covers `docs/FEATURES.md`
//! rows 6.1 (pre-trade: limits, black/white lists, self-trade
//! prevention) and 6.2 (in-trade: position-limit circuit breaker).

pub mod circuit_breaker;
pub mod error;
pub mod in_trade;
pub mod limits;
pub mod lists;
pub mod pre_trade;
pub mod self_trade;

pub use circuit_breaker::{CircuitBreaker, CircuitBreakerInTradeAdapter, CircuitBreakerPreTradeAdapter};
pub use error::{CircuitBreakerScope, RiskViolation};
pub use in_trade::InTradeMonitor;
pub use limits::{LimitChecker, RiskLimits};
pub use lists::{ListMode, SymbolVenueList};
pub use pre_trade::PreTradeCheck;
pub use self_trade::SelfTradePreventer;
```

- [x] **Step 3: Run the tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-risk circuit_breaker:: -- --nocapture
```
Expected: 5 passed, 0 failed.

- [x] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-risk --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run both commands plus the test command). If clippy flags the `unwrap_or_else(|poisoned| poisoned.into_inner())` pattern, keep it as-is -- it is the standard idiomatic way to recover from a poisoned `Mutex` without panicking, and is intentional here (a panic inside one risk-check call must not poison the mutex for all future calls).

- [x] **Step 5: Run the full `apexes-risk` test suite**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-risk
```
Expected: all tests from Tasks 1-6 pass (5 error + 12 limits + 8 lists + 8 self_trade + 5 circuit_breaker = 38 tests), 0 failed.

- [x] **Step 6: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [x] **Step 7: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-risk/src
git commit -S -s -m "feat: add CircuitBreaker to apexes-risk"
git push origin master
```

---

### Task 7: `CompositeRiskChecker`

**Files:**
- Create: `crates/apexes-risk/src/composite.rs`
- Modify: `crates/apexes-risk/src/lib.rs`

**Interfaces:**
- Consumes: `crate::pre_trade::PreTradeCheck`, `crate::in_trade::InTradeMonitor`, `crate::error::RiskViolation` (Tasks 1-2). Consumed by Task 8's `apexes-backtest` integration.
- Produces: `pub struct CompositeRiskChecker` with `new`/`with_pre_trade_check`/`with_in_trade_monitor`/`check`/`on_fill`.

- [x] **Step 1: Write `crates/apexes-risk/src/composite.rs`**

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

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{OrderType, Side, Venue};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    struct AlwaysRejectPreTrade(RiskViolation);

    impl PreTradeCheck for AlwaysRejectPreTrade {
        fn check(
            &self,
            _intent: &OrderIntent,
            _positions: &HashMap<String, Position>,
            _balances: &HashMap<String, Decimal>,
            _open_orders: &HashMap<OrderId, Order>,
            _now: DateTime<Utc>,
        ) -> Result<(), Vec<RiskViolation>> {
            Err(vec![self.0.clone()])
        }
    }

    struct AlwaysPassPreTrade;

    impl PreTradeCheck for AlwaysPassPreTrade {
        fn check(
            &self,
            _intent: &OrderIntent,
            _positions: &HashMap<String, Position>,
            _balances: &HashMap<String, Decimal>,
            _open_orders: &HashMap<OrderId, Order>,
            _now: DateTime<Utc>,
        ) -> Result<(), Vec<RiskViolation>> {
            Ok(())
        }
    }

    struct AlwaysRejectInTrade(RiskViolation);

    impl InTradeMonitor for AlwaysRejectInTrade {
        fn on_fill(
            &mut self,
            _fill: &Fill,
            _positions: &HashMap<String, Position>,
            _balances: &HashMap<String, Decimal>,
        ) -> Result<(), Vec<RiskViolation>> {
            Err(vec![self.0.clone()])
        }
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap()
    }

    fn sample_intent() -> OrderIntent {
        OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
        }
    }

    fn sample_fill() -> Fill {
        Fill {
            id: apexes_core::FillId(Uuid::new_v4()),
            order_id: apexes_core::OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            price: dec!(100),
            quantity: dec!(1),
            fee: dec!(0),
            fee_currency: None,
            filled_at: now(),
        }
    }

    #[test]
    fn no_checkers_registered_always_passes() {
        let composite = CompositeRiskChecker::new();
        let result = composite.check(
            &sample_intent(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn violations_from_multiple_checkers_are_all_aggregated() {
        let composite = CompositeRiskChecker::new()
            .with_pre_trade_check(Box::new(AlwaysRejectPreTrade(
                RiskViolation::SymbolBlacklisted {
                    symbol: "BTCUSDT".to_string(),
                },
            )))
            .with_pre_trade_check(Box::new(AlwaysRejectPreTrade(
                RiskViolation::SelfTradeRisk {
                    symbol: "BTCUSDT".to_string(),
                },
            )));
        let result = composite.check(
            &sample_intent(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            now(),
        );
        let violations = result.expect_err("expected both checkers' violations");
        assert_eq!(violations.len(), 2);
    }

    #[test]
    fn one_failing_checker_among_passing_ones_still_rejects() {
        let composite = CompositeRiskChecker::new()
            .with_pre_trade_check(Box::new(AlwaysPassPreTrade))
            .with_pre_trade_check(Box::new(AlwaysRejectPreTrade(
                RiskViolation::SymbolBlacklisted {
                    symbol: "BTCUSDT".to_string(),
                },
            )))
            .with_pre_trade_check(Box::new(AlwaysPassPreTrade));
        let result = composite.check(
            &sample_intent(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            now(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn all_checkers_passing_returns_ok() {
        let composite = CompositeRiskChecker::new()
            .with_pre_trade_check(Box::new(AlwaysPassPreTrade))
            .with_pre_trade_check(Box::new(AlwaysPassPreTrade));
        let result = composite.check(
            &sample_intent(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn on_fill_aggregates_violations_from_multiple_monitors() {
        let mut composite = CompositeRiskChecker::new()
            .with_in_trade_monitor(Box::new(AlwaysRejectInTrade(
                RiskViolation::EquityFloorBreached {
                    actual: dec!(0),
                    floor: dec!(100),
                },
            )))
            .with_in_trade_monitor(Box::new(AlwaysRejectInTrade(
                RiskViolation::PositionLimitExceeded {
                    symbol: "BTCUSDT".to_string(),
                    projected: dec!(20),
                    limit: dec!(10),
                },
            )));
        let result = composite.on_fill(&sample_fill(), &HashMap::new(), &HashMap::new());
        let violations = result.expect_err("expected both monitors' violations");
        assert_eq!(violations.len(), 2);
    }

    #[test]
    fn circuit_breaker_split_adapters_share_state_through_composite() {
        use crate::circuit_breaker::CircuitBreaker;
        use crate::limits::RiskLimits;

        let breaker = CircuitBreaker::new(RiskLimits {
            max_position_per_symbol: Some(dec!(10)),
            ..Default::default()
        });
        let (pre_trade_adapter, in_trade_adapter) = breaker.split();

        let mut composite = CompositeRiskChecker::new()
            .with_pre_trade_check(Box::new(pre_trade_adapter))
            .with_in_trade_monitor(Box::new(in_trade_adapter));

        let intent = sample_intent();
        assert!(composite
            .check(&intent, &HashMap::new(), &HashMap::new(), &HashMap::new(), now())
            .is_ok());

        let mut positions = HashMap::new();
        positions.insert(
            "BTCUSDT".to_string(),
            Position {
                symbol: "BTCUSDT".to_string(),
                quantity: dec!(15),
                average_entry_price: dec!(100),
            },
        );
        composite
            .on_fill(&sample_fill(), &positions, &HashMap::new())
            .unwrap_err();

        assert!(composite
            .check(&intent, &HashMap::new(), &HashMap::new(), &HashMap::new(), now())
            .is_err());
    }
}
```

- [x] **Step 2: Update `crates/apexes-risk/src/lib.rs`**

```rust
//! Apexes risk engine: pluggable pre-trade (`PreTradeCheck`) and
//! in-trade (`InTradeMonitor`) risk controls, shared across backtest,
//! paper, and live trading per the project's non-negotiable
//! backtest/live logic-parity requirement. Covers `docs/FEATURES.md`
//! rows 6.1 (pre-trade: limits, black/white lists, self-trade
//! prevention) and 6.2 (in-trade: position-limit circuit breaker).

pub mod circuit_breaker;
pub mod composite;
pub mod error;
pub mod in_trade;
pub mod limits;
pub mod lists;
pub mod pre_trade;
pub mod self_trade;

pub use circuit_breaker::{CircuitBreaker, CircuitBreakerInTradeAdapter, CircuitBreakerPreTradeAdapter};
pub use composite::CompositeRiskChecker;
pub use error::{CircuitBreakerScope, RiskViolation};
pub use in_trade::InTradeMonitor;
pub use limits::{LimitChecker, RiskLimits};
pub use lists::{ListMode, SymbolVenueList};
pub use pre_trade::PreTradeCheck;
pub use self_trade::SelfTradePreventer;
```

- [x] **Step 3: Run the tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-risk composite:: -- --nocapture
```
Expected: 6 passed, 0 failed.

- [x] **Step 4: Run the full `apexes-risk` test suite, clippy, and fmt**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-risk
cargo clippy -p apexes-risk --all-targets -- -D warnings
cargo fmt --check
```
Expected: 44 tests passed (5 error + 12 limits + 8 lists + 8 self_trade + 5 circuit_breaker + 6 composite), 0 failed; clippy and fmt clean (auto-fix with `cargo fmt` if needed and re-run all three commands).

- [x] **Step 5: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [x] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-risk/src
git commit -S -s -m "feat: add CompositeRiskChecker to apexes-risk"
git push origin master
```

---

### Task 8: `apexes-backtest` integration

**Files:**
- Modify: `crates/apexes-backtest/Cargo.toml` (add `apexes-risk` dependency)
- Modify: `crates/apexes-backtest/src/runner.rs` (`BacktestConfig` new field, `run_backtest` signature + event loop)
- Modify: `crates/apexes-backtest/src/runner.rs` (its own internal test's `BacktestConfig` literal, around line 332)
- Modify: `crates/apexes-backtest/tests/end_to_end.rs` (3 `BacktestConfig` literals)
- Modify: `crates/apexes-backtest/tests/live_storage_backtest.rs` (1 `BacktestConfig` literal)
- Modify: `crates/apexes-optimize/tests/grid_search_end_to_end.rs` (1 `BacktestConfig` literal)
- Modify: `crates/apexes-strategy-registry/tests/registry_end_to_end.rs` (1 `BacktestConfig` literal)
- Create: `crates/apexes-backtest/tests/risk_integration.rs`

**Interfaces:**
- Consumes: `apexes_risk::CompositeRiskChecker` (Task 7). Produces: `BacktestConfig.risk_checker: Option<CompositeRiskChecker>` field, consumed by nothing further in this plan (this is the plan's final integration point).

**IMPORTANT -- read before starting:** `BacktestConfig` has NO `Default` impl and is constructed via exhaustive struct literals at **7 locations** across 3 crates. Adding the new `risk_checker` field breaks compilation everywhere until every single one of these 7 call sites is updated with `risk_checker: None,`. This task's steps are ordered so that you add the field FIRST (Step 2), confirm the resulting compile errors match exactly these 7 locations (Step 3), then fix each one in turn (Steps 4-10). Do not skip the verification in Step 3 -- if `cargo build --workspace` reports a compile error anywhere NOT in this list of 7, stop and investigate before proceeding (it means either this plan's file list is stale or something else changed).

- [x] **Step 1: Add `apexes-risk` as a dependency of `apexes-backtest`**

Edit `crates/apexes-backtest/Cargo.toml`:

```toml
[package]
name = "apexes-backtest"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-storage = { path = "../apexes-storage" }
apexes-strategy = { path = "../apexes-strategy" }
apexes-risk = { path = "../apexes-risk" }
uuid = { workspace = true }
barter-data = { workspace = true }
barter = { workspace = true }
tokio = { workspace = true, features = ["time"] }
futures = { workspace = true }
rust_decimal = { workspace = true }
rust_decimal_macros = "1.40"
chrono = { workspace = true }
serde = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
uuid = { workspace = true, features = ["v4"] }
```

- [x] **Step 2: Add the `risk_checker` field to `BacktestConfig` and thread it through `run_backtest`**

In `crates/apexes-backtest/src/runner.rs`, find this exact block near the top of the file:

```rust
use crate::clock::{BacktestClock, ReplaySpeed};
use crate::cost_model::CostModel;
use crate::ledger::PortfolioLedger;
use crate::market_data_source::HistoricalMarketDataSource;
use crate::matching_engine::MatchingEngine;
use crate::performance::PerformanceTracker;
use crate::report::BacktestReport;
use apexes_core::{ApexesError, Fill, Instrument, MarketEvent, Order, OrderId, OrderType};
use apexes_strategy::{Strategy, StrategyContext};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::ops::Range;

/// Configuration for a single backtest run (design spec §8).
pub struct BacktestConfig {
    pub instruments: Vec<Instrument>,
    /// Informational / for-caller-use only: `run_backtest` itself never
    /// reads this field -- event selection is entirely the
    /// `HistoricalMarketDataSource`'s responsibility. Kept on the config
    /// so a caller can, for example, construct a
    /// `StorageMarketDataSource` using the same range, and for future
    /// use (e.g. runner-side event filtering).
    pub date_range: Range<NaiveDate>,
    /// Required -- no implicit default (design spec §8).
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub speed: ReplaySpeed,
    /// 0.0 (optimistic) .. 1.0 (pessimistic), see design spec §7.3.
    pub queue_position_factor: Decimal,
    /// Risk-free return rate used for Sharpe/Sortino/Calmar calculations
    /// (design spec §8, matching `barter::statistic::metric`'s
    /// `calculate` signatures).
    pub risk_free_return: Decimal,
}

/// Runs a full backtest: replays `market_data` in chronological order
/// through `strategy`, simulating fills via a `MatchingEngine`, tracking
/// portfolio state via a `PortfolioLedger`, and producing a
/// `BacktestReport` with overall + per-symbol + per-venue performance
/// summaries (design spec §8).
pub async fn run_backtest(
    config: BacktestConfig,
    market_data: impl HistoricalMarketDataSource,
    mut strategy: impl Strategy,
) -> Result<BacktestReport, ApexesError> {
```

Replace it with:

```rust
use crate::clock::{BacktestClock, ReplaySpeed};
use crate::cost_model::CostModel;
use crate::ledger::PortfolioLedger;
use crate::market_data_source::HistoricalMarketDataSource;
use crate::matching_engine::MatchingEngine;
use crate::performance::PerformanceTracker;
use crate::report::BacktestReport;
use apexes_core::{ApexesError, Fill, Instrument, MarketEvent, Order, OrderId, OrderType};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{Strategy, StrategyContext};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::ops::Range;

/// Configuration for a single backtest run (design spec §8).
pub struct BacktestConfig {
    pub instruments: Vec<Instrument>,
    /// Informational / for-caller-use only: `run_backtest` itself never
    /// reads this field -- event selection is entirely the
    /// `HistoricalMarketDataSource`'s responsibility. Kept on the config
    /// so a caller can, for example, construct a
    /// `StorageMarketDataSource` using the same range, and for future
    /// use (e.g. runner-side event filtering).
    pub date_range: Range<NaiveDate>,
    /// Required -- no implicit default (design spec §8).
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub speed: ReplaySpeed,
    /// 0.0 (optimistic) .. 1.0 (pessimistic), see design spec §7.3.
    pub queue_position_factor: Decimal,
    /// Risk-free return rate used for Sharpe/Sortino/Calmar calculations
    /// (design spec §8, matching `barter::statistic::metric`'s
    /// `calculate` signatures).
    pub risk_free_return: Decimal,
    /// Optional risk checks run before submitting each `OrderIntent`
    /// (rejected intents are logged via `tracing::warn!` and never
    /// reach the matching engine) and after each `Fill` (violations are
    /// logged, not propagated -- an in-trade violation does not undo a
    /// fill that already happened, per `apexes-risk`'s design spec §一
    /// "no automatic liquidation" decision). `None` means no risk
    /// checking is performed, preserving this crate's pre-existing
    /// behavior exactly.
    pub risk_checker: Option<CompositeRiskChecker>,
}

/// Runs a full backtest: replays `market_data` in chronological order
/// through `strategy`, simulating fills via a `MatchingEngine`, tracking
/// portfolio state via a `PortfolioLedger`, and producing a
/// `BacktestReport` with overall + per-symbol + per-venue performance
/// summaries (design spec §8).
pub async fn run_backtest(
    mut config: BacktestConfig,
    market_data: impl HistoricalMarketDataSource,
    mut strategy: impl Strategy,
) -> Result<BacktestReport, ApexesError> {
```

- [x] **Step 3: Confirm the resulting compile errors are exactly the 7 known call sites**

```bash
cd ~/src/oss/apexes
cargo build --workspace 2>&1 | grep -E "^error|-->" | head -60
```

Expected: multiple `error[E0063]: missing field \`risk_checker\` in initializer of \`BacktestConfig\`` errors, with `-->` lines pointing at exactly these 7 locations:
- `crates/apexes-backtest/src/runner.rs` (one location, its own test module)
- `crates/apexes-backtest/tests/end_to_end.rs` (three locations)
- `crates/apexes-backtest/tests/live_storage_backtest.rs` (one location)
- `crates/apexes-optimize/tests/grid_search_end_to_end.rs` (one location)
- `crates/apexes-strategy-registry/tests/registry_end_to_end.rs` (one location)

If any error appears at a location NOT in this list, STOP and investigate before proceeding -- do not fix call sites this plan doesn't know about without first understanding why they exist.

- [x] **Step 4: Insert the pre-trade check and in-trade monitor calls into the event loop**

In `crates/apexes-backtest/src/runner.rs`, find this exact block (the order-intent submission loop):

```rust
        for intent in intents {
            let submit_result = match intent.order_type {
                OrderType::Market => matching_engine
                    .submit_market_order(&intent, timestamp)
                    .map(|(order, fills)| (order, Some(fills))),
                OrderType::Limit => matching_engine
                    .submit_limit_order(&intent, timestamp)
                    .map(|order| (order, None)),
                OrderType::Stop | OrderType::StopLimit => matching_engine
                    .submit_stop_order(&intent, timestamp)
                    .map(|order| (order, None)),
            };

            match submit_result {
                Ok((order, immediate_fills)) => {
                    open_orders.insert(order.id, order);
                    if let Some(fills) = immediate_fills {
                        apply_fills(
                            &mut ledger,
                            &mut overall_tracker,
                            &mut per_symbol_trackers,
                            &mut per_venue_trackers,
                            &mut all_fills,
                            &fills,
                            config.risk_free_return,
                        )?;
                    }
                }
                Err(error) => {
                    tracing::warn!(?error, symbol = %intent.symbol, "order submission rejected");
                }
            }
        }
```

Replace it with:

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
                continue;
            }

            let submit_result = match intent.order_type {
                OrderType::Market => matching_engine
                    .submit_market_order(&intent, timestamp)
                    .map(|(order, fills)| (order, Some(fills))),
                OrderType::Limit => matching_engine
                    .submit_limit_order(&intent, timestamp)
                    .map(|order| (order, None)),
                OrderType::Stop | OrderType::StopLimit => matching_engine
                    .submit_stop_order(&intent, timestamp)
                    .map(|order| (order, None)),
            };

            match submit_result {
                Ok((order, immediate_fills)) => {
                    open_orders.insert(order.id, order);
                    if let Some(fills) = immediate_fills {
                        apply_fills(
                            &mut ledger,
                            &mut overall_tracker,
                            &mut per_symbol_trackers,
                            &mut per_venue_trackers,
                            &mut all_fills,
                            &fills,
                            config.risk_free_return,
                        )?;
                        if let Some(risk_checker) = &mut config.risk_checker {
                            for fill in &fills {
                                if let Err(violations) =
                                    risk_checker.on_fill(fill, ledger.positions(), ledger.balances())
                                {
                                    tracing::warn!(
                                        ?violations,
                                        symbol = %fill.symbol,
                                        "in-trade risk monitor flagged a violation"
                                    );
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(?error, symbol = %intent.symbol, "order submission rejected");
                }
            }
        }
```

- [x] **Step 5: Insert the in-trade monitor call into the Trade-event fill path**

In `crates/apexes-backtest/src/runner.rs`, find this exact block (inside the `MarketEvent::Trade(trade) => { ... }` arm, where resting-order fills from a `TradeEvent` are applied):

```rust
                let fill_results = matching_engine.on_trade_event(trade, timestamp);
                for (order_id, fills) in fill_results {
                    apply_fills(
                        &mut ledger,
                        &mut overall_tracker,
                        &mut per_symbol_trackers,
                        &mut per_venue_trackers,
                        &mut all_fills,
                        &fills,
                        config.risk_free_return,
                    )?;
                    if let Some(order) = open_orders.get_mut(&order_id)
                        && let Some(last_fill) = fills.last()
                    {
                        order.filled_quantity += fills.iter().map(|f| f.quantity).sum::<Decimal>();
                        order.updated_at = last_fill.filled_at;
                        if order.filled_quantity >= order.quantity {
                            order.status = apexes_core::OrderStatus::Filled;
                        } else {
                            order.status = apexes_core::OrderStatus::PartiallyFilled;
                        }
                    }
                }
```

Replace it with:

```rust
                let fill_results = matching_engine.on_trade_event(trade, timestamp);
                for (order_id, fills) in fill_results {
                    apply_fills(
                        &mut ledger,
                        &mut overall_tracker,
                        &mut per_symbol_trackers,
                        &mut per_venue_trackers,
                        &mut all_fills,
                        &fills,
                        config.risk_free_return,
                    )?;
                    if let Some(risk_checker) = &mut config.risk_checker {
                        for fill in &fills {
                            if let Err(violations) =
                                risk_checker.on_fill(fill, ledger.positions(), ledger.balances())
                            {
                                tracing::warn!(
                                    ?violations,
                                    symbol = %fill.symbol,
                                    "in-trade risk monitor flagged a violation"
                                );
                            }
                        }
                    }
                    if let Some(order) = open_orders.get_mut(&order_id)
                        && let Some(last_fill) = fills.last()
                    {
                        order.filled_quantity += fills.iter().map(|f| f.quantity).sum::<Decimal>();
                        order.updated_at = last_fill.filled_at;
                        if order.filled_quantity >= order.quantity {
                            order.status = apexes_core::OrderStatus::Filled;
                        } else {
                            order.status = apexes_core::OrderStatus::PartiallyFilled;
                        }
                    }
                }
```

This is the resting-limit-order fill path (fills that happen when a `TradeEvent` arrives and matches an existing resting order) -- distinct from Step 4's immediate-fill path (Market orders filled at submission time). Both paths produce `Fill`s that must reach `InTradeMonitor::on_fill`.

- [x] **Step 6: Fix `crates/apexes-backtest/src/runner.rs`'s own internal test**

Find this exact block in `runner.rs`'s `#[cfg(test)] mod tests`:

```rust
        let config = BacktestConfig {
            instruments: vec![btcusdt_instrument()],
            date_range: chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
                ..chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
            initial_balances,
            cost_model: zero_cost_model(),
            speed: ReplaySpeed::AsFastAsPossible,
            queue_position_factor: dec!(0.5),
            risk_free_return: Decimal::ZERO,
        };
```

Replace it with:

```rust
        let config = BacktestConfig {
            instruments: vec![btcusdt_instrument()],
            date_range: chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
                ..chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
            initial_balances,
            cost_model: zero_cost_model(),
            speed: ReplaySpeed::AsFastAsPossible,
            queue_position_factor: dec!(0.5),
            risk_free_return: Decimal::ZERO,
            risk_checker: None,
        };
```

- [x] **Step 7: Fix `crates/apexes-backtest/tests/end_to_end.rs`'s three call sites**

This file has three separate `BacktestConfig { ... risk_free_return: dec!(0), };` literals (around lines 92-106, 127-141, and 235-249 as of this plan's writing -- use `grep -n "risk_free_return: dec!(0),"` to find their current exact line numbers, since earlier edits in this same file may have shifted line numbers). Each one ends with exactly:

```rust
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
    };
```

Replace **each of the three occurrences** with:

```rust
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
        risk_checker: None,
    };
```

Run `grep -c "risk_checker: None," crates/apexes-backtest/tests/end_to_end.rs` after editing -- expect `3`.

- [x] **Step 8: Fix `crates/apexes-backtest/tests/live_storage_backtest.rs`**

Find this exact block:

```rust
    let config = BacktestConfig {
        instruments: vec![instrument],
        date_range: date..(date.succ_opt().unwrap()),
        initial_balances,
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
    };
```

Replace it with:

```rust
    let config = BacktestConfig {
        instruments: vec![instrument],
        date_range: date..(date.succ_opt().unwrap()),
        initial_balances,
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
        risk_checker: None,
    };
```

- [x] **Step 9: Fix `crates/apexes-optimize/tests/grid_search_end_to_end.rs`**

Find this exact block:

```rust
        let config = BacktestConfig {
            instruments: vec![btc_usdt_instrument()],
            date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
                ..NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
            initial_balances,
            cost_model: CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(0),
                    taker_bps: dec!(0),
                }),
                funding_rate_bps: None,
            },
            speed: ReplaySpeed::AsFastAsPossible,
            queue_position_factor: dec!(0.0),
            risk_free_return: dec!(0),
        };
```

Replace it with:

```rust
        let config = BacktestConfig {
            instruments: vec![btc_usdt_instrument()],
            date_range: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
                ..NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(),
            initial_balances,
            cost_model: CostModel {
                fee_schedule: Box::new(FlatFeeSchedule {
                    maker_bps: dec!(0),
                    taker_bps: dec!(0),
                }),
                funding_rate_bps: None,
            },
            speed: ReplaySpeed::AsFastAsPossible,
            queue_position_factor: dec!(0.0),
            risk_free_return: dec!(0),
            risk_checker: None,
        };
```

- [x] **Step 10: Fix `crates/apexes-strategy-registry/tests/registry_end_to_end.rs`**

Find the equivalent `BacktestConfig { ... risk_free_return: dec!(0), };` block in this file (same shape as Step 9's, ending the same way) and apply the identical fix: add `risk_checker: None,` immediately after the `risk_free_return: dec!(0),` line, before the closing `};`.

- [x] **Step 11: Verify the whole workspace compiles**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors. If any `E0063` errors remain, they indicate a missed call site -- find it with `grep -rn "risk_free_return:" crates/*/tests crates/*/src` and fix it following the same pattern (add `risk_checker: None,` right after the `risk_free_return` line).

- [x] **Step 12: Run the full existing `apexes-backtest`/`apexes-optimize`/`apexes-strategy-registry` test suites to confirm zero regressions**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-backtest
cargo test -p apexes-optimize
cargo test -p apexes-strategy-registry
```
Expected: every test that passed before this task still passes (same counts as recorded in this plan's Global Constraints / prior plans' final summaries) -- `risk_checker: None` must produce byte-for-byte identical behavior to the pre-existing code path, since `None` short-circuits both new `if let Some(risk_checker) = ...` blocks entirely.

- [x] **Step 13: Write the risk integration test**

Create `crates/apexes-backtest/tests/risk_integration.rs`:

```rust
//! Integration tests proving `BacktestConfig.risk_checker` actually
//! gates order submission and reacts to fills inside `run_backtest`'s
//! real event loop -- not just unit-testing `apexes-risk`'s types in
//! isolation.

use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, InMemoryMarketDataSource, ReplaySpeed,
    run_backtest,
};
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, PriceLevel, TradeEvent, Venue,
};
use apexes_risk::{CompositeRiskChecker, ListMode, SymbolVenueList};
use apexes_strategy::{OrderIntent, Position, Strategy, StrategyContext};
use chrono::{TimeZone, Utc};
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};

fn btc_usdt_instrument() -> Instrument {
    Instrument::new(
        "BTCUSDT",
        MarketType::Crypto,
        Venue::Binance,
        AssetClass::Spot,
        Some("BTC".to_string()),
        Some("USDT".to_string()),
        dec!(0.01),
        dec!(0.0001),
        dec!(1),
    )
    .unwrap()
}

fn sample_events() -> Vec<MarketEvent> {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let depth_snapshot = MarketEvent::Depth(DepthUpdate {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        bids: vec![],
        asks: vec![PriceLevel {
            price: dec!(1),
            quantity: dec!(1000),
        }],
        sequence: Some(1),
        is_snapshot: true,
        timestamp: t0,
    });
    let trade = MarketEvent::Trade(TradeEvent {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        price: dec!(100),
        quantity: dec!(1),
        side: None,
        trade_id: None,
        timestamp: t0 + chrono::Duration::seconds(1),
    });
    vec![depth_snapshot, trade]
}

fn base_config(events_range_end_day: chrono::NaiveDate) -> BacktestConfig {
    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(100000));
    BacktestConfig {
        instruments: vec![btc_usdt_instrument()],
        date_range: chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()..events_range_end_day,
        initial_balances,
        cost_model: CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        },
        speed: ReplaySpeed::AsFastAsPossible,
        queue_position_factor: dec!(0.0),
        risk_free_return: dec!(0),
        risk_checker: None,
    }
}

/// Buys 1 unit of BTCUSDT on the very first `MarketEvent` it sees, then
/// does nothing on subsequent events. Deterministic and side-effect-free
/// beyond that single order, so tests can assert exactly whether that
/// one order made it through.
struct BuyOnceStrategy {
    has_bought: bool,
}

impl Strategy for BuyOnceStrategy {
    fn on_market_event(
        &mut self,
        _event: &MarketEvent,
        _ctx: &StrategyContext,
    ) -> Vec<OrderIntent> {
        if self.has_bought {
            return vec![];
        }
        self.has_bought = true;
        vec![OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: apexes_core::Side::Buy,
            order_type: apexes_core::OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
        }]
    }
}

#[tokio::test]
async fn risk_checker_none_behaves_identically_to_no_risk_checking() {
    let mut config = base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    config.risk_checker = None;
    let market_data = InMemoryMarketDataSource::new(sample_events());
    let report = run_backtest(config, market_data, BuyOnceStrategy { has_bought: false })
        .await
        .unwrap();
    assert_eq!(report.trades.len(), 1);
}

#[tokio::test]
async fn blacklisted_symbol_is_rejected_before_reaching_the_matching_engine() {
    let mut blacklist = HashSet::new();
    blacklist.insert("BTCUSDT".to_string());
    let risk_checker = CompositeRiskChecker::new().with_pre_trade_check(Box::new(
        SymbolVenueList::new(ListMode::Blacklist(blacklist), ListMode::Disabled),
    ));

    let mut config = base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    config.risk_checker = Some(risk_checker);
    let market_data = InMemoryMarketDataSource::new(sample_events());
    let report = run_backtest(config, market_data, BuyOnceStrategy { has_bought: false })
        .await
        .unwrap();

    assert!(report.trades.is_empty());
}

#[tokio::test]
async fn non_blacklisted_symbol_still_trades_normally_through_a_configured_risk_checker() {
    let mut blacklist = HashSet::new();
    blacklist.insert("ETHUSDT".to_string()); // a different symbol -- BTCUSDT is unaffected.
    let risk_checker = CompositeRiskChecker::new().with_pre_trade_check(Box::new(
        SymbolVenueList::new(ListMode::Blacklist(blacklist), ListMode::Disabled),
    ));

    let mut config = base_config(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
    config.risk_checker = Some(risk_checker);
    let market_data = InMemoryMarketDataSource::new(sample_events());
    let report = run_backtest(config, market_data, BuyOnceStrategy { has_bought: false })
        .await
        .unwrap();

    assert_eq!(report.trades.len(), 1);
}
```

- [x] **Step 14: Run the new integration test**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-backtest --test risk_integration -- --nocapture
```
Expected: 3 passed, 0 failed.

- [x] **Step 15: Run clippy and fmt on every touched crate**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-backtest --all-targets -- -D warnings
cargo clippy -p apexes-optimize --all-targets -- -D warnings
cargo clippy -p apexes-strategy-registry --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run all four commands plus Step 12's three test commands and Step 14's test command to confirm nothing regressed).

- [x] **Step 16: Full workspace verification**

```bash
cd ~/src/oss/apexes
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```
Expected: all clean, 0 failures across every crate.

- [x] **Step 17: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-backtest crates/apexes-optimize/tests/grid_search_end_to_end.rs crates/apexes-strategy-registry/tests/registry_end_to_end.rs
git commit -S -s -m "feat: wire apexes-risk into apexes-backtest's run_backtest event loop"
git push origin master
```

---

### Task 9: Final whole-effort verification

**Files:** None created/modified -- this task only runs verification commands, updates `docs/FEATURES.md`, and confirms the end state.

- [x] **Step 1: Full workspace build**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [x] **Step 2: Full workspace test suite**

```bash
cd ~/src/oss/apexes
cargo test --workspace
```
Expected: all tests across all crates pass. Confirm `apexes-core`/`apexes-market-data`/`apexes-indicators`/`apexes-strategy`/`apexes-storage`/`apexes-strategy-registry` are completely unaffected by this plan (unchanged pass counts from before Task 1 started); `apexes-optimize` unaffected except its one updated `BacktestConfig` literal (test count unchanged); `apexes-backtest` gains exactly 3 new tests (Task 8's `risk_integration.rs`); `apexes-risk` is brand new with 44 tests (Tasks 1-7: 5 error + 12 limits + 8 lists + 8 self_trade + 5 circuit_breaker + 6 composite).

- [x] **Step 3: Full workspace clippy**

```bash
cd ~/src/oss/apexes
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: 0 warnings.

- [x] **Step 4: Full workspace fmt check**

```bash
cd ~/src/oss/apexes
cargo fmt --check
```
Expected: 0 diffs.

- [x] **Step 5: Update `docs/FEATURES.md`**

Read `docs/FEATURES.md`, find rows `6.1` (Pre-trade risk controls, currently `🔲 Not Started`) and `6.2` (In-trade risk controls, currently `🔲 Not Started`). Update BOTH rows' Status to `✅ Done`.

Row `6.1`'s Notes:
```
apexes-risk crate: PreTradeCheck trait with three implementations -- LimitChecker (per-order quantity/notional limits, per-symbol position limits, account balance floor, daily trade count/volume limits with UTC-calendar-day reset, all runtime-updatable via update_limits()), SymbolVenueList (independent symbol-level and venue-level blacklist/whitelist), SelfTradePreventer (rejects Limit/StopLimit orders whose price range overlaps an existing opposite-side open order; rejects Market orders outright against any opposite-side open order for the same symbol). CompositeRiskChecker aggregates multiple checkers, collecting every violation from every checker rather than stopping at the first failure. Wired into apexes-backtest::run_backtest via an optional BacktestConfig.risk_checker field -- rejected orders never reach the matching engine.
```

Row `6.2`'s Notes:
```
apexes-risk crate: CircuitBreaker implements both PreTradeCheck and InTradeMonitor (shared state via CircuitBreaker::split()'s Arc<Mutex<_>>-backed adapters) -- trips per-symbol when a Fill causes that symbol's position to exceed its configured limit, or account-wide when total balance drops below the configured equity floor. While tripped, PreTradeCheck::check rejects all new orders in that scope; auto-resets once the breaching condition clears, with an explicit reset() method for early manual override. No automatic liquidation -- CircuitBreaker only blocks future orders, never generates a reverse order to force-close an existing position. Post-trade risk & compliance reporting (row 6.3) remains with sub-project #15; margin/leverage monitoring (row 6.4) and extreme-market circuit breakers with automatic strategy de-risking (row 6.5) remain unstarted, deferred until a leveraged-position model exists.
```

**Important:** before finalizing, do not just increment counts by 2/2 -- actually count the current `✅ Done` / `🔲 Not Started` / `⏸️ Deferred` rows in the file (e.g. `grep -c` against the row-level status markers, excluding the legend rows and the Summary table's own rows) and confirm your new numbers match Done+2, Not Started-2, Total unchanged, against the real counts. Update the Summary counts table accordingly and the "Current active work" paragraph to note sub-project `#5`'s first phase (pre-trade + in-trade risk controls, rows `6.1`/`6.2`) is now complete via the `apexes-risk` crate; leave rows `6.4`/`6.5` and sub-project `#15`'s row `6.3` explicitly unstarted in the prose (don't imply they're done); leave "what's next" unstated since PyO3 bindings evaluation, `6.4`/`6.5`, and the next major sub-project (`#6` Paper Trading, `#7` OMS, etc.) are all still open options -- don't guess which one the user picks next.

- [x] **Step 6: Commit the FEATURES.md update**

```bash
cd ~/src/oss/apexes
git add docs/FEATURES.md
git commit -S -s -m "docs: mark pre-trade and in-trade risk controls complete in FEATURES.md"
git push origin master
```

- [x] **Step 7: Mark this plan's checkboxes complete**

```bash
cd ~/src/oss/apexes
sed -i '' 's/^- \[ \]/- [x]/' docs/superpowers/plans/2026-07-31-apexes-risk.md
```
Check `grep -c "^\- \[ \]" docs/superpowers/plans/2026-07-31-apexes-risk.md` reports `0` remaining unmarked checkboxes. Read the top of the file after running `sed` to verify the header's own descriptive sentence (`Steps use checkbox (\`- [ ]\`) syntax for tracking`) was NOT affected -- that occurrence is inside inline code backticks on its own line and does not match the `^- \[ \]` line-start pattern, so it should be unaffected, but confirm rather than assume.

```bash
git add docs/superpowers/plans/2026-07-31-apexes-risk.md
git commit -S -s -m "docs: mark apexes-risk plan complete"
git push origin master
```

- [x] **Step 8: Report final summary**

Summarize: `apexes-risk` crate's file list and line counts, total test count (broken down: unit tests per module), confirmation that `apexes-backtest`'s/`apexes-optimize`'s/`apexes-strategy-registry`'s pre-existing tests still pass unchanged after their small `BacktestConfig` additions, and the `apexes` commit SHAs for Tasks 1-9. No further commit needed for this verification-only task.

---

## Plan Self-Review Notes

**Spec coverage:** Design spec §一 (scope: `apexes-risk` depends on `apexes-core`+`apexes-strategy`, `PreTradeCheck`/`InTradeMonitor` traits, four implementations, `apexes-backtest` integration, explicit non-goals around `6.3`/`6.4`/`6.5`/auto-liquidation/persistence) → Task 1 (crate scaffold + dependency), Task 2 (traits), Tasks 3-6 (four implementations), Task 7 (`CompositeRiskChecker`), Task 8 (backtest integration). §二 (crate structure: `lib.rs`/`error.rs`/`pre_trade.rs`/`in_trade.rs`/`limits.rs`/`lists.rs`/`self_trade.rs`/`circuit_breaker.rs`/`composite.rs`) → every file created exactly as named across Tasks 1-7. §三 (`RiskViolation`/`CircuitBreakerScope`) → Task 1. §四 (trait definitions + `&self`/`&mut self` rationale) → Task 2. §五 (`RiskLimits`/`LimitChecker`, `SymbolVenueList`, `SelfTradePreventer`, `CircuitBreaker`, including the `Arc<Mutex<_>>` split-adapter design for `CircuitBreaker`'s dual trait implementation) → Tasks 3-6 respectively. §六 (`CompositeRiskChecker` + `apexes-backtest` integration, including the exact event-loop insertion points for both the immediate-fill path and the Trade-event resting-order-fill path) → Task 7 (composite) and Task 8 (integration, covering BOTH fill paths per §六's two code blocks). §七 (test strategy) → every task's own test steps, plus Task 8's `risk_integration.rs` end-to-end test and Task 9's full workspace verification. §八 (non-goals) → respected throughout, explicitly called out in Task 9's FEATURES.md Notes text for both rows.

**Type consistency check:** `PreTradeCheck::check`'s exact signature (`&self, intent: &OrderIntent, positions: &HashMap<String, Position>, balances: &HashMap<String, Decimal>, open_orders: &HashMap<OrderId, Order>, now: DateTime<Utc>`) is identical across Task 2's trait definition and every implementation in Tasks 3-6 and the `CompositeRiskChecker` in Task 7. `InTradeMonitor::on_fill`'s signature (`&mut self, fill: &Fill, positions: &HashMap<String, Position>, balances: &HashMap<String, Decimal>`) is likewise identical across Task 2, Task 6's `CircuitBreaker`, and Task 7's `CompositeRiskChecker`. `RiskLimits`'s six fields (`max_order_quantity`/`max_order_notional`/`max_position_per_symbol`/`min_account_equity`/`max_daily_trade_count`/`max_daily_volume`) are used identically by both `LimitChecker` (Task 3) and `CircuitBreaker` (Task 6) -- each holds its own independent copy per the design spec's explicit non-sharing decision (§5.4 of the design doc). `BacktestConfig.risk_checker: Option<CompositeRiskChecker>` (Task 8) matches every one of the 7 call-site fixes and the new `risk_integration.rs` test's usage.

**Placeholder scan:** No "TBD"/"TODO" strings; every code step contains complete, concrete Rust source, including full test module bodies (not "similar to the above" references). Task 8's 7-call-site enumeration is exhaustive and each fix shows the complete before/after code block rather than describing the change abstractly.
