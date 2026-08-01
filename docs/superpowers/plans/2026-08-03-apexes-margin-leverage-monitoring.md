# Apexes: Margin & Leverage Monitoring Implementation Plan (`docs/FEATURES.md` row `6.4`)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the design in `docs/superpowers/specs/2026-08-03-apexes-margin-leverage-monitoring-design.md`: a leveraged-position model (isolated + cross margin), tiered maintenance-margin brackets, real mark-price-driven margin/liquidation computation, periodic funding-payment settlement, a new `apexes-risk::MarginMonitor` checker, backtest/paper auto-liquidation with live-trading monitor-only behavior, and real Binance leverage/margin-mode API wiring — completing roadmap sub-project `#5`'s row `6.4`.

**Architecture:** Ten sequential tasks in dependency order. Tasks 1-2 add the core types (`apexes-core::MarginMode` + `Order` fields; `apexes-strategy`'s `Position`/`OrderIntent` extensions, `MarginTierTable`, `PositionMarginStatus`, `MarkPriceProvider`). Tasks 3-5 extend `apexes-backtest::PortfolioLedger` (funding payments, margin status, liquidation detection) and wire `run_event_loop` (shared by `run_backtest`/`run_paper_trading`) for auto-liquidation + `StrategyContext.margin_status`. Task 6 adds `apexes-risk::MarginMonitor` + `CompositeRiskChecker`'s third registration list. Task 7 wires `run_live_trading` for monitor-only behavior. Task 8 threads the new `OrderIntent` fields through `apexes-oms::OrderManager`. Task 9 adds real Binance leverage/margin-mode REST calls to `apexes-gateway-binance`. Task 10 is the final whole-workspace checkpoint + `docs/FEATURES.md` update.

**Tech Stack:** Rust 2024, existing workspace deps only. No new external crates — `apexes-storage::StorageReader` (already exists, mark-price pipeline plan) and `apexes-gateway-binance::BinanceRestClient::signed_request` (already exists) are reused directly.

## Global Constraints

- Rust edition 2024, workspace-wide `cargo check`/`cargo clippy --all-targets -- -D warnings`/`cargo fmt --check` must stay clean after every task.
- No `unsafe` blocks. Never bare `.unwrap()`/`.expect()` in production code (test code may use them freely).
- `thiserror` for all new error types; `serde`/`chrono`/`rust_decimal` conventions match existing crates exactly.
- Every commit signed: `git commit -S -s`.
- Existing spot-only behavior must not change for any task in this plan — every extension is additive (`leverage: None`/`margin_mode: None` must behave identically to today's code in every path).
- `MarginMode` is defined in `apexes-core` (NOT `apexes-strategy`) specifically to avoid a circular dependency: `apexes_core::Order` needs to carry a `margin_mode` field, and `apexes-strategy` already depends on `apexes-core` (the reverse dependency direction is forbidden). `apexes-strategy` re-exports it via `pub use apexes_core::MarginMode;` so callers can use `apexes_strategy::MarginMode` interchangeably.
- `MarginTierTable`/`PositionMarginStatus` are defined in `apexes-strategy` (NOT `apexes-backtest`) specifically because both `apexes-risk` and `apexes-backtest` already depend on `apexes-strategy`, but neither `apexes-strategy` nor `apexes-risk` depends on `apexes-backtest` — defining them in `apexes-backtest` would make it impossible for `apexes-risk::MarginMonitor`/`apexes-strategy::StrategyContext` to reference them. `PortfolioLedger::margin_status`'s actual computation logic still lives in `apexes-backtest` (the only place that has the ledger's actual data); it simply returns the `apexes-strategy` types.
- `apexes-storage`'s new `StorageReaderMarkPriceProvider` (the backtest-only implementation of `MarkPriceProvider`) is defined in `apexes-backtest` (NOT `apexes-storage`) because `apexes-storage` only depends on `apexes-core`, not `apexes-strategy` (where `MarkPriceProvider` is defined) — `apexes-backtest` already depends on both.
- Full design rationale (why isolated vs. cross margin behave differently, why live trading never auto-liquidates, why the tiered maintenance-margin table is configured externally rather than hardcoded) is in the design spec — read it before starting if anything below is unclear on *why*, not just *what*.

---

### Task 1: `apexes-core`: `MarginMode` enum + `Order` leverage/margin_mode fields

**Files:**
- Modify: `crates/apexes-core/src/order.rs`

**Interfaces:**
- Produces: `apexes_core::MarginMode` (enum: `Isolated`, `Cross`); `apexes_core::Order` gains `pub leverage: Option<Decimal>` and `pub margin_mode: Option<MarginMode>` fields.

- [ ] **Step 1: Add the `MarginMode` enum**

```rust
// crates/apexes-core/src/order.rs -- add near the top, alongside Side/OrderType:

/// Which margin allocation model a leveraged position uses. `Isolated`:
/// the position has its own dedicated margin allocation -- liquidation
/// only risks that position. `Cross`: the position shares the whole
/// account's net value (see `apexes_strategy`'s `PortfolioLedger::
/// margin_status` design) as its margin pool. Defined here (not in
/// `apexes-strategy`, where `Position`/`OrderIntent` live) specifically
/// because `Order` (this file) needs to carry it, and `apexes-strategy`
/// already depends on `apexes-core` -- the reverse dependency direction
/// would cycle. `apexes-strategy` re-exports this type via `pub use
/// apexes_core::MarginMode;`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarginMode {
    Isolated,
    Cross,
}
```

- [ ] **Step 2: Add the two new fields to `Order`**

```rust
// crates/apexes-core/src/order.rs -- Order struct, add after stop_price:
pub struct Order {
    pub id: OrderId,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
    /// `None`: spot order (today's behavior, unchanged). `Some(leverage)`:
    /// this order opens/adds to a leveraged perpetual position at this
    /// leverage. Set by `OrderManager::submit_order` (`apexes-oms`) from
    /// the originating `OrderIntent.leverage`, so `ExecutionGateway`
    /// implementations (e.g. `BinanceExecutionGateway`) can see it and
    /// set the exchange-side leverage/margin-mode before submitting.
    pub leverage: Option<Decimal>,
    /// Only meaningful when `leverage.is_some()`; `None` is treated as
    /// `MarginMode::Isolated` by convention (matching
    /// `OrderIntent.margin_mode`'s own default).
    pub margin_mode: Option<MarginMode>,
    pub filled_quantity: Decimal,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

- [ ] **Step 3: Fix every existing `Order { .. }` construction site in the workspace**

Run this to find every site needing the two new fields added (this crate's own test constructions, plus every other crate that builds an `Order` literal):

```bash
grep -rn "= Order {" crates/
```

At the time this plan was drafted, this returns 8 sites across 5 files: `crates/apexes-core/src/order.rs` (4, its own tests), `crates/apexes-backtest/src/matching_engine.rs` (3: `submit_market_order`/`submit_limit_order`/`submit_stop_order`), `crates/apexes-gateway-binance/tests/live_testnet.rs` (3), `crates/apexes-risk/src/self_trade.rs` (1), `crates/apexes-oms/src/manager.rs` (1). Re-run the grep yourself before starting in case this list has drifted since drafting.

For `crates/apexes-backtest/src/matching_engine.rs`'s three sites (`submit_market_order`, `submit_limit_order`, `submit_stop_order`): add `leverage: intent.leverage, margin_mode: intent.margin_mode,` (threading straight through from the `OrderIntent` each method already receives -- this requires `apexes_strategy::OrderIntent` to already have these fields, which Task 2 adds; if you're doing Task 1 before Task 2, temporarily add `leverage: None, margin_mode: None,` here instead and note in your report that Task 2 must update this to thread through real values).

For every other site (test-only constructions in `apexes-core::order`'s own tests, `apexes-risk::self_trade`'s test helper, `apexes-gateway-binance/tests/live_testnet.rs`, `apexes-oms::manager`'s test helper): add `leverage: None, margin_mode: None,` (these are all either pure spot-order test fixtures or, for `manager.rs`, a test-only `sample_intent()`-adjacent helper -- not the real `submit_order` production code path, which Task 8 handles separately).

- [ ] **Step 4: Add a `MarginMode` serde round-trip test**

Add to `crates/apexes-core/src/order.rs`'s existing `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn margin_mode_serde_round_trip() {
        for mode in [MarginMode::Isolated, MarginMode::Cross] {
            let json = serde_json::to_string(&mode).unwrap();
            let round_tripped: MarginMode = serde_json::from_str(&json).unwrap();
            assert_eq!(mode, round_tripped);
        }
    }
```

- [ ] **Step 5: Run this crate's tests, then a workspace-wide compile check**

Run: `cargo test -p apexes-core`
Expected: PASS, including the new `margin_mode_serde_round_trip` test.

Run: `cargo check --workspace`
Expected: this WILL fail in `apexes-backtest`/`apexes-risk`/`apexes-gateway-binance`/`apexes-oms` if `intent.leverage`/`intent.margin_mode` don't exist yet (Task 2 adds them to `OrderIntent`) -- if you did Step 3's `matching_engine.rs` fix with the temporary `None, None` fallback, this should compile cleanly for now. Confirm the ONLY remaining errors (if any) are about `OrderIntent` lacking `leverage`/`margin_mode` fields, not anything else -- if you see other unrelated errors, STOP and report them.

Run: `cargo clippy -p apexes-core --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-core/src/order.rs crates/apexes-backtest/src/matching_engine.rs crates/apexes-gateway-binance/tests/live_testnet.rs crates/apexes-risk/src/self_trade.rs crates/apexes-oms/src/manager.rs
git commit -S -s -m "feat(apexes-core): add MarginMode enum and Order.leverage/.margin_mode fields"
```

---

### Task 2: `apexes-strategy`: `Position`/`OrderIntent` leverage fields, `MarginTierTable`, `PositionMarginStatus`, `MarkPriceProvider`

**Files:**
- Modify: `crates/apexes-strategy/src/strategy.rs`
- Create: `crates/apexes-strategy/src/margin_tier.rs`
- Create: `crates/apexes-strategy/src/mark_price_provider.rs`
- Modify: `crates/apexes-strategy/src/lib.rs`
- Modify: `crates/apexes-backtest/src/matching_engine.rs` (finish Task 1's temporary fallback, if used)
- Modify every `Position { .. }`/`OrderIntent { .. }` construction site in the workspace

**Interfaces:**
- Consumes: `apexes_core::MarginMode` (Task 1).
- Produces: `apexes_strategy::MarginMode` (re-export), `apexes_strategy::Position` gains `leverage: Option<Decimal>`/`margin_mode: Option<MarginMode>`, `apexes_strategy::OrderIntent` gains the same two fields, `apexes_strategy::{MarginTier, MarginTierTable}`, `apexes_strategy::PositionMarginStatus { margin_ratio: Option<Decimal>, liquidation_price: Option<Decimal> }`, `apexes_strategy::MarkPriceProvider` (async trait: `async fn mark_price(&self, symbol: &str, venue: Venue) -> Result<Decimal, ApexesError>`).

- [ ] **Step 1: Add `pub use apexes_core::MarginMode;` and extend `Position`/`OrderIntent`**

```rust
// crates/apexes-strategy/src/strategy.rs -- add to imports:
use apexes_core::{AssetClass, MarginMode, Order, OrderId, OrderType, Side, Venue};
```

```rust
// crates/apexes-strategy/src/strategy.rs -- Position struct, add two fields:
pub struct Position {
    pub symbol: String,
    pub quantity: Decimal,
    pub average_entry_price: Decimal,
    /// `None`: spot net position (today's behavior, unchanged). `Some`:
    /// this is a leveraged perpetual position opened at this leverage.
    pub leverage: Option<Decimal>,
    /// Only meaningful when `leverage.is_some()`.
    pub margin_mode: Option<MarginMode>,
}
```

```rust
// crates/apexes-strategy/src/strategy.rs -- OrderIntent struct, add two fields
// (after routing):
pub struct OrderIntent {
    // ...existing fields unchanged...
    /// `None`: spot order (today's behavior). `Some(leverage)`: open/add
    /// to a leveraged perpetual position at this leverage.
    pub leverage: Option<Decimal>,
    /// Only read when `leverage.is_some()`; `None` defaults to
    /// `MarginMode::Isolated` (the safer default -- an isolated
    /// position's liquidation cannot cascade into other positions).
    pub margin_mode: Option<MarginMode>,
}
```

- [ ] **Step 2: Fix every existing `Position { .. }` and `OrderIntent { .. }` construction site in the workspace**

Run these to find every site (re-run yourself before starting, since this plan's authors' counts may have drifted):

```bash
grep -rln "Position {" crates/
grep -rln "OrderIntent {" crates/
```

At drafting time: `Position {` appears in `crates/apexes-backtest/src/ledger.rs`, `crates/apexes-risk/src/{limits.rs,composite.rs,circuit_breaker.rs}`, `crates/apexes-strategy/tests/drift_script_strategy.rs`, `crates/apexes-strategy/src/strategy.rs` (its own test), `crates/apexes-strategy/src/drift_dsl/{convert.rs,bindings.rs}`. `OrderIntent {` appears in many more files across `apexes-backtest`, `apexes-live-trading`, `apexes-risk`, `apexes-strategy`, `apexes-oms`, `apexes-ems`, `apexes-paper` (test fixtures and production construction sites alike).

For every one of these, add `leverage: None, margin_mode: None,` (every existing call site is spot-only by construction -- none of them need real values yet, since this task doesn't wire up any leveraged trading path; that happens in later tasks reading these fields, not writing them). The one exception: `crates/apexes-backtest/src/matching_engine.rs`'s three `Order { .. }` sites -- if Task 1 left `leverage: None, margin_mode: None,` there as a temporary fallback, change it now to `leverage: intent.leverage, margin_mode: intent.margin_mode,` (threading through from the real `OrderIntent`, now that it has these fields for real).

- [ ] **Step 3: Write `margin_tier.rs`**

```rust
// crates/apexes-strategy/src/margin_tier.rs (new file)
//! [`MarginTierTable`]: a tiered maintenance-margin bracket table,
//! modeled on real exchanges' (e.g. Binance USDS-M Futures) tiered
//! margin schedules -- higher notional value means a higher
//! maintenance margin rate and a lower maximum allowed leverage.
//! Configured externally (not hardcoded) and passed by the caller to
//! both `apexes-risk::MarginMonitor` and
//! `apexes-backtest::PortfolioLedger`'s margin methods, keyed by
//! symbol, so both consumers always compute against the exact same
//! configuration.

use rust_decimal::Decimal;

/// A single maintenance-margin bracket.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarginTier {
    /// This tier applies at/above this notional value (inclusive).
    pub notional_floor: Decimal,
    pub max_leverage: Decimal,
    pub maintenance_margin_rate: Decimal,
    /// Fixed deduction amount (the constant term real exchanges'
    /// tiered tables use to keep the maintenance-margin curve
    /// continuous across tier boundaries).
    pub maintenance_amount: Decimal,
}

/// One symbol's complete tiered margin table, sorted ascending by
/// `notional_floor`.
#[derive(Debug, Clone, PartialEq)]
pub struct MarginTierTable {
    pub tiers: Vec<MarginTier>,
}

impl MarginTierTable {
    /// Returns the tier applicable to `notional` -- the LAST tier
    /// (highest `notional_floor`) whose `notional_floor <= notional`.
    /// If `notional` is below every tier's floor, returns the first
    /// (lowest) tier. Returns `None` only if `self.tiers` is empty
    /// (a misconfigured table -- callers should treat this as a
    /// construction-time error, not a runtime one to silently ignore).
    pub fn tier_for_notional(&self, notional: Decimal) -> Option<&MarginTier> {
        self.tiers
            .iter()
            .rev()
            .find(|tier| tier.notional_floor <= notional)
            .or_else(|| self.tiers.first())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn sample_table() -> MarginTierTable {
        MarginTierTable {
            tiers: vec![
                MarginTier {
                    notional_floor: dec!(0),
                    max_leverage: dec!(20),
                    maintenance_margin_rate: dec!(0.005),
                    maintenance_amount: dec!(0),
                },
                MarginTier {
                    notional_floor: dec!(50000),
                    max_leverage: dec!(10),
                    maintenance_margin_rate: dec!(0.01),
                    maintenance_amount: dec!(250),
                },
                MarginTier {
                    notional_floor: dec!(250000),
                    max_leverage: dec!(5),
                    maintenance_margin_rate: dec!(0.025),
                    maintenance_amount: dec!(4000),
                },
            ],
        }
    }

    #[test]
    fn tier_for_notional_returns_lowest_tier_below_first_floor() {
        let table = sample_table();
        let tier = table.tier_for_notional(dec!(10000)).unwrap();
        assert_eq!(tier.max_leverage, dec!(20));
    }

    #[test]
    fn tier_for_notional_returns_exact_boundary_tier() {
        let table = sample_table();
        let tier = table.tier_for_notional(dec!(50000)).unwrap();
        assert_eq!(tier.max_leverage, dec!(10));
    }

    #[test]
    fn tier_for_notional_returns_highest_tier_above_last_floor() {
        let table = sample_table();
        let tier = table.tier_for_notional(dec!(1000000)).unwrap();
        assert_eq!(tier.max_leverage, dec!(5));
    }

    #[test]
    fn tier_for_notional_returns_none_for_empty_table() {
        let table = MarginTierTable { tiers: vec![] };
        assert!(table.tier_for_notional(dec!(100)).is_none());
    }
}
```

- [ ] **Step 4: Add `PositionMarginStatus` (same file, `margin_tier.rs`)**

```rust
// crates/apexes-strategy/src/margin_tier.rs -- add below MarginTierTable:

/// The real-time margin state of one leveraged position, computed by
/// `apexes_backtest::PortfolioLedger::margin_status` (the only place
/// with the actual ledger data) but defined here so both
/// `apexes-risk::MarginMonitor` and `apexes-strategy::StrategyContext`
/// (neither of which depends on `apexes-backtest`) can reference it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PositionMarginStatus {
    /// `None`: spot position, margin doesn't apply. `Some(ratio)`:
    /// current margin / maintenance margin requirement -- `<= 1.0`
    /// means liquidation has been triggered.
    pub margin_ratio: Option<Decimal>,
    /// `Some(price)` only for `MarginMode::Isolated` positions (a
    /// single, precise liquidation price is well-defined there).
    /// `None` for `MarginMode::Cross` positions -- since cross margin
    /// shares the whole account's equity as its pool, "this position's
    /// own" liquidation price isn't a precisely well-defined concept;
    /// only the shared account-level `margin_ratio` is meaningful.
    pub liquidation_price: Option<Decimal>,
}
```

- [ ] **Step 5: Write `mark_price_provider.rs`**

```rust
// crates/apexes-strategy/src/mark_price_provider.rs (new file)
//! [`MarkPriceProvider`]: supplies the current mark price for a
//! symbol/venue -- the index-and-funding-basis-adjusted price real
//! exchanges use for margin/liquidation math, deliberately distinct
//! from last-trade price or order-book mid (see
//! `docs/superpowers/specs/2026-08-02-apexes-mark-price-pipeline-design.md`
//! for the full rationale and the `apexes_core::MarketEvent::MarkPrice`
//! data this ultimately comes from).
//!
//! Backtest: `apexes_backtest` provides a `StorageReaderMarkPriceProvider`
//! (defined there, not here or in `apexes-storage` -- see this plan's
//! Global Constraints for why). Paper/live trading: no implementation
//! of this trait is needed at all -- `run_event_loop`/`run_live_trading`
//! already receive `MarketEvent::MarkPrice` directly and maintain a
//! plain `HashMap<String, Decimal>` (mirroring the existing
//! `last_prices` pattern), passed straight into `PortfolioLedger`'s
//! margin methods without going through this trait.

use apexes_core::{ApexesError, Venue};
use async_trait::async_trait;
use rust_decimal::Decimal;

#[async_trait]
pub trait MarkPriceProvider: Send + Sync {
    /// Returns the current mark price for `symbol` at `venue`. In a
    /// backtest, "current" means "as of the caller's current replay
    /// timestamp" -- see `StorageReaderMarkPriceProvider`
    /// (`apexes-backtest`) for how that's tracked.
    async fn mark_price(&self, symbol: &str, venue: Venue) -> Result<Decimal, ApexesError>;
}
```

Add `async-trait` to this crate's `Cargo.toml` if not already present (check via `file_read` on `crates/apexes-strategy/Cargo.toml` first -- this crate may already depend on it via `drift_dsl`'s existing async usage; only add if genuinely missing).

- [ ] **Step 6: Register the new modules and re-exports**

```rust
// crates/apexes-strategy/src/lib.rs -- add:
pub mod margin_tier;
pub mod mark_price_provider;
```

```rust
// crates/apexes-strategy/src/lib.rs -- extend the existing pub use block:
pub use apexes_core::MarginMode;
pub use margin_tier::{MarginTier, MarginTierTable, PositionMarginStatus};
pub use mark_price_provider::MarkPriceProvider;
```

- [ ] **Step 7: Run this crate's tests, clippy, fmt, then a workspace-wide compile check**

Run: `cargo test -p apexes-strategy`
Expected: PASS, including the new `MarginTierTable`/`tier_for_notional` tests.

Run: `cargo check --workspace`
Expected: clean now (Task 1's temporary fallback in `matching_engine.rs`, if used, is now resolved for real; every `Position`/`OrderIntent` construction site across the workspace has the two new fields).

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

Run: `cargo test --workspace -- --test-threads=1`
Expected: 0 failures (same pre-existing environmental-limitation ignored tests as always -- confirm nothing new fails).

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -S -s -m "feat(apexes-strategy): leveraged Position/OrderIntent fields, MarginTierTable, PositionMarginStatus, MarkPriceProvider"
```

---

### Task 3: `apexes-backtest::PortfolioLedger`: `apply_funding_payments`

**Files:**
- Modify: `crates/apexes-backtest/src/ledger.rs`

**Interfaces:**
- Consumes: `apexes_backtest::CostModel::funding_payment` (existing, currently dead code), `apexes_strategy::Position.leverage`/`.margin_mode` (Task 2).
- Produces: `PortfolioLedger::apply_funding_payments(&mut self, mark_prices: &HashMap<String, Decimal>, cost_model: &CostModel) -> Result<(), ApexesError>`.

- [ ] **Step 1: Write `apply_funding_payments`**

```rust
// crates/apexes-backtest/src/ledger.rs -- add to impl PortfolioLedger,
// after apply_fill:

use crate::cost_model::CostModel;

impl PortfolioLedger {
    // ... existing methods ...

    /// Settles one funding payment for every currently-open leveraged
    /// position (`Position.leverage.is_some()`), using `cost_model`'s
    /// `funding_payment(notional)` (already existing, previously dead
    /// code -- see `CostModel`'s own doc comment). Positive payment
    /// debits the position holder's quote-currency balance (funding
    /// paid); negative debits (i.e. credits) it (funding received).
    /// Spot positions (`leverage: None`) are skipped entirely -- this
    /// method is a no-op for spot-only backtests, matching the
    /// pre-existing "funding_rate_bps: None disables funding entirely"
    /// contract. Calling this when `cost_model.funding_rate_bps` is
    /// `None` is also a safe no-op (via `funding_payment`'s existing
    /// `None`-propagating behavior).
    ///
    /// Callers (`run_event_loop`) are responsible for deciding WHEN to
    /// call this -- this method itself performs exactly one settlement
    /// pass over all open leveraged positions each time it's called; it
    /// does not track time or funding-interval scheduling itself.
    pub fn apply_funding_payments(
        &mut self,
        mark_prices: &HashMap<String, Decimal>,
        cost_model: &CostModel,
    ) -> Result<(), ApexesError> {
        // Collect (symbol, quote_currency, payment) triples first so we
        // don't hold an immutable borrow of self.positions while later
        // mutably crediting self.balances.
        let mut settlements = Vec::new();
        for (symbol, position) in &self.positions {
            if position.leverage.is_none() || position.quantity.is_zero() {
                continue;
            }
            let instrument = self.instruments.get(symbol).ok_or_else(|| {
                ApexesError::InvalidBacktestConfig {
                    reason: format!("no Instrument registered for symbol {symbol}"),
                }
            })?;
            let quote = instrument.quote_currency.clone().ok_or_else(|| {
                ApexesError::InvalidBacktestConfig {
                    reason: format!("Instrument {symbol} has no quote_currency"),
                }
            })?;
            let mark_price = instrument
                .base_currency
                .as_ref()
                .and_then(|base| mark_prices.get(base))
                .copied()
                .unwrap_or(position.average_entry_price);
            let notional = position.quantity.abs() * mark_price;
            if let Some(payment) = cost_model.funding_payment(notional) {
                // A long position paying funding debits its quote
                // balance; a short position paying funding credits it
                // (mirroring real exchanges' sign convention where
                // funding flows from longs to shorts when the rate is
                // positive, or the reverse when negative).
                let signed_payment = if position.quantity.is_sign_positive() {
                    payment
                } else {
                    -payment
                };
                settlements.push((quote, signed_payment));
            }
        }

        for (quote, payment) in settlements {
            self.credit(&quote, -payment);
        }
        Ok(())
    }
}
```

- [ ] **Step 2: Write tests**

Add to `crates/apexes-backtest/src/ledger.rs`'s existing `#[cfg(test)] mod tests`:

```rust
    fn leveraged_fill(side: Side, price: Decimal, quantity: Decimal, leverage: Decimal) -> Fill {
        // Reuses sample_fill's shape; the Fill type itself has no
        // leverage field (only Position/OrderIntent/Order do) -- the
        // leverage is applied to the resulting Position via a manual
        // ledger mutation in these tests, since PortfolioLedger's
        // Position bookkeeping (update_position) doesn't yet thread
        // leverage through from Fill (Fill has no leverage field by
        // design -- leverage is a Position/order-level concept, not a
        // per-fill one). See Step 3 below for how tests seed a
        // leveraged Position directly.
        sample_fill(side, price, quantity, Decimal::ZERO)
    }

    fn cost_model_with_funding(funding_rate_bps: Decimal) -> CostModel {
        CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: Some(funding_rate_bps),
        }
    }

    #[test]
    fn apply_funding_payments_debits_long_position_holder() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(5)))
            .unwrap();
        // Manually mark the resulting position as leveraged (apply_fill
        // itself doesn't set leverage from a plain Fill -- see the
        // leveraged_fill helper's comment above).
        ledger.positions.get_mut("BTCUSDT").unwrap().leverage = Some(dec!(5));

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let cost_model = cost_model_with_funding(dec!(1)); // 0.01%

        let usdt_before = *ledger.balances().get("USDT").unwrap();
        ledger
            .apply_funding_payments(&mark_prices, &cost_model)
            .unwrap();
        let usdt_after = *ledger.balances().get("USDT").unwrap();

        // notional = 0.1 * 60000 = 6000; payment = 6000 * 0.0001 = 0.6
        assert_eq!(usdt_before - usdt_after, dec!(0.6));
    }

    #[test]
    fn apply_funding_payments_credits_short_position_holder() {
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));
        initial_balances.insert("BTC".to_string(), dec!(1));
        let mut ledger = PortfolioLedger::new(instruments, initial_balances);
        ledger
            .apply_fill(&leveraged_fill(Side::Sell, dec!(60000), dec!(0.1), dec!(5)))
            .unwrap();
        ledger.positions.get_mut("BTCUSDT").unwrap().leverage = Some(dec!(5));

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let cost_model = cost_model_with_funding(dec!(1));

        let usdt_before = *ledger.balances().get("USDT").unwrap();
        ledger
            .apply_funding_payments(&mark_prices, &cost_model)
            .unwrap();
        let usdt_after = *ledger.balances().get("USDT").unwrap();

        assert_eq!(usdt_after - usdt_before, dec!(0.6));
    }

    #[test]
    fn apply_funding_payments_skips_spot_positions() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(1)))
            .unwrap();
        // Deliberately NOT setting leverage -- this stays a spot position.

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let cost_model = cost_model_with_funding(dec!(1));

        let usdt_before = *ledger.balances().get("USDT").unwrap();
        ledger
            .apply_funding_payments(&mark_prices, &cost_model)
            .unwrap();
        let usdt_after = *ledger.balances().get("USDT").unwrap();

        assert_eq!(usdt_before, usdt_after, "spot positions must not be charged funding");
    }

    #[test]
    fn apply_funding_payments_is_a_noop_when_funding_disabled() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(5)))
            .unwrap();
        ledger.positions.get_mut("BTCUSDT").unwrap().leverage = Some(dec!(5));

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let cost_model = CostModel {
            fee_schedule: Box::new(FlatFeeSchedule {
                maker_bps: dec!(0),
                taker_bps: dec!(0),
            }),
            funding_rate_bps: None,
        };

        let usdt_before = *ledger.balances().get("USDT").unwrap();
        ledger
            .apply_funding_payments(&mark_prices, &cost_model)
            .unwrap();
        let usdt_after = *ledger.balances().get("USDT").unwrap();

        assert_eq!(usdt_before, usdt_after);
    }
```

**Note for the implementer:** these tests directly mutate `ledger.positions` (a private field) from within the `tests` submodule -- this is allowed since `mod tests` is a child module of `ledger.rs` and Rust privacy is scoped per-module-tree, not per-file (confirm this compiles; if `positions` is not directly accessible even from the child test module for some reason, add a `#[cfg(test)] pub(crate) fn set_position_leverage_for_test(...)` helper instead and use that -- but check first, since the existing test file structure suggests direct field access from `mod tests` should already work given `PortfolioLedger`'s fields have no `pub` visibility modifier restricting them from the same file's submodule).

Check `FlatFeeSchedule`'s import path in this file (it's used by other existing tests in `cost_model.rs`, not necessarily already imported into `ledger.rs`'s test module) via `file_read` before finalizing imports.

- [ ] **Step 3: Run this crate's tests, clippy, fmt**

Run: `cargo test -p apexes-backtest ledger`
Expected: PASS, 4/4 new tests plus all existing `ledger.rs` tests.

Run: `cargo clippy -p apexes-backtest --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-backtest/src/ledger.rs
git commit -S -s -m "feat(apexes-backtest): PortfolioLedger::apply_funding_payments -- wire up CostModel::funding_rate_bps"
```

---

### Task 4: `apexes-backtest::PortfolioLedger`: `margin_status` + `check_liquidations`

**Files:**
- Modify: `crates/apexes-backtest/src/ledger.rs`

**Interfaces:**
- Consumes: `apexes_strategy::{MarginTierTable, PositionMarginStatus, MarginMode}` (Task 2).
- Produces: `PortfolioLedger::margin_status(&self, mark_prices: &HashMap<String, Decimal>, margin_tiers: &HashMap<String, MarginTierTable>) -> HashMap<String, PositionMarginStatus>`; `PortfolioLedger::check_liquidations(&self, mark_prices: &HashMap<String, Decimal>, margin_tiers: &HashMap<String, MarginTierTable>) -> Vec<String>`.

- [ ] **Step 1: Write `margin_status`**

```rust
// crates/apexes-backtest/src/ledger.rs -- add to impl PortfolioLedger:
use apexes_strategy::{MarginMode, MarginTierTable, PositionMarginStatus};

impl PortfolioLedger {
    // ... existing methods ...

    /// Computes the real-time margin state of every currently-open
    /// leveraged position. Spot positions (`leverage: None`) are
    /// skipped entirely (they never appear in the returned map --
    /// margin doesn't apply to them, there is no sentinel entry).
    ///
    /// Isolated positions get a precise `margin_ratio` (position's own
    /// margin / that position's maintenance margin requirement, per its
    /// `MarginTierTable` bracket for its current notional) and a
    /// precise `liquidation_price` (the mark price at which
    /// `margin_ratio` would hit exactly `1.0`, solved algebraically).
    ///
    /// Cross positions get an ACCOUNT-LEVEL `margin_ratio` (this
    /// account's total equity, via `self.equity(mark_prices)`, divided
    /// by the SUM of every cross position's maintenance margin
    /// requirement) -- the same value for every cross position this
    /// account holds, since they all share one pool. `liquidation_price`
    /// is always `None` for cross positions (see `PositionMarginStatus`'s
    /// own doc comment for why a single position's liquidation price
    /// isn't a well-defined concept under cross margin).
    pub fn margin_status(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        margin_tiers: &HashMap<String, MarginTierTable>,
    ) -> HashMap<String, PositionMarginStatus> {
        let mut result = HashMap::new();

        // First pass: compute the account-wide cross-margin maintenance
        // requirement total, needed before we can compute any
        // individual cross position's margin_ratio.
        let mut cross_maintenance_total = Decimal::ZERO;
        for (symbol, position) in &self.positions {
            if position.leverage.is_none() || position.quantity.is_zero() {
                continue;
            }
            if position.margin_mode != Some(MarginMode::Cross) {
                continue;
            }
            if let Some(maintenance) = self.maintenance_requirement(symbol, position, mark_prices, margin_tiers) {
                cross_maintenance_total += maintenance;
            }
        }
        let account_equity = self.equity(mark_prices).unwrap_or(Decimal::ZERO);
        let cross_margin_ratio = if cross_maintenance_total.is_zero() {
            None
        } else {
            Some(account_equity / cross_maintenance_total)
        };

        for (symbol, position) in &self.positions {
            if position.leverage.is_none() || position.quantity.is_zero() {
                continue;
            }
            let Some(margin_mode) = position.margin_mode else {
                continue;
            };
            let maintenance = self.maintenance_requirement(symbol, position, mark_prices, margin_tiers);

            let status = match margin_mode {
                MarginMode::Isolated => {
                    let mark_price = self.mark_price_for_position(symbol, mark_prices);
                    let notional = position.quantity.abs() * mark_price;
                    let allocated_margin = notional / position.leverage.unwrap_or(Decimal::ONE);
                    let margin_ratio = maintenance
                        .filter(|m| !m.is_zero())
                        .map(|m| allocated_margin / m);
                    let liquidation_price = maintenance.and_then(|maintenance_amount| {
                        self.isolated_liquidation_price(
                            position,
                            allocated_margin,
                            maintenance_amount,
                            margin_tiers.get(symbol),
                        )
                    });
                    PositionMarginStatus {
                        margin_ratio,
                        liquidation_price,
                    }
                }
                MarginMode::Cross => PositionMarginStatus {
                    margin_ratio: cross_margin_ratio,
                    liquidation_price: None,
                },
            };
            result.insert(symbol.clone(), status);
        }

        result
    }

    /// Returns this position's maintenance margin requirement (notional
    /// * maintenance_margin_rate - maintenance_amount, per its
    /// `MarginTierTable` bracket for its current notional), or `None`
    /// if no tier table is configured for this symbol.
    fn maintenance_requirement(
        &self,
        symbol: &str,
        position: &apexes_strategy::Position,
        mark_prices: &HashMap<String, Decimal>,
        margin_tiers: &HashMap<String, MarginTierTable>,
    ) -> Option<Decimal> {
        let mark_price = self.mark_price_for_position(symbol, mark_prices);
        let notional = position.quantity.abs() * mark_price;
        let tier = margin_tiers.get(symbol)?.tier_for_notional(notional)?;
        Some(notional * tier.maintenance_margin_rate - tier.maintenance_amount)
    }

    /// Resolves the mark price to use for `symbol`'s notional/margin
    /// calculations: looks up `mark_prices` by the symbol's own base
    /// currency (matching `equity`'s existing convention), falling back
    /// to the position's `average_entry_price` if no mark price is
    /// available (a conservative approximation, not a silent zero).
    fn mark_price_for_position(&self, symbol: &str, mark_prices: &HashMap<String, Decimal>) -> Decimal {
        self.instruments
            .get(symbol)
            .and_then(|instrument| instrument.base_currency.as_ref())
            .and_then(|base| mark_prices.get(base))
            .copied()
            .unwrap_or_else(|| {
                self.positions
                    .get(symbol)
                    .map(|p| p.average_entry_price)
                    .unwrap_or(Decimal::ONE)
            })
    }

    /// Solves for the mark price at which an isolated position's
    /// `margin_ratio` would equal exactly `1.0` -- i.e.
    /// `allocated_margin + unrealized_pnl(price) == maintenance(price)`.
    /// Since `maintenance` itself depends on notional (and therefore on
    /// price), this uses the CURRENT tier's rate as a linear
    /// approximation (an accepted simplification -- see this plan's own
    /// non-goals: not a perfectly tier-boundary-aware iterative solve).
    fn isolated_liquidation_price(
        &self,
        position: &apexes_strategy::Position,
        allocated_margin: Decimal,
        _maintenance_at_current_price: Decimal,
        tier_table: Option<&MarginTierTable>,
    ) -> Option<Decimal> {
        let tier_table = tier_table?;
        let current_notional = position.quantity.abs() * position.average_entry_price;
        let tier = tier_table.tier_for_notional(current_notional)?;
        // Long: liquidation_price = entry - (allocated_margin - maintenance_amount) / (quantity * (1 - maintenance_margin_rate))
        // Short: liquidation_price = entry + (allocated_margin - maintenance_amount) / (quantity * (1 - maintenance_margin_rate))
        // (Standard linear perpetual-futures liquidation-price formula,
        // using this position's current tier's rate/amount.)
        let quantity = position.quantity.abs();
        if quantity.is_zero() {
            return None;
        }
        let denominator = quantity * (Decimal::ONE - tier.maintenance_margin_rate);
        if denominator.is_zero() {
            return None;
        }
        let margin_buffer = (allocated_margin - tier.maintenance_amount) / denominator;
        if position.quantity.is_sign_positive() {
            Some(position.average_entry_price - margin_buffer)
        } else {
            Some(position.average_entry_price + margin_buffer)
        }
    }
}
```

- [ ] **Step 2: Write `check_liquidations`**

```rust
// crates/apexes-backtest/src/ledger.rs -- add to impl PortfolioLedger:
impl PortfolioLedger {
    /// Returns every symbol whose current `margin_status` has
    /// `margin_ratio <= 1.0` (maintenance margin requirement can no
    /// longer be met by the allocated/pooled margin) -- these positions
    /// must be force-closed. Callers (`run_event_loop`, backtest/paper
    /// only -- `run_live_trading` calls this too but never force-closes,
    /// see Task 7) submit one market order per returned symbol.
    pub fn check_liquidations(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        margin_tiers: &HashMap<String, MarginTierTable>,
    ) -> Vec<String> {
        self.margin_status(mark_prices, margin_tiers)
            .into_iter()
            .filter_map(|(symbol, status)| {
                let ratio = status.margin_ratio?;
                (ratio <= Decimal::ONE).then_some(symbol)
            })
            .collect()
    }
}
```

- [ ] **Step 3: Write tests**

Add to `crates/apexes-backtest/src/ledger.rs`'s existing `#[cfg(test)] mod tests`:

```rust
    fn sample_margin_tiers() -> HashMap<String, apexes_strategy::MarginTierTable> {
        let mut tiers = HashMap::new();
        tiers.insert(
            "BTCUSDT".to_string(),
            apexes_strategy::MarginTierTable {
                tiers: vec![apexes_strategy::MarginTier {
                    notional_floor: dec!(0),
                    max_leverage: dec!(20),
                    maintenance_margin_rate: dec!(0.05),
                    maintenance_amount: dec!(0),
                }],
            },
        );
        tiers
    }

    #[test]
    fn margin_status_returns_none_ratio_for_spot_positions() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();
        // Spot position -- leverage stays None.

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let status = ledger.margin_status(&mark_prices, &sample_margin_tiers());

        assert!(
            !status.contains_key("BTCUSDT"),
            "spot positions must not appear in margin_status's output at all"
        );
    }

    #[test]
    fn margin_status_computes_isolated_margin_ratio_and_liquidation_price() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(1), dec!(0)))
            .unwrap();
        {
            let position = ledger.positions.get_mut("BTCUSDT").unwrap();
            position.leverage = Some(dec!(10));
            position.margin_mode = Some(apexes_strategy::MarginMode::Isolated);
        }

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let status = ledger.margin_status(&mark_prices, &sample_margin_tiers());

        let position_status = status.get("BTCUSDT").unwrap();
        // allocated_margin = notional/leverage = 60000/10 = 6000
        // maintenance = notional * 0.05 - 0 = 3000
        // margin_ratio = 6000 / 3000 = 2.0
        assert_eq!(position_status.margin_ratio, Some(dec!(2)));
        assert!(position_status.liquidation_price.is_some());
    }

    #[test]
    fn margin_status_returns_none_liquidation_price_for_cross_positions() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();
        {
            let position = ledger.positions.get_mut("BTCUSDT").unwrap();
            position.leverage = Some(dec!(5));
            position.margin_mode = Some(apexes_strategy::MarginMode::Cross);
        }

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000));
        let status = ledger.margin_status(&mark_prices, &sample_margin_tiers());

        let position_status = status.get("BTCUSDT").unwrap();
        assert_eq!(position_status.liquidation_price, None);
        assert!(position_status.margin_ratio.is_some());
    }

    #[test]
    fn check_liquidations_flags_a_position_whose_margin_ratio_has_collapsed() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(1), dec!(0)))
            .unwrap();
        {
            let position = ledger.positions.get_mut("BTCUSDT").unwrap();
            position.leverage = Some(dec!(20)); // very high leverage -- thin margin
            position.margin_mode = Some(apexes_strategy::MarginMode::Isolated);
        }

        // Mark price crashes far below entry -- with 20x leverage this
        // should have already breached maintenance margin.
        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(45000));
        let triggered = ledger.check_liquidations(&mark_prices, &sample_margin_tiers());

        assert!(triggered.contains(&"BTCUSDT".to_string()));
    }

    #[test]
    fn check_liquidations_does_not_flag_a_healthy_position() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();
        {
            let position = ledger.positions.get_mut("BTCUSDT").unwrap();
            position.leverage = Some(dec!(2)); // low leverage -- healthy margin
            position.margin_mode = Some(apexes_strategy::MarginMode::Isolated);
        }

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(60000)); // unchanged from entry
        let triggered = ledger.check_liquidations(&mark_prices, &sample_margin_tiers());

        assert!(!triggered.contains(&"BTCUSDT".to_string()));
    }
```

- [ ] **Step 4: Run this crate's tests, clippy, fmt**

Run: `cargo test -p apexes-backtest ledger`
Expected: PASS, all new tests plus existing ones.

Run: `cargo clippy -p apexes-backtest --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-backtest/src/ledger.rs
git commit -S -s -m "feat(apexes-backtest): PortfolioLedger::margin_status + check_liquidations"
```

---

### Task 5: `run_event_loop` wiring: auto-liquidation, funding settlement, `StrategyContext.margin_status`

**Files:**
- Modify: `crates/apexes-backtest/src/event_loop.rs`
- Create: `crates/apexes-backtest/src/mark_price_source.rs` (the `StorageReaderMarkPriceProvider` + a plain live-mark-price tracker)
- Modify: `crates/apexes-backtest/src/lib.rs`
- Modify: `crates/apexes-strategy/src/strategy.rs` (`StrategyContext` new field)
- Modify every `StrategyContext { .. }` construction site in the workspace

**Interfaces:**
- Consumes: `PortfolioLedger::{apply_funding_payments, margin_status, check_liquidations}` (Tasks 3-4), `MatchingEngine::submit_market_order` (existing), `apexes_strategy::MarkPriceProvider` (Task 2).
- Produces: `StrategyContext.margin_status: &HashMap<String, PositionMarginStatus>` (new field); `run_event_loop` gains a `margin_tiers: HashMap<String, MarginTierTable>` parameter and internally tracks `mark_prices`/`last_funding_settlement`/auto-liquidation.

- [ ] **Step 1: Add `margin_status` to `StrategyContext`**

```rust
// crates/apexes-strategy/src/strategy.rs -- StrategyContext, add field:
pub struct StrategyContext<'a> {
    pub positions: &'a HashMap<String, Position>,
    pub balances: &'a HashMap<String, Decimal>,
    pub open_orders: &'a HashMap<OrderId, Order>,
    pub open_parent_orders: &'a HashMap<ParentOrderId, ParentOrderStatus>,
    /// Real-time margin state of every currently-open leveraged
    /// position (spot positions never appear here), computed
    /// identically across backtest/paper/live via
    /// `PortfolioLedger::margin_status` -- a strategy can use this to
    /// proactively de-risk BEFORE an actual liquidation is triggered
    /// (backtest/paper auto-liquidate at `margin_ratio <= 1.0`; live
    /// trading never auto-liquidates but blocks new orders for the
    /// affected symbol via `apexes-risk::MarginMonitor`).
    pub margin_status: &'a HashMap<String, PositionMarginStatus>,
    pub now: DateTime<Utc>,
}
```

Add `PositionMarginStatus` to this file's `use apexes_strategy::...` or local imports as needed (it's defined in this SAME crate, `margin_tier.rs`, from Task 2 -- just `use crate::margin_tier::PositionMarginStatus;` or reference it directly since `strategy.rs` and `margin_tier.rs` are sibling modules in the same crate).

- [ ] **Step 2: Fix every existing `StrategyContext { .. }` construction site**

Run to find every site (re-run yourself, this plan's count may have drifted):

```bash
grep -rln "StrategyContext {" crates/
```

At drafting time this includes `crates/apexes-strategy/src/strategy.rs` (its own test), `crates/apexes-strategy/tests/drift_script_strategy.rs` (9 literals per this plan's own EMS-plan-era note -- re-count), `crates/apexes-backtest/src/event_loop.rs`, `crates/apexes-live-trading/src/runner.rs`. For every test-only site (not `event_loop.rs`, which Step 4 below handles specially), add `margin_status: &HashMap::new(),` (an empty map -- these tests don't exercise leveraged trading).

- [ ] **Step 3: Write `mark_price_source.rs`**

```rust
// crates/apexes-backtest/src/mark_price_source.rs (new file)
//! [`StorageReaderMarkPriceProvider`]: the backtest-only
//! `apexes_strategy::MarkPriceProvider` implementation, reading real
//! historical mark prices from `apexes_storage::StorageReader`'s
//! `mark_prices` table. Defined HERE (not in `apexes-storage`, which
//! doesn't depend on `apexes-strategy` where `MarkPriceProvider` is
//! defined) -- see this plan's Global Constraints.

use apexes_core::{ApexesError, Venue};
use apexes_storage::{EventType, StorageReader};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::sync::Mutex;

pub struct StorageReaderMarkPriceProvider {
    reader: StorageReader,
    /// Updated by the caller (`run_event_loop`) as it advances through
    /// replay -- "current" mark price means "as of this timestamp".
    current_timestamp: Mutex<DateTime<Utc>>,
}

impl StorageReaderMarkPriceProvider {
    pub fn new(reader: StorageReader, initial_timestamp: DateTime<Utc>) -> Self {
        Self {
            reader,
            current_timestamp: Mutex::new(initial_timestamp),
        }
    }

    pub fn advance_to(&self, timestamp: DateTime<Utc>) {
        *self.current_timestamp.lock().unwrap_or_else(|p| p.into_inner()) = timestamp;
    }
}

#[async_trait]
impl apexes_strategy::MarkPriceProvider for StorageReaderMarkPriceProvider {
    async fn mark_price(&self, symbol: &str, venue: Venue) -> Result<Decimal, ApexesError> {
        let now = *self.current_timestamp.lock().unwrap_or_else(|p| p.into_inner());
        let date = now.date_naive();
        let events = self
            .reader
            .read_batch(symbol, venue, date..date.succ_opt().unwrap_or(date), EventType::MarkPrice)
            .await
            .map_err(|error| ApexesError::InvalidBacktestConfig {
                reason: format!("failed to read mark_prices for {symbol}: {error}"),
            })?;

        events
            .into_iter()
            .filter_map(|event| match event {
                apexes_core::MarketEvent::MarkPrice(update) if update.timestamp <= now => {
                    Some((update.timestamp, update.mark_price))
                }
                _ => None,
            })
            .max_by_key(|(timestamp, _)| *timestamp)
            .map(|(_, price)| price)
            .ok_or_else(|| ApexesError::InvalidBacktestConfig {
                reason: format!("no mark price available for {symbol} at or before {now}"),
            })
    }
}
```

Register it: `pub mod mark_price_source;` in `crates/apexes-backtest/src/lib.rs`, plus `pub use mark_price_source::StorageReaderMarkPriceProvider;`.

- [ ] **Step 4: Wire `run_event_loop` for funding settlement, margin status, and auto-liquidation**

```rust
// crates/apexes-backtest/src/event_loop.rs -- EventLoopConfig gains a field:
pub struct EventLoopConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub cost_model: CostModel,
    pub queue_position_factor: Decimal,
    pub risk_free_return: Decimal,
    pub risk_checker: Option<std::sync::Arc<CompositeRiskChecker>>,
    /// Tiered maintenance-margin tables, keyed by symbol. Empty
    /// `HashMap` disables all margin/liquidation monitoring (every
    /// leveraged position's `margin_status` lookup simply finds no
    /// table and produces no margin_ratio -- an intentional silent
    /// no-op for callers that never use `OrderIntent.leverage`).
    pub margin_tiers: HashMap<String, apexes_strategy::MarginTierTable>,
}
```

Inside `run_event_loop`'s body, after the existing `last_prices` map is initialized, add:

```rust
    let mut mark_prices: HashMap<String, Decimal> = HashMap::new();
    let mut last_funding_settlement: Option<chrono::DateTime<chrono::Utc>> = None;

    /// Returns the most recent Binance-aligned 8-hour UTC boundary
    /// (00:00/08:00/16:00) at or before `timestamp`.
    fn last_funding_boundary(timestamp: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
        use chrono::Timelike;
        let hour_bucket = (timestamp.hour() / 8) * 8;
        timestamp
            .date_naive()
            .and_hms_opt(hour_bucket, 0, 0)
            .unwrap()
            .and_utc()
    }
```

Inside the main loop's `match &event { ... }` block, add a new arm (alongside the existing `MarketEvent::Depth`/`MarketEvent::Trade` arms) to maintain the mark-price map:

```rust
            MarketEvent::MarkPrice(update) => {
                mark_prices.insert(update.symbol.clone(), update.mark_price);
            }
```

Immediately after the existing `refresh_open_parent_orders(...).await;` call (before the `StrategyContext` is constructed), add the funding-settlement and margin-status/liquidation logic:

```rust
        let current_funding_boundary = last_funding_boundary(timestamp);
        if last_funding_settlement != Some(current_funding_boundary) {
            ledger.apply_funding_payments(&mark_prices, &config.cost_model)?;
            last_funding_settlement = Some(current_funding_boundary);
        }

        let margin_status = ledger.margin_status(&mark_prices, &config.margin_tiers);
        if let Some(checker) = &risk_checker {
            if let Err(violations) = checker.on_tick(&margin_status) {
                tracing::warn!(?violations, "position monitor flagged a margin violation");
            }
        }

        let symbols_to_liquidate = ledger.check_liquidations(&mark_prices, &config.margin_tiers);
        for symbol in symbols_to_liquidate {
            let Some(position) = ledger.positions().get(&symbol).cloned() else {
                continue;
            };
            let liquidation_intent = apexes_strategy::OrderIntent {
                symbol: symbol.clone(),
                venue: instruments_by_symbol
                    .get(&symbol)
                    .map(|i| i.venue)
                    .unwrap_or(apexes_core::Venue::Binance),
                asset_class: instruments_by_symbol
                    .get(&symbol)
                    .map(|i| i.asset_class)
                    .unwrap_or(apexes_core::AssetClass::Perpetual),
                side: if position.quantity.is_sign_positive() {
                    apexes_core::Side::Sell
                } else {
                    apexes_core::Side::Buy
                },
                order_type: OrderType::Market,
                quantity: position.quantity.abs(),
                price: None,
                stop_price: None,
                execution_algo: None,
                routing: apexes_strategy::OrderRouting::Fixed,
                leverage: None,
                margin_mode: None,
            };
            match lock_engine(&matching_engine).submit_market_order(&liquidation_intent, timestamp) {
                Ok((order, fills)) => {
                    tracing::error!(symbol = %symbol, "position force-liquidated (margin ratio breached)");
                    open_orders.insert(order.id, order);
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
                Err(error) => {
                    tracing::error!(?error, symbol = %symbol, "liquidation order submission failed");
                }
            }
        }
```

Then extend the `StrategyContext` construction with the new field:

```rust
        let ctx = StrategyContext {
            positions: ledger.positions(),
            balances: ledger.balances(),
            open_orders: &open_orders,
            open_parent_orders: &open_parent_orders,
            margin_status: &margin_status,
            now: timestamp,
        };
```

**Note on `CompositeRiskChecker::on_tick`:** this method does not exist yet -- Task 6 adds it. This task's `run_event_loop` code depends on it, so implement this task's OTHER changes first, verify `cargo check -p apexes-backtest` fails ONLY on the missing `on_tick` method (not anything else), and either wait for Task 6 to land first (recommended: do Task 6 before this task if executing out of plan order) or add a temporary local shim. Given this plan's stated task ORDER, Task 6 comes after this one -- if your environment executes tasks strictly in the written order, add a temporary private helper in `event_loop.rs`:

```rust
// TEMPORARY until Task 6 adds CompositeRiskChecker::on_tick -- delete
// this shim and use `checker.on_tick(...)` directly once Task 6 lands.
fn on_tick_shim(
    _checker: &CompositeRiskChecker,
    _margin_status: &HashMap<String, apexes_strategy::PositionMarginStatus>,
) -> Result<(), Vec<apexes_risk::RiskViolation>> {
    Ok(())
}
```

and call `on_tick_shim(checker, &margin_status)` instead of `checker.on_tick(&margin_status)` for now, clearly commented as temporary. Task 6 replaces this shim with the real call and removes the shim function entirely -- record this explicitly in your Task 5 report so Task 6's implementer/reviewer knows to look for it.

- [ ] **Step 5: Update `run_backtest`/`run_paper_trading`'s callers of `run_event_loop` for the new `EventLoopConfig.margin_tiers` field**

```bash
grep -rln "EventLoopConfig {" crates/
```

Add `margin_tiers: HashMap::new(),` to every existing `EventLoopConfig { .. }` construction site (an empty map preserves today's exact behavior -- no margin tables configured means no margin monitoring happens, a safe default for any caller not yet using leveraged trading).

- [ ] **Step 6: Run this crate's tests, clippy, fmt, then a workspace-wide check**

Run: `cargo test -p apexes-backtest`
Expected: PASS, 0 failures.

Run: `cargo check --workspace`
Expected: fails ONLY on `apexes-risk`'s missing `on_tick` if you used the temporary shim approach (expected, resolved by Task 6) -- confirm no OTHER unrelated failures.

Run: `cargo clippy -p apexes-backtest --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -S -s -m "feat(apexes-backtest): wire funding settlement, margin status, and auto-liquidation into run_event_loop"
```

---

### Task 6: `apexes-risk::MarginMonitor` + `CompositeRiskChecker`'s `PositionMonitor` list

**Files:**
- Create: `crates/apexes-risk/src/position_monitor.rs`
- Create: `crates/apexes-risk/src/margin_monitor.rs`
- Modify: `crates/apexes-risk/src/composite.rs`
- Modify: `crates/apexes-risk/src/error.rs`
- Modify: `crates/apexes-risk/src/lib.rs`
- Modify: `crates/apexes-backtest/src/event_loop.rs` (replace Task 5's temporary shim, if used)

**Interfaces:**
- Consumes: `apexes_strategy::PositionMarginStatus` (Task 2).
- Produces: `apexes_risk::PositionMonitor` trait (`fn on_tick(&mut self, margin_status: &HashMap<String, PositionMarginStatus>) -> Result<(), Vec<RiskViolation>>`), `apexes_risk::MarginMonitor` (+ `split()` into `MarginMonitorPreTradeAdapter`/`MarginMonitorPositionAdapter`), `CompositeRiskChecker::{with_position_monitor, on_tick}`.

- [ ] **Step 1: Add the `PositionMonitor` trait**

```rust
// crates/apexes-risk/src/position_monitor.rs (new file)
//! [`PositionMonitor`]: called once per event-loop tick (not tied to
//! fills, unlike `InTradeMonitor`) with the latest margin state of
//! every leveraged position, so implementations (e.g. `MarginMonitor`)
//! can react to margin deterioration caused purely by mark-price
//! movement -- which `InTradeMonitor::on_fill` alone cannot observe,
//! since it only fires after a NEW fill.

use std::collections::HashMap;

use apexes_strategy::PositionMarginStatus;

use crate::error::RiskViolation;

pub trait PositionMonitor: Send + Sync {
    fn on_tick(
        &mut self,
        margin_status: &HashMap<String, PositionMarginStatus>,
    ) -> Result<(), Vec<RiskViolation>>;
}
```

- [ ] **Step 2: Add a `RiskViolation::MarginRatioBreached` variant**

```rust
// crates/apexes-risk/src/error.rs -- add to the RiskViolation enum:
    #[error("margin ratio for {symbol} is {ratio} (at or below the warning threshold {threshold})")]
    MarginRatioBreached {
        symbol: String,
        ratio: Decimal,
        threshold: Decimal,
    },
```

- [ ] **Step 3: Write `margin_monitor.rs`**

```rust
// crates/apexes-risk/src/margin_monitor.rs (new file)
//! [`MarginMonitor`]: trips (opens) for a symbol when its margin ratio
//! falls at or below `warning_threshold`, blocking new orders for that
//! symbol until it recovers. Architecturally mirrors
//! [`crate::circuit_breaker::CircuitBreaker`] (shared state behind
//! `Arc<Mutex<_>>`, `split()` into two thin trait adapters) -- but
//! updates its tripped state from `PositionMonitor::on_tick` (driven by
//! margin state, itself driven by mark price -- not fills), unlike
//! `CircuitBreaker`'s `InTradeMonitor::on_fill`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use apexes_core::{Order, OrderId};
use apexes_strategy::{OrderIntent, Position, PositionMarginStatus};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::error::RiskViolation;
use crate::position_monitor::PositionMonitor;
use crate::pre_trade::PreTradeCheck;

pub struct MarginMonitor {
    /// A symbol trips when its `margin_ratio <= warning_threshold`
    /// (e.g. `1.5` -- trip BEFORE the `<= 1.0` auto-liquidation
    /// threshold `PortfolioLedger::check_liquidations` uses, giving a
    /// buffer for new-order blocking to actually help).
    warning_threshold: Decimal,
    tripped_symbols: HashSet<String>,
}

impl MarginMonitor {
    pub fn new(warning_threshold: Decimal) -> Self {
        Self {
            warning_threshold,
            tripped_symbols: HashSet::new(),
        }
    }

    pub fn split(self) -> (MarginMonitorPreTradeAdapter, MarginMonitorPositionAdapter) {
        let shared = Arc::new(Mutex::new(self));
        (
            MarginMonitorPreTradeAdapter(Arc::clone(&shared)),
            MarginMonitorPositionAdapter(shared),
        )
    }

    pub fn is_tripped(&self, symbol: &str) -> bool {
        self.tripped_symbols.contains(symbol)
    }

    pub fn reset(&mut self, symbol: &str) {
        self.tripped_symbols.remove(symbol);
    }

    fn on_tick_impl(
        &mut self,
        margin_status: &HashMap<String, PositionMarginStatus>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut violations = Vec::new();
        for (symbol, status) in margin_status {
            let Some(ratio) = status.margin_ratio else {
                continue;
            };
            if ratio <= self.warning_threshold {
                self.tripped_symbols.insert(symbol.clone());
                violations.push(RiskViolation::MarginRatioBreached {
                    symbol: symbol.clone(),
                    ratio,
                    threshold: self.warning_threshold,
                });
            } else {
                self.tripped_symbols.remove(symbol);
            }
        }
        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    fn check_impl(&self, intent: &OrderIntent) -> Result<(), Vec<RiskViolation>> {
        if self.tripped_symbols.contains(&intent.symbol) {
            return Err(vec![RiskViolation::MarginRatioBreached {
                symbol: intent.symbol.clone(),
                ratio: Decimal::ZERO,
                threshold: self.warning_threshold,
            }]);
        }
        Ok(())
    }
}

pub struct MarginMonitorPreTradeAdapter(Arc<Mutex<MarginMonitor>>);

impl PreTradeCheck for MarginMonitorPreTradeAdapter {
    fn check(
        &self,
        intent: &OrderIntent,
        _positions: &HashMap<String, Position>,
        _balances: &HashMap<String, Decimal>,
        _open_orders: &HashMap<OrderId, Order>,
        _now: DateTime<Utc>,
    ) -> Result<(), Vec<RiskViolation>> {
        let monitor = self.0.lock().unwrap_or_else(|p| p.into_inner());
        monitor.check_impl(intent)
    }
}

pub struct MarginMonitorPositionAdapter(Arc<Mutex<MarginMonitor>>);

impl PositionMonitor for MarginMonitorPositionAdapter {
    fn on_tick(
        &mut self,
        margin_status: &HashMap<String, PositionMarginStatus>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut monitor = self.0.lock().unwrap_or_else(|p| p.into_inner());
        monitor.on_tick_impl(margin_status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, OrderType, Side, Venue};
    use rust_decimal_macros::dec;

    fn sample_intent(symbol: &str) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Perpetual,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            execution_algo: None,
            routing: apexes_strategy::OrderRouting::Fixed,
            leverage: Some(dec!(10)),
            margin_mode: Some(apexes_strategy::MarginMode::Isolated),
        }
    }

    fn margin_status_with_ratio(symbol: &str, ratio: Decimal) -> HashMap<String, PositionMarginStatus> {
        let mut map = HashMap::new();
        map.insert(
            symbol.to_string(),
            PositionMarginStatus {
                margin_ratio: Some(ratio),
                liquidation_price: Some(dec!(50000)),
            },
        );
        map
    }

    #[test]
    fn trips_when_margin_ratio_breaches_warning_threshold() {
        let monitor = MarginMonitor::new(dec!(1.5));
        let (pre_trade, mut position) = monitor.split();

        let result = position.on_tick(&margin_status_with_ratio("BTCUSDT", dec!(1.2)));
        assert!(result.is_err());

        let check_result = pre_trade.check(
            &sample_intent("BTCUSDT"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Utc::now(),
        );
        assert!(check_result.is_err());
    }

    #[test]
    fn does_not_trip_for_a_healthy_margin_ratio() {
        let monitor = MarginMonitor::new(dec!(1.5));
        let (pre_trade, mut position) = monitor.split();

        let result = position.on_tick(&margin_status_with_ratio("BTCUSDT", dec!(3.0)));
        assert!(result.is_ok());

        let check_result = pre_trade.check(
            &sample_intent("BTCUSDT"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Utc::now(),
        );
        assert!(check_result.is_ok());
    }

    #[test]
    fn auto_resets_once_margin_ratio_recovers() {
        let monitor = MarginMonitor::new(dec!(1.5));
        let (pre_trade, mut position) = monitor.split();

        position
            .on_tick(&margin_status_with_ratio("BTCUSDT", dec!(1.2)))
            .unwrap_err();
        position
            .on_tick(&margin_status_with_ratio("BTCUSDT", dec!(2.0)))
            .unwrap();

        let check_result = pre_trade.check(
            &sample_intent("BTCUSDT"),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            Utc::now(),
        );
        assert!(check_result.is_ok());
    }
}
```

- [ ] **Step 4: Extend `CompositeRiskChecker` with the `PositionMonitor` list**

```rust
// crates/apexes-risk/src/composite.rs -- add imports:
use crate::position_monitor::PositionMonitor;
use apexes_strategy::PositionMarginStatus;
```

```rust
// crates/apexes-risk/src/composite.rs -- CompositeRiskChecker struct, add field:
#[derive(Default)]
pub struct CompositeRiskChecker {
    pre_trade_checks: Mutex<Vec<Box<dyn PreTradeCheck>>>,
    in_trade_monitors: Mutex<Vec<Box<dyn InTradeMonitor>>>,
    position_monitors: Mutex<Vec<Box<dyn PositionMonitor>>>,
}
```

```rust
// crates/apexes-risk/src/composite.rs -- add to impl CompositeRiskChecker:
    pub fn with_position_monitor(self, monitor: Box<dyn PositionMonitor>) -> Self {
        self.position_monitors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(monitor);
        self
    }

    /// Runs every registered `PositionMonitor::on_tick`, collecting
    /// every violation from every monitor into one combined result.
    /// Called once per event-loop tick, independent of fills (see
    /// `PositionMonitor`'s own doc comment for why).
    pub fn on_tick(
        &self,
        margin_status: &HashMap<String, PositionMarginStatus>,
    ) -> Result<(), Vec<RiskViolation>> {
        let mut monitors = self
            .position_monitors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let violations: Vec<RiskViolation> = monitors
            .iter_mut()
            .filter_map(|monitor| monitor.on_tick(margin_status).err())
            .flatten()
            .collect();

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }
```

Add a test mirroring the existing `on_fill_aggregates_violations_from_multiple_monitors` test's structure, but for `on_tick`/`PositionMonitor` (check `composite.rs`'s existing test module for the exact pattern to copy before writing this).

- [ ] **Step 5: Register the new module**

```rust
// crates/apexes-risk/src/lib.rs -- add:
pub mod margin_monitor;
pub mod position_monitor;
```

Check `lib.rs`'s existing `pub use` list and add `MarginMonitor`/`PositionMonitor` re-exports following the same convention as `CircuitBreaker`'s existing re-export.

- [ ] **Step 6: Replace Task 5's temporary shim in `event_loop.rs`, if it was used**

If Task 5 added the temporary `on_tick_shim` function: delete it, and change its call site back to `checker.on_tick(&margin_status)` directly (now that `CompositeRiskChecker::on_tick` genuinely exists). If Task 5 was executed AFTER this task in your environment's actual order (i.e. no shim exists), skip this step entirely.

- [ ] **Step 7: Run this crate's tests, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-risk`
Expected: PASS, including the 3 new `MarginMonitor` tests and the new `CompositeRiskChecker::on_tick` aggregation test.

Run: `cargo build --workspace`
Expected: 0 errors (this should be the point where the whole workspace compiles cleanly again, with no remaining shims).

Run: `cargo test --workspace -- --test-threads=1`
Expected: 0 failures (same pre-existing environmental-limitation ignored tests, nothing new).

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -S -s -m "feat(apexes-risk): MarginMonitor + PositionMonitor trait + CompositeRiskChecker.on_tick"
```

---

### Task 7: `run_live_trading` wiring: monitor-only margin behavior

**Files:**
- Modify: `crates/apexes-live-trading/src/runner.rs`

**Interfaces:**
- Consumes: `PortfolioLedger::{margin_status, check_liquidations, apply_funding_payments}` (Tasks 3-4), `CompositeRiskChecker::on_tick` (Task 6).
- Produces: no new public interfaces -- `run_live_trading` now maintains mark prices from `MarketEvent::MarkPrice`, calls the same margin-status/liquidation-check logic every tick, but NEVER submits a liquidation order (only logs).

- [ ] **Step 1: Track mark prices and add the monitor-only margin/funding logic**

```rust
// crates/apexes-live-trading/src/runner.rs -- add near the top of
// run_live_trading's local state (alongside the existing `last_prices`):
    let mut mark_prices: HashMap<String, Decimal> = HashMap::new();
    let mut last_funding_settlement: Option<chrono::DateTime<chrono::Utc>> = None;
```

Add the same `last_funding_boundary` helper function this plan's Task 5 added to `apexes-backtest/src/event_loop.rs` (duplicated here -- `apexes-live-trading` cannot import a private function from another crate; this mirrors the existing project convention of duplicating small helpers like `refresh_open_parent_orders` across `event_loop.rs`/`runner.rs`, per this project's prior EMS sub-project ledger notes):

```rust
fn last_funding_boundary(timestamp: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
    use chrono::Timelike;
    let hour_bucket = (timestamp.hour() / 8) * 8;
    timestamp
        .date_naive()
        .and_hms_opt(hour_bucket, 0, 0)
        .unwrap()
        .and_utc()
}
```

In the `market_event = market_events.next() => { ... }` branch, extend `update_last_price`'s existing match (or add a parallel check) to also update `mark_prices` when the event is `MarketEvent::MarkPrice`:

```rust
                if let MarketEvent::MarkPrice(update) = &event {
                    mark_prices.insert(update.symbol.clone(), update.mark_price);
                }
```

After the existing `refresh_open_parent_orders(ems.as_ref(), &mut open_parent_orders).await;` call and before `handle_market_event` is invoked, add:

```rust
                let current_funding_boundary = last_funding_boundary(event_timestamp(&event));
                if last_funding_settlement != Some(current_funding_boundary) {
                    // Live trading's own funding settlement is
                    // informational/local-ledger-only -- the REAL
                    // funding charge already happened on the exchange
                    // itself; this keeps our local ledger's equity
                    // curve consistent with what the exchange actually
                    // charged, matching backtest/paper's accounting
                    // convention for comparability, not because this
                    // ledger is authoritative for real funds.
                    ledger.apply_funding_payments(&mark_prices, &config.cost_model)?;
                    last_funding_settlement = Some(current_funding_boundary);
                }

                let margin_status = ledger.margin_status(&mark_prices, &config.margin_tiers);
                if let Some(checker) = &risk_checker {
                    if let Err(violations) = checker.on_tick(&margin_status) {
                        tracing::warn!(?violations, "position monitor flagged a margin violation (live)");
                    }
                }

                // MONITOR-ONLY: never submit a liquidation order in
                // live trading -- only log loudly and rely on
                // MarginMonitor's PreTradeCheck to block new orders for
                // the affected symbol. Real liquidation is the
                // exchange's own responsibility; this local ledger has
                // no authority to force-close a real position.
                for symbol in ledger.check_liquidations(&mark_prices, &config.margin_tiers) {
                    tracing::error!(
                        symbol = %symbol,
                        "LIVE TRADING: position margin ratio has breached the liquidation threshold -- \
                         this system will NOT auto-liquidate; the exchange's own liquidation engine may \
                         act independently. New orders for this symbol are blocked via MarginMonitor."
                    );
                }
```

**Note for the implementer:** `LiveTradingConfig` needs a `cost_model: CostModel` and `margin_tiers: HashMap<String, MarginTierTable>` field added (check whether `LiveTradingConfig` already has a `cost_model`-equivalent field via `file_read` before assuming -- if it doesn't have any cost/fee-related config at all today, since live trading doesn't simulate fills, you may need to add a minimal `cost_model: CostModel` field purely for the `funding_payment` computation's sake, or reconsider whether live funding settlement should use a simpler direct calculation not routed through `CostModel` at all -- if `CostModel` genuinely isn't already threaded into `apexes-live-trading` anywhere, STOP and report this as a design question rather than guessing, since the design spec assumed it would be reachable here without checking this crate's actual current fields).

- [ ] **Step 2: Update `handle_market_event`'s `StrategyContext` construction**

Find the existing `StrategyContext { .. }` construction in this file (inside `handle_market_event`) and add `margin_status: margin_status_ref,` where `margin_status_ref` is passed in as a new parameter to `handle_market_event` (threading the `margin_status` computed in Step 1 through to where the context is actually built -- check this file's exact current parameter-passing structure via `file_read` before finalizing, since `handle_market_event`'s exact signature may need a new `&HashMap<String, PositionMarginStatus>` parameter added alongside its existing ones).

- [ ] **Step 3: Run this crate's tests, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-live-trading -- --test-threads=1`
Expected: PASS, 0 failures.

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-live-trading/src/runner.rs
git commit -S -s -m "feat(apexes-live-trading): monitor-only margin/funding tracking in run_live_trading (no auto-liquidation)"
```

---

### Task 8: `apexes-oms::OrderManager::submit_order`: thread `leverage`/`margin_mode` through

**Files:**
- Modify: `crates/apexes-oms/src/manager.rs`

**Interfaces:**
- Consumes: `apexes_strategy::OrderIntent.leverage`/`.margin_mode` (Task 2), `apexes_core::Order.leverage`/`.margin_mode` (Task 1).
- Produces: no new public interfaces -- `OrderManager::submit_order`'s existing `Order` construction now copies the two new fields from the incoming `OrderIntent` instead of the temporary `None, None` Task 1 left there.

- [ ] **Step 1: Fix `submit_order`'s `Order` construction**

```rust
// crates/apexes-oms/src/manager.rs -- submit_order's Order construction, change:
        let mut order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: intent.symbol,
            venue: intent.venue,
            asset_class: intent.asset_class,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: intent.price,
            stop_price: intent.stop_price,
            leverage: intent.leverage,
            margin_mode: intent.margin_mode,
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
```

(This moves `intent.leverage`/`intent.margin_mode` before `intent` is otherwise consumed by-value elsewhere in this function -- check the surrounding code doesn't already move `intent` earlier in a way that would make this access invalid; if so, capture these two fields into local variables before the earlier move happens.)

- [ ] **Step 2: Run this crate's tests, clippy, fmt**

Run: `cargo test -p apexes-oms -- --test-threads=1`
Expected: PASS, 0 failures.

Run: `cargo clippy -p apexes-oms --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 3: Commit**

```bash
git add crates/apexes-oms/src/manager.rs
git commit -S -s -m "feat(apexes-oms): thread OrderIntent.leverage/.margin_mode through OrderManager::submit_order"
```

---

### Task 9: `apexes-gateway-binance`: real leverage/margin-mode API wiring

**Files:**
- Modify: `crates/apexes-gateway-binance/src/gateway.rs`
- Create: `crates/apexes-gateway-binance/tests/wiremock_leverage.rs`

**Interfaces:**
- Consumes: `apexes_core::Order.leverage`/`.margin_mode` (Task 1), `BinanceRestClient::signed_request` (existing).
- Produces: no new public interfaces on `ExecutionGateway` itself (the trait is unchanged) -- `BinanceExecutionGateway::submit_order` now internally calls Binance's leverage/margin-type endpoints before submitting, when needed.

- [ ] **Step 1: Add the leverage/margin-mode cache and setup logic**

```rust
// crates/apexes-gateway-binance/src/gateway.rs -- add imports:
use apexes_core::MarginMode;
use std::collections::HashMap;
use std::sync::Mutex;
```

```rust
// crates/apexes-gateway-binance/src/gateway.rs -- BinanceExecutionGateway struct, add field:
pub struct BinanceExecutionGateway {
    rest: BinanceRestClient,
    /// Per-symbol leverage/margin-mode last successfully set on the
    /// exchange -- avoids redundant `POST /fapi/v1/leverage`/
    /// `marginType` calls when consecutive orders for the same symbol
    /// use the same settings (Binance requires calling these BEFORE
    /// order submission, but not on every single order if unchanged).
    leverage_cache: Mutex<HashMap<String, (rust_decimal::Decimal, MarginMode)>>,
}
```

```rust
// crates/apexes-gateway-binance/src/gateway.rs -- BinanceExecutionGateway::new, update:
impl BinanceExecutionGateway {
    pub fn new(config: BinanceGatewayConfig) -> Self {
        Self {
            rest: BinanceRestClient::new(config),
            leverage_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Ensures `symbol` is set to `leverage`/`margin_mode` on the
    /// exchange BEFORE an order using them is submitted -- Binance
    /// requires `POST /fapi/v1/marginType` and `POST /fapi/v1/leverage`
    /// as separate, prior calls (not part of the order-submission
    /// request itself). Skips both calls entirely if the cache already
    /// reflects the requested values (no-op for repeated same-leverage
    /// orders). Returns `Err(reason)` if either call fails -- the
    /// caller (`submit_order`) treats this as `GatewayOutcome::Rejected`,
    /// never attempting the order submission itself.
    async fn ensure_leverage_and_margin_mode(
        &self,
        symbol: &str,
        leverage: rust_decimal::Decimal,
        margin_mode: MarginMode,
    ) -> Result<(), String> {
        {
            let cache = self.leverage_cache.lock().unwrap_or_else(|p| p.into_inner());
            if cache.get(symbol) == Some(&(leverage, margin_mode)) {
                return Ok(());
            }
        }

        let margin_type_str = match margin_mode {
            MarginMode::Isolated => "ISOLATED",
            MarginMode::Cross => "CROSSED",
        };
        let margin_type_params = format!("symbol={symbol}&marginType={margin_type_str}");
        let (status, body) = self
            .rest
            .signed_request("POST", ApiFamily::Futures, "/fapi/v1/marginType", &margin_type_params)
            .await?;
        // Binance returns an error code -4046 ("No need to change margin
        // type") when the requested mode is already active -- treat
        // this specific case as success, not a failure, since it means
        // the desired end state is already true.
        if !status.is_success() && !body.contains("-4046") {
            return Err(format!("failed to set margin type for {symbol}: HTTP {status}: {body}"));
        }

        let leverage_params = format!("symbol={symbol}&leverage={leverage}");
        let (status, body) = self
            .rest
            .signed_request("POST", ApiFamily::Futures, "/fapi/v1/leverage", &leverage_params)
            .await?;
        if !status.is_success() {
            return Err(format!("failed to set leverage for {symbol}: HTTP {status}: {body}"));
        }

        self.leverage_cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(symbol.to_string(), (leverage, margin_mode));
        Ok(())
    }
}
```

- [ ] **Step 2: Wire `submit_order` to call it first when needed**

Find this crate's existing `impl ExecutionGateway for BinanceExecutionGateway`'s `submit_order` method (via `file_read` on `gateway.rs` first to see its exact current body) and add, as the VERY FIRST thing inside the method body (before any existing request-building logic):

```rust
    async fn submit_order(&self, order: &Order) -> GatewayOutcome {
        if let (Some(leverage), Some(margin_mode)) = (order.leverage, order.margin_mode) {
            if let Err(reason) = self
                .ensure_leverage_and_margin_mode(&order.symbol, leverage, margin_mode)
                .await
            {
                return GatewayOutcome::Rejected {
                    reason: format!("leverage/margin mode setup failed: {reason}"),
                };
            }
        }
        // ... existing submit_order body, UNCHANGED, continues here ...
    }
```

(Spot orders, where `order.leverage` is `None`, skip this entirely and behave exactly as today.)

- [ ] **Step 3: Write `wiremock_leverage.rs`**

```rust
// crates/apexes-gateway-binance/tests/wiremock_leverage.rs (new file)
//! Verifies `BinanceExecutionGateway` calls Binance's leverage/margin-type
//! endpoints before submitting a leveraged order, caches the result to
//! avoid redundant calls, and rejects the order (without submitting it)
//! if either setup call fails.

use apexes_core::{AssetClass, MarginMode, Order, OrderId, OrderStatus, OrderType, Side, Venue};
use apexes_gateway_binance::{BinanceExecutionGateway, BinanceGatewayConfig};
use apexes_oms::{ExecutionGateway, GatewayOutcome};
use chrono::Utc;
use rust_decimal_macros::dec;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn leveraged_order(id: Uuid) -> Order {
    Order {
        id: OrderId(id),
        exchange_order_id: None,
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Perpetual,
        side: Side::Buy,
        order_type: OrderType::Market,
        quantity: dec!(1),
        price: None,
        stop_price: None,
        leverage: Some(dec!(10)),
        margin_mode: Some(MarginMode::Isolated),
        filled_quantity: dec!(0),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

async fn gateway_against(mock_server: &MockServer) -> BinanceExecutionGateway {
    BinanceExecutionGateway::new(BinanceGatewayConfig {
        api_key: "test-api-key".to_string(),
        api_secret: "test-api-secret".to_string(),
        spot_base_url: mock_server.uri(),
        futures_base_url: mock_server.uri(),
        recv_window_ms: 5000,
    })
}

#[tokio::test]
async fn submit_order_sets_margin_type_and_leverage_before_submitting() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fapi/v1/marginType"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"code": 200, "msg": "success"})))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/fapi/v1/leverage"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"leverage": 10, "symbol": "BTCUSDT"})))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/fapi/v1/order"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"orderId": 999})))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&leveraged_order(Uuid::new_v4())).await;

    assert_eq!(
        outcome,
        GatewayOutcome::Accepted {
            exchange_order_id: "999".to_string()
        }
    );
}

#[tokio::test]
async fn submit_order_does_not_re_set_leverage_for_the_same_symbol_and_settings() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fapi/v1/marginType"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"code": 200})))
        .expect(1) // exactly once across BOTH submit_order calls below
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/fapi/v1/leverage"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"leverage": 10})))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/fapi/v1/order"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"orderId": 1})))
        .expect(2)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    gateway.submit_order(&leveraged_order(Uuid::new_v4())).await;
    gateway.submit_order(&leveraged_order(Uuid::new_v4())).await; // same symbol/leverage/mode

    // wiremock's .expect(1) on marginType/leverage above asserts the
    // exact-once-not-twice expectation when the mock server is dropped
    // at the end of the test.
}

#[tokio::test]
async fn submit_order_is_rejected_without_submitting_when_leverage_setup_fails() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/fapi/v1/marginType"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"code": 200})))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/fapi/v1/leverage"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({"code": -4028, "msg": "Leverage is not valid"})))
        .mount(&mock_server)
        .await;
    // Deliberately no mock for /fapi/v1/order -- if submit_order calls
    // it anyway, wiremock will return a 404 rather than the test
    // silently passing.

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&leveraged_order(Uuid::new_v4())).await;

    assert!(matches!(outcome, GatewayOutcome::Rejected { .. }));
}
```

- [ ] **Step 4: Run this crate's tests, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-gateway-binance --test wiremock_leverage`
Expected: PASS, 3/3 new tests.

Run: `cargo test -p apexes-gateway-binance`
Expected: PASS, all existing tests plus the new ones (0 regressions -- confirm spot orders, which have `leverage: None`, skip the new leverage-setup path entirely and behave exactly as before).

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-gateway-binance/src/gateway.rs crates/apexes-gateway-binance/tests/wiremock_leverage.rs
git commit -S -s -m "feat(apexes-gateway-binance): set leverage/margin mode before submitting leveraged orders"
```

---

### Task 10: Final whole-branch code review and `docs/FEATURES.md` update

**Files:**
- Modify: `docs/FEATURES.md`

**Interfaces:** none (documentation/review-only task).

- [ ] **Step 1: Run the complete verification suite one final time**

Run: `cargo build --workspace`, `cargo test --workspace -- --test-threads=1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`.
Expected: all clean, matching every prior task's own verification (this is a final re-confirmation before declaring this plan done, per this project's established multi-sub-project-completion precedent).

- [ ] **Step 2: Escalate to a whole-branch review**

Per this project's established precedent (sub-project #8's and #9's final reviews, both escalated to a stronger reviewer profile for a correctness-critical whole-branch pass that individual per-task reviews can miss): dispatch an independent reviewer over the ENTIRE branch diff for this plan (base: the commit before Task 1 started, head: Task 9's final commit) -- not just individual tasks in isolation. Pay particular attention to: the isolated-vs-cross margin_ratio computation's correctness end-to-end, the auto-liquidation order construction in `run_event_loop` (correct side/quantity/no leverage-recursion), whether `run_live_trading`'s monitor-only path could ever accidentally submit a liquidation order, and whether every `Order`/`OrderIntent`/`Position`/`StrategyContext` construction site across the whole workspace was genuinely updated (not just the ones this plan's individual tasks explicitly enumerated). Fix any findings, get a fix-verification re-review, and only then consider this plan complete.

- [ ] **Step 3: Update `docs/FEATURES.md`**

Find row `6.4` (`grep -n "| 6.4 |" docs/FEATURES.md`) and update its Status column from `🔲 Not Started` to `✅ Done`, with Notes summarizing: leveraged `Position`/`OrderIntent` model (isolated + cross margin), tiered maintenance-margin brackets, real mark-price-driven margin ratio/liquidation price computation, periodic Binance-aligned funding settlement, `apexes-risk::MarginMonitor`, backtest/paper auto-liquidation with live-trading monitor-only behavior, and real Binance leverage/margin-type API wiring in `apexes-gateway-binance`.

Recompute the Summary counts table via actual `grep -c "✅ Done"`/`grep -c "🔲 Not Started"` (per this project's explicit standing instruction: NEVER estimate these counts, always recompute them from the real table contents) -- subtract 1 from each raw count for the legend rows near the top of the file that also contain these exact strings as their own legend-key text (verify via `grep -n` before trusting the raw count, per this project's standing memory note about this recurring mistake).

- [ ] **Step 4: Commit**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: mark FEATURES.md row 6.4 (margin & leverage monitoring) as Done"
```
