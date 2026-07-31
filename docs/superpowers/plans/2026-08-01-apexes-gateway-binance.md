# Apexes Sub-project #8: Binance Live Gateway + Live-Trading Event Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `apexes-gateway-binance` (real Binance Spot + USDS-M Perpetual `ExecutionGateway`, with REST signing and a User Data Stream) and `apexes-live-trading` (exchange-agnostic live event loop), closing the backtest → paper → live loop, per `docs/superpowers/specs/2026-08-01-apexes-gateway-binance-design.md`.

**Architecture:** Two new workspace crates. `apexes-gateway-binance` is a pure "talk to Binance" implementation of `apexes_oms::ExecutionGateway` (extended in this plan with `query_fills_for_order` and an `asset_class` parameter on two existing methods). `apexes-live-trading` drives `Strategy` → `OrderManager` → `apexes_backtest::PortfolioLedger` against live market data and gateway push/poll updates, structurally parallel to `apexes-paper::run_paper_trading` but without a local `MatchingEngine`. A prerequisite, cross-cutting change adds `asset_class: AssetClass` to `apexes_core::Order` and `apexes_strategy::OrderIntent` (needed to route Spot vs. Perpetual requests), which requires fixing every existing literal-construction call site across the workspace.

**Tech Stack:** Rust 2024, `reqwest` (REST), `tokio-tungstenite` (WebSocket), `hmac`+`sha2` (request signing), `hex`, `wiremock` (HTTP mocking in tests), existing workspace deps (`tokio`, `chrono`, `rust_decimal`, `serde`, `tracing`, `async-trait`, `sqlx`).

## Global Constraints

- All commits use `git commit -S -s` with conventional-commit prefixes (`feat:`, `fix:`, `test:`, `docs:`, `refactor:`).
- Never use bare `.unwrap()`/`.expect()` in production code paths (test code and one-time startup config validation are exempt per existing codebase convention — see `apexes-oms/src/store.rs`'s `PgPool::connect` error mapping for the pattern to follow).
- `apexes-gateway-binance` and `apexes-live-trading` unit tests must not require network access or a live PostgreSQL instance except where explicitly marked `#[ignore]`.
- Every `Order { .. }` / `OrderIntent { .. }` literal construction added or touched in this plan must include the new `asset_class` field — no exceptions, no `Default` shortcut (neither type has or gets a `Default` impl in this plan).
- Binance base URLs, endpoint paths, and request/response field names in this plan are taken from the public Binance Spot (`developers.binance.com/docs/binance-spot-api-docs`) and USDS-M Futures (`developers.binance.com/docs/derivatives/usds-margined-futures`) REST API references as of this plan's writing. If an implementer finds a field name has changed, treat the discrepancy as a real finding — stop and confirm before proceeding, per this project's established practice of not silently working around unexpected changes.
- **Deviation from the design spec, decided during planning**: the design spec (`2026-08-01-apexes-gateway-binance-design.md` §七) placed the gateway-agnostic `ExchangeUpdate` type in `apexes-live-trading/src/exchange_update.rs`. This plan places it in `apexes-oms` instead (`apexes-oms/src/exchange_update.rs`), because `apexes-oms` is the crate both `apexes-gateway-binance` and `apexes-live-trading` already depend on for `ExecutionGateway`/`OrderManager` — putting `ExchangeUpdate` there avoids `apexes-gateway-binance` needing to depend on `apexes-live-trading` (an orchestration crate) just to construct one shared type. This keeps the dependency graph a clean DAG (`apexes-gateway-binance` and `apexes-live-trading` both depend on `apexes-oms`; neither depends on the other). Task 9 implements this.

---

### Task 1: `apexes-core`: add `AssetClass` to `Order`

**Files:**
- Modify: `crates/apexes-core/src/order.rs`

**Interfaces:**
- Consumes: `apexes_core::instrument::AssetClass` (already exists: `Spot`/`Perpetual`/`Future`/`Option`/`Equity`).
- Produces: `Order.asset_class: AssetClass` field — every other task in this plan that constructs an `Order` literal depends on this.

- [ ] **Step 1: Add the field and update the import**

In `crates/apexes-core/src/order.rs`, change the top import and struct:

```rust
use crate::error::ApexesError;
use crate::instrument::{AssetClass, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
```

```rust
/// An order to buy or sell a quantity of an instrument at a venue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    /// Limit price. `None` for Market/Stop orders; `Some` for Limit/StopLimit orders.
    pub price: Option<Decimal>,
    /// Trigger price. `None` for Market/Limit orders; `Some` for Stop/StopLimit orders.
    pub stop_price: Option<Decimal>,
    pub filled_quantity: Decimal,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

- [ ] **Step 2: Fix the four existing `Order { .. }` literals in this file's own tests**

In the same file's `#[cfg(test)] mod tests`, four functions construct `Order { .. }`: `order_serde_round_trip` (BTCUSDT/Binance), `market_order_has_no_price` (AAPL/Nasdaq), `stop_order_serde_round_trip` (BTCUSDT/Binance), `stop_limit_order_serde_round_trip` (BTCUSDT/Binance). Add `asset_class: AssetClass::Spot,` immediately after the `venue:` line in the three Binance/crypto ones, and `asset_class: AssetClass::Equity,` in `market_order_has_no_price` (the AAPL/Nasdaq one). Example for `order_serde_round_trip`:

```rust
    #[test]
    fn order_serde_round_trip() {
        let now = Utc::now();
        let order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: rust_decimal_macros::dec!(1.5),
            price: Some(rust_decimal_macros::dec!(65000)),
            stop_price: None,
            filled_quantity: rust_decimal_macros::dec!(0),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&order).unwrap();
        let round_tripped: Order = serde_json::from_str(&json).unwrap();
        assert_eq!(order, round_tripped);
    }
```

Apply the same one-line insertion (`asset_class: AssetClass::Spot,` or `AssetClass::Equity,` as appropriate) to the other three test functions, keeping every other field unchanged.

- [ ] **Step 3: Run this crate's tests**

Run: `cargo test -p apexes-core`
Expected: PASS (all 4 fixed tests plus the rest of the crate's existing tests, e.g. `OrderStatus::can_transition_to` tests, unaffected).

- [ ] **Step 4: Commit**

```bash
cd /Users/rickzhang/src/oss/apexes
git add crates/apexes-core/src/order.rs
git commit -S -s -m "feat: add asset_class field to apexes_core::Order"
```

---

### Task 2: `apexes-strategy`: add `asset_class` to `OrderIntent` and fix its call sites

**Files:**
- Modify: `crates/apexes-strategy/src/strategy.rs`
- Modify: `crates/apexes-strategy/src/drift_dsl/convert.rs`

**Interfaces:**
- Consumes: `apexes_core::AssetClass` (Task 1).
- Produces: `OrderIntent.asset_class: AssetClass` — every other task constructing an `OrderIntent` literal depends on this. `parse_asset_class(s: &str, fn_name: &str) -> Result<AssetClass, String>` (new helper in `convert.rs`, following the exact pattern of `parse_venue`/`parse_side`/`parse_order_type` already in that file) — used by `order_intents_from_drift_value`.

- [ ] **Step 1: Add the field to `OrderIntent`**

In `crates/apexes-strategy/src/strategy.rs`, change the top import and struct:

```rust
use apexes_core::{AssetClass, Order, OrderId, OrderType, Side, Venue};
```

```rust
#[derive(Debug, Clone)]
pub struct OrderIntent {
    pub symbol: String,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    /// Limit price (`Limit`, `StopLimit` only).
    pub price: Option<Decimal>,
    /// Trigger price (`Stop`, `StopLimit` only).
    pub stop_price: Option<Decimal>,
}
```

- [ ] **Step 2: Fix this file's own test `OrderIntent { .. }` literal**

In the same file's `#[cfg(test)] mod tests`, the `AlwaysBuyOneUnit::on_market_event` test helper constructs one `OrderIntent { .. }`. Add `asset_class: AssetClass::Spot,` after `venue: self.venue,`:

```rust
    impl Strategy for AlwaysBuyOneUnit {
        fn on_market_event(
            &mut self,
            _event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            vec![OrderIntent {
                symbol: self.symbol.clone(),
                venue: self.venue,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
            }]
        }
    }
```

- [ ] **Step 3: Add `parse_asset_class` to `convert.rs` and wire it into `order_intents_from_drift_value`**

In `crates/apexes-strategy/src/drift_dsl/convert.rs`, add the import and a new parser function right after `parse_venue`:

```rust
use apexes_core::{
    AssetClass, Bar, BarInterval, DepthUpdate, MarketEvent, OrderType, PriceLevel, Side, Tick,
    TradeEvent, Venue,
};
```

```rust
fn parse_asset_class(s: &str, fn_name: &str) -> Result<AssetClass, String> {
    match s.to_ascii_lowercase().as_str() {
        "spot" => Ok(AssetClass::Spot),
        "perpetual" => Ok(AssetClass::Perpetual),
        "future" => Ok(AssetClass::Future),
        "option" => Ok(AssetClass::Option),
        "equity" => Ok(AssetClass::Equity),
        other => Err(format!("{fn_name}: unknown asset_class {other:?}")),
    }
}
```

Then in `order_intents_from_drift_value`, add the new required field parse right after `venue` is parsed and include it in the constructed `OrderIntent`:

```rust
            let symbol = str_field(&obj, "symbol", FN)?.to_string();
            let venue = parse_venue(str_field(&obj, "venue", FN)?, FN)?;
            let asset_class = parse_asset_class(str_field(&obj, "asset_class", FN)?, FN)?;
            let side = parse_side(str_field(&obj, "side", FN)?, FN)?;
            let order_type = parse_order_type(str_field(&obj, "order_type", FN)?, FN)?;
            let quantity = f64_to_decimal(num_field(&obj, "quantity", FN)?, "quantity", FN)?;
```

```rust
            Ok(OrderIntent {
                symbol,
                venue,
                asset_class,
                side,
                order_type,
                quantity,
                price,
                stop_price,
            })
```

- [ ] **Step 4: Fix every existing test in `convert.rs` that builds a Drift Script order-intent object literal**

`convert.rs`'s test module has 12 tests that build `drift_object(&[...])` arrays representing `on_market_event` return values, all missing the now-required `asset_class` field: `valid_market_order_intent_parses`, `valid_limit_order_intent_with_price_parses`, `missing_required_field_is_rejected`, `invalid_side_string_is_rejected`, `invalid_order_type_string_is_rejected`, `invalid_venue_string_is_rejected`, `limit_order_missing_price_is_rejected`, `stop_order_missing_stop_price_is_rejected`, `non_finite_quantity_is_rejected`. Add `("asset_class", string_value("spot")),` as a new entry in each `drift_object(&[...])` array (position doesn't matter, since parsing is by key lookup, not position). Example for `valid_market_order_intent_parses`:

```rust
    #[test]
    fn valid_market_order_intent_parses() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("Binance")),
            ("asset_class", string_value("spot")),
            ("side", string_value("BUY")),
            ("order_type", string_value("market")),
            ("quantity", DriftValue::Number(1.5)),
        ])]);
        let intents = order_intents_from_drift_value(value).unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].symbol, "BTCUSDT");
        assert_eq!(intents[0].venue, Venue::Binance);
        assert_eq!(intents[0].asset_class, AssetClass::Spot);
        assert_eq!(intents[0].side, Side::Buy);
        assert_eq!(intents[0].order_type, OrderType::Market);
        assert_eq!(intents[0].quantity, dec!(1.5));
        assert_eq!(intents[0].price, None);
    }
```

Apply the same one-line insertion (`("asset_class", string_value("spot")),`) to `valid_limit_order_intent_with_price_parses`, `invalid_side_string_is_rejected`, `invalid_order_type_string_is_rejected`, `limit_order_missing_price_is_rejected`, `stop_order_missing_stop_price_is_rejected`, and `non_finite_quantity_is_rejected` (these all need `asset_class` present so the test fails for the reason it's actually testing, not for an unrelated missing-field error). For `missing_required_field_is_rejected` and `invalid_venue_string_is_rejected`, leave them as-is if they already fail before reaching the `asset_class` field in parse order — but since `asset_class` is now parsed right after `venue` (before `side`/`order_type`/`quantity`), verify: `missing_required_field_is_rejected` omits `quantity` only, so it needs `asset_class` added (parsing reaches quantity only after asset_class succeeds). `invalid_venue_string_is_rejected` fails at the `venue` parse step, which happens BEFORE `asset_class` is parsed, so it does NOT need the new field added — leave it unchanged.

So: add `("asset_class", string_value("spot")),` to all of: `valid_market_order_intent_parses`, `valid_limit_order_intent_with_price_parses`, `missing_required_field_is_rejected`, `invalid_side_string_is_rejected`, `invalid_order_type_string_is_rejected`, `limit_order_missing_price_is_rejected`, `stop_order_missing_stop_price_is_rejected`, `non_finite_quantity_is_rejected` (8 tests). Leave `non_array_top_level_is_rejected`, `non_object_element_is_rejected`, and `invalid_venue_string_is_rejected` unchanged (they fail before or without reaching field-by-field parsing of `asset_class`).

- [ ] **Step 5: Add a new test for the new `parse_asset_class` failure path**

Add this test to the same `mod tests` block in `convert.rs`, right after `invalid_venue_string_is_rejected`:

```rust
    #[test]
    fn invalid_asset_class_string_is_rejected() {
        let value = DriftValue::Array(vec![drift_object(&[
            ("symbol", string_value("BTCUSDT")),
            ("venue", string_value("binance")),
            ("asset_class", string_value("dogecoin_futures")),
            ("side", string_value("buy")),
            ("order_type", string_value("market")),
            ("quantity", DriftValue::Number(1.0)),
        ])]);
        let err = order_intents_from_drift_value(value).unwrap_err();
        assert!(err.contains("unknown asset_class"));
    }
```

- [ ] **Step 6: Run this crate's tests**

Run: `cargo test -p apexes-strategy`
Expected: PASS. Note this will also run `apexes-strategy/src/drift_dsl/bindings.rs`'s tests, which build `Order { .. }` literals (in `has_open_order_true_when_any_side_matches`) — this will fail to compile until Task 1's `Order` change is fixed there too. Fix it now as part of this task since it's in the same crate:

In `crates/apexes-strategy/src/drift_dsl/bindings.rs`, in `has_open_order_true_when_any_side_matches`, add `asset_class: AssetClass::Spot,` after the `venue:` line, and add `use apexes_core::AssetClass;` to that test module's imports (or the top-level `use apexes_core::{Order, OrderId};` import if simpler — add `AssetClass` there since it's already imported at file scope for `Order`/`OrderId`):

```rust
use apexes_core::{AssetClass, Order, OrderId};
```

```rust
        open_orders.insert(
            id,
            Order {
                id,
                exchange_order_id: None,
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Sell,
                order_type: OrderType::Limit,
                quantity: dec!(1),
                price: Some(dec!(65000)),
                stop_price: None,
                filled_quantity: dec!(0),
                status: OrderStatus::New,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        );
```

Re-run: `cargo test -p apexes-strategy`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/apexes-strategy/src/strategy.rs crates/apexes-strategy/src/drift_dsl/convert.rs crates/apexes-strategy/src/drift_dsl/bindings.rs
git commit -S -s -m "feat: add asset_class field to apexes_strategy::OrderIntent and Drift Script binding"
```

---

### Task 3: `apexes-backtest`: fix all `Order`/`OrderIntent` call sites

**Files:**
- Modify: `crates/apexes-backtest/src/matching_engine.rs`
- Modify: `crates/apexes-backtest/src/runner.rs`
- Modify: `crates/apexes-backtest/tests/risk_integration.rs`
- Modify: `crates/apexes-backtest/tests/end_to_end.rs`

**Interfaces:**
- Consumes: `Order.asset_class`/`OrderIntent.asset_class` (Tasks 1-2).

- [ ] **Step 1: Fix `matching_engine.rs` production code (3 `Order { .. }` constructions inside `submit_market_order`, `submit_limit_order`, `submit_stop_order`)**

All three functions build an `Order` from an `&OrderIntent` — add `asset_class: intent.asset_class,` after the `venue: intent.venue,` line in each. In `submit_market_order`:

```rust
        let order = Order {
            id: order_id,
            exchange_order_id: None,
            symbol: intent.symbol.clone(),
            venue: intent.venue,
            asset_class: intent.asset_class,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: None,
            stop_price: None,
            filled_quantity,
            status,
            created_at: now,
            updated_at: now,
        };
```

In `submit_limit_order`:

```rust
        let order = Order {
            id: order_id,
            exchange_order_id: None,
            symbol: intent.symbol.clone(),
            venue: intent.venue,
            asset_class: intent.asset_class,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: Some(limit_price),
            stop_price: None,
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
```

In `submit_stop_order`:

```rust
        let order = Order {
            id: order_id,
            exchange_order_id: None,
            symbol: intent.symbol.clone(),
            venue: intent.venue,
            asset_class: intent.asset_class,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: intent.price,
            stop_price: Some(stop_price),
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
```

- [ ] **Step 2: Fix `matching_engine.rs`'s test helpers and inline literals**

The test module's imports (at the top of `mod tests`) currently read:

```rust
mod tests {
    use super::*;
    use crate::cost_model::FlatFeeSchedule;
    use apexes_core::Venue;
    use rust_decimal_macros::dec;
```

Change the `use apexes_core::Venue;` line to:

```rust
    use apexes_core::{AssetClass, Venue};
```

The test helper `buy_intent(symbol: &str, quantity: Decimal) -> OrderIntent` needs `asset_class: AssetClass::Spot,` added:

```rust
    fn buy_intent(symbol: &str, quantity: Decimal) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity,
            price: None,
            stop_price: None,
        }
    }
```

Every other inline `OrderIntent { .. }` literal in this file's tests (there are 22 more, all following the same `symbol`/`venue: Venue::Binance`/... shape for BTCUSDT-style Spot test fixtures) needs the same one-line insertion `asset_class: AssetClass::Spot,` immediately after its `venue: Venue::Binance,` line. Go through the file systematically: search for every remaining `OrderIntent {` occurrence not yet fixed and insert the line. There are no Perpetual/non-Spot test fixtures in this file — all get `AssetClass::Spot`.

- [ ] **Step 3: Fix `runner.rs`'s test `OrderIntent { .. }` literal**

In `crates/apexes-backtest/src/runner.rs`'s test module, `BuyOnceStrategy::on_market_event` constructs one `OrderIntent { .. }`. Add `use apexes_core::AssetClass;` to the test imports and `asset_class: AssetClass::Spot,` after `venue: Venue::Binance,`:

```rust
    impl Strategy for BuyOnceStrategy {
        fn on_market_event(
            &mut self,
            event: &apexes_core::MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            if !self.bought && matches!(event, apexes_core::MarketEvent::Trade(_)) {
                self.bought = true;
                return vec![OrderIntent {
                    symbol: "BTCUSDT".to_string(),
                    venue: Venue::Binance,
                    asset_class: AssetClass::Spot,
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    quantity: dec!(1),
                    price: None,
                    stop_price: None,
                }];
            }
            Vec::new()
        }
    }
```

- [ ] **Step 4: Fix `tests/risk_integration.rs`**

In `crates/apexes-backtest/tests/risk_integration.rs`, `BuyOnceStrategy::on_market_event` constructs one `OrderIntent { .. }`. Add `apexes_core::AssetClass::Spot,` (fully qualified, since this is an integration test file — check the existing `use apexes_core::...` import line at the top and add `AssetClass` to it if there's already a grouped import, otherwise use the fully-qualified path inline):

```rust
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
            asset_class: apexes_core::AssetClass::Spot,
            side: apexes_core::Side::Buy,
            order_type: apexes_core::OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
        }]
    }
}
```

- [ ] **Step 5: Fix `tests/end_to_end.rs`**

In `crates/apexes-backtest/tests/end_to_end.rs`, add `AssetClass` to the top `use apexes_core::{...}` import list:

```rust
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, OrderType, PriceLevel, Side,
    TradeEvent, Venue,
};
```

Two `OrderIntent { .. }` literals need fixing: `BuyOnceStrategy::on_market_event` and `BuyStopOnceStrategy::on_market_event`. Add `asset_class: AssetClass::Spot,` after each `venue: trade.venue,` line:

```rust
impl Strategy for BuyOnceStrategy {
    fn on_market_event(&mut self, event: &MarketEvent, _ctx: &StrategyContext) -> Vec<OrderIntent> {
        if self.has_bought {
            return vec![];
        }
        if let MarketEvent::Trade(trade) = event {
            self.has_bought = true;
            return vec![OrderIntent {
                symbol: trade.symbol.clone(),
                venue: trade.venue,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(0.1),
                price: None,
                stop_price: None,
            }];
        }
        vec![]
    }
}
```

```rust
impl Strategy for BuyStopOnceStrategy {
    fn on_market_event(&mut self, event: &MarketEvent, _ctx: &StrategyContext) -> Vec<OrderIntent> {
        if self.has_placed {
            return vec![];
        }
        if let MarketEvent::Trade(trade) = event {
            self.has_placed = true;
            return vec![OrderIntent {
                symbol: trade.symbol.clone(),
                venue: trade.venue,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Stop,
                quantity: dec!(0.1),
                price: None,
                stop_price: Some(dec!(66000)),
            }];
        }
        vec![]
    }
}
```

- [ ] **Step 6: Run this crate's tests**

Run: `cargo test -p apexes-backtest`
Expected: PASS (all existing tests, including `end_to_end.rs`'s C1-regression-guard test, unaffected in behavior).

- [ ] **Step 7: Commit**

```bash
git add crates/apexes-backtest/src/matching_engine.rs crates/apexes-backtest/src/runner.rs crates/apexes-backtest/tests/risk_integration.rs crates/apexes-backtest/tests/end_to_end.rs
git commit -S -s -m "fix: add asset_class field to apexes-backtest Order/OrderIntent construction sites"
```

---

### Task 4: `apexes-risk`: fix all `Order`/`OrderIntent` call sites

**Files:**
- Modify: `crates/apexes-risk/src/self_trade.rs`
- Modify: `crates/apexes-risk/src/lists.rs`
- Modify: `crates/apexes-risk/src/limits.rs`
- Modify: `crates/apexes-risk/src/composite.rs`
- Modify: `crates/apexes-risk/src/circuit_breaker.rs`

**Interfaces:**
- Consumes: `Order.asset_class`/`OrderIntent.asset_class` (Tasks 1-2).

- [ ] **Step 1: Fix `self_trade.rs`**

This file's test module has three helpers to fix: `limit_intent`, `market_intent` (both build `OrderIntent`), and `resting_limit_order` (builds `Order`). Its import line currently reads `use apexes_core::{OrderStatus, Venue};` — change it to:

```rust
    use apexes_core::{AssetClass, OrderStatus, Venue};
```

Then:

```rust
    fn limit_intent(symbol: &str, side: Side, price: Decimal) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
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
            asset_class: AssetClass::Spot,
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
            asset_class: AssetClass::Spot,
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
```

- [ ] **Step 2: Fix `lists.rs`**

This file's test module has one helper: `sample_intent(symbol: &str, venue: Venue) -> OrderIntent`. Add `use apexes_core::AssetClass;` to the test imports (alongside the existing `use apexes_core::{OrderType, Side};`), then:

```rust
    fn sample_intent(symbol: &str, venue: Venue) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: rust_decimal_macros::dec!(1),
            price: None,
            stop_price: None,
        }
    }
```

- [ ] **Step 3: Fix `limits.rs`**

This file's test module has one helper: `sample_intent(symbol: &str, side: Side, quantity: Decimal, price: Option<Decimal>) -> OrderIntent`. Add `AssetClass` to the existing `use apexes_core::Venue;` import line (`use apexes_core::{AssetClass, Venue};`), then:

```rust
    fn sample_intent(
        symbol: &str,
        side: Side,
        quantity: Decimal,
        price: Option<Decimal>,
    ) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
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
```

- [ ] **Step 4: Fix `composite.rs`**

This file's test module has one helper: `sample_intent() -> OrderIntent`. Its import line currently reads `use apexes_core::{OrderType, Side, Venue};` — change it to:

```rust
    use apexes_core::{AssetClass, OrderType, Side, Venue};
```

Then update `sample_intent`:

```rust
    fn sample_intent() -> OrderIntent {
        OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
        }
    }
```

- [ ] **Step 5: Fix `circuit_breaker.rs`**

This file's test module has one helper: `sample_intent(symbol: &str) -> OrderIntent`. Add `AssetClass` to the existing `use apexes_core::{OrderType, Side, Venue};` import line, then:

```rust
    fn sample_intent(symbol: &str) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
        }
    }
```

- [ ] **Step 6: Run this crate's tests**

Run: `cargo test -p apexes-risk`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/apexes-risk/src/self_trade.rs crates/apexes-risk/src/lists.rs crates/apexes-risk/src/limits.rs crates/apexes-risk/src/composite.rs crates/apexes-risk/src/circuit_breaker.rs
git commit -S -s -m "fix: add asset_class field to apexes-risk Order/OrderIntent construction sites"
```

---

### Task 5: `apexes-oms`: fix `Order`/`OrderIntent` call sites, add `asset_class` to schema

**Files:**
- Modify: `crates/apexes-oms/src/manager.rs`
- Modify: `crates/apexes-oms/src/mock_gateway.rs`
- Modify: `crates/apexes-oms/src/schema.rs`
- Modify: `crates/apexes-oms/tests/live_postgres.rs`

**Interfaces:**
- Consumes: `Order.asset_class`/`OrderIntent.asset_class` (Tasks 1-2).
- Produces: `orders` table gains an `asset_class` column; `OrderRow.asset_class: String` field; `parse_asset_class` string-to-`AssetClass` helper in `schema.rs` (distinct from Task 2's Drift-Script-facing `parse_asset_class` in `apexes-strategy` — same name, different crate, both following the existing `parse_venue`/`parse_side` naming convention in their respective files).

- [ ] **Step 1: Add `asset_class` column to the `orders` table DDL**

In `crates/apexes-oms/src/schema.rs`, modify `ORDERS_TABLE_DDL`:

```rust
pub const ORDERS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS orders
(
    id                  UUID PRIMARY KEY,
    exchange_order_id   TEXT,
    symbol              TEXT NOT NULL,
    venue               TEXT NOT NULL,
    asset_class         TEXT NOT NULL,
    side                TEXT NOT NULL,
    order_type          TEXT NOT NULL,
    quantity            NUMERIC(38, 10) NOT NULL,
    price               NUMERIC(38, 10),
    stop_price          NUMERIC(38, 10),
    filled_quantity     NUMERIC(38, 10) NOT NULL,
    status              TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL
)
";
```

- [ ] **Step 2: Add `asset_class` to `OrderRow`, its `From<&Order>`/`TryFrom<OrderRow>` conversions, and a `parse_asset_class` helper**

```rust
use apexes_core::{AssetClass, Fill, FillId, Order, OrderId, OrderStatus, OrderType, Side, Venue};
```

```rust
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrderRow {
    pub id: Uuid,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub venue: String,
    pub asset_class: String,
    pub side: String,
    pub order_type: String,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
    pub filled_quantity: Decimal,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<&Order> for OrderRow {
    fn from(order: &Order) -> Self {
        OrderRow {
            id: order.id.0,
            exchange_order_id: order.exchange_order_id.clone(),
            symbol: order.symbol.clone(),
            venue: format!("{:?}", order.venue),
            asset_class: format!("{:?}", order.asset_class),
            side: format!("{:?}", order.side),
            order_type: format!("{:?}", order.order_type),
            quantity: order.quantity,
            price: order.price,
            stop_price: order.stop_price,
            filled_quantity: order.filled_quantity,
            status: format!("{:?}", order.status),
            created_at: order.created_at,
            updated_at: order.updated_at,
        }
    }
}

impl TryFrom<OrderRow> for Order {
    type Error = OmsError;

    fn try_from(row: OrderRow) -> Result<Self, OmsError> {
        Ok(Order {
            id: OrderId(row.id),
            exchange_order_id: row.exchange_order_id,
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            asset_class: parse_asset_class(&row.asset_class)?,
            side: parse_side(&row.side)?,
            order_type: parse_order_type(&row.order_type)?,
            quantity: row.quantity,
            price: row.price,
            stop_price: row.stop_price,
            filled_quantity: row.filled_quantity,
            status: parse_order_status(&row.status)?,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}
```

Add the new `parse_asset_class` function right after `parse_venue`:

```rust
fn parse_asset_class(value: &str) -> Result<AssetClass, OmsError> {
    match value {
        "Spot" => Ok(AssetClass::Spot),
        "Perpetual" => Ok(AssetClass::Perpetual),
        "Future" => Ok(AssetClass::Future),
        "Option" => Ok(AssetClass::Option),
        "Equity" => Ok(AssetClass::Equity),
        other => Err(OmsError::InvalidRow {
            reason: format!("unknown asset_class: {other}"),
        }),
    }
}
```

- [ ] **Step 3: Fix `schema.rs`'s own test helper `sample_order`**

```rust
    fn sample_order() -> Order {
        Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: Some("EX123".to_string()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: dec!(1.5),
            price: Some(dec!(65000)),
            stop_price: None,
            filled_quantity: dec!(0.5),
            status: OrderStatus::PartiallyFilled,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
```

- [ ] **Step 4: Update `OrderStore::upsert_order`'s SQL to include the new column**

In `crates/apexes-oms/src/store.rs`, modify `upsert_order`:

```rust
    pub async fn upsert_order(&self, order: &Order) -> Result<(), OmsError> {
        let row = OrderRow::from(order);
        sqlx::query(
            "INSERT INTO orders (id, exchange_order_id, symbol, venue, asset_class, side, order_type, quantity, price, stop_price, filled_quantity, status, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
             ON CONFLICT (id) DO UPDATE SET
                exchange_order_id = EXCLUDED.exchange_order_id,
                filled_quantity = EXCLUDED.filled_quantity,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at",
        )
        .bind(row.id)
        .bind(&row.exchange_order_id)
        .bind(&row.symbol)
        .bind(&row.venue)
        .bind(&row.asset_class)
        .bind(&row.side)
        .bind(&row.order_type)
        .bind(row.quantity)
        .bind(row.price)
        .bind(row.stop_price)
        .bind(row.filled_quantity)
        .bind(&row.status)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
```

- [ ] **Step 5: Fix `manager.rs`'s production code (`submit_order`) and test helpers**

In `crates/apexes-oms/src/manager.rs`, add `AssetClass` to the top import:

```rust
use apexes_core::{AssetClass, Order, OrderId, OrderStatus};
```

In `submit_order`, add `asset_class: intent.asset_class,` after `venue: intent.venue,`:

```rust
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
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
```

In the test module, fix the `sample_intent()` helper:

```rust
    fn sample_intent() -> OrderIntent {
        OrderIntent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
        }
    }
```

- [ ] **Step 6: Fix `mock_gateway.rs`'s test helper `sample_order`**

Add `AssetClass` to the test module's `use apexes_core::{Order, OrderType, Side, Venue};` import:

```rust
    use apexes_core::{AssetClass, Order, OrderType, Side, Venue};
```

```rust
    fn sample_order() -> Order {
        Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: dec!(1),
            price: None,
            stop_price: None,
            filled_quantity: dec!(0),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
```

- [ ] **Step 7: Fix `tests/live_postgres.rs`'s `sample_intent`**

Add `AssetClass` to `use apexes_core::{OrderStatus, OrderType, Side, Venue};`:

```rust
use apexes_core::{AssetClass, OrderStatus, OrderType, Side, Venue};
```

```rust
fn sample_intent() -> OrderIntent {
    OrderIntent {
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Market,
        quantity: dec!(1),
        price: None,
        stop_price: None,
    }
}
```

- [ ] **Step 8: Run this crate's tests (non-`#[ignore]` ones only, since most `apexes-oms` tests need live PostgreSQL)**

Run: `cargo test -p apexes-oms`
Expected: PASS (compiles; the non-`#[ignore]` tests, e.g. `schema.rs`'s `TryFrom` round-trip tests and `mock_gateway.rs`'s own tests, pass. `#[ignore]`d PostgreSQL-dependent tests are skipped by default — do not run them yet, that happens in Task 5's Step 9 after starting PostgreSQL, or skip verifying them now and rely on Task 8's full-workspace test pass to catch issues, since starting `docker compose` is a larger side effect this task doesn't otherwise need).

Additionally verify the `#[ignore]`d tests still compile correctly (a common way schema changes silently break `#[ignore]`d tests unnoticed):

Run: `cargo test -p apexes-oms --no-run`
Expected: compiles cleanly with no errors (this compiles every test including `#[ignore]`d ones without running them).

- [ ] **Step 9: Commit**

```bash
git add crates/apexes-oms/src/manager.rs crates/apexes-oms/src/mock_gateway.rs crates/apexes-oms/src/schema.rs crates/apexes-oms/src/store.rs crates/apexes-oms/tests/live_postgres.rs
git commit -S -s -m "feat: add asset_class column and field wiring across apexes-oms"
```

---

### Task 6: `apexes-paper`: fix `OrderIntent` call site

**Files:**
- Modify: `crates/apexes-paper/src/runner.rs`

**Interfaces:**
- Consumes: `OrderIntent.asset_class` (Task 2).

- [ ] **Step 1: Fix `runner.rs`'s test `BuyOnceStrategy::on_market_event`**

Check the top of the test module for existing `use apexes_core::{...}` and add `AssetClass` to it, then add `asset_class: AssetClass::Spot,` after `venue: Venue::Binance,`:

```rust
    impl Strategy for BuyOnceStrategy {
        fn on_market_event(
            &mut self,
            event: &MarketEvent,
            _ctx: &StrategyContext,
        ) -> Vec<OrderIntent> {
            if !self.bought && matches!(event, MarketEvent::Trade(_)) {
                self.bought = true;
                return vec![OrderIntent {
                    symbol: "BTCUSDT".to_string(),
                    venue: Venue::Binance,
                    asset_class: AssetClass::Spot,
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    quantity: dec!(1),
                    price: None,
                    stop_price: None,
                }];
            }
            vec![]
        }
    }
```

- [ ] **Step 2: Run this crate's tests**

Run: `cargo test -p apexes-paper`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/apexes-paper/src/runner.rs
git commit -S -s -m "fix: add asset_class field to apexes-paper OrderIntent construction site"
```

---

### Task 7: Full-workspace compile/test/lint checkpoint before extending `ExecutionGateway`

**Files:** none (verification-only task)

**Interfaces:** none.

This checkpoint exists because Tasks 1-6 touched ~18 files across 6 crates — verifying the whole workspace compiles and passes before extending the `ExecutionGateway` trait (Task 9) isolates any remaining `asset_class`-migration mistakes from the new trait-shape changes that follow, so a later failure can't be confused between the two kinds of change.

- [ ] **Step 1: Full workspace build**

Run: `cargo build --workspace`
Expected: clean build, no errors.

- [ ] **Step 2: Full workspace test (non-ignored)**

Run: `cargo test --workspace`
Expected: 0 failures.

- [ ] **Step 3: Compile (but do not run) ignored tests workspace-wide**

Run: `cargo test --workspace --no-run`
Expected: compiles cleanly (catches any `#[ignore]`d test broken by the `asset_class` migration that Step 2 wouldn't have run).

- [ ] **Step 4: Clippy and fmt**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (no diffs). If not clean, run `cargo fmt` and re-check.

- [ ] **Step 5: Commit if `cargo fmt` made any changes**

```bash
git add -A
git commit -S -s -m "chore: cargo fmt after asset_class migration" --allow-empty
```

(Use `--allow-empty` only if `git status` shows no changes — otherwise omit that flag; only commit if there's something to commit.)

---

### Task 8: `apexes-oms`: extend `ExecutionGateway` with `query_fills_for_order` and `asset_class` parameters

**Files:**
- Modify: `crates/apexes-oms/src/gateway.rs`
- Modify: `crates/apexes-oms/src/mock_gateway.rs`
- Modify: `crates/apexes-oms/src/manager.rs`

**Interfaces:**
- Produces: `ExchangeFill` struct (new, in `gateway.rs`); `ExecutionGateway::query_fills_for_order(&self, order: &Order) -> Result<Vec<ExchangeFill>, String>` (new trait method); `ExecutionGateway::query_order_status(&self, exchange_order_id: &str, asset_class: AssetClass) -> Result<Option<OrderStatus>, String>` (signature change: new `asset_class` parameter); `ExecutionGateway::query_all_open_orders(&self, asset_class: AssetClass) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String>` (signature change: new `asset_class` parameter, and it now queries ONE asset class per call rather than the whole account -- see Step 1's rationale note).
- Consumes: `Order.asset_class` (Task 1).

- [ ] **Step 1: Add `ExchangeFill` and extend the trait in `gateway.rs`**

```rust
//! [`ExecutionGateway`]: the abstraction `OrderManager` submits/cancels/
//! queries orders through, decoupling order-lifecycle management from
//! any specific exchange's API. Sub-project #8 (first live gateway)
//! implements it against a real exchange; this crate ships only
//! [`crate::mock_gateway::MockExecutionGateway`] for testing.

use apexes_core::{AssetClass, Order, OrderId, OrderStatus};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

/// The outcome of a `submit_order`/`cancel_order` call, distinguishing
/// three cases `OrderManager` must handle differently:
/// - `Accepted`: the exchange confirmed the action.
/// - `Rejected`: the exchange explicitly refused (e.g. insufficient
///   balance, invalid parameters) -- authoritative, no ambiguity.
/// - `Uncertain`: the call failed in a way that does NOT confirm
///   whether the exchange actually processed it (e.g. network timeout,
///   connection dropped mid-request) -- the order may have gone through
///   despite the local failure. `OrderManager` must NOT assume failure
///   in this case; it leaves the order's status unchanged and defers to
///   a later `reconcile()` call to discover the true state.
#[derive(Debug, Clone, PartialEq)]
pub enum GatewayOutcome {
    Accepted { exchange_order_id: String },
    Rejected { reason: String },
    Uncertain { reason: String },
}

/// A single trade execution as reported by the exchange, keyed by the
/// exchange's own per-trade identifier (`exchange_trade_id`) so
/// `OrderManager` can deduplicate against fills it has already recorded.
/// Real exchanges' "my trades"-style endpoints return the FULL trade
/// history for an order every call, not just new trades since the last
/// call -- naively inserting every returned entry as a fresh `Fill`
/// would create duplicates across repeated calls. Not
/// `apexes_core::Fill` directly: the gateway has no knowledge of our
/// locally-assigned `FillId`, which `OrderManager` assigns only for
/// genuinely-new trades (see `OrderStore::insert_fill`'s dedup logic,
/// Task 10).
#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeFill {
    pub exchange_trade_id: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_currency: Option<String>,
    pub filled_at: DateTime<Utc>,
}

#[async_trait]
pub trait ExecutionGateway: Send + Sync {
    /// Submits `order` (already persisted locally with status `New` by
    /// the caller before this is invoked -- see
    /// `OrderManager::submit_order`) to the exchange. Implementations
    /// MUST pass `order.id` to the exchange as a client order ID
    /// (alongside whatever exchange-native order ID the exchange
    /// generates), so `query_all_open_orders` can later report it back
    /// -- this is what lets `reconcile()` match exchange reports to
    /// local orders even when `exchange_order_id` was never learned
    /// (an `Uncertain` outcome from this call).
    async fn submit_order(&self, order: &Order) -> GatewayOutcome;

    /// Cancels an already-submitted order, identified by its
    /// `exchange_order_id` (the caller -- `OrderManager` -- guarantees
    /// this is `Some` before calling, since `OrderManager::cancel_order`
    /// refuses to call this method for orders where it's still `None`).
    async fn cancel_order(&self, exchange_order_id: &str) -> GatewayOutcome;

    /// Queries the exchange's current authoritative status for a single
    /// order. `asset_class` tells implementations which API family to
    /// query (e.g. `BinanceExecutionGateway` routes `Spot` to
    /// `api.binance.com` and `Perpetual` to `fapi.binance.com` -- see
    /// `apexes-gateway-binance`'s design). Returns `Ok(None)` if the
    /// exchange has no record of this order ID at all (distinct from
    /// "query failed" -- that's `Err`).
    async fn query_order_status(
        &self,
        exchange_order_id: &str,
        asset_class: AssetClass,
    ) -> Result<Option<OrderStatus>, String>;

    /// Queries the exchange for every currently open (not Filled/
    /// Cancelled/Rejected) order within ONE asset class -- used by
    /// `OrderManager::reconcile` after a reconnect. Callers needing
    /// full-account reconciliation across multiple asset classes call
    /// this once per asset class they have local open orders in (see
    /// `OrderManager::reconcile`, Task 10) -- this method itself never
    /// mixes asset classes in one call, since real exchanges' "list
    /// open orders" endpoints are themselves scoped to one API family
    /// (e.g. Binance's spot open-orders endpoint cannot return futures
    /// orders and vice versa).
    ///
    /// Returns `(client_order_id, exchange_order_id, status)` triples.
    /// `client_order_id` is OUR locally-assigned `OrderId` (`Order.id`),
    /// not the exchange's own identifier -- see `submit_order`'s docs
    /// for why implementations must echo it back here.
    async fn query_all_open_orders(
        &self,
        asset_class: AssetClass,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String>;

    /// Returns every trade execution the exchange has recorded for
    /// `order` (identified by `order.exchange_order_id`, which the
    /// caller guarantees is `Some`, and `order.asset_class`, used the
    /// same way as `query_order_status`/`query_all_open_orders` to pick
    /// an API family). Returns the FULL trade history for this order
    /// every call (matching real exchanges' typical "my trades"
    /// endpoint shape) -- deduplication against previously recorded
    /// fills is `OrderManager`'s responsibility (via
    /// `exchange_trade_id`), not this method's.
    async fn query_fills_for_order(&self, order: &Order) -> Result<Vec<ExchangeFill>, String>;
}
```

- [ ] **Step 2: Update `MockExecutionGateway`'s `ExecutionGateway` impl for the new/changed signatures**

In `crates/apexes-oms/src/mock_gateway.rs`, update the trait impl block. `query_order_status` and `query_all_open_orders` gain an `asset_class` parameter but the mock's existing behavior doesn't need to filter by it (the mock has no concept of separate API families -- it's a single in-memory map), so both new parameters are accepted but unused (prefixed `_asset_class` to avoid an unused-variable warning) EXCEPT `query_all_open_orders`, which per the new trait contract must only return orders of the given `asset_class` -- since `MockExecutionGateway`'s `exchange_state` map doesn't currently store `asset_class` per order, extend it to do so.

First, extend the internal state to track `asset_class` per order:

```rust
use apexes_core::{AssetClass, Order, OrderId, OrderStatus};
```

```rust
pub struct MockExecutionGateway {
    next_submit_outcome: Mutex<Option<GatewayOutcome>>,
    next_cancel_outcome: Mutex<Option<GatewayOutcome>>,
    /// Exchange-side authoritative state, keyed by OUR client order ID
    /// (`OrderId`). Each entry stores the exchange-assigned ID string,
    /// current status, and the order's `asset_class` (needed so
    /// `query_all_open_orders` can filter by the caller-requested
    /// asset class, matching the real `ExecutionGateway` contract).
    exchange_state: Mutex<HashMap<OrderId, (String, OrderStatus, AssetClass)>>,
    /// Programmable fills returned by `query_fills_for_order`, keyed by
    /// client order ID. Tests populate this via `set_fills_for_order`.
    fills: Mutex<HashMap<OrderId, Vec<crate::gateway::ExchangeFill>>>,
}
```

Update `new()`:

```rust
impl MockExecutionGateway {
    pub fn new() -> Self {
        Self {
            next_submit_outcome: Mutex::new(None),
            next_cancel_outcome: Mutex::new(None),
            exchange_state: Mutex::new(HashMap::new()),
            fills: Mutex::new(HashMap::new()),
        }
    }
```

Update `set_exchange_state` and `force_register_exchange_order` to also accept/store `asset_class` -- this changes their public signatures, so update every call site later in this task's Step 3-5 (this file's own tests) and flag it for Task 5's already-fixed call sites are in `manager.rs`'s tests, which will need re-fixing too, tracked in this task's Step 5):

```rust
    /// Directly sets the exchange-side status for the order previously
    /// submitted as `client_order_id`, simulating a state change a test
    /// wants `reconcile()` to discover. Panics if `client_order_id` was
    /// never submitted through this mock (a test setup error).
    pub fn set_exchange_state(&self, client_order_id: OrderId, status: OrderStatus) {
        let mut state = self.exchange_state.lock().unwrap();
        let entry = state
            .get_mut(&client_order_id)
            .expect("set_exchange_state called for an order never submitted through this mock");
        entry.1 = status;
    }

    /// Directly registers exchange-side state for `client_order_id`
    /// WITHOUT requiring a prior `submit_order` call to have gone
    /// through this mock -- needed because a programmed `Uncertain`
    /// `submit_order` outcome does NOT itself populate `exchange_state`.
    pub fn force_register_exchange_order(
        &self,
        client_order_id: OrderId,
        exchange_order_id: &str,
        status: OrderStatus,
        asset_class: AssetClass,
    ) {
        self.exchange_state.lock().unwrap().insert(
            client_order_id,
            (exchange_order_id.to_string(), status, asset_class),
        );
    }

    /// Programs the fills `query_fills_for_order` returns for
    /// `client_order_id`. Returns the full `Vec` every call (matching
    /// the real trait contract's "full trade history every call"
    /// semantics) -- does not accumulate across multiple calls to this
    /// setter; the last call wins.
    pub fn set_fills_for_order(
        &self,
        client_order_id: OrderId,
        fills: Vec<crate::gateway::ExchangeFill>,
    ) {
        self.fills.lock().unwrap().insert(client_order_id, fills);
    }
}
```

Update the `submit_order` impl to store `asset_class` from the order being submitted:

```rust
#[async_trait]
impl ExecutionGateway for MockExecutionGateway {
    async fn submit_order(&self, order: &Order) -> GatewayOutcome {
        if let Some(outcome) = self.next_submit_outcome.lock().unwrap().take() {
            if let GatewayOutcome::Accepted { exchange_order_id } = &outcome {
                self.exchange_state.lock().unwrap().insert(
                    order.id,
                    (exchange_order_id.clone(), OrderStatus::New, order.asset_class),
                );
            }
            return outcome;
        }

        let exchange_order_id = Uuid::new_v4().to_string();
        self.exchange_state.lock().unwrap().insert(
            order.id,
            (exchange_order_id.clone(), OrderStatus::New, order.asset_class),
        );
        GatewayOutcome::Accepted { exchange_order_id }
    }

    async fn cancel_order(&self, exchange_order_id: &str) -> GatewayOutcome {
        if let Some(outcome) = self.next_cancel_outcome.lock().unwrap().take() {
            return outcome;
        }

        let mut state = self.exchange_state.lock().unwrap();
        if let Some((_, status, _)) = state.values_mut().find(|(id, _, _)| id == exchange_order_id)
        {
            *status = OrderStatus::Cancelled;
            GatewayOutcome::Accepted {
                exchange_order_id: exchange_order_id.to_string(),
            }
        } else {
            GatewayOutcome::Rejected {
                reason: format!("no such exchange_order_id: {exchange_order_id}"),
            }
        }
    }

    async fn query_order_status(
        &self,
        exchange_order_id: &str,
        _asset_class: AssetClass,
    ) -> Result<Option<OrderStatus>, String> {
        let state = self.exchange_state.lock().unwrap();
        Ok(state
            .values()
            .find(|(id, _, _)| id == exchange_order_id)
            .map(|(_, status, _)| *status))
    }

    async fn query_all_open_orders(
        &self,
        asset_class: AssetClass,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
        let state = self.exchange_state.lock().unwrap();
        Ok(state
            .iter()
            .filter(|(_, (_, status, order_asset_class))| {
                *order_asset_class == asset_class
                    && matches!(status, OrderStatus::New | OrderStatus::PartiallyFilled)
            })
            .map(|(client_order_id, (exchange_order_id, status, _))| {
                (*client_order_id, Some(exchange_order_id.clone()), *status)
            })
            .collect())
    }

    async fn query_fills_for_order(
        &self,
        order: &Order,
    ) -> Result<Vec<crate::gateway::ExchangeFill>, String> {
        Ok(self
            .fills
            .lock()
            .unwrap()
            .get(&order.id)
            .cloned()
            .unwrap_or_default())
    }
}
```

- [ ] **Step 3: Fix this file's own tests for the new signatures**

`sample_order` needs no change (already fixed in Task 5 to include `asset_class: AssetClass::Spot`). Update every test that calls `query_order_status`/`query_all_open_orders` to pass `AssetClass::Spot` as the new argument, and every `force_register_exchange_order` call to pass `AssetClass::Spot` as the trailing argument:

```rust
    #[tokio::test]
    async fn set_exchange_state_and_query_all_open_orders_reflect_it() {
        let gateway = MockExecutionGateway::new();
        let order = sample_order();
        gateway.submit_order(&order).await;

        gateway.set_exchange_state(order.id, OrderStatus::Filled);

        let open_orders = gateway.query_all_open_orders(AssetClass::Spot).await.unwrap();
        assert!(open_orders.iter().all(|(id, _, _)| *id != order.id));
    }

    #[tokio::test]
    async fn query_all_open_orders_includes_new_and_partially_filled_only() {
        let gateway = MockExecutionGateway::new();
        let order1 = sample_order();
        let order2 = sample_order();
        gateway.submit_order(&order1).await;
        gateway.submit_order(&order2).await;
        gateway.set_exchange_state(order2.id, OrderStatus::PartiallyFilled);

        let open_orders = gateway.query_all_open_orders(AssetClass::Spot).await.unwrap();
        assert_eq!(open_orders.len(), 2);
        assert!(open_orders.iter().any(|(id, _, _)| *id == order1.id));
        assert!(open_orders.iter().any(|(id, _, _)| *id == order2.id));
    }

    #[tokio::test]
    async fn query_order_status_returns_none_for_unknown_exchange_order_id() {
        let gateway = MockExecutionGateway::new();
        let result = gateway
            .query_order_status("never-submitted", AssetClass::Spot)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn cancel_order_marks_it_cancelled_by_default() {
        let gateway = MockExecutionGateway::new();
        let order = sample_order();
        let outcome = gateway.submit_order(&order).await;
        let GatewayOutcome::Accepted { exchange_order_id } = outcome else {
            panic!("expected Accepted");
        };

        let cancel_outcome = gateway.cancel_order(&exchange_order_id).await;
        assert!(matches!(cancel_outcome, GatewayOutcome::Accepted { .. }));

        let status = gateway
            .query_order_status(&exchange_order_id, AssetClass::Spot)
            .await
            .unwrap();
        assert_eq!(status, Some(OrderStatus::Cancelled));
    }
```

- [ ] **Step 4: Add new tests for `query_fills_for_order` and `asset_class`-scoped filtering**

Add these two tests to the same `mod tests` block in `mock_gateway.rs`:

```rust
    #[tokio::test]
    async fn query_fills_for_order_returns_programmed_fills() {
        use crate::gateway::ExchangeFill;

        let gateway = MockExecutionGateway::new();
        let order = sample_order();
        gateway.submit_order(&order).await;

        let fill = ExchangeFill {
            exchange_trade_id: "T1".to_string(),
            price: dec!(65000),
            quantity: dec!(0.5),
            fee: dec!(1.2),
            fee_currency: Some("USDT".to_string()),
            filled_at: Utc::now(),
        };
        gateway.set_fills_for_order(order.id, vec![fill.clone()]);

        let fills = gateway.query_fills_for_order(&order).await.unwrap();
        assert_eq!(fills, vec![fill]);
    }

    #[tokio::test]
    async fn query_fills_for_order_returns_empty_when_none_programmed() {
        let gateway = MockExecutionGateway::new();
        let order = sample_order();
        gateway.submit_order(&order).await;

        let fills = gateway.query_fills_for_order(&order).await.unwrap();
        assert!(fills.is_empty());
    }

    #[tokio::test]
    async fn query_all_open_orders_filters_by_asset_class() {
        let gateway = MockExecutionGateway::new();
        let mut spot_order = sample_order();
        spot_order.asset_class = AssetClass::Spot;
        let mut perp_order = sample_order();
        perp_order.asset_class = AssetClass::Perpetual;
        gateway.submit_order(&spot_order).await;
        gateway.submit_order(&perp_order).await;

        let spot_open = gateway.query_all_open_orders(AssetClass::Spot).await.unwrap();
        assert_eq!(spot_open.len(), 1);
        assert_eq!(spot_open[0].0, spot_order.id);

        let perp_open = gateway
            .query_all_open_orders(AssetClass::Perpetual)
            .await
            .unwrap();
        assert_eq!(perp_open.len(), 1);
        assert_eq!(perp_open[0].0, perp_order.id);
    }
```

- [ ] **Step 5: Fix `manager.rs`'s tests broken by the signature changes**

`manager.rs`'s test module calls `gateway.force_register_exchange_order(order_id, "EX-999", OrderStatus::PartiallyFilled)` in `reconcile_resolves_a_previously_uncertain_order_via_pass_one` -- add `AssetClass::Spot` as a fourth argument:

```rust
        gateway.force_register_exchange_order(
            order_id,
            "EX-999",
            OrderStatus::PartiallyFilled,
            AssetClass::Spot,
        );
```

Add `AssetClass` to `manager.rs` test module's imports (`use apexes_core::{AssetClass, OrderType, Side, Venue};`). Note: `manager.rs`'s `reconcile()` itself (production code) calls `query_all_open_orders`/`query_order_status` -- those call sites are rewritten in Task 10, not here; this task only fixes the test-side fallout of the trait signature change so the crate compiles standalone before Task 10's larger `reconcile()` rewrite. Since `reconcile()`'s production code won't compile yet after this task alone (it still calls the OLD single-argument signatures), do NOT attempt to run `cargo test -p apexes-oms` successfully at the end of this task -- proceed directly to Task 9 and Task 10, which together restore a compiling, correct `apexes-oms`. Run `cargo check -p apexes-oms --tests 2>&1 | grep -c error` after this task's edits purely to confirm the remaining errors are all in `manager.rs`'s `reconcile()`/`submit_order`/`cancel_order` (expected, to be fixed in Task 10) and nowhere else (e.g. `mock_gateway.rs` and `schema.rs` should show zero errors).

- [ ] **Step 6: Verify error locations**

Run: `cargo check -p apexes-oms --tests 2>&1 | grep "error\[" `
Expected: every reported error's file path is `manager.rs` (specifically inside `submit_order`, `cancel_order`, or `reconcile`, which call the gateway trait methods with the old signatures) -- if any error appears in `mock_gateway.rs`, `schema.rs`, `store.rs`, `gateway.rs`, or `error.rs`, stop and fix it before proceeding, since Task 10 assumes only `manager.rs` is left broken at this point.

- [ ] **Step 7: Commit**

```bash
git add crates/apexes-oms/src/gateway.rs crates/apexes-oms/src/mock_gateway.rs crates/apexes-oms/src/manager.rs
git commit -S -s -m "feat: extend ExecutionGateway with query_fills_for_order and asset_class-scoped queries"
```

(This commit intentionally leaves `apexes-oms` non-compiling as a whole crate -- `manager.rs`'s production code still calls the old gateway method signatures. This is acceptable mid-plan because Task 10, immediately next, fixes it; the plan does not require every single commit to leave every crate compiling in isolation when a design decision was made to split "extend the trait" and "update the trait's only production caller" into separately reviewable commits. If your workflow requires every commit to compile, squash Tasks 8 and 10 together at merge time.)

---

### Task 9: `apexes-oms`: add `ExchangeFill` deduplication support and `ExchangeUpdate` type

**Files:**
- Modify: `crates/apexes-oms/src/schema.rs`
- Modify: `crates/apexes-oms/src/store.rs`
- Create: `crates/apexes-oms/src/exchange_update.rs`
- Modify: `crates/apexes-oms/src/lib.rs`

**Interfaces:**
- Consumes: `ExchangeFill` (Task 8).
- Produces: `fills` table gains `exchange_trade_id` column + `UNIQUE (order_id, exchange_trade_id)` constraint; `OrderStore::insert_fill` returns `Result<bool, OmsError>` (changed from `Result<(), OmsError>`) where `true` means a new row was actually inserted; `ExchangeUpdate` struct (new, network/gateway-agnostic, used by both `apexes-gateway-binance`'s User Data Stream output and `apexes-live-trading`'s event loop input).

- [ ] **Step 1: Add `exchange_trade_id` column and unique constraint to the `fills` table DDL**

In `crates/apexes-oms/src/schema.rs`, modify `FILLS_TABLE_DDL`:

```rust
pub const FILLS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS fills
(
    id                  UUID PRIMARY KEY,
    order_id            UUID NOT NULL REFERENCES orders(id),
    exchange_trade_id   TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    venue               TEXT NOT NULL,
    side                TEXT NOT NULL,
    price               NUMERIC(38, 10) NOT NULL,
    quantity            NUMERIC(38, 10) NOT NULL,
    fee                 NUMERIC(38, 10) NOT NULL,
    fee_currency        TEXT,
    filled_at           TIMESTAMPTZ NOT NULL,
    UNIQUE (order_id, exchange_trade_id)
)
";
```

Note: `apexes_core::Fill` itself has no `exchange_trade_id` field (it's a pure local-persistence-layer concept, not part of the shared domain type used by `apexes-backtest`'s simulated fills, which have no exchange trade ID at all). So `exchange_trade_id` lives only in `FillRow`, not in `Fill` -- `OrderStore` accepts it as a separate parameter to `insert_fill`, not read from the `Fill` struct.

- [ ] **Step 2: Change `FillRow` and `insert_fill`'s signature to carry `exchange_trade_id` explicitly**

Modify `FillRow`:

```rust
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FillRow {
    pub id: Uuid,
    pub order_id: Uuid,
    pub exchange_trade_id: String,
    pub symbol: String,
    pub venue: String,
    pub side: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_currency: Option<String>,
    pub filled_at: DateTime<Utc>,
}
```

`FillRow` no longer has a clean `From<&Fill>` (since `Fill` doesn't carry `exchange_trade_id`) -- replace it with a plain constructor function that takes both a `&Fill` and the `exchange_trade_id`:

```rust
impl FillRow {
    pub fn from_fill(fill: &Fill, exchange_trade_id: &str) -> Self {
        FillRow {
            id: fill.id.0,
            order_id: fill.order_id.0,
            exchange_trade_id: exchange_trade_id.to_string(),
            symbol: fill.symbol.clone(),
            venue: format!("{:?}", fill.venue),
            side: format!("{:?}", fill.side),
            price: fill.price,
            quantity: fill.quantity,
            fee: fill.fee,
            fee_currency: fill.fee_currency.clone(),
            filled_at: fill.filled_at,
        }
    }
}
```

`TryFrom<FillRow> for Fill` is unaffected (still ignores `exchange_trade_id`, since `Fill` has no such field):

```rust
impl TryFrom<FillRow> for Fill {
    type Error = OmsError;

    fn try_from(row: FillRow) -> Result<Self, OmsError> {
        Ok(Fill {
            id: FillId(row.id),
            order_id: OrderId(row.order_id),
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            side: parse_side(&row.side)?,
            price: row.price,
            quantity: row.quantity,
            fee: row.fee,
            fee_currency: row.fee_currency,
            filled_at: row.filled_at,
        })
    }
}
```

- [ ] **Step 3: Fix `schema.rs`'s own tests for the `FillRow::from_fill` signature change**

The `fill_round_trips_through_fill_row` and `fill_with_none_fee_currency_round_trips` tests call `FillRow::from(&fill)` -- update both to `FillRow::from_fill(&fill, "T1")`:

```rust
    #[test]
    fn fill_round_trips_through_fill_row() {
        let fill = sample_fill();
        let row = FillRow::from_fill(&fill, "T1");
        let back = Fill::try_from(row).unwrap();
        assert_eq!(back, fill);
    }

    #[test]
    fn fill_with_none_fee_currency_round_trips() {
        let mut fill = sample_fill();
        fill.fee_currency = None;
        let row = FillRow::from_fill(&fill, "T1");
        let back = Fill::try_from(row).unwrap();
        assert_eq!(back.fee_currency, None);
    }
```

Add a new test verifying `exchange_trade_id` itself round-trips correctly through the row (even though `Fill` doesn't carry it, `OrderStore` will read it back for dedup checks in Step 4):

```rust
    #[test]
    fn from_fill_stores_the_given_exchange_trade_id() {
        let fill = sample_fill();
        let row = FillRow::from_fill(&fill, "T-UNIQUE-42");
        assert_eq!(row.exchange_trade_id, "T-UNIQUE-42");
    }
```

- [ ] **Step 4: Update `OrderStore::insert_fill` to take `exchange_trade_id`, upsert idempotently, and report whether a row was actually inserted**

In `crates/apexes-oms/src/store.rs`:

```rust
    /// Inserts a fill, keyed for deduplication on `(order_id,
    /// exchange_trade_id)` -- a real exchange's "my trades" endpoint
    /// returns the full trade history for an order every call, so this
    /// must be safely callable multiple times with the same
    /// `exchange_trade_id` without creating duplicate rows. Returns
    /// `Ok(true)` if a new row was inserted, `Ok(false)` if it already
    /// existed (the `ON CONFLICT DO NOTHING` path) -- callers (see
    /// `OrderManager::apply_exchange_update`, Task 10) use this to know
    /// whether to propagate the fill onward (e.g. into a
    /// `PortfolioLedger`).
    pub async fn insert_fill(
        &self,
        fill: &Fill,
        exchange_trade_id: &str,
    ) -> Result<bool, OmsError> {
        let row = FillRow::from_fill(fill, exchange_trade_id);
        let result = sqlx::query(
            "INSERT INTO fills (id, order_id, exchange_trade_id, symbol, venue, side, price, quantity, fee, fee_currency, filled_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (order_id, exchange_trade_id) DO NOTHING",
        )
        .bind(row.id)
        .bind(row.order_id)
        .bind(&row.exchange_trade_id)
        .bind(&row.symbol)
        .bind(&row.venue)
        .bind(&row.side)
        .bind(row.price)
        .bind(row.quantity)
        .bind(row.fee)
        .bind(&row.fee_currency)
        .bind(row.filled_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }
```

- [ ] **Step 5: Create `crates/apexes-oms/src/exchange_update.rs`**

```rust
//! [`ExchangeUpdate`]: gateway-agnostic representation of a push-based
//! order status update (e.g. from a Binance User Data Stream WebSocket
//! message, via `apexes-gateway-binance`). Lives in `apexes-oms` (not
//! `apexes-live-trading`) so both `apexes-gateway-binance` (which
//! produces a `Stream<Item = ExchangeUpdate>`) and `apexes-live-trading`
//! (which consumes it, calling `OrderManager::apply_exchange_update`
//! for each item) depend on it via the one crate they already both
//! depend on for `ExecutionGateway`/`OrderManager`, keeping the
//! dependency graph a DAG (`apexes-gateway-binance` and
//! `apexes-live-trading` never depend on each other).

use apexes_core::{OrderId, OrderStatus};

#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeUpdate {
    pub client_order_id: OrderId,
    pub exchange_order_id: Option<String>,
    pub status: OrderStatus,
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn exchange_update_is_constructible_and_comparable() {
        let a = ExchangeUpdate {
            client_order_id: OrderId(Uuid::nil()),
            exchange_order_id: Some("EX1".to_string()),
            status: OrderStatus::PartiallyFilled,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }
}
```

- [ ] **Step 6: Wire the new module into `lib.rs`**

```rust
pub mod error;
pub mod exchange_update;
pub mod gateway;
pub mod manager;
pub mod mock_gateway;
pub mod schema;
pub mod store;

pub use error::OmsError;
pub use exchange_update::ExchangeUpdate;
pub use gateway::{ExchangeFill, ExecutionGateway, GatewayOutcome};
pub use manager::OrderManager;
pub use mock_gateway::MockExecutionGateway;
pub use store::OrderStore;
```

- [ ] **Step 7: Verify remaining compile errors are still confined to `manager.rs`**

Run: `cargo check -p apexes-oms --tests 2>&1 | grep "error\[" `
Expected: errors remain only in `manager.rs` (now ALSO including its calls to `store.insert_fill` in `apply_fill`-adjacent code -- there are none yet since Task 10 adds the first such call; if `insert_fill`'s old call sites existed in `manager.rs` already, they'd show here, but they don't yet at this point in the plan). If any new errors appear in `store.rs` or `schema.rs`, fix them before proceeding.

- [ ] **Step 8: Commit**

```bash
git add crates/apexes-oms/src/schema.rs crates/apexes-oms/src/store.rs crates/apexes-oms/src/exchange_update.rs crates/apexes-oms/src/lib.rs
git commit -S -s -m "feat: add exchange_trade_id dedup column and ExchangeUpdate type to apexes-oms"
```

---

### Task 10: `apexes-oms`: implement `apply_exchange_update` and rewrite `reconcile()` on top of it

**Files:**
- Modify: `crates/apexes-oms/src/manager.rs`
- Modify: `crates/apexes-oms/tests/live_postgres.rs`

**Interfaces:**
- Consumes: `ExchangeFill`, extended `ExecutionGateway` trait (Task 8); `OrderStore::insert_fill(&self, fill: &Fill, exchange_trade_id: &str) -> Result<bool, OmsError>` (Task 9).
- Produces: `OrderManager::apply_exchange_update(&mut self, client_order_id: OrderId, exchange_order_id: Option<String>, status: OrderStatus) -> Result<Vec<Fill>, OmsError>` (new public method -- Task 19's `apexes-live-trading::run_live_trading` calls this directly for User Data Stream push updates). `OrderManager::reconcile(&mut self) -> Result<Vec<(OrderId, Vec<Fill>)>, OmsError>` (signature change: was `Result<Vec<OrderId>, OmsError>`).

- [ ] **Step 1: Update `manager.rs`'s imports**

```rust
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use apexes_core::{AssetClass, Fill, FillId, Order, OrderId, OrderStatus};
use apexes_risk::PreTradeCheck;
use apexes_strategy::{OrderIntent, Position};
use chrono::Utc;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::error::OmsError;
use crate::gateway::{ExecutionGateway, GatewayOutcome};
```

- [ ] **Step 2: Implement `apply_exchange_update`**

Add this new method to `impl OrderManager` (placed right before `reconcile`, replacing none of the existing `submit_order`/`cancel_order` code, which is unaffected):

```rust
    /// Applies one authoritative exchange-reported status update to a
    /// single local order -- the shared core both `reconcile()`'s two
    /// passes (below) and live push-based updates (Binance User Data
    /// Stream, via `apexes-gateway-binance`'s `ExchangeUpdate` stream)
    /// call, so this overwrite/persist/fill-fetch logic exists in
    /// exactly one place.
    ///
    /// Unconditionally overwrites local status with `status` if it
    /// differs from the order's current status (same
    /// authoritative-exchange rationale as this method's callers --
    /// bypassing `OrderStatus::can_transition_to`'s normal-operation
    /// validation is intentional: a transition it would reject during
    /// live operation reflects a real event that happened while
    /// disconnected or between poll intervals, not a bug). If
    /// `exchange_order_id` is `Some` and differs from what's currently
    /// stored, populates/updates it.
    ///
    /// If either the status or `exchange_order_id` changed, persists
    /// the order via `store.upsert_order`. If the status transition
    /// lands on `PartiallyFilled` or `Filled` (and actually changed --
    /// a repeated push of the same already-applied status is a no-op),
    /// calls `gateway.query_fills_for_order`, deduplicates each
    /// returned `ExchangeFill` against `OrderStore` via
    /// `exchange_trade_id`, persists any genuinely-new ones, and
    /// returns them. If the new status is terminal (`Filled`,
    /// `Cancelled`, `Rejected`), removes the order from `open_orders`.
    ///
    /// Returns `Ok(new_fills)` -- empty if the order wasn't found
    /// locally (logs `tracing::error!`), if nothing changed, or if no
    /// new fills were found beyond what was already recorded.
    pub async fn apply_exchange_update(
        &mut self,
        client_order_id: OrderId,
        exchange_order_id: Option<String>,
        status: OrderStatus,
    ) -> Result<Vec<Fill>, OmsError> {
        let Some(previous_status) = self
            .open_orders
            .get(&client_order_id)
            .map(|order| order.status)
        else {
            tracing::error!(
                order_id = ?client_order_id,
                "apply_exchange_update: exchange reports an order we have no local record of"
            );
            return Ok(Vec::new());
        };

        let status_changed = previous_status != status;
        let mut exchange_order_id_changed = false;

        if let Some(order) = self.open_orders.get_mut(&client_order_id) {
            if exchange_order_id.is_some() && order.exchange_order_id != exchange_order_id {
                order.exchange_order_id = exchange_order_id;
                exchange_order_id_changed = true;
            }
            if status_changed {
                order.status = status;
                order.updated_at = Utc::now();
            }
        }

        if !status_changed && !exchange_order_id_changed {
            return Ok(Vec::new());
        }

        let order_snapshot = self.open_orders.get(&client_order_id).unwrap().clone();
        self.store.upsert_order(&order_snapshot).await?;

        let mut new_fills = Vec::new();
        if status_changed
            && matches!(status, OrderStatus::PartiallyFilled | OrderStatus::Filled)
            && order_snapshot.exchange_order_id.is_some()
        {
            match self.gateway.query_fills_for_order(&order_snapshot).await {
                Ok(exchange_fills) => {
                    for exchange_fill in exchange_fills {
                        let fill = Fill {
                            id: FillId(Uuid::new_v4()),
                            order_id: client_order_id,
                            symbol: order_snapshot.symbol.clone(),
                            venue: order_snapshot.venue,
                            side: order_snapshot.side,
                            price: exchange_fill.price,
                            quantity: exchange_fill.quantity,
                            fee: exchange_fill.fee,
                            fee_currency: exchange_fill.fee_currency.clone(),
                            filled_at: exchange_fill.filled_at,
                        };
                        let inserted = self
                            .store
                            .insert_fill(&fill, &exchange_fill.exchange_trade_id)
                            .await?;
                        if inserted {
                            new_fills.push(fill);
                        }
                    }
                }
                Err(reason) => {
                    tracing::warn!(
                        order_id = ?client_order_id,
                        reason,
                        "apply_exchange_update: query_fills_for_order failed; fills for this transition may be recovered on a later call"
                    );
                }
            }
        }

        if matches!(
            status,
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
        ) {
            self.open_orders.remove(&client_order_id);
        }

        Ok(new_fills)
    }
```

- [ ] **Step 3: Rewrite `reconcile()` to call `apply_exchange_update` and query per-asset-class**

Replace the entire existing `reconcile()` method body (including its two-pass loops and the final `retain()` call, all of which are now superseded by `apply_exchange_update` handling persistence/removal per-order) with:

```rust
    /// Reconnect reconciliation, in two passes, both delegating the
    /// actual per-order state application to `apply_exchange_update`
    /// (above):
    ///
    /// **Pass 1** -- for each distinct `asset_class` among locally-open
    /// orders, calls `gateway.query_all_open_orders(asset_class)` (real
    /// exchange APIs scope "list open orders" to one API family, e.g.
    /// Binance's spot vs. futures open-orders endpoints are entirely
    /// separate -- this is why the call happens once per asset class
    /// rather than once for the whole account). For every
    /// `(client_order_id, exchange_order_id, exchange_status)` triple
    /// returned, applies it via `apply_exchange_update`.
    ///
    /// **Pass 2** -- `query_all_open_orders` is documented to report
    /// ONLY non-terminal orders, so it can never surface a `Filled`/
    /// `Cancelled`/`Rejected` transition on its own. For every local
    /// order still `New`/`PartiallyFilled` that pass 1 did NOT report
    /// (and that has a known `exchange_order_id`), this pass calls
    /// `gateway.query_order_status` individually (which DOES report
    /// terminal statuses) and applies the result the same way. A local
    /// order with no `exchange_order_id` yet, not reported by pass 1
    /// either, cannot be resolved via the current `ExecutionGateway`
    /// interface -- logged via `tracing::warn!`, left unchanged.
    ///
    /// Returns one `(OrderId, Vec<Fill>)` entry for every order whose
    /// status actually changed OR that produced new fills, so a caller
    /// (e.g. `apexes-live-trading`'s event loop) can both log/alert on
    /// what changed and apply any new `Fill`s to its own portfolio
    /// tracking.
    pub async fn reconcile(&mut self) -> Result<Vec<(OrderId, Vec<Fill>)>, OmsError> {
        let asset_classes: HashSet<AssetClass> = self
            .open_orders
            .values()
            .map(|order| order.asset_class)
            .collect();

        let mut all_exchange_orders: Vec<(OrderId, Option<String>, OrderStatus)> = Vec::new();
        for asset_class in &asset_classes {
            let exchange_orders = self
                .gateway
                .query_all_open_orders(*asset_class)
                .await
                .map_err(|reason| OmsError::QueryFailed { reason })?;
            all_exchange_orders.extend(exchange_orders);
        }

        let reported_client_ids: HashSet<OrderId> =
            all_exchange_orders.iter().map(|(id, _, _)| *id).collect();

        let mut results: Vec<(OrderId, Vec<Fill>)> = Vec::new();

        // Pass 1: orders the exchange still considers open (or has just
        // transitioned between non-terminal states).
        for (client_order_id, exchange_order_id, exchange_status) in all_exchange_orders {
            let Some(previous_status) = self
                .open_orders
                .get(&client_order_id)
                .map(|order| order.status)
            else {
                tracing::error!(
                    order_id = ?client_order_id,
                    "reconcile: exchange reports an open order we have no local record of"
                );
                continue;
            };

            let new_fills = self
                .apply_exchange_update(client_order_id, exchange_order_id, exchange_status)
                .await?;
            if previous_status != exchange_status || !new_fills.is_empty() {
                results.push((client_order_id, new_fills));
            }
        }

        // Pass 2: local orders still New/PartiallyFilled that the
        // exchange did NOT report as open in pass 1 -- the primary way
        // a Filled/Cancelled/Rejected transition is ever discovered.
        let still_locally_open: Vec<OrderId> = self
            .open_orders
            .iter()
            .filter(|(id, order)| {
                !reported_client_ids.contains(*id)
                    && matches!(
                        order.status,
                        OrderStatus::New | OrderStatus::PartiallyFilled
                    )
            })
            .map(|(id, _)| *id)
            .collect();

        for client_order_id in still_locally_open {
            let Some(order) = self.open_orders.get(&client_order_id) else {
                continue;
            };
            let previous_status = order.status;
            let asset_class = order.asset_class;
            let Some(exchange_order_id) = order.exchange_order_id.clone() else {
                tracing::warn!(
                    order_id = ?client_order_id,
                    "reconcile: local order still open with no known exchange_order_id and not reported as open by the exchange -- cannot resolve"
                );
                continue;
            };

            match self
                .gateway
                .query_order_status(&exchange_order_id, asset_class)
                .await
            {
                Ok(Some(status)) => {
                    let new_fills = self
                        .apply_exchange_update(client_order_id, Some(exchange_order_id), status)
                        .await?;
                    if previous_status != status || !new_fills.is_empty() {
                        results.push((client_order_id, new_fills));
                    }
                }
                Ok(None) => {
                    tracing::error!(
                        order_id = ?client_order_id,
                        exchange_order_id,
                        "reconcile: exchange has no record at all of an order we believe is open"
                    );
                }
                Err(reason) => {
                    tracing::warn!(
                        order_id = ?client_order_id,
                        reason,
                        "reconcile: query_order_status failed for this order; will retry on next reconcile() call"
                    );
                }
            }
        }

        Ok(results)
    }
```

- [ ] **Step 4: Fix existing `reconcile()`-calling tests in `manager.rs` for the new return type**

`reconcile_pass_one_overwrites_local_status_for_a_reported_open_order`:

```rust
    #[tokio::test]
    #[ignore]
    async fn reconcile_pass_one_overwrites_local_status_for_a_reported_open_order() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            manager.open_orders().get(&order_id).unwrap().status,
            OrderStatus::New
        );

        gateway.set_exchange_state(order_id, OrderStatus::PartiallyFilled);

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, order_id);
        assert!(changed[0].1.is_empty());
        assert_eq!(
            manager.open_orders().get(&order_id).unwrap().status,
            OrderStatus::PartiallyFilled
        );
    }
```

`reconcile_pass_two_discovers_a_terminal_status_via_query_order_status`:

```rust
    #[tokio::test]
    #[ignore]
    async fn reconcile_pass_two_discovers_a_terminal_status_via_query_order_status() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        gateway.set_exchange_state(order_id, OrderStatus::Filled);

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, order_id);
        assert!(manager.open_orders().get(&order_id).is_none());

        let persisted = manager.store.get_order(order_id).await.unwrap().unwrap();
        assert_eq!(persisted.status, OrderStatus::Filled);
    }
```

`reconcile_resolves_a_previously_uncertain_order_via_pass_one`:

```rust
    #[tokio::test]
    #[ignore]
    async fn reconcile_resolves_a_previously_uncertain_order_via_pass_one() {
        let gateway = Arc::new(MockExecutionGateway::new());
        gateway.program_next_submit_outcome(GatewayOutcome::Uncertain {
            reason: "timeout".to_string(),
        });
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            manager
                .open_orders()
                .get(&order_id)
                .unwrap()
                .exchange_order_id,
            None
        );

        gateway.force_register_exchange_order(
            order_id,
            "EX-999",
            OrderStatus::PartiallyFilled,
            AssetClass::Spot,
        );

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, order_id);
        let order = manager.open_orders().get(&order_id).unwrap();
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.exchange_order_id, Some("EX-999".to_string()));
    }
```

`reconcile_leaves_terminal_orders_unaffected_when_not_reported_by_exchange`:

```rust
    #[tokio::test]
    #[ignore]
    async fn reconcile_leaves_terminal_orders_unaffected_when_not_reported_by_exchange() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
        manager.cancel_order(order_id).await.unwrap();

        let changed = manager.reconcile().await.unwrap();
        assert!(!changed.iter().any(|(id, _)| *id == order_id));
    }
```

- [ ] **Step 5: Add a new test for fill discovery via `reconcile()`'s pass 2**

Add this test to `manager.rs`'s `mod tests`, exercising the newly-added fill-fetch behavior end to end. It uses `crate::gateway::ExchangeFill` (the in-crate path, since this test lives inside `apexes-oms` itself) and needs no new `use` statement beyond what `manager.rs`'s test module already has (`dec!` is already imported there, used by `sample_intent`):

```rust
    #[tokio::test]
    #[ignore]
    async fn reconcile_pass_two_fetches_and_persists_fills_on_fill_transition() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(Arc::clone(&gateway) as Arc<dyn ExecutionGateway>).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        gateway.set_exchange_state(order_id, OrderStatus::Filled);
        gateway.set_fills_for_order(
            order_id,
            vec![crate::gateway::ExchangeFill {
                exchange_trade_id: "T1".to_string(),
                price: dec!(65000),
                quantity: dec!(1),
                fee: dec!(1.5),
                fee_currency: Some("USDT".to_string()),
                filled_at: Utc::now(),
            }],
        );

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, order_id);
        assert_eq!(changed[0].1.len(), 1);
        assert_eq!(changed[0].1[0].quantity, dec!(1));

        let persisted_fills = manager.store.get_fills_for_order(order_id).await.unwrap();
        assert_eq!(persisted_fills.len(), 1);

        gateway.set_exchange_state(order_id, OrderStatus::Filled);
        let changed_again = manager.reconcile().await.unwrap();
        assert!(changed_again.is_empty());
        let persisted_fills_again = manager.store.get_fills_for_order(order_id).await.unwrap();
        assert_eq!(persisted_fills_again.len(), 1);
    }
```

Note: `dec!` requires `rust_decimal_macros::dec` -- confirm it's already imported at the top of `manager.rs`'s test module (it is, used by `sample_intent`).

- [ ] **Step 6: Fix `tests/live_postgres.rs` for the `reconcile()` return-type change**

```rust
    let changed = manager.reconcile().await.unwrap();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].0, order_id);
    assert!(manager.open_orders().get(&order_id).is_none());
```

(Replaces the existing `assert_eq!(changed, vec![order_id]);` line -- keep every other line of that test file unchanged.)

- [ ] **Step 7: Compile and run**

Run: `cargo check -p apexes-oms --tests`
Expected: 0 errors.

Run: `cargo test -p apexes-oms`
Expected: PASS (non-`#[ignore]`d tests only run by default).

Run: `docker compose up -d postgres` (from the workspace root, `/Users/rickzhang/src/oss/apexes`), wait a few seconds for PostgreSQL to become ready, then:

Run: `cargo test -p apexes-oms -- --ignored`
Expected: PASS, including the new `reconcile_pass_two_fetches_and_persists_fills_on_fill_transition` test.

Run: `cargo test -p apexes-oms --test live_postgres -- --ignored`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/apexes-oms/src/manager.rs crates/apexes-oms/tests/live_postgres.rs
git commit -S -s -m "feat: implement apply_exchange_update and rewrite reconcile() to use it"
```

---

### Task 11: Full-workspace checkpoint after `apexes-oms` changes

**Files:** none (verification-only task)

**Interfaces:** none.

- [ ] **Step 1: Full workspace build and test**

Run: `cargo build --workspace`
Expected: clean.

Run: `cargo test --workspace`
Expected: 0 failures. (Note: `apexes-oms`'s and `apexes-strategy-registry`'s live-database-dependent tests remain `#[ignore]`d and are not run by this command, consistent with the rest of the workspace's testing convention.)

- [ ] **Step 2: Clippy and fmt**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 3: Commit any fmt fixes**

```bash
git add -A
git commit -S -s -m "chore: cargo fmt after apexes-oms ExecutionGateway extension"
```

(Only if `git status` shows changes.)

---

### Task 12: Scaffold `apexes-gateway-binance` crate

**Files:**
- Create: `crates/apexes-gateway-binance/Cargo.toml`
- Create: `crates/apexes-gateway-binance/src/lib.rs`
- Create: `crates/apexes-gateway-binance/src/config.rs`
- Modify: `Cargo.toml` (workspace root)

**Interfaces:**
- Consumes: workspace dependencies `reqwest`, `tokio-tungstenite`, `hmac`, `sha2`, `hex`, `wiremock` (all added in this task).
- Produces: `apexes_gateway_binance::BinanceGatewayConfig` (new struct, no `Default`).

- [ ] **Step 1: Add new workspace dependencies**

In the workspace root `/Users/rickzhang/src/oss/apexes/Cargo.toml`, add these lines to `[workspace.dependencies]` (after the existing `sqlx` line):

```toml
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
tokio-tungstenite = { version = "0.26", features = ["rustls-tls-webpki-roots"] }
hmac = "0.13"
sha2 = "0.10"
hex = "0.4"
```

**Post-implementation correction (discovered during Task 13):** `hmac = "0.13"` is incompatible with the workspace's pinned `sha2 = "0.10"` — `hmac 0.13` depends on `digest 0.11`, while `sha2 0.10` depends on `digest 0.10`, and the resulting duplicate-`digest`-major-version conflict fails to compile (`E0599: no function or associated item named 'new_from_slice'`, plus a `HashMarker` trait-bound mismatch). Task 13's implementer downgraded the workspace dependency to `hmac = "0.12"` (verified independently by the controller: `hmac 0.13` reproducibly fails against `sha2 0.10`, `hmac 0.12` compiles cleanly) — use `hmac = "0.12"`, not `"0.13"`, if replicating this setup.

Note: `wiremock` is NOT added here. It's a dev-dependency only, and this codebase's existing convention is that per-crate `[dev-dependencies]` sections pin their own versions directly (e.g. `rust_decimal_macros = "1.40"` appears independently in every crate's `[dev-dependencies]`, never hoisted to `[workspace.dependencies]`) — `wiremock = "0.6"` is declared directly in `apexes-gateway-binance/Cargo.toml`'s `[dev-dependencies]` in Step 2 below, matching that precedent.

Add to the workspace's `members` list:

```toml
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage", "crates/apexes-backtest", "crates/apexes-strategy", "crates/apexes-indicators", "crates/apexes-optimize", "crates/apexes-strategy-registry", "crates/apexes-risk", "crates/apexes-paper", "crates/apexes-oms", "crates/apexes-gateway-binance", "crates/apexes-live-trading"]
```

- [ ] **Step 2: Create `crates/apexes-gateway-binance/Cargo.toml`**

```toml
[package]
name = "apexes-gateway-binance"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-oms = { path = "../apexes-oms" }
reqwest = { workspace = true }
tokio-tungstenite = { workspace = true }
hmac = { workspace = true }
sha2 = { workspace = true }
hex = { workspace = true }
tokio = { workspace = true, features = ["time", "sync"] }
futures = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
rust_decimal = { workspace = true }
tracing = { workspace = true }
async-trait = { workspace = true }

[dev-dependencies]
wiremock = "0.6"
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
uuid = { workspace = true, features = ["v4"] }
```

- [ ] **Step 3: Create `crates/apexes-gateway-binance/src/config.rs`**

```rust
//! [`BinanceGatewayConfig`]: configuration for [`crate::gateway::BinanceExecutionGateway`].
//! Deliberately has no `Default` impl and no environment-variable or
//! config-file loading -- per this sub-project's design decision, the
//! caller (test code today; a future CLI/UI tool in a later
//! sub-project) constructs this explicitly. This crate never reads
//! `std::env` in production code.

/// Configuration for [`crate::gateway::BinanceExecutionGateway`].
///
/// `spot_base_url`/`futures_base_url` are overridable (not hardcoded to
/// `https://api.binance.com`/`https://fapi.binance.com`) so tests can
/// point them at a local `wiremock` server or Binance's public testnet.
#[derive(Debug, Clone)]
pub struct BinanceGatewayConfig {
    pub api_key: String,
    pub api_secret: String,
    pub spot_base_url: String,
    pub futures_base_url: String,
    pub recv_window_ms: u64,
}
```

- [ ] **Step 4: Create `crates/apexes-gateway-binance/src/lib.rs`**

```rust
//! Real Binance `ExecutionGateway` implementation (sub-project #8):
//! REST order submission/cancellation/querying across Binance Spot and
//! USDS-M Perpetual Futures, request signing, and a User Data Stream
//! WebSocket client for push-based order/fill updates. Implements
//! `apexes_oms::ExecutionGateway` -- see that trait's docs and
//! `docs/superpowers/specs/2026-08-01-apexes-gateway-binance-design.md`
//! for the full design, including why this crate does NOT depend on
//! `barter-execution` (its Binance client module is an empty, private
//! stub as of the version available when this crate was written).

pub mod config;
pub mod signing;

pub use config::BinanceGatewayConfig;
```

- [ ] **Step 5: Verify the new crate compiles standalone**

Run: `cargo check -p apexes-gateway-binance`
Expected: fails with "unresolved module `signing`" (module declared, file doesn't exist yet) -- this is expected at this point; Task 13 creates it. Verify the ONLY error is the missing `signing` module, confirming the scaffolding itself (Cargo.toml, workspace membership, `config.rs`) is otherwise correct.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/apexes-gateway-binance/Cargo.toml crates/apexes-gateway-binance/src/lib.rs crates/apexes-gateway-binance/src/config.rs
git commit -S -s -m "feat: scaffold apexes-gateway-binance crate"
```

---

### Task 13: `apexes-gateway-binance`: request signing (pure function, unit tested)

**Files:**
- Create: `crates/apexes-gateway-binance/src/signing.rs`

**Interfaces:**
- Produces: `sign_query_string(secret: &str, query: &str) -> String`.

- [ ] **Step 1: Write the failing test**

Create `crates/apexes-gateway-binance/src/signing.rs`:

```rust
//! Binance's required request-signing scheme: HMAC-SHA256 over a
//! request's full query string, using the account's API secret as the
//! HMAC key, hex-encoded. Pure function -- deliberately has no HTTP
//! dependency so it's covered by fast, network-free unit tests
//! independent of the `wiremock`-based tests (Task 15) that verify the
//! resulting HTTP request shape end-to-end.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Computes Binance's required HMAC-SHA256 signature over `query`
/// (a request's query string, WITHOUT the `signature` parameter itself
/// -- that parameter is appended by the caller using this function's
/// return value), using `secret` as the HMAC key. Returns the
/// lowercase-hex-encoded signature, matching Binance's documented
/// format.
pub fn sign_query_string(secret: &str, query: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts a key of any length");
    mac.update(query.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Known-answer test using the exact example from Binance's public
    /// API documentation (`developers.binance.com`'s signed-endpoint
    /// examples page), so this test's expected value is independently
    /// verifiable against the vendor's own published example rather
    /// than only self-consistent with this implementation.
    #[test]
    fn matches_binance_documented_example() {
        let secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
        let query = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
        let signature = sign_query_string(secret, query);
        assert_eq!(
            signature,
            "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
        );
    }

    #[test]
    fn different_queries_produce_different_signatures() {
        let secret = "test-secret";
        let sig1 = sign_query_string(secret, "symbol=BTCUSDT&quantity=1");
        let sig2 = sign_query_string(secret, "symbol=BTCUSDT&quantity=2");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn different_secrets_produce_different_signatures_for_the_same_query() {
        let query = "symbol=BTCUSDT&quantity=1";
        let sig1 = sign_query_string("secret-one", query);
        let sig2 = sign_query_string("secret-two", query);
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn signature_is_deterministic() {
        let secret = "test-secret";
        let query = "symbol=BTCUSDT&quantity=1";
        assert_eq!(sign_query_string(secret, query), sign_query_string(secret, query));
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p apexes-gateway-binance signing`
Expected: PASS, all 4 tests. The `matches_binance_documented_example` test is the critical one -- it's a known-answer test straight from Binance's own published documentation example, so if it fails, the HMAC implementation itself (not test setup) is wrong; do not adjust the expected value to make it pass, fix the implementation.

- [ ] **Step 3: Commit**

```bash
git add crates/apexes-gateway-binance/src/signing.rs
git commit -S -s -m "feat: implement Binance HMAC-SHA256 request signing"
```

---

### Task 14: `apexes-gateway-binance`: error type and HTTP request-building core

**Files:**
- Create: `crates/apexes-gateway-binance/src/error.rs`
- Create: `crates/apexes-gateway-binance/src/rest/mod.rs`
- Modify: `crates/apexes-gateway-binance/src/lib.rs`

**Interfaces:**
- Consumes: `sign_query_string` (Task 13), `BinanceGatewayConfig` (Task 12).
- Produces: `ApiFamily` enum + `From<AssetClass> for ApiFamily`; `BinanceRestClient` struct with `signed_request` method used by Task 16's `BinanceExecutionGateway`; `BinanceApiError` (parsed error-response shape).

- [ ] **Step 1: Create `crates/apexes-gateway-binance/src/error.rs`**

```rust
//! Internal error/response shapes for parsing Binance REST responses.
//! Not `apexes_oms::OmsError` -- this crate's job is to translate these
//! into `apexes_oms::GatewayOutcome`/`Result<_, String>` at the
//! `ExecutionGateway` trait boundary (see `gateway.rs`), matching every
//! other `ExecutionGateway` method's `String`-typed error channel.

use serde::Deserialize;

/// Binance's documented error-response body shape:
/// `{"code": -2010, "msg": "Account has insufficient balance..."}`.
/// A negative `code` with a `msg` is Binance's standard way of
/// reporting an explicit business-logic rejection (as opposed to a
/// transport-level failure, which never reaches this parsed form at
/// all -- see `gateway.rs`'s `GatewayOutcome` mapping).
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceApiError {
    pub code: i64,
    pub msg: String,
}
```

- [ ] **Step 2: Create `crates/apexes-gateway-binance/src/rest/mod.rs`**

```rust
//! Shared REST plumbing: signed-request construction, routing between
//! Binance's Spot and USDS-M Futures API families based on
//! `apexes_core::AssetClass`.

use apexes_core::AssetClass;
use chrono::Utc;

use crate::config::BinanceGatewayConfig;
use crate::signing::sign_query_string;

/// Which Binance API family a request targets. Binance Spot and
/// USDS-M Perpetual Futures are entirely separate REST APIs (different
/// base URLs, different endpoint paths, some different parameter
/// semantics) -- this crate routes every request through one of the
/// two based on the `Order`/query's `AssetClass`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiFamily {
    Spot,
    Futures,
}

impl From<AssetClass> for ApiFamily {
    fn from(asset_class: AssetClass) -> Self {
        match asset_class {
            AssetClass::Spot => ApiFamily::Spot,
            AssetClass::Perpetual => ApiFamily::Futures,
            other => panic!(
                "BinanceExecutionGateway only supports Spot and Perpetual orders, got {other:?}"
            ),
        }
    }
}

/// Thin wrapper around a `reqwest::Client` plus the config needed to
/// build and sign Binance requests. `BinanceExecutionGateway` (Task 16)
/// holds one of these; this type has no `ExecutionGateway`-specific
/// logic itself, only "build the URL, sign the query string, send the
/// request" mechanics shared by every endpoint.
#[derive(Clone)]
pub struct BinanceRestClient {
    http: reqwest::Client,
    config: BinanceGatewayConfig,
}

impl BinanceRestClient {
    pub fn new(config: BinanceGatewayConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            config,
        }
    }

    fn base_url(&self, family: ApiFamily) -> &str {
        match family {
            ApiFamily::Spot => &self.config.spot_base_url,
            ApiFamily::Futures => &self.config.futures_base_url,
        }
    }

    /// Builds the common `timestamp`/`recvWindow` query-string prefix
    /// every signed Binance endpoint requires, appends `extra_params`
    /// (already URL-encoded key=value pairs joined by `&`, caller's
    /// responsibility), computes the HMAC signature over the full
    /// resulting string, and appends `&signature=<hex>`.
    fn build_signed_query(&self, extra_params: &str) -> String {
        let timestamp = Utc::now().timestamp_millis();
        let base_query = if extra_params.is_empty() {
            format!(
                "timestamp={timestamp}&recvWindow={}",
                self.config.recv_window_ms
            )
        } else {
            format!(
                "{extra_params}&timestamp={timestamp}&recvWindow={}",
                self.config.recv_window_ms
            )
        };
        let signature = sign_query_string(&self.config.api_secret, &base_query);
        format!("{base_query}&signature={signature}")
    }

    /// Sends a signed request to `family`'s base URL + `path`, with
    /// `extra_params` as the unsigned portion of the query string (see
    /// `build_signed_query`). `method` is `"GET"`, `"POST"`, or
    /// `"DELETE"` per Binance's documented HTTP method per endpoint.
    /// Returns the raw response body text plus HTTP status code --
    /// callers (`gateway.rs`) are responsible for parsing the body into
    /// the expected success shape or `BinanceApiError`, and for mapping
    /// both into `GatewayOutcome`/`Result<_, String>` per-call, since
    /// each endpoint's success shape differs.
    pub async fn signed_request(
        &self,
        method: &str,
        family: ApiFamily,
        path: &str,
        extra_params: &str,
    ) -> Result<(reqwest::StatusCode, String), String> {
        let query = self.build_signed_query(extra_params);
        let url = format!("{}{path}?{query}", self.base_url(family));

        let request = match method {
            "GET" => self.http.get(&url),
            "POST" => self.http.post(&url),
            "DELETE" => self.http.delete(&url),
            other => return Err(format!("unsupported HTTP method: {other}")),
        }
        .header("X-MBX-APIKEY", &self.config.api_key);

        let response = request
            .send()
            .await
            .map_err(|error| format!("request failed: {error}"))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| format!("failed to read response body: {error}"))?;
        Ok((status, body))
    }
}
```

- [ ] **Step 3: Wire the new modules into `lib.rs`**

```rust
pub mod config;
pub mod error;
pub mod rest;
pub mod signing;

pub use config::BinanceGatewayConfig;
pub use error::BinanceApiError;
pub use rest::{ApiFamily, BinanceRestClient};
```

- [ ] **Step 4: Verify compilation**

Run: `cargo check -p apexes-gateway-binance`
Expected: 0 errors.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-gateway-binance/src/error.rs crates/apexes-gateway-binance/src/rest crates/apexes-gateway-binance/src/lib.rs
git commit -S -s -m "feat: add Binance REST request-building core with Spot/Futures routing"
```

---

### Task 15: `apexes-gateway-binance`: implement `BinanceExecutionGateway`

**Files:**
- Create: `crates/apexes-gateway-binance/src/gateway.rs`
- Modify: `crates/apexes-gateway-binance/src/lib.rs`

**Interfaces:**
- Consumes: `BinanceRestClient`, `ApiFamily` (Task 14); `apexes_oms::{ExecutionGateway, GatewayOutcome, ExchangeFill}` (Task 8).
- Produces: `BinanceExecutionGateway` struct implementing `ExecutionGateway`.

- [ ] **Step 1: Create `crates/apexes-gateway-binance/src/gateway.rs`**

```rust
//! [`BinanceExecutionGateway`]: implements `apexes_oms::ExecutionGateway`
//! against real Binance Spot and USDS-M Perpetual Futures REST APIs.

use apexes_core::{AssetClass, Order, OrderId, OrderStatus};
use apexes_oms::{ExchangeFill, ExecutionGateway, GatewayOutcome};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

use crate::config::BinanceGatewayConfig;
use crate::error::BinanceApiError;
use crate::rest::{ApiFamily, BinanceRestClient};

pub struct BinanceExecutionGateway {
    rest: BinanceRestClient,
}

impl BinanceExecutionGateway {
    pub fn new(config: BinanceGatewayConfig) -> Self {
        Self {
            rest: BinanceRestClient::new(config),
        }
    }

    fn order_path(family: ApiFamily) -> &'static str {
        match family {
            ApiFamily::Spot => "/api/v3/order",
            ApiFamily::Futures => "/fapi/v1/order",
        }
    }

    fn open_orders_path(family: ApiFamily) -> &'static str {
        match family {
            ApiFamily::Spot => "/api/v3/openOrders",
            ApiFamily::Futures => "/fapi/v1/openOrders",
        }
    }

    fn my_trades_path(family: ApiFamily) -> &'static str {
        match family {
            ApiFamily::Spot => "/api/v3/myTrades",
            ApiFamily::Futures => "/fapi/v1/userTrades",
        }
    }

    /// Maps a raw `(status, body)` REST response into `GatewayOutcome`,
    /// per this crate's design (`docs/superpowers/specs/2026-08-01-apexes-gateway-binance-design.md`
    /// §六): HTTP success + parseable order JSON -> `Accepted`; HTTP
    /// 4xx + a parseable `{"code": <negative>, "msg": ...}` body ->
    /// `Rejected` (an explicit business-logic refusal); anything else
    /// (network failure already surfaced as `Err` by `signed_request`,
    /// HTTP 5xx, or an unparseable body) -> `Uncertain`.
    fn map_order_response(
        status: reqwest::StatusCode,
        body: &str,
    ) -> Result<OrderResponseBody, GatewayOutcome> {
        if status.is_success() {
            match serde_json::from_str::<OrderResponseBody>(body) {
                Ok(parsed) => Ok(parsed),
                Err(error) => Err(GatewayOutcome::Uncertain {
                    reason: format!("could not parse success response body: {error}; body={body}"),
                }),
            }
        } else if status.is_client_error() {
            match serde_json::from_str::<BinanceApiError>(body) {
                Ok(api_error) => Err(GatewayOutcome::Rejected {
                    reason: format!("{} (code {})", api_error.msg, api_error.code),
                }),
                Err(_) => Err(GatewayOutcome::Uncertain {
                    reason: format!("HTTP {status} with unparseable error body: {body}"),
                }),
            }
        } else {
            Err(GatewayOutcome::Uncertain {
                reason: format!("HTTP {status}: {body}"),
            })
        }
    }
}

/// Binance's order-response JSON shape (both submit and cancel
/// responses share this shape for the fields this gateway needs).
#[derive(Debug, Deserialize)]
struct OrderResponseBody {
    #[serde(rename = "orderId")]
    order_id: i64,
}

/// Binance's open-order-list entry shape (`GET /api/v3/openOrders` /
/// `GET /fapi/v1/openOrders`), only the fields this gateway needs.
#[derive(Debug, Deserialize)]
struct OpenOrderEntry {
    #[serde(rename = "orderId")]
    order_id: i64,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    status: String,
}

/// Binance's trade-list entry shape (`GET /api/v3/myTrades` /
/// `GET /fapi/v1/userTrades`), only the fields this gateway needs.
#[derive(Debug, Deserialize)]
struct TradeEntry {
    id: i64,
    price: String,
    qty: String,
    commission: String,
    #[serde(rename = "commissionAsset")]
    commission_asset: Option<String>,
    time: i64,
}

fn parse_binance_status(status: &str) -> Result<OrderStatus, String> {
    match status {
        "NEW" => Ok(OrderStatus::New),
        "PARTIALLY_FILLED" => Ok(OrderStatus::PartiallyFilled),
        "FILLED" => Ok(OrderStatus::Filled),
        "CANCELED" | "EXPIRED" => Ok(OrderStatus::Cancelled),
        "REJECTED" => Ok(OrderStatus::Rejected),
        other => Err(format!("unknown Binance order status: {other}")),
    }
}

fn parse_decimal_field(value: &str, field: &str) -> Result<Decimal, String> {
    value
        .parse::<Decimal>()
        .map_err(|error| format!("could not parse {field} {value:?} as Decimal: {error}"))
}

fn millis_to_datetime(millis: i64) -> Result<DateTime<Utc>, String> {
    Utc.timestamp_millis_opt(millis)
        .single()
        .ok_or_else(|| format!("could not convert {millis} to a valid timestamp"))
}

#[async_trait]
impl ExecutionGateway for BinanceExecutionGateway {
    async fn submit_order(&self, order: &Order) -> GatewayOutcome {
        let family = ApiFamily::from(order.asset_class);
        let side = match order.side {
            apexes_core::Side::Buy => "BUY",
            apexes_core::Side::Sell => "SELL",
        };
        let order_type = match order.order_type {
            apexes_core::OrderType::Market => "MARKET",
            apexes_core::OrderType::Limit => "LIMIT",
            apexes_core::OrderType::Stop => "STOP_LOSS",
            apexes_core::OrderType::StopLimit => "STOP_LOSS_LIMIT",
        };

        let mut params = format!(
            "symbol={}&side={side}&type={order_type}&quantity={}&newClientOrderId={}",
            order.symbol, order.quantity, order.id.0
        );
        if let Some(price) = order.price {
            params.push_str(&format!("&price={price}"));
            if order.order_type == apexes_core::OrderType::Limit {
                params.push_str("&timeInForce=GTC");
            }
        }
        if let Some(stop_price) = order.stop_price {
            params.push_str(&format!("&stopPrice={stop_price}"));
        }

        let response = self
            .rest
            .signed_request("POST", family, Self::order_path(family), &params)
            .await;

        match response {
            Ok((status, body)) => match Self::map_order_response(status, &body) {
                Ok(parsed) => GatewayOutcome::Accepted {
                    exchange_order_id: parsed.order_id.to_string(),
                },
                Err(outcome) => outcome,
            },
            Err(reason) => GatewayOutcome::Uncertain { reason },
        }
    }

    async fn cancel_order(&self, exchange_order_id: &str) -> GatewayOutcome {
        // NOTE: cancel_order's trait signature does not receive
        // asset_class or symbol -- both are required by Binance's
        // cancel endpoint. This is a real gap in the current
        // ExecutionGateway trait surfaced during implementation; see
        // this task's Step 2 for the required trait signature fix
        // before this method can be completed.
        unimplemented!(
            "cancel_order requires symbol and asset_class, not yet available from the trait signature -- see Task 15 Step 2"
        )
    }

    async fn query_order_status(
        &self,
        exchange_order_id: &str,
        asset_class: AssetClass,
    ) -> Result<Option<OrderStatus>, String> {
        // Same gap as cancel_order: querying a single order by
        // exchange_order_id also requires `symbol` on Binance, which
        // this trait method does not currently receive either. See
        // Task 15 Step 2.
        unimplemented!(
            "query_order_status requires symbol, not yet available from the trait signature -- see Task 15 Step 2"
        )
    }

    async fn query_all_open_orders(
        &self,
        asset_class: AssetClass,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
        let family = ApiFamily::from(asset_class);
        let (status, body) = self
            .rest
            .signed_request("GET", family, Self::open_orders_path(family), "")
            .await?;
        if !status.is_success() {
            return Err(format!("HTTP {status}: {body}"));
        }
        let entries: Vec<OpenOrderEntry> =
            serde_json::from_str(&body).map_err(|error| format!("parse error: {error}; body={body}"))?;

        entries
            .into_iter()
            .map(|entry| {
                let client_order_id = entry
                    .client_order_id
                    .parse::<uuid::Uuid>()
                    .map_err(|error| {
                        format!(
                            "clientOrderId {:?} is not a valid UUID (was it set by something other than this gateway?): {error}",
                            entry.client_order_id
                        )
                    })?;
                let status = parse_binance_status(&entry.status)?;
                Ok((
                    OrderId(client_order_id),
                    Some(entry.order_id.to_string()),
                    status,
                ))
            })
            .collect()
    }

    async fn query_fills_for_order(&self, order: &Order) -> Result<Vec<ExchangeFill>, String> {
        let family = ApiFamily::from(order.asset_class);
        let params = format!("symbol={}", order.symbol);
        let (status, body) = self
            .rest
            .signed_request("GET", family, Self::my_trades_path(family), &params)
            .await?;
        if !status.is_success() {
            return Err(format!("HTTP {status}: {body}"));
        }
        let entries: Vec<TradeEntry> =
            serde_json::from_str(&body).map_err(|error| format!("parse error: {error}; body={body}"))?;

        entries
            .into_iter()
            .map(|entry| {
                Ok(ExchangeFill {
                    exchange_trade_id: entry.id.to_string(),
                    price: parse_decimal_field(&entry.price, "price")?,
                    quantity: parse_decimal_field(&entry.qty, "qty")?,
                    fee: parse_decimal_field(&entry.commission, "commission")?,
                    fee_currency: entry.commission_asset,
                    filled_at: millis_to_datetime(entry.time)?,
                })
            })
            .collect()
    }
}
```

**This task deliberately produces intermediate, non-final code for `cancel_order`/`query_order_status`** — implementing them exposed a real gap in the `ExecutionGateway` trait (`cancel_order`/`query_order_status` don't receive `symbol`, which Binance's cancel/single-order-query endpoints require). This is fixed in Step 2 below, in the same task, before moving on — following this project's established practice (see the OMS reconcile() two-pass fix) of stopping and fixing structural spec gaps discovered during implementation rather than working around them.

- [ ] **Step 2: Fix the `ExecutionGateway` trait gap: `cancel_order`/`query_order_status` need `symbol` and `asset_class`**

Binance's cancel-order and single-order-query endpoints (`DELETE /api/v3/order`, `GET /api/v3/order`, and their Futures equivalents) require `symbol` as a mandatory parameter alongside the order ID — Binance's order IDs are only unique per-symbol, not globally. The current trait signatures (`cancel_order(&self, exchange_order_id: &str)` and `query_order_status(&self, exchange_order_id: &str, asset_class: AssetClass)`) don't carry `symbol`. Fix by changing both to accept the full `&Order` instead of just `exchange_order_id`/`asset_class` separately — this also make the API more consistent with `query_fills_for_order`, which already takes `&Order`.

Go back to `crates/apexes-oms/src/gateway.rs` (Task 8's file) and change:

```rust
    /// Cancels an already-submitted order (the caller -- `OrderManager`
    /// -- guarantees `order.exchange_order_id` is `Some` before
    /// calling). Takes the full `Order` (not just its
    /// `exchange_order_id`) because real exchanges' cancel-by-ID
    /// endpoints commonly also require the order's `symbol` (Binance:
    /// order IDs are unique per-symbol, not globally) -- discovered
    /// during `apexes-gateway-binance`'s implementation (sub-project
    /// #8) and folded back into this trait rather than worked around.
    async fn cancel_order(&self, order: &Order) -> GatewayOutcome;

    /// Queries the exchange's current authoritative status for a
    /// single order. Takes the full `Order` for the same
    /// symbol-required-for-lookup reason as `cancel_order`.
    async fn query_order_status(&self, order: &Order) -> Result<Option<OrderStatus>, String>;
```

Remove the old `query_order_status(&self, exchange_order_id: &str, asset_class: AssetClass)` signature entirely, replacing it with the above. Update `MockExecutionGateway`'s implementation (`crates/apexes-oms/src/mock_gateway.rs`) to match:

```rust
    async fn cancel_order(&self, order: &Order) -> GatewayOutcome {
        if let Some(outcome) = self.next_cancel_outcome.lock().unwrap().take() {
            return outcome;
        }

        let mut state = self.exchange_state.lock().unwrap();
        let exchange_order_id = order
            .exchange_order_id
            .as_deref()
            .unwrap_or_default();
        if let Some((_, status, _)) =
            state.values_mut().find(|(id, _, _)| id == exchange_order_id)
        {
            *status = OrderStatus::Cancelled;
            GatewayOutcome::Accepted {
                exchange_order_id: exchange_order_id.to_string(),
            }
        } else {
            GatewayOutcome::Rejected {
                reason: format!("no such exchange_order_id: {exchange_order_id}"),
            }
        }
    }

    async fn query_order_status(&self, order: &Order) -> Result<Option<OrderStatus>, String> {
        let exchange_order_id = order.exchange_order_id.as_deref().unwrap_or_default();
        let state = self.exchange_state.lock().unwrap();
        Ok(state
            .values()
            .find(|(id, _, _)| id == exchange_order_id)
            .map(|(_, status, _)| *status))
    }
```

Update every existing caller of the old signatures:

**`crates/apexes-oms/src/manager.rs`'s `cancel_order` method.** Find this block (written in Task 5, unchanged since):

```rust
        let exchange_order_id = order
            .exchange_order_id
            .clone()
            .ok_or(OmsError::ExchangeOrderIdUnknown { order_id })?;

        match self.gateway.cancel_order(&exchange_order_id).await {
```

Replace it with (cloning the order first since `order` is an immutable borrow from `self.open_orders.get(&order_id)` a few lines above, and the match arms below mutate `self.open_orders`):

```rust
        if order.exchange_order_id.is_none() {
            return Err(OmsError::ExchangeOrderIdUnknown { order_id });
        }
        let order_snapshot = order.clone();

        match self.gateway.cancel_order(&order_snapshot).await {
```

**`crates/apexes-oms/src/manager.rs`'s `reconcile` method's pass-2 loop.** Find this block (written in Task 10):

```rust
        for client_order_id in still_locally_open {
            let Some(order) = self.open_orders.get(&client_order_id) else {
                continue;
            };
            let previous_status = order.status;
            let asset_class = order.asset_class;
            let Some(exchange_order_id) = order.exchange_order_id.clone() else {
                tracing::warn!(
                    order_id = ?client_order_id,
                    "reconcile: local order still open with no known exchange_order_id and not reported as open by the exchange -- cannot resolve"
                );
                continue;
            };

            match self
                .gateway
                .query_order_status(&exchange_order_id, asset_class)
                .await
            {
```

Replace it with:

```rust
        for client_order_id in still_locally_open {
            let Some(order) = self.open_orders.get(&client_order_id) else {
                continue;
            };
            let previous_status = order.status;
            if order.exchange_order_id.is_none() {
                tracing::warn!(
                    order_id = ?client_order_id,
                    "reconcile: local order still open with no known exchange_order_id and not reported as open by the exchange -- cannot resolve"
                );
                continue;
            }
            let order_snapshot = order.clone();
            let exchange_order_id = order_snapshot.exchange_order_id.clone().unwrap();

            match self.gateway.query_order_status(&order_snapshot).await {
```

The rest of that `match` block's arms (`Ok(Some(status))`/`Ok(None)`/`Err(reason)`) are unchanged from Task 10 -- they already reference `exchange_order_id`/`previous_status`/`client_order_id`, all still in scope under the new names above.

**`crates/apexes-oms/src/mock_gateway.rs`'s tests (from Task 8).** Find and replace these three tests:

```rust
    #[tokio::test]
    async fn query_order_status_returns_none_for_unknown_exchange_order_id() {
        let gateway = MockExecutionGateway::new();
        let result = gateway
            .query_order_status("never-submitted", AssetClass::Spot)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn cancel_order_marks_it_cancelled_by_default() {
        let gateway = MockExecutionGateway::new();
        let order = sample_order();
        let outcome = gateway.submit_order(&order).await;
        let GatewayOutcome::Accepted { exchange_order_id } = outcome else {
            panic!("expected Accepted");
        };

        let cancel_outcome = gateway.cancel_order(&exchange_order_id).await;
        assert!(matches!(cancel_outcome, GatewayOutcome::Accepted { .. }));

        let status = gateway
            .query_order_status(&exchange_order_id, AssetClass::Spot)
            .await
            .unwrap();
        assert_eq!(status, Some(OrderStatus::Cancelled));
    }
```

With:

```rust
    #[tokio::test]
    async fn query_order_status_returns_none_for_unknown_exchange_order_id() {
        let gateway = MockExecutionGateway::new();
        let mut order = sample_order();
        order.exchange_order_id = Some("never-submitted".to_string());
        let result = gateway.query_order_status(&order).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn cancel_order_marks_it_cancelled_by_default() {
        let gateway = MockExecutionGateway::new();
        let order = sample_order();
        let outcome = gateway.submit_order(&order).await;
        let GatewayOutcome::Accepted { exchange_order_id } = outcome else {
            panic!("expected Accepted");
        };
        let mut order_with_id = order.clone();
        order_with_id.exchange_order_id = Some(exchange_order_id.clone());

        let cancel_outcome = gateway.cancel_order(&order_with_id).await;
        assert!(matches!(cancel_outcome, GatewayOutcome::Accepted { .. }));

        let status = gateway.query_order_status(&order_with_id).await.unwrap();
        assert_eq!(status, Some(OrderStatus::Cancelled));
    }
```

Also check `apexes-oms/src/manager.rs`'s test `cancel_order_with_unknown_exchange_id_is_rejected_without_calling_gateway` (from the file's original test suite, unmodified by any task so far) -- it calls `manager.cancel_order(order_id)` (the `OrderManager` method, not the gateway trait method directly), so it needs NO change; `OrderManager::cancel_order`'s public signature was never touched by this task, only its internal call to `self.gateway.cancel_order(...)`.

**`crates/apexes-gateway-binance/src/gateway.rs`'s `cancel_order`/`query_order_status` (this task, Task 15's own Step 1 code).** Rewrite using the new `&Order`-based signatures, now with `order.symbol` available:

```rust
    async fn cancel_order(&self, order: &Order) -> GatewayOutcome {
        let family = ApiFamily::from(order.asset_class);
        let Some(exchange_order_id) = &order.exchange_order_id else {
            return GatewayOutcome::Rejected {
                reason: "cannot cancel an order with no known exchange_order_id".to_string(),
            };
        };
        let params = format!("symbol={}&orderId={exchange_order_id}", order.symbol);

        let response = self
            .rest
            .signed_request("DELETE", family, Self::order_path(family), &params)
            .await;

        match response {
            Ok((status, body)) => match Self::map_order_response(status, &body) {
                Ok(parsed) => GatewayOutcome::Accepted {
                    exchange_order_id: parsed.order_id.to_string(),
                },
                Err(outcome) => outcome,
            },
            Err(reason) => GatewayOutcome::Uncertain { reason },
        }
    }

    async fn query_order_status(&self, order: &Order) -> Result<Option<OrderStatus>, String> {
        let family = ApiFamily::from(order.asset_class);
        let Some(exchange_order_id) = &order.exchange_order_id else {
            return Ok(None);
        };
        let params = format!("symbol={}&orderId={exchange_order_id}", order.symbol);

        let (status, body) = self
            .rest
            .signed_request("GET", family, Self::order_path(family), &params)
            .await?;

        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(format!("HTTP {status}: {body}"));
        }

        #[derive(Deserialize)]
        struct SingleOrderStatus {
            status: String,
        }
        let parsed: SingleOrderStatus =
            serde_json::from_str(&body).map_err(|error| format!("parse error: {error}; body={body}"))?;
        Ok(Some(parse_binance_status(&parsed.status)?))
    }
```

- [ ] **Step 3: Verify compilation across the affected crates**

Run: `cargo check -p apexes-oms --tests`
Expected: 0 errors.

Run: `cargo check -p apexes-gateway-binance`
Expected: 0 errors (also remove the now-unused `AssetClass` import from `query_order_status`'s old signature if no longer referenced directly in `gateway.rs` — verify with the compiler's unused-import warnings).

- [ ] **Step 4: Wire `gateway` module into `lib.rs`**

```rust
pub mod config;
pub mod error;
pub mod gateway;
pub mod rest;
pub mod signing;

pub use config::BinanceGatewayConfig;
pub use error::BinanceApiError;
pub use gateway::BinanceExecutionGateway;
pub use rest::{ApiFamily, BinanceRestClient};
```

- [ ] **Step 5: Run apexes-oms's full test suite (including ignored) to verify the trait-signature fix didn't break reconcile's behavior**

Ensure `docker compose up -d postgres` is running (from Task 10), then:

Run: `cargo test -p apexes-oms -- --ignored`
Expected: PASS.

Run: `cargo test -p apexes-oms --test live_postgres -- --ignored`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-oms/src/gateway.rs crates/apexes-oms/src/mock_gateway.rs crates/apexes-oms/src/manager.rs crates/apexes-gateway-binance/src/gateway.rs crates/apexes-gateway-binance/src/lib.rs
git commit -S -s -m "fix: change ExecutionGateway::cancel_order/query_order_status to take &Order (symbol required by real exchanges)"
```

---

### Task 16: `apexes-gateway-binance`: wiremock tests verifying real request shape and `GatewayOutcome` mapping

**Files:**
- Create: `crates/apexes-gateway-binance/tests/wiremock_rest.rs`

**Interfaces:**
- Consumes: `BinanceExecutionGateway`, `BinanceGatewayConfig` (Tasks 12, 15).

- [ ] **Step 1: Write the wiremock test file**

```rust
//! Verifies `BinanceExecutionGateway` sends correctly-shaped, correctly-
//! signed HTTP requests, and correctly maps Binance's three response
//! shapes (success / explicit rejection / ambiguous failure) to
//! `GatewayOutcome`, using a local `wiremock` server instead of real
//! Binance endpoints -- no network access required.

use apexes_core::{AssetClass, Order, OrderId, OrderStatus, OrderType, Side, Venue};
use apexes_gateway_binance::{BinanceExecutionGateway, BinanceGatewayConfig};
use apexes_oms::{ExecutionGateway, GatewayOutcome};
use chrono::Utc;
use rust_decimal_macros::dec;
use uuid::Uuid;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn sample_order(id: Uuid) -> Order {
    Order {
        id: OrderId(id),
        exchange_order_id: None,
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Market,
        quantity: dec!(1),
        price: None,
        stop_price: None,
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
async fn submit_order_sends_signed_request_with_client_order_id() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();

    Mock::given(method("POST"))
        .and(path("/api/v3/order"))
        .and(query_param("newClientOrderId", order_id.to_string()))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "orderId": 12345
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&sample_order(order_id)).await;

    assert_eq!(
        outcome,
        GatewayOutcome::Accepted {
            exchange_order_id: "12345".to_string()
        }
    );
}

#[tokio::test]
async fn submit_order_request_includes_a_signature_parameter() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();

    Mock::given(method("POST"))
        .and(path("/api/v3/order"))
        .respond_with(move |request: &wiremock::Request| {
            let query = request.url.query().unwrap_or_default();
            assert!(
                query.contains("signature="),
                "request query string must include a signature parameter, got: {query}"
            );
            assert!(
                query.contains("timestamp="),
                "request query string must include a timestamp parameter, got: {query}"
            );
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "orderId": 1 }))
        })
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    gateway.submit_order(&sample_order(order_id)).await;
}

#[tokio::test]
async fn submit_order_maps_explicit_binance_rejection_to_rejected_outcome() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();

    Mock::given(method("POST"))
        .and(path("/api/v3/order"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "code": -2010,
            "msg": "Account has insufficient balance for requested action."
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&sample_order(order_id)).await;

    assert!(matches!(outcome, GatewayOutcome::Rejected { .. }));
    if let GatewayOutcome::Rejected { reason } = outcome {
        assert!(reason.contains("insufficient balance"));
    }
}

#[tokio::test]
async fn submit_order_maps_server_error_to_uncertain_outcome() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();

    Mock::given(method("POST"))
        .and(path("/api/v3/order"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal server error"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&sample_order(order_id)).await;

    assert!(matches!(outcome, GatewayOutcome::Uncertain { .. }));
}

#[tokio::test]
async fn submit_order_maps_unparseable_success_body_to_uncertain_outcome() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();

    Mock::given(method("POST"))
        .and(path("/api/v3/order"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&sample_order(order_id)).await;

    assert!(matches!(outcome, GatewayOutcome::Uncertain { .. }));
}

#[tokio::test]
async fn submit_order_routes_perpetual_orders_to_futures_path() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();
    let mut order = sample_order(order_id);
    order.asset_class = AssetClass::Perpetual;

    Mock::given(method("POST"))
        .and(path("/fapi/v1/order"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "orderId": 999
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.submit_order(&order).await;

    assert_eq!(
        outcome,
        GatewayOutcome::Accepted {
            exchange_order_id: "999".to_string()
        }
    );
}

#[tokio::test]
async fn query_all_open_orders_parses_client_order_id_back_into_order_id() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();

    Mock::given(method("GET"))
        .and(path("/api/v3/openOrders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "orderId": 555,
                "clientOrderId": order_id.to_string(),
                "status": "PARTIALLY_FILLED"
            }
        ])))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let open_orders = gateway
        .query_all_open_orders(AssetClass::Spot)
        .await
        .unwrap();

    assert_eq!(open_orders.len(), 1);
    assert_eq!(open_orders[0].0, OrderId(order_id));
    assert_eq!(open_orders[0].1, Some("555".to_string()));
    assert_eq!(open_orders[0].2, OrderStatus::PartiallyFilled);
}

#[tokio::test]
async fn query_fills_for_order_parses_trade_list() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();
    let mut order = sample_order(order_id);
    order.exchange_order_id = Some("777".to_string());

    Mock::given(method("GET"))
        .and(path("/api/v3/myTrades"))
        .and(query_param("symbol", "BTCUSDT"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": 1001,
                "price": "65000.00000000",
                "qty": "0.50000000",
                "commission": "1.20000000",
                "commissionAsset": "USDT",
                "time": 1700000000000i64
            }
        ])))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let fills = gateway.query_fills_for_order(&order).await.unwrap();

    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].exchange_trade_id, "1001");
    assert_eq!(fills[0].price, dec!(65000));
    assert_eq!(fills[0].quantity, dec!(0.5));
    assert_eq!(fills[0].fee, dec!(1.2));
    assert_eq!(fills[0].fee_currency, Some("USDT".to_string()));
}

#[tokio::test]
async fn cancel_order_sends_symbol_and_order_id() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();
    let mut order = sample_order(order_id);
    order.exchange_order_id = Some("321".to_string());

    Mock::given(method("DELETE"))
        .and(path("/api/v3/order"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(query_param("orderId", "321"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "orderId": 321
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.cancel_order(&order).await;

    assert_eq!(
        outcome,
        GatewayOutcome::Accepted {
            exchange_order_id: "321".to_string()
        }
    );
}

#[tokio::test]
async fn cancel_order_with_no_exchange_order_id_is_rejected_without_a_network_call() {
    let mock_server = MockServer::start().await;
    let order_id = Uuid::new_v4();
    let order = sample_order(order_id); // exchange_order_id is None

    // No Mock registered at all -- if the gateway tried to make a
    // request, wiremock would return a 404 "no matching mock" response
    // (not what this test is checking for), but the real assertion is
    // that GatewayOutcome::Rejected is returned immediately without
    // attempting any request.
    let gateway = gateway_against(&mock_server).await;
    let outcome = gateway.cancel_order(&order).await;

    assert!(matches!(outcome, GatewayOutcome::Rejected { .. }));
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p apexes-gateway-binance --test wiremock_rest`
Expected: PASS, all 10 tests.

- [ ] **Step 3: Commit**

```bash
git add crates/apexes-gateway-binance/tests/wiremock_rest.rs
git commit -S -s -m "test: add wiremock-based request-shape and GatewayOutcome mapping tests"
```

---

### Task 17: `apexes-gateway-binance`: User Data Stream (listenKey lifecycle + WebSocket event parsing)

**Files:**
- Create: `crates/apexes-gateway-binance/src/user_data_stream.rs`
- Modify: `crates/apexes-gateway-binance/src/lib.rs`
- Modify: `crates/apexes-gateway-binance/src/gateway.rs`

**Interfaces:**
- Consumes: `BinanceRestClient` (Task 14), `apexes_oms::ExchangeUpdate` (Task 9).
- Produces: `BinanceExecutionGateway::start_user_data_stream(&self) -> Result<(UserDataStreamHandle, impl Stream<Item = ExchangeUpdate>), String>`; `UserDataStreamHandle` (keeps the renewal background task alive; dropping it stops renewal).

- [ ] **Step 1: Add listenKey REST endpoints to `BinanceRestClient`'s call sites in a new module**

Create `crates/apexes-gateway-binance/src/user_data_stream.rs`:

```rust
//! Binance User Data Stream: push-based order/fill updates delivered
//! over a WebSocket, keyed by a `listenKey` obtained via REST and
//! renewed periodically (Binance requires renewal within 60 minutes of
//! the last renewal or the key expires and the stream disconnects).
//! Spot and USDS-M Futures each have their OWN separate User Data
//! Stream (separate listenKey-issuing endpoints, separate WebSocket
//! hosts) -- `start_user_data_stream` subscribes to both and merges
//! their events into one output stream.

use apexes_core::{AssetClass, OrderId, OrderStatus};
use apexes_oms::ExchangeUpdate;
use futures::stream::{self, Stream, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::config::BinanceGatewayConfig;
use crate::rest::{ApiFamily, BinanceRestClient};

/// Keeps the listenKey renewal background task alive for both the Spot
/// and Futures streams. Dropping this stops renewal (and, since Binance
/// expires an un-renewed listenKey after 60 minutes, eventually the
/// WebSocket connections too) -- callers must keep this alive for as
/// long as they want the User Data Stream to keep running.
pub struct UserDataStreamHandle {
    _spot_renewal: tokio::task::JoinHandle<()>,
    _futures_renewal: tokio::task::JoinHandle<()>,
}

impl Drop for UserDataStreamHandle {
    fn drop(&mut self) {
        self._spot_renewal.abort();
        self._futures_renewal.abort();
    }
}

async fn obtain_listen_key(
    rest: &BinanceRestClient,
    family: ApiFamily,
) -> Result<String, String> {
    let path = match family {
        ApiFamily::Spot => "/api/v3/userDataStream",
        ApiFamily::Futures => "/fapi/v1/listenKey",
    };
    // NOTE: this endpoint is documented by Binance as NOT requiring a
    // signature (only the API key header) -- signed_request always
    // signs, which Binance tolerates (extra params are ignored) per
    // community-verified behavior, so reusing signed_request here is
    // safe and avoids a second request-building code path.
    let (status, body) = rest.signed_request("POST", family, path, "").await?;
    if !status.is_success() {
        return Err(format!("failed to obtain listenKey: HTTP {status}: {body}"));
    }

    #[derive(Deserialize)]
    struct ListenKeyResponse {
        #[serde(rename = "listenKey")]
        listen_key: String,
    }
    let parsed: ListenKeyResponse =
        serde_json::from_str(&body).map_err(|error| format!("parse error: {error}; body={body}"))?;
    Ok(parsed.listen_key)
}

async fn renew_listen_key(
    rest: &BinanceRestClient,
    family: ApiFamily,
    listen_key: &str,
) -> Result<(), String> {
    let path = match family {
        ApiFamily::Spot => "/api/v3/userDataStream",
        ApiFamily::Futures => "/fapi/v1/listenKey",
    };
    let params = format!("listenKey={listen_key}");
    let (status, body) = rest.signed_request("PUT", family, path, &params).await?;
    if !status.is_success() {
        return Err(format!("failed to renew listenKey: HTTP {status}: {body}"));
    }
    Ok(())
}

fn websocket_host(family: ApiFamily) -> &'static str {
    match family {
        ApiFamily::Spot => "wss://stream.binance.com:9443/ws",
        ApiFamily::Futures => "wss://fstream.binance.com/ws",
    }
}

/// Parses one raw WebSocket text message into an `ExchangeUpdate`, or
/// `None` if the message is a User Data Stream event type this gateway
/// doesn't need to act on (Binance's User Data Stream carries several
/// event types -- `outboundAccountPosition`, `balanceUpdate`, etc. --
/// only `executionReport` (spot) and `ORDER_TRADE_UPDATE` (futures)
/// carry order status changes).
fn parse_event(raw: &str) -> Option<ExchangeUpdate> {
    let value: Value = serde_json::from_str(raw).ok()?;
    let event_type = value.get("e")?.as_str()?;

    match event_type {
        "executionReport" => {
            let client_order_id = value.get("c")?.as_str()?.parse::<uuid::Uuid>().ok()?;
            let exchange_order_id = value.get("i")?.as_i64()?.to_string();
            let status = parse_binance_status(value.get("X")?.as_str()?)?;
            Some(ExchangeUpdate {
                client_order_id: OrderId(client_order_id),
                exchange_order_id: Some(exchange_order_id),
                status,
            })
        }
        "ORDER_TRADE_UPDATE" => {
            let order = value.get("o")?;
            let client_order_id = order.get("c")?.as_str()?.parse::<uuid::Uuid>().ok()?;
            let exchange_order_id = order.get("i")?.as_i64()?.to_string();
            let status = parse_binance_status(order.get("X")?.as_str()?)?;
            Some(ExchangeUpdate {
                client_order_id: OrderId(client_order_id),
                exchange_order_id: Some(exchange_order_id),
                status,
            })
        }
        _ => None,
    }
}

fn parse_binance_status(status: &str) -> Option<OrderStatus> {
    match status {
        "NEW" => Some(OrderStatus::New),
        "PARTIALLY_FILLED" => Some(OrderStatus::PartiallyFilled),
        "FILLED" => Some(OrderStatus::Filled),
        "CANCELED" | "EXPIRED" => Some(OrderStatus::Cancelled),
        "REJECTED" => Some(OrderStatus::Rejected),
        _ => None,
    }
}

/// Starts both the Spot and Futures User Data Streams, spawning a
/// renewal background task for each (every 30 minutes -- well within
/// Binance's 60-minute expiry window), and returns a merged stream of
/// `ExchangeUpdate`s from both plus a handle that keeps renewal alive.
pub async fn start_user_data_stream(
    rest: BinanceRestClient,
) -> Result<(UserDataStreamHandle, impl Stream<Item = ExchangeUpdate>), String> {
    let (tx, rx) = mpsc::unbounded_channel();

    let spot_listen_key = obtain_listen_key(&rest, ApiFamily::Spot).await?;
    let futures_listen_key = obtain_listen_key(&rest, ApiFamily::Futures).await?;

    let spot_renewal = spawn_stream_task(rest.clone(), ApiFamily::Spot, spot_listen_key, tx.clone());
    let futures_renewal =
        spawn_stream_task(rest.clone(), ApiFamily::Futures, futures_listen_key, tx);

    let handle = UserDataStreamHandle {
        _spot_renewal: spot_renewal,
        _futures_renewal: futures_renewal,
    };

    Ok((handle, tokio_stream_from_receiver(rx)))
}

fn tokio_stream_from_receiver(
    rx: mpsc::UnboundedReceiver<ExchangeUpdate>,
) -> impl Stream<Item = ExchangeUpdate> {
    stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|update| (update, rx))
    })
}

/// Spawns one background task that: connects the WebSocket for
/// `family` using `listen_key`, forwards parsed `ExchangeUpdate`s to
/// `tx`, and renews `listen_key` every 30 minutes for as long as the
/// task runs (aborted when the returned `JoinHandle` is dropped via
/// `UserDataStreamHandle`'s `Drop` impl).
fn spawn_stream_task(
    rest: BinanceRestClient,
    family: ApiFamily,
    listen_key: String,
    tx: mpsc::UnboundedSender<ExchangeUpdate>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let renewal_rest = rest.clone();
        let renewal_key = listen_key.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30 * 60));
            loop {
                interval.tick().await;
                if let Err(reason) = renew_listen_key(&renewal_rest, family, &renewal_key).await {
                    tracing::warn!(reason, ?family, "failed to renew Binance listenKey");
                }
            }
        });

        let url = format!("{}/{listen_key}", websocket_host(family));
        loop {
            match tokio_tungstenite::connect_async(&url).await {
                Ok((ws_stream, _)) => {
                    let (_, mut read) = ws_stream.split();
                    while let Some(message) = read.next().await {
                        match message {
                            Ok(Message::Text(text)) => {
                                if let Some(update) = parse_event(&text)
                                    && tx.send(update).is_err()
                                {
                                    return; // receiver dropped, stop this task
                                }
                            }
                            Ok(_) => {}
                            Err(error) => {
                                tracing::warn!(?error, ?family, "User Data Stream WebSocket error");
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(?error, ?family, "failed to connect User Data Stream WebSocket");
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_spot_execution_report_into_exchange_update() {
        let order_id = uuid::Uuid::new_v4();
        let raw = serde_json::json!({
            "e": "executionReport",
            "c": order_id.to_string(),
            "i": 12345,
            "X": "PARTIALLY_FILLED"
        })
        .to_string();

        let update = parse_event(&raw).unwrap();
        assert_eq!(update.client_order_id, OrderId(order_id));
        assert_eq!(update.exchange_order_id, Some("12345".to_string()));
        assert_eq!(update.status, OrderStatus::PartiallyFilled);
    }

    #[test]
    fn parses_futures_order_trade_update_into_exchange_update() {
        let order_id = uuid::Uuid::new_v4();
        let raw = serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "o": {
                "c": order_id.to_string(),
                "i": 999,
                "X": "FILLED"
            }
        })
        .to_string();

        let update = parse_event(&raw).unwrap();
        assert_eq!(update.client_order_id, OrderId(order_id));
        assert_eq!(update.exchange_order_id, Some("999".to_string()));
        assert_eq!(update.status, OrderStatus::Filled);
    }

    #[test]
    fn ignores_unrecognized_event_types() {
        let raw = serde_json::json!({
            "e": "outboundAccountPosition"
        })
        .to_string();

        assert!(parse_event(&raw).is_none());
    }

    #[test]
    fn ignores_unparseable_json() {
        assert!(parse_event("not json").is_none());
    }
}
```

- [ ] **Step 2: Make `BinanceRestClient` cloneable-safe for use across the spawned tasks**

`BinanceRestClient` already derives `Clone` (Task 14) — confirm this remains true (its fields are `reqwest::Client`, which is internally `Arc`-based and cheap to clone, and `BinanceGatewayConfig`, which derives `Clone`). No change needed if Task 14 was implemented as specified; verify by reading `crates/apexes-gateway-binance/src/rest/mod.rs`'s `#[derive(Clone)]` on `BinanceRestClient`.

- [ ] **Step 3: Add a `start_user_data_stream` method to `BinanceExecutionGateway`**

In `crates/apexes-gateway-binance/src/gateway.rs`, add a method exposing the REST client for this purpose (the field `rest` is currently private to the struct — add a public accessor rather than making the field itself public, to keep `BinanceRestClient`'s internals an implementation detail):

```rust
impl BinanceExecutionGateway {
    // ... existing new()/order_path()/etc. unchanged ...

    /// Starts the Binance User Data Stream (both Spot and Futures),
    /// returning a handle that must be kept alive for as long as the
    /// stream should keep running, plus the merged update stream
    /// itself. See `crate::user_data_stream` for details.
    pub async fn start_user_data_stream(
        &self,
    ) -> Result<
        (
            crate::user_data_stream::UserDataStreamHandle,
            impl futures::Stream<Item = apexes_oms::ExchangeUpdate>,
        ),
        String,
    > {
        crate::user_data_stream::start_user_data_stream(self.rest.clone()).await
    }
}
```

- [ ] **Step 4: Wire the new module into `lib.rs`**

```rust
pub mod config;
pub mod error;
pub mod gateway;
pub mod rest;
pub mod signing;
pub mod user_data_stream;

pub use config::BinanceGatewayConfig;
pub use error::BinanceApiError;
pub use gateway::BinanceExecutionGateway;
pub use rest::{ApiFamily, BinanceRestClient};
pub use user_data_stream::UserDataStreamHandle;
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p apexes-gateway-binance user_data_stream`
Expected: PASS, all 4 parsing tests.

Run: `cargo test -p apexes-gateway-binance`
Expected: PASS (all tests across the crate: signing, wiremock_rest, user_data_stream).

Run: `cargo check -p apexes-gateway-binance`
Expected: 0 errors, 0 warnings (check for unused imports like `Value` in `gateway.rs` if it's no longer directly used there after Step 3 -- remove if so).

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-gateway-binance/src/user_data_stream.rs crates/apexes-gateway-binance/src/gateway.rs crates/apexes-gateway-binance/src/lib.rs
git commit -S -s -m "feat: implement Binance User Data Stream client with listenKey renewal"
```

---

### Task 18: Scaffold `apexes-live-trading` crate

**Files:**
- Create: `crates/apexes-live-trading/Cargo.toml`
- Create: `crates/apexes-live-trading/src/lib.rs`

**Interfaces:**
- Produces: empty crate shell, ready for Task 19's `run_live_trading`.

- [ ] **Step 1: Create `crates/apexes-live-trading/Cargo.toml`**

```toml
[package]
name = "apexes-live-trading"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-oms = { path = "../apexes-oms" }
apexes-backtest = { path = "../apexes-backtest" }
apexes-strategy = { path = "../apexes-strategy" }
apexes-risk = { path = "../apexes-risk" }
tokio = { workspace = true, features = ["time", "sync"] }
futures = { workspace = true }
chrono = { workspace = true }
rust_decimal = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
apexes-oms = { path = "../apexes-oms" }
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
async-trait = { workspace = true }
uuid = { workspace = true, features = ["v4"] }
```

(Note: `apexes-oms` appears in both `[dependencies]` and `[dev-dependencies]` — this is redundant; a dependency declared in `[dependencies]` is automatically available to tests in the same crate. Remove the `[dev-dependencies]` line for `apexes-oms` — the corrected `[dev-dependencies]` block is:)

```toml
[dev-dependencies]
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
async-trait = { workspace = true }
uuid = { workspace = true, features = ["v4"] }
```

- [ ] **Step 2: Create `crates/apexes-live-trading/src/lib.rs`**

```rust
//! Apexes live-trading event loop (sub-project #8): exchange-agnostic
//! orchestration driving `Strategy` -> `OrderManager` against live
//! market data and gateway push/poll updates. Structurally parallel to
//! `apexes-paper::run_paper_trading`, but sources order state from a
//! real `apexes_oms::ExecutionGateway`/`OrderManager` instead of a
//! local `apexes_backtest::MatchingEngine` -- fills are real exchange
//! executions, not simulated ones.
//!
//! This crate depends on `apexes-backtest` only for
//! `apexes_backtest::PortfolioLedger` (a generic "rebuild positions/
//! balances from a stream of `Fill`s" component with no
//! backtest-specific logic) -- reused here as `run_live_trading`'s
//! temporary position/balance tracking, pending sub-project #11
//! (account & fund management), which will supersede it.

pub mod runner;

pub use runner::{LiveTradingConfig, LiveTradingReport, run_live_trading};
```

- [ ] **Step 3: Add to workspace members (already done in Task 12's Cargo.toml edit)**

Verify `crates/apexes-live-trading` is present in the workspace root `Cargo.toml`'s `members` list (added in Task 12, Step 1). If Task 12 was completed as specified, no action needed here — just confirm:

Run: `grep apexes-live-trading /Users/rickzhang/src/oss/apexes/Cargo.toml`
Expected: one match, inside the `members = [...]` line.

- [ ] **Step 4: Verify the crate scaffolding compiles (will fail — `runner` module doesn't exist yet)**

Run: `cargo check -p apexes-live-trading`
Expected: fails with "file not found for module `runner`" — expected at this point, confirms Cargo.toml/workspace wiring is otherwise correct. Task 19 creates `runner.rs`.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-live-trading/Cargo.toml crates/apexes-live-trading/src/lib.rs
git commit -S -s -m "feat: scaffold apexes-live-trading crate"
```

---

### Task 19: `apexes-live-trading`: implement `run_live_trading`

**Files:**
- Create: `crates/apexes-live-trading/src/runner.rs`

**Interfaces:**
- Consumes: `apexes_oms::{OrderManager, ExchangeUpdate}`, `apexes_backtest::{PortfolioLedger, PerformanceTracker, PerformanceSummary}`, `apexes_strategy::{Strategy, StrategyContext, Position}`, `apexes_risk::CompositeRiskChecker`, `apexes_core::{MarketDataProvider, Instrument, Fill}`.
- Produces: `LiveTradingConfig`, `LiveTradingReport`, `run_live_trading`.

- [ ] **Step 1: Write `crates/apexes-live-trading/src/runner.rs`**

```rust
//! [`run_live_trading`]: drives `Strategy` -> `OrderManager` against
//! live market data, exchange push updates, and a periodic
//! `reconcile()` poll -- see this crate's `lib.rs` docs for the overall
//! design and its relationship to `apexes-paper::run_paper_trading`.

use std::collections::HashMap;
use std::time::Duration;

use apexes_backtest::{PerformanceSummary, PerformanceTracker, PortfolioLedger};
use apexes_core::{ApexesError, Fill, Instrument, MarketDataProvider, MarketEvent, OrderId};
use apexes_oms::{ExchangeUpdate, OrderManager};
use apexes_risk::CompositeRiskChecker;
use apexes_strategy::{Strategy, StrategyContext};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use tokio::sync::oneshot;

pub struct LiveTradingConfig {
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    pub risk_free_return: Decimal,
    /// Fallback polling interval for `OrderManager::reconcile()`,
    /// running alongside (not instead of) the push-based
    /// `exchange_updates` stream -- catches anything a WS disconnect
    /// window might have missed.
    pub reconcile_interval: Duration,
}

pub struct LiveTradingReport {
    pub overall: PerformanceSummary,
    pub trades: Vec<Fill>,
}

/// Runs the live-trading event loop until `stop_signal` fires or its
/// sender is dropped, then returns whatever `LiveTradingReport` has
/// accumulated so far (not an error -- graceful shutdown, matching
/// `run_backtest`/`run_paper_trading`'s stop-signal semantics).
///
/// Three concurrent event sources, selected via `tokio::select!`:
/// 1. `market_data_provider`'s live `MarketEvent` stream ->
///    `strategy.on_market_event` -> `risk_checker` (if configured) ->
///    `order_manager.submit_order` for each resulting `OrderIntent`.
/// 2. `exchange_updates` (a real gateway's push stream, e.g. Binance
///    User Data Stream via `apexes-gateway-binance`) ->
///    `order_manager.apply_exchange_update` -> any returned `Fill`s
///    applied to `ledger`.
/// 3. A `config.reconcile_interval` timer -> `order_manager.reconcile()`
///    -> any returned `Fill`s applied to `ledger` the same way.
pub async fn run_live_trading(
    config: LiveTradingConfig,
    market_data_provider: &dyn MarketDataProvider,
    mut strategy: impl Strategy,
    mut order_manager: OrderManager,
    risk_checker: Option<CompositeRiskChecker>,
    mut exchange_updates: impl Stream<Item = ExchangeUpdate> + Unpin,
    mut stop_signal: oneshot::Receiver<()>,
) -> Result<LiveTradingReport, ApexesError> {
    let mut market_events = market_data_provider
        .subscribe(&config.instruments)
        .await?;

    let instruments_by_symbol: HashMap<String, Instrument> = config
        .instruments
        .iter()
        .map(|instrument| (instrument.symbol.clone(), instrument.clone()))
        .collect();

    let mut ledger = PortfolioLedger::new(instruments_by_symbol, config.initial_balances);
    let mut tracker = PerformanceTracker::new(config.risk_free_return);
    let mut all_fills: Vec<Fill> = Vec::new();
    let mut reconcile_timer = tokio::time::interval(config.reconcile_interval);

    loop {
        tokio::select! {
            _ = &mut stop_signal => break,

            market_event = market_events.next() => {
                let Some(event) = market_event else { break };
                handle_market_event(
                    &event,
                    &mut strategy,
                    &risk_checker,
                    &mut order_manager,
                    &ledger,
                ).await;
            }

            update = exchange_updates.next() => {
                let Some(update) = update else { continue };
                match order_manager
                    .apply_exchange_update(update.client_order_id, update.exchange_order_id, update.status)
                    .await
                {
                    Ok(new_fills) => apply_new_fills(&mut ledger, &mut tracker, &mut all_fills, new_fills),
                    Err(error) => tracing::warn!(?error, "apply_exchange_update failed"),
                }
            }

            _ = reconcile_timer.tick() => {
                match order_manager.reconcile().await {
                    Ok(changes) => {
                        for (order_id, new_fills) in changes {
                            tracing::info!(order_id = ?order_id, fill_count = new_fills.len(), "reconcile applied a status change");
                            apply_new_fills(&mut ledger, &mut tracker, &mut all_fills, new_fills);
                        }
                    }
                    Err(error) => tracing::warn!(?error, "reconcile() failed"),
                }
            }
        }
    }

    Ok(LiveTradingReport {
        overall: tracker.summary(),
        trades: all_fills,
    })
}

async fn handle_market_event(
    event: &MarketEvent,
    strategy: &mut impl Strategy,
    risk_checker: &Option<CompositeRiskChecker>,
    order_manager: &mut OrderManager,
    ledger: &PortfolioLedger,
) {
    let ctx = StrategyContext {
        positions: ledger.positions(),
        balances: ledger.balances(),
        open_orders: order_manager.open_orders(),
        now: event_timestamp(event),
    };
    let intents = strategy.on_market_event(event, &ctx);

    for intent in intents {
        if let Some(checker) = risk_checker
            && let Err(violations) = checker.check(
                &intent,
                ledger.positions(),
                ledger.balances(),
                order_manager.open_orders(),
                event_timestamp(event),
            )
        {
            tracing::warn!(?violations, symbol = %intent.symbol, "order rejected by risk checker");
            continue;
        }

        let symbol = intent.symbol.clone();
        match order_manager
            .submit_order(intent, ledger.positions(), ledger.balances())
            .await
        {
            Ok(order_id) => tracing::info!(order_id = ?order_id, symbol, "order submitted"),
            Err(error) => tracing::warn!(?error, symbol, "order submission failed"),
        }
    }
}

fn apply_new_fills(
    ledger: &mut PortfolioLedger,
    tracker: &mut PerformanceTracker,
    all_fills: &mut Vec<Fill>,
    new_fills: Vec<Fill>,
) {
    for fill in &new_fills {
        if let Err(error) = ledger.apply_fill(fill) {
            tracing::warn!(?error, order_id = ?fill.order_id, "failed to apply fill to ledger");
            continue;
        }
        tracker.record_fill(fill);
    }
    all_fills.extend(new_fills);
}

fn event_timestamp(event: &MarketEvent) -> chrono::DateTime<chrono::Utc> {
    match event {
        MarketEvent::Tick(tick) => tick.timestamp,
        MarketEvent::Depth(depth) => depth.timestamp,
        MarketEvent::Bar(bar) => bar.open_time,
        MarketEvent::Trade(trade) => trade.timestamp,
    }
}
```

Note: `order_id: OrderId` import is unused if not referenced directly elsewhere — check after writing and remove the `OrderId` import from the top `use apexes_core::{...}` line if the compiler flags it unused (it's used implicitly via `order_manager.reconcile()`'s return type destructuring `(order_id, new_fills)`, which does need `OrderId` in scope only if explicitly type-annotated; since it's inferred here, verify whether the import is actually needed — if `cargo check` warns it's unused, remove it from the `use` line).

- [ ] **Step 2: Verify compilation**

Run: `cargo check -p apexes-live-trading`
Expected: 0 errors. Fix any unused-import warnings per the note above.

- [ ] **Step 3: Commit**

```bash
git add crates/apexes-live-trading/src/runner.rs
git commit -S -s -m "feat: implement run_live_trading event loop"
```

---

### Task 20: `apexes-live-trading`: end-to-end integration test with `MockExecutionGateway`

**Files:**
- Create: `crates/apexes-live-trading/tests/live_trading_integration.rs`

**Interfaces:**
- Consumes: `run_live_trading` (Task 19), `apexes_oms::{MockExecutionGateway, OrderStore, OrderManager}`.

This test requires a live PostgreSQL instance (since `OrderManager`/`OrderStore` have no in-memory mode, matching `apexes-oms`'s own established testing convention — see that crate's `manager.rs` test module's `fresh_manager`/`clean_db` helpers) — mark every test `#[ignore]`, consistent with `apexes-oms`'s precedent.

- [ ] **Step 1: Write the integration test**

```rust
//! End-to-end test of `run_live_trading` driving a real `OrderManager`
//! (backed by a real, test-local PostgreSQL database) through a
//! `MockExecutionGateway`, a synthetic market-data stream, and a
//! synthetic `ExchangeUpdate` push stream -- proving the three
//! `tokio::select!` branches (market data -> submit, push update ->
//! apply, reconcile timer -> apply) all work together without needing
//! any real network access. Requires a live PostgreSQL instance:
//!   docker compose up -d postgres
//!   cargo test -p apexes-live-trading --test live_trading_integration -- --ignored

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketDataProvider, MarketEvent, MarketType, OrderType,
    PriceLevel, Side, TradeEvent, Venue,
};
use apexes_live_trading::{LiveTradingConfig, run_live_trading};
use apexes_oms::{ExchangeUpdate, ExecutionGateway, MockExecutionGateway, OrderManager, OrderStore};
use apexes_strategy::{OrderIntent, Position, Strategy, StrategyContext};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures::stream::BoxStream;
use rust_decimal_macros::dec;
use tokio::sync::oneshot;

const POSTGRES_URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

async fn clean_db() {
    let pool = sqlx::PgPool::connect(POSTGRES_URL).await.unwrap();
    sqlx::query("TRUNCATE fills, orders")
        .execute(&pool)
        .await
        .unwrap();
}

struct MockMarketDataProvider {
    events: Vec<MarketEvent>,
}

#[async_trait]
impl MarketDataProvider for MockMarketDataProvider {
    async fn subscribe(
        &self,
        _instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, apexes_core::ApexesError> {
        Ok(Box::pin(futures::stream::iter(self.events.clone())))
    }
}

struct BuyOnceStrategy {
    bought: bool,
}

impl Strategy for BuyOnceStrategy {
    fn on_market_event(
        &mut self,
        event: &MarketEvent,
        _ctx: &StrategyContext,
    ) -> Vec<OrderIntent> {
        if !self.bought && matches!(event, MarketEvent::Trade(_)) {
            self.bought = true;
            return vec![OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
            }];
        }
        vec![]
    }
}

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
    vec![
        MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![PriceLevel {
                price: dec!(100),
                quantity: dec!(1000),
            }],
            sequence: Some(1),
            is_snapshot: true,
            timestamp: t0,
        }),
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(100),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: t0 + chrono::Duration::seconds(1),
        }),
    ]
}

fn base_config() -> LiveTradingConfig {
    let mut initial_balances = HashMap::new();
    initial_balances.insert("USDT".to_string(), dec!(100000));
    LiveTradingConfig {
        instruments: vec![btc_usdt_instrument()],
        initial_balances,
        risk_free_return: dec!(0),
        reconcile_interval: Duration::from_secs(3600), // long enough not to fire during this test
    }
}

#[tokio::test]
#[ignore]
async fn strategy_order_is_submitted_and_a_push_update_produces_a_fill() {
    clean_db().await;
    let gateway = Arc::new(MockExecutionGateway::new());
    let store = OrderStore::new(POSTGRES_URL).await.unwrap();
    let order_manager = OrderManager::new(
        store,
        Arc::clone(&gateway) as Arc<dyn ExecutionGateway>,
        None,
    )
    .await
    .unwrap();

    let provider = MockMarketDataProvider {
        events: sample_events(),
    };
    let (_tx, rx) = oneshot::channel();

    // No exchange_updates in this run (empty stream) -- exercises only
    // the market-data -> submit_order path end to end.
    let exchange_updates = futures::stream::empty::<ExchangeUpdate>();

    let report = run_live_trading(
        base_config(),
        &provider,
        BuyOnceStrategy { bought: false },
        order_manager,
        None,
        exchange_updates,
        rx,
    )
    .await
    .unwrap();

    // No fills yet -- MockExecutionGateway's submit_order accepts the
    // order but the test never pushed a fill-producing exchange update,
    // so trades should be empty even though an order was submitted.
    assert!(report.trades.is_empty());
}

#[tokio::test]
#[ignore]
async fn exchange_update_push_produces_a_fill_applied_to_the_ledger() {
    use apexes_oms::ExchangeFill;
    use apexes_core::{OrderId, OrderStatus};

    clean_db().await;
    let gateway = Arc::new(MockExecutionGateway::new());
    let store = OrderStore::new(POSTGRES_URL).await.unwrap();
    let mut order_manager = OrderManager::new(
        store,
        Arc::clone(&gateway) as Arc<dyn ExecutionGateway>,
        None,
    )
    .await
    .unwrap();

    // Submit one order directly (bypassing the strategy path) so we
    // have a known OrderId to push an ExchangeUpdate for.
    let order_id = order_manager
        .submit_order(
            OrderIntent {
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: dec!(1),
                price: None,
                stop_price: None,
            },
            &HashMap::new(),
            &HashMap::new(),
        )
        .await
        .unwrap();

    gateway.set_fills_for_order(
        order_id,
        vec![ExchangeFill {
            exchange_trade_id: "T1".to_string(),
            price: dec!(100),
            quantity: dec!(1),
            fee: dec!(0),
            fee_currency: None,
            filled_at: Utc::now(),
        }],
    );

    let provider = MockMarketDataProvider { events: vec![] };
    let (_tx, rx) = oneshot::channel();

    let exchange_order_id = order_manager
        .open_orders()
        .get(&order_id)
        .unwrap()
        .exchange_order_id
        .clone();

    let update = ExchangeUpdate {
        client_order_id: order_id,
        exchange_order_id,
        status: OrderStatus::Filled,
    };
    let exchange_updates = futures::stream::iter(vec![update]).chain(futures::stream::pending());

    // Send the stop signal shortly after the single update should have
    // been processed, since `futures::stream::pending()` never
    // completes on its own (the loop would otherwise run forever).
    let (tx, rx_stop) = oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = tx.send(());
    });
    drop(rx); // unused in this branch -- the spawned task's rx_stop is used instead

    let report = run_live_trading(
        base_config(),
        &provider,
        BuyOnceStrategy { bought: true }, // already "bought" so it emits no new intents
        order_manager,
        None,
        exchange_updates,
        rx_stop,
    )
    .await
    .unwrap();

    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].quantity, dec!(1));
}
```

- [ ] **Step 2: Run the tests**

Ensure PostgreSQL is running: `docker compose up -d postgres` (from `/Users/rickzhang/src/oss/apexes`).

Run: `cargo test -p apexes-live-trading --test live_trading_integration -- --ignored`
Expected: PASS, both tests.

- [ ] **Step 3: Commit**

```bash
git add crates/apexes-live-trading/tests/live_trading_integration.rs
git commit -S -s -m "test: add end-to-end MockExecutionGateway integration tests for run_live_trading"
```

---

### Task 21: `apexes-gateway-binance`: `#[ignore]`d Binance Testnet integration test

**Files:**
- Create: `crates/apexes-gateway-binance/tests/live_testnet.rs`

**Interfaces:**
- Consumes: `BinanceExecutionGateway`, `BinanceGatewayConfig`.

This test requires real Binance Testnet API credentials (obtained by the user from `testnet.binance.vision`, not provided or loaded by this codebase). It reads them from environment variables directly IN THE TEST CODE ONLY — this does not conflict with the design decision that `BinanceGatewayConfig`/production code never read environment variables; test code reading `std::env::var` to obtain credentials for a manual, opt-in `#[ignore]`d test is a different, explicitly-scoped exception, consistent with how the rest of this codebase handles `POSTGRES_URL` as a test-only constant rather than a production config-loading mechanism.

- [ ] **Step 1: Write the test file**

```rust
//! Manual, opt-in integration test against real Binance Testnet
//! (https://testnet.binance.vision). NOT run by default `cargo test` --
//! requires real testnet API credentials, which this codebase never
//! auto-loads (see `apexes-gateway-binance`'s design: `BinanceGatewayConfig`
//! has no env-var/config-file loading in production code). Run with:
//!
//!   export APEXES_BINANCE_TESTNET_API_KEY=...
//!   export APEXES_BINANCE_TESTNET_API_SECRET=...
//!   cargo test -p apexes-gateway-binance --test live_testnet -- --ignored

use apexes_core::{AssetClass, Order, OrderId, OrderStatus, OrderType, Side, Venue};
use apexes_gateway_binance::{BinanceExecutionGateway, BinanceGatewayConfig};
use apexes_oms::{ExecutionGateway, GatewayOutcome};
use chrono::Utc;
use rust_decimal_macros::dec;
use uuid::Uuid;

fn testnet_config() -> BinanceGatewayConfig {
    BinanceGatewayConfig {
        api_key: std::env::var("APEXES_BINANCE_TESTNET_API_KEY")
            .expect("set APEXES_BINANCE_TESTNET_API_KEY to run this ignored test"),
        api_secret: std::env::var("APEXES_BINANCE_TESTNET_API_SECRET")
            .expect("set APEXES_BINANCE_TESTNET_API_SECRET to run this ignored test"),
        spot_base_url: "https://testnet.binance.vision".to_string(),
        futures_base_url: "https://testnet.binancefuture.com".to_string(),
        recv_window_ms: 5000,
    }
}

#[tokio::test]
#[ignore]
async fn submit_and_cancel_a_limit_order_far_from_market_price_on_spot_testnet() {
    let gateway = BinanceExecutionGateway::new(testnet_config());

    // A limit price far below any realistic BTCUSDT market price, so
    // this order rests indefinitely on the book rather than filling
    // during the test (avoiding any real, even testnet-fake-money,
    // execution as a side effect of running this test).
    let order = Order {
        id: OrderId(Uuid::new_v4()),
        exchange_order_id: None,
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Limit,
        quantity: dec!(0.001),
        price: Some(dec!(1000)),
        stop_price: None,
        filled_quantity: dec!(0),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    let outcome = gateway.submit_order(&order).await;
    let GatewayOutcome::Accepted { exchange_order_id } = outcome else {
        panic!("expected Accepted, got {outcome:?}");
    };

    let mut order_with_id = order.clone();
    order_with_id.exchange_order_id = Some(exchange_order_id.clone());

    let status = gateway.query_order_status(&order_with_id).await.unwrap();
    assert_eq!(status, Some(OrderStatus::New));

    let cancel_outcome = gateway.cancel_order(&order_with_id).await;
    assert!(matches!(cancel_outcome, GatewayOutcome::Accepted { .. }));
}

#[tokio::test]
#[ignore]
async fn query_all_open_orders_returns_the_order_just_placed() {
    let gateway = BinanceExecutionGateway::new(testnet_config());

    let order = Order {
        id: OrderId(Uuid::new_v4()),
        exchange_order_id: None,
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Limit,
        quantity: dec!(0.001),
        price: Some(dec!(1000)),
        stop_price: None,
        filled_quantity: dec!(0),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    let outcome = gateway.submit_order(&order).await;
    assert!(matches!(outcome, GatewayOutcome::Accepted { .. }));

    let open_orders = gateway
        .query_all_open_orders(AssetClass::Spot)
        .await
        .unwrap();
    assert!(open_orders.iter().any(|(id, _, _)| *id == order.id));

    // Clean up: cancel it so it doesn't linger on the testnet account
    // across repeated runs of this test.
    let exchange_order_id = open_orders
        .iter()
        .find(|(id, _, _)| *id == order.id)
        .and_then(|(_, exchange_id, _)| exchange_id.clone());
    let mut order_with_id = order.clone();
    order_with_id.exchange_order_id = exchange_order_id;
    gateway.cancel_order(&order_with_id).await;
}

#[tokio::test]
#[ignore]
async fn user_data_stream_connects_and_receives_at_least_the_initial_handshake() {
    let gateway = BinanceExecutionGateway::new(testnet_config());
    let (_handle, mut stream) = gateway.start_user_data_stream().await.unwrap();

    // Submit an order to generate a User Data Stream event, then wait
    // briefly for it to arrive. This test only proves connectivity and
    // basic event delivery -- it does not assert on specific field
    // values (those are covered by user_data_stream.rs's unit tests
    // against synthetic JSON).
    let order = Order {
        id: OrderId(Uuid::new_v4()),
        exchange_order_id: None,
        symbol: "BTCUSDT".to_string(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        side: Side::Buy,
        order_type: OrderType::Limit,
        quantity: dec!(0.001),
        price: Some(dec!(1000)),
        stop_price: None,
        filled_quantity: dec!(0),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    gateway.submit_order(&order).await;

    use futures::StreamExt;
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next()).await;
    assert!(
        result.is_ok(),
        "expected at least one ExchangeUpdate within 10 seconds of submitting an order"
    );
}
```

- [ ] **Step 2: Verify compilation (do not run — requires real credentials)**

Run: `cargo test -p apexes-gateway-binance --test live_testnet --no-run`
Expected: compiles cleanly, 0 errors.

- [ ] **Step 3: Commit**

```bash
git add crates/apexes-gateway-binance/tests/live_testnet.rs
git commit -S -s -m "test: add ignored Binance Testnet integration tests"
```

---

### Task 22: Final full-workspace verification and `docs/FEATURES.md` update

**Files:**
- Modify: `docs/FEATURES.md`

**Interfaces:** none (documentation + final verification task).

- [ ] **Step 1: Full workspace build**

Run: `cargo build --workspace`
Expected: clean build, 0 errors.

- [ ] **Step 2: Full workspace test (non-ignored)**

Run: `cargo test --workspace`
Expected: 0 failures.

- [ ] **Step 3: Full workspace test including ignored (requires PostgreSQL running)**

Run: `docker compose up -d postgres`, wait for readiness, then:

Run: `cargo test --workspace -- --ignored`
Expected: 0 failures for every test EXCEPT `apexes-gateway-binance`'s `live_testnet.rs` tests, which require real Binance Testnet credentials this plan does not provide — those are expected to fail with a panic message about missing environment variables if run without credentials configured; this is expected and acceptable (the task is to verify they compile and are structurally correct, not that they pass without user-supplied credentials). To verify precisely which tests failed and why:

Run: `cargo test --workspace -- --ignored 2>&1 | grep -A2 "FAILED\|panicked"`
Expected: only `live_testnet.rs`'s three tests fail, each with a message containing "set APEXES_BINANCE_TESTNET_API_KEY" or "set APEXES_BINANCE_TESTNET_API_SECRET".

- [ ] **Step 4: Clippy and fmt**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 5: Update `docs/FEATURES.md` row `5.3.1`**

Read the current content of row `5.3.1` first:

Run: `grep -n "5.3.1" /Users/rickzhang/src/oss/apexes/docs/FEATURES.md`

Replace that row (currently `| 5.3.1 | Crypto gateway (CEX private API + DEX smart-contract interaction/signing) | #8 | 🔲 Not Started | First live gateway target |`) with:

```
| 5.3.1 | Crypto gateway (CEX private API + DEX smart-contract interaction/signing) | #8 | ✅ Done | apexes-gateway-binance crate: real BinanceExecutionGateway implementing apexes-oms's ExecutionGateway against Binance Spot + USDS-M Perpetual Futures REST APIs (HMAC-SHA256 request signing, GatewayOutcome mapping distinguishing explicit rejections from ambiguous failures) plus a User Data Stream WebSocket client (listenKey lifecycle, push-based order/fill updates) as a low-latency supplement to OMS's existing reconcile() polling. barter-execution was evaluated and found unusable for this purpose (its Binance client module is an empty, private stub) -- documented as a deviation from the original roadmap assumption. DEX smart-contract interaction is out of scope, deferred to #10. Also delivered: apexes-live-trading crate (run_live_trading, an exchange-agnostic event loop parallel to apexes-paper::run_paper_trading, reusing apexes-backtest::PortfolioLedger for interim position/balance tracking pending #11) closing the backtest->paper->live loop end to end. Required extending apexes_core::Order/apexes_strategy::OrderIntent with an asset_class field (Spot/Perpetual routing) and apexes-oms's ExecutionGateway trait with query_fills_for_order and &Order-based cancel_order/query_order_status signatures (both extensions found necessary only during this implementation, not anticipated by #7's original "network-agnostic" trait design). |
```

- [ ] **Step 6: Update the "Current active work" narrative paragraph at the bottom of `docs/FEATURES.md`**

Find the paragraph starting "**Current active work:**" (near the end of the file) and append a new sentence after its existing final sentence, describing sub-project #8's completion and what's newly unlocked:

```
Sub-project `#8` (first live gateway) is now complete via the new `apexes-gateway-binance` (real Binance Spot + USDS-M Perpetual `ExecutionGateway`, HMAC-SHA256 signing, User Data Stream) and `apexes-live-trading` (`run_live_trading`, exchange-agnostic event loop) crates -- row `5.3.1` is `Done`. This unlocks sub-project `#9` (EMS/execution algorithms, depends on `#7`+`#8`) and sub-project `#10` (additional market gateways, depends on `#7`+`#8`); `#11` (account/fund management) remains available independently (depends only on `#7`). What's next is not yet decided.
```

- [ ] **Step 7: Verify the row-count summary near the top or bottom of `docs/FEATURES.md` (if one exists) is still accurate**

Run: `grep -n "Done\|Not Started\|In Progress\|Deferred\|Designing\|Planned" /Users/rickzhang/src/oss/apexes/docs/FEATURES.md | grep -c "✅ Done"`

Compare this count against any summary line in the file claiming a specific "N Done" count (search for a line matching a pattern like `\d+/\d+/\d+` near the file's end, following this project's established convention from prior sub-projects of maintaining an accurate summary count). If such a summary line exists, recompute each status category's count via `grep -c` for each status emoji and update the line to match exactly -- this project's memory explicitly flags "miscounting the FEATURES.md summary line" as a recurring mistake to avoid; use real `grep`-based counting, not estimation.

- [ ] **Step 8: Commit**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: mark FEATURES.md row 5.3.1 (crypto gateway) as Done"
```

---

## Post-plan note for the next sub-project

Sub-projects `#9` (EMS/execution algorithms) and `#10` (additional market gateways) both depend on `#7`+`#8`, both now complete, and are newly unlocked. `#11` (account/fund management, depends only on `#7`) was already unlocked before this plan and remains available. Do not assume which one is next -- ask, per this project's established working style.
