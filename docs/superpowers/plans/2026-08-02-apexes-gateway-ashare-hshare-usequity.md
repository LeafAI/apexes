# Apexes: Additional Market Gateways Implementation Plan (`docs/FEATURES.md` rows `5.3.2`/`5.3.3`)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the design in `docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md`: two new crates, `apexes-gateway-xtp` (A-share, protocol-mapping-only placeholder pending real XTP SDK/account access) and `apexes-gateway-ibkr` (H-share/US-equity, full IBKR TWS API protocol implementation validated against a hand-written mock TCP server, never connected to a real TWS/IB Gateway process) — completing roadmap sub-project `#10`'s rows `5.3.2`/`5.3.3` to the honestly-scoped completion level the design spec establishes.

**Architecture:** Nine sequential tasks in dependency order. Tasks 1-3 build `apexes-gateway-xtp` (config, `ExecutionGateway` impl with real request-mapping logic but `Uncertain`-returning stubs, `MarketDataProvider` pure placeholder). Tasks 4-7 build `apexes-gateway-ibkr` (config, wire protocol codec, `ExecutionGateway` impl with a real TCP client validated against a hand-written mock TWS server, `MarketDataProvider` at the same completion level). Task 8 is a full-workspace verification checkpoint. Task 9 is the final whole-branch review plus the `docs/FEATURES.md` update.

**Tech Stack:** Rust 2024, existing workspace deps only (`apexes-core`, `apexes-oms`, `async-trait`, `futures`, `tokio`). No new third-party dependencies — no protobuf/prost, no XTP/IBKR SDK crates.

## Global Constraints

- Rust edition 2024, workspace-wide `cargo check`/`cargo clippy --all-targets -- -D warnings`/`cargo fmt --check` must stay clean after every task.
- No `unsafe` blocks. Never bare `.unwrap()`/`.expect()` in production code (test code may use them freely).
- Every commit signed: `git commit -S -s`.
- **No panics, no `unimplemented!()`, no silent success on any placeholder/incomplete code path.** Every method that cannot yet do real work (because it depends on a closed-source SDK binary or a live TWS/IB Gateway process this plan has no access to) MUST return an explicit, documented `GatewayOutcome::Uncertain` or `Result::Err` describing exactly what's missing and why — never a bare `todo!()`, never a fabricated success result. This is the plan's single most important behavioral invariant; every task's self-review must check it.
- **Do not modify `apexes_oms::ExecutionGateway` or `apexes_core::MarketDataProvider`.** Both existing traits are reused exactly as-is; all protocol-specific behavior lives inside the new crates' own implementations.
- **Do not attempt to compile-link any real XTP SDK binary (`.so`/`.dll`) or connect to a real TWS/IB Gateway process anywhere in this plan.** Both are explicitly out of scope pending real account access (see design spec §一 "背景与关键决策"). `apexes-gateway-xtp`'s completion level is intentionally lower than `apexes-gateway-ibkr`'s — this asymmetry is a deliberate, spec-approved design decision reflecting real protocol-openness differences, not an inconsistency to "fix."
- `Venue::{Sse, Szse, Hkex, Nasdaq, Nyse}` and `MarketType::{AShare, HShare, UsEquity}` already exist in `apexes-core` (since sub-project `#0`) — do not add new `Venue`/`MarketType` variants; this plan only adds new crates that route existing venues to XTP/IBKR.
- Full design rationale (why CTP/PTrade/Futu/Longbridge were rejected as implementation targets, why XTP and IBKR have different completion levels, the full account-access research trail) is in the design spec — read it before starting if anything below is unclear on *why*, not just *what*.

---

### Task 1: `apexes-gateway-xtp` crate scaffold — `XtpGatewayConfig`, request-shape types

**Files:**
- Create: `crates/apexes-gateway-xtp/Cargo.toml`
- Create: `crates/apexes-gateway-xtp/src/lib.rs`
- Create: `crates/apexes-gateway-xtp/src/config.rs`
- Create: `crates/apexes-gateway-xtp/src/types.rs`
- Modify: `Cargo.toml` (workspace members)

**Interfaces:**
- Consumes: nothing new (pure new types).
- Produces: `apexes_gateway_xtp::{XtpGatewayConfig, XtpOrderInsertRequest, XtpMarket, XtpSide, XtpPriceType}`.

- [ ] **Step 1: Add the crate to the workspace**

```toml
# Cargo.toml -- add "crates/apexes-gateway-xtp" to the [workspace] members list
```

- [ ] **Step 2: Write `Cargo.toml` for the new crate**

```toml
# crates/apexes-gateway-xtp/Cargo.toml (new file)
[package]
name = "apexes-gateway-xtp"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-oms = { path = "../apexes-oms" }
async-trait = { workspace = true }
futures = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
chrono = { workspace = true }
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
uuid = { workspace = true, features = ["v4"] }
```

- [ ] **Step 3: Write the failing tests for the request-shape types**

```rust
// crates/apexes-gateway-xtp/src/types.rs (new file)
//! XTP protocol request-shape types. Field names/structure derived
//! from the publicly-maintained `ztsec/xtp_api_java` GitHub repository
//! (the only publicly accessible XTP protocol reference found without
//! a real account -- see
//! `docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md`
//! §一 for the full research trail). These types may need correction
//! once real account access is available.

/// Which Shanghai/Shenzhen A-share market a symbol trades on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XtpMarket {
    ShanghaiA,
    ShenzhenA,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XtpSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XtpPriceType {
    Limit,
    Market,
}

/// The XTP order-insert request shape (field names per
/// `ztsec/xtp_api_java`'s public `doc`/`xtp_api_demo` directories).
#[derive(Debug, Clone, PartialEq)]
pub struct XtpOrderInsertRequest {
    pub ticker: String,
    pub market: XtpMarket,
    pub side: XtpSide,
    pub price_type: XtpPriceType,
    pub price: f64,
    pub quantity: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xtp_market_variants_are_distinct() {
        assert_ne!(XtpMarket::ShanghaiA, XtpMarket::ShenzhenA);
    }

    #[test]
    fn xtp_side_variants_are_distinct() {
        assert_ne!(XtpSide::Buy, XtpSide::Sell);
    }

    #[test]
    fn xtp_price_type_variants_are_distinct() {
        assert_ne!(XtpPriceType::Limit, XtpPriceType::Market);
    }

    #[test]
    fn xtp_order_insert_request_is_constructible_and_comparable() {
        let a = XtpOrderInsertRequest {
            ticker: "600000".to_string(),
            market: XtpMarket::ShanghaiA,
            side: XtpSide::Buy,
            price_type: XtpPriceType::Limit,
            price: 10.5,
            quantity: 100,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }
}
```

Run: `cargo test -p apexes-gateway-xtp`
Expected: FAIL (crate not yet registered in `lib.rs`) -- expected; Step 5 registers it.

- [ ] **Step 4: Write `XtpGatewayConfig`**

```rust
// crates/apexes-gateway-xtp/src/config.rs (new file)
//! [`XtpGatewayConfig`]: configuration for [`crate::gateway::XtpExecutionGateway`].
//! Field names are best-effort inferred from the publicly-available
//! `ztsec/xtp_api_java` documentation -- account_key in particular is
//! a placeholder name that may not match XTP's actual credential
//! terminology once real account access is available. This crate
//! never reads `std::env` in production code, matching
//! `apexes-gateway-binance`'s established convention.

#[derive(Debug, Clone)]
pub struct XtpGatewayConfig {
    pub client_id: u8,
    pub account_key: String,
    pub trade_server_ip: String,
    pub trade_server_port: u16,
    pub quote_server_ip: String,
    pub quote_server_port: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_is_constructible_and_cloneable() {
        let config = XtpGatewayConfig {
            client_id: 1,
            account_key: "test-key".to_string(),
            trade_server_ip: "127.0.0.1".to_string(),
            trade_server_port: 6001,
            quote_server_ip: "127.0.0.1".to_string(),
            quote_server_port: 6002,
        };
        let cloned = config.clone();
        assert_eq!(config.client_id, cloned.client_id);
        assert_eq!(config.account_key, cloned.account_key);
    }
}
```

- [ ] **Step 5: Register the modules**

```rust
// crates/apexes-gateway-xtp/src/lib.rs (new file)
//! Apexes A-share gateway (XTP protocol). Covers `docs/FEATURES.md`
//! row 5.3.2. **Completion level: protocol-mapping-only placeholder**
//! -- the real XTP SDK is a closed-source binary requiring a real
//! account to download; every `ExecutionGateway`/`MarketDataProvider`
//! method in this crate returns an explicit `Uncertain`/`Err` result
//! rather than performing real network I/O. See
//! `docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md`
//! for the full scope rationale.

pub mod config;
pub mod types;

pub use config::XtpGatewayConfig;
pub use types::{XtpMarket, XtpOrderInsertRequest, XtpPriceType, XtpSide};
```

- [ ] **Step 6: Run this crate's tests**

Run: `cargo test -p apexes-gateway-xtp`
Expected: all 5 tests pass (4 from `types.rs` + 1 from `config.rs`).

- [ ] **Step 7: Run clippy, fmt, then a workspace-wide compile check**

Run: `cargo clippy -p apexes-gateway-xtp --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/apexes-gateway-xtp
git commit -S -s -m "feat(apexes-gateway-xtp): new crate skeleton -- XtpGatewayConfig, protocol request-shape types"
```

---

### Task 2: `apexes-gateway-xtp`: `XtpExecutionGateway`

**Files:**
- Create: `crates/apexes-gateway-xtp/src/gateway.rs`
- Modify: `crates/apexes-gateway-xtp/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_oms::{ExecutionGateway, GatewayOutcome, ExchangeFill}` (existing); `apexes_core::{Order, OrderId, OrderStatus, AssetClass, Side, OrderType, Venue}` (existing); `XtpGatewayConfig`, `XtpOrderInsertRequest`, `XtpMarket`, `XtpSide`, `XtpPriceType` (Task 1).
- Produces: `apexes_gateway_xtp::XtpExecutionGateway`.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/apexes-gateway-xtp/src/gateway.rs (new file)
//! [`XtpExecutionGateway`]: implements `apexes_oms::ExecutionGateway`
//! against the XTP protocol. Every method performs real, complete
//! local parameter validation and Order -> XTP-request-shape mapping,
//! then returns `GatewayOutcome::Uncertain` (never a panic, never
//! `unimplemented!()`, never a silent success) because the actual XTP
//! SDK binary has never been linked -- see this crate's `lib.rs` docs
//! for the full rationale.

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, MarginMode, Order, OrderId, OrderStatus, OrderType, Side, Venue};
    use apexes_oms::{ExecutionGateway, GatewayOutcome};
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn sample_config() -> XtpGatewayConfig {
        XtpGatewayConfig {
            client_id: 1,
            account_key: "test-key".to_string(),
            trade_server_ip: "127.0.0.1".to_string(),
            trade_server_port: 6001,
            quote_server_ip: "127.0.0.1".to_string(),
            quote_server_port: 6002,
        }
    }

    fn sample_order(venue: Venue, order_type: OrderType, side: Side) -> Order {
        Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "600000".to_string(),
            venue,
            asset_class: AssetClass::Equity,
            side,
            order_type,
            quantity: dec!(100),
            price: Some(dec!(10.5)),
            stop_price: None,
            leverage: None,
            margin_mode: None,
            filled_quantity: dec!(0),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn build_insert_request_maps_sse_buy_limit_correctly() {
        let order = sample_order(Venue::Sse, OrderType::Limit, Side::Buy);
        let request = XtpExecutionGateway::build_insert_request(&order).unwrap();
        assert_eq!(request.ticker, "600000");
        assert_eq!(request.market, XtpMarket::ShanghaiA);
        assert_eq!(request.side, XtpSide::Buy);
        assert_eq!(request.price_type, XtpPriceType::Limit);
        assert_eq!(request.price, 10.5);
        assert_eq!(request.quantity, 100);
    }

    #[test]
    fn build_insert_request_maps_szse_sell_market_correctly() {
        let order = sample_order(Venue::Szse, OrderType::Market, Side::Sell);
        let request = XtpExecutionGateway::build_insert_request(&order).unwrap();
        assert_eq!(request.market, XtpMarket::ShenzhenA);
        assert_eq!(request.side, XtpSide::Sell);
        assert_eq!(request.price_type, XtpPriceType::Market);
    }

    #[test]
    fn build_insert_request_rejects_unsupported_venue() {
        let order = sample_order(Venue::Binance, OrderType::Limit, Side::Buy);
        let result = XtpExecutionGateway::build_insert_request(&order);
        assert!(result.is_err());
    }

    #[test]
    fn build_insert_request_rejects_unsupported_order_type() {
        let order = sample_order(Venue::Sse, OrderType::Stop, Side::Buy);
        let result = XtpExecutionGateway::build_insert_request(&order);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn submit_order_returns_uncertain_for_a_valid_order() {
        let gateway = XtpExecutionGateway::new(sample_config());
        let order = sample_order(Venue::Sse, OrderType::Limit, Side::Buy);
        let outcome = gateway.submit_order(&order).await;
        assert!(matches!(outcome, GatewayOutcome::Uncertain { .. }));
    }

    #[tokio::test]
    async fn submit_order_returns_rejected_for_an_unsupported_venue() {
        let gateway = XtpExecutionGateway::new(sample_config());
        let order = sample_order(Venue::Binance, OrderType::Limit, Side::Buy);
        let outcome = gateway.submit_order(&order).await;
        assert!(matches!(outcome, GatewayOutcome::Rejected { .. }));
    }

    #[tokio::test]
    async fn cancel_order_returns_uncertain() {
        let gateway = XtpExecutionGateway::new(sample_config());
        let order = sample_order(Venue::Sse, OrderType::Limit, Side::Buy);
        let outcome = gateway.cancel_order(&order).await;
        assert!(matches!(outcome, GatewayOutcome::Uncertain { .. }));
    }

    #[tokio::test]
    async fn query_order_status_returns_err() {
        let gateway = XtpExecutionGateway::new(sample_config());
        let order = sample_order(Venue::Sse, OrderType::Limit, Side::Buy);
        let result = gateway.query_order_status(&order).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn query_all_open_orders_returns_err() {
        let gateway = XtpExecutionGateway::new(sample_config());
        let result = gateway.query_all_open_orders(AssetClass::Equity).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn query_fills_for_order_returns_err() {
        let gateway = XtpExecutionGateway::new(sample_config());
        let order = sample_order(Venue::Sse, OrderType::Limit, Side::Buy);
        let result = gateway.query_fills_for_order(&order).await;
        assert!(result.is_err());
    }

    #[allow(unused_imports)]
    use apexes_core::MarginMode as _MarginModeUnusedImportGuard;
}
```

Note: the trailing `#[allow(unused_imports)] use apexes_core::MarginMode as _MarginModeUnusedImportGuard;` line exists only because `MarginMode` is imported above for potential future use in `sample_order` but not read in this file's test bodies -- remove this line if `cargo clippy` does not flag an unused import in your actual run (clippy's exact warning depends on whether `sample_order`'s `margin_mode: None` field literal counts as a "use" of the imported name; verify with your own `cargo clippy` run in Step 6 below and delete this guard line if it is not needed).

Run: `cargo test -p apexes-gateway-xtp gateway`
Expected: FAIL with "cannot find function `build_insert_request`" / "cannot find type `XtpExecutionGateway`" -- expected; Step 2 implements it.

- [ ] **Step 2: Implement `XtpExecutionGateway`**

```rust
// crates/apexes-gateway-xtp/src/gateway.rs -- add above the
// #[cfg(test)] block:

use apexes_core::{AssetClass, Order, OrderId, OrderStatus};
use apexes_oms::{ExchangeFill, ExecutionGateway, GatewayOutcome};
use async_trait::async_trait;

use crate::config::XtpGatewayConfig;
use crate::types::{XtpMarket, XtpOrderInsertRequest, XtpPriceType, XtpSide};

pub struct XtpExecutionGateway {
    #[allow(dead_code)]
    config: XtpGatewayConfig,
}

impl XtpExecutionGateway {
    pub fn new(config: XtpGatewayConfig) -> Self {
        Self { config }
    }

    /// Real Order -> XTP protocol request-shape mapping, kept as an
    /// associated function (not requiring `&self`) so it's directly
    /// unit-testable without constructing a gateway instance.
    fn build_insert_request(order: &Order) -> Result<XtpOrderInsertRequest, String> {
        let market = match order.venue {
            apexes_core::Venue::Sse => XtpMarket::ShanghaiA,
            apexes_core::Venue::Szse => XtpMarket::ShenzhenA,
            other => return Err(format!("XTP gateway does not support venue {other:?}")),
        };
        let side = match order.side {
            apexes_core::Side::Buy => XtpSide::Buy,
            apexes_core::Side::Sell => XtpSide::Sell,
        };
        let price_type = match order.order_type {
            apexes_core::OrderType::Limit => XtpPriceType::Limit,
            apexes_core::OrderType::Market => XtpPriceType::Market,
            other => {
                return Err(format!("XTP gateway does not support order type {other:?}"));
            }
        };
        let price = order
            .price
            .and_then(|p| p.to_string().parse::<f64>().ok())
            .unwrap_or(0.0);
        let quantity = order.quantity.to_string().parse::<i64>().unwrap_or(0);
        Ok(XtpOrderInsertRequest {
            ticker: order.symbol.clone(),
            market,
            side,
            price_type,
            price,
            quantity,
        })
    }

    fn not_yet_linked_reason() -> String {
        "XTP SDK not yet linked -- pending real account access; see \
         docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md \
         §3 for scope"
            .to_string()
    }
}

#[async_trait]
impl ExecutionGateway for XtpExecutionGateway {
    async fn submit_order(&self, order: &Order) -> GatewayOutcome {
        match Self::build_insert_request(order) {
            Ok(_request) => GatewayOutcome::Uncertain {
                reason: Self::not_yet_linked_reason(),
            },
            Err(reason) => GatewayOutcome::Rejected { reason },
        }
    }

    async fn cancel_order(&self, _order: &Order) -> GatewayOutcome {
        GatewayOutcome::Uncertain {
            reason: Self::not_yet_linked_reason(),
        }
    }

    async fn query_order_status(&self, _order: &Order) -> Result<Option<OrderStatus>, String> {
        Err(Self::not_yet_linked_reason())
    }

    async fn query_all_open_orders(
        &self,
        _asset_class: AssetClass,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
        Err(Self::not_yet_linked_reason())
    }

    async fn query_fills_for_order(&self, _order: &Order) -> Result<Vec<ExchangeFill>, String> {
        Err(Self::not_yet_linked_reason())
    }
}
```

- [ ] **Step 3: Register the module**

```rust
// crates/apexes-gateway-xtp/src/lib.rs -- add:
pub mod gateway;
pub use gateway::XtpExecutionGateway;
```

- [ ] **Step 4: Run the new tests**

Run: `cargo test -p apexes-gateway-xtp gateway`
Expected: all 8 pass.

- [ ] **Step 5: Run this crate's tests, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-gateway-xtp`
Expected: all pass (5 from Task 1 + 8 from this task).

Run: `cargo clippy -p apexes-gateway-xtp --all-targets -- -D warnings`
Expected: 0 warnings. If clippy flags the `MarginMode` import as unused in the test module, remove the `#[allow(unused_imports)] use apexes_core::MarginMode as _MarginModeUnusedImportGuard;` line and the `MarginMode` import from the test's `use` statement entirely.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-gateway-xtp/src/gateway.rs crates/apexes-gateway-xtp/src/lib.rs
git commit -S -s -m "feat(apexes-gateway-xtp): XtpExecutionGateway -- real request mapping, Uncertain-returning stubs pending SDK access"
```

---

### Task 3: `apexes-gateway-xtp`: `XtpMarketDataProvider`

**Files:**
- Create: `crates/apexes-gateway-xtp/src/market_data.rs`
- Modify: `crates/apexes-gateway-xtp/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_core::{MarketDataProvider, Instrument, MarketEvent, ApexesError, Venue}` (existing).
- Produces: `apexes_gateway_xtp::XtpMarketDataProvider`.

- [ ] **Step 1: Write the failing test**

```rust
// crates/apexes-gateway-xtp/src/market_data.rs (new file)
//! [`XtpMarketDataProvider`]: pure placeholder implementation of
//! `apexes_core::MarketDataProvider` for XTP market data. Lower
//! completion level than `XtpExecutionGateway` -- XTP market data
//! subscription depends on the same unlinked closed-source SDK, and
//! unlike order submission there is no local request-shape mapping
//! worth doing ahead of time (a subscription request has no
//! meaningful "rejected vs uncertain" distinction the way an order
//! does), so this always returns `Err` directly.

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, Instrument, MarketDataProvider, MarketType, Venue};
    use rust_decimal_macros::dec;

    fn sample_instrument() -> Instrument {
        Instrument::new(
            "600000",
            MarketType::AShare,
            Venue::Sse,
            AssetClass::Equity,
            None,
            Some("CNY".to_string()),
            dec!(0.01),
            dec!(100),
            dec!(1),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn subscribe_returns_an_explicit_error() {
        let provider = XtpMarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_error_mentions_pending_account_access() {
        let provider = XtpMarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        let error = result.unwrap_err();
        assert!(error.to_string().contains("pending account access"));
    }
}
```

Run: `cargo test -p apexes-gateway-xtp market_data`
Expected: FAIL with "cannot find type `XtpMarketDataProvider`" -- expected; Step 2 implements it.

- [ ] **Step 2: Implement `XtpMarketDataProvider`**

```rust
// crates/apexes-gateway-xtp/src/market_data.rs -- add above the
// #[cfg(test)] block:

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::BoxStream;

pub struct XtpMarketDataProvider;

#[async_trait]
impl MarketDataProvider for XtpMarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let symbol = instruments
            .first()
            .map(|instrument| instrument.symbol.clone())
            .unwrap_or_else(|| "*".to_string());
        Err(ApexesError::SubscriptionFailed {
            symbol,
            venue: Venue::Sse,
            reason: "XTP market data not yet implemented -- pending account access".to_string(),
            source_detail: None,
        })
    }
}
```

- [ ] **Step 3: Register the module**

```rust
// crates/apexes-gateway-xtp/src/lib.rs -- add:
pub mod market_data;
pub use market_data::XtpMarketDataProvider;
```

- [ ] **Step 4: Run the new tests**

Run: `cargo test -p apexes-gateway-xtp market_data`
Expected: both pass.

- [ ] **Step 5: Run this crate's tests, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-gateway-xtp`
Expected: all pass (13 from Tasks 1-2 + 2 from this task = 15).

Run: `cargo clippy -p apexes-gateway-xtp --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-gateway-xtp/src/market_data.rs crates/apexes-gateway-xtp/src/lib.rs
git commit -S -s -m "feat(apexes-gateway-xtp): XtpMarketDataProvider -- pure placeholder pending SDK access"
```

---

### Task 4: `apexes-gateway-ibkr` crate scaffold — `IbkrGatewayConfig`, wire protocol codec

**Files:**
- Create: `crates/apexes-gateway-ibkr/Cargo.toml`
- Create: `crates/apexes-gateway-ibkr/src/lib.rs`
- Create: `crates/apexes-gateway-ibkr/src/config.rs`
- Create: `crates/apexes-gateway-ibkr/src/protocol.rs`
- Modify: `Cargo.toml` (workspace members)

**Interfaces:**
- Consumes: nothing new (pure new types + a pure codec).
- Produces: `apexes_gateway_ibkr::{IbkrGatewayConfig, protocol::{encode_message, decode_message}}`.

- [ ] **Step 1: Add the crate to the workspace**

```toml
# Cargo.toml -- add "crates/apexes-gateway-ibkr" to the [workspace] members list
```

- [ ] **Step 2: Write `Cargo.toml` for the new crate**

```toml
# crates/apexes-gateway-ibkr/Cargo.toml (new file)
[package]
name = "apexes-gateway-ibkr"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-oms = { path = "../apexes-oms" }
async-trait = { workspace = true }
futures = { workspace = true }
tokio = { workspace = true, features = ["net", "sync", "io-util"] }
tracing = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
chrono = { workspace = true }
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
uuid = { workspace = true, features = ["v4"] }
```

- [ ] **Step 3: Write the failing tests for the wire protocol codec**

```rust
// crates/apexes-gateway-ibkr/src/protocol.rs (new file)
//! IBKR TWS API wire protocol: a length-prefixed (4-byte big-endian),
//! null-byte-separated-field text protocol. This format is publicly
//! documented (unlike XTP's closed-source binary SDK) -- see
//! `docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md`
//! §4.2 for the design rationale.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_message_produces_length_prefixed_null_separated_fields() {
        let fields = vec!["PLACE_ORDER".to_string(), "42".to_string()];
        let encoded = encode_message(&fields);

        // 4-byte length prefix.
        let expected_body = b"PLACE_ORDER\042\0";
        let expected_len = expected_body.len() as u32;
        assert_eq!(&encoded[0..4], &expected_len.to_be_bytes());
        assert_eq!(&encoded[4..], expected_body);
    }

    #[test]
    fn decode_message_round_trips_encode_message() {
        let fields = vec!["PLACE_ORDER".to_string(), "600000".to_string(), "BUY".to_string()];
        let encoded = encode_message(&fields);

        let (decoded, consumed) = decode_message(&encoded).unwrap();
        assert_eq!(decoded, fields);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn decode_message_returns_none_for_incomplete_length_prefix() {
        let partial = vec![0u8, 0u8];
        assert!(decode_message(&partial).is_none());
    }

    #[test]
    fn decode_message_returns_none_for_incomplete_body() {
        let fields = vec!["PLACE_ORDER".to_string()];
        let encoded = encode_message(&fields);
        let truncated = &encoded[..encoded.len() - 2];
        assert!(decode_message(truncated).is_none());
    }

    #[test]
    fn decode_message_reports_correct_consumed_length_with_trailing_bytes() {
        let fields = vec!["A".to_string()];
        let encoded = encode_message(&fields);
        let mut buffer = encoded.clone();
        buffer.extend_from_slice(b"extra-trailing-bytes");

        let (decoded, consumed) = decode_message(&buffer).unwrap();
        assert_eq!(decoded, fields);
        assert_eq!(consumed, encoded.len());
    }
}
```

Run: `cargo test -p apexes-gateway-ibkr protocol`
Expected: FAIL with "cannot find function `encode_message`"/"cannot find function `decode_message`" -- expected; Step 4 implements them.

- [ ] **Step 4: Implement the codec**

```rust
// crates/apexes-gateway-ibkr/src/protocol.rs -- add above the
// #[cfg(test)] block:

/// Encodes a list of fields into one IBKR wire-protocol message: each
/// field is written verbatim followed by a single null byte, and the
/// whole resulting body is prefixed with its length as a 4-byte
/// big-endian unsigned integer.
pub fn encode_message(fields: &[String]) -> Vec<u8> {
    let mut body = Vec::new();
    for field in fields {
        body.extend_from_slice(field.as_bytes());
        body.push(0);
    }
    let mut buf = Vec::with_capacity(4 + body.len());
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    buf
}

/// Attempts to decode the next complete message from the front of
/// `buf`. Returns `None` if `buf` does not yet contain a complete
/// message (either the 4-byte length prefix itself is incomplete, or
/// the prefix is present but the declared body length exceeds what's
/// available) -- callers are responsible for buffering additional
/// bytes and retrying. On success, returns the decoded fields (empty
/// trailing fields from the null-byte splitting are filtered out) and
/// the total number of bytes consumed from the front of `buf`
/// (4 + body length), so the caller can advance past exactly this
/// message and leave any trailing bytes (a subsequent message, or a
/// partial one) untouched.
pub fn decode_message(buf: &[u8]) -> Option<(Vec<String>, usize)> {
    if buf.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return None;
    }
    let body = &buf[4..4 + len];
    let fields: Vec<String> = body
        .split(|&b| b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).to_string())
        .collect();
    Some((fields, 4 + len))
}
```

- [ ] **Step 5: Write `IbkrGatewayConfig`**

```rust
// crates/apexes-gateway-ibkr/src/config.rs (new file)
//! [`IbkrGatewayConfig`]: configuration for
//! [`crate::gateway::IbkrExecutionGateway`]. Default port values per
//! IBKR's publicly documented conventions: TWS paper trading 7497,
//! TWS live 7496, IB Gateway paper trading 4002, IB Gateway live 4001.
//! This crate never reads `std::env` in production code, matching
//! `apexes-gateway-binance`'s established convention.

#[derive(Debug, Clone)]
pub struct IbkrGatewayConfig {
    pub host: String,
    pub port: u16,
    pub client_id: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_is_constructible_and_cloneable() {
        let config = IbkrGatewayConfig {
            host: "127.0.0.1".to_string(),
            port: 7497,
            client_id: 1,
        };
        let cloned = config.clone();
        assert_eq!(config.host, cloned.host);
        assert_eq!(config.port, cloned.port);
        assert_eq!(config.client_id, cloned.client_id);
    }
}
```

- [ ] **Step 6: Register the modules**

```rust
// crates/apexes-gateway-ibkr/src/lib.rs (new file)
//! Apexes H-share/US-equity gateway (IBKR TWS API protocol). Covers
//! `docs/FEATURES.md` row 5.3.3. **Completion level: full protocol
//! implementation** (real TCP client, real message encoding/decoding)
//! **validated only against a hand-written mock TWS server** -- never
//! connected to a real TWS/IB Gateway process, pending real account
//! access. This is a deliberately HIGHER completion level than the
//! sibling `apexes-gateway-xtp` crate, because the IBKR wire protocol
//! is publicly documented (unlike XTP's closed-source SDK binary).
//! See `docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md`
//! for the full scope rationale.

pub mod config;
pub mod protocol;

pub use config::IbkrGatewayConfig;
```

- [ ] **Step 7: Run this crate's tests**

Run: `cargo test -p apexes-gateway-ibkr`
Expected: 6 pass (5 from `protocol.rs` + 1 from `config.rs`).

- [ ] **Step 8: Run clippy, fmt, then a workspace-wide compile check**

Run: `cargo clippy -p apexes-gateway-ibkr --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 9: Commit**

```bash
git add Cargo.toml crates/apexes-gateway-ibkr
git commit -S -s -m "feat(apexes-gateway-ibkr): new crate skeleton -- IbkrGatewayConfig, wire protocol codec"
```

---

### Task 5: `apexes-gateway-ibkr`: `IbkrExecutionGateway`

**Files:**
- Create: `crates/apexes-gateway-ibkr/src/gateway.rs`
- Modify: `crates/apexes-gateway-ibkr/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_oms::{ExecutionGateway, GatewayOutcome, ExchangeFill}` (existing); `apexes_core::{Order, OrderId, OrderStatus, AssetClass, Side, OrderType}` (existing); `IbkrGatewayConfig` (Task 4); `protocol::{encode_message, decode_message}` (Task 4).
- Produces: `apexes_gateway_ibkr::IbkrExecutionGateway`.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/apexes-gateway-ibkr/src/gateway.rs (new file)
//! [`IbkrExecutionGateway`]: real TCP client + IBKR wire protocol
//! encoding, implementing `apexes_oms::ExecutionGateway`. Never
//! connected to a real TWS/IB Gateway process -- every method that
//! requires a response the gateway has never actually received
//! returns `GatewayOutcome::Uncertain`/`Err` with an explicit reason,
//! never a fabricated success.

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, Order, OrderId, OrderStatus, OrderType, Side, Venue};
    use apexes_oms::{ExecutionGateway, GatewayOutcome};
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn sample_config() -> IbkrGatewayConfig {
        IbkrGatewayConfig {
            host: "127.0.0.1".to_string(),
            port: 7497,
            client_id: 1,
        }
    }

    fn sample_order(order_type: OrderType, side: Side) -> Order {
        Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "AAPL".to_string(),
            venue: Venue::Nasdaq,
            asset_class: AssetClass::Equity,
            side,
            order_type,
            quantity: dec!(100),
            price: Some(dec!(150.25)),
            stop_price: None,
            leverage: None,
            margin_mode: None,
            filled_quantity: dec!(0),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn order_to_ibkr_side_maps_buy_and_sell() {
        let buy_order = sample_order(OrderType::Limit, Side::Buy);
        let sell_order = sample_order(OrderType::Limit, Side::Sell);
        assert_eq!(IbkrExecutionGateway::order_to_ibkr_side(&buy_order), "BUY");
        assert_eq!(IbkrExecutionGateway::order_to_ibkr_side(&sell_order), "SELL");
    }

    #[test]
    fn order_to_ibkr_order_type_maps_all_four_types() {
        assert_eq!(
            IbkrExecutionGateway::order_to_ibkr_order_type(&sample_order(OrderType::Limit, Side::Buy)).unwrap(),
            "LMT"
        );
        assert_eq!(
            IbkrExecutionGateway::order_to_ibkr_order_type(&sample_order(OrderType::Market, Side::Buy)).unwrap(),
            "MKT"
        );
        assert_eq!(
            IbkrExecutionGateway::order_to_ibkr_order_type(&sample_order(OrderType::Stop, Side::Buy)).unwrap(),
            "STP"
        );
        assert_eq!(
            IbkrExecutionGateway::order_to_ibkr_order_type(&sample_order(OrderType::StopLimit, Side::Buy)).unwrap(),
            "STP LMT"
        );
    }

    #[tokio::test]
    async fn submit_order_returns_uncertain_when_not_connected() {
        let gateway = IbkrExecutionGateway::new(sample_config());
        let order = sample_order(OrderType::Limit, Side::Buy);
        let outcome = gateway.submit_order(&order).await;
        assert!(matches!(outcome, GatewayOutcome::Uncertain { .. }));
    }

    #[tokio::test]
    async fn cancel_order_returns_uncertain_when_not_connected() {
        let gateway = IbkrExecutionGateway::new(sample_config());
        let order = sample_order(OrderType::Limit, Side::Buy);
        let outcome = gateway.cancel_order(&order).await;
        assert!(matches!(outcome, GatewayOutcome::Uncertain { .. }));
    }

    #[tokio::test]
    async fn query_order_status_returns_err_when_not_connected() {
        let gateway = IbkrExecutionGateway::new(sample_config());
        let order = sample_order(OrderType::Limit, Side::Buy);
        let result = gateway.query_order_status(&order).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn query_all_open_orders_returns_err_when_not_connected() {
        let gateway = IbkrExecutionGateway::new(sample_config());
        let result = gateway.query_all_open_orders(AssetClass::Equity).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn query_fills_for_order_returns_err_when_not_connected() {
        let gateway = IbkrExecutionGateway::new(sample_config());
        let order = sample_order(OrderType::Limit, Side::Buy);
        let result = gateway.query_fills_for_order(&order).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn connect_fails_cleanly_against_an_unreachable_address() {
        let gateway = IbkrExecutionGateway::new(IbkrGatewayConfig {
            host: "127.0.0.1".to_string(),
            // Port 1 is a reserved/unroutable port on virtually every
            // system, so this connection attempt fails deterministically
            // without needing a real TWS process or a mock server.
            port: 1,
            client_id: 1,
        });
        let result = gateway.connect().await;
        assert!(result.is_err());
    }
}
```

Run: `cargo test -p apexes-gateway-ibkr gateway`
Expected: FAIL with "cannot find type `IbkrExecutionGateway`" -- expected; Step 2 implements it.

- [ ] **Step 2: Implement `IbkrExecutionGateway`**

```rust
// crates/apexes-gateway-ibkr/src/gateway.rs -- add above the
// #[cfg(test)] block:

use apexes_core::{AssetClass, Order, OrderId, OrderStatus};
use apexes_oms::{ExchangeFill, ExecutionGateway, GatewayOutcome};
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::config::IbkrGatewayConfig;
use crate::protocol::encode_message;

/// Real TCP client + IBKR wire protocol implementation, never
/// connected to a real TWS/IB Gateway process. `stream` is `None`
/// until `connect()` succeeds; every trading method that needs a
/// response the gateway has never actually received from a real TWS
/// process returns `GatewayOutcome::Uncertain`/`Err`, not a fabricated
/// success -- see this crate's `lib.rs` docs for the full rationale.
pub struct IbkrExecutionGateway {
    config: IbkrGatewayConfig,
    stream: Mutex<Option<TcpStream>>,
}

impl IbkrExecutionGateway {
    pub fn new(config: IbkrGatewayConfig) -> Self {
        Self {
            config,
            stream: Mutex::new(None),
        }
    }

    /// Establishes a real TCP connection to the configured host/port.
    /// This is genuinely real network I/O (unlike
    /// `apexes-gateway-xtp`'s pure request-shape mapping) -- it has
    /// simply never been exercised against a real TWS/IB Gateway
    /// process, only against this crate's own hand-written mock TCP
    /// server in integration tests (Task 6).
    pub async fn connect(&self) -> Result<(), String> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|error| format!("failed to connect to IBKR gateway at {addr}: {error}"))?;
        *self.stream.lock().await = Some(stream);
        Ok(())
    }

    pub(crate) fn order_to_ibkr_side(order: &Order) -> &'static str {
        match order.side {
            apexes_core::Side::Buy => "BUY",
            apexes_core::Side::Sell => "SELL",
        }
    }

    pub(crate) fn order_to_ibkr_order_type(order: &Order) -> Result<&'static str, String> {
        match order.order_type {
            apexes_core::OrderType::Limit => Ok("LMT"),
            apexes_core::OrderType::Market => Ok("MKT"),
            apexes_core::OrderType::Stop => Ok("STP"),
            apexes_core::OrderType::StopLimit => Ok("STP LMT"),
        }
    }

    async fn send_message(&self, fields: &[String]) -> Result<(), String> {
        let mut guard = self.stream.lock().await;
        let stream = guard
            .as_mut()
            .ok_or_else(|| "not connected to an IBKR gateway -- call connect() first".to_string())?;
        let bytes = encode_message(fields);
        stream
            .write_all(&bytes)
            .await
            .map_err(|error| format!("failed to write to IBKR socket: {error}"))
    }

    fn no_response_reason() -> String {
        "sent to socket, but never connected to a real TWS/IB Gateway process -- no response \
         was ever received to confirm processing; see \
         docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md \
         §4 for scope"
            .to_string()
    }
}

#[async_trait]
impl ExecutionGateway for IbkrExecutionGateway {
    async fn submit_order(&self, order: &Order) -> GatewayOutcome {
        let side = Self::order_to_ibkr_side(order);
        let order_type = match Self::order_to_ibkr_order_type(order) {
            Ok(order_type) => order_type,
            Err(reason) => return GatewayOutcome::Rejected { reason },
        };
        let fields = vec![
            "PLACE_ORDER".to_string(),
            order.id.0.to_string(),
            order.symbol.clone(),
            side.to_string(),
            order_type.to_string(),
            order.quantity.to_string(),
        ];
        match self.send_message(&fields).await {
            Ok(()) => GatewayOutcome::Uncertain {
                reason: Self::no_response_reason(),
            },
            Err(reason) => GatewayOutcome::Uncertain { reason },
        }
    }

    async fn cancel_order(&self, order: &Order) -> GatewayOutcome {
        let fields = vec!["CANCEL_ORDER".to_string(), order.id.0.to_string()];
        match self.send_message(&fields).await {
            Ok(()) => GatewayOutcome::Uncertain {
                reason: Self::no_response_reason(),
            },
            Err(reason) => GatewayOutcome::Uncertain { reason },
        }
    }

    async fn query_order_status(&self, order: &Order) -> Result<Option<OrderStatus>, String> {
        let fields = vec!["QUERY_ORDER_STATUS".to_string(), order.id.0.to_string()];
        self.send_message(&fields).await?;
        Err(Self::no_response_reason())
    }

    async fn query_all_open_orders(
        &self,
        asset_class: AssetClass,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
        let fields = vec![
            "QUERY_ALL_OPEN_ORDERS".to_string(),
            format!("{asset_class:?}"),
        ];
        self.send_message(&fields).await?;
        Err(Self::no_response_reason())
    }

    async fn query_fills_for_order(&self, order: &Order) -> Result<Vec<ExchangeFill>, String> {
        let fields = vec!["QUERY_FILLS_FOR_ORDER".to_string(), order.id.0.to_string()];
        self.send_message(&fields).await?;
        Err(Self::no_response_reason())
    }
}
```

- [ ] **Step 3: Register the module**

```rust
// crates/apexes-gateway-ibkr/src/lib.rs -- add:
pub mod gateway;
pub use gateway::IbkrExecutionGateway;
```

- [ ] **Step 4: Run the new tests**

Run: `cargo test -p apexes-gateway-ibkr gateway`
Expected: all 8 pass.

- [ ] **Step 5: Run this crate's tests, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-gateway-ibkr`
Expected: all pass (6 from Task 4 + 8 from this task = 14).

Run: `cargo clippy -p apexes-gateway-ibkr --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-gateway-ibkr/src/gateway.rs crates/apexes-gateway-ibkr/src/lib.rs
git commit -S -s -m "feat(apexes-gateway-ibkr): IbkrExecutionGateway -- real TCP client + wire protocol, Uncertain-returning stubs pending real TWS connection"
```

---

### Task 6: `apexes-gateway-ibkr`: mock TWS server integration tests

**Files:**
- Create: `crates/apexes-gateway-ibkr/tests/mock_tws.rs`

**Interfaces:**
- Consumes: `apexes_gateway_ibkr::{IbkrExecutionGateway, IbkrGatewayConfig}` (Tasks 4-5); `apexes_oms::{ExecutionGateway, GatewayOutcome}` (existing).
- Produces: nothing new (integration tests only).

- [ ] **Step 1: Write the failing integration test**

```rust
// crates/apexes-gateway-ibkr/tests/mock_tws.rs (new file)
//! Verifies `IbkrExecutionGateway` genuinely establishes a real TCP
//! connection and sends correctly wire-protocol-encoded messages, by
//! running a hand-written mock TCP server that speaks the same
//! length-prefixed/null-separated protocol -- no real TWS/IB Gateway
//! process required. This is the IBKR-protocol equivalent of
//! `apexes-gateway-binance`'s `wiremock`-based tests, but hand-rolled
//! at the TCP level since IBKR's protocol is not HTTP.

use apexes_core::{AssetClass, Order, OrderId, OrderStatus, OrderType, Side, Venue};
use apexes_gateway_ibkr::{IbkrExecutionGateway, IbkrGatewayConfig};
use apexes_oms::ExecutionGateway;
use chrono::Utc;
use rust_decimal_macros::dec;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use uuid::Uuid;

fn sample_order() -> Order {
    Order {
        id: OrderId(Uuid::new_v4()),
        exchange_order_id: None,
        symbol: "AAPL".to_string(),
        venue: Venue::Nasdaq,
        asset_class: AssetClass::Equity,
        side: Side::Buy,
        order_type: OrderType::Limit,
        quantity: dec!(100),
        price: Some(dec!(150.25)),
        stop_price: None,
        leverage: None,
        margin_mode: None,
        filled_quantity: dec!(0),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

/// Decodes exactly one IBKR wire-protocol message from a raw byte
/// buffer, panicking with a descriptive message if the buffer does
/// not contain a complete message -- used only in this test file to
/// assert on what the mock server actually received.
fn decode_one_message(buf: &[u8]) -> Vec<String> {
    apexes_gateway_ibkr::protocol::decode_message(buf)
        .expect("mock server should have received one complete IBKR wire-protocol message")
        .0
}

#[tokio::test]
async fn connect_establishes_a_real_tcp_connection_to_a_mock_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (_socket, _peer_addr) = listener.accept().await.unwrap();
    });

    let gateway = IbkrExecutionGateway::new(IbkrGatewayConfig {
        host: addr.ip().to_string(),
        port: addr.port(),
        client_id: 1,
    });
    let result = gateway.connect().await;
    assert!(result.is_ok());

    server_task.await.unwrap();
}

#[tokio::test]
async fn submit_order_sends_a_correctly_encoded_place_order_message() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let order = sample_order();
    let expected_order_id = order.id.0.to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _peer_addr) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = socket.read(&mut buf).await.unwrap();
        decode_one_message(&buf[..n])
    });

    let gateway = IbkrExecutionGateway::new(IbkrGatewayConfig {
        host: addr.ip().to_string(),
        port: addr.port(),
        client_id: 1,
    });
    gateway.connect().await.unwrap();
    let outcome = gateway.submit_order(&order).await;

    let received_fields = server_task.await.unwrap();
    assert_eq!(received_fields[0], "PLACE_ORDER");
    assert_eq!(received_fields[1], expected_order_id);
    assert_eq!(received_fields[2], "AAPL");
    assert_eq!(received_fields[3], "BUY");
    assert_eq!(received_fields[4], "LMT");
    assert_eq!(received_fields[5], "100");

    // The gateway sent the message successfully but never received a
    // parsed response back (the mock server above only reads, it does
    // not reply) -- so the outcome must be Uncertain, never a
    // fabricated Accepted.
    assert!(matches!(
        outcome,
        apexes_oms::GatewayOutcome::Uncertain { .. }
    ));
}

#[tokio::test]
async fn cancel_order_sends_a_correctly_encoded_cancel_message() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let order = sample_order();
    let expected_order_id = order.id.0.to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _peer_addr) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = socket.read(&mut buf).await.unwrap();
        decode_one_message(&buf[..n])
    });

    let gateway = IbkrExecutionGateway::new(IbkrGatewayConfig {
        host: addr.ip().to_string(),
        port: addr.port(),
        client_id: 1,
    });
    gateway.connect().await.unwrap();
    gateway.cancel_order(&order).await;

    let received_fields = server_task.await.unwrap();
    assert_eq!(received_fields[0], "CANCEL_ORDER");
    assert_eq!(received_fields[1], expected_order_id);
}

#[tokio::test]
async fn submit_order_before_connect_returns_uncertain_not_connected() {
    let gateway = IbkrExecutionGateway::new(IbkrGatewayConfig {
        host: "127.0.0.1".to_string(),
        port: 65535,
        client_id: 1,
    });
    let outcome = gateway.submit_order(&sample_order()).await;
    match outcome {
        apexes_oms::GatewayOutcome::Uncertain { reason } => {
            assert!(reason.contains("not connected"));
        }
        other => panic!("expected Uncertain with a not-connected reason, got {other:?}"),
    }
}
```

Run: `cargo test -p apexes-gateway-ibkr --test mock_tws`
Expected: FAIL initially with a compile error if `apexes_gateway_ibkr::protocol::decode_message` is not `pub` at the crate root -- confirm `pub mod protocol;` (already present from Task 4 Step 6) makes `protocol::decode_message` reachable as `apexes_gateway_ibkr::protocol::decode_message` from an external test file; no source changes should be needed since `protocol.rs`'s `decode_message`/`encode_message` were already declared `pub fn` in Task 4. If it fails for any OTHER reason (e.g. a genuine logic bug in `connect`/`submit_order`), fix the underlying implementation, not the test.

- [ ] **Step 2: Run the tests to confirm they pass against the already-implemented gateway**

Run: `cargo test -p apexes-gateway-ibkr --test mock_tws`
Expected: all 4 pass. (Unlike prior tasks, this task does not add new implementation code -- `IbkrExecutionGateway`/the protocol codec were already fully implemented in Tasks 4-5. This task's sole job is proving those existing implementations behave correctly against a real TCP server, which is why Step 1's tests should pass immediately once any reachability issue is resolved.)

- [ ] **Step 3: Run this crate's full test suite, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-gateway-ibkr`
Expected: all pass (14 from Tasks 4-5 + 4 from this task = 18).

Run: `cargo clippy -p apexes-gateway-ibkr --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-gateway-ibkr/tests/mock_tws.rs
git commit -S -s -m "test(apexes-gateway-ibkr): mock TWS server integration tests -- verify real TCP connection and wire-protocol encoding"
```

---

### Task 7: `apexes-gateway-ibkr`: `IbkrMarketDataProvider`

**Files:**
- Create: `crates/apexes-gateway-ibkr/src/market_data.rs`
- Modify: `crates/apexes-gateway-ibkr/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_core::{MarketDataProvider, Instrument, MarketEvent, ApexesError, Tick, Venue}` (existing); `protocol::encode_message` (Task 4); `IbkrGatewayConfig` (Task 4).
- Produces: `apexes_gateway_ibkr::IbkrMarketDataProvider`.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/apexes-gateway-ibkr/src/market_data.rs (new file)
//! [`IbkrMarketDataProvider`]: implements `apexes_core::MarketDataProvider`
//! against the IBKR TWS API's market-data request protocol
//! (`reqMktData`). Same completion level as `IbkrExecutionGateway` --
//! real request construction and wire-protocol encoding, never
//! connected to a real TWS/IB Gateway process. Unlike
//! `IbkrExecutionGateway`, this type owns its OWN connection (rather
//! than sharing `IbkrExecutionGateway`'s) to keep the two concerns --
//! order execution and market data -- independently testable and
//! independently connectable, matching how a real TWS client can
//! subscribe to market data without necessarily placing orders (and
//! vice versa).

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, Instrument, MarketDataProvider, MarketType, Venue};
    use rust_decimal_macros::dec;

    fn sample_config() -> IbkrGatewayConfig {
        IbkrGatewayConfig {
            host: "127.0.0.1".to_string(),
            port: 7497,
            client_id: 1,
        }
    }

    fn sample_instrument() -> Instrument {
        Instrument::new(
            "AAPL",
            MarketType::UsEquity,
            Venue::Nasdaq,
            AssetClass::Equity,
            None,
            Some("USD".to_string()),
            dec!(0.01),
            dec!(1),
            dec!(1),
        )
        .unwrap()
    }

    #[test]
    fn build_market_data_request_maps_instrument_symbol() {
        let fields = IbkrMarketDataProvider::build_market_data_request(&sample_instrument());
        assert_eq!(fields[0], "REQ_MKT_DATA");
        assert_eq!(fields[1], "AAPL");
    }

    #[tokio::test]
    async fn subscribe_returns_err_when_not_connected() {
        let provider = IbkrMarketDataProvider::new(sample_config());
        let result = provider.subscribe(&[sample_instrument()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_error_mentions_no_real_tws_connection() {
        let provider = IbkrMarketDataProvider::new(sample_config());
        let result = provider.subscribe(&[sample_instrument()]).await;
        let error = result.unwrap_err();
        assert!(error.to_string().to_lowercase().contains("connect"));
    }
}
```

Run: `cargo test -p apexes-gateway-ibkr market_data`
Expected: FAIL with "cannot find type `IbkrMarketDataProvider`" -- expected; Step 2 implements it.

- [ ] **Step 2: Implement `IbkrMarketDataProvider`**

```rust
// crates/apexes-gateway-ibkr/src/market_data.rs -- add above the
// #[cfg(test)] block:

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::BoxStream;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::config::IbkrGatewayConfig;
use crate::protocol::encode_message;

pub struct IbkrMarketDataProvider {
    config: IbkrGatewayConfig,
    stream: Mutex<Option<TcpStream>>,
}

impl IbkrMarketDataProvider {
    pub fn new(config: IbkrGatewayConfig) -> Self {
        Self {
            config,
            stream: Mutex::new(None),
        }
    }

    /// Establishes this provider's own real TCP connection,
    /// independent of any `IbkrExecutionGateway` instance -- market
    /// data and order execution are separate TWS API concerns that a
    /// real client can use independently.
    pub async fn connect(&self) -> Result<(), String> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|error| format!("failed to connect to IBKR gateway at {addr}: {error}"))?;
        *self.stream.lock().await = Some(stream);
        Ok(())
    }

    /// Real Instrument -> IBKR `reqMktData` request-field mapping.
    fn build_market_data_request(instrument: &Instrument) -> Vec<String> {
        vec![
            "REQ_MKT_DATA".to_string(),
            instrument.symbol.clone(),
        ]
    }
}

#[async_trait]
impl MarketDataProvider for IbkrMarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let mut guard = self.stream.lock().await;
        let stream = guard.as_mut().ok_or_else(|| ApexesError::SubscriptionFailed {
            symbol: instruments
                .first()
                .map(|instrument| instrument.symbol.clone())
                .unwrap_or_else(|| "*".to_string()),
            venue: instruments
                .first()
                .map(|instrument| instrument.venue)
                .unwrap_or(Venue::Nasdaq),
            reason: "not connected to an IBKR gateway -- call connect() first".to_string(),
            source_detail: None,
        })?;

        for instrument in instruments {
            let fields = Self::build_market_data_request(instrument);
            let bytes = encode_message(&fields);
            stream
                .write_all(&bytes)
                .await
                .map_err(|error| ApexesError::SubscriptionFailed {
                    symbol: instrument.symbol.clone(),
                    venue: instrument.venue,
                    reason: format!("failed to write reqMktData to IBKR socket: {error}"),
                    source_detail: None,
                })?;
        }

        Err(ApexesError::SubscriptionFailed {
            symbol: instruments
                .first()
                .map(|instrument| instrument.symbol.clone())
                .unwrap_or_else(|| "*".to_string()),
            venue: instruments
                .first()
                .map(|instrument| instrument.venue)
                .unwrap_or(Venue::Nasdaq),
            reason: "reqMktData sent, but never connected to a real TWS/IB Gateway process -- \
                     no tickPrice response was ever received to build a live event stream from"
                .to_string(),
            source_detail: None,
        })
    }
}
```

- [ ] **Step 3: Register the module**

```rust
// crates/apexes-gateway-ibkr/src/lib.rs -- add:
pub mod market_data;
pub use market_data::IbkrMarketDataProvider;
```

- [ ] **Step 4: Run the new tests**

Run: `cargo test -p apexes-gateway-ibkr market_data`
Expected: all 3 pass.

- [ ] **Step 5: Run this crate's full test suite, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-gateway-ibkr`
Expected: all pass (18 from Tasks 4-6 + 3 from this task = 21).

Run: `cargo clippy -p apexes-gateway-ibkr --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-gateway-ibkr/src/market_data.rs crates/apexes-gateway-ibkr/src/lib.rs
git commit -S -s -m "feat(apexes-gateway-ibkr): IbkrMarketDataProvider -- real reqMktData request construction, pending real TWS connection"
```

---

### Task 8: Full-workspace verification checkpoint

**Files:** none (verification-only task).

**Interfaces:** none.

- [ ] **Step 1: Run the complete verification suite**

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo test --workspace -- --test-threads=1`
Expected: 0 failures. Record the exact passed/ignored counts (via `grep "test result:"` summed across every suite, per this project's established convention of never estimating these numbers) for comparison against the pre-this-plan baseline (552 passed / 0 failed / 45 ignored, per sub-project `#11`'s final Task 10 checkpoint).

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 2: Grep for leftover placeholders and forbidden panic-style stubs**

Run: `grep -rn "TODO\|FIXME\|todo!\|unimplemented!" crates/apexes-gateway-xtp/src crates/apexes-gateway-ibkr/src`
Expected: no matches.

Run: `grep -rn "panic!\|unreachable!" crates/apexes-gateway-xtp/src crates/apexes-gateway-ibkr/src`
Expected: no matches outside `#[cfg(test)]` blocks (both crates' entire production-code design is built around returning explicit `Uncertain`/`Err` results instead of panicking -- any match here outside a test module is a plan violation that must be fixed before proceeding).

- [ ] **Step 3: Commit** (only if Step 1/2 required any fixes; otherwise this task produces no diff and can be skipped without a commit)

```bash
git add -A
git commit -S -s -m "chore: full-workspace verification checkpoint for additional market gateways plan"
```

---

### Task 9: Final whole-branch code review and `docs/FEATURES.md` update

**Files:**
- Modify: `docs/FEATURES.md`

**Interfaces:** none (documentation/review-only task).

- [ ] **Step 1: Run the complete verification suite one final time**

Run: `cargo build --workspace`, `cargo test --workspace -- --test-threads=1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`.
Expected: all clean, matching this project's established multi-sub-project-completion precedent (rows 6.2/6.4/6.5/sub-project #11's Task 10 all ended with this exact step).

- [ ] **Step 2: Escalate to a whole-branch review**

Per this project's established precedent (rows 6.2/6.4/6.5's final reviews and sub-project `#11`'s Task 10, all escalated to the most capable available reviewer profile for a correctness-critical whole-branch pass): dispatch an independent reviewer over the ENTIRE branch diff for this plan (base: the commit before Task 1 started, head: Task 7's final commit). Pay particular attention to:

- **The "no panics, no silent success" invariant, re-verified independently**: for every method in `XtpExecutionGateway`, `XtpMarketDataProvider`, `IbkrExecutionGateway`, and `IbkrMarketDataProvider`, confirm it returns an explicit `Uncertain`/`Err`/`Rejected` result rather than panicking, calling `unimplemented!()`, or fabricating a success outcome -- grep the whole diff for `panic!`/`unreachable!`/`todo!`/`unimplemented!` outside test modules yourself, don't just trust Task 8's own grep.
- **`XtpExecutionGateway::build_insert_request`'s correctness**: re-derive the Sse/Szse -> `XtpMarket` mapping and the `Side`/`OrderType` mappings independently against the test cases in Task 2, and confirm unsupported venues/order types genuinely produce `Rejected` (not `Uncertain`) -- this distinction (a real, confident local validation failure vs. "we don't know what the real exchange would say") is the one piece of nuanced error-classification logic in the whole plan and deserves independent verification.
- **The IBKR wire protocol codec's round-trip correctness**: re-derive `encode_message`/`decode_message`'s byte-level behavior independently (length prefix, null-byte field separation, partial-message handling) rather than trusting the existing test suite's green checkmarks alone.
- **Whether `IbkrExecutionGateway`/`IbkrMarketDataProvider`'s mock-TWS-server tests (Task 6) actually exercise real TCP I/O** (not an in-process fake) -- confirm the test file genuinely binds a `TcpListener` and the gateway genuinely connects to it over a real (loopback) socket.
- **Whether `ExecutionGateway`/`MarketDataProvider` (the pre-existing traits) were modified anywhere in this diff** -- they must not be; grep `crates/apexes-oms/src/gateway.rs` and `crates/apexes-core/src/market_data.rs` in the diff yourself to confirm zero changes.
- **Whether the completion-level asymmetry between `apexes-gateway-xtp` (protocol-mapping placeholder) and `apexes-gateway-ibkr` (full protocol implementation) is consistently documented** in both crates' `lib.rs` doc comments and reflected accurately in the `docs/FEATURES.md` update this task performs -- this asymmetry is intentional per the design spec, but the review should confirm it's not silently mischaracterized as equal completion anywhere.

Fix any findings, get a fix-verification re-review, and only then consider this plan complete.

- [ ] **Step 3: Update `docs/FEATURES.md`**

Find rows `5.3.2` and `5.3.3` (`grep -n "| 5.3.2 |\\|| 5.3.3 |" docs/FEATURES.md`) and update each row's Status column from `🔲 Not Started` to `🚧 In Progress` (NOT `✅ Done` -- per the design spec §六, neither row reaches this project's "Done = real, verified capability" bar, since no real XTP SDK link or real TWS/IB Gateway connection was ever established). Notes must summarize for each row: the account-access research trail (Longbridge has no sandbox, Futu stopped mainland account opening in 2022, IBKR has high account-opening barriers, CTP is closed-source with no public protocol docs, PTrade's architecture doesn't match `ExecutionGateway`'s client-initiates model), the resulting scope decision (XTP for A-share as a protocol-mapping-only placeholder; IBKR for H-share/US-equity with a full protocol implementation validated against a hand-written mock TCP server), and the explicit completion-level asymmetry between the two new crates plus what remains as future work (real XTP SDK linking, real TWS/IB Gateway connection validation) once real account access becomes available.

Recompute the Summary counts table via actual `grep -c "✅ Done"`/`grep -c "🔲 Not Started"`/`grep -c "🚧 In Progress"` (per this project's explicit standing instruction: NEVER estimate these counts, always recompute them from the real table contents) -- subtract 2 from each raw count (the legend row near the top of the file, AND the Summary table's own count row, both of which also contain these exact strings as their own text, per this project's standing memory note about this recurring double-count mistake). Note this task changes counts across THREE status categories (`🔲 Not Started` decreases by 2, `🚧 In Progress` increases by 2, `✅ Done` unchanged), unlike prior plans in this project's history which only moved counts between two categories -- recompute all three via `grep -c`, do not assume the arithmetic from memory.

- [ ] **Step 4: Commit**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: mark FEATURES.md rows 5.3.2/5.3.3 (additional market gateways) as In Progress"
```
