# Apexes: Account & Fund Management Implementation Plan (`docs/FEATURES.md` rows `7.1`/`7.2`/`7.3`)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the design in `docs/superpowers/specs/2026-08-02-apexes-account-design.md`: a new `apexes-account` crate providing a unified `Account` trait across backtest/paper/live environments (`AccountRegistry` for multi-account aggregation, `FxRateProvider` for cross-currency valuation), real Binance balance/position/transfer/deposit-withdrawal-history integration, a `PortfolioLedger::realized_pnl` extension, and balance/position reconciliation against the real exchange (alert-only, never auto-correcting) — completing roadmap sub-project `#11`.

**Architecture:** Ten sequential tasks in dependency order. Task 1 adds shared `apexes-core` types (`WalletId`, `FundMovement`, `FundMovementStatus`, `ApexesError::UnsupportedForEnvironment`) needed by both the gateway and account layers, avoiding the circular dependency the design's §四 analysis identifies. Task 2 extends `PortfolioLedger` with `realized_pnl` tracking (spot and leveraged paths). Task 3 adds the new real Binance API methods to `BinanceExecutionGateway`. Task 4 creates the `apexes-account` crate skeleton with the `Account` trait, `WalletId`/`FundMovement` re-exports, and `ReconciliationTolerance`/`ReconciliationDiscrepancy`. Task 5 adds `FxRateProvider`/`StaticFxRateProvider`. Task 6 implements `Account` for `PortfolioLedger` and `PaperAccount`. Task 7 implements `LiveAccount` (wraps `BinanceExecutionGateway`). Task 8 adds `AccountRegistry` and `compute_pnl`/`AccountPnl`. Task 9 is a full-workspace verification checkpoint. Task 10 is the final whole-branch review and the `docs/FEATURES.md` update.

**Tech Stack:** Rust 2024, existing workspace deps only. `wiremock` (already an `apexes-gateway-binance` dev-dependency at crate-local version `"0.6"`) is added the same way to `apexes-account`'s dev-dependencies for `LiveAccount` tests.

## Global Constraints

- Rust edition 2024, workspace-wide `cargo check`/`cargo clippy --all-targets -- -D warnings`/`cargo fmt --check` must stay clean after every task.
- No `unsafe` blocks. Never bare `.unwrap()`/`.expect()` in production code (test code may use them freely).
- `thiserror` for all new error types; `serde`/`chrono`/`rust_decimal` conventions match existing crates exactly.
- Every commit signed: `git commit -S -s`.
- Existing behavior must not change for any task in this plan — every extension is additive. In particular, `PortfolioLedger::realized_pnl` (Task 2) must not alter the return value, signature, or observable behavior of any existing method (`balances`, `positions`, `equity`, `margin_status`, `check_liquidations`, `check_de_risking`, `apply_fill`, `apply_leveraged_fill`, `apply_funding_payments`) — it is a pure additive side-effect plus one new getter.
- **No real fund-movement-out-of-the-account code paths**: per the design's explicit scope exclusion, this plan implements deposit/withdrawal *history* (read-only) but never a withdrawal-initiation API call. Do not add one, even as a stub — there is no `withdraw()` method anywhere in this plan.
- **Circular dependency avoidance is load-bearing**: `WalletId`/`FundMovement`/`FundMovementStatus` MUST be defined in `apexes-core` (Task 1), not `apexes-account` and not `apexes-gateway-binance` — `apexes-gateway-binance`'s new methods (Task 3) need these types in their signatures, and `apexes-account::LiveAccount` (Task 7) needs to depend on `apexes-gateway-binance`; defining them in either of those two crates creates a cycle. See design spec §四 for the full analysis; do not "simplify" this by moving the types later.
- `FxRateProvider::fx_rate` (Task 5) is a **synchronous** method, not `async`, because `Account::equity()` (Task 4) is itself synchronous and cannot call an async method without blocking. Do not add `#[async_trait]` to `FxRateProvider`.
- `LiveAccount` (Task 7) wraps the **concrete type** `Arc<BinanceExecutionGateway>`, not a trait object — this is a deliberate design choice (see spec §3.5), so its tests require a real `BinanceExecutionGateway` pointed at a local `wiremock` server (reusing `BinanceGatewayConfig`'s overridable `spot_base_url`/`futures_base_url`), not a hand-rolled mock gateway.
- Full design rationale (why `Account` is unified across backtest/paper/live, why reconciliation never auto-corrects, why FX rates are a pluggable trait rather than hardcoded) is in the design spec — read it before starting if anything below is unclear on *why*, not just *what*.

---

### Task 1: `apexes-core`: `WalletId`, `FundMovement`, `FundMovementStatus`, `ApexesError::UnsupportedForEnvironment`

**Files:**
- Create: `crates/apexes-core/src/account.rs`
- Modify: `crates/apexes-core/src/error.rs`
- Modify: `crates/apexes-core/src/lib.rs`

**Interfaces:**
- Consumes: nothing new (pure new types).
- Produces: `apexes_core::{WalletId, FundMovement, FundMovementStatus}`; `ApexesError::UnsupportedForEnvironment { reason: String }`.

- [ ] **Step 1: Write the failing tests for the new `apexes-core` types**

```rust
// crates/apexes-core/src/account.rs (new file)
//! Shared account/fund-movement types used by `apexes-gateway-binance`
//! (real exchange API method signatures) and `apexes-account` (the
//! `Account` trait and its implementations). Defined here, not in
//! either of those two crates, to avoid a circular dependency:
//! `apexes-gateway-binance`'s new real-API methods need these types in
//! their signatures, and `apexes-account::LiveAccount` depends on
//! `apexes-gateway-binance` -- see
//! `docs/superpowers/specs/2026-08-02-apexes-account-design.md` §四
//! for the full analysis.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A wallet/sub-account category on an exchange that supports internal
/// transfers between them (e.g. Binance Spot vs. USDS-M Futures).
/// backtest/paper environments have no concept of separate wallets --
/// `apexes_account::Account::internal_transfer` returns
/// `ApexesError::UnsupportedForEnvironment` there instead of using this
/// type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WalletId {
    Spot,
    UsdsFutures,
}

/// A single deposit or withdrawal record as reported by an exchange.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundMovement {
    pub asset: String,
    pub amount: Decimal,
    pub status: FundMovementStatus,
    pub timestamp: DateTime<Utc>,
    /// The exchange's own transaction identifier -- used by callers to
    /// deduplicate across repeated history queries, the same pattern
    /// `apexes_oms::ExchangeFill::exchange_trade_id` uses for fills.
    pub exchange_tx_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FundMovementStatus {
    Pending,
    Completed,
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn wallet_id_variants_are_distinct() {
        assert_ne!(WalletId::Spot, WalletId::UsdsFutures);
    }

    #[test]
    fn fund_movement_is_constructible_and_comparable() {
        let a = FundMovement {
            asset: "USDT".to_string(),
            amount: dec!(100),
            status: FundMovementStatus::Completed,
            timestamp: Utc::now(),
            exchange_tx_id: "tx-1".to_string(),
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn fund_movement_status_variants_are_distinct() {
        assert_ne!(FundMovementStatus::Pending, FundMovementStatus::Completed);
        assert_ne!(FundMovementStatus::Completed, FundMovementStatus::Failed);
    }
}
```

Run: `cargo test -p apexes-core account::tests`
Expected: FAIL with "unresolved module `account`" or "cannot find" (not yet registered in `lib.rs`) -- expected at this point; Step 3 registers it.

- [ ] **Step 2: Add `ApexesError::UnsupportedForEnvironment`**

```rust
// crates/apexes-core/src/error.rs -- add to the ApexesError enum,
// after the existing OrderRejected variant:
    #[error("operation not supported in this environment: {reason}")]
    UnsupportedForEnvironment { reason: String },
```

Also add a test to the existing `#[cfg(test)] mod tests` block in that file:

```rust
    #[test]
    fn unsupported_for_environment_display() {
        let err = ApexesError::UnsupportedForEnvironment {
            reason: "backtest ledgers have no real wallets to transfer between".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "operation not supported in this environment: backtest ledgers have no real wallets to transfer between"
        );
    }
```

- [ ] **Step 3: Register the new module**

```rust
// crates/apexes-core/src/lib.rs -- add alongside the existing
// `pub mod`/`pub use` lines for order.rs/fill.rs/instrument.rs:
pub mod account;
pub use account::{FundMovement, FundMovementStatus, WalletId};
```

- [ ] **Step 4: Run this crate's tests**

Run: `cargo test -p apexes-core`
Expected: all pass, including the four new tests from Steps 1/2.

- [ ] **Step 5: Run clippy, fmt, then a workspace-wide compile check**

Run: `cargo clippy -p apexes-core --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean (run `cargo fmt` first if not).

Run: `cargo check --workspace`
Expected: 0 errors (this task touches only `apexes-core`, which every other crate already depends on, so this also verifies nothing downstream broke from the new `pub mod`/`pub use` lines).

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-core/src/account.rs crates/apexes-core/src/error.rs crates/apexes-core/src/lib.rs
git commit -S -s -m "feat(apexes-core): WalletId/FundMovement/FundMovementStatus, UnsupportedForEnvironment error"
```

---

### Task 2: `apexes-backtest`: `PortfolioLedger::realized_pnl`

**Files:**
- Modify: `crates/apexes-backtest/src/ledger.rs`

**Interfaces:**
- Consumes: existing `PortfolioLedger` internals (`positions`, `update_position`, `close_leveraged`).
- Produces: `PortfolioLedger::realized_pnl(&self) -> &HashMap<String, Decimal>`.

- [ ] **Step 1: Add the field**

```rust
// crates/apexes-backtest/src/ledger.rs -- add to the PortfolioLedger
// struct definition, after the existing `positions` field:
    /// Per-symbol accumulated realized profit/loss. Spot reductions
    /// (`update_position`'s reduce/flip branch) and leveraged closes
    /// (`close_leveraged`) both write into this map; opening/adding to
    /// a position never does (there is no realized PnL to book until
    /// quantity is reduced). See `docs/superpowers/specs/2026-08-02-apexes-account-design.md`
    /// §五 for the full design rationale.
    realized_pnl: HashMap<String, Decimal>,
```

Update the `PortfolioLedger::new` constructor to initialize it:

```rust
// In PortfolioLedger::new, add to the Self { .. } literal:
            realized_pnl: HashMap::new(),
```

- [ ] **Step 2: Write the failing tests**

Add to the existing `#[cfg(test)] mod tests` block in `crates/apexes-backtest/src/ledger.rs`, near the other `realized`/PnL-adjacent tests:

```rust
    #[test]
    fn realized_pnl_starts_empty() {
        let ledger = ledger_with_10k_usdt();
        assert!(ledger.realized_pnl().is_empty());
    }

    #[test]
    fn spot_partial_sell_realizes_pnl_against_average_entry_price() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.2), dec!(0)))
            .unwrap();
        // Sell half at a higher price: realizes (65000-60000)*0.1 = 500 profit.
        ledger
            .apply_fill(&sample_fill(Side::Sell, dec!(65000), dec!(0.1), dec!(0)))
            .unwrap();

        assert_eq!(
            *ledger.realized_pnl().get("BTCUSDT").unwrap(),
            dec!(500)
        );
        // Position still open (0.1 left) -- quantity/average_entry_price
        // are unaffected by this new field.
        let position = ledger.positions().get("BTCUSDT").unwrap();
        assert_eq!(position.quantity, dec!(0.1));
        assert_eq!(position.average_entry_price, dec!(60000));
    }

    #[test]
    fn spot_short_side_realized_pnl_sign_is_reversed() {
        // Start with a base-asset balance so a short-then-cover sequence
        // is representable (this ledger models net spot position, not a
        // literal borrow -- selling first requires an existing balance,
        // matching `position_flips_sign_and_resets_entry_price_on_full_reversal`'s
        // existing setup pattern).
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));
        initial_balances.insert("BTC".to_string(), dec!(1));
        let mut ledger = PortfolioLedger::new(instruments, initial_balances);

        // Sell 0.2 at 65000 (opens a "short" relative to the flat position
        // this ledger tracks -- quantity goes negative).
        ledger
            .apply_fill(&sample_fill(Side::Sell, dec!(65000), dec!(0.2), dec!(0)))
            .unwrap();
        // Buy back 0.1 at 60000: price fell, so covering a short realizes
        // a profit of (65000-60000)*0.1 = 500 (reversed sign vs. a long).
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.1), dec!(0)))
            .unwrap();

        assert_eq!(
            *ledger.realized_pnl().get("BTCUSDT").unwrap(),
            dec!(500)
        );
    }

    #[test]
    fn spot_opening_and_adding_does_not_write_realized_pnl() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.05), dec!(0)))
            .unwrap();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(70000), dec!(0.05), dec!(0)))
            .unwrap();

        assert!(ledger.realized_pnl().get("BTCUSDT").is_none());
    }

    #[test]
    fn realized_pnl_accumulates_across_multiple_partial_closes() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&sample_fill(Side::Buy, dec!(60000), dec!(0.3), dec!(0)))
            .unwrap();
        // First partial close: (65000-60000)*0.1 = 500.
        ledger
            .apply_fill(&sample_fill(Side::Sell, dec!(65000), dec!(0.1), dec!(0)))
            .unwrap();
        // Second partial close: (70000-60000)*0.1 = 1000.
        ledger
            .apply_fill(&sample_fill(Side::Sell, dec!(70000), dec!(0.1), dec!(0)))
            .unwrap();

        // Accumulated, not overwritten: 500 + 1000 = 1500.
        assert_eq!(
            *ledger.realized_pnl().get("BTCUSDT").unwrap(),
            dec!(1500)
        );
    }

    #[test]
    fn leveraged_partial_close_writes_realized_pnl_field() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(60000),
                dec!(1),
                dec!(10),
            ))
            .unwrap();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Sell,
                dec!(65000),
                dec!(0.5),
                dec!(10),
            ))
            .unwrap();

        // Same (65000-60000)*0.5 = 2500 the existing balance-based test
        // (`leveraged_partial_close_releases_proportional_margin_and_realizes_pnl`)
        // already asserts shows up in the credited balance -- now also
        // asserted directly against the new field.
        assert_eq!(
            *ledger.realized_pnl().get("BTCUSDT").unwrap(),
            dec!(2500)
        );
    }

    #[test]
    fn leveraged_open_and_add_does_not_write_realized_pnl() {
        let mut ledger = ledger_with_10k_usdt();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(60000),
                dec!(0.5),
                dec!(10),
            ))
            .unwrap();
        ledger
            .apply_fill(&leveraged_fill_for_symbol(
                "BTCUSDT",
                Side::Buy,
                dec!(62000),
                dec!(0.5),
                dec!(10),
            ))
            .unwrap();

        assert!(ledger.realized_pnl().get("BTCUSDT").is_none());
    }
```

Run: `cargo test -p apexes-backtest realized_pnl`
Expected: FAIL with "no method named `realized_pnl`" (not yet implemented) -- expected; Steps 3-5 implement it.

- [ ] **Step 3: Add the read-only accessor**

```rust
// crates/apexes-backtest/src/ledger.rs -- add alongside the existing
// `pub fn balances(&self)`/`pub fn positions(&self)` methods:
    pub fn realized_pnl(&self) -> &HashMap<String, Decimal> {
        &self.realized_pnl
    }
```

- [ ] **Step 4: Wire spot realized PnL into `update_position`**

Locate the existing `update_position` method's reduce/flip branch (the `else` arm of the `same_direction` check). Replace it with a version that also computes and accumulates realized PnL for the reduced portion:

```rust
// crates/apexes-backtest/src/ledger.rs -- inside `update_position`,
// replace the existing `} else { ... }` branch (the reduce/flip case)
// with:
        } else {
            // Reducing or flipping a position: the reduced portion
            // realizes PnL against the position's average entry price;
            // average entry price itself is unchanged for that portion.
            // If flipping past zero, the new position's entry price is
            // this fill's price (unchanged from before this task).
            let closing_quantity = position.quantity.abs().min(signed_quantity.abs());
            if !closing_quantity.is_zero() {
                let realized = if position.quantity.is_sign_positive() {
                    (fill.price - position.average_entry_price) * closing_quantity
                } else {
                    (position.average_entry_price - fill.price) * closing_quantity
                };
                *self
                    .realized_pnl
                    .entry(fill.symbol.clone())
                    .or_insert(Decimal::ZERO) += realized;
            }

            let new_quantity = position.quantity + signed_quantity;
            if position.quantity.abs() >= signed_quantity.abs() {
                position.quantity = new_quantity;
            } else {
                position.quantity = new_quantity;
                position.average_entry_price = fill.price;
            }
        }
```

Note: this reads `position.quantity`/`position.average_entry_price` (for computing `realized`) BEFORE the `position.quantity = new_quantity` mutation later in the same branch -- the existing code already had this ordering property (average entry price is read before being reassigned), this change only adds a computation that reads the same pre-mutation values, so no reordering of the existing mutations is needed. Because `position` is a `&mut Position` borrowed from `self.positions`, and `self.realized_pnl` is a different field, updating `self.realized_pnl` here requires ending the `position` borrow first or restructuring slightly -- do this by computing `realized_pnl_update: Option<(String, Decimal)>` before taking the mutable borrow of `position`, or by reading the needed values into local `Decimal`s before entering the mutable-borrow scope. Concretely, restructure `update_position` as:

```rust
    fn update_position(&mut self, fill: &Fill) {
        let signed_quantity = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };

        let existing = self.positions.get(&fill.symbol).cloned();
        let (existing_quantity, existing_average_entry_price) = existing
            .as_ref()
            .map(|p| (p.quantity, p.average_entry_price))
            .unwrap_or((Decimal::ZERO, Decimal::ZERO));

        let same_direction = existing_quantity.is_zero()
            || (existing_quantity.is_sign_positive() == signed_quantity.is_sign_positive());

        if !same_direction && !existing_quantity.is_zero() {
            let closing_quantity = existing_quantity.abs().min(signed_quantity.abs());
            if !closing_quantity.is_zero() {
                let realized = if existing_quantity.is_sign_positive() {
                    (fill.price - existing_average_entry_price) * closing_quantity
                } else {
                    (existing_average_entry_price - fill.price) * closing_quantity
                };
                *self
                    .realized_pnl
                    .entry(fill.symbol.clone())
                    .or_insert(Decimal::ZERO) += realized;
            }
        }

        let position = self
            .positions
            .entry(fill.symbol.clone())
            .or_insert(Position {
                symbol: fill.symbol.clone(),
                quantity: Decimal::ZERO,
                average_entry_price: Decimal::ZERO,
                leverage: None,
                margin_mode: None,
                allocated_margin: Decimal::ZERO,
            });

        if same_direction {
            let existing_value = position.quantity.abs() * position.average_entry_price;
            let new_value = signed_quantity.abs() * fill.price;
            let new_quantity = position.quantity + signed_quantity;
            if !new_quantity.is_zero() {
                position.average_entry_price = (existing_value + new_value) / new_quantity.abs();
            }
            position.quantity = new_quantity;
        } else {
            let new_quantity = position.quantity + signed_quantity;
            if position.quantity.abs() >= signed_quantity.abs() {
                position.quantity = new_quantity;
            } else {
                position.quantity = new_quantity;
                position.average_entry_price = fill.price;
            }
        }
    }
```

This is a pure refactor of the existing method's control flow (reads a cloned snapshot of the pre-fill position first to avoid the borrow conflict, computes realized PnL from that snapshot, THEN does the exact same mutation logic the method already had) -- the `same_direction`/quantity/average-entry-price mutation semantics are byte-for-byte unchanged from before this task; only the new realized-PnL bookkeeping is added.

- [ ] **Step 5: Wire leveraged realized PnL into `close_leveraged`**

Locate the existing `close_leveraged` method, which already computes a local `realized_pnl` variable for the closing portion (used today only to compute `self.credit(quote, margin_released + realized_pnl)`). Add one line accumulating it into the new field, right after that local variable is computed:

```rust
// crates/apexes-backtest/src/ledger.rs -- inside `close_leveraged`,
// immediately after the existing:
//     let realized_pnl = if position.quantity.is_sign_positive() {
//         (fill.price - position.average_entry_price) * closing_quantity
//     } else {
//         (position.average_entry_price - fill.price) * closing_quantity
//     };
// add:
        *self
            .realized_pnl
            .entry(fill.symbol.clone())
            .or_insert(Decimal::ZERO) += realized_pnl;
```

Place this line before the later `self.credit(...)` calls in the same method (ordering relative to `credit` calls does not matter functionally since they touch different fields, but keeping it near the `realized_pnl` computation keeps the code readable).

- [ ] **Step 6: Run this crate's tests**

Run: `cargo test -p apexes-backtest`
Expected: all pass, including every existing test (in particular `leveraged_partial_close_releases_proportional_margin_and_realizes_pnl`, `position_flips_sign_and_resets_entry_price_on_full_reversal`, `position_average_entry_price_is_volume_weighted`, and every other `ledger.rs` test, which must show byte-for-byte identical behavior to before this task) plus all seven new tests from Step 2.

- [ ] **Step 7: Run clippy, fmt, then a workspace-wide compile check**

Run: `cargo clippy -p apexes-backtest --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 8: Commit**

```bash
git add crates/apexes-backtest/src/ledger.rs
git commit -S -s -m "feat(apexes-backtest): PortfolioLedger::realized_pnl -- spot and leveraged close accounting"
```

---

### Task 3: `apexes-gateway-binance`: real balance/position/transfer/deposit-withdrawal-history APIs

**Files:**
- Modify: `crates/apexes-gateway-binance/src/gateway.rs`
- Create: `crates/apexes-gateway-binance/tests/wiremock_account.rs`

**Interfaces:**
- Consumes: `apexes_core::{WalletId, FundMovement, FundMovementStatus}` (Task 1); existing `BinanceRestClient::signed_request`, `ApiFamily`.
- Produces: `BinanceExecutionGateway::{account_balances, account_positions, internal_transfer, deposit_history, withdrawal_history}`.

- [ ] **Step 1: Write the failing wiremock tests**

```rust
// crates/apexes-gateway-binance/tests/wiremock_account.rs (new file)
//! Verifies `BinanceExecutionGateway`'s new account/fund-management
//! methods (balances, positions, internal transfer, deposit/withdrawal
//! history) send correctly-shaped, correctly-signed requests and map
//! Binance's response shapes correctly -- using a local `wiremock`
//! server, no real network access.

use apexes_core::{FundMovementStatus, WalletId};
use apexes_gateway_binance::{BinanceExecutionGateway, BinanceGatewayConfig};
use rust_decimal_macros::dec;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
async fn account_balances_merges_spot_and_futures_wallets() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/v3/account"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "balances": [
                {"asset": "USDT", "free": "1000.0", "locked": "0.0"},
                {"asset": "BTC", "free": "0.5", "locked": "0.1"}
            ]
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/fapi/v2/balance"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"asset": "USDT", "balance": "500.0"}
        ])))
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let balances = gateway.account_balances().await.unwrap();

    // Spot USDT (1000, free+locked) + Futures USDT (500) = 1500.
    assert_eq!(*balances.get("USDT").unwrap(), dec!(1500));
    // BTC: free 0.5 + locked 0.1 = 0.6, only present in Spot.
    assert_eq!(*balances.get("BTC").unwrap(), dec!(0.6));
}

#[tokio::test]
async fn account_positions_maps_futures_position_risk_response() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/fapi/v2/positionRisk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "symbol": "BTCUSDT",
                "positionAmt": "0.5",
                "entryPrice": "60000.0",
                "leverage": "10",
                "marginType": "isolated",
                "isolatedMargin": "3000.0"
            },
            {
                "symbol": "ETHUSDT",
                "positionAmt": "0.0",
                "entryPrice": "0.0",
                "leverage": "5",
                "marginType": "cross",
                "isolatedMargin": "0.0"
            }
        ])))
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let positions = gateway.account_positions().await.unwrap();

    // Zero-quantity entries (ETHUSDT here) are omitted -- no open position.
    assert_eq!(positions.len(), 1);
    let btc = positions.get("BTCUSDT").unwrap();
    assert_eq!(btc.quantity, dec!(0.5));
    assert_eq!(btc.average_entry_price, dec!(60000));
    assert_eq!(btc.leverage, Some(dec!(10)));
    assert_eq!(btc.allocated_margin, dec!(3000));
}

#[tokio::test]
async fn internal_transfer_sends_correct_transfer_type() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/sapi/v1/asset/transfer"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"tranId": 123456})),
        )
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let result = gateway
        .internal_transfer(WalletId::Spot, WalletId::UsdsFutures, "USDT", dec!(100))
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn internal_transfer_surfaces_explicit_rejection() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/sapi/v1/asset/transfer"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "code": -3020,
            "msg": "Transfer out amount exceeds max amount."
        })))
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let result = gateway
        .internal_transfer(WalletId::Spot, WalletId::UsdsFutures, "USDT", dec!(100))
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn deposit_history_maps_response_to_fund_movements() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/sapi/v1/capital/deposit/hisrec"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": "dep-1",
                "amount": "50.0",
                "coin": "USDT",
                "status": 1,
                "insertTime": 1700000000000i64
            }
        ])))
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let history = gateway.deposit_history().await.unwrap();

    assert_eq!(history.len(), 1);
    assert_eq!(history[0].asset, "USDT");
    assert_eq!(history[0].amount, dec!(50));
    assert_eq!(history[0].status, FundMovementStatus::Completed);
    assert_eq!(history[0].exchange_tx_id, "dep-1");
}

#[tokio::test]
async fn withdrawal_history_maps_response_to_fund_movements() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/sapi/v1/capital/withdraw/history"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": "wd-1",
                "amount": "20.0",
                "coin": "USDT",
                "status": 6,
                "applyTime": "2024-01-01 00:00:00"
            }
        ])))
        .mount(&mock_server)
        .await;

    let gateway = gateway_against(&mock_server).await;
    let history = gateway.withdrawal_history().await.unwrap();

    assert_eq!(history.len(), 1);
    assert_eq!(history[0].asset, "USDT");
    assert_eq!(history[0].amount, dec!(20));
    assert_eq!(history[0].status, FundMovementStatus::Completed);
    assert_eq!(history[0].exchange_tx_id, "wd-1");
}
```

Run: `cargo test -p apexes-gateway-binance --test wiremock_account`
Expected: FAIL with "no method named `account_balances`" etc. (not yet implemented) -- expected; Steps 2-6 implement them.

- [ ] **Step 2: Add `account_balances`**

```rust
// crates/apexes-gateway-binance/src/gateway.rs -- add near the other
// public methods on `impl BinanceExecutionGateway`:

#[derive(Debug, serde::Deserialize)]
struct SpotAccountResponse {
    balances: Vec<SpotBalanceEntry>,
}

#[derive(Debug, serde::Deserialize)]
struct SpotBalanceEntry {
    asset: String,
    free: Decimal,
    locked: Decimal,
}

#[derive(Debug, serde::Deserialize)]
struct FuturesBalanceEntry {
    asset: String,
    balance: Decimal,
}

impl BinanceExecutionGateway {
    /// Queries real Spot (`GET /api/v3/account`) and USDS-M Futures
    /// (`GET /fapi/v2/balance`) balances, merging both wallets into a
    /// single asset-symbol -> total-quantity map (matching
    /// `PortfolioLedger::balances`'s "one map, not split by wallet"
    /// convention). Spot's `free`+`locked` are summed (both are
    /// genuinely-held balance, just some of it reserved by open
    /// orders); the same asset present in both wallets is summed
    /// across them.
    pub async fn account_balances(&self) -> Result<HashMap<String, Decimal>, String> {
        let mut totals: HashMap<String, Decimal> = HashMap::new();

        let (status, body) = self
            .rest
            .signed_request("GET", ApiFamily::Spot, "/api/v3/account", "")
            .await?;
        if !status.is_success() {
            return Err(format!("account_balances (spot): HTTP {status}: {body}"));
        }
        let spot: SpotAccountResponse = serde_json::from_str(&body)
            .map_err(|error| format!("account_balances (spot): parse error: {error}; body={body}"))?;
        for entry in spot.balances {
            *totals.entry(entry.asset).or_insert(Decimal::ZERO) += entry.free + entry.locked;
        }

        let (status, body) = self
            .rest
            .signed_request("GET", ApiFamily::Futures, "/fapi/v2/balance", "")
            .await?;
        if !status.is_success() {
            return Err(format!("account_balances (futures): HTTP {status}: {body}"));
        }
        let futures: Vec<FuturesBalanceEntry> = serde_json::from_str(&body).map_err(|error| {
            format!("account_balances (futures): parse error: {error}; body={body}")
        })?;
        for entry in futures {
            *totals.entry(entry.asset).or_insert(Decimal::ZERO) += entry.balance;
        }

        Ok(totals)
    }
}
```

Add `use std::collections::HashMap;` to the top of `gateway.rs` if not already present (it is -- this file already imports it for `leverage_cache`).

- [ ] **Step 3: Add `account_positions`**

```rust
// crates/apexes-gateway-binance/src/gateway.rs -- add:

#[derive(Debug, serde::Deserialize)]
struct PositionRiskEntry {
    symbol: String,
    #[serde(rename = "positionAmt")]
    position_amt: Decimal,
    #[serde(rename = "entryPrice")]
    entry_price: Decimal,
    leverage: Decimal,
    #[serde(rename = "marginType")]
    margin_type: String,
    #[serde(rename = "isolatedMargin")]
    isolated_margin: Decimal,
}

impl BinanceExecutionGateway {
    /// Queries real USDS-M Futures positions (`GET
    /// /fapi/v2/positionRisk`), mapping each non-zero entry to
    /// `apexes_strategy::Position`. Zero-quantity entries (a symbol
    /// Binance always reports even with no open position) are omitted
    /// entirely -- matching `PortfolioLedger::positions()`'s convention
    /// of only containing symbols with an actual net position.
    pub async fn account_positions(&self) -> Result<HashMap<String, Position>, String> {
        let (status, body) = self
            .rest
            .signed_request("GET", ApiFamily::Futures, "/fapi/v2/positionRisk", "")
            .await?;
        if !status.is_success() {
            return Err(format!("account_positions: HTTP {status}: {body}"));
        }
        let entries: Vec<PositionRiskEntry> = serde_json::from_str(&body)
            .map_err(|error| format!("account_positions: parse error: {error}; body={body}"))?;

        let mut positions = HashMap::new();
        for entry in entries {
            if entry.position_amt.is_zero() {
                continue;
            }
            let margin_mode = if entry.margin_type == "isolated" {
                MarginMode::Isolated
            } else {
                MarginMode::Cross
            };
            positions.insert(
                entry.symbol.clone(),
                Position {
                    symbol: entry.symbol,
                    quantity: entry.position_amt,
                    average_entry_price: entry.entry_price,
                    leverage: Some(entry.leverage),
                    margin_mode: Some(margin_mode),
                    allocated_margin: entry.isolated_margin,
                },
            );
        }
        Ok(positions)
    }
}
```

Add `use apexes_strategy::Position;` to the top of `gateway.rs`. This introduces a new dependency from `apexes-gateway-binance` on `apexes-strategy` -- add it to `crates/apexes-gateway-binance/Cargo.toml`:

```toml
# crates/apexes-gateway-binance/Cargo.toml -- add to [dependencies]:
apexes-strategy = { path = "../apexes-strategy" }
```

- [ ] **Step 4: Add `internal_transfer`**

```rust
// crates/apexes-gateway-binance/src/gateway.rs -- add:

impl BinanceExecutionGateway {
    fn wallet_transfer_type(from: WalletId, to: WalletId) -> Result<&'static str, String> {
        match (from, to) {
            (WalletId::Spot, WalletId::UsdsFutures) => Ok("MAIN_UMFUTURE"),
            (WalletId::UsdsFutures, WalletId::Spot) => Ok("UMFUTURE_MAIN"),
            (WalletId::Spot, WalletId::Spot) | (WalletId::UsdsFutures, WalletId::UsdsFutures) => {
                Err("internal_transfer: from_wallet and to_wallet must differ".to_string())
            }
        }
    }

    /// Real wallet-to-wallet transfer (`POST /sapi/v1/asset/transfer`)
    /// within the SAME Binance account -- e.g. Spot <-> USDS-M
    /// Futures. Never transfers funds out of the account (no
    /// withdrawal support -- see this crate's design constraints).
    pub async fn internal_transfer(
        &self,
        from_wallet: WalletId,
        to_wallet: WalletId,
        asset: &str,
        amount: Decimal,
    ) -> Result<(), String> {
        let transfer_type = Self::wallet_transfer_type(from_wallet, to_wallet)?;
        let params = format!("type={transfer_type}&asset={asset}&amount={amount}");
        let (status, body) = self
            .rest
            .signed_request("POST", ApiFamily::Spot, "/sapi/v1/asset/transfer", &params)
            .await?;
        if !status.is_success() {
            return Err(format!("internal_transfer: HTTP {status}: {body}"));
        }
        Ok(())
    }
}
```

Add `use apexes_core::WalletId;` to the top of `gateway.rs`.

- [ ] **Step 5: Add `deposit_history`/`withdrawal_history`**

```rust
// crates/apexes-gateway-binance/src/gateway.rs -- add:

#[derive(Debug, serde::Deserialize)]
struct DepositHistoryEntry {
    id: String,
    amount: Decimal,
    coin: String,
    status: i32,
    #[serde(rename = "insertTime")]
    insert_time: i64,
}

#[derive(Debug, serde::Deserialize)]
struct WithdrawHistoryEntry {
    id: String,
    amount: Decimal,
    coin: String,
    status: i32,
    #[serde(rename = "applyTime")]
    apply_time: String,
}

impl BinanceExecutionGateway {
    /// Maps Binance's deposit-status integer codes to
    /// `FundMovementStatus`. Per Binance's documented deposit status
    /// codes: `0` = pending, `1` = success (completed), `6` = credited
    /// but the underlying blockchain transaction is unconfirmed
    /// (treated as `Pending` here -- not yet safely spendable);
    /// anything else maps to `Failed` (conservative default -- an
    /// unrecognized code should never silently read as `Completed`).
    fn deposit_status(code: i32) -> FundMovementStatus {
        match code {
            1 => FundMovementStatus::Completed,
            0 | 6 => FundMovementStatus::Pending,
            _ => FundMovementStatus::Failed,
        }
    }

    /// Maps Binance's withdrawal-status integer codes to
    /// `FundMovementStatus`. Per Binance's documented withdrawal status
    /// codes: `6` = completed; `0`/`1`/`2`/`4`/`5` = various in-flight
    /// states (all `Pending`); `3` = rejected, `7` = cancelled -- both
    /// `Failed` (did not result in a successful withdrawal).
    fn withdrawal_status(code: i32) -> FundMovementStatus {
        match code {
            6 => FundMovementStatus::Completed,
            3 | 7 => FundMovementStatus::Failed,
            _ => FundMovementStatus::Pending,
        }
    }

    /// Read-only query of real deposit history (`GET
    /// /sapi/v1/capital/deposit/hisrec`). This crate never implements
    /// deposit/withdrawal INITIATION -- only history queries, per this
    /// plan's explicit scope constraint.
    pub async fn deposit_history(&self) -> Result<Vec<FundMovement>, String> {
        let (status, body) = self
            .rest
            .signed_request("GET", ApiFamily::Spot, "/sapi/v1/capital/deposit/hisrec", "")
            .await?;
        if !status.is_success() {
            return Err(format!("deposit_history: HTTP {status}: {body}"));
        }
        let entries: Vec<DepositHistoryEntry> = serde_json::from_str(&body)
            .map_err(|error| format!("deposit_history: parse error: {error}; body={body}"))?;
        Ok(entries
            .into_iter()
            .map(|entry| FundMovement {
                asset: entry.coin,
                amount: entry.amount,
                status: Self::deposit_status(entry.status),
                timestamp: Utc
                    .timestamp_millis_opt(entry.insert_time)
                    .single()
                    .unwrap_or_else(Utc::now),
                exchange_tx_id: entry.id,
            })
            .collect())
    }

    /// Read-only query of real withdrawal history (`GET
    /// /sapi/v1/capital/withdraw/history`). Binance reports
    /// `applyTime` as a `"YYYY-MM-DD HH:MM:SS"` string (UTC) for this
    /// endpoint (unlike deposit history's millisecond epoch) --
    /// parsed accordingly; an unparseable timestamp falls back to
    /// `Utc::now()` rather than failing the whole call (matching
    /// `deposit_history`'s same fallback for a symmetrical, deliberately
    /// forgiving parsing policy on this best-effort historical field).
    pub async fn withdrawal_history(&self) -> Result<Vec<FundMovement>, String> {
        let (status, body) = self
            .rest
            .signed_request(
                "GET",
                ApiFamily::Spot,
                "/sapi/v1/capital/withdraw/history",
                "",
            )
            .await?;
        if !status.is_success() {
            return Err(format!("withdrawal_history: HTTP {status}: {body}"));
        }
        let entries: Vec<WithdrawHistoryEntry> = serde_json::from_str(&body)
            .map_err(|error| format!("withdrawal_history: parse error: {error}; body={body}"))?;
        Ok(entries
            .into_iter()
            .map(|entry| {
                let timestamp = chrono::NaiveDateTime::parse_from_str(
                    &entry.apply_time,
                    "%Y-%m-%d %H:%M:%S",
                )
                .map(|naive| naive.and_utc())
                .unwrap_or_else(|_| Utc::now());
                FundMovement {
                    asset: entry.coin,
                    amount: entry.amount,
                    status: Self::withdrawal_status(entry.status),
                    timestamp,
                    exchange_tx_id: entry.id,
                }
            })
            .collect())
    }
}
```

Add `use apexes_core::{FundMovement, FundMovementStatus};` to the top of `gateway.rs`.

- [ ] **Step 6: Run the new tests**

Run: `cargo test -p apexes-gateway-binance --test wiremock_account`
Expected: all six pass.

- [ ] **Step 7: Run this crate's tests, clippy, fmt, then a workspace-wide check**

Run: `cargo test -p apexes-gateway-binance`
Expected: all pass (existing `wiremock_rest.rs`/`wiremock_leverage.rs` tests unaffected).

Run: `cargo clippy -p apexes-gateway-binance --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 8: Commit**

```bash
git add crates/apexes-gateway-binance/Cargo.toml crates/apexes-gateway-binance/src/gateway.rs crates/apexes-gateway-binance/tests/wiremock_account.rs
git commit -S -s -m "feat(apexes-gateway-binance): real account_balances/account_positions/internal_transfer/deposit_history/withdrawal_history"
```

---

### Task 4: `apexes-account` crate skeleton -- `Account` trait, `ReconciliationTolerance`/`ReconciliationDiscrepancy`

**Files:**
- Create: `crates/apexes-account/Cargo.toml`
- Create: `crates/apexes-account/src/lib.rs`
- Create: `crates/apexes-account/src/account.rs`
- Create: `crates/apexes-account/src/reconciliation.rs`
- Modify: `Cargo.toml` (workspace members)

**Interfaces:**
- Consumes: `apexes_core::{ApexesError, WalletId, FundMovement, FundMovementStatus}` (Task 1); `apexes_strategy::Position`.
- Produces: `apexes_account::{Account, ReconciliationTolerance, ReconciliationDiscrepancy}`; re-exports `apexes_account::{WalletId, FundMovement, FundMovementStatus}`.

- [ ] **Step 1: Add the crate to the workspace**

```toml
# Cargo.toml -- add "crates/apexes-account" to the [workspace] members list
```

- [ ] **Step 2: Write `Cargo.toml` for the new crate**

```toml
# crates/apexes-account/Cargo.toml (new file)
[package]
name = "apexes-account"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-strategy = { path = "../apexes-strategy" }
apexes-backtest = { path = "../apexes-backtest" }
apexes-paper = { path = "../apexes-paper" }
apexes-gateway-binance = { path = "../apexes-gateway-binance" }
async-trait = { workspace = true }
rust_decimal = { workspace = true }
chrono = { workspace = true }
tokio = { workspace = true, features = ["sync"] }

[dev-dependencies]
wiremock = "0.6"
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
uuid = { workspace = true, features = ["v4"] }
```

- [ ] **Step 3: Write the failing tests for `ReconciliationTolerance`/`ReconciliationDiscrepancy`**

```rust
// crates/apexes-account/src/reconciliation.rs (new file)
//! [`ReconciliationTolerance`]/[`ReconciliationDiscrepancy`]: the
//! configurable-tolerance comparison result type used by
//! `Account::reconcile_against_exchange`. See
//! `docs/superpowers/specs/2026-08-02-apexes-account-design.md` §3.2.

use rust_decimal::Decimal;

/// Tolerance for `Account::reconcile_against_exchange`: a discrepancy
/// is only reported if it exceeds BOTH the absolute AND relative
/// thresholds (satisfying either is enough to be considered "within
/// tolerance") -- the more permissive combination, chosen so a small
/// asset's proportionally-large-looking relative error isn't
/// over-reported.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReconciliationTolerance {
    pub absolute: Decimal,
    /// e.g. `dec!(0.01)` for 1%.
    pub relative_fraction: Decimal,
}

impl ReconciliationTolerance {
    /// Returns `true` if `|local - exchange|` is within tolerance
    /// (either the absolute threshold OR the relative-to-`local`
    /// threshold is satisfied).
    pub fn within(&self, local: Decimal, exchange: Decimal) -> bool {
        let diff = (local - exchange).abs();
        if diff <= self.absolute {
            return true;
        }
        if local.is_zero() {
            return false;
        }
        let relative = diff / local.abs();
        relative <= self.relative_fraction
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReconciliationDiscrepancy {
    pub asset_or_symbol: String,
    pub local_value: Decimal,
    pub exchange_value: Decimal,
    pub difference: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn tolerance() -> ReconciliationTolerance {
        ReconciliationTolerance {
            absolute: dec!(1),
            relative_fraction: dec!(0.01),
        }
    }

    #[test]
    fn within_absolute_tolerance_passes() {
        assert!(tolerance().within(dec!(100), dec!(100.5)));
    }

    #[test]
    fn within_relative_tolerance_passes_even_when_absolute_diff_exceeds_threshold() {
        // diff = 5, exceeds absolute tolerance of 1, but 5/1000 = 0.5%
        // is within the 1% relative tolerance.
        assert!(tolerance().within(dec!(1000), dec!(1005)));
    }

    #[test]
    fn exceeding_both_thresholds_fails() {
        assert!(!tolerance().within(dec!(100), dec!(150)));
    }

    #[test]
    fn zero_local_value_only_uses_absolute_tolerance() {
        assert!(tolerance().within(Decimal::ZERO, dec!(0.5)));
        assert!(!tolerance().within(Decimal::ZERO, dec!(5)));
    }

    #[test]
    fn discrepancy_is_constructible_and_comparable() {
        let a = ReconciliationDiscrepancy {
            asset_or_symbol: "USDT".to_string(),
            local_value: dec!(100),
            exchange_value: dec!(95),
            difference: dec!(5),
        };
        let b = a.clone();
        assert_eq!(a, b);
    }
}
```

Run: `cargo test -p apexes-account reconciliation`
Expected: FAIL (crate not yet registered in `lib.rs`) -- expected; Step 5 registers it.

- [ ] **Step 4: Write the `Account` trait**

```rust
// crates/apexes-account/src/account.rs (new file)
//! [`Account`]: unified read/write interface over balances, positions,
//! realized PnL, equity, fund-movement history, internal transfers,
//! and reconciliation -- implemented identically across backtest
//! (`apexes_backtest::PortfolioLedger`), paper (`apexes_paper::PaperAccount`),
//! and live (`apexes_account::LiveAccount`) environments. See
//! `docs/superpowers/specs/2026-08-02-apexes-account-design.md` §3.1.

use std::collections::HashMap;

use apexes_core::{ApexesError, FundMovement, WalletId};
use apexes_strategy::Position;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::fx_rate::FxRateProvider;
use crate::reconciliation::{ReconciliationDiscrepancy, ReconciliationTolerance};

#[async_trait]
pub trait Account: Send + Sync {
    /// Per-asset balance snapshot.
    fn balances(&self) -> HashMap<String, Decimal>;

    /// Per-symbol current net position snapshot.
    fn positions(&self) -> HashMap<String, Position>;

    /// Per-symbol accumulated realized profit/loss.
    fn realized_pnl(&self) -> HashMap<String, Decimal>;

    /// Total equity, converted to `valuation_currency` using
    /// `mark_prices` (same convention as
    /// `apexes_backtest::PortfolioLedger::equity`) with `fx_provider`
    /// as a fallback for assets not present in `mark_prices`.
    fn equity(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        fx_provider: &dyn FxRateProvider,
        valuation_currency: &str,
    ) -> Result<Decimal, ApexesError>;

    /// Read-only deposit history. Never initiates a deposit.
    async fn deposit_history(&self) -> Result<Vec<FundMovement>, ApexesError>;

    /// Read-only withdrawal history. Never initiates a withdrawal --
    /// this crate has no withdrawal-initiation code path anywhere.
    async fn withdrawal_history(&self) -> Result<Vec<FundMovement>, ApexesError>;

    /// Real wallet-to-wallet transfer within the same account (e.g.
    /// Binance Spot <-> USDS-M Futures). backtest/paper environments
    /// return `Err(ApexesError::UnsupportedForEnvironment)`.
    async fn internal_transfer(
        &self,
        from_wallet: WalletId,
        to_wallet: WalletId,
        asset: &str,
        amount: Decimal,
    ) -> Result<(), ApexesError>;

    /// Compares real exchange balances/positions against local state,
    /// returning discrepancies exceeding `tolerance`. NEVER mutates
    /// local state -- alert-only. backtest/paper environments return
    /// an empty `Vec` (local state IS the source of truth there, so
    /// reconciliation is trivially satisfied, not an error).
    async fn reconcile_against_exchange(
        &self,
        tolerance: ReconciliationTolerance,
    ) -> Result<Vec<ReconciliationDiscrepancy>, ApexesError>;
}
```

- [ ] **Step 5: Register the modules**

> This task delivers only `account.rs`/`fx_rate.rs`/`reconciliation.rs` -- `live_account.rs`/`pnl.rs`/`registry.rs` are Tasks 7/8's deliverables and must NOT be created yet (per this plan's "every task ends in a working, independently-testable state" rule, `lib.rs` must only reference modules that exist by the end of THIS task). `account.rs`'s `use crate::fx_rate::FxRateProvider;` needs `fx_rate.rs` to exist with at least the trait declaration -- this task creates it with ONLY the `FxRateProvider` trait (no `StaticFxRateProvider` implementation yet; that's added in Task 5).

```rust
// crates/apexes-account/src/fx_rate.rs (new file) -- this task adds
// only the trait; Task 5 adds StaticFxRateProvider.
//! [`FxRateProvider`]: pluggable cross-currency rate lookup for
//! `Account::equity`. See
//! `docs/superpowers/specs/2026-08-02-apexes-account-design.md` §3.3
//! for why this is synchronous, not `async`.

use apexes_core::ApexesError;
use rust_decimal::Decimal;

pub trait FxRateProvider: Send + Sync {
    fn fx_rate(&self, from_currency: &str, to_currency: &str) -> Result<Decimal, ApexesError>;
}
```

```rust
// crates/apexes-account/src/lib.rs (new file)
//! Apexes account & fund management: a unified `Account` trait across
//! backtest, paper, and live-trading environments, real Binance
//! balance/position/transfer/deposit-withdrawal-history integration,
//! cross-currency valuation (`FxRateProvider`), multi-account
//! aggregation (`AccountRegistry`), and alert-only reconciliation
//! against the real exchange. Covers `docs/FEATURES.md` rows 7.1/7.2/7.3.

pub mod account;
pub mod fx_rate;
pub mod reconciliation;

pub use account::Account;
pub use apexes_core::{FundMovement, FundMovementStatus, WalletId};
pub use fx_rate::FxRateProvider;
pub use reconciliation::{ReconciliationDiscrepancy, ReconciliationTolerance};
```

- [ ] **Step 6: Run this crate's tests**

Run: `cargo test -p apexes-account`
Expected: all 5 tests from Step 3 pass. `account.rs` has no tests yet (it's a trait definition only, implementations come in Tasks 6/7) but must compile.

- [ ] **Step 7: Run clippy, fmt, then a workspace-wide compile check**

Run: `cargo clippy -p apexes-account --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/apexes-account
git commit -S -s -m "feat(apexes-account): new crate skeleton -- Account trait, ReconciliationTolerance/Discrepancy, FxRateProvider trait"
```

---

### Task 5: `apexes-account`: `StaticFxRateProvider`

**Files:**
- Modify: `crates/apexes-account/src/fx_rate.rs`
- Modify: `crates/apexes-account/src/lib.rs`

**Interfaces:**
- Consumes: `FxRateProvider` (Task 4).
- Produces: `apexes_account::StaticFxRateProvider`.

- [ ] **Step 1: Write the failing tests**

Add to `crates/apexes-account/src/fx_rate.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    #[test]
    fn same_currency_is_always_rate_one() {
        let provider = StaticFxRateProvider::new(HashMap::new());
        assert_eq!(provider.fx_rate("USDT", "USDT").unwrap(), dec!(1));
    }

    #[test]
    fn looks_up_configured_cross_currency_rate() {
        let mut rates = HashMap::new();
        rates.insert(("USDT".to_string(), "CNY".to_string()), dec!(7.2));
        let provider = StaticFxRateProvider::new(rates);

        assert_eq!(provider.fx_rate("USDT", "CNY").unwrap(), dec!(7.2));
    }

    #[test]
    fn missing_rate_is_an_error() {
        let provider = StaticFxRateProvider::new(HashMap::new());
        let result = provider.fx_rate("USDT", "CNY");
        assert!(result.is_err());
    }
}
```

Run: `cargo test -p apexes-account fx_rate`
Expected: FAIL with "cannot find type `StaticFxRateProvider`" -- expected; Step 2 implements it.

- [ ] **Step 2: Implement `StaticFxRateProvider`**

```rust
// crates/apexes-account/src/fx_rate.rs -- add below the existing
// `FxRateProvider` trait:

use std::collections::HashMap;

/// A configuration-driven, in-memory FX rate table. `from_currency ==
/// to_currency` always returns `Decimal::ONE` without needing an entry
/// in `rates`. Real (asynchronous, network-backed) FX data sources are
/// explicitly out of scope for this crate -- see the design spec's
/// "not included" section.
pub struct StaticFxRateProvider {
    rates: HashMap<(String, String), Decimal>,
}

impl StaticFxRateProvider {
    pub fn new(rates: HashMap<(String, String), Decimal>) -> Self {
        Self { rates }
    }
}

impl FxRateProvider for StaticFxRateProvider {
    fn fx_rate(&self, from_currency: &str, to_currency: &str) -> Result<Decimal, ApexesError> {
        if from_currency == to_currency {
            return Ok(Decimal::ONE);
        }
        self.rates
            .get(&(from_currency.to_string(), to_currency.to_string()))
            .copied()
            .ok_or_else(|| ApexesError::InvalidBacktestConfig {
                reason: format!("no FX rate configured for {from_currency} -> {to_currency}"),
            })
    }
}
```

Note: reusing `ApexesError::InvalidBacktestConfig` here (rather than adding a new variant) matches this project's existing convention of reusing descriptive-`reason`-string variants across crates rather than adding a narrow new variant per call site (the same pattern `ApexesError::UnsupportedForEnvironment`'s own doc comment in Task 1 notes for `internal_transfer`).

- [ ] **Step 3: Register the export**

```rust
// crates/apexes-account/src/lib.rs -- change:
// pub use fx_rate::FxRateProvider;
// to:
pub use fx_rate::{FxRateProvider, StaticFxRateProvider};
```

- [ ] **Step 4: Run this crate's tests**

Run: `cargo test -p apexes-account`
Expected: all pass (5 from Task 4 + 3 new).

- [ ] **Step 5: Run clippy, fmt, then a workspace-wide check**

Run: `cargo clippy -p apexes-account --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-account/src/fx_rate.rs crates/apexes-account/src/lib.rs
git commit -S -s -m "feat(apexes-account): StaticFxRateProvider"
```

---

### Task 6: `impl Account for PortfolioLedger` and `impl Account for PaperAccount`

**Files:**
- Create: `crates/apexes-account/src/ledger_impl.rs` (impl `Account` for `PortfolioLedger`)
- Create: `crates/apexes-account/src/paper_impl.rs` (impl `Account` for `PaperAccount`)
- Modify: `crates/apexes-account/src/lib.rs`
- Modify: `crates/apexes-paper/src/account.rs` (new `realized_pnl_snapshot` method, needed by `paper_impl.rs`)

**Interfaces:**
- Consumes: `apexes_account::{Account, FxRateProvider, ReconciliationTolerance, ReconciliationDiscrepancy}` (Tasks 4-5); `PortfolioLedger::realized_pnl` (Task 2).
- Produces: `impl Account for PortfolioLedger`; `impl Account for PaperAccount<'_>`; `PaperAccount::realized_pnl_snapshot`.

> **Why these impls live in `apexes-account`, not in `apexes-backtest`/`apexes-paper`**: per the design's own dependency-direction section (§二), `apexes-account` depends on `apexes-backtest`/`apexes-paper` (not the other way around) specifically so it can implement `Account` FOR their types. Rust's orphan rule allows a crate to `impl SomeLocalTrait for SomeForeignType` as long as it defines the trait (even if it doesn't define the type) -- since `apexes-account` defines `Account`, it's the right place for `impl Account for PortfolioLedger`/`impl Account for PaperAccount`, not `apexes-backtest`/`apexes-paper` themselves (which don't and must not depend on `apexes-account`, avoiding a cycle). Write both impls as new files inside `apexes-account`'s own `src/`.

- [ ] **Step 1: Write the failing tests for `impl Account for PortfolioLedger`**

```rust
// crates/apexes-account/src/ledger_impl.rs (new file)
//! `impl Account for apexes_backtest::PortfolioLedger` -- the backtest
//! environment's implementation. Deposit/withdrawal history and
//! internal transfers are meaningless for a backtest ledger (no real
//! wallets), and reconciliation is trivially satisfied (local state IS
//! the source of truth) -- see the behavior-differences table in
//! `docs/superpowers/specs/2026-08-02-apexes-account-design.md` §六.

#[cfg(test)]
mod tests {
    use crate::account::Account;
    use crate::fx_rate::StaticFxRateProvider;
    use crate::reconciliation::ReconciliationTolerance;
    use apexes_backtest::PortfolioLedger;
    use apexes_core::{AssetClass, Instrument, MarketType, Venue};
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn btc_usdt() -> Instrument {
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

    fn ledger_with_10k_usdt() -> PortfolioLedger {
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));
        PortfolioLedger::new(instruments, initial_balances)
    }

    #[test]
    fn account_balances_delegates_to_ledger() {
        let ledger = ledger_with_10k_usdt();
        let balances = Account::balances(&ledger);
        assert_eq!(*balances.get("USDT").unwrap(), dec!(10000));
    }

    #[test]
    fn account_positions_delegates_to_ledger() {
        let ledger = ledger_with_10k_usdt();
        assert!(Account::positions(&ledger).is_empty());
    }

    #[test]
    fn account_realized_pnl_delegates_to_ledger() {
        let ledger = ledger_with_10k_usdt();
        assert!(Account::realized_pnl(&ledger).is_empty());
    }

    #[test]
    fn account_equity_matches_ledger_equity_with_no_fx_needed() {
        let ledger = ledger_with_10k_usdt();
        let fx = StaticFxRateProvider::new(HashMap::new());
        let mark_prices = HashMap::new();

        let equity = Account::equity(&ledger, &mark_prices, &fx, "USDT").unwrap();
        assert_eq!(equity, dec!(10000));
    }

    #[tokio::test]
    async fn deposit_and_withdrawal_history_are_empty_for_backtest() {
        let ledger = ledger_with_10k_usdt();
        assert!(Account::deposit_history(&ledger).await.unwrap().is_empty());
        assert!(
            Account::withdrawal_history(&ledger)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn internal_transfer_is_unsupported_for_backtest() {
        let ledger = ledger_with_10k_usdt();
        let result = Account::internal_transfer(
            &ledger,
            apexes_core::WalletId::Spot,
            apexes_core::WalletId::UsdsFutures,
            "USDT",
            dec!(100),
        )
        .await;
        assert!(matches!(
            result,
            Err(apexes_core::ApexesError::UnsupportedForEnvironment { .. })
        ));
    }

    #[tokio::test]
    async fn reconcile_against_exchange_is_trivially_empty_for_backtest() {
        let ledger = ledger_with_10k_usdt();
        let tolerance = ReconciliationTolerance {
            absolute: dec!(1),
            relative_fraction: dec!(0.01),
        };
        let discrepancies = Account::reconcile_against_exchange(&ledger, tolerance)
            .await
            .unwrap();
        assert!(discrepancies.is_empty());
    }
}
```

Run: `cargo test -p apexes-account ledger_impl`
Expected: FAIL with "the trait `Account` is not implemented for `PortfolioLedger`" -- expected; Step 2 implements it.

- [ ] **Step 2: Implement `Account for PortfolioLedger`**

```rust
// crates/apexes-account/src/ledger_impl.rs -- add above the
// #[cfg(test)] block:

use std::collections::HashMap;

use apexes_backtest::PortfolioLedger;
use apexes_core::{ApexesError, FundMovement, WalletId};
use apexes_strategy::Position;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::account::Account;
use crate::fx_rate::FxRateProvider;
use crate::reconciliation::{ReconciliationDiscrepancy, ReconciliationTolerance};

#[async_trait]
impl Account for PortfolioLedger {
    fn balances(&self) -> HashMap<String, Decimal> {
        self.balances().clone()
    }

    fn positions(&self) -> HashMap<String, Position> {
        self.positions().clone()
    }

    fn realized_pnl(&self) -> HashMap<String, Decimal> {
        self.realized_pnl().clone()
    }

    fn equity(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        _fx_provider: &dyn FxRateProvider,
        _valuation_currency: &str,
    ) -> Result<Decimal, ApexesError> {
        // A backtest ledger's balances are already assumed
        // single-market/single-quote-currency (its existing `equity`
        // contract) -- no FX fallback is exercised in practice today,
        // but the parameter is threaded through for interface
        // uniformity with `LiveAccount`, which DOES need it once a
        // second real market exists.
        PortfolioLedger::equity(self, mark_prices)
    }

    async fn deposit_history(&self) -> Result<Vec<FundMovement>, ApexesError> {
        Ok(Vec::new())
    }

    async fn withdrawal_history(&self) -> Result<Vec<FundMovement>, ApexesError> {
        Ok(Vec::new())
    }

    async fn internal_transfer(
        &self,
        _from_wallet: WalletId,
        _to_wallet: WalletId,
        _asset: &str,
        _amount: Decimal,
    ) -> Result<(), ApexesError> {
        Err(ApexesError::UnsupportedForEnvironment {
            reason: "backtest ledgers have no real wallets to transfer between".to_string(),
        })
    }

    async fn reconcile_against_exchange(
        &self,
        _tolerance: ReconciliationTolerance,
    ) -> Result<Vec<ReconciliationDiscrepancy>, ApexesError> {
        Ok(Vec::new())
    }
}
```

Note the `fn balances(&self) -> HashMap<String, Decimal> { self.balances().clone() }` line calls `PortfolioLedger`'s own INHERENT `balances()` method (returns `&HashMap`) and clones it -- this is legal because Rust resolves `self.balances()` to the inherent method first (inherent methods take priority over trait methods with the same name), so there is no infinite-recursion risk despite the same-name call.

Update `crates/apexes-account/Cargo.toml` -- `apexes-strategy` is already a dependency (added in Task 4), and `apexes-backtest` is already a dependency (added in Task 4); no new dependency lines needed here.

- [ ] **Step 3: Register the module**

```rust
// crates/apexes-account/src/lib.rs -- add:
pub mod ledger_impl;
```

(No new `pub use` needed -- `impl Account for PortfolioLedger` is used via the `Account` trait already exported, not via a new named export.)

- [ ] **Step 4: Run the new tests**

Run: `cargo test -p apexes-account ledger_impl`
Expected: all 7 pass.

- [ ] **Step 5: Write the failing tests for `impl Account for PaperAccount`**

```rust
// crates/apexes-account/src/paper_impl.rs (new file)
//! `impl Account for apexes_paper::PaperAccount` -- delegates to the
//! `&PortfolioLedger` it wraps, exactly like `PaperAccount`'s own
//! existing `balances_snapshot`/`positions_snapshot`/`equity_snapshot`
//! methods already do.

#[cfg(test)]
mod tests {
    use crate::account::Account;
    use crate::fx_rate::StaticFxRateProvider;
    use apexes_backtest::PortfolioLedger;
    use apexes_core::{AssetClass, Instrument, MarketType, Venue};
    use apexes_paper::PaperAccount;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn btc_usdt() -> Instrument {
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

    fn ledger_with_10k_usdt() -> PortfolioLedger {
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));
        PortfolioLedger::new(instruments, initial_balances)
    }

    #[test]
    fn paper_account_balances_matches_underlying_ledger() {
        let ledger = ledger_with_10k_usdt();
        let paper_account = PaperAccount::new(&ledger);

        let balances = Account::balances(&paper_account);
        assert_eq!(*balances.get("USDT").unwrap(), dec!(10000));
    }

    #[test]
    fn paper_account_equity_matches_underlying_ledger() {
        let ledger = ledger_with_10k_usdt();
        let paper_account = PaperAccount::new(&ledger);
        let fx = StaticFxRateProvider::new(HashMap::new());
        let mark_prices = HashMap::new();

        let equity = Account::equity(&paper_account, &mark_prices, &fx, "USDT").unwrap();
        assert_eq!(equity, dec!(10000));
    }

    #[tokio::test]
    async fn paper_account_internal_transfer_is_unsupported() {
        let ledger = ledger_with_10k_usdt();
        let paper_account = PaperAccount::new(&ledger);

        let result = Account::internal_transfer(
            &paper_account,
            apexes_core::WalletId::Spot,
            apexes_core::WalletId::UsdsFutures,
            "USDT",
            dec!(100),
        )
        .await;
        assert!(matches!(
            result,
            Err(apexes_core::ApexesError::UnsupportedForEnvironment { .. })
        ));
    }
}
```

Run: `cargo test -p apexes-account paper_impl`
Expected: FAIL with "the trait `Account` is not implemented for `PaperAccount<'_>`" -- expected; Step 6 implements it.

- [ ] **Step 6: Implement `Account for PaperAccount<'_>`**

```rust
// crates/apexes-account/src/paper_impl.rs -- add above the
// #[cfg(test)] block:

use std::collections::HashMap;

use apexes_core::{ApexesError, FundMovement, WalletId};
use apexes_paper::PaperAccount;
use apexes_strategy::Position;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::account::Account;
use crate::fx_rate::FxRateProvider;
use crate::reconciliation::{ReconciliationDiscrepancy, ReconciliationTolerance};

#[async_trait]
impl Account for PaperAccount<'_> {
    fn balances(&self) -> HashMap<String, Decimal> {
        self.balances_snapshot()
    }

    fn positions(&self) -> HashMap<String, Position> {
        self.positions_snapshot()
    }

    fn realized_pnl(&self) -> HashMap<String, Decimal> {
        // PaperAccount has no existing realized_pnl_snapshot method --
        // add one now, mirroring balances_snapshot/positions_snapshot's
        // existing delegation pattern (see crates/apexes-paper/src/account.rs).
        self.realized_pnl_snapshot()
    }

    fn equity(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        _fx_provider: &dyn FxRateProvider,
        _valuation_currency: &str,
    ) -> Result<Decimal, ApexesError> {
        self.equity_snapshot(mark_prices)
    }

    async fn deposit_history(&self) -> Result<Vec<FundMovement>, ApexesError> {
        Ok(Vec::new())
    }

    async fn withdrawal_history(&self) -> Result<Vec<FundMovement>, ApexesError> {
        Ok(Vec::new())
    }

    async fn internal_transfer(
        &self,
        _from_wallet: WalletId,
        _to_wallet: WalletId,
        _asset: &str,
        _amount: Decimal,
    ) -> Result<(), ApexesError> {
        Err(ApexesError::UnsupportedForEnvironment {
            reason: "paper trading accounts have no real wallets to transfer between".to_string(),
        })
    }

    async fn reconcile_against_exchange(
        &self,
        _tolerance: ReconciliationTolerance,
    ) -> Result<Vec<ReconciliationDiscrepancy>, ApexesError> {
        Ok(Vec::new())
    }
}
```

This references a new `PaperAccount::realized_pnl_snapshot` method that doesn't exist yet -- add it to `apexes-paper` (a small, additive change mirroring the existing three snapshot methods exactly):

```rust
// crates/apexes-paper/src/account.rs -- add alongside the existing
// balances_snapshot/positions_snapshot/equity_snapshot methods:
    pub fn realized_pnl_snapshot(&self) -> HashMap<String, Decimal> {
        self.ledger.realized_pnl().clone()
    }
```

Add a test for it in `apexes-paper`'s existing test module in the same file, reusing its existing `ledger_with_usdt_balance` helper (already defined there, used by `balances_snapshot_matches_ledger_balances` and the other tests in this module):

```rust
    #[test]
    fn realized_pnl_snapshot_matches_ledger_realized_pnl() {
        let ledger = ledger_with_usdt_balance(dec!(1000));
        let account = PaperAccount::new(&ledger);
        assert_eq!(
            account.realized_pnl_snapshot(),
            ledger.realized_pnl().clone()
        );
    }
```

- [ ] **Step 7: Register the module**

```rust
// crates/apexes-account/src/lib.rs -- add:
pub mod paper_impl;
```

- [ ] **Step 8: Run this crate's and apexes-paper's tests**

Run: `cargo test -p apexes-account`
Expected: all pass (12 from Tasks 4-5 + 7 from Step 4 + 3 new from Step 6, plus any placeholder-module tests).

Run: `cargo test -p apexes-paper`
Expected: all pass, including the new `realized_pnl_snapshot_matches_ledger_realized_pnl` test.

- [ ] **Step 9: Run clippy, fmt, then a workspace-wide check**

Run: `cargo clippy -p apexes-account -p apexes-paper --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 10: Commit**

```bash
git add crates/apexes-account/src/lib.rs crates/apexes-account/src/ledger_impl.rs crates/apexes-account/src/paper_impl.rs crates/apexes-paper/src/account.rs
git commit -S -s -m "feat(apexes-account,apexes-paper): impl Account for PortfolioLedger and PaperAccount"
```

---

### Task 7: `apexes-account`: `LiveAccount`

**Files:**
- Create: `crates/apexes-account/src/live_account.rs`
- Modify: `crates/apexes-account/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_gateway_binance::{BinanceExecutionGateway, BinanceGatewayConfig}` (existing); `account_balances`/`account_positions`/`internal_transfer`/`deposit_history`/`withdrawal_history` (Task 3); `Account` trait (Task 4).
- Produces: `apexes_account::LiveAccount`.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/apexes-account/src/live_account.rs (new file)
//! [`LiveAccount`]: wraps a real `Arc<BinanceExecutionGateway>`,
//! implementing `Account` by maintaining a local cache (updated via
//! `refresh()`) for the synchronous balance/position/realized-pnl/
//! equity methods, and forwarding directly to the gateway for
//! deposit/withdrawal history, internal transfers, and reconciliation
//! (all of which need a fresh real network request every call). See
//! `docs/superpowers/specs/2026-08-02-apexes-account-design.md` §3.5.

#[cfg(test)]
mod tests {
    use crate::account::Account;
    use crate::fx_rate::StaticFxRateProvider;
    use crate::live_account::LiveAccount;
    use crate::reconciliation::ReconciliationTolerance;
    use apexes_gateway_binance::{BinanceExecutionGateway, BinanceGatewayConfig};
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use std::sync::Arc;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn gateway_against(mock_server: &MockServer) -> Arc<BinanceExecutionGateway> {
        Arc::new(BinanceExecutionGateway::new(BinanceGatewayConfig {
            api_key: "test-api-key".to_string(),
            api_secret: "test-api-secret".to_string(),
            spot_base_url: mock_server.uri(),
            futures_base_url: mock_server.uri(),
            recv_window_ms: 5000,
        }))
    }

    fn mount_empty_balances_and_positions(mock_server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/api/v3/account"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"balances": []})),
            )
            .mount(mock_server);
        Mock::given(method("GET"))
            .and(path("/fapi/v2/balance"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(mock_server);
        Mock::given(method("GET"))
            .and(path("/fapi/v2/positionRisk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(mock_server);
    }

    #[tokio::test]
    async fn refresh_populates_balances_from_the_gateway() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v3/account"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "balances": [{"asset": "USDT", "free": "1000.0", "locked": "0.0"}]
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/balance"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/positionRisk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&mock_server)
            .await;

        let gateway = gateway_against(&mock_server).await;
        let live_account = LiveAccount::new(gateway);
        live_account.refresh().await.unwrap();

        let balances = Account::balances(&live_account);
        assert_eq!(*balances.get("USDT").unwrap(), dec!(1000));
    }

    #[tokio::test]
    async fn balances_before_first_refresh_are_empty() {
        let mock_server = MockServer::start().await;
        mount_empty_balances_and_positions(&mock_server);
        let gateway = gateway_against(&mock_server).await;
        let live_account = LiveAccount::new(gateway);

        assert!(Account::balances(&live_account).is_empty());
    }

    #[tokio::test]
    async fn deposit_history_forwards_to_the_gateway() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/sapi/v1/capital/deposit/hisrec"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"id": "dep-1", "amount": "10.0", "coin": "USDT", "status": 1, "insertTime": 1700000000000i64}
            ])))
            .mount(&mock_server)
            .await;

        let gateway = gateway_against(&mock_server).await;
        let live_account = LiveAccount::new(gateway);

        let history = Account::deposit_history(&live_account).await.unwrap();
        assert_eq!(history.len(), 1);
    }

    #[tokio::test]
    async fn internal_transfer_forwards_to_the_gateway() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/sapi/v1/asset/transfer"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"tranId": 1})),
            )
            .mount(&mock_server)
            .await;

        let gateway = gateway_against(&mock_server).await;
        let live_account = LiveAccount::new(gateway);

        let result = Account::internal_transfer(
            &live_account,
            apexes_core::WalletId::Spot,
            apexes_core::WalletId::UsdsFutures,
            "USDT",
            dec!(50),
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn reconcile_against_exchange_reports_discrepancy_beyond_tolerance() {
        let mock_server = MockServer::start().await;
        // Local cache will be empty (no refresh() called); exchange
        // reports a real USDT balance -- a large discrepancy.
        Mock::given(method("GET"))
            .and(path("/api/v3/account"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "balances": [{"asset": "USDT", "free": "1000.0", "locked": "0.0"}]
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/balance"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/positionRisk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&mock_server)
            .await;

        let gateway = gateway_against(&mock_server).await;
        let live_account = LiveAccount::new(gateway);
        let tolerance = ReconciliationTolerance {
            absolute: dec!(1),
            relative_fraction: dec!(0.01),
        };

        let discrepancies = Account::reconcile_against_exchange(&live_account, tolerance)
            .await
            .unwrap();
        assert_eq!(discrepancies.len(), 1);
        assert_eq!(discrepancies[0].asset_or_symbol, "USDT");
        assert_eq!(discrepancies[0].exchange_value, dec!(1000));
    }

    #[tokio::test]
    async fn reconcile_against_exchange_reports_nothing_within_tolerance() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v3/account"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "balances": [{"asset": "USDT", "free": "1000.0", "locked": "0.0"}]
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/balance"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v2/positionRisk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&mock_server)
            .await;

        let gateway = gateway_against(&mock_server).await;
        let live_account = LiveAccount::new(gateway);
        live_account.refresh().await.unwrap();

        let tolerance = ReconciliationTolerance {
            absolute: dec!(1),
            relative_fraction: dec!(0.01),
        };
        let discrepancies = Account::reconcile_against_exchange(&live_account, tolerance)
            .await
            .unwrap();
        assert!(discrepancies.is_empty());
    }
}
```

Run: `cargo test -p apexes-account live_account`
Expected: FAIL with "cannot find type `LiveAccount`" -- expected; Step 2 implements it.

- [ ] **Step 2: Implement `LiveAccount`**

```rust
// crates/apexes-account/src/live_account.rs -- add above the
// #[cfg(test)] block:

use std::collections::HashMap;
use std::sync::Arc;

use apexes_core::{ApexesError, FundMovement, WalletId};
use apexes_gateway_binance::BinanceExecutionGateway;
use apexes_strategy::Position;
use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::RwLock;

use crate::account::Account;
use crate::fx_rate::FxRateProvider;
use crate::reconciliation::{ReconciliationDiscrepancy, ReconciliationTolerance};

/// Wraps a real `Arc<BinanceExecutionGateway>`. The synchronous
/// `Account` methods (`balances`/`positions`/`realized_pnl`/`equity`)
/// read a local cache populated by `refresh()` -- NOT a fresh network
/// call every time, matching `Account::balances`'s synchronous
/// signature. `reconcile_against_exchange` always issues a fresh
/// request (comparing against the cache, not refreshing it) -- the
/// whole point of reconciliation is catching a STALE cache, so it must
/// not silently refresh the cache first (that would make every
/// reconciliation trivially pass).
pub struct LiveAccount {
    gateway: Arc<BinanceExecutionGateway>,
    cached_balances: RwLock<HashMap<String, Decimal>>,
    cached_positions: RwLock<HashMap<String, Position>>,
}

impl LiveAccount {
    pub fn new(gateway: Arc<BinanceExecutionGateway>) -> Self {
        Self {
            gateway,
            cached_balances: RwLock::new(HashMap::new()),
            cached_positions: RwLock::new(HashMap::new()),
        }
    }

    /// Refreshes the local cache from real `account_balances`/
    /// `account_positions` calls. Callers (a future account-monitoring
    /// task, or `run_live_trading`) decide the polling interval -- this
    /// type performs no scheduling itself.
    pub async fn refresh(&self) -> Result<(), ApexesError> {
        let balances = self
            .gateway
            .account_balances()
            .await
            .map_err(|reason| ApexesError::ConnectionFailed {
                reason,
                source_detail: None,
            })?;
        let positions = self
            .gateway
            .account_positions()
            .await
            .map_err(|reason| ApexesError::ConnectionFailed {
                reason,
                source_detail: None,
            })?;

        *self.cached_balances.write().await = balances;
        *self.cached_positions.write().await = positions;
        Ok(())
    }
}

#[async_trait]
impl Account for LiveAccount {
    fn balances(&self) -> HashMap<String, Decimal> {
        self.cached_balances
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    fn positions(&self) -> HashMap<String, Position> {
        self.cached_positions
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    fn realized_pnl(&self) -> HashMap<String, Decimal> {
        // Binance exposes no single "account-level accumulated realized
        // PnL" field; deriving it from persisted Fill history (via
        // apexes-oms::OrderManager, per the design spec §3.5) is left
        // to a future enhancement once a concrete OrderManager wiring
        // point exists for this crate -- returning an empty map here is
        // a deliberate, explicitly-documented gap, not a silent one.
        HashMap::new()
    }

    fn equity(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        fx_provider: &dyn FxRateProvider,
        valuation_currency: &str,
    ) -> Result<Decimal, ApexesError> {
        let balances = self.balances();
        let mut total = Decimal::ZERO;
        for (asset, balance) in &balances {
            if balance.is_zero() {
                continue;
            }
            let price = if asset == valuation_currency {
                Decimal::ONE
            } else if let Some(price) = mark_prices.get(asset) {
                *price
            } else {
                fx_provider.fx_rate(asset, valuation_currency)?
            };
            total += balance * price;
        }
        Ok(total)
    }

    async fn deposit_history(&self) -> Result<Vec<FundMovement>, ApexesError> {
        self.gateway
            .deposit_history()
            .await
            .map_err(|reason| ApexesError::ConnectionFailed {
                reason,
                source_detail: None,
            })
    }

    async fn withdrawal_history(&self) -> Result<Vec<FundMovement>, ApexesError> {
        self.gateway
            .withdrawal_history()
            .await
            .map_err(|reason| ApexesError::ConnectionFailed {
                reason,
                source_detail: None,
            })
    }

    async fn internal_transfer(
        &self,
        from_wallet: WalletId,
        to_wallet: WalletId,
        asset: &str,
        amount: Decimal,
    ) -> Result<(), ApexesError> {
        self.gateway
            .internal_transfer(from_wallet, to_wallet, asset, amount)
            .await
            .map_err(|reason| ApexesError::ConnectionFailed {
                reason,
                source_detail: None,
            })
    }

    async fn reconcile_against_exchange(
        &self,
        tolerance: ReconciliationTolerance,
    ) -> Result<Vec<ReconciliationDiscrepancy>, ApexesError> {
        let exchange_balances =
            self.gateway
                .account_balances()
                .await
                .map_err(|reason| ApexesError::ConnectionFailed {
                    reason,
                    source_detail: None,
                })?;
        let local_balances = self.balances();

        let mut discrepancies = Vec::new();
        let mut all_assets: std::collections::HashSet<&String> =
            local_balances.keys().collect();
        all_assets.extend(exchange_balances.keys());

        for asset in all_assets {
            let local_value = local_balances.get(asset).copied().unwrap_or(Decimal::ZERO);
            let exchange_value = exchange_balances
                .get(asset)
                .copied()
                .unwrap_or(Decimal::ZERO);
            if !tolerance.within(local_value, exchange_value) {
                discrepancies.push(ReconciliationDiscrepancy {
                    asset_or_symbol: asset.clone(),
                    local_value,
                    exchange_value,
                    difference: (local_value - exchange_value).abs(),
                });
            }
        }
        Ok(discrepancies)
    }
}
```

Add `tokio` with the `sync` feature to `apexes-account`'s dependencies if not already present (Task 4 already added `tokio = { workspace = true, features = ["sync"] }` -- verify it's there, no change needed if so).

- [ ] **Step 3: Register the module**

```rust
// crates/apexes-account/src/lib.rs -- add:
pub mod live_account;
pub use live_account::LiveAccount;
```

- [ ] **Step 4: Run the new tests**

Run: `cargo test -p apexes-account live_account`
Expected: all 7 pass.

- [ ] **Step 5: Run this crate's tests, clippy, fmt, then a full workspace check**

Run: `cargo test -p apexes-account`
Expected: all pass (every test from Tasks 4-7).

Run: `cargo clippy -p apexes-account --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-account/src/lib.rs crates/apexes-account/src/live_account.rs
git commit -S -s -m "feat(apexes-account): LiveAccount -- real Binance-backed Account implementation"
```

---

### Task 8: `apexes-account`: `AccountRegistry`, `AccountPnl`/`compute_pnl`

**Files:**
- Create: `crates/apexes-account/src/registry.rs`
- Create: `crates/apexes-account/src/pnl.rs`
- Modify: `crates/apexes-account/src/lib.rs`

**Interfaces:**
- Consumes: `Account`, `FxRateProvider` (Tasks 4-5).
- Produces: `apexes_account::{AccountRegistry, AccountPnl, compute_pnl}`.

- [ ] **Step 1: Write the failing tests for `AccountRegistry`**

```rust
// crates/apexes-account/src/registry.rs (new file)
//! [`AccountRegistry`]: holds multiple named `Arc<dyn Account>`
//! instances, supporting lookup by name and cross-account aggregate
//! queries (e.g. total equity). Deliberately NOT built on
//! `apexes_core::registry::Registry` (that locator is keyed by Rust
//! type + a business key, for "the one gateway"/"one provider per
//! venue" use cases; here the natural key is a human-readable account
//! name, a different enough shape that reusing it would be awkward --
//! see `docs/superpowers/specs/2026-08-02-apexes-account-design.md`
//! §3.4).

#[cfg(test)]
mod tests {
    use crate::account::Account;
    use crate::fx_rate::StaticFxRateProvider;
    use crate::reconciliation::{ReconciliationDiscrepancy, ReconciliationTolerance};
    use crate::registry::AccountRegistry;
    use apexes_core::{ApexesError, FundMovement, WalletId};
    use apexes_strategy::Position;
    use async_trait::async_trait;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct FixedEquityAccount {
        equity: Decimal,
        fails: bool,
    }

    #[async_trait]
    impl Account for FixedEquityAccount {
        fn balances(&self) -> HashMap<String, Decimal> {
            HashMap::new()
        }
        fn positions(&self) -> HashMap<String, Position> {
            HashMap::new()
        }
        fn realized_pnl(&self) -> HashMap<String, Decimal> {
            HashMap::new()
        }
        fn equity(
            &self,
            _mark_prices: &HashMap<String, Decimal>,
            _fx_provider: &dyn crate::fx_rate::FxRateProvider,
            _valuation_currency: &str,
        ) -> Result<Decimal, ApexesError> {
            if self.fails {
                Err(ApexesError::InvalidBacktestConfig {
                    reason: "simulated failure".to_string(),
                })
            } else {
                Ok(self.equity)
            }
        }
        async fn deposit_history(&self) -> Result<Vec<FundMovement>, ApexesError> {
            Ok(Vec::new())
        }
        async fn withdrawal_history(&self) -> Result<Vec<FundMovement>, ApexesError> {
            Ok(Vec::new())
        }
        async fn internal_transfer(
            &self,
            _from_wallet: WalletId,
            _to_wallet: WalletId,
            _asset: &str,
            _amount: Decimal,
        ) -> Result<(), ApexesError> {
            Ok(())
        }
        async fn reconcile_against_exchange(
            &self,
            _tolerance: ReconciliationTolerance,
        ) -> Result<Vec<ReconciliationDiscrepancy>, ApexesError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn register_and_get_by_name() {
        let mut registry = AccountRegistry::new();
        let account: Arc<dyn Account> = Arc::new(FixedEquityAccount {
            equity: dec!(100),
            fails: false,
        });
        registry.register("main", account);

        assert!(registry.get("main").is_some());
        assert!(registry.get("missing").is_none());
    }

    #[test]
    fn total_equity_across_all_sums_every_account() {
        let mut registry = AccountRegistry::new();
        registry.register(
            "a",
            Arc::new(FixedEquityAccount {
                equity: dec!(100),
                fails: false,
            }) as Arc<dyn Account>,
        );
        registry.register(
            "b",
            Arc::new(FixedEquityAccount {
                equity: dec!(250),
                fails: false,
            }) as Arc<dyn Account>,
        );

        let fx = StaticFxRateProvider::new(HashMap::new());
        let total = registry
            .total_equity_across_all(&HashMap::new(), &fx, "USDT")
            .unwrap();
        assert_eq!(total, dec!(350));
    }

    #[test]
    fn total_equity_across_all_fails_if_any_account_fails() {
        let mut registry = AccountRegistry::new();
        registry.register(
            "a",
            Arc::new(FixedEquityAccount {
                equity: dec!(100),
                fails: false,
            }) as Arc<dyn Account>,
        );
        registry.register(
            "b",
            Arc::new(FixedEquityAccount {
                equity: Decimal::ZERO,
                fails: true,
            }) as Arc<dyn Account>,
        );

        let fx = StaticFxRateProvider::new(HashMap::new());
        let result = registry.total_equity_across_all(&HashMap::new(), &fx, "USDT");
        assert!(result.is_err());
    }
}
```

Run: `cargo test -p apexes-account registry`
Expected: FAIL with "cannot find type `AccountRegistry`" -- expected; Step 2 implements it.

- [ ] **Step 2: Implement `AccountRegistry`**

```rust
// crates/apexes-account/src/registry.rs -- add above the
// #[cfg(test)] block:

use std::collections::HashMap;
use std::sync::Arc;

use apexes_core::ApexesError;
use rust_decimal::Decimal;

use crate::account::Account;
use crate::fx_rate::FxRateProvider;

pub struct AccountRegistry {
    accounts: HashMap<String, Arc<dyn Account>>,
}

impl Default for AccountRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl AccountRegistry {
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: impl Into<String>, account: Arc<dyn Account>) {
        self.accounts.insert(name.into(), account);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Account>> {
        self.accounts.get(name).cloned()
    }

    /// Sums `equity()` across every registered account using the same
    /// `mark_prices`/`fx_provider`/`valuation_currency`. Any single
    /// account's `equity()` failing fails the WHOLE aggregate (never
    /// silently skips a failed account) -- a partial total-equity
    /// figure that silently omits one account is worse than no figure
    /// at all for a number this safety-relevant.
    pub fn total_equity_across_all(
        &self,
        mark_prices: &HashMap<String, Decimal>,
        fx_provider: &dyn FxRateProvider,
        valuation_currency: &str,
    ) -> Result<Decimal, ApexesError> {
        let mut total = Decimal::ZERO;
        for account in self.accounts.values() {
            total += account.equity(mark_prices, fx_provider, valuation_currency)?;
        }
        Ok(total)
    }
}
```

- [ ] **Step 3: Write the failing tests for `compute_pnl`**

```rust
// crates/apexes-account/src/pnl.rs (new file)
//! [`AccountPnl`]/[`compute_pnl`]: a free function computing a
//! cross-currency-valued P&L snapshot from any `&dyn Account`, rather
//! than a method every `Account` implementation would need to
//! duplicate -- pure composition over the trait's already-exposed
//! data. See `docs/superpowers/specs/2026-08-02-apexes-account-design.md`
//! §3.6.

#[cfg(test)]
mod tests {
    use crate::account::Account;
    use crate::fx_rate::StaticFxRateProvider;
    use crate::pnl::compute_pnl;
    use apexes_backtest::PortfolioLedger;
    use apexes_core::{AssetClass, Instrument, MarketType, Side, Venue};
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn btc_usdt() -> Instrument {
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

    fn ledger_with_10k_usdt() -> PortfolioLedger {
        let mut instruments = HashMap::new();
        instruments.insert("BTCUSDT".to_string(), btc_usdt());
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(10000));
        PortfolioLedger::new(instruments, initial_balances)
    }

    #[test]
    fn compute_pnl_reports_realized_unrealized_and_total_equity() {
        let mut ledger = ledger_with_10k_usdt();
        // Open 0.2 BTC at 60000, then partially close 0.1 at 65000
        // (realizes 500), leaving 0.1 open.
        ledger
            .apply_fill(&apexes_core::Fill {
                id: apexes_core::FillId(uuid::Uuid::new_v4()),
                order_id: apexes_core::OrderId(uuid::Uuid::new_v4()),
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                side: Side::Buy,
                price: dec!(60000),
                quantity: dec!(0.2),
                fee: dec!(0),
                fee_currency: None,
                filled_at: chrono::Utc::now(),
                leverage: None,
                margin_mode: None,
            })
            .unwrap();
        ledger
            .apply_fill(&apexes_core::Fill {
                id: apexes_core::FillId(uuid::Uuid::new_v4()),
                order_id: apexes_core::OrderId(uuid::Uuid::new_v4()),
                symbol: "BTCUSDT".to_string(),
                venue: Venue::Binance,
                side: Side::Sell,
                price: dec!(65000),
                quantity: dec!(0.1),
                fee: dec!(0),
                fee_currency: None,
                filled_at: chrono::Utc::now(),
                leverage: None,
                margin_mode: None,
            })
            .unwrap();

        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC".to_string(), dec!(70000));
        let fx = StaticFxRateProvider::new(HashMap::new());

        let pnl = compute_pnl(&ledger, &mark_prices, &fx, "USDT").unwrap();

        assert_eq!(*pnl.realized.get("BTCUSDT").unwrap(), dec!(500));
        // Unrealized: (70000 - 60000) * 0.1 remaining = 1000.
        assert_eq!(*pnl.unrealized.get("BTCUSDT").unwrap(), dec!(1000));
        assert_eq!(pnl.total_equity, Account::equity(&ledger, &mark_prices, &fx, "USDT").unwrap());
    }
}
```

Run: `cargo test -p apexes-account pnl`
Expected: FAIL with "cannot find function `compute_pnl`" -- expected; Step 4 implements it.

- [ ] **Step 4: Implement `AccountPnl`/`compute_pnl`**

```rust
// crates/apexes-account/src/pnl.rs -- add above the #[cfg(test)] block:

use std::collections::HashMap;

use apexes_core::ApexesError;
use rust_decimal::Decimal;

use crate::account::Account;
use crate::fx_rate::FxRateProvider;

#[derive(Debug, Clone, PartialEq)]
pub struct AccountPnl {
    pub realized: HashMap<String, Decimal>,
    pub unrealized: HashMap<String, Decimal>,
    pub total_equity: Decimal,
}

/// Computes a `AccountPnl` snapshot from any `&dyn Account`.
/// `mark_prices`: current price for each open position's underlying
/// asset (same convention as `Account::equity`). A symbol with a
/// non-zero position but no entry in `mark_prices` is a configuration
/// error -- returns `Err`, matching
/// `PortfolioLedger::margin_status`'s established "never silently
/// skip a missing mark price" precedent.
pub fn compute_pnl(
    account: &dyn Account,
    mark_prices: &HashMap<String, Decimal>,
    fx_provider: &dyn FxRateProvider,
    valuation_currency: &str,
) -> Result<AccountPnl, ApexesError> {
    let realized = account.realized_pnl();
    let positions = account.positions();

    let mut unrealized = HashMap::new();
    for (symbol, position) in &positions {
        if position.quantity.is_zero() {
            continue;
        }
        let mark_price = mark_prices.get(symbol).ok_or_else(|| {
            ApexesError::InvalidBacktestConfig {
                reason: format!("no mark price provided for open position {symbol}"),
            }
        })?;
        let pnl = (*mark_price - position.average_entry_price) * position.quantity;
        unrealized.insert(symbol.clone(), pnl);
    }

    let total_equity = account.equity(mark_prices, fx_provider, valuation_currency)?;

    Ok(AccountPnl {
        realized,
        unrealized,
        total_equity,
    })
}
```

Note: `mark_prices` here is keyed by SYMBOL (e.g. `"BTCUSDT"`), matching how `positions()` is keyed, which differs from `Account::equity`'s `mark_prices` (keyed by base-asset symbol, e.g. `"BTC"`, matching `balances()`). Both conventions coexist because `equity()` prices BALANCES (asset-keyed) while `compute_pnl`'s unrealized-PnL loop prices POSITIONS (symbol-keyed) -- callers must supply a `mark_prices` map covering whichever keys the specific call needs (in practice, both asset symbols like `"BTC"` and instrument symbols like `"BTCUSDT"` populated in the same map, since `run_event_loop`/`run_live_trading` already track a `last_prices`-style map keyed however their own `MarketEvent` stream naturally keys it -- this is consistent with `PortfolioLedger::margin_status`'s existing `mark_prices` parameter, which is also symbol-keyed for its own per-position lookups while `equity()`'s `mark_prices` is asset-keyed, an established precedent already present in this codebase before this plan).

- [ ] **Step 5: Register the modules**

```rust
// crates/apexes-account/src/lib.rs -- add:
pub mod pnl;
pub mod registry;

pub use pnl::{AccountPnl, compute_pnl};
pub use registry::AccountRegistry;
```

- [ ] **Step 6: Run the new tests**

Run: `cargo test -p apexes-account`
Expected: all pass (every test across Tasks 4-8).

- [ ] **Step 7: Run clippy, fmt, then a full workspace check**

Run: `cargo clippy -p apexes-account --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

Run: `cargo check --workspace`
Expected: 0 errors.

- [ ] **Step 8: Commit**

```bash
git add crates/apexes-account/src/lib.rs crates/apexes-account/src/registry.rs crates/apexes-account/src/pnl.rs
git commit -S -s -m "feat(apexes-account): AccountRegistry, AccountPnl/compute_pnl"
```

---

### Task 9: Full-workspace verification checkpoint

**Files:** none (verification-only task).

**Interfaces:** none.

- [ ] **Step 1: Run the complete verification suite**

Run: `cargo build --workspace`
Expected: 0 errors.

Run: `cargo test --workspace -- --test-threads=1`
Expected: 0 failures. Record the exact passed/ignored counts (via `grep "test result:"` summed across every suite, per this project's established convention of never estimating these numbers) for comparison against the pre-this-plan baseline (505 passed / 0 failed / 45 ignored, per row `6.5`'s final Task 6 checkpoint).

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: 0 warnings.

Run: `cargo fmt --check`
Expected: clean.

- [ ] **Step 2: Grep for leftover placeholders**

Run: `grep -rn "TODO\|FIXME\|todo!\|unimplemented!" crates/apexes-account/src crates/apexes-core/src/account.rs`
Expected: no matches (this task's own new files must be placeholder-free; the workspace-wide grep the final review will perform in Task 10 covers everything else).

- [ ] **Step 3: Commit** (only if Step 1/2 required any fixes; otherwise this task produces no diff and can be skipped without a commit)

```bash
git add -A
git commit -S -s -m "chore: full-workspace verification checkpoint for account & fund management plan"
```

---

### Task 10: Final whole-branch code review and `docs/FEATURES.md` update

**Files:**
- Modify: `docs/FEATURES.md`

**Interfaces:** none (documentation/review-only task).

- [ ] **Step 1: Run the complete verification suite one final time**

Run: `cargo build --workspace`, `cargo test --workspace -- --test-threads=1`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`.
Expected: all clean, matching this project's established multi-sub-project-completion precedent (rows 6.2/6.4/6.5 all ended with this exact step).

- [ ] **Step 2: Escalate to a whole-branch review**

Per this project's established precedent (rows 6.2/6.4/6.5's final reviews, all escalated to the most capable available reviewer profile for a correctness-critical whole-branch pass): dispatch an independent reviewer over the ENTIRE branch diff for this plan (base: the commit before Task 1 started, head: Task 8's final commit). Pay particular attention to:

- **The circular-dependency resolution**: confirm `WalletId`/`FundMovement`/`FundMovementStatus` genuinely live in `apexes-core` (not `apexes-account`, not `apexes-gateway-binance`), and confirm `apexes-account`'s `Account` trait implementations for externally-defined types (`PortfolioLedger`, `PaperAccount`) are placed inside `apexes-account` itself (per Rust's orphan rule -- see Task 6's note), NOT accidentally added as new dependencies pointing the wrong direction.
- **`realized_pnl`'s correctness for both spot and leveraged paths**: re-derive the spot reduce-branch's realized-PnL formula independently (long: `(fill.price - average_entry_price) * closing_quantity`; short: reversed sign) and confirm it matches `update_position`'s refactored control flow; confirm `close_leveraged`'s existing realized-PnL local variable is now ALSO accumulated into the new field (not just used for the existing balance credit), and that this is a pure addition with zero change to any existing test's expected balance/position values.
- **Whether `LiveAccount`'s `equity()`/`reconcile_against_exchange()` ever silently trust stale cache data as if it were fresh**: confirm `reconcile_against_exchange` always issues a NEW real request rather than comparing the cache against itself, and that `refresh()` is the only method that ever writes to the cache.
- **Whether `AccountRegistry::total_equity_across_all` could silently produce a partial (wrong) total** if one account's `equity()` call fails -- confirm it propagates the error rather than skipping the failed account.
- **Whether any code path in this plan could initiate a real withdrawal or send funds outside the account** -- grep the entire diff for any call resembling a withdrawal-initiation endpoint (there should be none; only `deposit_history`/`withdrawal_history`, both read-only GETs, plus `internal_transfer`, which only moves funds between wallets of the SAME account).
- **Whether `apexes-gateway-binance`'s new `account_positions`/`account_balances` methods handle the zero-position/zero-balance filtering correctly** (no spurious zero-quantity entries leaking into the returned maps, matching `PortfolioLedger::positions()`'s "only non-zero symbols present" convention).
- **Whether the `FxRateProvider` sync/async decision is consistently applied** -- confirm no code path anywhere in this plan accidentally tries to call `fx_rate` from an async context expecting `.await` on it (it should never be awaited anywhere, since it's synchronous).

Fix any findings, get a fix-verification re-review, and only then consider this plan complete.

- [ ] **Step 3: Update `docs/FEATURES.md`**

Find rows `7.1`, `7.2`, `7.3` (`grep -n "| 7\." docs/FEATURES.md`) and update each row's Status column from `🔲 Not Started` to `✅ Done`, with Notes summarizing: the new `apexes-account` crate (`Account` trait unifying backtest/paper/live via `PortfolioLedger`/`PaperAccount`/`LiveAccount` implementations, `AccountRegistry` for multi-account aggregation, `FxRateProvider`/`StaticFxRateProvider` for cross-currency valuation), `PortfolioLedger::realized_pnl` (spot and leveraged realized-PnL tracking), real Binance balance/position/internal-transfer/deposit-withdrawal-history integration (read-only history, no withdrawal initiation), and alert-only `reconcile_against_exchange` (never auto-corrects local state).

Recompute the Summary counts table via actual `grep -c "✅ Done"`/`grep -c "🔲 Not Started"` (per this project's explicit standing instruction: NEVER estimate these counts, always recompute them from the real table contents) -- subtract 2 from each raw count (the legend row near the top of the file, AND the Summary table's own count row, both of which also contain these exact strings as their own text, per this project's standing memory note about this recurring double-count mistake).

- [ ] **Step 4: Commit**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: mark FEATURES.md rows 7.1/7.2/7.3 (account & fund management) as Done"
```
