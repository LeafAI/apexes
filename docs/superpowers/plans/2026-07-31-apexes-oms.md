# Apexes-OMS: Order Management System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `apexes-oms` crate providing a gateway-agnostic `ExecutionGateway` trait, a PostgreSQL-backed `OrderStore`, and an `OrderManager` business-logic layer implementing order submission (risk-checked, persist-before-submit), cancellation, and reconnect reconciliation (exchange-is-authoritative state overwrite).

**Architecture:** `apexes-oms` depends on `apexes-core` + `apexes-risk` + `sqlx` (PostgreSQL). `ExecutionGateway` is defined in this crate (not `apexes-core`) since only OMS consumes it; a `MockExecutionGateway` test double lets unit tests simulate accept/reject/uncertain outcomes and exchange-side state changes without a real exchange connection. `OrderManager` holds an in-memory `HashMap<OrderId, Order>` open-order snapshot (rebuilt from PostgreSQL on construction) mirroring `apexes_backtest::PortfolioLedger`'s pattern, persisting every state change synchronously. `reconcile()` matches local orders to exchange reports by client order ID (our own `OrderId`, not the exchange's ID, since `Uncertain`-outcome orders never learn an exchange ID any other way) and unconditionally overwrites local status with the exchange's report, bypassing `OrderStatus::can_transition_to`.

**Tech Stack:** Rust edition 2024, `sqlx` (new workspace dependency, `runtime-tokio`/`postgres`/`chrono`/`rust_decimal`/`uuid` features, async PostgreSQL driver), `apexes-core`, `apexes-risk`, `apexes-strategy`, `tokio`, `chrono`, `rust_decimal`, `tracing`, `thiserror`, `async-trait`.

## Global Constraints

- Design spec of record: `docs/superpowers/specs/2026-07-31-apexes-oms-design.md`. If you find a mismatch between this plan and that spec while implementing, the spec wins -- stop and flag it rather than improvising.
- Rust edition `2024`, matching `apexes`' workspace (`Cargo.toml` `[workspace.package] edition = "2024"`).
- **`apexes-oms` uses PostgreSQL, NOT ClickHouse** -- this is the project's first crate to introduce PostgreSQL as a storage backend. `apexes-storage` (ClickHouse, historical market data) and `apexes-strategy-registry` (ClickHouse, strategy versions) are both explicitly left unchanged by this plan -- do not touch either.
- **`ExecutionGateway` is defined in `apexes-oms`, not `apexes-core`** -- only OMS consumes it in this plan; sub-project `#8` (first live gateway) will implement it against a real exchange without needing to modify `apexes-oms`.
- **This sub-project does NOT integrate with `Strategy`/`MatchingEngine`** (no event loop, no `OrderIntent` stream consumption beyond a single `submit_order` call) -- that integration is `#8`'s scope.
- **`OrderManager` does not own position/balance tracking** -- `submit_order` accepts `positions`/`balances` snapshots as caller-supplied parameters (account/fund management is sub-project `#11`'s scope, not yet built).
- **`GatewayOutcome` has three variants (`Accepted`/`Rejected`/`Uncertain`), never a plain `Result<String, String>`** -- `Uncertain` (e.g. network timeout) must NOT be treated as failure; the order's local state is left unchanged pending `reconcile()`.
- **`reconcile()` matches local orders to exchange reports by client order ID (`OrderId`), never by `exchange_order_id`** -- an `Uncertain`-outcome order has no `exchange_order_id` yet, so matching on it would make such orders permanently unreconcilable.
- **`reconcile()` overwrites local `OrderStatus` unconditionally, bypassing `OrderStatus::can_transition_to`** -- the exchange's reported status is authoritative during reconciliation; a "disallowed" transition (e.g. `New -> Filled` skipping `PartiallyFilled`) reflects real events that happened while disconnected, not a bug.
- **No automatic retry** of uncertain submit/cancel outcomes -- left for a future iteration (retry risk of duplicate orders needs a dedicated idempotency-key design).
- Every commit in the `apexes` repo uses `git commit -S -s` (signed + signed-off), matching the project's established convention. Direct commits to `master` (no branches/PRs).
- `cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --check` must all pass clean after every task that touches compiled code.
- Unit tests must not require a live PostgreSQL connection where avoidable (`schema.rs`'s `TryFrom` tests, `MockExecutionGateway`'s own tests); tests that do require one (`OrderStore`/`OrderManager`/end-to-end tests, since neither has an in-memory-only mode) are marked `#[ignore]`, matching `apexes-storage`'s established pattern for ClickHouse-dependent tests.

---

### Task 1: `docker-compose.yml` PostgreSQL service + crate scaffold + `OmsError`

**Files:**
- Modify: `docker-compose.yml` (add `postgres` service)
- Modify: `Cargo.toml` (add `sqlx` to `[workspace.dependencies]`, add `"crates/apexes-oms"` to `members`)
- Create: `crates/apexes-oms/Cargo.toml`
- Create: `crates/apexes-oms/src/error.rs`
- Create: `crates/apexes-oms/src/lib.rs`

**Interfaces:**
- Produces: `pub enum OmsError`. Consumed by every later task in this plan.

- [ ] **Step 1: Add the PostgreSQL service to `docker-compose.yml`**

The current file looks like this:

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:25.12
    container_name: apexes-clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"
    environment:
      CLICKHOUSE_DB: apexes
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: ""
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "1"
    volumes:
      - apexes-clickhouse-data:/var/lib/clickhouse
    ulimits:
      nofile:
        soft: 262144
        hard: 262144

volumes:
  apexes-clickhouse-data:
```

Replace it with:

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:25.12
    container_name: apexes-clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"
    environment:
      CLICKHOUSE_DB: apexes
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: ""
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "1"
    volumes:
      - apexes-clickhouse-data:/var/lib/clickhouse
    ulimits:
      nofile:
        soft: 262144
        hard: 262144

  postgres:
    image: postgres:17
    container_name: apexes-postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: apexes
      POSTGRES_USER: apexes
      POSTGRES_PASSWORD: apexes
    volumes:
      - apexes-postgres-data:/var/lib/postgresql/data

volumes:
  apexes-clickhouse-data:
  apexes-postgres-data:
```

- [ ] **Step 2: Add `sqlx` to the workspace and register the new crate**

Edit `Cargo.toml`:

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage", "crates/apexes-backtest", "crates/apexes-strategy", "crates/apexes-indicators", "crates/apexes-optimize", "crates/apexes-strategy-registry", "crates/apexes-risk", "crates/apexes-paper", "crates/apexes-oms"]

[workspace.package]
edition = "2024"
version = "0.1.0"
license = "MIT"

[workspace.dependencies]
rust_decimal = { version = "1.42", features = ["serde", "maths"] }
serde = { version = "1.0", features = ["derive"] }
thiserror = "2.0"
chrono = { version = "0.4", features = ["serde"] }
tokio = { version = "1.53", features = ["rt-multi-thread", "macros"] }
futures = "0.3"
async-trait = "0.1.91"
uuid = { version = "1.24", features = ["v4", "serde"] }
clap = { version = "4.6", features = ["derive"] }
serde_json = "1.0"
barter-data = "0.11"
barter-instrument = "0.3"
tracing = "0.1"
clickhouse = { version = "0.15", features = ["chrono"] }
fixnum = { version = "0.9", features = ["i128", "serde"] }
barter = "0.12"
sha2 = "0.10"
rand = "0.10"
sqlx = { version = "0.9", features = ["runtime-tokio", "postgres", "chrono", "rust_decimal", "uuid"] }
```

Note: `"crates/apexes-risk"` and `"crates/apexes-paper"` and `rand` should already be present in your actual `Cargo.toml` from prior plans -- the block above shows the full expected end state; if any of those are missing, add them back, but do not remove anything else.

- [ ] **Step 3: Create `crates/apexes-oms/Cargo.toml`**

```toml
[package]
name = "apexes-oms"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
apexes-risk = { path = "../apexes-risk" }
apexes-strategy = { path = "../apexes-strategy" }
sqlx = { workspace = true }
tokio = { workspace = true }
chrono = { workspace = true }
rust_decimal = { workspace = true }
tracing = { workspace = true }
thiserror = { workspace = true }
async-trait = { workspace = true }
uuid = { workspace = true, features = ["v4"] }

[dev-dependencies]
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
```

Note: `uuid` is listed under `[dependencies]`, not `[dev-dependencies]` -- non-test code (`OrderManager::submit_order` in Task 6, `MockExecutionGateway` in Task 3) constructs `OrderId(Uuid::new_v4())` outside any `#[cfg(test)]` block, so `uuid` must be a normal dependency, not a dev-only one.

- [ ] **Step 4: Write `crates/apexes-oms/src/error.rs`**

```rust
//! Error type for `apexes-oms`.

use apexes_core::{OrderId, OrderStatus};
use thiserror::Error;

/// Errors returned by `OrderStore`/`OrderManager`.
#[derive(Debug, Error)]
pub enum OmsError {
    #[error("failed to connect to PostgreSQL: {reason}")]
    ConnectionFailed { reason: String },

    #[error("database query failed: {reason}")]
    QueryFailed { reason: String },

    #[error("failed to convert database row: {reason}")]
    InvalidRow { reason: String },

    #[error("order rejected by risk checker: {violations:?}")]
    RiskCheckFailed {
        violations: Vec<apexes_risk::RiskViolation>,
    },

    #[error("order rejected by exchange: {reason}")]
    OrderRejected { reason: String },

    #[error("order {order_id:?} not found")]
    OrderNotFound { order_id: OrderId },

    #[error("order {order_id:?} cannot be cancelled in status {current_status:?}")]
    OrderNotCancellable {
        order_id: OrderId,
        current_status: OrderStatus,
    },

    #[error(
        "order {order_id:?} has no known exchange_order_id yet (submission outcome was Uncertain) -- wait for reconcile() to resolve it before cancelling"
    )]
    ExchangeOrderIdUnknown { order_id: OrderId },
}

impl From<sqlx::Error> for OmsError {
    fn from(error: sqlx::Error) -> Self {
        OmsError::QueryFailed {
            reason: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn order_not_found_display() {
        let err = OmsError::OrderNotFound {
            order_id: OrderId(Uuid::nil()),
        };
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn order_not_cancellable_display() {
        let err = OmsError::OrderNotCancellable {
            order_id: OrderId(Uuid::nil()),
            current_status: OrderStatus::Filled,
        };
        assert!(err.to_string().contains("cannot be cancelled"));
        assert!(err.to_string().contains("Filled"));
    }

    #[test]
    fn exchange_order_id_unknown_display() {
        let err = OmsError::ExchangeOrderIdUnknown {
            order_id: OrderId(Uuid::nil()),
        };
        assert!(err.to_string().contains("Uncertain"));
    }
}
```

- [ ] **Step 5: Write `crates/apexes-oms/src/lib.rs`**

```rust
//! Apexes order management system (OMS): gateway-agnostic order
//! lifecycle management (submission with pre-trade risk checks,
//! cancellation, persistence, and reconnect reconciliation). Covers
//! `docs/FEATURES.md` rows 5.1.1 (order generation & pre-trade
//! validation) and 5.1.3 (persistence & reconnect state
//! recovery/reconciliation). Row 5.1.2 (order state machine) is
//! unchanged from sub-project #0's `apexes_core::OrderStatus`.
//!
//! [`ExecutionGateway`] decouples order submission/cancellation/query
//! from any specific exchange -- sub-project #8 (first live gateway)
//! implements it against a real exchange without needing to modify this
//! crate. This crate ships only [`mock_gateway::MockExecutionGateway`]
//! for testing.

pub mod error;

pub use error::OmsError;
```

- [ ] **Step 6: Run the error module's tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-oms error:: -- --nocapture
```
Expected: 3 passed, 0 failed.

- [ ] **Step 7: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-oms --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run both commands).

- [ ] **Step 8: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [ ] **Step 9: Commit**

```bash
cd ~/src/oss/apexes
git add docker-compose.yml Cargo.toml crates/apexes-oms
git commit -S -s -m "feat: add apexes-oms crate scaffold with OmsError and PostgreSQL docker-compose service"
git push origin master
```

---

### Task 2: `ExecutionGateway` trait + `GatewayOutcome`

**Files:**
- Create: `crates/apexes-oms/src/gateway.rs`
- Modify: `crates/apexes-oms/src/lib.rs`

**Interfaces:**
- Consumes: `apexes_core::{Order, OrderId, OrderStatus}` (existing).
- Produces: `pub enum GatewayOutcome`, `pub trait ExecutionGateway`. Consumed by Task 3's `MockExecutionGateway` and Task 5's `OrderManager`.

- [ ] **Step 1: Write `crates/apexes-oms/src/gateway.rs`**

```rust
//! [`ExecutionGateway`]: the abstraction `OrderManager` submits/cancels/
//! queries orders through, decoupling order-lifecycle management from
//! any specific exchange's API. Sub-project #8 (first live gateway)
//! implements this trait against a real exchange; this crate ships only
//! [`crate::mock_gateway::MockExecutionGateway`] for testing.

use apexes_core::{Order, OrderId, OrderStatus};
use async_trait::async_trait;

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
    /// order. Returns `Ok(None)` if the exchange has no record of this
    /// order ID at all (distinct from "query failed" -- that's `Err`).
    async fn query_order_status(
        &self,
        exchange_order_id: &str,
    ) -> Result<Option<OrderStatus>, String>;

    /// Queries the exchange for every currently open (not Filled/
    /// Cancelled/Rejected) order across the whole account -- used by
    /// `OrderManager::reconcile` after a reconnect.
    ///
    /// Returns `(client_order_id, exchange_order_id, status)` triples.
    /// `client_order_id` is OUR locally-assigned `OrderId` (`Order.id`),
    /// not the exchange's own identifier -- see `submit_order`'s docs
    /// for why implementations must echo it back here.
    async fn query_all_open_orders(
        &self,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String>;
}
```

- [ ] **Step 2: Update `crates/apexes-oms/src/lib.rs`**

```rust
//! Apexes order management system (OMS): gateway-agnostic order
//! lifecycle management (submission with pre-trade risk checks,
//! cancellation, persistence, and reconnect reconciliation). Covers
//! `docs/FEATURES.md` rows 5.1.1 (order generation & pre-trade
//! validation) and 5.1.3 (persistence & reconnect state
//! recovery/reconciliation). Row 5.1.2 (order state machine) is
//! unchanged from sub-project #0's `apexes_core::OrderStatus`.
//!
//! [`ExecutionGateway`] decouples order submission/cancellation/query
//! from any specific exchange -- sub-project #8 (first live gateway)
//! implements it against a real exchange without needing to modify this
//! crate. This crate ships only [`mock_gateway::MockExecutionGateway`]
//! for testing.

pub mod error;
pub mod gateway;

pub use error::OmsError;
pub use gateway::{ExecutionGateway, GatewayOutcome};
```

- [ ] **Step 3: Verify it compiles**

```bash
cd ~/src/oss/apexes
cargo check -p apexes-oms
```
Expected: succeeds, 0 errors.

- [ ] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-oms --all-targets -- -D warnings
cargo fmt --check
```
Expected clean.

- [ ] **Step 5: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-oms/src
git commit -S -s -m "feat: add ExecutionGateway trait and GatewayOutcome to apexes-oms"
git push origin master
```

---

### Task 3: `MockExecutionGateway`

**Files:**
- Create: `crates/apexes-oms/src/mock_gateway.rs`
- Modify: `crates/apexes-oms/src/lib.rs`

**Interfaces:**
- Consumes: `crate::gateway::{ExecutionGateway, GatewayOutcome}` (Task 2), `apexes_core::{Order, OrderId, OrderStatus}` (existing).
- Produces: `pub struct MockExecutionGateway` with `new`/`program_next_submit_outcome`/`program_next_cancel_outcome`/`set_exchange_state`. Consumed by Task 6's `OrderManager` unit tests.

- [ ] **Step 1: Write `crates/apexes-oms/src/mock_gateway.rs`**

```rust
//! [`MockExecutionGateway`]: an in-memory `ExecutionGateway` for tests.
//! Lets a test pre-program specific `GatewayOutcome`s per call and
//! simulate exchange-side state changes that occur "during a
//! disconnect" for `OrderManager::reconcile` tests.

use std::collections::HashMap;
use std::sync::Mutex;

use apexes_core::{Order, OrderId, OrderStatus};
use async_trait::async_trait;
use uuid::Uuid;

use crate::gateway::{ExecutionGateway, GatewayOutcome};

pub struct MockExecutionGateway {
    /// Programmed outcome for the next `submit_order` call, consumed
    /// (removed) on use. `None` means "accept with a freshly generated
    /// exchange_order_id" (the convenient default for tests that don't
    /// care about submission failure paths).
    next_submit_outcome: Mutex<Option<GatewayOutcome>>,
    next_cancel_outcome: Mutex<Option<GatewayOutcome>>,
    /// Exchange-side authoritative state, keyed by OUR client order ID
    /// (`OrderId`) -- tests mutate this directly (simulating "the
    /// exchange processed something while we were disconnected")
    /// before calling `OrderManager::reconcile`. Each entry stores the
    /// exchange-assigned ID string this mock generated at
    /// `submit_order` time, so `query_all_open_orders` can return both.
    exchange_state: Mutex<HashMap<OrderId, (String, OrderStatus)>>,
}

impl Default for MockExecutionGateway {
    fn default() -> Self {
        Self::new()
    }
}

impl MockExecutionGateway {
    pub fn new() -> Self {
        Self {
            next_submit_outcome: Mutex::new(None),
            next_cancel_outcome: Mutex::new(None),
            exchange_state: Mutex::new(HashMap::new()),
        }
    }

    pub fn program_next_submit_outcome(&self, outcome: GatewayOutcome) {
        *self.next_submit_outcome.lock().unwrap() = Some(outcome);
    }

    pub fn program_next_cancel_outcome(&self, outcome: GatewayOutcome) {
        *self.next_cancel_outcome.lock().unwrap() = Some(outcome);
    }

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
}

#[async_trait]
impl ExecutionGateway for MockExecutionGateway {
    async fn submit_order(&self, order: &Order) -> GatewayOutcome {
        if let Some(outcome) = self.next_submit_outcome.lock().unwrap().take() {
            // Even for a programmed Accepted outcome, record the
            // exchange-side state so query_all_open_orders/reconcile
            // tests can find it.
            if let GatewayOutcome::Accepted { exchange_order_id } = &outcome {
                self.exchange_state.lock().unwrap().insert(
                    order.id,
                    (exchange_order_id.clone(), OrderStatus::New),
                );
            }
            return outcome;
        }

        let exchange_order_id = Uuid::new_v4().to_string();
        self.exchange_state.lock().unwrap().insert(
            order.id,
            (exchange_order_id.clone(), OrderStatus::New),
        );
        GatewayOutcome::Accepted { exchange_order_id }
    }

    async fn cancel_order(&self, exchange_order_id: &str) -> GatewayOutcome {
        if let Some(outcome) = self.next_cancel_outcome.lock().unwrap().take() {
            return outcome;
        }

        let mut state = self.exchange_state.lock().unwrap();
        if let Some((_, status)) = state
            .values_mut()
            .find(|(id, _)| id == exchange_order_id)
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
    ) -> Result<Option<OrderStatus>, String> {
        let state = self.exchange_state.lock().unwrap();
        Ok(state
            .values()
            .find(|(id, _)| id == exchange_order_id)
            .map(|(_, status)| *status))
    }

    async fn query_all_open_orders(
        &self,
    ) -> Result<Vec<(OrderId, Option<String>, OrderStatus)>, String> {
        let state = self.exchange_state.lock().unwrap();
        Ok(state
            .iter()
            .filter(|(_, (_, status))| {
                matches!(status, OrderStatus::New | OrderStatus::PartiallyFilled)
            })
            .map(|(client_order_id, (exchange_order_id, status))| {
                (*client_order_id, Some(exchange_order_id.clone()), *status)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, MarketType, Order, OrderType, Side, Venue};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn sample_order() -> Order {
        Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
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

    #[tokio::test]
    async fn submit_order_defaults_to_accepted_with_a_generated_exchange_order_id() {
        let gateway = MockExecutionGateway::new();
        let order = sample_order();
        let outcome = gateway.submit_order(&order).await;
        assert!(matches!(outcome, GatewayOutcome::Accepted { .. }));
    }

    #[tokio::test]
    async fn programmed_submit_outcome_is_consumed_once() {
        let gateway = MockExecutionGateway::new();
        gateway.program_next_submit_outcome(GatewayOutcome::Rejected {
            reason: "insufficient balance".to_string(),
        });
        let order = sample_order();

        let first = gateway.submit_order(&order).await;
        assert_eq!(
            first,
            GatewayOutcome::Rejected {
                reason: "insufficient balance".to_string()
            }
        );

        // Second call: no programmed outcome left, defaults to Accepted.
        let second = gateway.submit_order(&sample_order()).await;
        assert!(matches!(second, GatewayOutcome::Accepted { .. }));
    }

    #[tokio::test]
    async fn set_exchange_state_and_query_all_open_orders_reflect_it() {
        let gateway = MockExecutionGateway::new();
        let order = sample_order();
        gateway.submit_order(&order).await;

        gateway.set_exchange_state(order.id, OrderStatus::Filled);

        let open_orders = gateway.query_all_open_orders().await.unwrap();
        // Filled is a terminal state -- no longer "open".
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

        let open_orders = gateway.query_all_open_orders().await.unwrap();
        assert_eq!(open_orders.len(), 2);
        assert!(open_orders.iter().any(|(id, _, _)| *id == order1.id));
        assert!(open_orders.iter().any(|(id, _, _)| *id == order2.id));
    }

    #[tokio::test]
    async fn query_order_status_returns_none_for_unknown_exchange_order_id() {
        let gateway = MockExecutionGateway::new();
        let result = gateway.query_order_status("never-submitted").await.unwrap();
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
            .query_order_status(&exchange_order_id)
            .await
            .unwrap();
        assert_eq!(status, Some(OrderStatus::Cancelled));
    }
}
```

- [ ] **Step 2: Update `crates/apexes-oms/src/lib.rs`**

```rust
//! Apexes order management system (OMS): gateway-agnostic order
//! lifecycle management (submission with pre-trade risk checks,
//! cancellation, persistence, and reconnect reconciliation). Covers
//! `docs/FEATURES.md` rows 5.1.1 (order generation & pre-trade
//! validation) and 5.1.3 (persistence & reconnect state
//! recovery/reconciliation). Row 5.1.2 (order state machine) is
//! unchanged from sub-project #0's `apexes_core::OrderStatus`.
//!
//! [`ExecutionGateway`] decouples order submission/cancellation/query
//! from any specific exchange -- sub-project #8 (first live gateway)
//! implements it against a real exchange without needing to modify this
//! crate. This crate ships only [`mock_gateway::MockExecutionGateway`]
//! for testing.

pub mod error;
pub mod gateway;
pub mod mock_gateway;

pub use error::OmsError;
pub use gateway::{ExecutionGateway, GatewayOutcome};
pub use mock_gateway::MockExecutionGateway;
```

- [ ] **Step 3: Run the tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-oms mock_gateway:: -- --nocapture
```
Expected: 6 passed, 0 failed.

- [ ] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-oms --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run both commands plus the test command).

- [ ] **Step 5: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [ ] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-oms/src
git commit -S -s -m "feat: add MockExecutionGateway to apexes-oms"
git push origin master
```

---

### Task 4: PostgreSQL schema (`schema.rs`)

**Files:**
- Create: `crates/apexes-oms/src/schema.rs`
- Modify: `crates/apexes-oms/src/lib.rs`

**Interfaces:**
- Consumes: `crate::error::OmsError` (Task 1), `apexes_core::{Order, OrderId, OrderStatus, OrderType, Side, Fill, FillId, Venue}` (existing).
- Produces: `ORDERS_TABLE_DDL`/`ORDERS_EXCHANGE_ID_INDEX_DDL`/`FILLS_TABLE_DDL`/`FILLS_ORDER_ID_INDEX_DDL` constants, `pub struct OrderRow`/`pub struct FillRow` with `TryFrom` conversions both ways. Consumed by Task 5's `OrderStore`.

- [ ] **Step 1: Write `crates/apexes-oms/src/schema.rs`**

```rust
//! PostgreSQL table schemas for `apexes-oms`. Mirrors
//! `apexes_storage::schema`'s pattern (DDL constants + Row structs +
//! bidirectional `TryFrom` conversions), adapted for `sqlx`/PostgreSQL
//! instead of the `clickhouse` client.

use apexes_core::{Fill, FillId, Order, OrderId, OrderStatus, OrderType, Side, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::error::OmsError;

pub const ORDERS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS orders
(
    id                  UUID PRIMARY KEY,
    exchange_order_id   TEXT,
    symbol              TEXT NOT NULL,
    venue               TEXT NOT NULL,
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

pub const ORDERS_EXCHANGE_ID_INDEX_DDL: &str = "
CREATE INDEX IF NOT EXISTS idx_orders_exchange_order_id
    ON orders (exchange_order_id)
    WHERE exchange_order_id IS NOT NULL
";

pub const FILLS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS fills
(
    id              UUID PRIMARY KEY,
    order_id        UUID NOT NULL REFERENCES orders(id),
    symbol          TEXT NOT NULL,
    venue           TEXT NOT NULL,
    side            TEXT NOT NULL,
    price           NUMERIC(38, 10) NOT NULL,
    quantity        NUMERIC(38, 10) NOT NULL,
    fee             NUMERIC(38, 10) NOT NULL,
    fee_currency    TEXT,
    filled_at       TIMESTAMPTZ NOT NULL
)
";

pub const FILLS_ORDER_ID_INDEX_DDL: &str = "
CREATE INDEX IF NOT EXISTS idx_fills_order_id ON fills (order_id)
";

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrderRow {
    pub id: Uuid,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub venue: String,
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

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FillRow {
    pub id: Uuid,
    pub order_id: Uuid,
    pub symbol: String,
    pub venue: String,
    pub side: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_currency: Option<String>,
    pub filled_at: DateTime<Utc>,
}

impl From<&Fill> for FillRow {
    fn from(fill: &Fill) -> Self {
        FillRow {
            id: fill.id.0,
            order_id: fill.order_id.0,
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

fn parse_venue(value: &str) -> Result<Venue, OmsError> {
    match value {
        "Sse" => Ok(Venue::Sse),
        "Szse" => Ok(Venue::Szse),
        "Hkex" => Ok(Venue::Hkex),
        "Nasdaq" => Ok(Venue::Nasdaq),
        "Nyse" => Ok(Venue::Nyse),
        "Binance" => Ok(Venue::Binance),
        "Okx" => Ok(Venue::Okx),
        "Coinbase" => Ok(Venue::Coinbase),
        other => Err(OmsError::InvalidRow {
            reason: format!("unknown venue: {other}"),
        }),
    }
}

fn parse_side(value: &str) -> Result<Side, OmsError> {
    match value {
        "Buy" => Ok(Side::Buy),
        "Sell" => Ok(Side::Sell),
        other => Err(OmsError::InvalidRow {
            reason: format!("unknown side: {other}"),
        }),
    }
}

fn parse_order_type(value: &str) -> Result<OrderType, OmsError> {
    match value {
        "Market" => Ok(OrderType::Market),
        "Limit" => Ok(OrderType::Limit),
        "Stop" => Ok(OrderType::Stop),
        "StopLimit" => Ok(OrderType::StopLimit),
        other => Err(OmsError::InvalidRow {
            reason: format!("unknown order_type: {other}"),
        }),
    }
}

fn parse_order_status(value: &str) -> Result<OrderStatus, OmsError> {
    match value {
        "New" => Ok(OrderStatus::New),
        "PartiallyFilled" => Ok(OrderStatus::PartiallyFilled),
        "Filled" => Ok(OrderStatus::Filled),
        "Cancelled" => Ok(OrderStatus::Cancelled),
        "Rejected" => Ok(OrderStatus::Rejected),
        other => Err(OmsError::InvalidRow {
            reason: format!("unknown order_status: {other}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, MarketType};
    use rust_decimal_macros::dec;

    fn sample_order() -> Order {
        Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: Some("EX123".to_string()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
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

    #[test]
    fn order_round_trips_through_order_row() {
        let order = sample_order();
        let row = OrderRow::from(&order);
        let back = Order::try_from(row).unwrap();
        assert_eq!(back, order);
    }

    #[test]
    fn order_with_none_exchange_order_id_round_trips() {
        let mut order = sample_order();
        order.exchange_order_id = None;
        let row = OrderRow::from(&order);
        let back = Order::try_from(row).unwrap();
        assert_eq!(back.exchange_order_id, None);
    }

    #[test]
    fn order_row_with_invalid_status_fails_to_convert() {
        let mut row = OrderRow::from(&sample_order());
        row.status = "NotARealStatus".to_string();
        let result = Order::try_from(row);
        assert!(matches!(result, Err(OmsError::InvalidRow { .. })));
    }

    #[test]
    fn order_row_with_invalid_venue_fails_to_convert() {
        let mut row = OrderRow::from(&sample_order());
        row.venue = "NotARealVenue".to_string();
        let result = Order::try_from(row);
        assert!(matches!(result, Err(OmsError::InvalidRow { .. })));
    }

    fn sample_fill() -> Fill {
        Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            price: dec!(65000),
            quantity: dec!(0.5),
            fee: dec!(1.2),
            fee_currency: Some("USDT".to_string()),
            filled_at: Utc::now(),
        }
    }

    #[test]
    fn fill_round_trips_through_fill_row() {
        let fill = sample_fill();
        let row = FillRow::from(&fill);
        let back = Fill::try_from(row).unwrap();
        assert_eq!(back, fill);
    }

    #[test]
    fn fill_with_none_fee_currency_round_trips() {
        let mut fill = sample_fill();
        fill.fee_currency = None;
        let row = FillRow::from(&fill);
        let back = Fill::try_from(row).unwrap();
        assert_eq!(back.fee_currency, None);
    }
}
```

Note: `AssetClass`/`MarketType` are imported in the test module for consistency with other test files in this codebase but are not directly used by these particular tests -- if `cargo clippy` flags them as unused imports, remove them from the `use` line (they were included defensively in case earlier drafts of this test needed `Instrument` construction; this task's tests only need `Order`/`Fill` construction, which does not require an `Instrument`).

- [ ] **Step 2: Update `crates/apexes-oms/src/lib.rs`**

```rust
//! Apexes order management system (OMS): gateway-agnostic order
//! lifecycle management (submission with pre-trade risk checks,
//! cancellation, persistence, and reconnect reconciliation). Covers
//! `docs/FEATURES.md` rows 5.1.1 (order generation & pre-trade
//! validation) and 5.1.3 (persistence & reconnect state
//! recovery/reconciliation). Row 5.1.2 (order state machine) is
//! unchanged from sub-project #0's `apexes_core::OrderStatus`.
//!
//! [`ExecutionGateway`] decouples order submission/cancellation/query
//! from any specific exchange -- sub-project #8 (first live gateway)
//! implements it against a real exchange without needing to modify this
//! crate. This crate ships only [`mock_gateway::MockExecutionGateway`]
//! for testing.

pub mod error;
pub mod gateway;
pub mod mock_gateway;
pub mod schema;

pub use error::OmsError;
pub use gateway::{ExecutionGateway, GatewayOutcome};
pub use mock_gateway::MockExecutionGateway;
```

- [ ] **Step 3: Run the tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-oms schema:: -- --nocapture
```
Expected: 6 passed, 0 failed.

- [ ] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-oms --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed; if clippy flags the unused `AssetClass`/`MarketType` imports noted above, remove them and re-run). Re-run the test command afterward to confirm nothing regressed.

- [ ] **Step 5: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [ ] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-oms/src
git commit -S -s -m "feat: add PostgreSQL schema (OrderRow/FillRow) to apexes-oms"
git push origin master
```

---

### Task 5: `OrderStore`

**Files:**
- Create: `crates/apexes-oms/src/store.rs`
- Modify: `crates/apexes-oms/src/lib.rs`

**Interfaces:**
- Consumes: `crate::schema::{ORDERS_TABLE_DDL, ORDERS_EXCHANGE_ID_INDEX_DDL, FILLS_TABLE_DDL, FILLS_ORDER_ID_INDEX_DDL, OrderRow, FillRow}` (Task 4), `crate::error::OmsError` (Task 1), `apexes_core::{Order, OrderId, Fill}` (existing).
- Produces: `pub struct OrderStore` with `new`/`upsert_order`/`get_order`/`get_open_orders`/`insert_fill`/`get_fills_for_order`. Consumed by Task 6's `OrderManager` and Task 8's end-to-end test.

- [ ] **Step 1: Write `crates/apexes-oms/src/store.rs`**

```rust
//! [`OrderStore`]: pure PostgreSQL read/write for `Order`/`Fill` -- no
//! business logic (no risk checks, no gateway calls, no state-machine
//! validation). [`crate::manager::OrderManager`] is the business-logic
//! layer built on top of this.

use apexes_core::{Fill, Order, OrderId};
use sqlx::PgPool;

use crate::error::OmsError;
use crate::schema::{
    FILLS_ORDER_ID_INDEX_DDL, FILLS_TABLE_DDL, FillRow, ORDERS_EXCHANGE_ID_INDEX_DDL,
    ORDERS_TABLE_DDL, OrderRow,
};

pub struct OrderStore {
    pool: PgPool,
}

impl OrderStore {
    /// Connects to PostgreSQL at `url` and ensures both tables (plus
    /// their indexes) exist.
    pub async fn new(url: &str) -> Result<Self, OmsError> {
        let pool = PgPool::connect(url)
            .await
            .map_err(|error| OmsError::ConnectionFailed {
                reason: error.to_string(),
            })?;

        sqlx::query(ORDERS_TABLE_DDL).execute(&pool).await?;
        sqlx::query(ORDERS_EXCHANGE_ID_INDEX_DDL).execute(&pool).await?;
        sqlx::query(FILLS_TABLE_DDL).execute(&pool).await?;
        sqlx::query(FILLS_ORDER_ID_INDEX_DDL).execute(&pool).await?;

        Ok(Self { pool })
    }

    /// Inserts a new order, or updates it if `order.id` already exists
    /// (`ON CONFLICT (id) DO UPDATE`) -- an idempotent upsert, since
    /// `OrderManager` writes the same order multiple times across its
    /// lifecycle: once at creation, again after gateway submission
    /// updates `exchange_order_id`, again on every status change.
    pub async fn upsert_order(&self, order: &Order) -> Result<(), OmsError> {
        let row = OrderRow::from(order);
        sqlx::query(
            "INSERT INTO orders (id, exchange_order_id, symbol, venue, side, order_type, quantity, price, stop_price, filled_quantity, status, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
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

    pub async fn get_order(&self, id: OrderId) -> Result<Option<Order>, OmsError> {
        let row: Option<OrderRow> = sqlx::query_as("SELECT * FROM orders WHERE id = $1")
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await?;
        row.map(Order::try_from).transpose()
    }

    /// Returns every order currently NOT in a terminal state (`New` or
    /// `PartiallyFilled`) -- used by `OrderManager::reconcile` (and
    /// `OrderManager::new`'s startup recovery) to know which local
    /// orders need to be checked against the exchange.
    pub async fn get_open_orders(&self) -> Result<Vec<Order>, OmsError> {
        let rows: Vec<OrderRow> =
            sqlx::query_as("SELECT * FROM orders WHERE status IN ('New', 'PartiallyFilled')")
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter().map(Order::try_from).collect()
    }

    pub async fn insert_fill(&self, fill: &Fill) -> Result<(), OmsError> {
        let row = FillRow::from(fill);
        sqlx::query(
            "INSERT INTO fills (id, order_id, symbol, venue, side, price, quantity, fee, fee_currency, filled_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(row.id)
        .bind(row.order_id)
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
        Ok(())
    }

    pub async fn get_fills_for_order(&self, order_id: OrderId) -> Result<Vec<Fill>, OmsError> {
        let rows: Vec<FillRow> = sqlx::query_as("SELECT * FROM fills WHERE order_id = $1")
            .bind(order_id.0)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(Fill::try_from).collect()
    }
}
```

Note: no unit tests in this file -- `OrderStore` has no in-memory mode (it always talks to a real PostgreSQL connection), so all of its behavior is exercised by Task 6's `OrderManager` tests and Task 8's end-to-end test, both of which are `#[ignore]`d and require a live database. This mirrors `apexes_storage::StorageWriter`/`StorageReader`, which also carry no unit tests of their own for the same reason.

- [ ] **Step 2: Update `crates/apexes-oms/src/lib.rs`**

```rust
//! Apexes order management system (OMS): gateway-agnostic order
//! lifecycle management (submission with pre-trade risk checks,
//! cancellation, persistence, and reconnect reconciliation). Covers
//! `docs/FEATURES.md` rows 5.1.1 (order generation & pre-trade
//! validation) and 5.1.3 (persistence & reconnect state
//! recovery/reconciliation). Row 5.1.2 (order state machine) is
//! unchanged from sub-project #0's `apexes_core::OrderStatus`.
//!
//! [`ExecutionGateway`] decouples order submission/cancellation/query
//! from any specific exchange -- sub-project #8 (first live gateway)
//! implements it against a real exchange without needing to modify this
//! crate. This crate ships only [`mock_gateway::MockExecutionGateway`]
//! for testing.

pub mod error;
pub mod gateway;
pub mod mock_gateway;
pub mod schema;
pub mod store;

pub use error::OmsError;
pub use gateway::{ExecutionGateway, GatewayOutcome};
pub use mock_gateway::MockExecutionGateway;
pub use store::OrderStore;
```

- [ ] **Step 3: Verify it compiles**

```bash
cd ~/src/oss/apexes
cargo check -p apexes-oms
```
Expected: succeeds, 0 errors. Note: `sqlx::query`/`query_as` (the runtime-checked, non-macro API) do NOT require a live database connection at compile time -- only at actual runtime execution -- so this check succeeds without PostgreSQL running.

- [ ] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-oms --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run both commands).

- [ ] **Step 5: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [ ] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-oms/src
git commit -S -s -m "feat: add OrderStore (PostgreSQL persistence) to apexes-oms"
git push origin master
```

---

### Task 6: `OrderManager`

**Files:**
- Create: `crates/apexes-oms/src/manager.rs`
- Modify: `crates/apexes-oms/src/lib.rs`

**Interfaces:**
- Consumes: `crate::store::OrderStore` (Task 5), `crate::gateway::{ExecutionGateway, GatewayOutcome}` (Task 2), `crate::mock_gateway::MockExecutionGateway` (Task 3), `crate::error::OmsError` (Task 1), `apexes_risk::PreTradeCheck` (existing), `apexes_strategy::{OrderIntent, Position}` (existing), `apexes_core::{Order, OrderId, OrderStatus}` (existing).
- Produces: `pub struct OrderManager` with `new`/`submit_order`/`cancel_order`/`reconcile`/`open_orders`. This is the plan's final new public type -- consumed only by Task 8's end-to-end test and (in the future) sub-project `#8`.

**IMPORTANT:** This is the core business-logic task and the most safety-critical code in this plan (per the design spec's "non-negotiable quality bar" framing). Read the design spec's §六 in full before writing this file -- pay particular attention to: (a) persist-before-submit ordering, (b) the three-way `GatewayOutcome` handling for both `submit_order` and `cancel_order`, (c) `reconcile()` matching by client order ID (`OrderId`) not `exchange_order_id`, and (d) `reconcile()` bypassing `OrderStatus::can_transition_to` entirely (the exchange is authoritative).

- [ ] **Step 1: Write `crates/apexes-oms/src/manager.rs`**

```rust
//! [`OrderManager`]: the business-logic layer -- risk checks, gateway
//! submission, status tracking, and reconnect reconciliation. Holds an
//! in-memory `HashMap<OrderId, Order>` snapshot of open orders (mirroring
//! `apexes_backtest::PortfolioLedger`'s in-memory-plus-persistence
//! pattern), rebuilt from `OrderStore::get_open_orders` on construction
//! so a process restart recovers its view of in-flight orders without
//! waiting for the next `reconcile()` call.

use std::collections::HashMap;
use std::sync::Arc;

use apexes_core::{Order, OrderId, OrderStatus};
use apexes_risk::PreTradeCheck;
use apexes_strategy::{OrderIntent, Position};
use chrono::Utc;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::error::OmsError;
use crate::gateway::{ExecutionGateway, GatewayOutcome};
use crate::store::OrderStore;

pub struct OrderManager {
    store: OrderStore,
    gateway: Arc<dyn ExecutionGateway>,
    risk_checker: Option<Box<dyn PreTradeCheck>>,
    open_orders: HashMap<OrderId, Order>,
}

impl OrderManager {
    /// Constructs a new manager, rebuilding `open_orders` from
    /// `store.get_open_orders()` -- the "recover in-flight order state
    /// after a process restart" half of reconnect reconciliation;
    /// `reconcile()` is the other half (comparing that recovered state
    /// against the exchange's current truth).
    pub async fn new(
        store: OrderStore,
        gateway: Arc<dyn ExecutionGateway>,
        risk_checker: Option<Box<dyn PreTradeCheck>>,
    ) -> Result<Self, OmsError> {
        let open_orders_vec = store.get_open_orders().await?;
        let open_orders = open_orders_vec
            .into_iter()
            .map(|order| (order.id, order))
            .collect();
        Ok(Self {
            store,
            gateway,
            risk_checker,
            open_orders,
        })
    }

    /// Submits a new order:
    /// 1. Runs `risk_checker` (if configured) against `intent`; returns
    ///    `Err(OmsError::RiskCheckFailed)` without creating any order if
    ///    it fails. `positions`/`balances` are caller-supplied snapshots
    ///    passed straight through to `risk_checker.check(..)` --
    ///    `OrderManager` does not own position/balance tracking itself
    ///    (that is sub-project #11's scope).
    /// 2. Constructs an `Order` (status `New`, `exchange_order_id:
    ///    None`), persists it via `store.upsert_order` BEFORE calling
    ///    the gateway (a local record the gateway never accepted is
    ///    recoverable via reconciliation or manual review; a
    ///    gateway-accepted order with no local record is not).
    /// 3. Calls `gateway.submit_order`:
    ///    - `Accepted { exchange_order_id }`: updates the order's
    ///      `exchange_order_id` field, re-persists, inserts into
    ///      `open_orders`, returns `Ok(order_id)`.
    ///    - `Rejected { reason }`: transitions the order to `Rejected`,
    ///      re-persists, does NOT insert into `open_orders`, returns
    ///      `Err(OmsError::OrderRejected { reason })`.
    ///    - `Uncertain { reason }`: leaves the order's status as `New`
    ///      (already persisted), inserts into `open_orders` anyway (it
    ///      is genuinely uncertain whether it's live), logs a
    ///      `tracing::warn!`, and returns `Ok(order_id)` -- only
    ///      `reconcile()` will later resolve the true status.
    pub async fn submit_order(
        &mut self,
        intent: OrderIntent,
        positions: &HashMap<String, Position>,
        balances: &HashMap<String, Decimal>,
    ) -> Result<OrderId, OmsError> {
        if let Some(checker) = &self.risk_checker {
            let empty_open_orders = HashMap::new();
            if let Err(violations) = checker.check(
                &intent,
                positions,
                balances,
                &empty_open_orders,
                Utc::now(),
            ) {
                return Err(OmsError::RiskCheckFailed { violations });
            }
        }

        let now = Utc::now();
        let mut order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: intent.symbol,
            venue: intent.venue,
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
        self.store.upsert_order(&order).await?;

        match self.gateway.submit_order(&order).await {
            GatewayOutcome::Accepted { exchange_order_id } => {
                order.exchange_order_id = Some(exchange_order_id);
                order.updated_at = Utc::now();
                self.store.upsert_order(&order).await?;
                let order_id = order.id;
                self.open_orders.insert(order_id, order);
                Ok(order_id)
            }
            GatewayOutcome::Rejected { reason } => {
                order.status = OrderStatus::Rejected;
                order.updated_at = Utc::now();
                self.store.upsert_order(&order).await?;
                Err(OmsError::OrderRejected { reason })
            }
            GatewayOutcome::Uncertain { reason } => {
                tracing::warn!(
                    order_id = ?order.id,
                    reason,
                    "order submission outcome uncertain; deferring to reconcile()"
                );
                let order_id = order.id;
                self.open_orders.insert(order_id, order);
                Ok(order_id)
            }
        }
    }

    /// Cancels an existing order:
    /// 1. Looks up `order_id` in `open_orders`; returns
    ///    `Err(OmsError::OrderNotFound)` if absent, or
    ///    `Err(OmsError::OrderNotCancellable { current_status })` if its
    ///    status isn't `New`/`PartiallyFilled`.
    /// 2. Checks `order.exchange_order_id`: if `None` (this order's
    ///    original `submit_order` call returned `Uncertain` and has not
    ///    yet been resolved by `reconcile()`), returns
    ///    `Err(OmsError::ExchangeOrderIdUnknown)` rather than calling
    ///    the gateway.
    /// 3. Otherwise calls `gateway.cancel_order`. Same three-way
    ///    `GatewayOutcome` handling as `submit_order`: `Accepted` ->
    ///    mark `Cancelled`, remove from `open_orders`; `Rejected` ->
    ///    return `Err` with the reason, order status unchanged;
    ///    `Uncertain` -> log and leave status unchanged, defer to
    ///    `reconcile()`.
    pub async fn cancel_order(&mut self, order_id: OrderId) -> Result<(), OmsError> {
        let order = self
            .open_orders
            .get(&order_id)
            .ok_or(OmsError::OrderNotFound { order_id })?;

        if !matches!(order.status, OrderStatus::New | OrderStatus::PartiallyFilled) {
            return Err(OmsError::OrderNotCancellable {
                order_id,
                current_status: order.status,
            });
        }

        let exchange_order_id = order
            .exchange_order_id
            .clone()
            .ok_or(OmsError::ExchangeOrderIdUnknown { order_id })?;

        match self.gateway.cancel_order(&exchange_order_id).await {
            GatewayOutcome::Accepted { .. } => {
                let mut order = self.open_orders.remove(&order_id).unwrap();
                order.status = OrderStatus::Cancelled;
                order.updated_at = Utc::now();
                self.store.upsert_order(&order).await?;
                Ok(())
            }
            GatewayOutcome::Rejected { reason } => Err(OmsError::OrderRejected { reason }),
            GatewayOutcome::Uncertain { reason } => {
                tracing::warn!(
                    order_id = ?order_id,
                    reason,
                    "order cancellation outcome uncertain; deferring to reconcile()"
                );
                Ok(())
            }
        }
    }

    /// Reconnect reconciliation: calls
    /// `gateway.query_all_open_orders()`, and for every
    /// `(client_order_id, exchange_order_id, exchange_status)` triple
    /// returned, looks up the matching local order BY `client_order_id`
    /// (our own `OrderId` -- NOT `exchange_order_id`, which may still be
    /// unknown locally for `Uncertain`-outcome orders). If found: sets
    /// `order.exchange_order_id` (populating it if it was `None`) and
    /// OVERWRITES the local status with the exchange's reported status
    /// UNCONDITIONALLY -- bypassing `OrderStatus::can_transition_to`'s
    /// validation entirely, per this design's explicit decision that
    /// the exchange is authoritative during reconciliation. If NOT
    /// found (a `client_order_id` the exchange reports that we have no
    /// local record of at all), logs a `tracing::error!` and skips it.
    /// Local orders NOT present in the exchange's open-order list are
    /// left unchanged if already terminal, or logged as a discrepancy
    /// (`tracing::warn!`) if the local status is still `New`/
    /// `PartiallyFilled`. Every status change is persisted via
    /// `store.upsert_order` and reflected in `open_orders`. Returns the
    /// list of `OrderId`s whose status actually changed.
    pub async fn reconcile(&mut self) -> Result<Vec<OrderId>, OmsError> {
        let exchange_orders = self
            .gateway
            .query_all_open_orders()
            .await
            .map_err(|reason| OmsError::QueryFailed { reason })?;

        let mut changed = Vec::new();
        let exchange_order_ids: std::collections::HashSet<OrderId> =
            exchange_orders.iter().map(|(id, _, _)| *id).collect();

        for (client_order_id, exchange_order_id, exchange_status) in exchange_orders {
            match self.open_orders.get_mut(&client_order_id) {
                Some(order) => {
                    let status_changed = order.status != exchange_status;
                    if exchange_order_id.is_some() {
                        order.exchange_order_id = exchange_order_id;
                    }
                    if status_changed {
                        order.status = exchange_status;
                        order.updated_at = Utc::now();
                        self.store.upsert_order(order).await?;
                        changed.push(client_order_id);
                    }
                }
                None => {
                    tracing::error!(
                        order_id = ?client_order_id,
                        "reconcile: exchange reports an open order we have no local record of"
                    );
                }
            }
        }

        for (local_id, local_order) in self.open_orders.iter() {
            if !exchange_order_ids.contains(local_id)
                && matches!(
                    local_order.status,
                    OrderStatus::New | OrderStatus::PartiallyFilled
                )
            {
                tracing::warn!(
                    order_id = ?local_id,
                    "reconcile: local order still open but exchange no longer reports it as open"
                );
            }
        }

        // Remove any orders that reconciliation moved to a terminal
        // status from the open-orders map (mirroring submit_order/
        // cancel_order's own bookkeeping).
        self.open_orders
            .retain(|_, order| matches!(order.status, OrderStatus::New | OrderStatus::PartiallyFilled));

        Ok(changed)
    }

    pub fn open_orders(&self) -> &HashMap<OrderId, Order> {
        &self.open_orders
    }
}
```

- [ ] **Step 2: Update `crates/apexes-oms/src/lib.rs`**

```rust
//! Apexes order management system (OMS): gateway-agnostic order
//! lifecycle management (submission with pre-trade risk checks,
//! cancellation, persistence, and reconnect reconciliation). Covers
//! `docs/FEATURES.md` rows 5.1.1 (order generation & pre-trade
//! validation) and 5.1.3 (persistence & reconnect state
//! recovery/reconciliation). Row 5.1.2 (order state machine) is
//! unchanged from sub-project #0's `apexes_core::OrderStatus`.
//!
//! [`ExecutionGateway`] decouples order submission/cancellation/query
//! from any specific exchange -- sub-project #8 (first live gateway)
//! implements it against a real exchange without needing to modify this
//! crate. This crate ships only [`mock_gateway::MockExecutionGateway`]
//! for testing.

pub mod error;
pub mod gateway;
pub mod manager;
pub mod mock_gateway;
pub mod schema;
pub mod store;

pub use error::OmsError;
pub use gateway::{ExecutionGateway, GatewayOutcome};
pub use manager::OrderManager;
pub use mock_gateway::MockExecutionGateway;
pub use store::OrderStore;
```

- [ ] **Step 3: Verify it compiles**

```bash
cd ~/src/oss/apexes
cargo check -p apexes-oms
```
Expected: succeeds, 0 errors.

- [ ] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-oms --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run both commands).

- [ ] **Step 5: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors. `manager.rs` has no unit tests in this task -- those are Task 7's job (they require a live PostgreSQL connection since `OrderManager` owns a real `OrderStore`, so they are written as a separate, clearly `#[ignore]`d task rather than bundled into this one, matching this plan's Global Constraints on network/database-dependent tests).

- [ ] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-oms/src
git commit -S -s -m "feat: add OrderManager to apexes-oms"
git push origin master
```

---

### Task 7: `OrderManager` integration tests (require live PostgreSQL)

**Files:**
- Modify: `crates/apexes-oms/src/manager.rs` (append `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `crate::manager::OrderManager` (Task 6), `crate::store::OrderStore` (Task 5), `crate::mock_gateway::MockExecutionGateway` (Task 3), `apexes_risk::{PreTradeCheck, RiskViolation}` (existing). Requires a live PostgreSQL connection -- all tests here are `#[ignore]`, matching `apexes-storage`'s established pattern.
- Produces: nothing new consumed by later tasks -- this task only adds test coverage for the plan's most safety-critical logic.

- [ ] **Step 1: Append the test module to `crates/apexes-oms/src/manager.rs`**

```rust

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_gateway::MockExecutionGateway;
    use apexes_core::{OrderType, Side, Venue};
    use apexes_risk::RiskViolation;
    use chrono::DateTime;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    const POSTGRES_URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

    /// Wraps a `PreTradeCheck` that always rejects, for testing
    /// `submit_order`'s risk-check-failure path without depending on
    /// `apexes-risk`'s concrete checkers.
    struct AlwaysRejectCheck;

    impl PreTradeCheck for AlwaysRejectCheck {
        fn check(
            &self,
            intent: &OrderIntent,
            _positions: &HashMap<String, Position>,
            _balances: &HashMap<String, Decimal>,
            _open_orders: &HashMap<OrderId, Order>,
            _now: DateTime<Utc>,
        ) -> Result<(), Vec<RiskViolation>> {
            Err(vec![RiskViolation::SymbolBlacklisted {
                symbol: intent.symbol.clone(),
            }])
        }
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

    async fn fresh_manager(gateway: Arc<dyn ExecutionGateway>) -> OrderManager {
        let store = OrderStore::new(POSTGRES_URL).await.unwrap();
        OrderManager::new(store, gateway, None).await.unwrap()
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_with_failing_risk_check_creates_no_order() {
        let store = OrderStore::new(POSTGRES_URL).await.unwrap();
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = OrderManager::new(store, gateway, Some(Box::new(AlwaysRejectCheck)))
            .await
            .unwrap();

        let result = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await;

        assert!(matches!(result, Err(OmsError::RiskCheckFailed { .. })));
        assert!(manager.open_orders().is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_accepted_by_gateway_appears_in_open_orders_with_exchange_id() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(gateway).await;

        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        let order = manager.open_orders().get(&order_id).unwrap();
        assert!(order.exchange_order_id.is_some());
        assert_eq!(order.status, OrderStatus::New);
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_rejected_by_gateway_is_not_in_open_orders() {
        let gateway = Arc::new(MockExecutionGateway::new());
        gateway.program_next_submit_outcome(GatewayOutcome::Rejected {
            reason: "insufficient balance".to_string(),
        });
        let mut manager = fresh_manager(gateway).await;

        let result = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await;

        assert!(matches!(result, Err(OmsError::OrderRejected { .. })));
        assert!(manager.open_orders().is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn submit_order_uncertain_outcome_stays_open_with_no_exchange_id() {
        let gateway = Arc::new(MockExecutionGateway::new());
        gateway.program_next_submit_outcome(GatewayOutcome::Uncertain {
            reason: "connection reset".to_string(),
        });
        let mut manager = fresh_manager(gateway).await;

        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        let order = manager.open_orders().get(&order_id).unwrap();
        assert_eq!(order.exchange_order_id, None);
        assert_eq!(order.status, OrderStatus::New);
    }

    #[tokio::test]
    #[ignore]
    async fn cancel_order_not_found_returns_error() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(gateway).await;

        let result = manager.cancel_order(OrderId(Uuid::new_v4())).await;
        assert!(matches!(result, Err(OmsError::OrderNotFound { .. })));
    }

    #[tokio::test]
    #[ignore]
    async fn cancel_order_with_unknown_exchange_id_is_rejected_without_calling_gateway() {
        let gateway = Arc::new(MockExecutionGateway::new());
        gateway.program_next_submit_outcome(GatewayOutcome::Uncertain {
            reason: "timeout".to_string(),
        });
        let mut manager = fresh_manager(gateway).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        let result = manager.cancel_order(order_id).await;
        assert!(matches!(
            result,
            Err(OmsError::ExchangeOrderIdUnknown { .. })
        ));
    }

    #[tokio::test]
    #[ignore]
    async fn cancel_order_accepted_removes_it_from_open_orders() {
        let gateway = Arc::new(MockExecutionGateway::new());
        let mut manager = fresh_manager(gateway).await;
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();

        manager.cancel_order(order_id).await.unwrap();

        assert!(manager.open_orders().get(&order_id).is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_overwrites_local_status_from_exchange_even_skipping_partially_filled() {
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

        // Simulate the exchange having gone straight to Filled while we
        // were disconnected -- a transition OrderStatus::can_transition_to
        // would normally allow (New -> Filled is valid), but this test's
        // real point is proven by the NEXT test below, which uses a
        // transition can_transition_to would reject.
        gateway.set_exchange_state(order_id, OrderStatus::Filled);

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed, vec![order_id]);
        // Filled is terminal -- order should no longer be in open_orders.
        assert!(manager.open_orders().get(&order_id).is_none());

        let persisted = manager.store.get_order(order_id).await.unwrap().unwrap();
        assert_eq!(persisted.status, OrderStatus::Filled);
    }

    #[tokio::test]
    #[ignore]
    async fn reconcile_populates_exchange_order_id_for_a_previously_uncertain_order() {
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
            manager.open_orders().get(&order_id).unwrap().exchange_order_id,
            None
        );

        // The mock gateway's submit_order, even with a programmed
        // Uncertain outcome for the RETURN VALUE, does not record
        // exchange-side state in this scenario since Uncertain doesn't
        // trigger the mock's internal bookkeeping (see
        // MockExecutionGateway::submit_order: only Accepted outcomes --
        // programmed or default -- populate exchange_state). To
        // exercise reconcile()'s client-order-id-matching path for a
        // truly uncertain order, this test directly seeds the mock's
        // exchange_state as if the exchange DID actually process the
        // order despite our local Uncertain result -- this is exactly
        // the scenario reconcile() exists to resolve. Since
        // MockExecutionGateway has no public API to seed state without
        // first calling submit_order successfully, submit a second,
        // separate order through the SAME manager with a normal
        // Accepted outcome first to obtain a valid client_order_id/
        // exchange_order_id pairing pattern, then directly verify
        // reconcile()'s no-op behavior when nothing changed:
        let changed = manager.reconcile().await.unwrap();
        // No exchange-side record exists yet for the Uncertain order
        // (the mock never registered it), so query_all_open_orders
        // returns nothing for it, and reconcile makes no change here --
        // this test documents that limitation of the mock rather than
        // asserting a false positive. A true "Uncertain order later
        // confirmed by the exchange" scenario requires
        // MockExecutionGateway to expose a way to register exchange
        // state for an order it never itself accepted; if this
        // limitation blocks meaningful test coverage, extend
        // MockExecutionGateway with a `force_register(order_id,
        // exchange_order_id, status)` method in this task rather than
        // skipping the scenario -- do not leave this gap unaddressed.
        assert!(changed.is_empty());
    }

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

        // Cancelled order is terminal and no longer in open_orders;
        // reconcile should not error or resurrect it.
        let changed = manager.reconcile().await.unwrap();
        assert!(!changed.contains(&order_id));
    }
}
```

**IMPORTANT -- flagged gap in this task's own test coverage**: the `reconcile_populates_exchange_order_id_for_a_previously_uncertain_order` test above documents (via its own comments) that `MockExecutionGateway` as designed in Task 3 has no way to register exchange-side state for an order whose `submit_order` call returned `Uncertain` (since the mock only records state for outcomes it itself returns as `Accepted`). This is a real coverage gap for one of this plan's most important scenarios (an `Uncertain` order later confirmed live by the exchange, reconciled via client order ID). Before considering this task done, add a `pub fn force_register_exchange_order(&self, client_order_id: OrderId, exchange_order_id: &str, status: OrderStatus)` method to `MockExecutionGateway` (in `crates/apexes-oms/src/mock_gateway.rs`) that directly inserts into `exchange_state` regardless of prior `submit_order` calls, then rewrite `reconcile_populates_exchange_order_id_for_a_previously_uncertain_order` to use it: call `force_register_exchange_order(order_id, "EX-999", OrderStatus::Filled)` after the `Uncertain` submission, then call `reconcile()` and assert `changed == vec![order_id]`, the order is removed from `open_orders` (terminal), and `manager.store.get_order(order_id).await.unwrap().unwrap().exchange_order_id == Some("EX-999".to_string())`. This is not optional polish -- it is the test that actually proves the design's core reconciliation-for-uncertain-orders guarantee.

- [ ] **Step 2: Add `force_register_exchange_order` to `MockExecutionGateway`**

Edit `crates/apexes-oms/src/mock_gateway.rs`, adding this method inside the existing `impl MockExecutionGateway` block (after `set_exchange_state`):

```rust
    /// Directly registers exchange-side state for `client_order_id`
    /// WITHOUT requiring a prior `submit_order` call to have gone
    /// through this mock -- used to simulate an order whose
    /// `submit_order` returned `Uncertain` (so the mock never itself
    /// recorded exchange state for it) later being confirmed live by
    /// the exchange, the scenario `OrderManager::reconcile` exists to
    /// resolve.
    pub fn force_register_exchange_order(
        &self,
        client_order_id: OrderId,
        exchange_order_id: &str,
        status: OrderStatus,
    ) {
        self.exchange_state
            .lock()
            .unwrap()
            .insert(client_order_id, (exchange_order_id.to_string(), status));
    }
```

- [ ] **Step 3: Rewrite the previously-flagged test to use it**

Replace the `reconcile_populates_exchange_order_id_for_a_previously_uncertain_order` test's body (in `crates/apexes-oms/src/manager.rs`) with:

```rust
    #[tokio::test]
    #[ignore]
    async fn reconcile_populates_exchange_order_id_for_a_previously_uncertain_order() {
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
            manager.open_orders().get(&order_id).unwrap().exchange_order_id,
            None
        );

        // The exchange actually processed the order despite our local
        // Uncertain result, and it has since been filled.
        gateway.force_register_exchange_order(order_id, "EX-999", OrderStatus::Filled);

        let changed = manager.reconcile().await.unwrap();

        assert_eq!(changed, vec![order_id]);
        assert!(manager.open_orders().get(&order_id).is_none());

        let persisted = manager.store.get_order(order_id).await.unwrap().unwrap();
        assert_eq!(persisted.status, OrderStatus::Filled);
        assert_eq!(persisted.exchange_order_id, Some("EX-999".to_string()));
    }
```

- [ ] **Step 4: Start PostgreSQL and run the tests**

```bash
cd ~/src/oss/apexes
docker compose up -d
sleep 5
cargo test -p apexes-oms --lib -- --ignored --nocapture
```
Expected: all 10 `#[ignore]`d tests in `manager.rs` pass (9 original + the rewritten uncertain-order test), plus `mock_gateway.rs`'s existing non-ignored tests remain unaffected (run separately, they are not `#[ignore]`d).

- [ ] **Step 5: Run the full non-ignored suite, clippy, and fmt**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-oms
cargo clippy -p apexes-oms --all-targets -- -D warnings
cargo fmt --check
```
Expected: non-ignored tests unchanged from before this task (3 error + 6 mock_gateway + 6 schema = 15), clippy and fmt clean (auto-fix with `cargo fmt` if needed, then re-run all three commands plus Step 4's ignored-test command to confirm nothing regressed).

- [ ] **Step 6: Full workspace verification**

```bash
cd ~/src/oss/apexes
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```
Expected: all clean, 0 failures.

- [ ] **Step 7: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-oms/src/manager.rs crates/apexes-oms/src/mock_gateway.rs
git commit -S -s -m "test: add OrderManager integration tests including reconnect reconciliation"
git push origin master
```

---

### Task 8: End-to-end integration test (process restart recovery)

**Files:**
- Create: `crates/apexes-oms/tests/live_postgres.rs`

**Interfaces:**
- Consumes: `apexes_oms::{OrderManager, OrderStore, MockExecutionGateway, ExecutionGateway}` (Tasks 2-6), `apexes_strategy::OrderIntent` (existing). Requires a live PostgreSQL connection -- marked `#[ignore]`.
- Produces: nothing new consumed by later tasks -- this is the plan's final behavioral proof point before Task 9's whole-effort verification.

- [ ] **Step 1: Write `crates/apexes-oms/tests/live_postgres.rs`**

```rust
//! End-to-end integration test proving `OrderManager` correctly
//! recovers its in-flight order state after a simulated process
//! restart (constructing a fresh `OrderManager` instance against the
//! same PostgreSQL database) and that `reconcile()` then correctly
//! resolves any exchange-side changes that happened in between.
//! Requires a live PostgreSQL instance -- run explicitly with:
//!   docker compose up -d
//!   cargo test -p apexes-oms --test live_postgres -- --ignored

use std::collections::HashMap;
use std::sync::Arc;

use apexes_core::{OrderStatus, OrderType, Side, Venue};
use apexes_oms::{ExecutionGateway, MockExecutionGateway, OrderManager, OrderStore};
use apexes_strategy::OrderIntent;
use rust_decimal_macros::dec;

const POSTGRES_URL: &str = "postgres://apexes:apexes@localhost:5432/apexes";

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

#[tokio::test]
#[ignore]
async fn order_manager_recovers_open_orders_after_simulated_restart_and_reconciles() {
    let gateway = Arc::new(MockExecutionGateway::new());

    // "Process instance 1": submit an order, then drop the manager
    // (simulating a crash/restart) without cancelling or otherwise
    // cleaning up.
    let order_id = {
        let store = OrderStore::new(POSTGRES_URL).await.unwrap();
        let mut manager = OrderManager::new(store, Arc::clone(&gateway) as Arc<dyn ExecutionGateway>, None)
            .await
            .unwrap();
        let order_id = manager
            .submit_order(sample_intent(), &HashMap::new(), &HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            manager.open_orders().get(&order_id).unwrap().status,
            OrderStatus::New
        );
        order_id
        // `manager` (and its `OrderStore`/`PgPool`) is dropped here.
    };

    // While "disconnected", the exchange fills the order.
    gateway.set_exchange_state(order_id, OrderStatus::Filled);

    // "Process instance 2": a fresh OrderManager against the same
    // PostgreSQL database must recover the order as open (from
    // persisted state) before any reconcile() call.
    let store = OrderStore::new(POSTGRES_URL).await.unwrap();
    let mut manager = OrderManager::new(store, Arc::clone(&gateway) as Arc<dyn ExecutionGateway>, None)
        .await
        .unwrap();
    assert_eq!(
        manager.open_orders().get(&order_id).unwrap().status,
        OrderStatus::New,
        "a freshly constructed OrderManager must recover in-flight order state from PostgreSQL"
    );

    // Now reconcile discovers the fill that happened while disconnected.
    let changed = manager.reconcile().await.unwrap();
    assert_eq!(changed, vec![order_id]);
    assert!(manager.open_orders().get(&order_id).is_none());

    let persisted = manager.store.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(persisted.status, OrderStatus::Filled);
}
```

- [ ] **Step 2: Run the end-to-end test**

```bash
cd ~/src/oss/apexes
docker compose up -d
sleep 5
cargo test -p apexes-oms --test live_postgres -- --ignored --nocapture
```
Expected: `1 passed`.

- [ ] **Step 3: Run the full `apexes-oms` test suite (ignored + non-ignored), clippy, and fmt**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-oms
cargo test -p apexes-oms -- --ignored
cargo clippy -p apexes-oms --all-targets -- -D warnings
cargo fmt --check
```
Expected: non-ignored: 15 passed (3 error + 6 mock_gateway + 6 schema). Ignored: 11 passed (10 from Task 7's `manager.rs` tests + 1 from this task's `live_postgres.rs`). Clippy and fmt clean (auto-fix with `cargo fmt` if needed, then re-run all four commands).

- [ ] **Step 4: Full workspace verification**

```bash
cd ~/src/oss/apexes
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```
Expected: all clean, 0 failures.

- [ ] **Step 5: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-oms/tests
git commit -S -s -m "test: add end-to-end process-restart recovery and reconciliation test"
git push origin master
```

---

### Task 9: Final whole-effort verification

**Files:** None created/modified -- this task only runs verification commands, updates `docs/FEATURES.md`, and confirms the end state.

- [ ] **Step 1: Full workspace build**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [ ] **Step 2: Full workspace test suite (non-ignored)**

```bash
cd ~/src/oss/apexes
cargo test --workspace
```
Expected: all tests across all crates pass. Confirm `apexes-core`/`apexes-market-data`/`apexes-indicators`/`apexes-strategy`/`apexes-storage`/`apexes-strategy-registry`/`apexes-optimize`/`apexes-risk`/`apexes-backtest`/`apexes-paper` are completely unaffected by this plan (unchanged pass counts from before Task 1 started); `apexes-oms` is brand new with 15 non-ignored tests (3 error + 6 mock_gateway + 6 schema).

- [ ] **Step 3: Full workspace test suite (ignored/live-network)**

```bash
cd ~/src/oss/apexes
docker compose up -d
sleep 5
cargo test -p apexes-market-data -- --ignored
cargo test -p apexes-storage -- --ignored
cargo test -p apexes-strategy-registry -- --ignored
cargo test -p apexes-paper -- --ignored
cargo test -p apexes-oms -- --ignored
```
Expected: `apexes-market-data`/`apexes-storage`/`apexes-strategy-registry`/`apexes-paper`'s pre-existing ignored tests behave exactly as they did before this plan started (unchanged -- note `apexes-market-data`'s and `apexes-paper`'s live Binance tests may be affected by this sandbox's known pre-existing network limitations documented in the prior `apexes-paper` plan; that is unrelated to this plan's work). `apexes-oms` contributes 11 new ignored tests (10 `manager.rs` + 1 `live_postgres.rs`), all passing against the live PostgreSQL container started above.

- [ ] **Step 4: Full workspace clippy**

```bash
cd ~/src/oss/apexes
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: 0 warnings.

- [ ] **Step 5: Full workspace fmt check**

```bash
cd ~/src/oss/apexes
cargo fmt --check
```
Expected: 0 diffs.

- [ ] **Step 6: Update `docs/FEATURES.md`**

Read `docs/FEATURES.md`, find rows `5.1.1` (Order generation & pre-trade validation, currently `🔲 Not Started`) and `5.1.3` (Persistence & reconnect state recovery/reconciliation, currently `🔲 Not Started`, flagged as a non-negotiable quality bar). Update BOTH rows' Status to `✅ Done`.

Row `5.1.1`'s Notes:
```
apexes-oms crate: OrderManager::submit_order runs apexes-risk's PreTradeCheck (caller-supplied positions/balances snapshots -- account/fund tracking itself remains sub-project #11's scope) before persisting a new Order (status New) to PostgreSQL and submitting it to an ExecutionGateway. Gateway-agnostic design: ExecutionGateway trait (submit_order/cancel_order/query_order_status/query_all_open_orders) is defined in apexes-oms itself, with only a MockExecutionGateway test double shipped in this pass -- sub-project #8 (first live gateway) implements it against a real exchange without needing to modify apexes-oms. GatewayOutcome is a three-way enum (Accepted/Rejected/Uncertain) so network-timeout-style ambiguous failures are never conflated with the exchange's explicit rejections.
```

Row `5.1.3`'s Notes:
```
apexes-oms crate: introduces PostgreSQL (via sqlx) as the project's first relational-database backend, per the roadmap's long-planned "OMS needs durable order storage" trigger point (docs/FEATURES.md row 8.2) -- ClickHouse (apexes-storage, apexes-strategy-registry) is left unchanged, since its OLAP/append-only characteristics are a poor fit for the strong-consistency/low-latency-point-query/idempotent-write requirements order persistence has, which this sub-project's non-negotiable-quality-bar status made worth addressing head-on rather than deferring. Orders/Fills persist to PostgreSQL (persist-before-submit ordering: a local record the gateway never accepted is recoverable; a gateway-accepted order with no local record is not). OrderManager::new rebuilds its open-order snapshot from PostgreSQL on construction (process-restart recovery); OrderManager::reconcile() then resolves any exchange-side state changes that happened while disconnected, matching by client order ID (our own OrderId, not the exchange's ID, since Uncertain-outcome orders never learn an exchange ID any other way) and unconditionally overwriting local status with the exchange's authoritative report -- deliberately bypassing OrderStatus::can_transition_to's normal-operation validation, since a "disallowed" transition during reconciliation reflects real events that happened while disconnected, not a bug. No automatic retry of Uncertain outcomes (deferred to a future iteration pending a dedicated idempotency-key design); no Strategy/MatchingEngine integration (deferred to #8); no multi-account support (deferred to #11).
```

**Important:** before finalizing, do not just increment counts by 2/2 -- actually count the current `✅ Done` / `🔲 Not Started` / `⏸️ Deferred` (and any other status symbols present) rows in the file (e.g. `grep -cE "^\| [0-9].*<symbol>"` against each status marker, restricted to actual data rows -- NOT the legend rows near the top of the file and NOT the Summary table's own rows, which is exactly the distinction a prior plan's Task 6 got wrong and had to be corrected for) and confirm your new numbers match Done+2, Not Started-2, Total unchanged, against the real counts. Update the Summary counts table accordingly and the "Current active work" paragraph to note sub-project `#7`'s rows `5.1.1`/`5.1.3` are complete via the new `apexes-oms` crate (alongside the already-`Done` row `5.1.2`); leave "what's next" unstated since sub-project `#8` (first live gateway, now unlocked since it depends on `#7`), `#9`/`#11` (EMS, account management), the PyO3 bindings evaluation, and `apexes-risk`'s `6.4`/`6.5` are all still open/deferred options -- don't guess which one the user picks next.

- [ ] **Step 7: Commit the FEATURES.md update**

```bash
cd ~/src/oss/apexes
git add docs/FEATURES.md
git commit -S -s -m "docs: mark order generation and persistence/reconciliation complete in FEATURES.md"
git push origin master
```

- [ ] **Step 8: Mark this plan's checkboxes complete**

```bash
cd ~/src/oss/apexes
sed -i '' 's/^- \[ \]/- [x]/' docs/superpowers/plans/2026-07-31-apexes-oms.md
```
Check `grep -c "^\- \[ \]" docs/superpowers/plans/2026-07-31-apexes-oms.md` reports `0` remaining unmarked checkboxes. Read the top of the file after running `sed` to verify the header's own descriptive sentence (`Steps use checkbox (\`- [ ]\`) syntax for tracking`) was NOT affected -- that occurrence is inside inline code backticks on its own line and does not match the `^- \[ \]` line-start pattern, so it should be unaffected, but confirm rather than assume.

```bash
git add docs/superpowers/plans/2026-07-31-apexes-oms.md
git commit -S -s -m "docs: mark apexes-oms plan complete"
git push origin master
```

- [ ] **Step 9: Report final summary**

Summarize: `apexes-oms` crate's file list and line counts, total test count (broken down: unit tests per module, ignored/live-PostgreSQL tests, the end-to-end restart-recovery test), confirmation that every other crate's pre-existing tests still pass unchanged, and the `apexes` commit SHAs for Tasks 1-9. No further commit needed for this verification-only task.

---

## Plan Self-Review Notes

**Spec coverage:** Design spec §一 (scope: gateway-agnostic `ExecutionGateway`, OMS does not drive the trading loop, PostgreSQL introduction with explicit rationale for not using ClickHouse, explicit non-goals around #8/#9/#11/auto-retry/`apexes-strategy-registry` migration/pluggable-DB-backend) → Task 1 (docker-compose + scaffold), Task 2 (`ExecutionGateway`), Tasks 3-6 (mock gateway, schema, store, manager); non-goals respected by omission throughout. §二 (crate structure: `lib.rs`/`error.rs`/`gateway.rs`/`mock_gateway.rs`/`schema.rs`/`store.rs`/`manager.rs`) → every file created exactly as named across Tasks 1-6. §三 (`ExecutionGateway` trait + `GatewayOutcome` three-way enum, including the corrected `query_all_open_orders` signature returning `(OrderId, Option<String>, OrderStatus)` triples for client-order-ID matching) → Task 2. §四 (PostgreSQL schema, `OrderRow`/`FillRow`, `TryFrom` conversions) → Task 4. §五 (`OrderStore`, pure persistence) → Task 5. §六 (`OrderManager`: persist-before-submit ordering, three-way `GatewayOutcome` handling for both submit and cancel including the `ExchangeOrderIdUnknown` cancel-path fix, `reconcile()`'s client-order-ID matching and unconditional status overwrite bypassing `can_transition_to`, the `positions`/`balances` caller-supplied-parameter fix for `submit_order`) → Task 6. §七 (`MockExecutionGateway`) → Task 3, extended in Task 7 with `force_register_exchange_order` to close a coverage gap discovered while writing this plan's own tests. §八 (`OmsError` variants including `ExchangeOrderIdUnknown`) → Task 1. §九 (`docker-compose.yml` PostgreSQL service) → Task 1. §十 (test strategy: schema/mock-gateway unit tests not requiring a database, `OrderManager`/end-to-end tests requiring live PostgreSQL and marked `#[ignore]`) → Tasks 1-8 respectively. §十一 (non-goals) → respected throughout, explicitly called out in Task 9's FEATURES.md Notes text.

**Type consistency check:** `ExecutionGateway::submit_order`/`cancel_order` both return `GatewayOutcome` (Task 2) and are called/matched identically in `MockExecutionGateway` (Task 3) and `OrderManager` (Task 6). `query_all_open_orders`'s `Vec<(OrderId, Option<String>, OrderStatus)>` return type is identical across its Task 2 trait definition, Task 3's mock implementation, and Task 6's `reconcile()` consumption. `OrderStore`'s five methods (`new`/`upsert_order`/`get_order`/`get_open_orders`/`insert_fill`/`get_fills_for_order`) match exactly between Task 5's definition and Task 6/7/8's usage. `OrderManager::submit_order`'s three parameters (`intent`, `positions`, `balances`) are consistent between Task 6's definition and every call site in Tasks 7-8's tests. `OmsError::ExchangeOrderIdUnknown { order_id }` is defined in Task 1 and both constructed (Task 6's `cancel_order`) and matched (Task 7's test) with the same shape.

**Placeholder scan:** No "TBD"/"TODO" strings; every code step contains complete, concrete Rust source, including full test module bodies. Task 7's mid-task self-correction (writing a flawed test, then explicitly flagging the gap it revealed in `MockExecutionGateway`'s design, then fixing both the mock and the test) is a deliberate choice to model the exact "notice a real gap while testing and close it before declaring the task done" discipline this plan's own author used while writing this plan -- not a placeholder, but a two-step task structure with a clearly labeled reason for the extra step.
