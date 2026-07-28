# Apexes Foundation Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Apexes Cargo workspace (`apexes-core` + `apexes-cli`) with the shared, multi-market core types (`Instrument`, `Order`, `Fill`, `MarketEvent`, `MarketDataProvider`, `ApexesError`) that every future sub-project depends on. No business logic, no network I/O, no gateway implementations.

**Architecture:** A two-crate Cargo workspace under `crates/`. `apexes-core` is a library crate containing only data types, one trait (`MarketDataProvider`), and one piece of real logic (the `OrderStatus` state machine). `apexes-cli` is a binary crate that depends on `apexes-core` and does nothing but parse `--version`/`--help` via `clap`, proving the workspace links correctly.

**Tech Stack:** Rust edition 2024, `rust_decimal` (exact price/qty arithmetic), `serde` (serialization), `thiserror` (error type), `chrono` (timestamps), `tokio` + `futures` + `async-trait` (async streaming trait), `uuid` (order/fill IDs), `clap` (CLI skeleton).

## Global Constraints

- Rust edition: `2024` for every crate (per project convention).
- All price/quantity fields use `rust_decimal::Decimal` — never `f64` or `f32`, anywhere in `apexes-core`.
- `MarketDataProvider` must be object-safe (usable as `Box<dyn MarketDataProvider>`) — implemented via `#[async_trait]`, not native `impl Trait` return position.
- `ApexesError` must derive `Clone` — error variants that carry underlying-error context store it as `Option<String>` (formatted), never `Box<dyn Error>`.
- No `unsafe` blocks anywhere in this plan.
- No `.unwrap()`/`.expect()` in non-test code — library code returns `Result`.
- Every commit uses `git commit -S -s` (signed + signed-off), per project convention.
- Crate version floors (from crates.io, checked 2026-07-28): `rust_decimal = "1.42"`, `serde = "1.0"`, `thiserror = "2.0"`, `chrono = "0.4"`, `tokio = "1.53"`, `futures = "0.3"`, `async-trait = "0.1.91"`, `uuid = "1.24"`, `clap = "4.6"`.
- Design spec of record: `docs/superpowers/specs/2026-07-28-apexes-foundation-skeleton-design.md`. Every type/field in this plan must match that spec exactly — if you find a mismatch while implementing, the spec wins; stop and flag it rather than improvising.

---

### Task 1: Workspace scaffold + `apexes-core` crate shell

**Files:**
- Create: `Cargo.toml` (workspace root)
- Create: `crates/apexes-core/Cargo.toml`
- Create: `crates/apexes-core/src/lib.rs`

**Interfaces:**
- Consumes: nothing (first task)
- Produces: a workspace that `cargo build --workspace` succeeds on with an empty `apexes-core` lib. Module declarations `pub mod error;`, `pub mod instrument;`, `pub mod order;`, `pub mod fill;`, `pub mod market_data;` will be added in later tasks — this task only creates `lib.rs` with a doc comment, no modules yet.

- [ ] **Step 1: Create the workspace root `Cargo.toml`**

Only `apexes-core` is listed as a member for now — `apexes-cli` doesn't exist on disk yet, and Cargo requires every listed member to exist. Task 8 adds `crates/apexes-cli` back into `members`.

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core"]

[workspace.package]
edition = "2024"
version = "0.1.0"
license = "MIT"

[workspace.dependencies]
rust_decimal = { version = "1.42", features = ["serde"] }
serde = { version = "1.0", features = ["derive"] }
thiserror = "2.0"
chrono = { version = "0.4", features = ["serde"] }
tokio = { version = "1.53", features = ["rt-multi-thread", "macros"] }
futures = "0.3"
async-trait = "0.1.91"
uuid = { version = "1.24", features = ["v4", "serde"] }
clap = { version = "4.6", features = ["derive"] }
serde_json = "1.0"
```

- [ ] **Step 2: Create `crates/apexes-core/Cargo.toml`**

```toml
[package]
name = "apexes-core"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
rust_decimal = { workspace = true }
serde = { workspace = true }
thiserror = { workspace = true }
chrono = { workspace = true }
tokio = { workspace = true }
futures = { workspace = true }
async-trait = { workspace = true }
uuid = { workspace = true }

[dev-dependencies]
serde_json = { workspace = true }
```

- [ ] **Step 3: Create `crates/apexes-core/src/lib.rs`**

```rust
//! Apexes core types: shared `Instrument`, `Order`, `Fill`, `MarketEvent`,
//! and `MarketDataProvider` used across backtest, paper, and live trading.
//!
//! This crate contains only data types and trait definitions — no gateway
//! implementations, no network I/O, no business logic beyond the
//! `OrderStatus` state machine.
```

- [ ] **Step 4: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: `Compiling apexes-core v0.1.0 (...)` then `Finished` — success, zero warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/apexes-core
git commit -S -s -m "chore: scaffold apexes-core crate in workspace"
```

---

### Task 2: `ApexesError`

**Files:**
- Create: `crates/apexes-core/src/error.rs`
- Modify: `crates/apexes-core/src/lib.rs`

**Interfaces:**
- Consumes: nothing new
- Produces: `pub enum ApexesError` with variants `InvalidInstrument { reason: String }`, `InvalidStateTransition { from: OrderStatus, to: OrderStatus }`, `ConnectionFailed { reason: String, source_detail: Option<String> }`, `SubscriptionFailed { symbol: String, venue: Venue, reason: String, source_detail: Option<String> }`. Note: the field is named `source_detail`, not `source` — `thiserror`'s derive macro special-cases any field literally named `source` (requiring it to implement `std::error::Error`), which conflicts with `Option<String>`; `source_detail` avoids that conflict while keeping `#[derive(thiserror::Error)]`. This task defines the enum shape referencing `OrderStatus`/`Venue` types that don't exist yet — Rust allows forward references across modules, but the crate won't compile until Task 3 (which adds `OrderStatus`) and Task 4 (which adds `Venue`) land. To keep every task independently buildable, this task uses placeholder generic bounds — see Step 1 for the actual resolution.

**Note on ordering:** `ApexesError::InvalidStateTransition` needs `OrderStatus`, and `SubscriptionFailed` needs `Venue`. To keep this task compilable on its own (bite-sized, independently testable), we define `ApexesError` in this task with only the two variants that don't depend on other modules (`InvalidInstrument`, `ConnectionFailed`), then extend it with `InvalidStateTransition` and `SubscriptionFailed` in Task 3 and Task 4 respectively once `OrderStatus` and `Venue` exist.

- [ ] **Step 1: Create `crates/apexes-core/src/error.rs` with the two market/instrument-independent variants**

```rust
use thiserror::Error;

/// Unified error type for apexes-core. Cloneable — variants that carry
/// underlying-error context store it as a formatted `String`, not
/// `Box<dyn Error>`, so `ApexesError` can be cloned freely.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ApexesError {
    #[error("invalid instrument: {reason}")]
    InvalidInstrument { reason: String },

    #[error("market data connection failed: {reason}")]
    ConnectionFailed {
        reason: String,
        source_detail: Option<String>,
    },
}
```

Note: the field is named `source_detail`, not `source` — `thiserror`'s derive macro treats any field literally named `source` as the error's `std::error::Error::source()` chain target, requiring it to implement `std::error::Error`. `Option<String>` doesn't satisfy that, which fails to compile (`E0599`). `source_detail` avoids the conflict while keeping the `#[derive(thiserror::Error)]` macro.

- [ ] **Step 2: Write a construction + Display test**

Append to `crates/apexes-core/src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_instrument_display() {
        let err = ApexesError::InvalidInstrument {
            reason: "venue does not belong to market_type".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "invalid instrument: venue does not belong to market_type"
        );
    }

    #[test]
    fn connection_failed_is_cloneable() {
        let err = ApexesError::ConnectionFailed {
            reason: "timeout".to_string(),
            source_detail: Some("io error: connection reset".to_string()),
        };
        let cloned = err.clone();
        assert_eq!(err, cloned);
    }
}
```

- [ ] **Step 3: Wire the module into `lib.rs`**

Modify `crates/apexes-core/src/lib.rs`:

```rust
//! Apexes core types: shared `Instrument`, `Order`, `Fill`, `MarketEvent`,
//! and `MarketDataProvider` used across backtest, paper, and live trading.
//!
//! This crate contains only data types and trait definitions — no gateway
//! implementations, no network I/O, no business logic beyond the
//! `OrderStatus` state machine.

pub mod error;

pub use error::ApexesError;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p apexes-core`
Expected: `test error::tests::invalid_instrument_display ... ok`, `test error::tests::connection_failed_is_cloneable ... ok`, 2 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-core/src/error.rs crates/apexes-core/src/lib.rs
git commit -S -s -m "feat(apexes-core): add ApexesError with InvalidInstrument and ConnectionFailed variants"
```

---

### Task 3: `Instrument`, `MarketType`, `Venue`, `AssetClass`

**Files:**
- Create: `crates/apexes-core/src/instrument.rs`
- Modify: `crates/apexes-core/src/lib.rs`
- Modify: `crates/apexes-core/src/error.rs` (no new variants needed here — `InvalidInstrument` already exists from Task 2)

**Interfaces:**
- Consumes: `ApexesError::InvalidInstrument { reason: String }` from Task 2
- Produces: `pub struct Instrument { symbol, market_type, venue, asset_class, base_currency, quote_currency, tick_size, lot_size, multiplier }`, `pub enum MarketType { AShare, HShare, UsEquity, Crypto }`, `pub enum Venue { Sse, Szse, Hkex, Nasdaq, Nyse, Binance, Okx, Coinbase }` with `impl Venue { pub fn market_type(&self) -> MarketType }`, `pub enum AssetClass { Spot, Perpetual, Future, Option, Equity }`, and `impl Instrument { pub fn new(...) -> Result<Self, ApexesError> }`. Later tasks (Order, Fill, MarketEvent) depend on `Venue` (not full `Instrument`) — see design spec §4.2/§4.3.

- [ ] **Step 1: Write failing tests first**

Create `crates/apexes-core/src/instrument.rs`:

```rust
use crate::error::ApexesError;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketType {
    AShare,
    HShare,
    UsEquity,
    Crypto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Venue {
    Sse,
    Szse,
    Hkex,
    Nasdaq,
    Nyse,
    Binance,
    Okx,
    Coinbase,
}

impl Venue {
    pub fn market_type(&self) -> MarketType {
        match self {
            Venue::Sse | Venue::Szse => MarketType::AShare,
            Venue::Hkex => MarketType::HShare,
            Venue::Nasdaq | Venue::Nyse => MarketType::UsEquity,
            Venue::Binance | Venue::Okx | Venue::Coinbase => MarketType::Crypto,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AssetClass {
    Spot,
    Perpetual,
    Future,
    Option,
    Equity,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Instrument {
    pub symbol: String,
    pub market_type: MarketType,
    pub venue: Venue,
    pub asset_class: AssetClass,
    pub base_currency: Option<String>,
    pub quote_currency: Option<String>,
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub multiplier: Decimal,
}

impl Instrument {
    /// Validates that `venue` belongs to `market_type` before constructing.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: impl Into<String>,
        market_type: MarketType,
        venue: Venue,
        asset_class: AssetClass,
        base_currency: Option<String>,
        quote_currency: Option<String>,
        tick_size: Decimal,
        lot_size: Decimal,
        multiplier: Decimal,
    ) -> Result<Self, ApexesError> {
        if venue.market_type() != market_type {
            return Err(ApexesError::InvalidInstrument {
                reason: format!(
                    "venue {venue:?} does not belong to market_type {market_type:?}"
                ),
            });
        }
        Ok(Self {
            symbol: symbol.into(),
            market_type,
            venue,
            asset_class,
            base_currency,
            quote_currency,
            tick_size,
            lot_size,
            multiplier,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn venue_market_type_mapping() {
        assert_eq!(Venue::Sse.market_type(), MarketType::AShare);
        assert_eq!(Venue::Szse.market_type(), MarketType::AShare);
        assert_eq!(Venue::Hkex.market_type(), MarketType::HShare);
        assert_eq!(Venue::Nasdaq.market_type(), MarketType::UsEquity);
        assert_eq!(Venue::Nyse.market_type(), MarketType::UsEquity);
        assert_eq!(Venue::Binance.market_type(), MarketType::Crypto);
        assert_eq!(Venue::Okx.market_type(), MarketType::Crypto);
        assert_eq!(Venue::Coinbase.market_type(), MarketType::Crypto);
    }

    #[test]
    fn new_succeeds_when_venue_matches_market_type() {
        let instrument = Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            Some("BTC".to_string()),
            Some("USDT".to_string()),
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        );
        assert!(instrument.is_ok());
        let instrument = instrument.unwrap();
        assert_eq!(instrument.symbol, "BTCUSDT");
        assert_eq!(instrument.venue, Venue::Binance);
    }

    #[test]
    fn new_fails_when_venue_does_not_match_market_type() {
        let result = Instrument::new(
            "600519",
            MarketType::AShare,
            Venue::Binance,
            AssetClass::Equity,
            None,
            Some("CNY".to_string()),
            dec!(0.01),
            dec!(100),
            dec!(1),
        );
        assert!(matches!(result, Err(ApexesError::InvalidInstrument { .. })));
    }

    #[test]
    fn instrument_serde_round_trip() {
        let instrument = Instrument::new(
            "0700",
            MarketType::HShare,
            Venue::Hkex,
            AssetClass::Equity,
            None,
            Some("HKD".to_string()),
            dec!(0.2),
            dec!(100),
            dec!(1),
        )
        .unwrap();
        let json = serde_json::to_string(&instrument).unwrap();
        let round_tripped: Instrument = serde_json::from_str(&json).unwrap();
        assert_eq!(instrument, round_tripped);
    }
}
```

- [ ] **Step 2: Add `rust_decimal_macros` as a dev-dependency**

Modify `crates/apexes-core/Cargo.toml`, add to `[dev-dependencies]`:

```toml
rust_decimal_macros = "1.40"
```

- [ ] **Step 3: Run tests to verify they fail (module not yet wired into lib.rs)**

Run: `cargo test -p apexes-core`
Expected: compile error — `instrument` module not declared in `lib.rs`.

- [ ] **Step 4: Wire the module into `lib.rs`**

Modify `crates/apexes-core/src/lib.rs`:

```rust
pub mod error;
pub mod instrument;

pub use error::ApexesError;
pub use instrument::{AssetClass, Instrument, MarketType, Venue};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p apexes-core`
Expected: 4 new tests pass (`venue_market_type_mapping`, `new_succeeds_when_venue_matches_market_type`, `new_fails_when_venue_does_not_match_market_type`, `instrument_serde_round_trip`), plus the 2 from Task 2 — 6 total passed.

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-core/src/instrument.rs crates/apexes-core/src/lib.rs crates/apexes-core/Cargo.toml
git commit -S -s -m "feat(apexes-core): add Instrument, MarketType, Venue, AssetClass"
```

---

### Task 4: `OrderStatus` state machine + `ApexesError::InvalidStateTransition`

**Files:**
- Modify: `crates/apexes-core/src/error.rs` (add `InvalidStateTransition` variant)
- Create: `crates/apexes-core/src/order.rs` (partial — `OrderStatus` only; full `Order` struct comes in Task 5)
- Modify: `crates/apexes-core/src/lib.rs`

**Interfaces:**
- Consumes: nothing new
- Produces: `pub enum OrderStatus { New, PartiallyFilled, Filled, Cancelled, Rejected }` with `impl OrderStatus { pub fn can_transition_to(&self, next: OrderStatus) -> bool; pub fn transition_to(&mut self, next: OrderStatus) -> Result<(), ApexesError> }`. `ApexesError` gains `InvalidStateTransition { from: OrderStatus, to: OrderStatus }`.

This is the one piece of real logic in this sub-project — full transition coverage is required per the design spec's test plan (§6).

- [ ] **Step 1: Add `InvalidStateTransition` to `ApexesError`**

Modify `crates/apexes-core/src/error.rs`. First add the import at the top:

```rust
use crate::order::OrderStatus;
use thiserror::Error;
```

Then add the variant to the enum:

```rust
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ApexesError {
    #[error("invalid instrument: {reason}")]
    InvalidInstrument { reason: String },

    #[error("invalid order state transition: {from:?} -> {to:?}")]
    InvalidStateTransition { from: OrderStatus, to: OrderStatus },

    #[error("market data connection failed: {reason}")]
    ConnectionFailed {
        reason: String,
        source_detail: Option<String>,
    },
}
```

- [ ] **Step 2: Write failing tests for the state machine**

Create `crates/apexes-core/src/order.rs`:

```rust
use crate::error::ApexesError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

impl OrderStatus {
    /// Returns true if `self -> next` is a valid transition.
    pub fn can_transition_to(&self, next: OrderStatus) -> bool {
        use OrderStatus::*;
        matches!(
            (self, next),
            (New, PartiallyFilled)
                | (New, Filled)
                | (New, Cancelled)
                | (New, Rejected)
                | (PartiallyFilled, PartiallyFilled)
                | (PartiallyFilled, Filled)
                | (PartiallyFilled, Cancelled)
        )
    }

    /// Attempts to transition `self` to `next`, mutating in place on success.
    pub fn transition_to(&mut self, next: OrderStatus) -> Result<(), ApexesError> {
        if self.can_transition_to(next) {
            *self = next;
            Ok(())
        } else {
            Err(ApexesError::InvalidStateTransition { from: *self, to: next })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_to_partially_filled_is_valid() {
        let mut status = OrderStatus::New;
        assert!(status.transition_to(OrderStatus::PartiallyFilled).is_ok());
        assert_eq!(status, OrderStatus::PartiallyFilled);
    }

    #[test]
    fn new_to_filled_is_valid() {
        let mut status = OrderStatus::New;
        assert!(status.transition_to(OrderStatus::Filled).is_ok());
        assert_eq!(status, OrderStatus::Filled);
    }

    #[test]
    fn new_to_cancelled_is_valid() {
        let mut status = OrderStatus::New;
        assert!(status.transition_to(OrderStatus::Cancelled).is_ok());
        assert_eq!(status, OrderStatus::Cancelled);
    }

    #[test]
    fn new_to_rejected_is_valid() {
        let mut status = OrderStatus::New;
        assert!(status.transition_to(OrderStatus::Rejected).is_ok());
        assert_eq!(status, OrderStatus::Rejected);
    }

    #[test]
    fn partially_filled_to_partially_filled_is_valid() {
        let mut status = OrderStatus::PartiallyFilled;
        assert!(status.transition_to(OrderStatus::PartiallyFilled).is_ok());
        assert_eq!(status, OrderStatus::PartiallyFilled);
    }

    #[test]
    fn partially_filled_to_filled_is_valid() {
        let mut status = OrderStatus::PartiallyFilled;
        assert!(status.transition_to(OrderStatus::Filled).is_ok());
        assert_eq!(status, OrderStatus::Filled);
    }

    #[test]
    fn partially_filled_to_cancelled_is_valid() {
        let mut status = OrderStatus::PartiallyFilled;
        assert!(status.transition_to(OrderStatus::Cancelled).is_ok());
        assert_eq!(status, OrderStatus::Cancelled);
    }

    #[test]
    fn filled_to_partially_filled_is_invalid() {
        let mut status = OrderStatus::Filled;
        let result = status.transition_to(OrderStatus::PartiallyFilled);
        assert!(matches!(
            result,
            Err(ApexesError::InvalidStateTransition {
                from: OrderStatus::Filled,
                to: OrderStatus::PartiallyFilled
            })
        ));
        // status is unchanged on failure
        assert_eq!(status, OrderStatus::Filled);
    }

    #[test]
    fn rejected_to_new_is_invalid() {
        let mut status = OrderStatus::Rejected;
        let result = status.transition_to(OrderStatus::New);
        assert!(result.is_err());
        assert_eq!(status, OrderStatus::Rejected);
    }

    #[test]
    fn partially_filled_to_rejected_is_invalid() {
        let mut status = OrderStatus::PartiallyFilled;
        let result = status.transition_to(OrderStatus::Rejected);
        assert!(result.is_err());
    }

    #[test]
    fn cancelled_to_filled_is_invalid() {
        let mut status = OrderStatus::Cancelled;
        let result = status.transition_to(OrderStatus::Filled);
        assert!(result.is_err());
    }

    #[test]
    fn new_to_new_is_invalid() {
        // New has no self-loop (unlike PartiallyFilled)
        let mut status = OrderStatus::New;
        let result = status.transition_to(OrderStatus::New);
        assert!(result.is_err());
    }
}
```

- [ ] **Step 3: Wire the module into `lib.rs`**

Modify `crates/apexes-core/src/lib.rs`:

```rust
pub mod error;
pub mod instrument;
pub mod order;

pub use error::ApexesError;
pub use instrument::{AssetClass, Instrument, MarketType, Venue};
pub use order::OrderStatus;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p apexes-core`
Expected: all 12 new `order::tests::*` pass, plus the 6 from Tasks 2-3 — 18 total passed.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-core/src/error.rs crates/apexes-core/src/order.rs crates/apexes-core/src/lib.rs
git commit -S -s -m "feat(apexes-core): add OrderStatus state machine with validated transitions"
```

---

### Task 5: `Order` and `Fill` structs

**Files:**
- Modify: `crates/apexes-core/src/order.rs` (add `Order`, `OrderId`, `Side`, `OrderType`)
- Create: `crates/apexes-core/src/fill.rs` (`Fill`, `FillId`)
- Modify: `crates/apexes-core/src/lib.rs`

**Interfaces:**
- Consumes: `OrderStatus` (Task 4), `Venue` (Task 3)
- Produces: `pub struct OrderId(pub Uuid)`, `pub struct Order { id, exchange_order_id, symbol, venue, side, order_type, quantity, price, filled_quantity, status, created_at, updated_at }`, `pub enum Side { Buy, Sell }`, `pub enum OrderType { Market, Limit }`, `pub struct FillId(pub Uuid)`, `pub struct Fill { id, order_id, symbol, venue, side, price, quantity, fee, fee_currency, filled_at }`.

- [ ] **Step 1: Write failing tests for `Order`**

Append to `crates/apexes-core/src/order.rs` (above the existing `#[cfg(test)] mod tests` block — insert before it):

```rust
use crate::instrument::Venue;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub venue: Venue,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub filled_quantity: Decimal,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

Add these tests inside the existing `#[cfg(test)] mod tests` block (after the last existing test):

```rust
    #[test]
    fn order_serde_round_trip() {
        let now = Utc::now();
        let order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: rust_decimal_macros::dec!(1.5),
            price: Some(rust_decimal_macros::dec!(65000)),
            filled_quantity: rust_decimal_macros::dec!(0),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&order).unwrap();
        let round_tripped: Order = serde_json::from_str(&json).unwrap();
        assert_eq!(order, round_tripped);
    }

    #[test]
    fn market_order_has_no_price() {
        let now = Utc::now();
        let order = Order {
            id: OrderId(Uuid::new_v4()),
            exchange_order_id: None,
            symbol: "AAPL".to_string(),
            venue: Venue::Nasdaq,
            side: Side::Sell,
            order_type: OrderType::Market,
            quantity: rust_decimal_macros::dec!(10),
            price: None,
            filled_quantity: rust_decimal_macros::dec!(0),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        };
        assert_eq!(order.price, None);
        assert_eq!(order.order_type, OrderType::Market);
    }
```

No `Cargo.toml` change is needed for this step: `rust_decimal_macros` was already added as a dev-dependency in Task 3 Step 2, and the test module's existing `use super::*;` picks up `Order`, `OrderId`, `Side`, `OrderType`, `Uuid`, `Venue` automatically from the top-level imports added above.

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p apexes-core`
Expected: PASS — the `order` module was already wired into `lib.rs` in Task 4, so this compiles immediately with the two new tests (`order_serde_round_trip`, `market_order_has_no_price`) passing alongside all previous tests.

- [ ] **Step 3: Write failing tests for `Fill`**

Create `crates/apexes-core/src/fill.rs`:

```rust
use crate::instrument::Venue;
use crate::order::{OrderId, Side};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FillId(pub Uuid);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub id: FillId,
    pub order_id: OrderId,
    pub symbol: String,
    pub venue: Venue,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_currency: Option<String>,
    pub filled_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn fill_serde_round_trip() {
        let fill = Fill {
            id: FillId(Uuid::new_v4()),
            order_id: OrderId(Uuid::new_v4()),
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            side: Side::Buy,
            price: dec!(65000),
            quantity: dec!(0.5),
            fee: dec!(3.25),
            fee_currency: Some("USDT".to_string()),
            filled_at: Utc::now(),
        };
        let json = serde_json::to_string(&fill).unwrap();
        let round_tripped: Fill = serde_json::from_str(&json).unwrap();
        assert_eq!(fill, round_tripped);
    }
}
```

- [ ] **Step 4: Wire the `fill` module into `lib.rs`**

Modify `crates/apexes-core/src/lib.rs`:

```rust
pub mod error;
pub mod fill;
pub mod instrument;
pub mod order;

pub use error::ApexesError;
pub use fill::{Fill, FillId};
pub use instrument::{AssetClass, Instrument, MarketType, Venue};
pub use order::{Order, OrderId, OrderStatus, OrderType, Side};
```

- [ ] **Step 5: Run all tests**

Run: `cargo test -p apexes-core`
Expected: 21 total passed (18 from before + `order_serde_round_trip`, `market_order_has_no_price`, `fill_serde_round_trip`).

- [ ] **Step 6: Commit**

```bash
git add crates/apexes-core/src/order.rs crates/apexes-core/src/fill.rs crates/apexes-core/src/lib.rs
git commit -S -s -m "feat(apexes-core): add Order and Fill types"
```

---

### Task 6: `MarketEvent` model (`Tick`, `DepthUpdate`, `Bar`, `TradeEvent`)

**Files:**
- Create: `crates/apexes-core/src/market_data.rs` (this task: event types only; `MarketDataProvider` trait comes in Task 7)
- Modify: `crates/apexes-core/src/lib.rs`

**Interfaces:**
- Consumes: `Venue` (Task 3), `Side` (Task 5)
- Produces: `pub enum MarketEvent { Tick(Tick), Depth(DepthUpdate), Bar(Bar), Trade(TradeEvent) }`, `pub struct Tick { symbol, venue, bid_price, bid_size, ask_price, ask_size, timestamp }`, `pub struct DepthUpdate { symbol, venue, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>, timestamp }`, `pub struct PriceLevel { price, quantity }`, `pub struct Bar { symbol, venue, open, high, low, close, volume, interval, open_time, close_time }`, `pub enum BarInterval { OneSecond, OneMinute, FiveMinutes, FifteenMinutes, OneHour, OneDay }`, `pub struct TradeEvent { symbol, venue, price, quantity, side: Option<Side>, trade_id, timestamp }`.

- [ ] **Step 1: Write the types and failing round-trip tests**

Create `crates/apexes-core/src/market_data.rs`:

```rust
use crate::instrument::Venue;
use crate::order::Side;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketEvent {
    Tick(Tick),
    Depth(DepthUpdate),
    Bar(Bar),
    Trade(TradeEvent),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tick {
    pub symbol: String,
    pub venue: Venue,
    pub bid_price: Decimal,
    pub bid_size: Decimal,
    pub ask_price: Decimal,
    pub ask_size: Decimal,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DepthUpdate {
    pub symbol: String,
    pub venue: Venue,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub sequence: Option<u64>,
    pub is_snapshot: bool,
    pub timestamp: DateTime<Utc>,
}

// NOTE: `sequence`/`is_snapshot` were added to `DepthUpdate` after Task 6's
// original review, per the final whole-branch review's I-1 finding — real
// incremental depth feeds need a sequence number to apply updates in order
// and detect gaps. See design spec §4.3 for the full rationale.

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bar {
    pub symbol: String,
    pub venue: Venue,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub interval: BarInterval,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BarInterval {
    OneSecond,
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
    OneDay,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeEvent {
    pub symbol: String,
    pub venue: Venue,
    pub price: Decimal,
    pub quantity: Decimal,
    pub side: Option<Side>,
    pub trade_id: Option<String>,
    pub timestamp: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn tick_event_serde_round_trip() {
        let event = MarketEvent::Tick(Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(64999.5),
            bid_size: dec!(0.8),
            ask_price: dec!(65000.5),
            ask_size: dec!(1.2),
            timestamp: Utc::now(),
        });
        let json = serde_json::to_string(&event).unwrap();
        let round_tripped: MarketEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, round_tripped);
    }

    #[test]
    fn depth_event_serde_round_trip() {
        let event = MarketEvent::Depth(DepthUpdate {
            symbol: "AAPL".to_string(),
            venue: Venue::Nasdaq,
            bids: vec![
                PriceLevel { price: dec!(190.50), quantity: dec!(100) },
                PriceLevel { price: dec!(190.45), quantity: dec!(200) },
            ],
            asks: vec![PriceLevel { price: dec!(190.55), quantity: dec!(150) }],
            timestamp: Utc::now(),
        });
        let json = serde_json::to_string(&event).unwrap();
        let round_tripped: MarketEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, round_tripped);
    }

    #[test]
    fn bar_event_serde_round_trip() {
        let now = Utc::now();
        let event = MarketEvent::Bar(Bar {
            symbol: "0700".to_string(),
            venue: Venue::Hkex,
            open: dec!(320.0),
            high: dec!(325.0),
            low: dec!(318.0),
            close: dec!(322.5),
            volume: dec!(1_500_000),
            interval: BarInterval::OneDay,
            open_time: now,
            close_time: now,
        });
        let json = serde_json::to_string(&event).unwrap();
        let round_tripped: MarketEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, round_tripped);
    }

    #[test]
    fn trade_event_serde_round_trip() {
        let event = MarketEvent::Trade(TradeEvent {
            symbol: "600519".to_string(),
            venue: Venue::Sse,
            price: dec!(1680.0),
            quantity: dec!(100),
            side: Some(Side::Sell),
            trade_id: Some("T123456".to_string()),
            timestamp: Utc::now(),
        });
        let json = serde_json::to_string(&event).unwrap();
        let round_tripped: MarketEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, round_tripped);
    }

    #[test]
    fn trade_event_side_is_optional() {
        let event = TradeEvent {
            symbol: "ETHUSDT".to_string(),
            venue: Venue::Okx,
            price: dec!(3200.0),
            quantity: dec!(2.0),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        };
        assert_eq!(event.side, None);
    }
}
```

- [ ] **Step 2: Wire the module into `lib.rs`**

Modify `crates/apexes-core/src/lib.rs`:

```rust
pub mod error;
pub mod fill;
pub mod instrument;
pub mod market_data;
pub mod order;

pub use error::ApexesError;
pub use fill::{Fill, FillId};
pub use instrument::{AssetClass, Instrument, MarketType, Venue};
pub use market_data::{Bar, BarInterval, DepthUpdate, MarketEvent, PriceLevel, Tick, TradeEvent};
pub use order::{Order, OrderId, OrderStatus, OrderType, Side};
```

- [ ] **Step 3: Run all tests**

Run: `cargo test -p apexes-core`
Expected: 26 total passed (21 from before + 5 new market_data tests).

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-core/src/market_data.rs crates/apexes-core/src/lib.rs
git commit -S -s -m "feat(apexes-core): add MarketEvent model (Tick, Depth, Bar, Trade)"
```

---

### Task 7: `MarketDataProvider` trait + remaining `ApexesError` variant

**Files:**
- Modify: `crates/apexes-core/src/error.rs` (add `SubscriptionFailed` variant)
- Modify: `crates/apexes-core/src/market_data.rs` (add `MarketDataProvider` trait)
- Modify: `crates/apexes-core/Cargo.toml` (add `async-trait` to `[dependencies]` — already listed in workspace deps from Task 1, just needs adding to this crate's `[dependencies]` table, which was already done in Task 1 Step 2. No change needed.)

**Interfaces:**
- Consumes: `Instrument` (Task 3), `MarketEvent` (Task 6), `ApexesError` (Task 2/4)
- Produces: `pub trait MarketDataProvider: Send + Sync { async fn subscribe(&self, instruments: &[Instrument]) -> Result<BoxStream<'static, MarketEvent>, ApexesError>; }` (via `#[async_trait]`). `ApexesError` gains `SubscriptionFailed { symbol: String, venue: Venue, reason: String, source_detail: Option<String> }`.

- [ ] **Step 1: Add `SubscriptionFailed` to `ApexesError`**

Modify `crates/apexes-core/src/error.rs`. Update the imports:

```rust
use crate::instrument::Venue;
use crate::order::OrderStatus;
use thiserror::Error;
```

Add the variant to the enum (after `ConnectionFailed`):

```rust
    #[error("subscription failed for {symbol} on {venue:?}: {reason}")]
    SubscriptionFailed {
        symbol: String,
        venue: Venue,
        reason: String,
        source_detail: Option<String>,
    },
```

- [ ] **Step 2: Add a test for `SubscriptionFailed`**

Append to the `#[cfg(test)] mod tests` block in `crates/apexes-core/src/error.rs`:

```rust
    #[test]
    fn subscription_failed_display() {
        let err = ApexesError::SubscriptionFailed {
            symbol: "BTCUSDT".to_string(),
            venue: crate::instrument::Venue::Binance,
            reason: "rate limited".to_string(),
            source_detail: None,
        };
        assert_eq!(
            err.to_string(),
            "subscription failed for BTCUSDT on Binance: rate limited"
        );
    }
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p apexes-core`
Expected: 27 total passed (26 from before + `subscription_failed_display`).

- [ ] **Step 4: Add the `MarketDataProvider` trait (no unit test — this is a trait with no implementers in this sub-project; correctness is verified by compilation)**

Append to `crates/apexes-core/src/market_data.rs`, after the existing type definitions but before the `#[cfg(test)]` block:

```rust
use crate::error::ApexesError;
use crate::instrument::Instrument;
use async_trait::async_trait;
use futures::stream::BoxStream;

/// Streams normalized [`MarketEvent`]s for a set of [`Instrument`]s.
///
/// Implementations are expected to be registered behind `Box<dyn
/// MarketDataProvider>` so data sources can be swapped via configuration
/// (free vs. paid feeds) without touching strategy or risk code.
#[async_trait]
pub trait MarketDataProvider: Send + Sync {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError>;
}
```

- [ ] **Step 5: Verify the trait is object-safe by compiling a smoke-test helper**

Append to the `#[cfg(test)] mod tests` block in `crates/apexes-core/src/market_data.rs`:

```rust
    #[test]
    fn market_data_provider_is_object_safe() {
        // This function only needs to compile — it proves MarketDataProvider
        // can be used as a trait object, which #[async_trait] enables.
        fn _assert_object_safe(_providers: Vec<Box<dyn MarketDataProvider>>) {}
    }
```

- [ ] **Step 6: Run all tests**

Run: `cargo test -p apexes-core`
Expected: 28 total passed (27 from before + `market_data_provider_is_object_safe`).

- [ ] **Step 7: Export `MarketDataProvider` from `lib.rs`**

Modify `crates/apexes-core/src/lib.rs`:

```rust
pub use market_data::{
    Bar, BarInterval, DepthUpdate, MarketDataProvider, MarketEvent, PriceLevel, Tick, TradeEvent,
};
```

- [ ] **Step 8: Final full-crate test run**

Run: `cargo test -p apexes-core`
Expected: 28 passed, 0 failed.

- [ ] **Step 9: Commit**

```bash
git add crates/apexes-core/src/error.rs crates/apexes-core/src/market_data.rs crates/apexes-core/src/lib.rs
git commit -S -s -m "feat(apexes-core): add MarketDataProvider trait and SubscriptionFailed error"
```

---

### Task 8: `apexes-cli` skeleton

**Files:**
- Create: `crates/apexes-cli/Cargo.toml`
- Create: `crates/apexes-cli/src/main.rs`
- Modify: `Cargo.toml` (workspace root — restore `apexes-cli` as a member)

**Interfaces:**
- Consumes: `apexes-core` (path dependency, unused directly by any code yet — this task proves the link works)
- Produces: a `apexes` binary that supports `--version` and `--help`.

- [ ] **Step 1: Restore the workspace member list**

Modify `Cargo.toml` (workspace root):

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli"]
```

- [ ] **Step 2: Create `crates/apexes-cli/Cargo.toml`**

```toml
[package]
name = "apexes-cli"
edition.workspace = true
version.workspace = true
license.workspace = true

[[bin]]
name = "apexes"
path = "src/main.rs"

[dependencies]
apexes-core = { path = "../apexes-core" }
clap = { workspace = true }
```

- [ ] **Step 3: Create `crates/apexes-cli/src/main.rs`**

```rust
//! Apexes CLI entry point. This foundation-phase skeleton only proves
//! apexes-cli links against apexes-core and parses --version/--help;
//! real subcommands are added as subsystems land.

use clap::Parser;

#[derive(Parser)]
#[command(name = "apexes", version, about = "Apexes multi-market trading system")]
struct Cli;

fn main() {
    // apexes-core is depended upon (not yet used) to prove the workspace
    // links correctly end-to-end; referencing a type here keeps the
    // dependency from being flagged as unused once real subcommands land.
    let _ = std::any::type_name::<apexes_core::Instrument>();

    Cli::parse();
}
```

- [ ] **Step 4: Build and manually verify `--version`/`--help`**

Run: `cargo build --workspace`
Expected: `Compiling apexes-cli v0.1.0 (...)` then `Finished` — zero warnings.

Run: `cargo run -p apexes-cli -- --version`
Expected: prints `apexes 0.1.0`.

Run: `cargo run -p apexes-cli -- --help`
Expected: prints usage text including `Apexes multi-market trading system`.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/apexes-cli
git commit -S -s -m "feat(apexes-cli): add CLI skeleton with --version/--help"
```

---

### Task 9: Workspace-wide verification (clippy, fmt, full test suite)

**Files:**
- None created/modified — this task only runs verification commands and fixes any issues surfaced.

**Interfaces:**
- Consumes: the entire workspace built in Tasks 1-8.
- Produces: a workspace passing all Definition-of-Done checks from the design spec §7.

- [ ] **Step 1: Run the full test suite**

Run: `cargo test --workspace`
Expected: 28 tests passed in `apexes-core`, 0 in `apexes-cli` (no tests written for the CLI skeleton — it has no logic to unit test), 0 failed.

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --workspace -- -D warnings`
Expected: no warnings, exit code 0. If clippy flags anything (e.g. `too_many_arguments` on `Instrument::new` — already suppressed via `#[allow(clippy::too_many_arguments)]` in Task 3), fix inline and re-run until clean.

- [ ] **Step 3: Run fmt check**

Run: `cargo fmt --check`
Expected: no output, exit code 0. If formatting differs, run `cargo fmt` (without `--check`) to auto-fix, then re-run `cargo fmt --check` to confirm.

- [ ] **Step 4: Run a final full build**

Run: `cargo build --workspace`
Expected: `Finished` with zero warnings.

- [ ] **Step 5: If any fixes were needed in Steps 2-3, commit them**

```bash
git add -A
git commit -S -s -m "chore: fix clippy warnings and formatting"
```

If no fixes were needed, skip this commit — nothing to commit.

- [ ] **Step 6: Update `docs/FEATURES.md` status for foundation-phase rows**

Modify `docs/FEATURES.md`: change status for rows `1.5`, `4.3`, `5.1.2`, `0.1`–`0.7` from `📝 Designing` to `✅ Done` (these are exactly the rows tagged sub-project `#0` in the "Foundation" section and the two cross-referenced rows `1.5` and `4.3`/`5.1.2` that this plan fully delivers the type-level portion of — leave their Notes text as-is, only change the status emoji/word). Also update the summary counts table at the bottom: move 10 rows from "📝 Designing" to "✅ Done" (Designing: 10 → 0, Done: 0 → 10; Not Started stays 59; Deferred stays 1; Total stays 70).

Run: `grep -c '✅ Done' docs/FEATURES.md` after editing — expect `12` (2 pre-existing occurrences: the legend row at line 15 and the summary-table row at line 164, both already contain the literal text "✅ Done" before this edit — plus the 10 newly-updated feature rows).

- [ ] **Step 7: Commit the FEATURES.md update**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: mark foundation skeleton features as done"
```

---

## Plan Self-Review Notes

**Spec coverage:** Every section of `docs/superpowers/specs/2026-07-28-apexes-foundation-skeleton-design.md` maps to a task — §3 (workspace/crate layout) → Task 1 & 8; §4.1 (Instrument) → Task 3; §4.2 (Order/Fill/OrderStatus) → Tasks 4-5; §4.3 (MarketEvent/MarketDataProvider) → Tasks 6-7; §4.4 (ApexesError) → Tasks 2, 4, 7 (split across tasks since variants depend on types introduced incrementally); §5 (apexes-cli) → Task 8; §6 (test plan) → covered throughout Tasks 2-7, all listed transition/round-trip cases present; §7 (Definition of Done) → Task 9.

**Placeholder scan:** No TBD/TODO/"implement later" strings in any step. Every code block is complete, compilable Rust.

**Type consistency:** `OrderStatus`, `Venue`, `Side`, `Instrument`, `MarketEvent`, `ApexesError` are used with identical field/variant names across all tasks that reference them — verified against the design spec's exact signatures in each task's "Interfaces: Produces" line.
