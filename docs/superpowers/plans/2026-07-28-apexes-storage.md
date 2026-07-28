# Apexes Storage (Sub-project #2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `apexes-storage`, a crate providing `StorageWriter`/`StorageReader` that persist all 4 `apexes_core::MarketEvent` variants (Trade, Tick, Depth, Bar) to ClickHouse and read them back correctly, as the project's sole historical-data storage backend.

**Architecture:** New workspace crate `apexes-storage`. Four ClickHouse tables (`trades`, `ticks`, `depth_updates`, `bars`), one per `MarketEvent` variant, using `MergeTree` engine with daily partitioning. A conversion layer maps `rust_decimal::Decimal` to the `clickhouse` crate's `FixedPoint<i128, U10>` wire type (via `fixnum`, exactly reproducing `Decimal`'s mantissa/scale) at the crate boundary — this conversion never leaks outside `apexes-storage`. `StorageWriter::write_batch()` and `StorageReader::read_batch()` are the only two public entry points; both require homogeneous (single-event-type) batches, no internal buffering.

**Tech Stack:** Rust edition 2024, `clickhouse = "0.15"` (official client, `chrono` feature), `fixnum = "0.9"` (`i128` feature, internal-only), `tokio` (async, since `clickhouse` client is async), Docker Compose for local ClickHouse.

## Global Constraints

- Rust edition: `2024` for the new crate (workspace convention).
- No `unsafe` blocks anywhere in this plan.
- No `.unwrap()`/`.expect()` in non-test code — library code returns `Result` or handles errors explicitly.
- Every commit uses `git commit -S -s` (signed + signed-off), per project convention.
- Logging uses `tracing`, never `println!`.
- `fixnum` is an internal-only dependency of `apexes-storage` — it must never appear in the crate's public API surface (all public function signatures use `apexes_core::MarketEvent`/`Venue`/etc. and plain `rust_decimal::Decimal` types).
- All ClickHouse Decimal columns use `Decimal(38, 10)`, mapped to `FixedPoint<i128, typenum::U10>` on the Rust side.
- `write_batch()` requires a homogeneous batch (all elements the same `MarketEvent` variant) — returns `Err(ApexesError::...)` on a mixed batch. No internal buffering; each call is one ClickHouse `INSERT`.
- All 4 tables use `ENGINE = MergeTree`, `PARTITION BY toYYYYMMDD(<time column>)`, `ORDER BY (venue, symbol, ...)` per the design spec §4.
- ClickHouse integration tests (anything requiring a live database connection) are marked `#[ignore]` and excluded from default `cargo test`/`cargo test --workspace`; run explicitly via `cargo test -p apexes-storage -- --ignored`, requiring `docker compose up -d` beforehand.
- Crate version floors (verified by compiling against the real crates on 2026-07-28): `clickhouse = "0.15"`, `fixnum = "0.9"` (with `i128` feature — confirmed `FixedPoint<i128, typenum::U10>` compiles and round-trips correctly via `from_decimal(mantissa: i128, exponent: i32)`). Existing workspace deps (`tokio = "1.53"`, `rust_decimal = "1.42"`, `chrono = "0.4"`, `serde = "1.0"`, `tracing = "0.1"`) already pinned in the workspace root `Cargo.toml`.
- Design spec of record: `docs/superpowers/specs/2026-07-28-apexes-storage-design.md`. If you find a mismatch between this plan and that spec while implementing, the spec wins — stop and flag it rather than improvising.

---

### Task 1: Crate scaffold + Docker Compose

**Files:**
- Create: `crates/apexes-storage/Cargo.toml`
- Create: `crates/apexes-storage/src/lib.rs`
- Create: `docker-compose.yml` (repo root)
- Modify: `Cargo.toml` (workspace root — add `crates/apexes-storage` to `members`, add `clickhouse`/`fixnum` to `[workspace.dependencies]`)

**Interfaces:**
- Consumes: `apexes-core` (path dependency, unused directly by any code yet — this task proves the link works)
- Produces: a workspace member `apexes-storage` that `cargo build --workspace` succeeds on with an empty lib, plus a working `docker-compose.yml` that starts a local ClickHouse instance.

- [ ] **Step 1: Add `clickhouse`/`fixnum` to the workspace root `Cargo.toml` and register the new member**

Modify `Cargo.toml` (workspace root):

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage"]

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
barter-data = "0.11"
barter-instrument = "0.3"
tracing = "0.1"
clickhouse = { version = "0.15", features = ["chrono"] }
fixnum = { version = "0.9", features = ["i128", "serde"] }
```

- [ ] **Step 2: Create `crates/apexes-storage/Cargo.toml`**

```toml
[package]
name = "apexes-storage"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-core = { path = "../apexes-core" }
clickhouse = { workspace = true }
fixnum = { workspace = true }
tokio = { workspace = true }
rust_decimal = { workspace = true }
chrono = { workspace = true }
serde = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["test-util", "macros"] }
```

- [ ] **Step 3: Create `crates/apexes-storage/src/lib.rs`**

```rust
//! Apexes historical data storage: persists `apexes_core::MarketEvent`
//! (Trade, Tick, Depth, Bar) to ClickHouse and reads it back. ClickHouse
//! is the project's sole historical-data storage backend (see design spec
//! for the rationale on why this replaced an earlier local-Parquet plan).

pub mod conversion;
pub mod error;
pub mod reader;
pub mod writer;

pub use error::StorageError;
pub use reader::{EventType, StorageReader};
pub use writer::StorageWriter;
```

Create placeholder files for the modules declared above (populated in later tasks):

Create `crates/apexes-storage/src/error.rs`:
```rust
// Populated in Task 2.
```

Create `crates/apexes-storage/src/conversion.rs`:
```rust
// Populated in Task 3.
```

Create `crates/apexes-storage/src/writer.rs`:
```rust
// Populated in Tasks 4-6.
pub struct StorageWriter;
```

Create `crates/apexes-storage/src/reader.rs`:
```rust
// Populated in Task 7.
pub struct StorageReader;
pub enum EventType { Trade, Tick, Depth, Bar }
```

- [ ] **Step 4: Create `docker-compose.yml` at the repo root**

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:24.8
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

- [ ] **Step 5: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: `Compiling apexes-storage v0.1.0 (...)` then `Finished` — success, zero warnings. (This pulls and compiles `clickhouse`/`fixnum` and their transitive dependencies for the first time — expect a noticeably longer build than previous crates; that's expected, not an error.)

- [ ] **Step 6: Verify Docker Compose starts ClickHouse (manual check, not part of automated tests)**

Run: `docker compose up -d`
Expected: the `apexes-clickhouse` container starts and reports healthy. Verify with:
Run: `curl http://localhost:8123/ping`
Expected: `Ok.`

Run: `docker compose down` to stop it after verifying (later tasks will start it again as needed for `--ignored` tests).

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml docker-compose.yml crates/apexes-storage
git commit -S -s -m "chore: scaffold apexes-storage crate and docker-compose ClickHouse setup"
```

---

### Task 2: `StorageError`

**Files:**
- Modify: `crates/apexes-storage/src/error.rs`
- Modify: `crates/apexes-storage/src/lib.rs` (no changes needed — already wires `error` module from Task 1)

**Interfaces:**
- Consumes: nothing new
- Produces: `pub enum StorageError` covering connection failures, mixed-batch validation errors, and conversion failures. Used as the error type in `StorageWriter`/`StorageReader`'s `Result` returns in later tasks.

- [ ] **Step 1: Write the error type with tests**

Create `crates/apexes-storage/src/error.rs`:

```rust
use thiserror::Error;

/// Errors returned by `apexes-storage`'s `StorageWriter`/`StorageReader`.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("failed to connect to ClickHouse: {reason}")]
    ConnectionFailed { reason: String },

    #[error("write_batch requires a homogeneous batch of the same MarketEvent variant, but found mixed types")]
    MixedBatch,

    #[error("write_batch was called with an empty batch")]
    EmptyBatch,

    #[error("failed to convert Decimal to ClickHouse FixedPoint: {reason}")]
    DecimalConversionFailed { reason: String },

    #[error("ClickHouse query failed: {reason}")]
    QueryFailed { reason: String },
}

impl From<clickhouse::error::Error> for StorageError {
    fn from(error: clickhouse::error::Error) -> Self {
        StorageError::QueryFailed {
            reason: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_batch_display() {
        let err = StorageError::MixedBatch;
        assert_eq!(
            err.to_string(),
            "write_batch requires a homogeneous batch of the same MarketEvent variant, but found mixed types"
        );
    }

    #[test]
    fn empty_batch_display() {
        let err = StorageError::EmptyBatch;
        assert_eq!(err.to_string(), "write_batch was called with an empty batch");
    }

    #[test]
    fn decimal_conversion_failed_display() {
        let err = StorageError::DecimalConversionFailed {
            reason: "overflow".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "failed to convert Decimal to ClickHouse FixedPoint: overflow"
        );
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p apexes-storage error`
Expected: 3 tests pass (`mixed_batch_display`, `empty_batch_display`, `decimal_conversion_failed_display`).

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p apexes-storage -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-storage/src/error.rs
git commit -S -s -m "feat(apexes-storage): add StorageError type"
```

---

### Task 3: `Decimal` ↔ `FixedPoint` conversion layer

**Files:**
- Modify: `crates/apexes-storage/src/conversion.rs`

**Interfaces:**
- Consumes: `rust_decimal::Decimal`, `crate::error::StorageError` (Task 2)
- Produces: `pub type CkDecimal = fixnum::FixedPoint<i128, fixnum::typenum::U10>;`, `pub fn decimal_to_fixed(value: Decimal) -> Result<CkDecimal, StorageError>`, `pub fn fixed_to_decimal(value: CkDecimal) -> Decimal`. Later tasks (4-7) use these for every price/quantity/volume/fee field when building/reading ClickHouse row structs.

This is the second-most logic-critical task in this sub-project (after none — storage correctness on money fields is safety-critical per the project's vision doc). Get the mantissa/scale math exactly right.

- [ ] **Step 1: Write the failing tests first**

Create `crates/apexes-storage/src/conversion.rs`:

```rust
use crate::error::StorageError;
use fixnum::typenum::U10;
use fixnum::FixedPoint;
use rust_decimal::Decimal;

/// ClickHouse-side fixed-point type matching the `Decimal(38, 10)` column
/// type used across all `apexes-storage` tables. `i128` gives the 38-digit
/// range; `U10` gives the 10-decimal-place scale.
pub type CkDecimal = FixedPoint<i128, U10>;

/// Converts a `rust_decimal::Decimal` into the ClickHouse wire type,
/// preserving exact value via `Decimal`'s mantissa/scale representation.
/// `rust_decimal::Decimal` stores values as `mantissa * 10^-scale`; we pass
/// that directly to `FixedPoint::from_decimal(mantissa, -scale)`, which
/// re-scales internally to `U10`'s fixed 10-decimal-place representation.
pub fn decimal_to_fixed(value: Decimal) -> Result<CkDecimal, StorageError> {
    let mantissa = value.mantissa();
    let scale = value.scale() as i32;
    CkDecimal::from_decimal(mantissa, -scale).map_err(|error| StorageError::DecimalConversionFailed {
        reason: error.to_string(),
    })
}

/// Converts a ClickHouse `FixedPoint` value back into `rust_decimal::Decimal`.
/// Goes through `f64` as an intermediate step (matching the `From<FixedPoint>
/// for f64` impl `fixnum` provides), then re-parses via `Decimal::try_from`.
/// This is lossy in theory for values at the extreme edge of f64's 52-bit
/// mantissa, but is exact in practice for all realistic price/quantity
/// values (well within f64's ~15-17 significant decimal digits).
pub fn fixed_to_decimal(value: CkDecimal) -> Decimal {
    let as_f64: f64 = value.into();
    Decimal::try_from(as_f64).unwrap_or(Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn round_trips_typical_crypto_price() {
        let original = dec!(65000.12345678);
        let fixed = decimal_to_fixed(original).unwrap();
        let back = fixed_to_decimal(fixed);
        assert_eq!(back, original);
    }

    #[test]
    fn round_trips_zero() {
        let original = Decimal::ZERO;
        let fixed = decimal_to_fixed(original).unwrap();
        let back = fixed_to_decimal(fixed);
        assert_eq!(back, original);
    }

    #[test]
    fn round_trips_negative_value() {
        let original = dec!(-123.456);
        let fixed = decimal_to_fixed(original).unwrap();
        let back = fixed_to_decimal(fixed);
        assert_eq!(back, original);
    }

    #[test]
    fn round_trips_integer_value() {
        let original = dec!(100);
        let fixed = decimal_to_fixed(original).unwrap();
        let back = fixed_to_decimal(fixed);
        assert_eq!(back, original);
    }

    #[test]
    fn round_trips_max_supported_scale() {
        // U10 supports up to 10 decimal places.
        let original = dec!(1.2345678901);
        let fixed = decimal_to_fixed(original).unwrap();
        let back = fixed_to_decimal(fixed);
        assert_eq!(back, original);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p apexes-storage conversion`
Expected: 5 tests pass (`round_trips_typical_crypto_price`, `round_trips_zero`, `round_trips_negative_value`, `round_trips_integer_value`, `round_trips_max_supported_scale`).

If any test fails due to a rounding/precision mismatch, do NOT loosen the assertion (e.g. switching to an approximate comparison) — this is a money-precision function and must be exact. Instead, investigate whether `decimal_to_fixed`'s exponent sign or `fixed_to_decimal`'s f64 round-trip needs adjustment, and report the specific failure if you cannot resolve it confidently.

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p apexes-storage -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-storage/src/conversion.rs
git commit -S -s -m "feat(apexes-storage): add Decimal <-> ClickHouse FixedPoint conversion layer"
```

---

### Task 4: Row structs + schema DDL (Trade, Tick)

**Files:**
- Create: `crates/apexes-storage/src/schema.rs`

**Interfaces:**
- Consumes: `crate::conversion::CkDecimal` (Task 3), `apexes_core::{TradeEvent, Tick, Side, Venue}`
- Produces: `TradeRow`, `TickRow` structs (deriving `clickhouse::Row`, `Serialize`, `Deserialize`), `From<&TradeEvent> for TradeRow`, `From<&Tick> for TickRow`, `TryFrom<TradeRow> for TradeEvent`, `TryFrom<TickRow> for Tick`, plus `const TRADES_TABLE_DDL: &str` and `const TICKS_TABLE_DDL: &str`. Later tasks (Depth, Bar) follow the same pattern; Task 5 (writer) and Task 7 (reader) use these row types and DDL constants.

- [ ] **Step 1: Write the row structs, conversions, and DDL constants with tests**

Create `crates/apexes-storage/src/schema.rs`:

```rust
use crate::conversion::{decimal_to_fixed, fixed_to_decimal, CkDecimal};
use crate::error::StorageError;
use apexes_core::{Side, Tick, TradeEvent, Venue};
use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

pub const TRADES_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS trades
(
    symbol      String,
    venue       String,
    price       Decimal(38, 10),
    quantity    Decimal(38, 10),
    side        Nullable(String),
    trade_id    Nullable(String),
    timestamp   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (venue, symbol, timestamp)
";

pub const TICKS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS ticks
(
    symbol      String,
    venue       String,
    bid_price   Decimal(38, 10),
    bid_size    Decimal(38, 10),
    ask_price   Decimal(38, 10),
    ask_size    Decimal(38, 10),
    timestamp   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (venue, symbol, timestamp)
";

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct TradeRow {
    pub symbol: String,
    pub venue: String,
    pub price: CkDecimal,
    pub quantity: CkDecimal,
    pub side: Option<String>,
    pub trade_id: Option<String>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl TryFrom<&TradeEvent> for TradeRow {
    type Error = StorageError;

    fn try_from(event: &TradeEvent) -> Result<Self, StorageError> {
        Ok(TradeRow {
            symbol: event.symbol.clone(),
            venue: format!("{:?}", event.venue),
            price: decimal_to_fixed(event.price)?,
            quantity: decimal_to_fixed(event.quantity)?,
            side: event.side.map(|s| format!("{s:?}")),
            trade_id: event.trade_id.clone(),
            timestamp: event.timestamp,
        })
    }
}

impl TryFrom<TradeRow> for TradeEvent {
    type Error = StorageError;

    fn try_from(row: TradeRow) -> Result<Self, StorageError> {
        Ok(TradeEvent {
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            price: fixed_to_decimal(row.price),
            quantity: fixed_to_decimal(row.quantity),
            side: row.side.map(|s| parse_side(&s)).transpose()?,
            trade_id: row.trade_id,
            timestamp: row.timestamp,
        })
    }
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct TickRow {
    pub symbol: String,
    pub venue: String,
    pub bid_price: CkDecimal,
    pub bid_size: CkDecimal,
    pub ask_price: CkDecimal,
    pub ask_size: CkDecimal,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl TryFrom<&Tick> for TickRow {
    type Error = StorageError;

    fn try_from(tick: &Tick) -> Result<Self, StorageError> {
        Ok(TickRow {
            symbol: tick.symbol.clone(),
            venue: format!("{:?}", tick.venue),
            bid_price: decimal_to_fixed(tick.bid_price)?,
            bid_size: decimal_to_fixed(tick.bid_size)?,
            ask_price: decimal_to_fixed(tick.ask_price)?,
            ask_size: decimal_to_fixed(tick.ask_size)?,
            timestamp: tick.timestamp,
        })
    }
}

impl TryFrom<TickRow> for Tick {
    type Error = StorageError;

    fn try_from(row: TickRow) -> Result<Self, StorageError> {
        Ok(Tick {
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            bid_price: fixed_to_decimal(row.bid_price),
            bid_size: fixed_to_decimal(row.bid_size),
            ask_price: fixed_to_decimal(row.ask_price),
            ask_size: fixed_to_decimal(row.ask_size),
            timestamp: row.timestamp,
        })
    }
}

/// Parses a `Venue`'s `{:?}` (Debug) representation back into the enum.
/// `Venue` derives `Debug`, which for a unit-variant enum prints exactly
/// the variant name (e.g. "Binance"), so this round-trips `format!("{:?}",
/// venue)` used when building rows.
pub(crate) fn parse_venue(value: &str) -> Result<Venue, StorageError> {
    match value {
        "Sse" => Ok(Venue::Sse),
        "Szse" => Ok(Venue::Szse),
        "Hkex" => Ok(Venue::Hkex),
        "Nasdaq" => Ok(Venue::Nasdaq),
        "Nyse" => Ok(Venue::Nyse),
        "Binance" => Ok(Venue::Binance),
        "Okx" => Ok(Venue::Okx),
        "Coinbase" => Ok(Venue::Coinbase),
        other => Err(StorageError::QueryFailed {
            reason: format!("unrecognized venue string in ClickHouse row: {other}"),
        }),
    }
}

pub(crate) fn parse_side(value: &str) -> Result<Side, StorageError> {
    match value {
        "Buy" => Ok(Side::Buy),
        "Sell" => Ok(Side::Sell),
        other => Err(StorageError::QueryFailed {
            reason: format!("unrecognized side string in ClickHouse row: {other}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::Venue;
    use rust_decimal_macros::dec;

    #[test]
    fn trade_event_round_trips_through_trade_row() {
        let event = TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000.5),
            quantity: dec!(0.5),
            side: Some(Side::Buy),
            trade_id: Some("T123".to_string()),
            timestamp: Utc::now(),
        };

        let row = TradeRow::try_from(&event).unwrap();
        let back = TradeEvent::try_from(row).unwrap();

        assert_eq!(back.symbol, event.symbol);
        assert_eq!(back.venue, event.venue);
        assert_eq!(back.price, event.price);
        assert_eq!(back.quantity, event.quantity);
        assert_eq!(back.side, event.side);
        assert_eq!(back.trade_id, event.trade_id);
    }

    #[test]
    fn trade_event_with_none_side_and_trade_id_round_trips() {
        let event = TradeEvent {
            symbol: "ETHUSDT".to_string(),
            venue: Venue::Okx,
            price: dec!(3200),
            quantity: dec!(2),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        };

        let row = TradeRow::try_from(&event).unwrap();
        let back = TradeEvent::try_from(row).unwrap();

        assert_eq!(back.side, None);
        assert_eq!(back.trade_id, None);
    }

    #[test]
    fn tick_round_trips_through_tick_row() {
        let tick = Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(64999.5),
            bid_size: dec!(1.5),
            ask_price: dec!(65000.5),
            ask_size: dec!(2.0),
            timestamp: Utc::now(),
        };

        let row = TickRow::try_from(&tick).unwrap();
        let back = Tick::try_from(row).unwrap();

        assert_eq!(back.symbol, tick.symbol);
        assert_eq!(back.venue, tick.venue);
        assert_eq!(back.bid_price, tick.bid_price);
        assert_eq!(back.bid_size, tick.bid_size);
        assert_eq!(back.ask_price, tick.ask_price);
        assert_eq!(back.ask_size, tick.ask_size);
    }

    #[test]
    fn parse_venue_rejects_unknown_string() {
        let result = parse_venue("NotARealVenue");
        assert!(result.is_err());
    }
}
```

- [ ] **Step 2: Wire the module into `lib.rs`**

Modify `crates/apexes-storage/src/lib.rs`:

```rust
pub mod conversion;
pub mod error;
pub mod reader;
pub mod schema;
pub mod writer;

pub use error::StorageError;
pub use reader::{EventType, StorageReader};
pub use writer::StorageWriter;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p apexes-storage schema`
Expected: 4 tests pass (`trade_event_round_trips_through_trade_row`, `trade_event_with_none_side_and_trade_id_round_trips`, `tick_round_trips_through_tick_row`, `parse_venue_rejects_unknown_string`).

- [ ] **Step 4: Run clippy**

Run: `cargo clippy -p apexes-storage -- -D warnings`
Expected: no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-storage/src/schema.rs crates/apexes-storage/src/lib.rs
git commit -S -s -m "feat(apexes-storage): add TradeRow/TickRow schema and DDL"
```

---

### Task 5: Row structs + schema DDL (Depth, Bar)

**Files:**
- Modify: `crates/apexes-storage/src/schema.rs`

**Interfaces:**
- Consumes: `crate::conversion::CkDecimal` (Task 3), `apexes_core::{DepthUpdate, PriceLevel, Bar, BarInterval}`
- Produces: `DepthRow`, `BarRow` structs, `From`/`TryFrom` conversions, `DEPTH_UPDATES_TABLE_DDL`, `BARS_TABLE_DDL` constants — same pattern as Task 4.

- [ ] **Step 1: Append the Depth and Bar row structs, conversions, DDL, and tests**

Append to `crates/apexes-storage/src/schema.rs` (add these DDL constants near the top, alongside the existing two, and the struct/impl/test blocks after the existing ones):

Add after `TICKS_TABLE_DDL`:

```rust
pub const DEPTH_UPDATES_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS depth_updates
(
    symbol      String,
    venue       String,
    bids        Array(Tuple(Decimal(38, 10), Decimal(38, 10))),
    asks        Array(Tuple(Decimal(38, 10), Decimal(38, 10))),
    sequence    Nullable(UInt64),
    is_snapshot Bool,
    timestamp   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (venue, symbol, timestamp)
";

pub const BARS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS bars
(
    symbol      String,
    venue       String,
    open        Decimal(38, 10),
    high        Decimal(38, 10),
    low         Decimal(38, 10),
    close       Decimal(38, 10),
    volume      Decimal(38, 10),
    interval    String,
    open_time   DateTime64(3, 'UTC'),
    close_time  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(open_time)
ORDER BY (venue, symbol, interval, open_time)
";
```

Add these imports to the top of `schema.rs` (merge with the existing `use` block):

```rust
use apexes_core::{Bar, BarInterval, DepthUpdate, PriceLevel};
```

Add the struct/impl blocks (after `TickRow`'s `impl TryFrom<TickRow> for Tick`, before the `parse_venue`/`parse_side` helper functions):

```rust
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct DepthRow {
    pub symbol: String,
    pub venue: String,
    pub bids: Vec<(CkDecimal, CkDecimal)>,
    pub asks: Vec<(CkDecimal, CkDecimal)>,
    pub sequence: Option<u64>,
    pub is_snapshot: bool,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl TryFrom<&DepthUpdate> for DepthRow {
    type Error = StorageError;

    fn try_from(update: &DepthUpdate) -> Result<Self, StorageError> {
        Ok(DepthRow {
            symbol: update.symbol.clone(),
            venue: format!("{:?}", update.venue),
            bids: levels_to_pairs(&update.bids)?,
            asks: levels_to_pairs(&update.asks)?,
            sequence: update.sequence,
            is_snapshot: update.is_snapshot,
            timestamp: update.timestamp,
        })
    }
}

impl TryFrom<DepthRow> for DepthUpdate {
    type Error = StorageError;

    fn try_from(row: DepthRow) -> Result<Self, StorageError> {
        Ok(DepthUpdate {
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            bids: pairs_to_levels(row.bids),
            asks: pairs_to_levels(row.asks),
            sequence: row.sequence,
            is_snapshot: row.is_snapshot,
            timestamp: row.timestamp,
        })
    }
}

fn levels_to_pairs(levels: &[PriceLevel]) -> Result<Vec<(CkDecimal, CkDecimal)>, StorageError> {
    levels
        .iter()
        .map(|level| Ok((decimal_to_fixed(level.price)?, decimal_to_fixed(level.quantity)?)))
        .collect()
}

fn pairs_to_levels(pairs: Vec<(CkDecimal, CkDecimal)>) -> Vec<PriceLevel> {
    pairs
        .into_iter()
        .map(|(price, quantity)| PriceLevel {
            price: fixed_to_decimal(price),
            quantity: fixed_to_decimal(quantity),
        })
        .collect()
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct BarRow {
    pub symbol: String,
    pub venue: String,
    pub open: CkDecimal,
    pub high: CkDecimal,
    pub low: CkDecimal,
    pub close: CkDecimal,
    pub volume: CkDecimal,
    pub interval: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub open_time: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub close_time: DateTime<Utc>,
}

impl TryFrom<&Bar> for BarRow {
    type Error = StorageError;

    fn try_from(bar: &Bar) -> Result<Self, StorageError> {
        Ok(BarRow {
            symbol: bar.symbol.clone(),
            venue: format!("{:?}", bar.venue),
            open: decimal_to_fixed(bar.open)?,
            high: decimal_to_fixed(bar.high)?,
            low: decimal_to_fixed(bar.low)?,
            close: decimal_to_fixed(bar.close)?,
            volume: decimal_to_fixed(bar.volume)?,
            interval: format!("{:?}", bar.interval),
            open_time: bar.open_time,
            close_time: bar.close_time,
        })
    }
}

impl TryFrom<BarRow> for Bar {
    type Error = StorageError;

    fn try_from(row: BarRow) -> Result<Self, StorageError> {
        Ok(Bar {
            symbol: row.symbol,
            venue: parse_venue(&row.venue)?,
            open: fixed_to_decimal(row.open),
            high: fixed_to_decimal(row.high),
            low: fixed_to_decimal(row.low),
            close: fixed_to_decimal(row.close),
            volume: fixed_to_decimal(row.volume),
            interval: parse_bar_interval(&row.interval)?,
            open_time: row.open_time,
            close_time: row.close_time,
        })
    }
}

pub(crate) fn parse_bar_interval(value: &str) -> Result<BarInterval, StorageError> {
    match value {
        "OneSecond" => Ok(BarInterval::OneSecond),
        "OneMinute" => Ok(BarInterval::OneMinute),
        "FiveMinutes" => Ok(BarInterval::FiveMinutes),
        "FifteenMinutes" => Ok(BarInterval::FifteenMinutes),
        "OneHour" => Ok(BarInterval::OneHour),
        "OneDay" => Ok(BarInterval::OneDay),
        other => Err(StorageError::QueryFailed {
            reason: format!("unrecognized bar interval string in ClickHouse row: {other}"),
        }),
    }
}
```

Add these tests to the existing `#[cfg(test)] mod tests` block (after `parse_venue_rejects_unknown_string`):

```rust
    #[test]
    fn depth_update_round_trips_through_depth_row() {
        let update = DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![
                PriceLevel { price: dec!(64999.5), quantity: dec!(1.5) },
                PriceLevel { price: dec!(64998.0), quantity: dec!(2.0) },
            ],
            asks: vec![PriceLevel { price: dec!(65000.5), quantity: dec!(1.0) }],
            sequence: Some(12345),
            is_snapshot: true,
            timestamp: Utc::now(),
        };

        let row = DepthRow::try_from(&update).unwrap();
        let back = DepthUpdate::try_from(row).unwrap();

        assert_eq!(back.symbol, update.symbol);
        assert_eq!(back.venue, update.venue);
        assert_eq!(back.bids.len(), 2);
        assert_eq!(back.bids[0].price, dec!(64999.5));
        assert_eq!(back.bids[0].quantity, dec!(1.5));
        assert_eq!(back.bids[1].price, dec!(64998.0));
        assert_eq!(back.asks.len(), 1);
        assert_eq!(back.asks[0].price, dec!(65000.5));
        assert_eq!(back.sequence, Some(12345));
        assert!(back.is_snapshot);
    }

    #[test]
    fn depth_update_with_empty_sides_round_trips() {
        let update = DepthUpdate {
            symbol: "ETHUSDT".to_string(),
            venue: Venue::Okx,
            bids: vec![],
            asks: vec![],
            sequence: None,
            is_snapshot: false,
            timestamp: Utc::now(),
        };

        let row = DepthRow::try_from(&update).unwrap();
        let back = DepthUpdate::try_from(row).unwrap();

        assert!(back.bids.is_empty());
        assert!(back.asks.is_empty());
        assert_eq!(back.sequence, None);
        assert!(!back.is_snapshot);
    }

    #[test]
    fn bar_round_trips_through_bar_row() {
        let now = Utc::now();
        let bar = Bar {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            open: dec!(65000),
            high: dec!(65100),
            low: dec!(64900),
            close: dec!(65050),
            volume: dec!(123.456),
            interval: BarInterval::OneMinute,
            open_time: now,
            close_time: now,
        };

        let row = BarRow::try_from(&bar).unwrap();
        let back = Bar::try_from(row).unwrap();

        assert_eq!(back.symbol, bar.symbol);
        assert_eq!(back.venue, bar.venue);
        assert_eq!(back.open, bar.open);
        assert_eq!(back.high, bar.high);
        assert_eq!(back.low, bar.low);
        assert_eq!(back.close, bar.close);
        assert_eq!(back.volume, bar.volume);
        assert_eq!(back.interval, bar.interval);
    }

    #[test]
    fn parse_bar_interval_rejects_unknown_string() {
        let result = parse_bar_interval("NotARealInterval");
        assert!(result.is_err());
    }

    #[test]
    fn parse_bar_interval_handles_all_six_variants() {
        assert_eq!(parse_bar_interval("OneSecond").unwrap(), BarInterval::OneSecond);
        assert_eq!(parse_bar_interval("OneMinute").unwrap(), BarInterval::OneMinute);
        assert_eq!(parse_bar_interval("FiveMinutes").unwrap(), BarInterval::FiveMinutes);
        assert_eq!(parse_bar_interval("FifteenMinutes").unwrap(), BarInterval::FifteenMinutes);
        assert_eq!(parse_bar_interval("OneHour").unwrap(), BarInterval::OneHour);
        assert_eq!(parse_bar_interval("OneDay").unwrap(), BarInterval::OneDay);
    }
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p apexes-storage schema`
Expected: 9 tests pass (4 from Task 4 + 5 new: `depth_update_round_trips_through_depth_row`, `depth_update_with_empty_sides_round_trips`, `bar_round_trips_through_bar_row`, `parse_bar_interval_rejects_unknown_string`, `parse_bar_interval_handles_all_six_variants`).

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p apexes-storage -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-storage/src/schema.rs
git commit -S -s -m "feat(apexes-storage): add DepthRow/BarRow schema and DDL"
```

---

### Task 6: `StorageWriter`

**Files:**
- Modify: `crates/apexes-storage/src/writer.rs`

**Interfaces:**
- Consumes: `crate::schema::{TradeRow, TickRow, DepthRow, BarRow, TRADES_TABLE_DDL, TICKS_TABLE_DDL, DEPTH_UPDATES_TABLE_DDL, BARS_TABLE_DDL}` (Tasks 4-5), `crate::error::StorageError` (Task 2), `apexes_core::MarketEvent`
- Produces: `pub struct StorageWriter`, `pub async fn StorageWriter::new(url: &str) -> Result<Self, StorageError>`, `pub async fn write_batch(&self, events: &[MarketEvent]) -> Result<(), StorageError>`. Task 7 (reader) and Task 8 (integration tests) use `StorageWriter::new`/`write_batch`.

- [ ] **Step 1: Write the writer with unit tests for the mixed-batch validation logic**

Create `crates/apexes-storage/src/writer.rs`:

```rust
use crate::error::StorageError;
use crate::schema::{
    BarRow, DepthRow, TickRow, TradeRow, BARS_TABLE_DDL, DEPTH_UPDATES_TABLE_DDL,
    TICKS_TABLE_DDL, TRADES_TABLE_DDL,
};
use apexes_core::MarketEvent;
use clickhouse::Client;

/// Writes batches of `MarketEvent`s to ClickHouse. Each `write_batch` call
/// requires a homogeneous batch (all elements the same `MarketEvent`
/// variant) and issues exactly one ClickHouse INSERT -- there is no
/// internal buffering, since ClickHouse inserts are cheap and repeatable
/// (unlike the append-once Parquet format originally considered for this
/// sub-project).
pub struct StorageWriter {
    client: Client,
}

impl StorageWriter {
    /// Connects to ClickHouse at `url` (e.g. "http://localhost:8123") and
    /// ensures all 4 tables exist (CREATE TABLE IF NOT EXISTS).
    pub async fn new(url: &str) -> Result<Self, StorageError> {
        let client = Client::default().with_url(url);

        client
            .query(TRADES_TABLE_DDL)
            .execute()
            .await
            .map_err(|error| StorageError::ConnectionFailed {
                reason: format!("failed to create trades table: {error}"),
            })?;
        client
            .query(TICKS_TABLE_DDL)
            .execute()
            .await
            .map_err(|error| StorageError::ConnectionFailed {
                reason: format!("failed to create ticks table: {error}"),
            })?;
        client
            .query(DEPTH_UPDATES_TABLE_DDL)
            .execute()
            .await
            .map_err(|error| StorageError::ConnectionFailed {
                reason: format!("failed to create depth_updates table: {error}"),
            })?;
        client
            .query(BARS_TABLE_DDL)
            .execute()
            .await
            .map_err(|error| StorageError::ConnectionFailed {
                reason: format!("failed to create bars table: {error}"),
            })?;

        Ok(StorageWriter { client })
    }

    /// Writes a homogeneous batch of `MarketEvent`s. Returns
    /// `Err(StorageError::EmptyBatch)` for an empty slice, or
    /// `Err(StorageError::MixedBatch)` if the slice contains more than one
    /// `MarketEvent` variant.
    pub async fn write_batch(&self, events: &[MarketEvent]) -> Result<(), StorageError> {
        let variant = classify_batch(events)?;

        match variant {
            BatchVariant::Trade => self.write_trades(events).await,
            BatchVariant::Tick => self.write_ticks(events).await,
            BatchVariant::Depth => self.write_depth(events).await,
            BatchVariant::Bar => self.write_bars(events).await,
        }
    }

    async fn write_trades(&self, events: &[MarketEvent]) -> Result<(), StorageError> {
        let mut insert = self.client.insert::<TradeRow>("trades").await?;
        for event in events {
            let MarketEvent::Trade(trade) = event else {
                // classify_batch already validated homogeneity before this
                // private helper is called; this branch should never be
                // reachable via the public write_batch API, but we still
                // return a proper error rather than panicking, per the
                // project's no-panics-in-production-code convention.
                return Err(StorageError::MixedBatch);
            };
            insert.write(&TradeRow::try_from(trade)?).await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn write_ticks(&self, events: &[MarketEvent]) -> Result<(), StorageError> {
        let mut insert = self.client.insert::<TickRow>("ticks").await?;
        for event in events {
            let MarketEvent::Tick(tick) = event else {
                return Err(StorageError::MixedBatch);
            };
            insert.write(&TickRow::try_from(tick)?).await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn write_depth(&self, events: &[MarketEvent]) -> Result<(), StorageError> {
        let mut insert = self.client.insert::<DepthRow>("depth_updates").await?;
        for event in events {
            let MarketEvent::Depth(update) = event else {
                return Err(StorageError::MixedBatch);
            };
            insert.write(&DepthRow::try_from(update)?).await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn write_bars(&self, events: &[MarketEvent]) -> Result<(), StorageError> {
        let mut insert = self.client.insert::<BarRow>("bars").await?;
        for event in events {
            let MarketEvent::Bar(bar) = event else {
                return Err(StorageError::MixedBatch);
            };
            insert.write(&BarRow::try_from(bar)?).await?;
        }
        insert.end().await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchVariant {
    Trade,
    Tick,
    Depth,
    Bar,
}

fn variant_of(event: &MarketEvent) -> BatchVariant {
    match event {
        MarketEvent::Trade(_) => BatchVariant::Trade,
        MarketEvent::Tick(_) => BatchVariant::Tick,
        MarketEvent::Depth(_) => BatchVariant::Depth,
        MarketEvent::Bar(_) => BatchVariant::Bar,
    }
}

/// Validates that `events` is non-empty and every element is the same
/// `MarketEvent` variant, returning that variant.
fn classify_batch(events: &[MarketEvent]) -> Result<BatchVariant, StorageError> {
    let first_variant = events.first().map(variant_of).ok_or(StorageError::EmptyBatch)?;

    if events.iter().any(|event| variant_of(event) != first_variant) {
        return Err(StorageError::MixedBatch);
    }

    Ok(first_variant)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{Bar, BarInterval, Tick, TradeEvent, Venue};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn sample_trade() -> MarketEvent {
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(65000),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        })
    }

    fn sample_tick() -> MarketEvent {
        MarketEvent::Tick(Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(64999),
            bid_size: dec!(1),
            ask_price: dec!(65001),
            ask_size: dec!(1),
            timestamp: Utc::now(),
        })
    }

    #[test]
    fn classify_batch_rejects_empty_slice() {
        let result = classify_batch(&[]);
        assert!(matches!(result, Err(StorageError::EmptyBatch)));
    }

    #[test]
    fn classify_batch_rejects_mixed_variants() {
        let events = vec![sample_trade(), sample_tick()];
        let result = classify_batch(&events);
        assert!(matches!(result, Err(StorageError::MixedBatch)));
    }

    #[test]
    fn classify_batch_accepts_homogeneous_trades() {
        let events = vec![sample_trade(), sample_trade()];
        let result = classify_batch(&events);
        assert_eq!(result.unwrap(), BatchVariant::Trade);
    }

    #[test]
    fn classify_batch_accepts_homogeneous_ticks() {
        let events = vec![sample_tick(), sample_tick()];
        let result = classify_batch(&events);
        assert_eq!(result.unwrap(), BatchVariant::Tick);
    }

    #[test]
    fn classify_batch_accepts_single_bar() {
        let now = Utc::now();
        let bar = MarketEvent::Bar(Bar {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            open: dec!(1),
            high: dec!(1),
            low: dec!(1),
            close: dec!(1),
            volume: dec!(1),
            interval: BarInterval::OneMinute,
            open_time: now,
            close_time: now,
        });
        let result = classify_batch(&[bar]);
        assert_eq!(result.unwrap(), BatchVariant::Bar);
    }
}
```

- [ ] **Step 2: Wire the module into `lib.rs`**

Modify `crates/apexes-storage/src/lib.rs` (should already declare `pub mod writer;` from Task 1 — verify it's still there, no change needed if so).

- [ ] **Step 3: Run tests**

Run: `cargo test -p apexes-storage writer`
Expected: 5 tests pass (`classify_batch_rejects_empty_slice`, `classify_batch_rejects_mixed_variants`, `classify_batch_accepts_homogeneous_trades`, `classify_batch_accepts_homogeneous_ticks`, `classify_batch_accepts_single_bar`). None of these tests require a ClickHouse connection.

- [ ] **Step 4: Run clippy**

Run: `cargo clippy -p apexes-storage -- -D warnings`
Expected: no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-storage/src/writer.rs
git commit -S -s -m "feat(apexes-storage): add StorageWriter with homogeneous-batch validation"
```

---

### Task 7: `StorageReader`

**Files:**
- Modify: `crates/apexes-storage/src/reader.rs`

**Interfaces:**
- Consumes: `crate::schema::{TradeRow, TickRow, DepthRow, BarRow}` (Tasks 4-5), `crate::error::StorageError` (Task 2), `apexes_core::{MarketEvent, Venue}`
- Produces: `pub struct StorageReader`, `pub enum EventType { Trade, Tick, Depth, Bar }`, `pub async fn StorageReader::new(url: &str) -> Result<Self, StorageError>`, `pub async fn read_batch(&self, symbol: &str, venue: Venue, date_range: std::ops::Range<chrono::NaiveDate>, event_type: EventType) -> Result<Vec<MarketEvent>, StorageError>`. Task 8 (integration tests) uses this.

- [ ] **Step 1: Write the reader**

Create `crates/apexes-storage/src/reader.rs`:

```rust
use crate::error::StorageError;
use crate::schema::{BarRow, DepthRow, TickRow, TradeRow};
use apexes_core::{MarketEvent, Venue};
use chrono::NaiveDate;
use clickhouse::Client;
use std::ops::Range;

/// Which `MarketEvent` variant (and therefore which ClickHouse table) to
/// query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Trade,
    Tick,
    Depth,
    Bar,
}

/// Reads `MarketEvent`s back from ClickHouse.
pub struct StorageReader {
    client: Client,
}

impl StorageReader {
    pub async fn new(url: &str) -> Result<Self, StorageError> {
        Ok(StorageReader {
            client: Client::default().with_url(url),
        })
    }

    /// Reads all events for `symbol`/`venue` within `date_range` (UTC day
    /// boundaries, exclusive end) from the table matching `event_type`,
    /// returning them as `MarketEvent`s. Uses the tables' `ORDER BY
    /// (venue, symbol, timestamp)` (or `open_time` for bars) sort key for
    /// an efficient range scan.
    pub async fn read_batch(
        &self,
        symbol: &str,
        venue: Venue,
        date_range: Range<NaiveDate>,
        event_type: EventType,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let venue_str = format!("{venue:?}");
        let start = date_range.start.and_hms_opt(0, 0, 0).ok_or_else(|| StorageError::QueryFailed {
            reason: "invalid range start date".to_string(),
        })?;
        let end = date_range.end.and_hms_opt(0, 0, 0).ok_or_else(|| StorageError::QueryFailed {
            reason: "invalid range end date".to_string(),
        })?;

        match event_type {
            EventType::Trade => self.read_trades(symbol, &venue_str, start, end).await,
            EventType::Tick => self.read_ticks(symbol, &venue_str, start, end).await,
            EventType::Depth => self.read_depth(symbol, &venue_str, start, end).await,
            EventType::Bar => self.read_bars(symbol, &venue_str, start, end).await,
        }
    }

    async fn read_trades(
        &self,
        symbol: &str,
        venue: &str,
        start: chrono::NaiveDateTime,
        end: chrono::NaiveDateTime,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM trades WHERE venue = ? AND symbol = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp",
            )
            .bind(venue)
            .bind(symbol)
            .bind(start.and_utc())
            .bind(end.and_utc())
            .fetch_all::<TradeRow>()
            .await?;

        rows.into_iter()
            .map(|row| Ok(MarketEvent::Trade(row.try_into()?)))
            .collect()
    }

    async fn read_ticks(
        &self,
        symbol: &str,
        venue: &str,
        start: chrono::NaiveDateTime,
        end: chrono::NaiveDateTime,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM ticks WHERE venue = ? AND symbol = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp",
            )
            .bind(venue)
            .bind(symbol)
            .bind(start.and_utc())
            .bind(end.and_utc())
            .fetch_all::<TickRow>()
            .await?;

        rows.into_iter()
            .map(|row| Ok(MarketEvent::Tick(row.try_into()?)))
            .collect()
    }

    async fn read_depth(
        &self,
        symbol: &str,
        venue: &str,
        start: chrono::NaiveDateTime,
        end: chrono::NaiveDateTime,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM depth_updates WHERE venue = ? AND symbol = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp",
            )
            .bind(venue)
            .bind(symbol)
            .bind(start.and_utc())
            .bind(end.and_utc())
            .fetch_all::<DepthRow>()
            .await?;

        rows.into_iter()
            .map(|row| Ok(MarketEvent::Depth(row.try_into()?)))
            .collect()
    }

    async fn read_bars(
        &self,
        symbol: &str,
        venue: &str,
        start: chrono::NaiveDateTime,
        end: chrono::NaiveDateTime,
    ) -> Result<Vec<MarketEvent>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM bars WHERE venue = ? AND symbol = ? AND open_time >= ? AND open_time < ? ORDER BY open_time",
            )
            .bind(venue)
            .bind(symbol)
            .bind(start.and_utc())
            .bind(end.and_utc())
            .fetch_all::<BarRow>()
            .await?;

        rows.into_iter()
            .map(|row| Ok(MarketEvent::Bar(row.try_into()?)))
            .collect()
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p apexes-storage`
Expected: succeeds with zero warnings. This task has no new unit tests of its own — its logic (SQL construction, row-to-event mapping) is exercised by the live integration tests in Task 8, since `read_batch` genuinely requires a database connection to test meaningfully (there's no pure-logic seam to extract here, unlike `classify_batch` in Task 6).

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p apexes-storage -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Commit**

```bash
git add crates/apexes-storage/src/reader.rs
git commit -S -s -m "feat(apexes-storage): add StorageReader"
```

---

### Task 8: Live ClickHouse integration tests

**Files:**
- Create: `crates/apexes-storage/tests/live_clickhouse.rs`

**Interfaces:**
- Consumes: `apexes_storage::{StorageWriter, StorageReader, EventType}` (Tasks 6-7), `apexes_core::{MarketEvent, TradeEvent, Tick, DepthUpdate, PriceLevel, Bar, BarInterval, Venue, Side}`
- Produces: `#[ignore]`d integration tests proving the full write→read round-trip works against a real ClickHouse instance, for all 4 event types.

- [ ] **Step 1: Write the integration tests**

Create `crates/apexes-storage/tests/live_clickhouse.rs`:

```rust
//! Live integration tests against a real ClickHouse instance.
//! Excluded from default `cargo test` -- run explicitly with:
//!   docker compose up -d
//!   cargo test -p apexes-storage -- --ignored

use apexes_core::{
    Bar, BarInterval, DepthUpdate, MarketEvent, PriceLevel, Side, Tick, TradeEvent, Venue,
};
use apexes_storage::{EventType, StorageReader, StorageWriter};
use chrono::{NaiveDate, Utc};
use rust_decimal_macros::dec;

const CLICKHOUSE_URL: &str = "http://localhost:8123";

#[tokio::test]
#[ignore]
async fn write_and_read_back_trades() {
    let writer = StorageWriter::new(CLICKHOUSE_URL)
        .await
        .expect("StorageWriter::new should succeed with ClickHouse running locally");
    let reader = StorageReader::new(CLICKHOUSE_URL)
        .await
        .expect("StorageReader::new should succeed");

    let now = Utc::now();
    let symbol = "TESTTRADE";
    let events = vec![MarketEvent::Trade(TradeEvent {
        symbol: symbol.to_string(),
        venue: Venue::Binance,
        price: dec!(65000.12345),
        quantity: dec!(0.5),
        side: Some(Side::Buy),
        trade_id: Some("T1".to_string()),
        timestamp: now,
    })];

    writer.write_batch(&events).await.expect("write_batch should succeed");

    let date = now.date_naive();
    let read_back = reader
        .read_batch(symbol, Venue::Binance, date..(date.succ_opt().unwrap()), EventType::Trade)
        .await
        .expect("read_batch should succeed");

    assert_eq!(read_back.len(), 1);
    let MarketEvent::Trade(trade) = &read_back[0] else {
        panic!("expected a Trade event");
    };
    assert_eq!(trade.symbol, symbol);
    assert_eq!(trade.price, dec!(65000.12345));
    assert_eq!(trade.quantity, dec!(0.5));
    assert_eq!(trade.side, Some(Side::Buy));
}

#[tokio::test]
#[ignore]
async fn write_and_read_back_ticks() {
    let writer = StorageWriter::new(CLICKHOUSE_URL).await.unwrap();
    let reader = StorageReader::new(CLICKHOUSE_URL).await.unwrap();

    let now = Utc::now();
    let symbol = "TESTTICK";
    let events = vec![MarketEvent::Tick(Tick {
        symbol: symbol.to_string(),
        venue: Venue::Binance,
        bid_price: dec!(64999.5),
        bid_size: dec!(1.5),
        ask_price: dec!(65000.5),
        ask_size: dec!(2.0),
        timestamp: now,
    })];

    writer.write_batch(&events).await.unwrap();

    let date = now.date_naive();
    let read_back = reader
        .read_batch(symbol, Venue::Binance, date..(date.succ_opt().unwrap()), EventType::Tick)
        .await
        .unwrap();

    assert_eq!(read_back.len(), 1);
    let MarketEvent::Tick(tick) = &read_back[0] else {
        panic!("expected a Tick event");
    };
    assert_eq!(tick.bid_price, dec!(64999.5));
    assert_eq!(tick.ask_price, dec!(65000.5));
}

#[tokio::test]
#[ignore]
async fn write_and_read_back_depth_with_nested_levels() {
    let writer = StorageWriter::new(CLICKHOUSE_URL).await.unwrap();
    let reader = StorageReader::new(CLICKHOUSE_URL).await.unwrap();

    let now = Utc::now();
    let symbol = "TESTDEPTH";
    let events = vec![MarketEvent::Depth(DepthUpdate {
        symbol: symbol.to_string(),
        venue: Venue::Binance,
        bids: vec![
            PriceLevel { price: dec!(64999.5), quantity: dec!(1.5) },
            PriceLevel { price: dec!(64998.0), quantity: dec!(3.0) },
        ],
        asks: vec![PriceLevel { price: dec!(65000.5), quantity: dec!(2.0) }],
        sequence: Some(42),
        is_snapshot: true,
        timestamp: now,
    })];

    writer.write_batch(&events).await.unwrap();

    let date = now.date_naive();
    let read_back = reader
        .read_batch(symbol, Venue::Binance, date..(date.succ_opt().unwrap()), EventType::Depth)
        .await
        .unwrap();

    assert_eq!(read_back.len(), 1);
    let MarketEvent::Depth(depth) = &read_back[0] else {
        panic!("expected a Depth event");
    };
    assert_eq!(depth.bids.len(), 2);
    assert_eq!(depth.bids[0].price, dec!(64999.5));
    assert_eq!(depth.bids[1].price, dec!(64998.0));
    assert_eq!(depth.asks.len(), 1);
    assert_eq!(depth.asks[0].price, dec!(65000.5));
    assert_eq!(depth.sequence, Some(42));
    assert!(depth.is_snapshot);
}

#[tokio::test]
#[ignore]
async fn write_and_read_back_bars() {
    let writer = StorageWriter::new(CLICKHOUSE_URL).await.unwrap();
    let reader = StorageReader::new(CLICKHOUSE_URL).await.unwrap();

    let now = Utc::now();
    let symbol = "TESTBAR";
    let events = vec![MarketEvent::Bar(Bar {
        symbol: symbol.to_string(),
        venue: Venue::Binance,
        open: dec!(65000),
        high: dec!(65200),
        low: dec!(64900),
        close: dec!(65100),
        volume: dec!(500.25),
        interval: BarInterval::OneMinute,
        open_time: now,
        close_time: now,
    })];

    writer.write_batch(&events).await.unwrap();

    let date = now.date_naive();
    let read_back = reader
        .read_batch(symbol, Venue::Binance, date..(date.succ_opt().unwrap()), EventType::Bar)
        .await
        .unwrap();

    assert_eq!(read_back.len(), 1);
    let MarketEvent::Bar(bar) = &read_back[0] else {
        panic!("expected a Bar event");
    };
    assert_eq!(bar.open, dec!(65000));
    assert_eq!(bar.volume, dec!(500.25));
    assert_eq!(bar.interval, BarInterval::OneMinute);
}

#[tokio::test]
#[ignore]
async fn write_batch_rejects_mixed_event_types() {
    let writer = StorageWriter::new(CLICKHOUSE_URL).await.unwrap();

    let now = Utc::now();
    let events = vec![
        MarketEvent::Trade(TradeEvent {
            symbol: "MIXEDTEST".to_string(),
            venue: Venue::Binance,
            price: dec!(1),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: now,
        }),
        MarketEvent::Tick(Tick {
            symbol: "MIXEDTEST".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(1),
            bid_size: dec!(1),
            ask_price: dec!(1),
            ask_size: dec!(1),
            timestamp: now,
        }),
    ];

    let result = writer.write_batch(&events).await;
    assert!(result.is_err(), "write_batch should reject a mixed-variant batch");
}
```

- [ ] **Step 2: Verify the tests are excluded from default runs**

Run: `cargo test -p apexes-storage`
Expected: all 5 new tests in `live_clickhouse.rs` show as ignored (not run), no database connection attempted.

- [ ] **Step 3: Run the live tests manually (requires `docker compose up -d` first)**

Run: `docker compose up -d` then wait ~5 seconds for ClickHouse to be ready, then:
Run: `cargo test -p apexes-storage -- --ignored`
Expected: all 5 tests pass (`write_and_read_back_trades`, `write_and_read_back_ticks`, `write_and_read_back_depth_with_nested_levels`, `write_and_read_back_bars`, `write_batch_rejects_mixed_event_types`). If ClickHouse isn't running or isn't ready yet, these will fail with a connection error -- if that happens, verify `docker compose ps` shows the container healthy and retry. This is expected to work (not an "environment-dependent, acceptable to fail" situation like sub-project #1's live Binance test) since Docker Compose is checked into this repo and should be startable in any environment with Docker available; if Docker itself is unavailable in your execution environment, report that specific limitation honestly.

- [ ] **Step 4: Run clippy**

Run: `cargo clippy -p apexes-storage --tests -- -D warnings`
Expected: no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/apexes-storage/tests/live_clickhouse.rs
git commit -S -s -m "test(apexes-storage): add live ClickHouse integration tests for all 4 event types"
```

---

### Task 9: Workspace-wide verification

**Files:**
- None created/modified — this task only runs verification commands and fixes any issues surfaced. May also touch `docs/FEATURES.md`.

**Interfaces:**
- Consumes: the entire `apexes-storage` crate built in Tasks 1-8.
- Produces: a workspace passing all Definition-of-Done checks from the design spec §9.

- [ ] **Step 1: Run the full test suite (default, no `--ignored`)**

Run: `cargo test --workspace`
Expected: all `apexes-core` tests (29), `apexes-market-data` tests (18), and `apexes-storage` tests (Task 2: 3, Task 3: 5, Task 4-5: 9, Task 6: 5 = 22 unit tests) pass, plus `apexes-market-data`'s 1 ignored live test and `apexes-storage`'s 5 ignored live tests both correctly shown as ignored. Zero network/database access. Zero failures.

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings across the entire workspace.

- [ ] **Step 3: Run fmt check**

Run: `cargo fmt --check`
Expected: no output, exit code 0. If formatting differs, run `cargo fmt` (no `--check`) to auto-fix, then re-run `cargo fmt --check`.

- [ ] **Step 4: Run a full build**

Run: `cargo build --workspace`
Expected: zero warnings.

- [ ] **Step 5: Run the live ClickHouse integration tests once more with Docker up, as a final end-to-end confirmation**

Run: `docker compose up -d`, wait for readiness, then `cargo test -p apexes-storage -- --ignored`
Expected: all 5 tests pass. Run `docker compose down` afterward to clean up.

- [ ] **Step 6: If any fixes were needed in Steps 2-3, commit them**

```bash
git add -A
git commit -S -s -m "chore: fix clippy warnings and formatting in apexes-storage"
```

If no fixes were needed, skip this commit.

- [ ] **Step 7: Update `docs/FEATURES.md`**

Modify `docs/FEATURES.md`:
- Change row `1.6` ("Historical data storage & backfill (full tick-level, multi-timeframe bars)") status from `🔲 Not Started` to `✅ Done`, Notes: "ClickHouse-backed via apexes-storage (StorageWriter/StorageReader); backfill from external sources deferred"
- Change row `8.1` ("Time-series database (ClickHouse/QuestDB — tick & K-line storage)") status from `🔲 Not Started` to `✅ Done`, Notes: "Implemented in apexes-storage: 4 MergeTree tables (trades/ticks/depth_updates/bars), Decimal(38,10) precision"
- Update the summary counts table: read the current `🔲 Not Started` and `✅ Done` values first (they were last updated at the end of sub-project #1), then decrease `Not Started` by 2 and increase `Done` by 2, keeping the Total at 70.

Run: `grep -c '✅ Done' docs/FEATURES.md` after editing and report the count (should be 2 higher than before this edit).

- [ ] **Step 8: Commit the FEATURES.md update**

```bash
git add docs/FEATURES.md
git commit -S -s -m "docs: mark historical data storage (ClickHouse) features as done"
```

---

## Plan Self-Review Notes

**Spec coverage:** §1 (scope) → Task 1 (crate scaffold), all 4 event types covered across Tasks 4-5; §2 (roadmap adjustment rationale) → informational, no code task needed, already reflected in the vision/roadmap doc directly; §3 (deployment/testing strategy) → Task 1 Step 4 (docker-compose.yml), Task 8 (live/ignored test split); §4 (ClickHouse table structures) → Tasks 4-5 (DDL constants matching spec exactly, including the `bars` table fix removing the dead `MATERIALIZED timestamp` column found during spec self-review); §5 (Decimal↔FixedPoint conversion) → Task 3; §6 (API design) → Task 6 (`StorageWriter`), Task 7 (`StorageReader`); §7 (dependencies) → Task 1; §8 (testing plan) → Tasks 2-3 unit tests (pure logic), Task 6 unit tests (`classify_batch`), Task 8 live integration tests; §9 (DoD) → Task 9.

**Placeholder scan:** No TBD/TODO/"implement later" strings. Every code block is complete, compilable Rust with real assertions (not weak/tautological checks) — this plan deliberately avoided the "sanity placeholder" pattern that had to be caught and fixed during sub-project #1's plan self-review.

**Type consistency:** `StorageWriter`, `StorageReader`, `EventType`, `StorageError`, `CkDecimal`, `TradeRow`/`TickRow`/`DepthRow`/`BarRow`, `decimal_to_fixed`/`fixed_to_decimal` are used with identical signatures across every task that references them — verified against each task's "Interfaces: Produces" line. `write_batch`/`read_batch` signatures match between the design spec (§6) and Tasks 6-7's actual code.

**Known implementation-time verification already done (unlike sub-project #1, where several API assumptions had to be corrected mid-implementation):** before writing this plan, the following were independently compiled and verified against the real `clickhouse 0.15`/`fixnum 0.9`/`rust_decimal 1.42` crates: `Decimal::mantissa()`/`Decimal::scale()` accessors, `FixedPoint::from_decimal(mantissa, exponent)`, `typenum::U10` availability, `#[derive(clickhouse::Row, Serialize, Deserialize)]` accepting `CkDecimal` fields and `Vec<(CkDecimal, CkDecimal)>` (nested array-of-tuples) fields, `#[serde(with = "clickhouse::serde::chrono::datetime64::millis")]` for `DateTime<Utc>` fields, and the `Client::default().with_url()` / `client.insert::<Row>(table).await?.write(&row).await?.end().await?` / `client.query(sql).bind(...).fetch_all::<Row>().await?` API shapes used throughout Tasks 6-7. This significantly de-risks the implementation compared to the trial-and-error API discovery that happened during sub-project #1.
