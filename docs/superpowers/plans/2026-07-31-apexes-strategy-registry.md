# Apexes-Strategy-Registry: Strategy Version Management & Canary Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `apexes-strategy-registry` crate providing ClickHouse-backed strategy version storage (`VersionStore`: publish/get/list/rollback) and a simplified, backtest-only canary comparator (`CanaryComparator`: score multiple versions against the same historical data, optionally auto-promote the winner).

**Architecture:** `VersionStore` persists complete `.drift` script source + metadata as immutable rows in a `strategy_versions` MergeTree table, and tracks each strategy's mutable "current version pointer" via a `strategy_current_version` `ReplacingMergeTree` table (insert-only, dedup-on-read via `FINAL`). `CanaryComparator` wraps a `&VersionStore`, reads each requested version's script source, and scores it via a caller-supplied `Fn(&str) -> Box<dyn Evaluator>` factory (reusing `apexes-optimize`'s `Evaluator` trait) against a shared `InMemoryMarketDataSource`.

**Tech Stack:** Rust edition 2024, `clickhouse` 0.15 (already pinned), `sha2` (new workspace dependency, content hashing), `serde_json` (params snapshot encoding), `apexes-optimize`'s `Evaluator` trait, `apexes-backtest`'s `InMemoryMarketDataSource`, `apexes-strategy`'s `DriftScriptStrategy`.

## Global Constraints

- Design spec of record: `docs/superpowers/specs/2026-07-31-apexes-strategy-registry-design.md`. If you find a mismatch between this plan and that spec while implementing, the spec wins -- stop and flag it rather than improvising.
- Rust edition `2024`, matching `apexes`' workspace (`Cargo.toml` `[workspace.package] edition = "2024"`).
- **No real production canary/traffic-splitting** in this plan -- the "canary" comparison is backtest-only, against a caller-supplied `InMemoryMarketDataSource`, never against live/paper trading flow. This is an explicit scope boundary from the design spec §一.
- **No new storage backend beyond ClickHouse** -- the mutable "current version" pointer is implemented via ClickHouse's `ReplacingMergeTree` engine + `FINAL` reads, not PostgreSQL or any local file.
- Every commit in the `apexes` repo uses `git commit -S -s` (signed + signed-off), matching the project's established convention. Direct commits to `master` (no branches/PRs).
- `cargo build --workspace` / `cargo test --workspace` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo fmt --check` must all pass clean after every task that touches compiled code.
- Unit tests must not require a live ClickHouse instance; tests that do require one are marked `#[ignore]`, matching `apexes-storage`'s established pattern (`crates/apexes-storage/tests/live_clickhouse.rs`).
- `strategy_versions` is a pure append-only table -- no task in this plan implements deletion of a historical version row.

---

### Task 1: Crate scaffold + `schema.rs` (table DDL + Row structs)

**Files:**
- Modify: `Cargo.toml` (add `sha2` to `[workspace.dependencies]`, add `"crates/apexes-strategy-registry"` to `members`)
- Create: `crates/apexes-strategy-registry/Cargo.toml`
- Create: `crates/apexes-strategy-registry/src/lib.rs`
- Create: `crates/apexes-strategy-registry/src/error.rs`
- Create: `crates/apexes-strategy-registry/src/schema.rs`

**Interfaces:**
- Produces: `pub enum RegistryError` (in `crate::error`), `pub struct VersionRow` / `pub struct CurrentVersionRow` (in `crate::schema`), `STRATEGY_VERSIONS_TABLE_DDL` / `STRATEGY_CURRENT_VERSION_TABLE_DDL` constants. Consumed by Task 2's `VersionStore`.

- [x] **Step 1: Add `sha2` to the workspace and register the new crate**

```bash
cd ~/src/oss/apexes
cat Cargo.toml
```

Edit `Cargo.toml`:

```toml
[workspace]
resolver = "3"
members = ["crates/apexes-core", "crates/apexes-cli", "crates/apexes-market-data", "crates/apexes-storage", "crates/apexes-backtest", "crates/apexes-strategy", "crates/apexes-indicators", "crates/apexes-optimize", "crates/apexes-strategy-registry"]

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
```

- [x] **Step 2: Create `crates/apexes-strategy-registry/Cargo.toml`**

```toml
[package]
name = "apexes-strategy-registry"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
apexes-strategy = { path = "../apexes-strategy" }
apexes-storage = { path = "../apexes-storage" }
apexes-optimize = { path = "../apexes-optimize" }
apexes-backtest = { path = "../apexes-backtest" }
clickhouse = { workspace = true }
chrono = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
rust_decimal = { workspace = true }
sha2 = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }
thiserror = { workspace = true }
async-trait = { workspace = true }

[dev-dependencies]
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["test-util", "macros"] }
```

- [x] **Step 3: Write `crates/apexes-strategy-registry/src/error.rs`**

```rust
//! Error type for `apexes-strategy-registry`.

use thiserror::Error;

/// Errors returned by `VersionStore`/`CanaryComparator`.
#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("failed to connect to ClickHouse: {reason}")]
    ConnectionFailed { reason: String },

    #[error("ClickHouse query failed: {reason}")]
    QueryFailed { reason: String },

    #[error("strategy '{strategy_name}' has no version {version}")]
    VersionNotFound { strategy_name: String, version: u32 },

    #[error("failed to serialize/deserialize params snapshot: {reason}")]
    ParamsSerializationFailed { reason: String },

    #[error(
        "no version among {versions:?} for strategy '{strategy_name}' evaluated successfully"
    )]
    NoSuccessfulVersion {
        strategy_name: String,
        versions: Vec<u32>,
    },
}

impl From<clickhouse::error::Error> for RegistryError {
    fn from(error: clickhouse::error::Error) -> Self {
        RegistryError::QueryFailed {
            reason: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_not_found_display() {
        let err = RegistryError::VersionNotFound {
            strategy_name: "threshold_strategy".to_string(),
            version: 5,
        };
        assert_eq!(
            err.to_string(),
            "strategy 'threshold_strategy' has no version 5"
        );
    }

    #[test]
    fn no_successful_version_display() {
        let err = RegistryError::NoSuccessfulVersion {
            strategy_name: "threshold_strategy".to_string(),
            versions: vec![1, 2, 3],
        };
        assert_eq!(
            err.to_string(),
            "no version among [1, 2, 3] for strategy 'threshold_strategy' evaluated successfully"
        );
    }
}
```

- [x] **Step 4: Run the error module's tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy-registry error:: -- --nocapture
```
Expected: FAIL with a compile error (crate has no `lib.rs` yet) -- proceed to Step 5.

- [x] **Step 5: Write `crates/apexes-strategy-registry/src/schema.rs`**

```rust
//! ClickHouse table schemas for `apexes-strategy-registry`.
//!
//! Two tables:
//! - `strategy_versions` (`MergeTree`, pure append-only): one row per
//!   published version, holding the complete `.drift` script source and
//!   metadata. Never updated or deleted, per this crate's audit-trail
//!   requirement (see design spec §一/§八).
//! - `strategy_current_version` (`ReplacingMergeTree`, insert-only): each
//!   `publish`/`set_current` call inserts a new row; reading with `FINAL`
//!   deduplicates to the most-recently-inserted row per `strategy_name`,
//!   giving a mutable "current version pointer" semantic without
//!   introducing a second storage backend.

use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};

pub const STRATEGY_VERSIONS_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS strategy_versions
(
    strategy_name   String,
    version         UInt32,
    script_source   String,
    content_hash    String,
    params          Nullable(String),
    description     Nullable(String),
    created_at      DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY strategy_name
ORDER BY (strategy_name, version)
";

pub const STRATEGY_CURRENT_VERSION_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS strategy_current_version
(
    strategy_name    String,
    current_version  UInt32,
    updated_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY strategy_name
";

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct VersionRow {
    pub strategy_name: String,
    pub version: u32,
    pub script_source: String,
    pub content_hash: String,
    pub params: Option<String>,
    pub description: Option<String>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct CurrentVersionRow {
    pub strategy_name: String,
    pub current_version: u32,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_row_is_constructible_with_no_params_or_description() {
        let row = VersionRow {
            strategy_name: "threshold_strategy".to_string(),
            version: 1,
            script_source: "let x = 1;".to_string(),
            content_hash: "abc123".to_string(),
            params: None,
            description: None,
            created_at: Utc::now(),
        };
        assert_eq!(row.version, 1);
        assert!(row.params.is_none());
    }

    #[test]
    fn current_version_row_is_constructible() {
        let row = CurrentVersionRow {
            strategy_name: "threshold_strategy".to_string(),
            current_version: 3,
            updated_at: Utc::now(),
        };
        assert_eq!(row.current_version, 3);
    }
}
```

- [x] **Step 6: Write `crates/apexes-strategy-registry/src/lib.rs`**

```rust
//! Apexes strategy version management & simplified (backtest-only)
//! canary release: `VersionStore` persists `.drift` script versions and
//! a mutable "current version" pointer to ClickHouse; `CanaryComparator`
//! scores multiple versions against the same historical market data
//! using `apexes_optimize::Evaluator`, optionally promoting the winner.

pub mod comparator;
pub mod error;
pub mod schema;
pub mod version_store;

pub use comparator::CanaryComparator;
pub use error::RegistryError;
pub use version_store::{VersionRecord, VersionStore};
```

This will fail to compile until Tasks 2 and 3 create `version_store.rs`/`comparator.rs` -- that's expected; do not create empty stub files for them in this task.

- [x] **Step 7: Run only the modules that exist so far**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy-registry error:: schema:: 2>&1 | head -40
```
Expected: compile error referencing missing `comparator`/`version_store` modules (from `lib.rs`'s `pub mod` declarations) -- this is expected and resolved by Task 2. Do NOT attempt to fix it in this task.

- [x] **Step 8: Commit**

Since the crate does not compile yet (by design -- `lib.rs` declares modules Task 2/3 create), do not commit yet. Proceed directly to Task 2; Task 2's Step 8 will make the first commit covering both this task's and Task 2's files together, since a plan task must leave the workspace in a state where `cargo build --workspace` succeeds before committing (per Global Constraints).

---

### Task 2: `VersionStore`

**Files:**
- Create: `crates/apexes-strategy-registry/src/version_store.rs`

**Interfaces:**
- Consumes: `crate::error::RegistryError`, `crate::schema::{STRATEGY_VERSIONS_TABLE_DDL, STRATEGY_CURRENT_VERSION_TABLE_DDL, VersionRow, CurrentVersionRow}` (Task 1).
- Produces: `pub struct VersionStore`, `pub struct VersionRecord`, with methods `new`, `publish`, `get_version`, `list_versions`, `current_version`, `set_current`. Consumed by Task 3's `CanaryComparator` and Task 4's integration test.

- [x] **Step 1: Write `crates/apexes-strategy-registry/src/version_store.rs`**

```rust
//! [`VersionStore`]: persists `.drift` script versions to ClickHouse
//! (pure CRUD, no comparison/promotion logic -- see
//! [`crate::comparator::CanaryComparator`] for that).

use std::collections::HashMap;

use chrono::Utc;
use clickhouse::Client;
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};

use crate::error::RegistryError;
use crate::schema::{
    CurrentVersionRow, STRATEGY_CURRENT_VERSION_TABLE_DDL, STRATEGY_VERSIONS_TABLE_DDL,
    VersionRow,
};

/// One version's full record, as returned by
/// [`VersionStore::get_version`]/[`VersionStore::list_versions`].
#[derive(Debug, Clone, PartialEq)]
pub struct VersionRecord {
    pub strategy_name: String,
    pub version: u32,
    pub script_source: String,
    pub content_hash: String,
    pub params: Option<HashMap<String, Decimal>>,
    pub description: Option<String>,
    pub created_at: chrono::DateTime<Utc>,
}

impl VersionRecord {
    fn from_row(row: VersionRow) -> Result<Self, RegistryError> {
        let params = row
            .params
            .map(|json| {
                serde_json::from_str::<HashMap<String, Decimal>>(&json).map_err(|error| {
                    RegistryError::ParamsSerializationFailed {
                        reason: error.to_string(),
                    }
                })
            })
            .transpose()?;

        Ok(VersionRecord {
            strategy_name: row.strategy_name,
            version: row.version,
            script_source: row.script_source,
            content_hash: row.content_hash,
            params,
            description: row.description,
            created_at: row.created_at,
        })
    }
}

fn content_hash(script_source: &str) -> String {
    let digest = Sha256::digest(script_source.as_bytes());
    format!("{digest:x}")
}

/// Persists `.drift` script versions and a mutable "current version"
/// pointer to ClickHouse.
pub struct VersionStore {
    client: Client,
}

impl VersionStore {
    /// Connects to ClickHouse at `url` and ensures both tables exist
    /// (`CREATE TABLE IF NOT EXISTS`), matching
    /// `apexes_storage::StorageWriter::new`'s established pattern.
    pub async fn new(url: &str) -> Result<Self, RegistryError> {
        let client = Client::default().with_url(url);

        client
            .query(STRATEGY_VERSIONS_TABLE_DDL)
            .execute()
            .await
            .map_err(|error| RegistryError::ConnectionFailed {
                reason: format!("failed to create strategy_versions table: {error}"),
            })?;
        client
            .query(STRATEGY_CURRENT_VERSION_TABLE_DDL)
            .execute()
            .await
            .map_err(|error| RegistryError::ConnectionFailed {
                reason: format!("failed to create strategy_current_version table: {error}"),
            })?;

        Ok(VersionStore { client })
    }

    /// Publishes a new version of `strategy_name`. Queries the current
    /// max version for this strategy name (treating "no rows" as 0),
    /// inserts a new row with `version = max + 1`, computes
    /// `content_hash` via SHA-256 of `script_source`. If this is the
    /// strategy's first-ever version, ALSO inserts a
    /// `strategy_current_version` row pointing at the new version 1 --
    /// a strategy is never left with zero current-version rows once at
    /// least one version has been published. Returns the newly assigned
    /// version number.
    ///
    /// Known limitation: concurrent `publish` calls for the same
    /// `strategy_name` can race on the `max(version)` read, both
    /// computing the same "next" version number and each inserting a
    /// row -- ClickHouse provides no cross-row uniqueness constraint to
    /// prevent this. This is an accepted limitation for the current
    /// single-writer usage pattern (see design spec §四); a real
    /// multi-writer coordination mechanism is out of scope for this
    /// crate.
    pub async fn publish(
        &self,
        strategy_name: &str,
        script_source: &str,
        params: Option<HashMap<String, Decimal>>,
        description: Option<&str>,
    ) -> Result<u32, RegistryError> {
        let max_version: Option<u32> = self
            .client
            .query("SELECT max(version) FROM strategy_versions WHERE strategy_name = ?")
            .bind(strategy_name)
            .fetch_optional()
            .await?;
        let next_version = max_version.unwrap_or(0) + 1;

        let params_json = params
            .map(|map| {
                serde_json::to_string(&map).map_err(|error| {
                    RegistryError::ParamsSerializationFailed {
                        reason: error.to_string(),
                    }
                })
            })
            .transpose()?;

        let now = Utc::now();
        let mut insert = self.client.insert::<VersionRow>("strategy_versions").await?;
        insert
            .write(&VersionRow {
                strategy_name: strategy_name.to_string(),
                version: next_version,
                script_source: script_source.to_string(),
                content_hash: content_hash(script_source),
                params: params_json,
                description: description.map(str::to_string),
                created_at: now,
            })
            .await?;
        insert.end().await?;

        if next_version == 1 {
            self.set_current_unchecked(strategy_name, 1).await?;
        }

        Ok(next_version)
    }

    /// Reads back a single version's full record. Returns
    /// `Err(RegistryError::VersionNotFound { .. })` if no such row
    /// exists.
    pub async fn get_version(
        &self,
        strategy_name: &str,
        version: u32,
    ) -> Result<VersionRecord, RegistryError> {
        let row: Option<VersionRow> = self
            .client
            .query(
                "SELECT ?fields FROM strategy_versions WHERE strategy_name = ? AND version = ?",
            )
            .bind(strategy_name)
            .bind(version)
            .fetch_optional()
            .await?;

        match row {
            Some(row) => VersionRecord::from_row(row),
            None => Err(RegistryError::VersionNotFound {
                strategy_name: strategy_name.to_string(),
                version,
            }),
        }
    }

    /// Lists every version of `strategy_name`, ordered by version
    /// ascending. Returns an empty `Vec` (not an error) if the strategy
    /// name has never been published.
    pub async fn list_versions(
        &self,
        strategy_name: &str,
    ) -> Result<Vec<VersionRecord>, RegistryError> {
        let rows: Vec<VersionRow> = self
            .client
            .query(
                "SELECT ?fields FROM strategy_versions WHERE strategy_name = ? ORDER BY version",
            )
            .bind(strategy_name)
            .fetch_all()
            .await?;

        rows.into_iter().map(VersionRecord::from_row).collect()
    }

    /// Returns the current version pointer for `strategy_name`, or
    /// `None` if the strategy has never been published.
    pub async fn current_version(
        &self,
        strategy_name: &str,
    ) -> Result<Option<u32>, RegistryError> {
        let row: Option<CurrentVersionRow> = self
            .client
            .query(
                "SELECT ?fields FROM strategy_current_version FINAL WHERE strategy_name = ? ORDER BY updated_at DESC LIMIT 1",
            )
            .bind(strategy_name)
            .fetch_optional()
            .await?;

        Ok(row.map(|row| row.current_version))
    }

    /// Points `strategy_name`'s current-version pointer at `version`
    /// (rollback when `version` is less than the existing current
    /// version, or a forward move otherwise -- the operation is
    /// identical either way). Returns
    /// `Err(RegistryError::VersionNotFound { .. })` if `version` does
    /// not exist in `strategy_versions` -- the pointer is never allowed
    /// to reference a non-existent version.
    pub async fn set_current(
        &self,
        strategy_name: &str,
        version: u32,
    ) -> Result<(), RegistryError> {
        // Validate existence first so the pointer is never left
        // referencing a non-existent version.
        self.get_version(strategy_name, version).await?;
        self.set_current_unchecked(strategy_name, version).await
    }

    /// Inserts a new `strategy_current_version` row without validating
    /// that `version` exists -- used internally by `publish` (which has
    /// just inserted the version itself in the same call, so a
    /// round-trip existence check would be redundant) and by
    /// `set_current` (after its own explicit validation).
    async fn set_current_unchecked(
        &self,
        strategy_name: &str,
        version: u32,
    ) -> Result<(), RegistryError> {
        let mut insert = self
            .client
            .insert::<CurrentVersionRow>("strategy_current_version")
            .await?;
        insert
            .write(&CurrentVersionRow {
                strategy_name: strategy_name.to_string(),
                current_version: version,
                updated_at: Utc::now(),
            })
            .await?;
        insert.end().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_hash_is_deterministic() {
        let a = content_hash("let x = 1;");
        let b = content_hash("let x = 1;");
        assert_eq!(a, b);
    }

    #[test]
    fn content_hash_differs_for_different_content() {
        let a = content_hash("let x = 1;");
        let b = content_hash("let x = 2;");
        assert_ne!(a, b);
    }

    #[test]
    fn content_hash_is_64_hex_chars_for_sha256() {
        let hash = content_hash("anything");
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn version_record_from_row_with_no_params() {
        let row = VersionRow {
            strategy_name: "threshold_strategy".to_string(),
            version: 1,
            script_source: "let x = 1;".to_string(),
            content_hash: "abc".to_string(),
            params: None,
            description: None,
            created_at: Utc::now(),
        };
        let record = VersionRecord::from_row(row).unwrap();
        assert_eq!(record.params, None);
    }

    #[test]
    fn version_record_from_row_with_params_round_trips() {
        use rust_decimal_macros::dec;

        let mut params = HashMap::new();
        params.insert("threshold".to_string(), dec!(100000));
        let params_json = serde_json::to_string(&params).unwrap();

        let row = VersionRow {
            strategy_name: "threshold_strategy".to_string(),
            version: 2,
            script_source: "let x = 1;".to_string(),
            content_hash: "abc".to_string(),
            params: Some(params_json),
            description: Some("tuned by apexes-optimize".to_string()),
            created_at: Utc::now(),
        };
        let record = VersionRecord::from_row(row).unwrap();
        assert_eq!(record.params, Some(params));
        assert_eq!(record.description, Some("tuned by apexes-optimize".to_string()));
    }

    #[test]
    fn version_record_from_row_with_malformed_params_json_fails() {
        let row = VersionRow {
            strategy_name: "threshold_strategy".to_string(),
            version: 1,
            script_source: "let x = 1;".to_string(),
            content_hash: "abc".to_string(),
            params: Some("not valid json".to_string()),
            description: None,
            created_at: Utc::now(),
        };
        let result = VersionRecord::from_row(row);
        assert!(matches!(
            result,
            Err(RegistryError::ParamsSerializationFailed { .. })
        ));
    }
}
```

- [x] **Step 2: Run the new unit tests**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy-registry version_store:: -- --nocapture
```
Expected: still a compile error, because `lib.rs` also declares `pub mod comparator;` which doesn't exist yet (Task 3). Confirm the ONLY remaining error mentions `comparator` -- if any error mentions `version_store` or `schema` or `error`, fix those first before proceeding.

- [x] **Step 3: Commit is deferred to Task 3**

Do not commit yet -- `cargo build --workspace` still fails until Task 3 creates `comparator.rs` (referenced by `lib.rs`'s `pub mod comparator;`). Proceed directly to Task 3.

---

### Task 3: `CanaryComparator`

**Files:**
- Create: `crates/apexes-strategy-registry/src/comparator.rs`

**Interfaces:**
- Consumes: `crate::version_store::{VersionStore, VersionRecord}` (Task 2), `crate::error::RegistryError` (Task 1), `apexes_optimize::Evaluator` (existing), `apexes_backtest::InMemoryMarketDataSource` (existing).
- Produces: `pub struct CanaryComparator<'a>` with `new`, `compare`, `compare_and_promote`. Consumed by Task 5's end-to-end integration test.

- [x] **Step 1: Write `crates/apexes-strategy-registry/src/comparator.rs`**

```rust
//! [`CanaryComparator`]: a simplified, backtest-only "canary" comparison
//! -- scores multiple published versions of a strategy against the same
//! historical market data and, optionally, promotes the best-scoring
//! version. This is NOT production traffic splitting (see design spec
//! §一/§八 for why that is out of scope: it depends on sub-projects #6
//! Paper Trading / #7 OMS / #8 live gateway, none of which exist yet).

use apexes_backtest::InMemoryMarketDataSource;
use apexes_optimize::Evaluator;
use rust_decimal::Decimal;

use crate::error::RegistryError;
use crate::version_store::VersionStore;

/// Compares multiple versions of a strategy by scoring each against the
/// same historical market data.
pub struct CanaryComparator<'a> {
    version_store: &'a VersionStore,
}

impl<'a> CanaryComparator<'a> {
    pub fn new(version_store: &'a VersionStore) -> Self {
        Self { version_store }
    }

    /// Evaluates each of `versions` against `market_data`, constructing
    /// a fresh `Evaluator` per version via
    /// `evaluator_factory(&version.script_source)`. Reads each
    /// version's `script_source` (and `params` -- used as-is, per that
    /// version's own stored snapshot, defaulting to an empty map if
    /// `None`) from `self.version_store` FIRST, before evaluating
    /// anything: returns `Err(RegistryError::VersionNotFound { .. })`
    /// immediately if any requested version doesn't exist, rather than
    /// silently skipping it or evaluating a partial set.
    ///
    /// Per-version evaluation failures (`Evaluator::evaluate` returning
    /// `Err`) are recorded as `Err(message)` in the returned Vec's
    /// corresponding slot, not propagated as a whole-comparison
    /// failure -- one broken version must not prevent seeing the other
    /// versions' scores, mirroring
    /// `apexes_optimize::GridSearchOptimizer`'s per-combination-failure
    /// semantics.
    pub async fn compare(
        &self,
        strategy_name: &str,
        versions: &[u32],
        market_data: &InMemoryMarketDataSource,
        evaluator_factory: impl Fn(&str) -> Box<dyn Evaluator>,
    ) -> Result<Vec<(u32, Result<Decimal, String>)>, RegistryError> {
        let mut records = Vec::with_capacity(versions.len());
        for &version in versions {
            let record = self.version_store.get_version(strategy_name, version).await?;
            records.push(record);
        }

        let mut results = Vec::with_capacity(records.len());
        for record in records {
            let params = record.params.clone().unwrap_or_default();
            let mut evaluator = evaluator_factory(&record.script_source);
            let outcome = evaluator.evaluate(&params, market_data).await;
            if let Err(message) = &outcome {
                tracing::error!(
                    "canary comparison: strategy '{}' version {} failed: {message}",
                    record.strategy_name,
                    record.version,
                );
            }
            results.push((record.version, outcome));
        }

        Ok(results)
    }

    /// Runs [`Self::compare`], then finds the version with the highest
    /// `Ok` score (versions with `Err` results are excluded from
    /// selection) and calls
    /// `self.version_store.set_current(strategy_name, winning_version)`.
    /// Returns the full comparison `Vec` (every version's result, not
    /// just the winner) plus the promoted version number. Returns
    /// `Err(RegistryError::NoSuccessfulVersion { .. })` if every version
    /// in `versions` failed to evaluate -- the pointer is left unchanged
    /// in this case.
    pub async fn compare_and_promote(
        &self,
        strategy_name: &str,
        versions: &[u32],
        market_data: &InMemoryMarketDataSource,
        evaluator_factory: impl Fn(&str) -> Box<dyn Evaluator>,
    ) -> Result<(Vec<(u32, Result<Decimal, String>)>, u32), RegistryError> {
        let results = self
            .compare(strategy_name, versions, market_data, evaluator_factory)
            .await?;

        let winner = results
            .iter()
            .filter_map(|(version, outcome)| outcome.as_ref().ok().map(|score| (*version, *score)))
            .max_by_key(|(_, score)| *score);

        match winner {
            Some((winning_version, _)) => {
                self.version_store
                    .set_current(strategy_name, winning_version)
                    .await?;
                Ok((results, winning_version))
            }
            None => Err(RegistryError::NoSuccessfulVersion {
                strategy_name: strategy_name.to_string(),
                versions: versions.to_vec(),
            }),
        }
    }
}
```

- [x] **Step 2: Run `cargo check` for the whole crate**

```bash
cd ~/src/oss/apexes
cargo check -p apexes-strategy-registry 2>&1 | tail -40
```
Expected: succeeds with 0 errors (all three modules -- `error`, `schema`, `version_store`, `comparator` -- now exist and `lib.rs` compiles).

- [x] **Step 3: Run all unit tests written so far**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy-registry
```
Expected: all tests from Tasks 1-3 pass (`error::tests` 2, `schema::tests` 2, `version_store::tests` 6), 0 failed. `comparator.rs` has no unit tests yet -- its logic is covered by Task 4's mock-based tests and Task 5's end-to-end test.

- [x] **Step 4: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-strategy-registry --all-targets -- -D warnings
cargo fmt --check
```
Expected clean. If `cargo fmt --check` reports diffs, run `cargo fmt` to auto-fix, then re-run this step's two commands again to confirm both are clean.

- [x] **Step 5: Full workspace build check**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors -- this is the first point in this plan where the new crate compiles as part of the whole workspace.

- [x] **Step 6: Commit Tasks 1-3 together**

Tasks 1-3 are committed as one unit because `lib.rs`'s `pub mod` declarations mean the crate does not compile until all three modules exist (per Global Constraints: every task that touches compiled code must leave `cargo build --workspace` passing before committing).

```bash
cd ~/src/oss/apexes
git add Cargo.toml crates/apexes-strategy-registry
git commit -S -s -m "feat: add apexes-strategy-registry crate with VersionStore and CanaryComparator"
git push origin master
```

---

### Task 4: `CanaryComparator` unit tests with a mock `Evaluator`

**Files:**
- Modify: `crates/apexes-strategy-registry/src/comparator.rs` (append `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `crate::comparator::CanaryComparator` (Task 3), `crate::version_store::VersionStore` (Task 2). Requires a live ClickHouse instance for `VersionStore::new`/`publish` (this task's tests connect to `http://localhost:8123`, matching `apexes-storage`'s pattern of ClickHouse-dependent tests being `#[ignore]`d rather than mocked at the `VersionStore` layer -- see Step 1's rationale below).
- Produces: nothing new consumed by later tasks -- this task only adds test coverage.

- [x] **Step 1: Decide test strategy for `CanaryComparator` (uses real `VersionStore`, marked `#[ignore]`)**

`CanaryComparator` takes `&VersionStore` directly (not a trait), and `VersionStore` has no in-memory/mock variant (per the design spec, ClickHouse is the sole backend, and introducing a parallel trait solely to enable mocking would contradict the "no new storage backend" constraint and add speculative abstraction not requested by the design). Therefore `CanaryComparator`'s tests require a real ClickHouse connection and are marked `#[ignore]`, consistent with `crates/apexes-storage/tests/live_clickhouse.rs`'s established pattern for any test that needs a live database. These tests live in `comparator.rs`'s own `#[cfg(test)]` module (not a separate `tests/` integration file) since they exercise `CanaryComparator` in isolation from the rest of the crate's public API surface, matching how `apexes-optimize/src/grid_search.rs` keeps its `MockEvaluator`-based tests inline in the same file as the code under test.

- [x] **Step 2: Append the test module to `crates/apexes-strategy-registry/src/comparator.rs`**

```rust

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version_store::VersionStore;
    use async_trait::async_trait;
    use chrono::Utc;
    use std::collections::HashMap;

    const CLICKHOUSE_URL: &str = "http://localhost:8123";

    /// A mock `Evaluator` that returns a deterministic score derived
    /// from the script source's content (or fails for a specific
    /// sentinel substring), so `CanaryComparator`'s selection logic can
    /// be tested without running any real backtest.
    struct MockEvaluator {
        script_source: String,
        fail: bool,
    }

    #[async_trait]
    impl Evaluator for MockEvaluator {
        async fn evaluate(
            &mut self,
            params: &HashMap<String, Decimal>,
            _market_data: &InMemoryMarketDataSource,
        ) -> Result<Decimal, String> {
            if self.fail {
                return Err(format!("simulated failure for {}", self.script_source));
            }
            // Score derived from the "score" param if present, else 0 --
            // lets tests control ranking via each version's params
            // snapshot.
            Ok(params.get("score").copied().unwrap_or(Decimal::ZERO))
        }
    }

    fn unique_strategy_name(prefix: &str) -> String {
        format!("{prefix}{}", Utc::now().timestamp_nanos_opt().unwrap_or(0))
    }

    #[tokio::test]
    #[ignore]
    async fn compare_returns_scores_in_the_order_of_requested_versions() {
        let store = VersionStore::new(CLICKHOUSE_URL).await.unwrap();
        let strategy_name = unique_strategy_name("canary_compare_order_");

        let mut low_params = HashMap::new();
        low_params.insert("score".to_string(), rust_decimal_macros::dec!(1));
        store
            .publish(&strategy_name, "let x = 1;", Some(low_params), None)
            .await
            .unwrap();

        let mut high_params = HashMap::new();
        high_params.insert("score".to_string(), rust_decimal_macros::dec!(9));
        store
            .publish(&strategy_name, "let x = 2;", Some(high_params), None)
            .await
            .unwrap();

        let comparator = CanaryComparator::new(&store);
        let market_data = InMemoryMarketDataSource::new(vec![]);

        let results = comparator
            .compare(&strategy_name, &[1, 2], &market_data, |source| {
                Box::new(MockEvaluator {
                    script_source: source.to_string(),
                    fail: false,
                })
            })
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[0].1, Ok(rust_decimal_macros::dec!(1)));
        assert_eq!(results[1].0, 2);
        assert_eq!(results[1].1, Ok(rust_decimal_macros::dec!(9)));
    }

    #[tokio::test]
    #[ignore]
    async fn compare_records_per_version_failure_without_aborting_others() {
        let store = VersionStore::new(CLICKHOUSE_URL).await.unwrap();
        let strategy_name = unique_strategy_name("canary_compare_failure_");

        store
            .publish(&strategy_name, "let x = 1; // fails", None, None)
            .await
            .unwrap();
        let mut ok_params = HashMap::new();
        ok_params.insert("score".to_string(), rust_decimal_macros::dec!(5));
        store
            .publish(&strategy_name, "let x = 2;", Some(ok_params), None)
            .await
            .unwrap();

        let comparator = CanaryComparator::new(&store);
        let market_data = InMemoryMarketDataSource::new(vec![]);

        let results = comparator
            .compare(&strategy_name, &[1, 2], &market_data, |source| {
                let fail = source.contains("fails");
                Box::new(MockEvaluator {
                    script_source: source.to_string(),
                    fail,
                })
            })
            .await
            .unwrap();

        assert_eq!(results[0].0, 1);
        assert!(results[0].1.is_err());
        assert_eq!(results[1].0, 2);
        assert_eq!(results[1].1, Ok(rust_decimal_macros::dec!(5)));
    }

    #[tokio::test]
    #[ignore]
    async fn compare_fails_closed_when_a_requested_version_does_not_exist() {
        let store = VersionStore::new(CLICKHOUSE_URL).await.unwrap();
        let strategy_name = unique_strategy_name("canary_compare_missing_");
        store
            .publish(&strategy_name, "let x = 1;", None, None)
            .await
            .unwrap();

        let comparator = CanaryComparator::new(&store);
        let market_data = InMemoryMarketDataSource::new(vec![]);

        let result = comparator
            .compare(&strategy_name, &[1, 99], &market_data, |source| {
                Box::new(MockEvaluator {
                    script_source: source.to_string(),
                    fail: false,
                })
            })
            .await;

        assert!(matches!(
            result,
            Err(RegistryError::VersionNotFound { version: 99, .. })
        ));
    }

    #[tokio::test]
    #[ignore]
    async fn compare_and_promote_sets_current_to_the_highest_scoring_version() {
        let store = VersionStore::new(CLICKHOUSE_URL).await.unwrap();
        let strategy_name = unique_strategy_name("canary_promote_");

        let mut low_params = HashMap::new();
        low_params.insert("score".to_string(), rust_decimal_macros::dec!(1));
        store
            .publish(&strategy_name, "let x = 1;", Some(low_params), None)
            .await
            .unwrap();
        let mut high_params = HashMap::new();
        high_params.insert("score".to_string(), rust_decimal_macros::dec!(9));
        store
            .publish(&strategy_name, "let x = 2;", Some(high_params), None)
            .await
            .unwrap();

        assert_eq!(store.current_version(&strategy_name).await.unwrap(), Some(1));

        let comparator = CanaryComparator::new(&store);
        let market_data = InMemoryMarketDataSource::new(vec![]);
        let (results, winner) = comparator
            .compare_and_promote(&strategy_name, &[1, 2], &market_data, |source| {
                Box::new(MockEvaluator {
                    script_source: source.to_string(),
                    fail: false,
                })
            })
            .await
            .unwrap();

        assert_eq!(winner, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(
            store.current_version(&strategy_name).await.unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    #[ignore]
    async fn compare_and_promote_leaves_pointer_unchanged_when_all_versions_fail() {
        let store = VersionStore::new(CLICKHOUSE_URL).await.unwrap();
        let strategy_name = unique_strategy_name("canary_promote_all_fail_");

        store
            .publish(&strategy_name, "let x = 1;", None, None)
            .await
            .unwrap();
        store
            .publish(&strategy_name, "let x = 2;", None, None)
            .await
            .unwrap();

        let comparator = CanaryComparator::new(&store);
        let market_data = InMemoryMarketDataSource::new(vec![]);
        let result = comparator
            .compare_and_promote(&strategy_name, &[1, 2], &market_data, |source| {
                Box::new(MockEvaluator {
                    script_source: source.to_string(),
                    fail: true,
                })
            })
            .await;

        assert!(matches!(
            result,
            Err(RegistryError::NoSuccessfulVersion { .. })
        ));
        assert_eq!(
            store.current_version(&strategy_name).await.unwrap(),
            Some(1)
        );
    }

    #[tokio::test]
    #[ignore]
    async fn compare_uses_each_versions_own_params_snapshot() {
        let store = VersionStore::new(CLICKHOUSE_URL).await.unwrap();
        let strategy_name = unique_strategy_name("canary_own_params_");

        let mut params_v1 = HashMap::new();
        params_v1.insert("score".to_string(), rust_decimal_macros::dec!(11));
        store
            .publish(&strategy_name, "let x = 1;", Some(params_v1), None)
            .await
            .unwrap();
        let mut params_v2 = HashMap::new();
        params_v2.insert("score".to_string(), rust_decimal_macros::dec!(22));
        store
            .publish(&strategy_name, "let x = 2;", Some(params_v2), None)
            .await
            .unwrap();

        let comparator = CanaryComparator::new(&store);
        let market_data = InMemoryMarketDataSource::new(vec![]);
        let results = comparator
            .compare(&strategy_name, &[1, 2], &market_data, |source| {
                Box::new(MockEvaluator {
                    script_source: source.to_string(),
                    fail: false,
                })
            })
            .await
            .unwrap();

        assert_eq!(results[0].1, Ok(rust_decimal_macros::dec!(11)));
        assert_eq!(results[1].1, Ok(rust_decimal_macros::dec!(22)));
    }
}
```

- [x] **Step 3: Start ClickHouse and run the new tests**

```bash
cd ~/src/oss/apexes
docker compose up -d
sleep 5
cargo test -p apexes-strategy-registry -- --ignored --nocapture
```
Expected: all 6 new `#[ignore]`d tests pass (plus any prior `#[ignore]`d tests from `apexes-storage` if run in the same invocation -- run `cargo test -p apexes-strategy-registry -- --ignored` scoped to this crate only, as shown, to avoid coupling this task's pass/fail to unrelated crates).

- [x] **Step 4: Run the full (non-ignored) test suite plus clippy/fmt**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy-registry
cargo clippy -p apexes-strategy-registry --all-targets -- -D warnings
cargo fmt --check
```
Expected: non-ignored suite passes (10 tests: 2 error + 2 schema + 6 version_store), clippy clean, fmt clean. If `cargo fmt --check` reports diffs, run `cargo fmt`, then re-run all three commands in this step again to confirm.

- [x] **Step 5: Full workspace verification**

```bash
cd ~/src/oss/apexes
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```
Expected: all clean, 0 failures.

- [x] **Step 6: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-strategy-registry/src/comparator.rs
git commit -S -s -m "test: add CanaryComparator unit tests with a mock Evaluator"
git push origin master
```

---

### Task 5: End-to-end integration test (real `.drift` script + `run_backtest` + `VersionStore` + `CanaryComparator`)

**Files:**
- Create: `crates/apexes-strategy-registry/tests/fixtures/threshold_strategy_v1.drift`
- Create: `crates/apexes-strategy-registry/tests/fixtures/threshold_strategy_v2.drift`
- Create: `crates/apexes-strategy-registry/tests/registry_end_to_end.rs`

**Interfaces:**
- Consumes: `apexes_strategy_registry::{VersionStore, CanaryComparator}` (Tasks 2-3), `apexes_strategy::drift_dsl::DriftScriptStrategy::from_file_with_params` (existing), `apexes_backtest::{run_backtest, BacktestConfig, CostModel, FlatFeeSchedule, ReplaySpeed, InMemoryMarketDataSource}` (existing), `apexes_optimize::Evaluator` (existing).
- Produces: nothing new consumed by later tasks -- this is the plan's final behavioral proof point before Task 6's whole-effort verification.

- [x] **Step 1: Create the two fixture scripts**

```bash
mkdir -p ~/src/oss/apexes/crates/apexes-strategy-registry/tests/fixtures
cat > ~/src/oss/apexes/crates/apexes-strategy-registry/tests/fixtures/threshold_strategy_v1.drift << 'EOF'
let threshold = param("threshold", 100000);

function on_market_event(event, ctx) {
  let price = event.type == "bar" ? event.close : event.price;
  if (price > threshold) {
    return [{
      symbol: event.symbol,
      venue: event.venue,
      side: "buy",
      order_type: "market",
      quantity: 0.01,
    }];
  }
  return [];
}
EOF
cat > ~/src/oss/apexes/crates/apexes-strategy-registry/tests/fixtures/threshold_strategy_v2.drift << 'EOF'
// v2: identical trading logic to v1, but with an explanatory comment --
// distinct script content (and therefore a distinct content_hash) is
// enough for this test's purposes, since the test drives version
// selection via each version's stored `threshold` param rather than via
// behavioral differences between the two scripts.
let threshold = param("threshold", 100000);

function on_market_event(event, ctx) {
  let price = event.type == "bar" ? event.close : event.price;
  if (price > threshold) {
    return [{
      symbol: event.symbol,
      venue: event.venue,
      side: "buy",
      order_type: "market",
      quantity: 0.01,
    }];
  }
  return [];
}
EOF
```

- [x] **Step 2: Write the end-to-end test**

Create `crates/apexes-strategy-registry/tests/registry_end_to_end.rs`:

```rust
//! End-to-end test wiring `VersionStore` + `CanaryComparator` through a
//! real `DriftScriptStrategy` + `run_backtest`, proving the full
//! publish -> compare -> promote -> rollback pipeline works together.
//! Requires a live ClickHouse instance -- all tests here are `#[ignore]`,
//! matching `apexes-storage`'s established pattern (see
//! `crates/apexes-storage/tests/live_clickhouse.rs`).

use apexes_backtest::{
    BacktestConfig, CostModel, FlatFeeSchedule, HistoricalMarketDataSource,
    InMemoryMarketDataSource, ReplaySpeed, run_backtest,
};
use apexes_core::{
    AssetClass, DepthUpdate, Instrument, MarketEvent, MarketType, PriceLevel, TradeEvent, Venue,
};
use apexes_optimize::Evaluator;
use apexes_strategy::drift_dsl::DriftScriptStrategy;
use apexes_strategy_registry::{CanaryComparator, VersionStore};
use async_trait::async_trait;
use chrono::{NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::path::PathBuf;

const CLICKHOUSE_URL: &str = "http://localhost:8123";

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn unique_strategy_name(prefix: &str) -> String {
    format!(
        "{prefix}{}",
        Utc::now().timestamp_nanos_opt().unwrap_or(0)
    )
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

/// Same fixture-data shape as `apexes-optimize`'s end-to-end test: a
/// liquidity-abundant depth snapshot followed by six ascending-then-
/// descending trade prices, so different `threshold` values produce a
/// predictably different number of trades.
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

    let trades = [
        dec!(90000),
        dec!(95000),
        dec!(100000),
        dec!(105000),
        dec!(110000),
        dec!(100000),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, price)| {
        MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price,
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: t0 + chrono::Duration::seconds(i as i64 + 1),
        })
    });

    std::iter::once(depth_snapshot).chain(trades).collect()
}

/// Runs a `.drift` script (given as source text, written to a temp file
/// since `DriftScriptStrategy::from_file_with_params` takes a path) as a
/// backtest and scores it by `report.overall.trade_count`. Uses the same
/// dedicated-OS-thread + `futures::executor::block_on` bridge as
struct SourceBackedEvaluator {
    script_path: PathBuf,
    _temp_dir: tempfile_holder::TempFile,
}

mod tempfile_holder {
    use std::path::PathBuf;

    /// Minimal RAII temp-file holder: writes `contents` to a uniquely
    /// named file under `std::env::temp_dir()` at construction, deletes
    /// it on drop. Avoids adding a `tempfile` crate dependency for this
    /// one narrow use.
    pub struct TempFile {
        path: PathBuf,
    }

    impl TempFile {
        pub fn write(contents: &str) -> (Self, PathBuf) {
            let path = std::env::temp_dir().join(format!(
                "apexes-strategy-registry-test-{}.drift",
                uuid::Uuid::new_v4()
            ));
            std::fs::write(&path, contents).expect("failed to write temp .drift file");
            (Self { path: path.clone() }, path)
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

mod tempfile_holder {
    use std::path::PathBuf;

    /// Minimal RAII temp-file holder: writes `contents` to a uniquely
    /// named file under `std::env::temp_dir()` at construction, deletes
    /// it on drop. Avoids adding a `tempfile` crate dependency for this
    /// one narrow use.
    pub struct TempFile {
        path: PathBuf,
    }

    impl TempFile {
        pub fn write(contents: &str) -> (Self, PathBuf) {
            let path = std::env::temp_dir().join(format!(
                "apexes-strategy-registry-test-{}.drift",
                uuid::Uuid::new_v4()
            ));
            std::fs::write(&path, contents).expect("failed to write temp .drift file");
            (Self { path: path.clone() }, path)
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl SourceBackedEvaluator {
    fn new(script_source: &str) -> Self {
        let (temp_dir, script_path) = tempfile_holder::TempFile::write(script_source);
        Self {
            script_path,
            _temp_dir: temp_dir,
        }
    }
}

#[async_trait]
impl Evaluator for SourceBackedEvaluator {
    async fn evaluate(
        &mut self,
        params: &HashMap<String, Decimal>,
        market_data: &InMemoryMarketDataSource,
    ) -> Result<Decimal, String> {
        let script_path = self.script_path.clone();
        let owned_params = params.clone();
        let mut initial_balances = HashMap::new();
        initial_balances.insert("USDT".to_string(), dec!(100000));

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
        let market_data_events = market_data.events().await.map_err(|e| e.to_string())?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let outcome: Result<Decimal, String> = {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let _guard = rt.enter();
                let market_data = InMemoryMarketDataSource::new(market_data_events);
                futures::executor::block_on(async {
                    let strategy =
                        DriftScriptStrategy::from_file_with_params(&script_path, owned_params)
                            .map_err(|e| e.to_string())?;
                    let report = run_backtest(config, market_data, strategy)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(Decimal::from(report.overall.trade_count))
                })
            };
            let _ = tx.send(outcome);
        });

        rx.await.map_err(|e| e.to_string())?
    }
}

#[tokio::test]
#[ignore]
async fn publish_compare_and_promote_selects_the_threshold_with_more_trades() {
    let store = VersionStore::new(CLICKHOUSE_URL).await.unwrap();
    let strategy_name = unique_strategy_name("registry_e2e_");

    let v1_source = std::fs::read_to_string(fixture_path("threshold_strategy_v1.drift")).unwrap();
    let v2_source = std::fs::read_to_string(fixture_path("threshold_strategy_v2.drift")).unwrap();

    // v1: threshold=97000 -> 4 of the 6 sample trade prices exceed it.
    let mut v1_params = HashMap::new();
    v1_params.insert("threshold".to_string(), dec!(97000));
    let v1 = store
        .publish(&strategy_name, &v1_source, Some(v1_params), Some("initial version"))
        .await
        .unwrap();
    assert_eq!(v1, 1);
    assert_eq!(
        store.current_version(&strategy_name).await.unwrap(),
        Some(1)
    );

    // v2: threshold=80000 -> all 6 sample trade prices exceed it -- more
    // trades than v1, so v2 should win the comparison.
    let mut v2_params = HashMap::new();
    v2_params.insert("threshold".to_string(), dec!(80000));
    let v2 = store
        .publish(&strategy_name, &v2_source, Some(v2_params), Some("lower threshold"))
        .await
        .unwrap();
    assert_eq!(v2, 2);
    // Publishing v2 must NOT move the pointer -- only an explicit
    // `set_current`/`compare_and_promote` call does that.
    assert_eq!(
        store.current_version(&strategy_name).await.unwrap(),
        Some(1)
    );

    let versions = store.list_versions(&strategy_name).await.unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].version, 1);
    assert_eq!(versions[0].description.as_deref(), Some("initial version"));
    assert_eq!(versions[1].version, 2);

    let comparator = CanaryComparator::new(&store);
    let market_data = InMemoryMarketDataSource::new(sample_events());

    let (results, winner) = comparator
        .compare_and_promote(&strategy_name, &[1, 2], &market_data, |source| {
            Box::new(SourceBackedEvaluator::new(source))
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (1, Ok(dec!(4))));
    assert_eq!(results[1], (2, Ok(dec!(6))));
    assert_eq!(winner, 2);
    assert_eq!(
        store.current_version(&strategy_name).await.unwrap(),
        Some(2)
    );

    // Roll back to v1 and confirm the pointer moves accordingly.
    store.set_current(&strategy_name, 1).await.unwrap();
    assert_eq!(
        store.current_version(&strategy_name).await.unwrap(),
        Some(1)
    );
}
```

- [x] **Step 3: Add `uuid` as a dev-dependency (used by `tempfile_holder`)**

Edit `crates/apexes-strategy-registry/Cargo.toml`'s `[dev-dependencies]` section:

```toml
[dev-dependencies]
rust_decimal_macros = "1.40"
tokio = { workspace = true, features = ["test-util", "macros"] }
chrono = { workspace = true }
futures = { workspace = true }
uuid = { workspace = true }
apexes-core = { path = "../apexes-core" }
```

- [x] **Step 4: Run the end-to-end test**

```bash
cd ~/src/oss/apexes
docker compose up -d
sleep 5
cargo test -p apexes-strategy-registry --test registry_end_to_end -- --ignored --nocapture
```
Expected: `1 passed`. If `results[0]`/`results[1]`'s trade counts don't match `dec!(4)`/`dec!(6)`, print `results` (temporarily) and cross-check against `sample_events()`'s six trade prices (`90000, 95000, 100000, 105000, 110000, 100000`) and each version's `threshold` (v1: `97000` -> prices `100000, 105000, 110000, 100000` exceed it = 4; v2: `80000` -> all six exceed it = 6) before assuming the fixture's math is wrong -- this is the same trade-counting logic already proven correct in `apexes-optimize`'s end-to-end test (`crates/apexes-optimize/tests/grid_search_end_to_end.rs`), reused here with the same depth-snapshot-before-any-trade ordering requirement.

- [x] **Step 5: Run the full crate test suite (ignored + non-ignored)**

```bash
cd ~/src/oss/apexes
cargo test -p apexes-strategy-registry
cargo test -p apexes-strategy-registry -- --ignored
```
Expected: non-ignored: 10 passed (2 error + 2 schema + 6 version_store). Ignored: 7 passed (6 from Task 4's `comparator.rs` tests + 1 from this task's `registry_end_to_end.rs`).

- [x] **Step 6: Run clippy and fmt**

```bash
cd ~/src/oss/apexes
cargo clippy -p apexes-strategy-registry --all-targets -- -D warnings
cargo fmt --check
```
Expected clean (auto-fix with `cargo fmt` if needed, then re-run both commands plus Step 5's non-ignored test command once more to confirm nothing regressed).

- [x] **Step 7: Commit**

```bash
cd ~/src/oss/apexes
git add crates/apexes-strategy-registry/tests crates/apexes-strategy-registry/Cargo.toml
git commit -S -s -m "test: add end-to-end publish/compare/promote/rollback integration test"
git push origin master
```

---

### Task 6: Final whole-effort verification

**Files:** None created/modified -- this task only runs verification commands, updates `docs/FEATURES.md`, and confirms the end state.

- [x] **Step 1: Full workspace build**

```bash
cd ~/src/oss/apexes
cargo build --workspace
```
Expected: succeeds, 0 errors.

- [x] **Step 2: Full workspace test suite (non-ignored)**

```bash
cd ~/src/oss/apexes
cargo test --workspace
```
Expected: all tests across all crates pass. Confirm `apexes-core`/`apexes-market-data`/`apexes-indicators`/`apexes-backtest`/`apexes-strategy`/`apexes-optimize` are completely unaffected by this plan (unchanged pass counts from before Task 1 started); `apexes-storage` unaffected; `apexes-strategy-registry` contributes exactly 10 new non-ignored tests (2 `error` + 2 `schema` + 6 `version_store`).

- [x] **Step 3: Full workspace test suite (ignored/live-ClickHouse)**

```bash
cd ~/src/oss/apexes
docker compose up -d
sleep 5
cargo test -p apexes-storage -- --ignored
cargo test -p apexes-strategy-registry -- --ignored
```
Expected: `apexes-storage`'s pre-existing `#[ignore]`d tests still pass unchanged; `apexes-strategy-registry` contributes 7 new `#[ignore]`d tests (6 from `comparator.rs` + 1 end-to-end), all passing.

- [x] **Step 4: Full workspace clippy**

```bash
cd ~/src/oss/apexes
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: 0 warnings.

- [x] **Step 5: Full workspace fmt check**

```bash
cd ~/src/oss/apexes
cargo fmt --check
```
Expected: 0 diffs.

- [x] **Step 6: Update `docs/FEATURES.md`**

Read `docs/FEATURES.md`, find row `2.5` (`Strategy version management & canary release`, currently `🔲 Not Started`). Update its Status to `✅ Done` and its Notes column to:
```
apexes-strategy-registry crate: VersionStore (ClickHouse-backed, append-only strategy_versions table + ReplacingMergeTree strategy_current_version pointer table) provides publish/get_version/list_versions/current_version/set_current (rollback = set_current to an earlier version). CanaryComparator provides a simplified, backtest-only canary comparison (compare/compare_and_promote) reusing apexes-optimize's Evaluator trait via a caller-supplied script-source-to-Evaluator factory closure, scoring multiple published versions against the same historical market data. Real production traffic-splitting canary release is deferred until sub-projects #6 (Paper Trading) / #7 (OMS) / #8 (live gateway) exist to provide an execution environment to split traffic across.
```

**Important:** before finalizing, do not just increment the prior counts by 1/1 -- actually count the current `✅ Done` / `🔲 Not Started` / `⏸️ Deferred` rows in the file (e.g. `grep -c` against the row-level status markers, excluding the legend rows and the Summary table's own rows) and confirm your new numbers match Done+1, Not Started-1, Total unchanged, against the real counts. Update the Summary counts table accordingly and the "Current active work" paragraph to note sub-project `#4`'s five roadmap-scoped phases (`2.1`-`2.5`) are now all complete (PyO3 bindings, part of `2.1`'s full scope per the roadmap, remain explicitly deferred and unscheduled); leave "what's next" unstated since sub-project `#5` (risk engine) and the PyO3 evaluation are both still open options -- don't guess which one the user picks next.

- [x] **Step 7: Commit the FEATURES.md update**

```bash
cd ~/src/oss/apexes
git add docs/FEATURES.md
git commit -S -s -m "docs: mark strategy version management complete in FEATURES.md"
git push origin master
```

- [x] **Step 8: Mark this plan's checkboxes complete**

```bash
cd ~/src/oss/apexes
sed -i '' 's/^- \[ \]/- [x]/' docs/superpowers/plans/2026-07-31-apexes-strategy-registry.md
```
Check `grep -c "^\- \[ \]" docs/superpowers/plans/2026-07-31-apexes-strategy-registry.md` reports `0` remaining unmarked checkboxes. Read the top of the file after running `sed` to verify the header's own descriptive sentence (`Steps use checkbox (\`- [ ]\`) syntax for tracking`) was NOT affected -- that occurrence is inside inline code backticks on its own line and does not match the `^- \[ \]` line-start pattern, so it should be unaffected, but confirm rather than assume.

```bash
git add docs/superpowers/plans/2026-07-31-apexes-strategy-registry.md
git commit -S -s -m "docs: mark apexes-strategy-registry plan complete"
git push origin master
```

- [x] **Step 9: Report final summary**

Summarize: `apexes-strategy-registry` crate's file list and line counts, total test count (broken down: unit tests per module, ignored/live-ClickHouse tests, the end-to-end integration test), confirmation that every other crate's pre-existing tests still pass unchanged, and the `apexes` commit SHAs for Tasks 1-6. No further commit needed for this verification-only task.

---

## Plan Self-Review Notes

**Spec coverage:** Design spec §一 (scope: `VersionStore`/`CanaryComparator`, ClickHouse-only storage, backtest-only simplified canary, no production traffic splitting) → Tasks 1-6 cover all included items; §一's "不包含" exclusions (no production canary, no automated promotion scheduling, no version diff/visualization, no RBAC, no deletion, no Rust-native `Strategy` versioning, no new storage backend) are respected by omission -- no task implements any of them. §二 (crate structure) → Task 1 (scaffold), Task 2 (`version_store.rs`), Task 3 (`comparator.rs`). §三 (schema DDL + Row structs) → Task 1 Step 5. §四 (`VersionStore` API, including the documented concurrent-`publish` limitation) → Task 2. §五 (`CanaryComparator` API, including the `evaluator_factory` design rationale) → Task 3. §六 (`RegistryError` variants) → Task 1 Step 3. §七 (test strategy: schema/Row unit tests, `VersionStore` unit tests, `CanaryComparator` mock-based unit tests, end-to-end integration test, full workspace verification) → Tasks 1-6 respectively. §八 (non-goals) → respected throughout, explicitly called out in Task 6 Step 6's FEATURES.md Notes text.

**Type consistency check:** `VersionStore::publish` returns `Result<u32, RegistryError>` (Task 2) and is called that way in Task 4/5's tests. `VersionRecord.params: Option<HashMap<String, Decimal>>` (Task 2) matches `CanaryComparator::compare`'s use of `record.params.clone().unwrap_or_default()` (Task 3). `CanaryComparator::compare`'s `evaluator_factory: impl Fn(&str) -> Box<dyn Evaluator>` (Task 3) matches every call site in Tasks 4/5's tests, which all pass a closure of that exact shape. `RegistryError::VersionNotFound { strategy_name, version }` (Task 1) is constructed and matched with those exact field names in Task 2 (`get_version`) and asserted against in Task 4/5's tests.

**Placeholder scan:** No "TBD"/"TODO" strings; every code step contains complete, concrete Rust source. Task 5's `registry_end_to_end.rs` contains only the `SourceBackedEvaluator` (and its `tempfile_holder` helper module) as its `Evaluator` implementation -- no unused/dead scaffold code, keeping clippy clean without requiring a separate cleanup step.
