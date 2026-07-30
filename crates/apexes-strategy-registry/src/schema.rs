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
