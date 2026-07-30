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
    CurrentVersionRow, STRATEGY_CURRENT_VERSION_TABLE_DDL, STRATEGY_VERSIONS_TABLE_DDL, VersionRow,
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
        let mut insert = self
            .client
            .insert::<VersionRow>("strategy_versions")
            .await?;
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
            .query("SELECT ?fields FROM strategy_versions WHERE strategy_name = ? AND version = ?")
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
            .query("SELECT ?fields FROM strategy_versions WHERE strategy_name = ? ORDER BY version")
            .bind(strategy_name)
            .fetch_all()
            .await?;

        rows.into_iter().map(VersionRecord::from_row).collect()
    }

    /// Returns the current version pointer for `strategy_name`, or
    /// `None` if the strategy has never been published.
    pub async fn current_version(&self, strategy_name: &str) -> Result<Option<u32>, RegistryError> {
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
        assert_eq!(
            record.description,
            Some("tuned by apexes-optimize".to_string())
        );
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
