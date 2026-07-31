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
            let record = self
                .version_store
                .get_version(strategy_name, version)
                .await?;
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

        assert_eq!(
            store.current_version(&strategy_name).await.unwrap(),
            Some(1)
        );

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
