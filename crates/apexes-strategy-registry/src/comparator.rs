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
