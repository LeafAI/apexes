//! [`GridSearchOptimizer`]: exhaustively evaluates every combination in
//! a [`SearchSpace`]'s Cartesian product, sequentially, against a single
//! pre-loaded set of market events shared across all combinations.

use apexes_backtest::InMemoryMarketDataSource;
use apexes_core::MarketEvent;
use async_trait::async_trait;

use crate::evaluator::Evaluator;
use crate::optimizer::{EvaluatedCombination, OptimizationResult, ParameterOptimizer};
use crate::search_space::SearchSpace;

/// Exhaustively evaluates every combination in a `SearchSpace`'s
/// Cartesian product, sequentially, against a single pre-loaded set of
/// market events (loaded once by the caller, not re-fetched per
/// combination -- avoids repeated storage reads / event-vec clones
/// across the many backtests a grid search runs).
pub struct GridSearchOptimizer {
    market_data: InMemoryMarketDataSource,
}

impl GridSearchOptimizer {
    pub fn new(events: Vec<MarketEvent>) -> Self {
        Self {
            market_data: InMemoryMarketDataSource::new(events),
        }
    }
}

#[async_trait]
impl ParameterOptimizer for GridSearchOptimizer {
    async fn optimize(
        &self,
        search_space: &SearchSpace,
        evaluator: &mut dyn Evaluator,
    ) -> OptimizationResult {
        let mut all_results = Vec::new();
        let mut best: Option<EvaluatedCombination> = None;

        for params in search_space.combinations() {
            let score = match evaluator.evaluate(&params, &self.market_data).await {
                Ok(score) => Some(score),
                Err(message) => {
                    tracing::error!("grid search combination {params:?} failed: {message}");
                    None
                }
            };

            let is_new_best = match (score, best.as_ref().and_then(|b| b.score)) {
                (Some(candidate), Some(current_best)) => candidate > current_best,
                (Some(_), None) => true,
                (None, _) => false,
            };

            let combination = EvaluatedCombination { params, score };
            if is_new_best {
                best = Some(combination.clone());
            }
            all_results.push(combination);
        }

        OptimizationResult {
            best,
            all_results,
            error: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evaluator::Evaluator;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    /// A mock `Evaluator` that returns a deterministic score derived
    /// directly from the `"x"` parameter (or fails for a specific
    /// sentinel value), so `GridSearchOptimizer`'s selection logic can
    /// be tested without running any real backtest.
    struct MockEvaluator {
        fail_on_x: Option<Decimal>,
    }

    #[async_trait]
    impl Evaluator for MockEvaluator {
        async fn evaluate(
            &mut self,
            params: &HashMap<String, Decimal>,
            _market_data: &InMemoryMarketDataSource,
        ) -> Result<Decimal, String> {
            let x = params["x"];
            if Some(x) == self.fail_on_x {
                return Err(format!("simulated failure for x={x}"));
            }
            Ok(x)
        }
    }

    #[tokio::test]
    async fn finds_the_highest_scoring_combination() {
        let optimizer = GridSearchOptimizer::new(vec![]);
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1), dec!(5), dec!(3)]);
        let mut evaluator = MockEvaluator { fail_on_x: None };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.error, None);
        let best = result.best.expect("expected a best combination");
        assert_eq!(best.params["x"], dec!(5));
        assert_eq!(best.score, Some(dec!(5)));
    }

    #[tokio::test]
    async fn all_results_contains_every_combination() {
        let optimizer = GridSearchOptimizer::new(vec![]);
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1), dec!(2), dec!(3)]);
        let mut evaluator = MockEvaluator { fail_on_x: None };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.all_results.len(), 3);
        let mut scores: Vec<Decimal> = result
            .all_results
            .iter()
            .map(|c| c.score.unwrap())
            .collect();
        scores.sort();
        assert_eq!(scores, vec![dec!(1), dec!(2), dec!(3)]);
    }

    #[tokio::test]
    async fn a_failed_combination_is_excluded_from_best_but_recorded() {
        let optimizer = GridSearchOptimizer::new(vec![]);
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1), dec!(5), dec!(3)]);
        // "5" would otherwise be the best score, but evaluation fails for it.
        let mut evaluator = MockEvaluator {
            fail_on_x: Some(dec!(5)),
        };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        let best = result
            .best
            .expect("expected a best combination among the non-failed ones");
        assert_eq!(best.params["x"], dec!(3));
        assert_eq!(result.all_results.len(), 3);
        let failed = result
            .all_results
            .iter()
            .find(|c| c.params["x"] == dec!(5))
            .unwrap();
        assert_eq!(failed.score, None);
    }

    #[tokio::test]
    async fn best_is_none_when_every_combination_fails() {
        let optimizer = GridSearchOptimizer::new(vec![]);
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1)]);
        let mut evaluator = MockEvaluator {
            fail_on_x: Some(dec!(1)),
        };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.best, None);
        assert_eq!(result.all_results.len(), 1);
        assert_eq!(result.error, None);
    }

    #[tokio::test]
    async fn empty_search_space_produces_empty_result() {
        let optimizer = GridSearchOptimizer::new(vec![]);
        let search_space = SearchSpace::new();
        let mut evaluator = MockEvaluator { fail_on_x: None };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.best, None);
        assert!(result.all_results.is_empty());
        assert_eq!(result.error, None);
    }
}
