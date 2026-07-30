//! [`Evaluator`]: the caller-supplied "run one backtest with these
//! parameters, return a score" callback that every `ParameterOptimizer`
//! implementation drives.

use apexes_backtest::InMemoryMarketDataSource;
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Evaluates a single parameter combination and returns its objective
/// score. Implementations decide how to construct the strategy (e.g.
/// `apexes_strategy::drift_dsl::DriftScriptStrategy::from_file_with_params`),
/// run the backtest (`apexes_backtest::run_backtest`), and compute the
/// score from the resulting `BacktestReport` (e.g. via a caller-supplied
/// `Fn(&apexes_backtest::BacktestReport) -> Decimal` objective closure)
/// -- this trait only fixes the input (`params`, `market_data`) and
/// output (`Decimal` score) shape, not any of that internal wiring.
#[async_trait]
pub trait Evaluator {
    /// Returns `Err(message)` if constructing the strategy or running
    /// the backtest failed for this parameter combination (e.g. a
    /// malformed `.drift` script, or a backtest configuration error).
    /// Every `ParameterOptimizer` implementation treats this as the
    /// worst possible score for ranking purposes, logs the error via
    /// `tracing::error!`, and continues with the remaining combinations
    /// -- one bad combination must not abort the whole search.
    async fn evaluate(
        &mut self,
        params: &HashMap<String, Decimal>,
        market_data: &InMemoryMarketDataSource,
    ) -> Result<Decimal, String>;
}
