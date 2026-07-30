//! [`ParameterOptimizer`]: the pluggable parameter-search algorithm
//! interface. Two implementations exist in this crate:
//! [`crate::grid_search::GridSearchOptimizer`] (in-process exhaustive
//! search, added in a later task) and
//! [`crate::external_process::ExternalProcessOptimizer`] (delegates to
//! an external subprocess, added in a later task).

use crate::evaluator::Evaluator;
use crate::search_space::SearchSpace;
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// One parameter combination's evaluated result, whether it succeeded
/// or failed.
#[derive(Debug, Clone, PartialEq)]
pub struct EvaluatedCombination {
    pub params: HashMap<String, Decimal>,
    /// `None` if this combination's `Evaluator::evaluate` call returned
    /// `Err` -- excluded from "best" selection, but still recorded here
    /// so callers can inspect which combinations failed. The error
    /// message itself is logged via `tracing::error!` at the time of
    /// failure, not stored on this struct.
    pub score: Option<Decimal>,
}

/// The full result of an optimization run: the best-scoring combination
/// (if at least one combination succeeded) plus every combination's
/// result, in the order they were evaluated.
#[derive(Debug, Clone, PartialEq)]
pub struct OptimizationResult {
    /// `None` only if every single combination failed (or the search
    /// space was empty) -- callers must handle this case explicitly
    /// rather than assuming a best combination always exists.
    pub best: Option<EvaluatedCombination>,
    pub all_results: Vec<EvaluatedCombination>,
    /// `Some(message)` only for a run-level (not per-combination)
    /// failure -- e.g. `ExternalProcessOptimizer`'s subprocess crashed,
    /// exited unexpectedly, or sent malformed/unrecognized protocol
    /// messages. `GridSearchOptimizer` never sets this field (it has no
    /// such failure mode). Distinct from per-combination failures: this
    /// field describes something that invalidates the ENTIRE run, not
    /// just one parameter combination's result.
    pub error: Option<String>,
}

/// A pluggable parameter-search algorithm. This trait boundary is what
/// lets `GridSearchOptimizer` and `ExternalProcessOptimizer` share the
/// same `Evaluator` and caller code; a future third implementation only
/// needs to implement this trait, no other code changes.
#[async_trait]
pub trait ParameterOptimizer {
    async fn optimize(
        &self,
        search_space: &SearchSpace,
        evaluator: &mut dyn Evaluator,
    ) -> OptimizationResult;
}
