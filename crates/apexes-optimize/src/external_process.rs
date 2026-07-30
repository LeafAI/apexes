//! [`ExternalProcessOptimizer`]: delegates parameter search to an
//! external subprocess (any language) speaking a newline-delimited-JSON
//! protocol, per the project roadmap's "pluggable external ML service"
//! requirement. Deliberately NOT a native dynamic-library plugin -- see
//! docs/superpowers/specs/2026-07-30-apexes-optimize-design.md §一/§七
//! for why (no stable Rust ABI, would require `unsafe` code).
//!
//! Protocol (see design spec §七 for full details): the host sends one
//! `{"search_space": {...}}` line at startup; the subprocess then sends
//! zero or more `{"evaluate": {...}}` lines (in whatever order/quantity
//! it chooses), each answered by the host with one `{"score": ...}` or
//! `{"error": "..."}` line; the subprocess finally sends one
//! `{"done": {"best_params": ..., "best_score": ...}}` or
//! `{"done": null}` line and the exchange ends.

use std::collections::HashMap;
use std::process::Stdio;

use apexes_backtest::InMemoryMarketDataSource;
use apexes_core::MarketEvent;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

use crate::evaluator::Evaluator;
use crate::optimizer::{EvaluatedCombination, OptimizationResult, ParameterOptimizer};
use crate::search_space::SearchSpace;

#[derive(Serialize)]
struct SearchSpaceMessage {
    search_space: HashMap<String, Vec<Decimal>>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PluginMessage {
    Evaluate { evaluate: HashMap<String, Decimal> },
    Done { done: Option<DoneResult> },
}

#[derive(Deserialize)]
struct DoneResult {
    best_params: HashMap<String, Decimal>,
    best_score: Decimal,
}

#[derive(Serialize)]
#[serde(untagged)]
enum HostResponse {
    Score { score: Decimal },
    Error { error: String },
}

/// Delegates parameter search to an external subprocess speaking the
/// newline-delimited-JSON protocol documented above.
pub struct ExternalProcessOptimizer {
    command: String,
    args: Vec<String>,
    market_data: InMemoryMarketDataSource,
}

impl ExternalProcessOptimizer {
    pub fn new(command: impl Into<String>, args: Vec<String>, events: Vec<MarketEvent>) -> Self {
        Self {
            command: command.into(),
            args,
            market_data: InMemoryMarketDataSource::new(events),
        }
    }
}

#[async_trait]
impl ParameterOptimizer for ExternalProcessOptimizer {
    async fn optimize(
        &self,
        search_space: &SearchSpace,
        evaluator: &mut dyn Evaluator,
    ) -> OptimizationResult {
        let mut child = match Command::new(&self.command)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                return OptimizationResult {
                    best: None,
                    all_results: Vec::new(),
                    error: Some(format!("failed to spawn external optimizer process: {e}")),
                };
            }
        };

        let mut stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let mut lines = BufReader::new(stdout).lines();

        let initial_message = SearchSpaceMessage {
            search_space: search_space.parameters().clone(),
        };
        if let Err(e) = write_json_line(&mut stdin, &initial_message).await {
            return OptimizationResult {
                best: None,
                all_results: Vec::new(),
                error: Some(format!(
                    "failed to write search space to external process: {e}"
                )),
            };
        }

        let mut all_results = Vec::new();

        loop {
            let line = match lines.next_line().await {
                Ok(Some(line)) => line,
                Ok(None) => {
                    return OptimizationResult {
                        best: None,
                        all_results,
                        error: Some(
                            "external optimizer process exited before sending a done message"
                                .to_string(),
                        ),
                    };
                }
                Err(e) => {
                    return OptimizationResult {
                        best: None,
                        all_results,
                        error: Some(format!("failed to read from external process: {e}")),
                    };
                }
            };

            let message: PluginMessage = match serde_json::from_str(&line) {
                Ok(message) => message,
                Err(e) => {
                    return OptimizationResult {
                        best: None,
                        all_results,
                        error: Some(format!(
                            "external optimizer sent malformed JSON ({line:?}): {e}"
                        )),
                    };
                }
            };

            match message {
                PluginMessage::Evaluate { evaluate: params } => {
                    let score = match evaluator.evaluate(&params, &self.market_data).await {
                        Ok(score) => Some(score),
                        Err(message) => {
                            tracing::error!(
                                "external optimizer combination {params:?} failed: {message}"
                            );
                            None
                        }
                    };

                    let response = match score {
                        Some(score) => HostResponse::Score { score },
                        None => HostResponse::Error {
                            error: "evaluation failed, see host logs".to_string(),
                        },
                    };
                    if let Err(e) = write_json_line(&mut stdin, &response).await {
                        return OptimizationResult {
                            best: None,
                            all_results,
                            error: Some(format!(
                                "failed to write response to external process: {e}"
                            )),
                        };
                    }

                    all_results.push(EvaluatedCombination { params, score });
                }
                PluginMessage::Done { done: None } => {
                    return OptimizationResult {
                        best: None,
                        all_results,
                        error: None,
                    };
                }
                PluginMessage::Done { done: Some(result) } => {
                    let best = EvaluatedCombination {
                        params: result.best_params,
                        score: Some(result.best_score),
                    };
                    return OptimizationResult {
                        best: Some(best),
                        all_results,
                        error: None,
                    };
                }
            }
        }
    }
}

async fn write_json_line<T: Serialize>(
    stdin: &mut tokio::process::ChildStdin,
    value: &T,
) -> std::io::Result<()> {
    let mut line =
        serde_json::to_string(value).expect("serialization of a fixed message shape cannot fail");
    line.push('\n');
    stdin.write_all(line.as_bytes()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn fixture_path(name: &str) -> String {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name)
            .to_string_lossy()
            .to_string()
    }

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
    async fn finds_the_highest_scoring_combination_via_external_process() {
        let optimizer = ExternalProcessOptimizer::new(
            "python3",
            vec![fixture_path("mock_optimizer.py")],
            vec![],
        );
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1), dec!(5), dec!(3)]);
        let mut evaluator = MockEvaluator { fail_on_x: None };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.error, None);
        let best = result.best.expect("expected a best combination");
        assert_eq!(best.params["x"], dec!(5));
        assert_eq!(best.score, Some(dec!(5)));
        assert_eq!(result.all_results.len(), 3);
    }

    #[tokio::test]
    async fn a_failed_combination_is_recorded_but_excluded_from_best() {
        let optimizer = ExternalProcessOptimizer::new(
            "python3",
            vec![fixture_path("mock_optimizer.py")],
            vec![],
        );
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1), dec!(5), dec!(3)]);
        let mut evaluator = MockEvaluator {
            fail_on_x: Some(dec!(5)),
        };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.error, None);
        let best = result
            .best
            .expect("expected a best combination among the non-failed ones");
        assert_eq!(best.params["x"], dec!(3));
        let failed = result
            .all_results
            .iter()
            .find(|c| c.params["x"] == dec!(5))
            .unwrap();
        assert_eq!(failed.score, None);
    }

    #[tokio::test]
    async fn spawn_failure_is_reported_as_a_run_level_error() {
        let optimizer = ExternalProcessOptimizer::new(
            "this-command-definitely-does-not-exist-anywhere",
            vec![],
            vec![],
        );
        let search_space = SearchSpace::new().with_parameter("x", vec![dec!(1)]);
        let mut evaluator = MockEvaluator { fail_on_x: None };

        let result = optimizer.optimize(&search_space, &mut evaluator).await;

        assert_eq!(result.best, None);
        assert!(result.error.is_some());
    }
}
