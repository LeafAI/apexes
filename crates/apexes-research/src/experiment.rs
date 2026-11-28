use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    Artifact, ArtifactKind, ResearchError, ResearchStore, invalid, validate_id, validate_text,
};

/// All references are immutable content identities, never mutable file names or
/// registry "current" pointers. Runtime must pin the executor build/dependency
/// identity; configuration must include costs, latency and all execution options.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Experiment {
    schema_version: u32,
    strategy: String,
    dataset: String,
    hypothesis: String,
    configuration: String,
    runtime: String,
    seed: u64,
}

impl Experiment {
    pub fn new(
        strategy: String,
        dataset: String,
        hypothesis: String,
        configuration: String,
        runtime: &str,
        seed: u64,
    ) -> Result<Self, ResearchError> {
        let experiment = Self {
            schema_version: 1,
            strategy,
            dataset,
            hypothesis,
            configuration,
            runtime: runtime.to_owned(),
            seed,
        };
        experiment.validate()?;
        Ok(experiment)
    }

    pub fn strategy(&self) -> &str {
        &self.strategy
    }
    pub fn dataset(&self) -> &str {
        &self.dataset
    }
    pub fn hypothesis(&self) -> &str {
        &self.hypothesis
    }
    pub fn configuration(&self) -> &str {
        &self.configuration
    }
    pub fn runtime(&self) -> &str {
        &self.runtime
    }
    pub fn seed(&self) -> u64 {
        self.seed
    }

    fn validate(&self) -> Result<(), ResearchError> {
        if self.schema_version != 1 {
            return Err(invalid("unsupported experiment schema version"));
        }
        for id in [
            &self.strategy,
            &self.dataset,
            &self.hypothesis,
            &self.configuration,
        ] {
            validate_id(id)?;
        }
        validate_text(&self.runtime, 4096)
    }
}

/// Fully verified retained inputs passed to the caller's executor.
#[derive(Debug)]
pub struct ResolvedExperiment {
    pub strategy: Artifact,
    pub dataset: Artifact,
    pub hypothesis: Artifact,
    pub configuration: Artifact,
    pub runtime: String,
    pub seed: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    experiment: Experiment,
    output: Vec<u8>,
}

impl ResearchStore {
    /// Executes only after verifying all four input identities and kinds. Failed
    /// executions do not produce a successful result receipt. Output must be
    /// deterministic bytes (exclude wall-clock timings and unordered maps).
    pub fn run<F>(&self, experiment: &Experiment, execute: F) -> Result<String, ResearchError>
    where
        F: FnOnce(&ResolvedExperiment) -> Result<Vec<u8>, String>,
    {
        let resolved = self.resolve(experiment)?;
        let output = execute(&resolved).map_err(ResearchError::Execution)?;
        if output.is_empty() {
            return Err(invalid("experiment output must not be empty"));
        }
        let receipt = Receipt {
            experiment: experiment.clone(),
            output,
        };
        self.put(&Artifact::new(
            ArtifactKind::Result,
            "experiment-result",
            "1",
            BTreeMap::new(),
            serde_json::to_vec(&receipt)?,
        )?)
    }

    /// Rehydrates the exact retained inputs, reruns the supplied executor and
    /// compares exact output bytes. A match is reproducibility evidence, not
    /// evidence of profitability or production suitability.
    pub fn verify<F>(&self, result_id: &str, execute: F) -> Result<(), ResearchError>
    where
        F: FnOnce(&ResolvedExperiment) -> Result<Vec<u8>, String>,
    {
        let artifact = self.get_kind(result_id, ArtifactKind::Result)?;
        let receipt: Receipt = serde_json::from_slice(artifact.content())?;
        let resolved = self.resolve(&receipt.experiment)?;
        if receipt.output.is_empty() {
            return Err(invalid("experiment output must not be empty"));
        }
        let actual = execute(&resolved).map_err(ResearchError::Execution)?;
        if actual != receipt.output {
            return Err(ResearchError::ReplayMismatch);
        }
        Ok(())
    }

    fn resolve(&self, experiment: &Experiment) -> Result<ResolvedExperiment, ResearchError> {
        experiment.validate()?;
        Ok(ResolvedExperiment {
            strategy: self.get_kind(&experiment.strategy, ArtifactKind::Strategy)?,
            dataset: self.get_kind(&experiment.dataset, ArtifactKind::Dataset)?,
            hypothesis: self.get_kind(&experiment.hypothesis, ArtifactKind::Hypothesis)?,
            configuration: self.get_kind(&experiment.configuration, ArtifactKind::Configuration)?,
            runtime: experiment.runtime.clone(),
            seed: experiment.seed,
        })
    }

    fn get_kind(&self, id: &str, expected: ArtifactKind) -> Result<Artifact, ResearchError> {
        let artifact = self.get(id)?;
        if artifact.kind() != expected {
            return Err(ResearchError::WrongKind {
                expected,
                actual: artifact.kind(),
            });
        }
        Ok(artifact)
    }
}
