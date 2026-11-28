//! F05.01: immutable research artifacts and reproducible experiment receipts.
//!
//! This offline library does not promote strategies or change execution/risk policy.
//! Callers supply exact dataset bytes, a pinned runtime identity and an executor
//! backed by the existing unified trading loop. Replay checks exact result bytes,
//! not statistical similarity or live-trading qualification.

mod artifact;
mod experiment;
mod store;

pub use artifact::{Artifact, ArtifactKind};
pub use experiment::{Experiment, ResolvedExperiment};
pub use store::ResearchStore;

#[derive(Debug, thiserror::Error)]
pub enum ResearchError {
    #[error("invalid research artifact: {0}")]
    Invalid(String),
    #[error("research artifact integrity check failed")]
    Integrity,
    #[error("expected {expected:?} artifact, found {actual:?}")]
    WrongKind {
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("research storage operation {operation} failed: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("research serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("experiment execution failed: {0}")]
    Execution(String),
    #[error("experiment replay produced different result bytes")]
    ReplayMismatch,
}

fn invalid(message: &str) -> ResearchError {
    ResearchError::Invalid(message.to_owned())
}

fn validate_text(value: &str, maximum: usize) -> Result<(), ResearchError> {
    if value.trim().is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(invalid(
            "text must be nonblank, bounded and contain no control characters",
        ));
    }
    Ok(())
}

fn validate_id(id: &str) -> Result<(), ResearchError> {
    if id.len() != 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid(
            "identity must be 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}
