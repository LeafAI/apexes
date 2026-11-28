use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{ResearchError, invalid, validate_text};

pub(crate) const MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    Strategy,
    Dataset,
    Hypothesis,
    Configuration,
    Result,
}

/// Identity covers schema, kind, logical name, revision, metadata and exact bytes.
/// Fields are private so validation cannot be bypassed by direct construction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    schema_version: u32,
    kind: ArtifactKind,
    name: String,
    revision: String,
    metadata: BTreeMap<String, String>,
    content: Vec<u8>,
}

impl Artifact {
    pub fn new(
        kind: ArtifactKind,
        name: &str,
        revision: &str,
        metadata: BTreeMap<String, String>,
        content: Vec<u8>,
    ) -> Result<Self, ResearchError> {
        let artifact = Self {
            schema_version: 1,
            kind,
            name: name.to_owned(),
            revision: revision.to_owned(),
            metadata,
            content,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn id(&self) -> Result<String, ResearchError> {
        self.validate()?;
        Ok(format!("{:x}", Sha256::digest(self.encoded()?)))
    }

    pub fn kind(&self) -> ArtifactKind {
        self.kind
    }

    pub fn content(&self) -> &[u8] {
        &self.content
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn revision(&self) -> &str {
        &self.revision
    }

    pub fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    pub(crate) fn validate(&self) -> Result<(), ResearchError> {
        if self.schema_version != 1 {
            return Err(invalid("unsupported artifact schema version"));
        }
        validate_text(&self.name, 256)?;
        validate_text(&self.revision, 256)?;
        if self.content.is_empty() || self.content.len() > MAX_BYTES || self.metadata.len() > 128 {
            return Err(invalid(
                "content must be nonempty and bounded; at most 128 metadata entries",
            ));
        }
        for (key, value) in &self.metadata {
            validate_text(key, 256)?;
            validate_text(value, 4096)?;
        }
        Ok(())
    }

    pub(crate) fn encoded(&self) -> Result<Vec<u8>, ResearchError> {
        let bytes = serde_json::to_vec(self)?;
        if bytes.len() > MAX_BYTES {
            return Err(invalid("encoded artifact exceeds 64 MiB"));
        }
        Ok(bytes)
    }
}
