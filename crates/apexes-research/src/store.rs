use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::artifact::MAX_BYTES;
use crate::{Artifact, ResearchError, invalid, validate_id};

/// Local content-addressed store. The root must be owned by a trusted user.
/// Publication uses a same-directory hard link (atomic, no replacement).
/// Concurrent publishers of the same artifact are safe. Symlink artifacts are
/// rejected; hostile concurrent filesystem mutation is outside this API's scope.
#[derive(Debug, Clone)]
pub struct ResearchStore {
    root: PathBuf,
}

impl ResearchStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, ResearchError> {
        fs::create_dir_all(root.as_ref()).map_err(|e| io("create root", e))?;
        let root = fs::canonicalize(root.as_ref()).map_err(|e| io("resolve root", e))?;
        if !root.is_dir() {
            return Err(invalid("store root must be a directory"));
        }
        Ok(Self { root })
    }

    pub fn get(&self, id: &str) -> Result<Artifact, ResearchError> {
        let path = self.path(id)?;
        let metadata = fs::symlink_metadata(&path).map_err(|e| io("inspect artifact", e))?;
        if !metadata.is_file() || metadata.len() > MAX_BYTES as u64 {
            return Err(invalid(
                "artifact must be a regular file no larger than 64 MiB",
            ));
        }
        let file = File::open(path).map_err(|e| io("open artifact", e))?;
        let mut bytes = Vec::new();
        file.take(MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| io("read artifact", e))?;
        if bytes.len() > MAX_BYTES {
            return Err(invalid("artifact exceeds 64 MiB"));
        }
        let artifact: Artifact = serde_json::from_slice(&bytes)?;
        if artifact.id()? != id {
            return Err(ResearchError::Integrity);
        }
        Ok(artifact)
    }

    pub fn put(&self, artifact: &Artifact) -> Result<String, ResearchError> {
        let id = artifact.id()?;
        let destination = self.path(&id)?;
        let temporary = self.root.join(format!(".pending-{}", uuid::Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|e| io("create temporary artifact", e))?;
        let published = self.publish(&mut file, &temporary, &destination, artifact, &id);
        let cleanup = fs::remove_file(&temporary).map_err(|e| io("remove temporary artifact", e));
        published?;
        cleanup?;
        Ok(id)
    }

    fn publish(
        &self,
        file: &mut File,
        temporary: &Path,
        destination: &Path,
        artifact: &Artifact,
        id: &str,
    ) -> Result<(), ResearchError> {
        file.write_all(&artifact.encoded()?)
            .map_err(|e| io("write artifact", e))?;
        file.sync_all().map_err(|e| io("sync artifact", e))?;
        match fs::hard_link(temporary, destination) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if self.get(id)? != *artifact {
                    return Err(ResearchError::Integrity);
                }
            }
            Err(e) => return Err(io("publish artifact", e)),
        }
        File::open(&self.root)
            .and_then(|directory| directory.sync_all())
            .map_err(|e| io("sync store directory", e))
    }

    fn path(&self, id: &str) -> Result<PathBuf, ResearchError> {
        validate_id(id)?;
        Ok(self.root.join(format!("{id}.json")))
    }
}

fn io(operation: &'static str, source: std::io::Error) -> ResearchError {
    ResearchError::Io { operation, source }
}
