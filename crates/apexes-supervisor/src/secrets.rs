//! Tokens and private files. Secrets only ever travel by environment
//! variable and `0600` file -- never on a command line, never in a launch
//! config.

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

use crate::error::SupervisorError;

/// 64 hex characters from the operating system's random source (two v4
/// UUIDs, each 122 random bits); well over the engine's 16-character
/// minimum.
pub fn generate_token() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

fn io_error(action: &'static str, path: &Path, source: std::io::Error) -> SupervisorError {
    SupervisorError::Io {
        action,
        path: path.to_path_buf(),
        source,
    }
}

/// Writes `contents` to `path`, readable and writable by the owner only,
/// creating parent directories. Re-tightens the mode if the file existed.
pub fn write_private(path: &Path, contents: &str) -> Result<(), SupervisorError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| io_error("create directory", parent, e))?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| io_error("write", path, e))?;
    file.write_all(contents.as_bytes())
        .map_err(|e| io_error("write", path, e))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| io_error("restrict permissions of", path, e))
}

/// A private log file opened for appending (children write their output
/// here, so it must not be world-readable).
pub fn open_private_log(path: &Path) -> Result<std::fs::File, SupervisorError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| io_error("create directory", parent, e))?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| io_error("open log", path, e))
}

pub fn read_trimmed(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "apexes-sup-{name}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn tokens_are_long_hex_and_distinct() {
        let a = generate_token();
        let b = generate_token();
        assert_ne!(a, b);
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(a.len() >= apexes_security::MIN_TOKEN_LEN);
    }

    #[test]
    fn private_files_are_owner_only_even_if_they_existed_wider() {
        let dir = temp_dir("private");
        let path = dir.join("nested/a.token");
        write_private(&path, "secret\n").unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        // Widen it, then write again: the mode must be tightened back.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_private(&path, "secret2\n").unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(read_trimmed(&path).as_deref(), Some("secret2"));
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn logs_are_private_and_append() {
        let dir = temp_dir("log");
        let path = dir.join("x.log");
        {
            let mut f = open_private_log(&path).unwrap();
            writeln!(f, "one").unwrap();
        }
        {
            let mut f = open_private_log(&path).unwrap();
            writeln!(f, "two").unwrap();
        }
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "one\ntwo\n");
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn reading_a_missing_or_empty_file_is_none() {
        let dir = temp_dir("read");
        assert_eq!(read_trimmed(&dir.join("nope")), None);
        std::fs::write(dir.join("empty"), "  \n").unwrap();
        assert_eq!(read_trimmed(&dir.join("empty")), None);
        std::fs::remove_dir_all(dir).ok();
    }
}
