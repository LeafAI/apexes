//! Where the supervisor keeps its files, and the validated [`SessionId`]
//! that names them. A session id becomes part of file names, so it is
//! checked before any path is built from it (no `..`, no separators).

use std::path::{Path, PathBuf};

use apexes_core::binance_network::BinanceNetwork;

use crate::error::RequestError;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(String);

impl SessionId {
    pub fn parse(value: &str) -> Result<Self, RequestError> {
        let ok = !value.is_empty()
            && value.len() <= 64
            && !value.starts_with('.')
            && value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
        if ok {
            Ok(Self(value.to_string()))
        } else {
            Err(RequestError::BadSessionId {
                value: value.to_string(),
            })
        }
    }

    /// A fresh id like `testnet-1a2b3c4d`, readable in the console banner.
    pub fn generate(network: BinanceNetwork) -> Self {
        let unique = uuid::Uuid::new_v4().simple().to_string();
        Self(format!("{}-{}", network.as_str(), &unique[..8]))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Layout under the supervisor's root (normally `~/.apexes`). The engine
/// and gateway write their PID files to the same places, so these names are
/// shared contracts, not private choices.
#[derive(Debug, Clone)]
pub struct SupervisorPaths {
    home: PathBuf,
    root: PathBuf,
}

impl SupervisorPaths {
    /// The engine and gateway write to `$HOME/.apexes`, so the supervisor
    /// is built from the same home it gives its children as `HOME`: the
    /// two cannot disagree about where PID files live.
    pub fn from_home(home: impl Into<PathBuf>) -> Self {
        let home = home.into();
        let root = home.join(".apexes");
        Self { home, root }
    }

    pub fn home(&self) -> &Path {
        &self.home
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn sessions_dir(&self) -> PathBuf {
        self.root.join("sessions")
    }

    fn gateway_file(&self, network: BinanceNetwork, extension: &str) -> PathBuf {
        self.root
            .join(format!("market-data-gateway-{network}.{extension}"))
    }

    pub fn gateway_pid(&self, network: BinanceNetwork) -> PathBuf {
        self.gateway_file(network, "pid")
    }
    pub fn gateway_token(&self, network: BinanceNetwork) -> PathBuf {
        self.gateway_file(network, "token")
    }
    pub fn gateway_meta(&self, network: BinanceNetwork) -> PathBuf {
        self.gateway_file(network, "json")
    }
    pub fn gateway_log(&self, network: BinanceNetwork) -> PathBuf {
        self.gateway_file(network, "log")
    }

    fn session_file(&self, id: &SessionId, extension: &str) -> PathBuf {
        self.sessions_dir().join(format!("{id}.{extension}"))
    }

    pub fn session_pid(&self, id: &SessionId) -> PathBuf {
        self.session_file(id, "pid")
    }
    pub fn session_config(&self, id: &SessionId) -> PathBuf {
        self.session_file(id, "json")
    }
    pub fn session_token(&self, id: &SessionId) -> PathBuf {
        self.session_file(id, "token")
    }
    pub fn session_log(&self, id: &SessionId) -> PathBuf {
        self.session_file(id, "log")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_that_could_escape_the_directory_are_rejected() {
        for bad in [
            "",
            "..",
            ".hidden",
            "a/b",
            "a\\b",
            "../etc/passwd",
            "a b",
            "a\0b",
            &"x".repeat(65),
        ] {
            assert!(SessionId::parse(bad).is_err(), "{bad:?} must be rejected");
        }
        for good in ["live-1", "testnet-1a2b3c4d", "a.b_c-d", "X"] {
            assert!(SessionId::parse(good).is_ok(), "{good:?} must be accepted");
        }
    }

    #[test]
    fn generated_ids_are_valid_distinct_and_name_the_network() {
        let a = SessionId::generate(BinanceNetwork::Testnet);
        let b = SessionId::generate(BinanceNetwork::Testnet);
        assert_ne!(a, b);
        assert!(a.as_str().starts_with("testnet-"));
        assert!(SessionId::parse(a.as_str()).is_ok());
        assert!(
            SessionId::generate(BinanceNetwork::Mainnet)
                .as_str()
                .starts_with("mainnet-")
        );
    }

    #[test]
    fn paths_match_the_files_the_binaries_write() {
        let paths = SupervisorPaths::from_home("/h");
        let id = SessionId::parse("s-1").unwrap();
        assert_eq!(
            paths.session_pid(&id),
            PathBuf::from("/h/.apexes/sessions/s-1.pid")
        );
        assert_eq!(
            paths.gateway_pid(BinanceNetwork::Testnet),
            PathBuf::from("/h/.apexes/market-data-gateway-testnet.pid")
        );
        assert_ne!(
            paths.gateway_pid(BinanceNetwork::Testnet),
            paths.gateway_pid(BinanceNetwork::Mainnet),
            "networks must not share a PID file"
        );
    }
}
