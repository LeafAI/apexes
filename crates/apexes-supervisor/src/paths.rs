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

/// A gateway slot is part of file names, so it is restricted to a safe set.
pub fn valid_slot(slot: &str) -> bool {
    !slot.is_empty() && slot.len() <= 16 && slot.chars().all(|c| c.is_ascii_alphanumeric())
}

/// Which gateway process. A network can have several, each streaming its own
/// instruments; `slot: None` is the single gateway older builds kept in flat
/// files, which is still found (and stopped) but never started again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayKey {
    pub network: BinanceNetwork,
    pub slot: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayFiles {
    pub pid: PathBuf,
    pub meta: PathBuf,
    pub token: PathBuf,
    pub log: PathBuf,
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

    /// Where gateways started by this supervisor keep their files.
    pub fn gateways_dir(&self) -> PathBuf {
        self.root.join("gateways")
    }

    /// The files of one gateway: its PID file, the supervisor's record of
    /// what it streams, its bearer token and its log.
    pub fn gateway_files(&self, key: &GatewayKey) -> GatewayFiles {
        let file = |extension: &str| match &key.slot {
            // Older builds ran one gateway per network, in flat files.
            None => self
                .root
                .join(format!("market-data-gateway-{}.{extension}", key.network)),
            Some(slot) => self
                .gateways_dir()
                .join(format!("{}-{slot}.{extension}", key.network)),
        };
        GatewayFiles {
            pid: file("pid"),
            meta: file("json"),
            token: file("token"),
            log: file("log"),
        }
    }

    /// Every gateway of `network` that has a record on disk, running or not:
    /// the one an older build left, then those in `gateways/`, by slot.
    pub fn gateway_keys(&self, network: BinanceNetwork) -> Vec<GatewayKey> {
        let mut keys = vec![GatewayKey {
            network,
            slot: None,
        }];
        let prefix = format!("{network}-");
        let mut slots: Vec<String> = std::fs::read_dir(self.gateways_dir())
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                let slot = name.strip_prefix(&prefix)?.strip_suffix(".json")?;
                valid_slot(slot).then(|| slot.to_string())
            })
            .collect();
        slots.sort();
        keys.extend(slots.into_iter().map(|slot| GatewayKey {
            network,
            slot: Some(slot),
        }));
        keys
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
        let key = |network, slot: Option<&str>| GatewayKey {
            network,
            slot: slot.map(str::to_string),
        };
        assert_eq!(
            paths.gateway_files(&key(BinanceNetwork::Testnet, None)).pid,
            PathBuf::from("/h/.apexes/market-data-gateway-testnet.pid"),
            "the file an older build's gateway wrote"
        );
        assert_eq!(
            paths
                .gateway_files(&key(BinanceNetwork::Testnet, Some("ab12")))
                .pid,
            PathBuf::from("/h/.apexes/gateways/testnet-ab12.pid")
        );
        assert_ne!(
            paths
                .gateway_files(&key(BinanceNetwork::Testnet, Some("ab12")))
                .pid,
            paths
                .gateway_files(&key(BinanceNetwork::Mainnet, Some("ab12")))
                .pid,
            "networks must not share a PID file"
        );
        assert_ne!(
            paths
                .gateway_files(&key(BinanceNetwork::Testnet, Some("a")))
                .pid,
            paths
                .gateway_files(&key(BinanceNetwork::Testnet, Some("b")))
                .pid,
            "gateways on one network must not share a PID file"
        );
    }

    #[test]
    fn slots_are_filename_safe() {
        assert!(valid_slot("ab12CD"));
        for bad in ["", "a/b", "../x", "a.b", "a b", "ÿ", &"x".repeat(17)] {
            assert!(!valid_slot(bad), "{bad:?}");
        }
    }

    #[test]
    fn keys_list_the_legacy_gateway_first_then_slots_in_order_ignoring_strays() {
        let home =
            std::env::temp_dir().join(format!("apexes-paths-{}", uuid::Uuid::new_v4().simple()));
        let paths = SupervisorPaths::from_home(&home);
        std::fs::create_dir_all(paths.gateways_dir()).unwrap();
        for name in [
            "testnet-bb.json",
            "testnet-aa.json",
            "mainnet-cc.json",
            "testnet-bad.slot.json",
            "testnet-aa.pid",
            "testnet-.json",
            "notes.json",
        ] {
            std::fs::write(paths.gateways_dir().join(name), "{}").unwrap();
        }
        let slots: Vec<Option<String>> = paths
            .gateway_keys(BinanceNetwork::Testnet)
            .into_iter()
            .map(|k| k.slot)
            .collect();
        assert_eq!(
            slots,
            vec![None, Some("aa".to_string()), Some("bb".to_string())]
        );
        std::fs::remove_dir_all(&home).ok();
    }
}
