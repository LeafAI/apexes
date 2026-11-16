//! Market-data gateways: any number per Binance network, each streaming its
//! own instruments, reused by every session they cover.
//!
//! A gateway's instrument set is fixed when it starts, and sessions attach
//! to it. Asking for a symbol no running gateway carries therefore starts
//! another gateway for it; it never restarts one underneath running
//! sessions, and never refuses. Sessions only receive their own symbols (the
//! engine filters), so sharing a gateway that carries more is safe.

use apexes_core::binance_network::BinanceNetwork;
use serde::{Deserialize, Serialize};

use crate::error::SupervisorError;
use crate::paths::{GatewayFiles, GatewayKey};
use crate::pidfile;
use crate::process::{self, child_env, command_line, free_loopback_port, reap, spawn_detached};
use crate::secrets::{generate_token, read_trimmed, write_private};
use crate::supervisor::{GATEWAY_BINARY, Supervisor};

/// How long a dead gateway's leftover files are kept before they are tidied
/// away. Long enough that a gateway another supervisor is still starting is
/// never mistaken for a dead one.
const DEAD_GATEWAY_GRACE: std::time::Duration = std::time::Duration::from_secs(600);

/// What the supervisor recorded when it started a gateway: the instruments
/// it streams and its port.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayMeta {
    pub instruments: Vec<String>,
    pub port: u16,
}

/// A running gateway a session can use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayHandle {
    pub network: BinanceNetwork,
    pub port: u16,
    /// Bearer token the gateway requires, when the supervisor started it.
    pub token: Option<String>,
    /// True if this call started it; false if one was already running.
    pub started: bool,
    /// The symbols it streams.
    pub instruments: Vec<String>,
}

/// A gateway that is up, with what is known about it.
#[derive(Debug, Clone)]
pub(crate) struct RunningGateway {
    pub key: GatewayKey,
    pub files: GatewayFiles,
    pub pid: pidfile::PidFile,
    /// `None` for one this supervisor has no record of.
    pub instruments: Option<Vec<String>>,
}

/// True when a child's log says it could not bind its port because something
/// else already holds it.
fn lost_port_race(log_tail: &str) -> bool {
    let log = log_tail.to_lowercase();
    log.contains("address already in use") || log.contains("failed to bind")
}

fn covers(running: &[String], requested: &[String]) -> bool {
    requested.iter().all(|symbol| running.contains(symbol))
}

fn sorted(symbols: &[String]) -> Vec<String> {
    let mut symbols = symbols.to_vec();
    symbols.sort();
    symbols.dedup();
    symbols
}

/// The running gateway to reuse for `requested`: among those that carry every
/// requested symbol, the one carrying the fewest others (so unrelated
/// sessions are not piled onto one big feed), ties broken by position.
/// `None` when none covers it, and a gateway of unknown contents never does.
pub(crate) fn pick_reusable<'a>(
    running: &'a [Vec<String>],
    requested: &[String],
) -> Option<&'a Vec<String>> {
    running
        .iter()
        .filter(|instruments| covers(instruments, requested))
        .min_by_key(|instruments| instruments.len())
}

fn new_slot() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..8].to_string()
}

impl Supervisor {
    fn read_meta(&self, files: &GatewayFiles) -> Option<GatewayMeta> {
        std::fs::read_to_string(&files.meta)
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
    }

    /// Every gateway of `network` that is up right now.
    pub(crate) fn running_gateways(&self, network: BinanceNetwork) -> Vec<RunningGateway> {
        self.paths
            .gateway_keys(network)
            .into_iter()
            .filter_map(|key| {
                let files = self.paths.gateway_files(&key);
                let pid = pidfile::read(&files.pid).filter(|file| pidfile::is_serving(*file))?;
                let instruments = self.read_meta(&files).map(|meta| meta.instruments);
                Some(RunningGateway {
                    key,
                    files,
                    pid,
                    instruments,
                })
            })
            .collect()
    }

    /// Returns a gateway for `network` that streams every symbol in
    /// `symbols`: a running one that covers them, otherwise a new one for
    /// exactly those symbols. Existing gateways, and the sessions attached
    /// to them, are never touched.
    pub fn ensure_gateway(
        &self,
        network: BinanceNetwork,
        symbols: &[String],
    ) -> Result<GatewayHandle, SupervisorError> {
        let requested = sorted(symbols);
        let running = self.running_gateways(network);
        let known: Vec<Vec<String>> = running
            .iter()
            .filter_map(|g| g.instruments.clone())
            .collect();
        if let Some(chosen) = pick_reusable(&known, &requested) {
            let gateway = running
                .iter()
                .find(|g| g.instruments.as_ref() == Some(chosen))
                .ok_or_else(|| SupervisorError::Io {
                    action: "find",
                    path: self.paths.gateways_dir(),
                    source: std::io::Error::other("the chosen gateway vanished"),
                })?;
            return Ok(GatewayHandle {
                network,
                port: gateway.pid.port,
                token: read_trimmed(&gateway.files.token),
                started: false,
                instruments: chosen.clone(),
            });
        }
        self.prune_dead_gateways(network);
        self.start_gateway(network, requested)
    }

    /// Removes the files of gateways that are no longer running and have been
    /// idle longer than [`DEAD_GATEWAY_GRACE`], so a long-lived install does
    /// not collect a token and a log for every gateway it ever started.
    fn prune_dead_gateways(&self, network: BinanceNetwork) {
        let running: Vec<GatewayKey> = self
            .running_gateways(network)
            .into_iter()
            .map(|g| g.key)
            .collect();
        for key in self.paths.gateway_keys(network) {
            if key.slot.is_none() || running.contains(&key) {
                continue;
            }
            let files = self.paths.gateway_files(&key);
            let idle = std::fs::metadata(&files.meta)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|at| at.elapsed().ok());
            if idle.is_some_and(|idle| idle > DEAD_GATEWAY_GRACE) {
                for path in [&files.pid, &files.meta, &files.token, &files.log] {
                    let _ = std::fs::remove_file(path);
                }
            }
        }
    }

    /// Starts a gateway, retrying when it loses the race for its port.
    ///
    /// The supervisor picks a free port and hands it to the child, but
    /// between releasing it and the child binding it another process can take
    /// it (this happens under load). The child then exits at once with a bind
    /// error, which is recognisable and safe to retry on a fresh port: nothing
    /// was started. Any other startup failure is reported as it is.
    fn start_gateway(
        &self,
        network: BinanceNetwork,
        instruments: Vec<String>,
    ) -> Result<GatewayHandle, SupervisorError> {
        const ATTEMPTS: usize = 4;
        let key = GatewayKey {
            network,
            slot: Some(new_slot()),
        };
        let mut last = None;
        for attempt in 1..=ATTEMPTS {
            match self.start_gateway_once(&key, instruments.clone()) {
                Err(SupervisorError::ExitedDuringStartup { log_tail, .. })
                    if lost_port_race(&log_tail) && attempt < ATTEMPTS =>
                {
                    tracing::warn!(attempt, "gateway lost the race for its port; retrying");
                    last = Some(log_tail);
                }
                other => return other,
            }
        }
        // Unreachable in practice: the last attempt always returns above.
        Err(SupervisorError::StartupTimeout {
            what: format!("the {network} market-data gateway"),
            seconds: 0,
            log_tail: last.unwrap_or_default(),
        })
    }

    fn start_gateway_once(
        &self,
        key: &GatewayKey,
        instruments: Vec<String>,
    ) -> Result<GatewayHandle, SupervisorError> {
        let network = key.network;
        let files = self.paths.gateway_files(key);
        let binary = self.resolve_binary(&self.gateway_binary, GATEWAY_BINARY)?;
        let port = free_loopback_port().map_err(|source| SupervisorError::Io {
            action: "find a free port for",
            path: binary.clone(),
            source,
        })?;
        let token = generate_token();
        write_private(&files.token, &token)?;
        let meta = GatewayMeta {
            instruments: instruments.clone(),
            port,
        };
        let meta_json =
            serde_json::to_string_pretty(&meta).map_err(|error| SupervisorError::Io {
                action: "encode",
                path: files.meta.clone(),
                source: std::io::Error::other(error),
            })?;
        write_private(&files.meta, &meta_json)?;

        let args = vec![
            "--host".to_string(),
            "127.0.0.1".to_string(),
            "--port".to_string(),
            port.to_string(),
            "--instruments".to_string(),
            instruments.join(","),
            "--network".to_string(),
            network.as_str().to_string(),
            "--pid-file".to_string(),
            files.pid.to_string_lossy().into_owned(),
        ];
        let env = child_env(
            &self.env,
            self.paths.home(),
            &[("APEXES_GATEWAY_TOKEN".to_string(), token.clone())],
        );
        let what = format!(
            "the {network} market-data gateway for {}",
            instruments.join(",")
        );
        let mut child = spawn_detached(&binary, &args, &env, &files.log)?;
        match self.wait_until_serving(&mut child, &files.pid, &files.log, &what) {
            Ok(file) => {
                reap(child);
                Ok(GatewayHandle {
                    network,
                    port: file.port,
                    token: Some(token),
                    started: true,
                    instruments,
                })
            }
            Err(error) => {
                // A gateway that never came up leaves nothing for the next
                // attempt to trip over; the token is a secret, so it goes too.
                let _ = child.kill();
                let _ = child.wait();
                let _ = std::fs::remove_file(&files.token);
                let _ = std::fs::remove_file(&files.meta);
                let _ = std::fs::remove_file(&files.pid);
                Err(error)
            }
        }
    }

    /// Stops every gateway of `network` (SIGTERM), refusing while any session
    /// of that network is still running -- they would lose their market data
    /// -- and refusing a PID that is not that gateway. Stops as many as it
    /// can and reports the first problem.
    pub fn stop_gateway(&self, network: BinanceNetwork) -> Result<(), SupervisorError> {
        let running = self.running_gateways(network);
        if running.is_empty() {
            return Ok(());
        }
        let in_use: Vec<String> = self
            .discover()
            .into_iter()
            .filter(|s| s.alive && s.network == Some(network))
            .map(|s| s.id.to_string())
            .collect();
        if !in_use.is_empty() {
            return Err(SupervisorError::GatewayInUse {
                network,
                sessions: in_use,
            });
        }
        let name = self.gateway_name.clone();
        let mut first_error = None;
        for gateway in running {
            // A gateway given its own PID file is identified by that path on
            // its command line, so one gateway's PID can never be mistaken
            // for another's.
            let pid_path = gateway.files.pid.to_string_lossy().into_owned();
            let must_contain: Vec<&str> = match gateway.key.slot {
                Some(_) => vec![pid_path.as_str()],
                None => Vec::new(),
            };
            if let Err(error) = self.signal_if_ours(gateway.pid.pid, &name, &must_contain) {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) fn signal_if_ours(
        &self,
        pid: u32,
        binary_name: &str,
        must_contain: &[&str],
    ) -> Result<(), SupervisorError> {
        if pid <= 1 || pid == std::process::id() {
            return Err(SupervisorError::UnsafeToSignal {
                pid,
                reason: "not a valid target".to_string(),
            });
        }
        let Some(line) = command_line(pid) else {
            return Err(SupervisorError::UnsafeToSignal {
                pid,
                reason: "its command line could not be read".to_string(),
            });
        };
        if !line.contains(binary_name) || must_contain.iter().any(|needle| !line.contains(needle)) {
            return Err(SupervisorError::UnsafeToSignal {
                pid,
                reason: format!(
                    "it is not {binary_name} for this session (it is running {line:?})"
                ),
            });
        }
        process::terminate(pid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|i| i.to_string()).collect()
    }

    #[test]
    fn a_bind_failure_is_recognised_as_a_lost_port_race_and_nothing_else_is() {
        assert!(lost_port_race(
            "apexes-market-data-gateway: failed to bind 127.0.0.1:5000: Address already in use (os error 48)"
        ));
        assert!(lost_port_race("ADDRESS ALREADY IN USE"));
        assert!(!lost_port_race(
            "failed to connect to upstream market data at startup"
        ));
        assert!(!lost_port_race(""));
    }

    #[test]
    fn coverage_is_subset_semantics() {
        assert!(covers(&s(&["BTCUSDT", "ETHUSDT"]), &s(&["BTCUSDT"])));
        assert!(covers(&s(&["BTCUSDT"]), &s(&["BTCUSDT"])));
        assert!(!covers(&s(&["BTCUSDT"]), &s(&["BTCUSDT", "ETHUSDT"])));
        assert!(covers(&s(&["BTCUSDT"]), &[]));
    }

    fn set(items: &[&str]) -> Vec<String> {
        s(items)
    }

    #[test]
    fn a_covering_gateway_is_reused_and_the_smallest_one_wins() {
        let running = vec![
            set(&["BTCUSDT", "ETHUSDT", "SOLUSDT"]),
            set(&["ETHUSDT"]),
            set(&["BTCUSDT", "ETHUSDT"]),
        ];
        assert_eq!(
            pick_reusable(&running, &set(&["ETHUSDT"])),
            Some(&set(&["ETHUSDT"])),
            "exact match beats a bigger feed"
        );
        assert_eq!(
            pick_reusable(&running, &set(&["BTCUSDT", "ETHUSDT"])),
            Some(&set(&["BTCUSDT", "ETHUSDT"]))
        );
        assert_eq!(
            pick_reusable(&running, &set(&["BTCUSDT"])),
            Some(&set(&["BTCUSDT", "ETHUSDT"])),
            "the smallest feed that still carries it"
        );
    }

    #[test]
    fn nothing_is_reused_when_no_gateway_carries_every_symbol() {
        let running = vec![set(&["BTCUSDT"]), set(&["ETHUSDT"])];
        assert_eq!(
            pick_reusable(&running, &set(&["BTCUSDT", "ETHUSDT"])),
            None,
            "two feeds do not add up to one that carries both"
        );
        assert_eq!(pick_reusable(&running, &set(&["SOLUSDT"])), None);
        assert_eq!(pick_reusable(&[], &set(&["BTCUSDT"])), None);
    }

    #[test]
    fn slots_are_short_unique_and_filename_safe() {
        let a = new_slot();
        let b = new_slot();
        assert_ne!(a, b);
        assert!(crate::paths::valid_slot(&a), "{a}");
    }

    #[test]
    fn instrument_lists_are_sorted_and_deduplicated() {
        assert_eq!(
            sorted(&s(&["ETHUSDT", "BTCUSDT", "ETHUSDT"])),
            s(&["BTCUSDT", "ETHUSDT"])
        );
    }
}
