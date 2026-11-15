//! The shared market-data gateway: one per Binance network, reused by every
//! session on that network.

use apexes_core::binance_network::BinanceNetwork;
use serde::{Deserialize, Serialize};

use crate::error::SupervisorError;
use crate::pidfile;
use crate::process::{self, child_env, command_line, free_loopback_port, reap, spawn_detached};
use crate::secrets::{generate_token, read_trimmed, write_private};
use crate::supervisor::{GATEWAY_BINARY, Supervisor};

/// What the supervisor recorded when it started a gateway: the instruments
/// it streams (which a later session cannot add to) and its port.
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

impl Supervisor {
    fn running_gateway(&self, network: BinanceNetwork) -> Option<pidfile::PidFile> {
        pidfile::read(&self.paths.gateway_pid(network)).filter(|file| pidfile::is_serving(*file))
    }

    /// Returns a gateway for `network` that streams every symbol in
    /// `symbols`, starting one if none is running.
    ///
    /// A gateway already running keeps its instrument set (sessions are
    /// attached to it), so a request it cannot serve is refused with an
    /// explanation rather than silently restarting the gateway underneath
    /// other sessions.
    pub fn ensure_gateway(
        &self,
        network: BinanceNetwork,
        symbols: &[String],
    ) -> Result<GatewayHandle, SupervisorError> {
        let requested = sorted(symbols);
        if let Some(file) = self.running_gateway(network) {
            let meta: Option<GatewayMeta> =
                std::fs::read_to_string(self.paths.gateway_meta(network))
                    .ok()
                    .and_then(|text| serde_json::from_str(&text).ok());
            let Some(meta) = meta else {
                return Err(SupervisorError::GatewayUnknownInstruments { network });
            };
            if !covers(&meta.instruments, &requested) {
                return Err(SupervisorError::GatewayInstrumentMismatch {
                    network,
                    running: meta.instruments,
                    requested,
                });
            }
            return Ok(GatewayHandle {
                network,
                port: file.port,
                token: read_trimmed(&self.paths.gateway_token(network)),
                started: false,
            });
        }
        self.start_gateway(network, requested)
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
        let mut last = None;
        for attempt in 1..=ATTEMPTS {
            match self.start_gateway_once(network, instruments.clone()) {
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
        network: BinanceNetwork,
        instruments: Vec<String>,
    ) -> Result<GatewayHandle, SupervisorError> {
        let binary = self.resolve_binary(&self.gateway_binary, GATEWAY_BINARY)?;
        let port = free_loopback_port().map_err(|source| SupervisorError::Io {
            action: "find a free port for",
            path: binary.clone(),
            source,
        })?;
        let token = generate_token();
        write_private(&self.paths.gateway_token(network), &token)?;
        let meta = GatewayMeta {
            instruments: instruments.clone(),
            port,
        };
        let meta_json =
            serde_json::to_string_pretty(&meta).map_err(|error| SupervisorError::Io {
                action: "encode",
                path: self.paths.gateway_meta(network),
                source: std::io::Error::other(error),
            })?;
        write_private(&self.paths.gateway_meta(network), &meta_json)?;

        let args = vec![
            "--host".to_string(),
            "127.0.0.1".to_string(),
            "--port".to_string(),
            port.to_string(),
            "--instruments".to_string(),
            instruments.join(","),
            "--network".to_string(),
            network.as_str().to_string(),
        ];
        let env = child_env(
            &self.env,
            self.paths.home(),
            &[("APEXES_GATEWAY_TOKEN".to_string(), token.clone())],
        );
        let log = self.paths.gateway_log(network);
        let what = format!("the {network} market-data gateway");
        let mut child = spawn_detached(&binary, &args, &env, &log)?;
        match self.wait_until_serving(&mut child, &self.paths.gateway_pid(network), &log, &what) {
            Ok(file) => {
                reap(child);
                Ok(GatewayHandle {
                    network,
                    port: file.port,
                    token: Some(token),
                    started: true,
                })
            }
            Err(error) => {
                // A gateway that never came up leaves nothing for the next
                // attempt to trip over; the token is a secret, so it goes too.
                let _ = child.kill();
                let _ = child.wait();
                let _ = std::fs::remove_file(self.paths.gateway_token(network));
                let _ = std::fs::remove_file(self.paths.gateway_meta(network));
                Err(error)
            }
        }
    }

    /// Stops the gateway for `network` (SIGTERM), refusing while any
    /// session of that network is still running -- they would lose their
    /// market data -- and refusing a PID that is not the gateway.
    pub fn stop_gateway(&self, network: BinanceNetwork) -> Result<(), SupervisorError> {
        let Some(file) = self.running_gateway(network) else {
            return Ok(());
        };
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
        self.signal_if_ours(file.pid, &name, &[])
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

    #[test]
    fn instrument_lists_are_sorted_and_deduplicated() {
        assert_eq!(
            sorted(&s(&["ETHUSDT", "BTCUSDT", "ETHUSDT"])),
            s(&["BTCUSDT", "ETHUSDT"])
        );
    }
}
