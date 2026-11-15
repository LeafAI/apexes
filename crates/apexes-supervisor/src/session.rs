//! Launching, discovering and stopping engine sessions.

use std::path::PathBuf;

use apexes_core::binance_network::BinanceNetwork;
use apexes_engine::credential_var_names;
use apexes_engine::launch_config::{LaunchConfig, SessionMode};

use crate::error::SupervisorError;
use crate::gateway::GatewayHandle;
use crate::paths::SessionId;
use crate::pidfile;
use crate::process::{child_env, reap, spawn_detached};
use crate::request::{LaunchRequest, ValidRequest};
use crate::secrets::{generate_token, read_trimmed, write_private};
use crate::supervisor::{CONTROL_TOKEN_NAME, ENGINE_BINARY, Supervisor};

/// A session the supervisor started or found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInfo {
    pub id: SessionId,
    pub pid: u32,
    pub port: u16,
    /// `Some` when the session's launch config could be read.
    pub network: Option<BinanceNetwork>,
    /// PID exists and its port accepts connections.
    pub alive: bool,
    /// `ws://127.0.0.1:<port>/session/ws`.
    pub control_url: String,
    /// Bearer token for the control endpoint, from the session's `0600`
    /// token file. Absent for a session this supervisor did not create.
    pub token: Option<String>,
    pub log_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchedSession {
    pub info: SessionInfo,
    pub gateway: GatewayHandle,
}

fn control_url(port: u16) -> String {
    format!("ws://127.0.0.1:{port}/session/ws")
}

impl Supervisor {
    /// The two variables a session needs, checked in the supervisor's own
    /// environment; returns their values.
    fn credentials_for(
        &self,
        credentials_ref: &str,
        network: BinanceNetwork,
    ) -> Result<Vec<(String, String)>, SupervisorError> {
        let (key_var, secret_var) = credential_var_names(credentials_ref, network);
        let mut found = Vec::new();
        for var in [key_var, secret_var] {
            let value = (self.env)(&var).ok_or_else(|| SupervisorError::MissingCredentials {
                credentials_ref: credentials_ref.to_string(),
                network,
                missing_var: var.clone(),
            })?;
            found.push((var, value));
        }
        Ok(found)
    }

    /// The first credential variable `credentials_ref` still needs on
    /// `network`, or `None` when both are set. Lets a launcher warn before
    /// the operator presses go.
    pub fn missing_credential(
        &self,
        credentials_ref: &str,
        network: BinanceNetwork,
    ) -> Option<String> {
        match self.credentials_for(credentials_ref, network) {
            Ok(_) => None,
            Err(SupervisorError::MissingCredentials { missing_var, .. }) => Some(missing_var),
            Err(_) => None,
        }
    }

    /// Checks everything that can be checked without starting a process:
    /// the request, the credentials, and both binaries. Exposed so a
    /// launcher can show the problem before the operator presses go.
    pub fn preflight(&self, request: LaunchRequest) -> Result<ValidRequest, SupervisorError> {
        let valid = request.validate()?;
        self.credentials_for(&valid.request.credentials_ref, valid.request.network)?;
        self.resolve_binary(&self.gateway_binary, crate::supervisor::GATEWAY_BINARY)?;
        self.resolve_binary(&self.engine_binary, ENGINE_BINARY)?;
        if self.session_alive(&valid.session_id) {
            return Err(SupervisorError::SessionAlreadyRunning {
                session_id: valid.session_id.to_string(),
            });
        }
        Ok(valid)
    }

    fn session_alive(&self, id: &SessionId) -> bool {
        pidfile::read(&self.paths.session_pid(id)).is_some_and(pidfile::is_serving)
    }

    /// Starts (or reuses) the gateway, then an engine, and returns once the
    /// engine is serving and has survived its settle period.
    pub fn launch(&self, request: LaunchRequest) -> Result<LaunchedSession, SupervisorError> {
        let valid = self.preflight(request)?;
        let request = &valid.request;
        let network = request.network;
        let id = valid.session_id.clone();

        let credentials = self.credentials_for(&request.credentials_ref, network)?;
        let gateway = self.ensure_gateway(network, &request.symbols)?;

        let config = valid
            .launch_config(gateway.port)
            .map_err(SupervisorError::InvalidRequest)?;
        let config_json =
            serde_json::to_string_pretty(&config).map_err(|error| SupervisorError::Io {
                action: "encode",
                path: self.paths.session_config(&id),
                source: std::io::Error::other(error),
            })?;
        let config_path = self.paths.session_config(&id);
        write_private(&config_path, &config_json)?;

        let token = generate_token();
        write_private(&self.paths.session_token(&id), &token)?;
        // A stale PID file from an earlier run of this id must not be
        // mistaken for the new engine's.
        let _ = std::fs::remove_file(self.paths.session_pid(&id));

        let mut extras = credentials;
        extras.push((
            "APEXES_CONTROL_TOKENS".to_string(),
            format!("{CONTROL_TOKEN_NAME}:operator:{token}"),
        ));
        if let Some(gateway_token) = &gateway.token {
            extras.push(("APEXES_GATEWAY_TOKEN".to_string(), gateway_token.clone()));
        }
        extras.push((
            "APEXES_AUDIT_LOG".to_string(),
            self.paths
                .sessions_dir()
                .join(format!("{id}.audit.jsonl"))
                .to_string_lossy()
                .into_owned(),
        ));
        let env = child_env(&self.env, self.paths.home(), &extras);

        let binary = self.resolve_binary(&self.engine_binary, ENGINE_BINARY)?;
        let args = vec![
            "--launch-config".to_string(),
            config_path.to_string_lossy().into_owned(),
        ];
        let log = self.paths.session_log(&id);
        let what = format!("engine session {id}");
        let mut child = spawn_detached(&binary, &args, &env, &log)?;

        let outcome = self
            .wait_until_serving(&mut child, &self.paths.session_pid(&id), &log, &what)
            .and_then(|file| {
                self.watch_settle(&mut child, &log, &what)?;
                Ok(file)
            });
        match outcome {
            Ok(file) => {
                reap(child);
                Ok(LaunchedSession {
                    info: SessionInfo {
                        id: id.clone(),
                        pid: file.pid,
                        port: file.port,
                        network: Some(network),
                        alive: true,
                        control_url: control_url(file.port),
                        token: Some(token),
                        log_path: log,
                    },
                    gateway,
                })
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = std::fs::remove_file(self.paths.session_token(&id));
                Err(error)
            }
        }
    }

    /// Every session with a PID file, oldest first by id. Reads files only;
    /// never starts or signals anything.
    pub fn discover(&self) -> Vec<SessionInfo> {
        let Ok(entries) = std::fs::read_dir(self.paths.sessions_dir()) else {
            return Vec::new();
        };
        let mut sessions: Vec<SessionInfo> = entries
            .flatten()
            .filter_map(|entry| {
                let path = entry.path();
                if path.extension()? != "pid" {
                    return None;
                }
                let id = SessionId::parse(path.file_stem()?.to_str()?).ok()?;
                let file = pidfile::read(&path)?;
                let network = std::fs::read_to_string(self.paths.session_config(&id))
                    .ok()
                    .and_then(|text| serde_json::from_str::<LaunchConfig>(&text).ok())
                    .and_then(|config| match config.mode {
                        SessionMode::Live { network, .. } => Some(network),
                        _ => None,
                    });
                Some(SessionInfo {
                    pid: file.pid,
                    port: file.port,
                    network,
                    alive: pidfile::is_serving(file),
                    control_url: control_url(file.port),
                    token: read_trimmed(&self.paths.session_token(&id)),
                    log_path: self.paths.session_log(&id),
                    id,
                })
            })
            .collect();
        sessions.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
        sessions
    }

    /// Asks a session to stop through its own control endpoint (the
    /// engine's `Stop` command, which unwinds the trading loop cleanly),
    /// then verifies it exited. Returns once the process is gone, or
    /// `StopTimedOut` if it is still there after `wait`.
    ///
    /// No signal is sent unless `force_after_timeout` is set, and then only
    /// SIGTERM to a PID verified to be this session's engine. Never SIGKILL:
    /// a live session killed hard can leave orders on the exchange.
    pub fn request_stop(
        &self,
        id: &SessionId,
        wait: std::time::Duration,
        force_after_timeout: bool,
    ) -> Result<(), SupervisorError> {
        let info = self
            .discover()
            .into_iter()
            .find(|s| &s.id == id && s.alive)
            .ok_or_else(|| SupervisorError::NoSuchSession {
                session_id: id.to_string(),
            })?;
        let token = info.token.clone();
        crate::control::send_stop(&info.control_url, token.as_deref())?;

        let deadline = std::time::Instant::now() + wait;
        while std::time::Instant::now() < deadline {
            if !pidfile::pid_exists(info.pid) {
                return Ok(());
            }
            std::thread::sleep(self.poll);
        }
        if !force_after_timeout {
            return Err(SupervisorError::StopTimedOut {
                session_id: id.to_string(),
                seconds: wait.as_secs(),
            });
        }
        let name = self.engine_name.clone();
        self.signal_if_ours(info.pid, &name, &[id.as_str()])
    }

    /// Deletes a *stopped* session's files (config, token, PID, log).
    /// Refuses while it is running.
    pub fn forget(&self, id: &SessionId) -> Result<(), SupervisorError> {
        if self.session_alive(id) {
            return Err(SupervisorError::SessionStillRunning {
                session_id: id.to_string(),
            });
        }
        for path in [
            self.paths.session_pid(id),
            self.paths.session_config(id),
            self.paths.session_token(id),
            self.paths.session_log(id),
        ] {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(SupervisorError::Io {
                        action: "remove",
                        path,
                        source,
                    });
                }
            }
        }
        Ok(())
    }
}
