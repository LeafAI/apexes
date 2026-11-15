//! [`Supervisor`]: starts the market-data gateway and an engine for a
//! validated [`LaunchRequest`], and finds and stops them later.
//!
//! Properties this exists to guarantee:
//! - **Engines outlive the GUI.** Children are spawned in their own process
//!   group with no inherited stdio; closing or crashing the window never
//!   stops a live session.
//! - **Secrets never touch a command line or a launch file.** Control and
//!   gateway tokens go by environment and `0600` files; exchange credentials
//!   are copied only from the supervisor's own environment, by the exact
//!   per-network variable names, and children get an allow-listed
//!   environment, so mainnet keys can never reach a testnet session.
//! - **Nothing is started if it cannot work.** Validation, credentials and
//!   binaries are all checked before the first process is spawned.

use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::SupervisorError;
use crate::paths::SupervisorPaths;
use crate::pidfile;
use crate::process::{EnvLookup, locate_binary, log_tail};

pub const ENGINE_BINARY: &str = "apexes-engine";
pub const GATEWAY_BINARY: &str = "apexes-market-data-gateway";

/// Name the supervisor's own control token is registered under in an
/// engine's `APEXES_CONTROL_TOKENS` (it appears in that engine's audit log).
pub const CONTROL_TOKEN_NAME: &str = "gui";

#[derive(Clone)]
pub struct Supervisor {
    pub(crate) paths: SupervisorPaths,
    pub(crate) env: EnvLookup,
    pub(crate) current_exe: Option<PathBuf>,
    pub(crate) engine_binary: Option<PathBuf>,
    pub(crate) gateway_binary: Option<PathBuf>,
    /// File names the identity check expects to see in a process's command
    /// line before it will be signalled. Follows an explicit binary path
    /// (so a differently named build is still recognised) and falls back to
    /// the production names.
    pub(crate) engine_name: String,
    pub(crate) gateway_name: String,
    pub(crate) startup_timeout: Duration,
    /// After an engine reports ready, how long it is watched for dying
    /// anyway. A live engine writes its PID file only once startup recovery
    /// has succeeded (its control port opens earlier, so an operator can
    /// always Halt), so "PID file present" already means "recovered"; this
    /// window is a second safety net for a crash straight after.
    pub(crate) settle: Duration,
    pub(crate) poll: Duration,
}

impl Supervisor {
    /// A supervisor rooted at `home` (children get it as `HOME`, so their
    /// PID files land where the supervisor looks), reading the ambient
    /// process environment for credentials.
    pub fn new(home: impl Into<PathBuf>) -> Self {
        Self::with_env(
            home,
            Arc::new(|name: &str| std::env::var(name).ok().filter(|v| !v.is_empty())),
        )
    }

    /// Like [`Supervisor::new`] with an explicit environment lookup, so
    /// tests never touch process-global state.
    pub fn with_env(home: impl Into<PathBuf>, env: EnvLookup) -> Self {
        Self {
            paths: SupervisorPaths::from_home(home),
            env,
            current_exe: std::env::current_exe().ok(),
            engine_binary: None,
            gateway_binary: None,
            engine_name: ENGINE_BINARY.to_string(),
            gateway_name: GATEWAY_BINARY.to_string(),
            startup_timeout: Duration::from_secs(60),
            settle: Duration::from_secs(8),
            poll: Duration::from_millis(100),
        }
    }

    pub fn paths(&self) -> &SupervisorPaths {
        &self.paths
    }

    /// Uses these exact binaries. The identity check before any signal
    /// then expects *their* file names, so it stays strict for whatever is
    /// actually being run.
    pub fn with_binaries(mut self, engine: PathBuf, gateway: PathBuf) -> Self {
        let file_name = |path: &Path, fallback: &str| {
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| fallback.to_string())
        };
        self.engine_name = file_name(&engine, ENGINE_BINARY);
        self.gateway_name = file_name(&gateway, GATEWAY_BINARY);
        self.engine_binary = Some(engine);
        self.gateway_binary = Some(gateway);
        self
    }

    pub fn with_timing(
        mut self,
        startup_timeout: Duration,
        settle: Duration,
        poll: Duration,
    ) -> Self {
        self.startup_timeout = startup_timeout;
        self.settle = settle;
        self.poll = poll;
        self
    }

    pub(crate) fn resolve_binary(
        &self,
        explicit: &Option<PathBuf>,
        name: &'static str,
    ) -> Result<PathBuf, SupervisorError> {
        if let Some(path) = explicit {
            return if path.is_file() {
                Ok(path.clone())
            } else {
                Err(SupervisorError::BinaryNotFound {
                    name,
                    looked_for: vec![path.clone()],
                })
            };
        }
        locate_binary(
            name,
            self.current_exe.as_deref(),
            (self.env)("PATH").as_deref(),
        )
    }

    /// Waits until `pid_path` names `child` and its port answers. Fails fast
    /// if the child exits, with the tail of its log.
    pub(crate) fn wait_until_serving(
        &self,
        child: &mut Child,
        pid_path: &Path,
        log_path: &Path,
        what: &str,
    ) -> Result<pidfile::PidFile, SupervisorError> {
        let deadline = Instant::now() + self.startup_timeout;
        loop {
            if let Some(status) = child.try_wait().map_err(|source| SupervisorError::Io {
                action: "check on",
                path: pid_path.to_path_buf(),
                source,
            })? {
                return Err(SupervisorError::ExitedDuringStartup {
                    what: what.to_string(),
                    status: status.to_string(),
                    log_tail: log_tail(log_path, 15),
                });
            }
            // Matching the PID to our own child rejects a stale file left
            // by an earlier run.
            if let Some(file) = pidfile::read(pid_path)
                && file.pid == child.id()
                && pidfile::port_open(file.port)
            {
                return Ok(file);
            }
            if Instant::now() >= deadline {
                return Err(SupervisorError::StartupTimeout {
                    what: what.to_string(),
                    seconds: self.startup_timeout.as_secs(),
                    log_tail: log_tail(log_path, 15),
                });
            }
            std::thread::sleep(self.poll);
        }
    }

    /// Watches `child` for `self.settle` after it is serving.
    pub(crate) fn watch_settle(
        &self,
        child: &mut Child,
        log_path: &Path,
        what: &str,
    ) -> Result<(), SupervisorError> {
        let deadline = Instant::now() + self.settle;
        while Instant::now() < deadline {
            if let Some(status) = child.try_wait().map_err(|source| SupervisorError::Io {
                action: "check on",
                path: log_path.to_path_buf(),
                source,
            })? {
                return Err(SupervisorError::ExitedDuringStartup {
                    what: what.to_string(),
                    status: status.to_string(),
                    log_tail: log_tail(log_path, 15),
                });
            }
            std::thread::sleep(self.poll);
        }
        Ok(())
    }
}
