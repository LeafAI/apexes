//! Errors the supervisor reports. Every variant names what failed and, where
//! it helps, what to do about it; none carries a secret.

use std::path::PathBuf;

use apexes_core::binance_network::BinanceNetwork;

#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    #[error("invalid launch request: {0}")]
    InvalidRequest(#[from] RequestError),
    #[error(
        "missing {network} credentials for {credentials_ref:?}: set {missing_var} in the environment the supervisor runs in"
    )]
    MissingCredentials {
        credentials_ref: String,
        network: BinanceNetwork,
        missing_var: String,
    },
    #[error(
        "could not find the {name} binary (looked for {looked_for:?}); build it with `cargo build -p {name}`"
    )]
    BinaryNotFound {
        name: &'static str,
        looked_for: Vec<PathBuf>,
    },
    #[error("could not {action} {path:?}: {source}")]
    Io {
        action: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(
        "the {network} market-data gateway is still used by running session(s) {sessions:?}; stop those first"
    )]
    GatewayInUse {
        network: BinanceNetwork,
        sessions: Vec<String>,
    },
    #[error("{what} exited during startup ({status}). Last log lines:\n{log_tail}")]
    ExitedDuringStartup {
        what: String,
        status: String,
        log_tail: String,
    },
    #[error("{what} did not become ready within {seconds}s. Last log lines:\n{log_tail}")]
    StartupTimeout {
        what: String,
        seconds: u64,
        log_tail: String,
    },
    #[error("session {session_id:?} is already running; pick a different id or stop it first")]
    SessionAlreadyRunning { session_id: String },
    #[error(
        "session {session_id:?} was asked to stop but is still running after {seconds}s; check its log, or stop it again with force"
    )]
    StopTimedOut { session_id: String, seconds: u64 },
    #[error("could not reach the control endpoint of the session: {reason}")]
    ControlUnreachable { reason: String },
    #[error("no running session {session_id:?}")]
    NoSuchSession { session_id: String },
    #[error("refusing to signal pid {pid}: {reason}")]
    UnsafeToSignal { pid: u32, reason: String },
    #[error("session {session_id:?} is still running; stop it before forgetting its files")]
    SessionStillRunning { session_id: String },
}

/// Why a [`crate::LaunchRequest`] was refused before any process work.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RequestError {
    #[error("mainnet trades real funds: type MAINNET exactly to confirm")]
    MainnetNotConfirmed,
    #[error("at least one instrument is required")]
    NoInstruments,
    #[error(
        "invalid instrument symbol {symbol:?}: use upper-case letters and digits, 5-20 characters"
    )]
    BadSymbol { symbol: String },
    #[error(
        "instrument {symbol:?} must quote in one of USDT, USDC or BUSD (the market-data feed only supports those)"
    )]
    UnsupportedQuote { symbol: String },
    #[error("invalid credentials reference {value:?}: use lower-case letters, digits and '-'")]
    BadCredentialsRef { value: String },
    #[error(
        "invalid session id {value:?}: use letters, digits, '.', '_' or '-' (at most 64 characters)"
    )]
    BadSessionId { value: String },
    #[error("the Postgres URL must start with postgres:// or postgresql://")]
    BadPostgresUrl,
    #[error("{0}")]
    Config(String),
}
