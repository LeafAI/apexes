//! Process supervision for Apexes sessions: start the market-data gateway
//! and an engine for a validated request, find them again later, and stop
//! them. No GUI dependency, so it is testable without a window and any front
//! end (the operator console today) is a thin layer over it.
//!
//! Unix only (process groups, `kill(pid, 0)`, `ps`): developed and tested on
//! macOS and Linux.

pub mod control;
pub mod error;
pub mod gateway;
pub mod paths;
pub mod pidfile;
pub mod process;
pub mod request;
pub mod secrets;
pub mod session;
pub mod supervisor;

pub use error::{RequestError, SupervisorError};
pub use gateway::{GatewayHandle, GatewayMeta};
pub use paths::{GatewayFiles, GatewayKey, SessionId, SupervisorPaths};
pub use request::{LaunchRequest, MAINNET_CONFIRMATION, ValidRequest};
pub use session::{LaunchedSession, SessionInfo};
pub use supervisor::Supervisor;
