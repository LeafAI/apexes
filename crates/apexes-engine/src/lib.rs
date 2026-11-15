//! `apexes-engine`: one OS process per trading session (design spec
//! §四). Spawned and supervised by `apexes-ui`, never linked against by
//! it -- see `apexes_strategy_loader` for the shared strategy-loading
//! logic both crates use instead.
//!
//! `main.rs` (a later task) parses CLI args / reads the `LaunchConfig`
//! JSON handed to it by `apexes-ui`, sets CPU affinity, and dispatches
//! to whichever of `run_backtest`/`run_paper_trading`/
//! `run_live_trading` the selected `SessionMode` needs.

pub mod credentials;
pub mod gateway_client;
pub mod launch_config;
pub mod protocol;
pub mod server;

pub use apexes_strategy_loader::StrategyRef;
pub mod risk_limits;
pub use credentials::{CredentialsError, credential_var_names, resolve_binance_credentials};
pub use gateway_client::GatewayMarketDataProvider;
pub use launch_config::{LaunchConfig, SessionMode};
pub use protocol::{EngineStatus, SessionMessage};
pub use risk_limits::{LiveRiskLimits, RiskLimitsError};
pub use server::{ServerSecurity, build_app, build_app_secured, build_app_with_kill_switch};
