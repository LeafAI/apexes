//! Error type for `apexes-strategy`'s Drift Script integration.

use std::path::PathBuf;
use thiserror::Error;

/// Errors that can occur constructing a [`crate::drift_dsl::DriftScriptStrategy`].
///
/// Runtime errors during `Strategy::on_market_event` (script throws, script
/// returns a malformed value) are deliberately NOT represented here -- the
/// `Strategy` trait's `on_market_event` returns `Vec<OrderIntent>` with no
/// `Result`, so runtime failures are logged via `tracing::error!` and the
/// call returns an empty `Vec` instead of propagating an error (a single
/// tick's strategy failure must never halt the whole backtest/live loop).
#[derive(Debug, Error)]
pub enum StrategyError {
    #[error("failed to read script file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse script: {0}")]
    Parse(String),

    #[error("script has no top-level `on_market_event` function")]
    MissingEntryPoint,

    #[error("`on_market_event` is defined but is not a function")]
    EntryPointNotCallable,

    #[error("`on_order_outcome` is defined but is not a function")]
    OrderOutcomeNotCallable,

    #[error("unknown parameter: {name}")]
    UnknownParam { name: String },
}
