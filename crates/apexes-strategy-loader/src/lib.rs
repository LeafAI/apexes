//! Loads a [`apexes_strategy::Strategy`] from a [`StrategyRef`] --
//! either a Drift Script file path or a compile-time-fixed Rust-native
//! name table.
//!
//! This crate is intentionally split out of `apexes-engine` (a binary
//! crate meant to be spawned as a subprocess per trading session, see
//! design spec §四) so that `apexes-ui` (design spec §六.3/§七) can
//! reuse the exact same `list_available_strategies`/`load_strategy`
//! logic to populate its session-launcher dropdown WITHOUT linking
//! against a binary crate it only ever spawns as a subprocess. Both
//! `apexes-engine` and `apexes-ui` depend on this crate directly.

pub mod strategy_loader;

pub use strategy_loader::{StrategyLoadError, list_available_strategies, load_strategy};

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// A reference to a selectable strategy -- either a Drift Script file
/// on disk, or the name of a compile-time-registered Rust-native
/// strategy (see [`strategy_loader::RUST_NATIVE_STRATEGIES`]).
///
/// Serializable: this is the shape carried inside
/// `apexes_engine::launch_config::LaunchConfig` from `apexes-ui` to a
/// spawned `apexes-engine` process (design spec §四.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StrategyRef {
    DriftScript { path: PathBuf },
    RustNative { name: String },
}
