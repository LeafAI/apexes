//! Apexes strategy version management & simplified (backtest-only)
//! canary release: `VersionStore` persists `.drift` script versions and
//! a mutable "current version" pointer to ClickHouse; `CanaryComparator`
//! scores multiple versions against the same historical market data
//! using `apexes_optimize::Evaluator`, optionally promoting the winner.

pub mod comparator;
pub mod error;
pub mod schema;
pub mod version_store;

pub use comparator::CanaryComparator;
pub use error::RegistryError;
pub use version_store::{VersionRecord, VersionStore};
