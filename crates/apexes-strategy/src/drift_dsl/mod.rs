//! Drift Script integration: [`DriftScriptStrategy`] runs a user-authored
//! `.drift` script as a [`crate::Strategy`] implementation, so researchers
//! can author and iterate on strategy logic without a Rust compile cycle.

mod bindings;
mod convert;
mod indicators;
mod strategy;

pub use strategy::DriftScriptStrategy;
