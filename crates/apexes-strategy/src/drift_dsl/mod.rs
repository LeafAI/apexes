//! Drift Script integration: [`DriftScriptStrategy`] runs a user-authored
//! `.drift` script as a [`crate::Strategy`] implementation, so researchers
//! can author and iterate on strategy logic without a Rust compile cycle.

mod bindings;
mod convert;
mod indicators;
mod params;
mod signals;
mod strategy;

pub use strategy::{DriftScriptStrategy, WarmupError};

mod symbol_state;

mod scoped_indicators;

mod context_market;

mod order_outcome;

mod fill_outcome;

mod terminal_outcome;

mod parent_outcome;
