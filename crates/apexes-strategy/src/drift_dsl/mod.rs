//! Drift Script integration: [`DriftScriptStrategy`] runs a user-authored
//! `.drift` script as a [`crate::Strategy`] implementation, so researchers
//! can author and iterate on strategy logic without a Rust compile cycle.

mod bindings;
mod convert;
mod strategy;

// `DriftScriptStrategy` is not yet defined (populated in a later task);
// re-exporting it here is deferred until then to keep this checkpoint
// compiling.
#[allow(unused_imports)]
use bindings as _;
#[allow(unused_imports)]
use convert as _;
#[allow(unused_imports)]
use strategy as _;
