//! Drift Script integration: [`DriftScriptStrategy`] runs a user-authored
//! `.drift` script as a [`crate::Strategy`] implementation, so researchers
//! can author and iterate on strategy logic without a Rust compile cycle.

mod bindings;
// `convert`'s public functions are not yet consumed outside its own tests
// (Task 5 wires them into `bindings`/`strategy`) -- `dead_code` is allowed
// at this checkpoint only, removed once Task 5 lands.
#[allow(dead_code)]
mod convert;
mod strategy;

// `DriftScriptStrategy` is not yet defined (populated in a later task);
// re-exporting it here is deferred until then to keep this checkpoint
// compiling.
#[allow(unused_imports)]
use bindings as _;
#[allow(unused_imports)]
use strategy as _;
