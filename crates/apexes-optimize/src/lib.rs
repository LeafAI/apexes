//! Apexes parameter optimization interface: `SearchSpace` (parameter
//! name -> candidate values), a pluggable `ParameterOptimizer` trait,
//! and two implementations -- `GridSearchOptimizer` (in-process
//! exhaustive search) and `ExternalProcessOptimizer` (delegates to an
//! external subprocess over a newline-delimited-JSON protocol, per the
//! project roadmap's "pluggable external ML service" requirement --
//! deliberately NOT a native dynamic-library plugin, since Rust has no
//! stable ABI and that approach would require `unsafe` code).

pub mod search_space;

pub use search_space::SearchSpace;
