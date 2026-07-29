//! `apexes-backtest` reuses `apexes_core::ApexesError` as its sole error
//! type — no new crate-local error enum — since backtest-domain errors
//! (insufficient balance, invalid config, order rejection) are the exact
//! same errors future live-trading sub-projects will also need to
//! represent identically, per design spec §9.

pub use apexes_core::ApexesError;
