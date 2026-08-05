//! `apexes-engine` binary entry point.
//!
//! **Not yet implemented** -- this crate's `main.rs` (CLI parsing, CPU
//! affinity, dispatch to `run_backtest`/`run_paper_trading`/
//! `run_live_trading` based on the parsed `LaunchConfig`) is built in a
//! later task of this plan. This scaffold task only provides
//! `LaunchConfig`/`SessionMode`/`StrategyRef` (`launch_config.rs`) and
//! the shared strategy-loading logic (`apexes-strategy-loader`).
fn main() {
    unimplemented!(
        "apexes-engine main.rs is implemented in a later task of this plan (see docs/superpowers/plans/2026-08-05-apexes-13c-strategy-monitoring-tuning.md)"
    );
}
