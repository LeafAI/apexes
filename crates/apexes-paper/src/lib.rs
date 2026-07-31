//! Apexes paper trading: drives `apexes-backtest`'s shared event-loop
//! core (`apexes_backtest::event_loop::run_event_loop`) against a live
//! `apexes_core::MarketDataProvider` feed instead of pre-loaded
//! historical data, applying simulated order-submission latency
//! (`apexes_backtest::LatencyModel`) and accepting an explicit stop
//! signal since a live feed has no natural end. Covers
//! `docs/FEATURES.md` rows 4.1 (`PaperAccount`) and 4.2
//! (`run_paper_trading`); row 4.3 (order lifecycle state machine) is
//! unchanged from sub-project #0's `apexes_core::OrderStatus`.

pub mod account;
pub mod runner;

pub use account::{AccountSnapshot, PaperAccount};
pub use apexes_backtest::LatencyModel;
pub use runner::{PaperTradingConfig, run_paper_trading};
