//! Apexes EMS (execution management system, sub-project #9): smart
//! order routing (SOR), cross-market order splitting, and four
//! execution algorithms (TWAP/VWAP/Iceberg/POV), usable identically
//! across backtest, paper, and live trading. See
//! `docs/superpowers/specs/2026-08-01-apexes-ems-design.md` for the
//! full design.
//!
//! Two `Ems` implementations: [`in_memory::InMemoryEms`] (backtest/
//! paper trading, paced by virtual/event time via
//! `Ems::on_market_event`) and [`postgres::PostgresEms`] (live
//! trading, crash-recoverable, paced by real wall-clock timers).
//!
//! This crate deliberately does NOT depend on `apexes-backtest`
//! (`apexes-backtest` depends on THIS crate for `InMemoryEms`, so the
//! reverse dependency would cycle) -- see [`child_order_sink`] for how
//! `InMemoryEms` submits child orders without needing
//! `apexes_backtest::MatchingEngine` directly.

pub mod child_order_sink;
pub mod ems;
pub mod error;
pub mod in_memory;

pub use child_order_sink::{ChildOrderIntent, ChildOrderSink};
pub use ems::{AlgoOrderRequest, Ems};
pub use error::EmsError;
pub use in_memory::InMemoryEms;
