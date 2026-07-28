//! Apexes core types: shared `Instrument`, `Order`, `Fill`, `MarketEvent`,
//! and `MarketDataProvider` used across backtest, paper, and live trading.
//!
//! This crate contains only data types and trait definitions — no gateway
//! implementations, no network I/O, no business logic beyond the
//! `OrderStatus` state machine.
