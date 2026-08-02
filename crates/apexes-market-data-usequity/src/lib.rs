//! Apexes US-equity market data ingestion. Covers `docs/FEATURES.md`
//! rows 1.4.1/1.4.2. Placeholder-only -- Alpaca/Polygon.io both
//! require an account/API key (no real account exists); IEX Cloud is
//! confirmed shut down and entirely out of scope (see this crate's
//! `provider` module doc comment for the full research trail). See
//! `docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md`
//! for the full scope rationale.

pub mod provider;

pub use provider::UsEquityMarketDataProvider;
