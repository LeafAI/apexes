//! Apexes H-share market data ingestion. Covers `docs/FEATURES.md`
//! rows 1.3.1/1.3.2. Placeholder-only -- no real free or paid source
//! is reachable without a real account (see this crate's `provider`
//! module doc comment for the full research trail). See
//! `docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md`
//! for the full scope rationale.

pub mod provider;

pub use provider::HShareMarketDataProvider;
