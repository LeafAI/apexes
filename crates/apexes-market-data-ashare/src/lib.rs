//! Apexes A-share market data ingestion. Covers `docs/FEATURES.md`
//! rows 1.2.1/1.2.2/1.2.3. `eastmoney_provider` is a genuinely real
//! implementation (real HTTP requests, real response parsing)
//! developed and tested against `https://push2.eastmoney.com`
//! (verified during design research to require no API key); `level2`
//! is a placeholder-only module for the closed-source/paid Level-2
//! tick-data and call-auction feeds (Hundsun UF2.0, XTP, Huaxin
//! Qidian). See
//! `docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md`
//! for the full scope rationale.

pub mod config;
pub mod eastmoney_provider;
pub mod level2;

pub use config::EastmoneyConfig;
pub use eastmoney_provider::{EastmoneyMarketDataProvider, EastmoneyQuote};
pub use level2::Level2MarketDataProvider;
