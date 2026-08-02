//! Apexes A-share gateway (XTP protocol). Covers `docs/FEATURES.md`
//! row 5.3.2. **Completion level: protocol-mapping-only placeholder**
//! -- the real XTP SDK is a closed-source binary requiring a real
//! account to download; every `ExecutionGateway`/`MarketDataProvider`
//! method in this crate returns an explicit `Uncertain`/`Err` result
//! rather than performing real network I/O. See
//! `docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md`
//! for the full scope rationale.

pub mod config;
pub mod gateway;
pub mod types;

pub use config::XtpGatewayConfig;
pub use gateway::XtpExecutionGateway;
pub use types::{XtpMarket, XtpOrderInsertRequest, XtpPriceType, XtpSide};
