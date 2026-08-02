//! Apexes H-share/US-equity gateway (IBKR TWS API protocol). Covers
//! `docs/FEATURES.md` row 5.3.3. **Completion level: full protocol
//! implementation** (real TCP client, real message encoding/decoding)
//! **validated only against a hand-written mock TWS server** -- never
//! connected to a real TWS/IB Gateway process, pending real account
//! access. This is a deliberately HIGHER completion level than the
//! sibling `apexes-gateway-xtp` crate, because the IBKR wire protocol
//! is publicly documented (unlike XTP's closed-source SDK binary).
//! See `docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md`
//! for the full scope rationale.

pub mod config;
pub mod protocol;

pub use config::IbkrGatewayConfig;
