//! Apexes H-share/US-equity gateway (IBKR TWS API protocol). Covers
//! `docs/FEATURES.md` row 5.3.3. **Completion level: wire-framing-complete**
//! (real TCP client, real 4-byte-length-prefixed/null-separated message
//! framing) **validated only against a hand-written mock TWS server** --
//! never connected to a real TWS/IB Gateway process, pending real account
//! access. Message payloads use simplified symbolic verbs
//! (`PLACE_ORDER`/`CANCEL_ORDER`/`REQ_MKT_DATA`, per the design spec's
//! own scope) rather than TWS's real numeric message IDs, and there is
//! no API handshake (API-prefix/version negotiation/`startApi`) --
//! `IbkrGatewayConfig::client_id` is stored but never transmitted.
//! This is a deliberately HIGHER completion level than the sibling
//! `apexes-gateway-xtp` crate (which never opens a socket at all),
//! because the IBKR wire protocol is publicly documented (unlike
//! XTP's closed-source SDK binary), but it is not a full TWS API
//! client implementation. See
//! `docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md`
//! for the full scope rationale.

pub mod config;
pub mod gateway;
pub mod market_data;
pub mod protocol;

pub use config::IbkrGatewayConfig;
pub use gateway::IbkrExecutionGateway;
pub use market_data::IbkrMarketDataProvider;
