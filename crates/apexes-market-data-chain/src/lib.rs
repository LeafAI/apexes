//! Apexes crypto DEX/on-chain market data ingestion. Covers
//! `docs/FEATURES.md` rows 1.1.2/1.1.3/1.1.4. Genuinely real
//! implementation: real HTTP JSON-RPC requests and real WebSocket
//! subscriptions, developed and tested against
//! `https://ethereum-rpc.publicnode.com` / `wss://ethereum-rpc.publicnode.com`
//! (verified during design research to require no API key), but the
//! endpoint is always caller-configured, never hardcoded as a
//! default. Automated tests use `wiremock`/a hand-written mock
//! WebSocket server -- never the real `publicnode.com` endpoint. See
//! `docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md`
//! for the full scope rationale.

pub mod config;
pub mod eth_rpc_provider;

pub use config::EthRpcConfig;
pub use eth_rpc_provider::{EthLog, EthRpcProvider};
