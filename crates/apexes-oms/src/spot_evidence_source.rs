//! Strict read-only recovery evidence; deliberately no execution methods.
use crate::TerminalOrderEvidence;
use apexes_core::{Instrument, Order};
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpotEvidenceBalance {
    pub free: Decimal,
    pub locked: Decimal,
}

/// Trusted implementation must bind the actual authenticated REST
/// credential and network to the same owner encoding as persistence.
/// A successful result must be complete, not a filtered or truncated page.
/// No defaults: unsupported adapters must not manufacture empty evidence.
#[async_trait]
pub trait SpotEvidenceSource: Send + Sync {
    fn owner_fingerprint(
        &self,
        strategy_ref: &serde_json::Value,
        instruments: &[Instrument],
    ) -> Result<String, String>;
    async fn terminal_order(&self, order: &Order) -> Result<TerminalOrderEvidence, String>;
    /// Count every raw account-wide Spot open-order row, including
    /// unknown client IDs and statuses. Zero is the only accepted result.
    async fn account_open_order_count(&self) -> Result<usize, String>;
    /// Complete account-wide free and locked amounts, including zeros.
    async fn account_wallet(&self) -> Result<HashMap<String, SpotEvidenceBalance>, String>;
}
