//! F04.03 — A detached, causal top-of-book observation from an executor.
//! Simulated liquidity reflects that executor's consumption overlay, not
//! an exchange promise or independent source-completeness attestation.

use apexes_core::{MarketEvent, PriceLevel, Venue};
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;

/// The configured validation policy behind a simulated book observation.
/// These names describe evidence requirements, not exchange guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookIntegrity {
    SnapshotOnly,
    Contiguous,
    VerifiedRanges,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BookObservation {
    pub symbol: String,
    pub venue: Venue,
    pub observed_at: DateTime<Utc>,
    pub decision_at: DateTime<Utc>,
    pub max_age_ms: u32,
    pub integrity: BookIntegrity,
    pub sequence: Option<u64>,
    /// Best remaining simulated liquidity (not raw source quantities).
    pub best_bid: Option<PriceLevel>,
    pub best_ask: Option<PriceLevel>,
}

impl BookObservation {
    /// Check scope, exact callback time and configured freshness. A saved
    /// snapshot cannot be passed off as a fresh observation.
    pub fn matches_at(&self, event: &MarketEvent, now: DateTime<Utc>) -> bool {
        self.symbol == event.symbol()
            && self.venue == event.venue()
            && self.decision_at == now
            && self.max_age_ms > 0
            && self.observed_at <= now
            && now - self.observed_at <= TimeDelta::milliseconds(i64::from(self.max_age_ms))
    }

    pub fn age_ms(&self) -> i64 {
        (self.decision_at - self.observed_at).num_milliseconds()
    }

    pub fn spread(&self) -> Option<Decimal> {
        self.best_ask?.price.checked_sub(self.best_bid?.price)
    }
}
