//! [`MarkPriceProvider`]: supplies the current mark price for a
//! symbol/venue -- the index-and-funding-basis-adjusted price real
//! exchanges use for margin/liquidation math, deliberately distinct
//! from last-trade price or order-book mid (see
//! `docs/superpowers/specs/2026-08-02-apexes-mark-price-pipeline-design.md`
//! for the full rationale and the `apexes_core::MarketEvent::MarkPrice`
//! data this ultimately comes from).
//!
//! Backtest: `apexes_backtest` provides a `StorageReaderMarkPriceProvider`
//! (defined there, not here or in `apexes-storage` -- see this plan's
//! Global Constraints for why). Paper/live trading: no implementation
//! of this trait is needed at all -- `run_event_loop`/`run_live_trading`
//! already receive `MarketEvent::MarkPrice` directly and maintain a
//! plain `HashMap<String, Decimal>` (mirroring the existing
//! `last_prices` pattern), passed straight into `PortfolioLedger`'s
//! margin methods without going through this trait.

use apexes_core::{ApexesError, Venue};
use async_trait::async_trait;
use rust_decimal::Decimal;

#[async_trait]
pub trait MarkPriceProvider: Send + Sync {
    /// Returns the current mark price for `symbol` at `venue`. In a
    /// backtest, "current" means "as of the caller's current replay
    /// timestamp" -- see `StorageReaderMarkPriceProvider`
    /// (`apexes-backtest`) for how that's tracked.
    async fn mark_price(&self, symbol: &str, venue: Venue) -> Result<Decimal, ApexesError>;
}
