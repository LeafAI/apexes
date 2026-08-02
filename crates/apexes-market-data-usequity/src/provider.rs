//! Placeholder for US-equity market data (`docs/FEATURES.md` rows
//! 1.4.1/1.4.2). IEX Cloud is confirmed shut down (the service was
//! unreachable during design research, matching its publicly known
//! August 2024 discontinuation) and is EXCLUDED entirely -- no type
//! or module documents it, since there is no live protocol left to
//! document. Alpaca and Polygon.io (both free-tier and paid-tier)
//! require account signup/an API key even for their free tiers
//! (confirmed via a live 401 Unauthorized response during design
//! research) -- both are placeholder-only here. Every method returns
//! an explicit `Err`, never a panic, never `unimplemented!()`, never a
//! fabricated success. See the design spec §3/§4.4 for the full
//! research trail.

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::BoxStream;

pub struct UsEquityMarketDataProvider;

#[async_trait]
impl MarketDataProvider for UsEquityMarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let symbol = instruments
            .first()
            .map(|instrument| instrument.symbol.clone())
            .unwrap_or_else(|| "*".to_string());
        Err(ApexesError::SubscriptionFailed {
            symbol,
            venue: Venue::Nasdaq,
            reason: "US-equity market data not yet implemented -- Alpaca and Polygon.io both \
                     require account signup/an API key even for their free tiers (no real \
                     account exists); IEX Cloud is confirmed shut down and out of scope \
                     entirely; see \
                     docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md \
                     §4.4 for scope"
                .to_string(),
            source_detail: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, Instrument, MarketDataProvider, MarketType, Venue};
    use rust_decimal_macros::dec;

    fn sample_instrument() -> Instrument {
        Instrument::new(
            "AAPL",
            MarketType::UsEquity,
            Venue::Nasdaq,
            AssetClass::Equity,
            None,
            Some("USD".to_string()),
            dec!(0.01),
            dec!(1),
            dec!(1),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn subscribe_returns_an_explicit_error() {
        let provider = UsEquityMarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_error_mentions_account_or_key() {
        let provider = UsEquityMarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        let error = result.err().expect("subscribe should return Err");
        let message = error.to_string().to_lowercase();
        assert!(message.contains("account") || message.contains("key"));
    }
}
