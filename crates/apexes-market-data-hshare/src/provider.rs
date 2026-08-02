//! Placeholder for H-share market data (`docs/FEATURES.md` rows
//! 1.3.1/1.3.2). Futu OpenAPI/Longbridge OpenAPI (free tier) and HKEX
//! OMD-C direct connect (paid) were all already researched during the
//! broker-gateway sub-project: Futu stopped opening new mainland
//! China accounts (2022-12-31) and pulled its app (2023-05-19);
//! Longbridge/LongPort has no sandbox (real-money-only API); HKEX
//! OMD-C is a closed-source institutional direct-connect feed with no
//! public trial access. This module has no live network code of any
//! kind -- every method returns an explicit `Err`, never a panic,
//! never `unimplemented!()`, never a fabricated success. See the
//! design spec §3/§4.3 for the full research trail.

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::BoxStream;

pub struct HShareMarketDataProvider;

#[async_trait]
impl MarketDataProvider for HShareMarketDataProvider {
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
            venue: Venue::Hkex,
            reason: "H-share market data not yet implemented -- no real account exists for \
                     Futu (stopped new mainland China account opening 2022-12-31) or \
                     Longbridge (no sandbox, real-money-only API), and HKEX OMD-C is a \
                     closed-source paid direct-connect feed; see \
                     docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md \
                     §4.3 for scope"
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
            "0700",
            MarketType::HShare,
            Venue::Hkex,
            AssetClass::Equity,
            None,
            Some("HKD".to_string()),
            dec!(0.01),
            dec!(100),
            dec!(1),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn subscribe_returns_an_explicit_error() {
        let provider = HShareMarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_error_mentions_no_account_access() {
        let provider = HShareMarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        let error = result.err().expect("subscribe should return Err");
        assert!(error.to_string().to_lowercase().contains("account"));
    }
}
