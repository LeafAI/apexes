//! Placeholder for A-share paid Level-2 tick data, call-auction, and
//! order-by-order commit/fill parsing (`docs/FEATURES.md` rows
//! 1.2.2/1.2.3). All three real candidate sources (Hundsun UF2.0,
//! XTP, Huaxin Qidian) are closed-source/paid with no public trial
//! access -- see the design spec §3 for the full research trail. This
//! module is a pure placeholder: it does NOT define any real
//! Level-2/call-auction wire-format types (unlike `apexes-gateway-xtp`,
//! which does define real request-shape types for order placement) --
//! there is no publicly documented wire format for any of these three
//! sources to reference. Every method returns an explicit `Err` --
//! never a panic, never `unimplemented!()`, never a fabricated
//! success.

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::BoxStream;

pub struct Level2MarketDataProvider;

#[async_trait]
impl MarketDataProvider for Level2MarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let symbol = instruments
            .first()
            .map(|instrument| instrument.symbol.clone())
            .unwrap_or_else(|| "*".to_string());
        let venue = instruments
            .first()
            .map(|instrument| instrument.venue)
            .unwrap_or(Venue::Sse);
        Err(ApexesError::SubscriptionFailed {
            symbol,
            venue,
            reason: "A-share Level-2/call-auction data not yet implemented -- pending real SDK \
                     access to a closed-source/paid provider (Hundsun UF2.0, XTP, or Huaxin \
                     Qidian); see \
                     docs/superpowers/specs/2026-08-02-apexes-market-data-remaining-design.md \
                     §4.2 for scope"
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
            "600000",
            MarketType::AShare,
            Venue::Sse,
            AssetClass::Equity,
            None,
            Some("CNY".to_string()),
            dec!(0.01),
            dec!(100),
            dec!(1),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn subscribe_returns_an_explicit_error() {
        let provider = Level2MarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_error_mentions_pending_sdk_access() {
        let provider = Level2MarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        let error = result.err().expect("subscribe should return Err");
        assert!(error.to_string().to_lowercase().contains("pending"));
    }
}
