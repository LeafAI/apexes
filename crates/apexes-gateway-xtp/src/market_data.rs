//! [`XtpMarketDataProvider`]: pure placeholder implementation of
//! `apexes_core::MarketDataProvider` for XTP market data. Lower
//! completion level than `XtpExecutionGateway` -- XTP market data
//! subscription depends on the same unlinked closed-source SDK, and
//! unlike order submission there is no local request-shape mapping
//! worth doing ahead of time (a subscription request has no
//! meaningful "rejected vs uncertain" distinction the way an order
//! does), so this always returns `Err` directly.

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::BoxStream;

pub struct XtpMarketDataProvider;

#[async_trait]
impl MarketDataProvider for XtpMarketDataProvider {
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
            venue: Venue::Sse,
            reason: "XTP market data not yet implemented -- pending account access".to_string(),
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
        let provider = XtpMarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_error_mentions_pending_account_access() {
        let provider = XtpMarketDataProvider;
        let result = provider.subscribe(&[sample_instrument()]).await;
        // `unwrap_err()` is unavailable: it requires the `Ok` type
        // (`BoxStream<'static, MarketEvent>`) to implement `Debug`, which
        // `dyn Stream` does not. `err()` has no bound on `T`.
        let error = result.err().unwrap();
        assert!(error.to_string().contains("pending account access"));
    }
}
