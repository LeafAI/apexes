use apexes_core::{ApexesError, AssetClass, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::BoxStream;

/// Filters `instruments` down to those tradable via this provider: Binance
/// venue and Spot asset class. Non-matching instruments are skipped with a
/// warning rather than causing the whole call to fail.
fn filter_binance_spot_instruments(instruments: &[Instrument]) -> Vec<&Instrument> {
    instruments
        .iter()
        .filter(|instrument| {
            if instrument.venue != Venue::Binance {
                tracing::warn!(
                    symbol = %instrument.symbol,
                    venue = ?instrument.venue,
                    "skipping instrument: BinanceMarketDataProvider only supports Venue::Binance"
                );
                return false;
            }
            if instrument.asset_class != AssetClass::Spot {
                tracing::warn!(
                    symbol = %instrument.symbol,
                    asset_class = ?instrument.asset_class,
                    "skipping instrument: BinanceMarketDataProvider only supports AssetClass::Spot"
                );
                return false;
            }
            true
        })
        .collect()
}

/// A `MarketDataProvider` backed by Binance Spot public WebSocket feeds,
/// via `barter-data`. Produces `Trade`, `Tick`, `Depth`, and self-aggregated
/// `Bar` events.
pub struct BinanceMarketDataProvider;

impl BinanceMarketDataProvider {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BinanceMarketDataProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MarketDataProvider for BinanceMarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let filtered = filter_binance_spot_instruments(instruments);

        if filtered.is_empty() {
            return Err(ApexesError::SubscriptionFailed {
                symbol: instruments
                    .first()
                    .map(|i| i.symbol.clone())
                    .unwrap_or_default(),
                venue: Venue::Binance,
                reason: "no instruments matched Venue::Binance + AssetClass::Spot after filtering"
                    .to_string(),
                source_detail: None,
            });
        }

        // Task 7 replaces this with the real subscription wiring.
        unimplemented!("subscription wiring lands in Task 7")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::MarketType;
    use rust_decimal_macros::dec;

    fn binance_spot_instrument(symbol: &str) -> Instrument {
        Instrument::new(
            symbol,
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Spot,
            None,
            None,
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap()
    }

    fn non_binance_instrument() -> Instrument {
        Instrument::new(
            "600519",
            MarketType::AShare,
            Venue::Sse,
            AssetClass::Equity,
            None,
            None,
            dec!(0.01),
            dec!(100),
            dec!(1),
        )
        .unwrap()
    }

    fn binance_perpetual_instrument() -> Instrument {
        Instrument::new(
            "BTCUSDT",
            MarketType::Crypto,
            Venue::Binance,
            AssetClass::Perpetual,
            None,
            None,
            dec!(0.01),
            dec!(0.0001),
            dec!(1),
        )
        .unwrap()
    }

    #[test]
    fn filters_out_non_binance_venue() {
        let instruments = vec![non_binance_instrument(), binance_spot_instrument("BTCUSDT")];
        let filtered = filter_binance_spot_instruments(&instruments);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].symbol, "BTCUSDT");
    }

    #[test]
    fn filters_out_non_spot_asset_class() {
        let instruments = vec![
            binance_perpetual_instrument(),
            binance_spot_instrument("ETHUSDT"),
        ];
        let filtered = filter_binance_spot_instruments(&instruments);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].symbol, "ETHUSDT");
    }

    #[test]
    fn keeps_binance_spot_instruments() {
        let instruments = vec![
            binance_spot_instrument("BTCUSDT"),
            binance_spot_instrument("ETHUSDT"),
        ];
        let filtered = filter_binance_spot_instruments(&instruments);
        assert_eq!(filtered.len(), 2);
    }

    #[tokio::test]
    async fn subscribe_returns_subscription_failed_when_all_instruments_filtered_out() {
        let provider = BinanceMarketDataProvider::new();
        let instruments = vec![non_binance_instrument()];

        let result = provider.subscribe(&instruments).await;

        assert!(matches!(
            result,
            Err(ApexesError::SubscriptionFailed { .. })
        ));
    }
}
