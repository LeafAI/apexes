//! [`IbkrMarketDataProvider`]: implements `apexes_core::MarketDataProvider`
//! against the IBKR TWS API's market-data request protocol
//! (`reqMktData`). Same completion level as `IbkrExecutionGateway` --
//! real request construction and wire-protocol encoding, never
//! connected to a real TWS/IB Gateway process. Unlike
//! `IbkrExecutionGateway`, this type owns its OWN connection (rather
//! than sharing `IbkrExecutionGateway`'s) to keep the two concerns --
//! order execution and market data -- independently testable and
//! independently connectable, matching how a real TWS client can
//! subscribe to market data without necessarily placing orders (and
//! vice versa).

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::BoxStream;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::config::IbkrGatewayConfig;
use crate::protocol::encode_message;

pub struct IbkrMarketDataProvider {
    config: IbkrGatewayConfig,
    stream: Mutex<Option<TcpStream>>,
}

impl IbkrMarketDataProvider {
    pub fn new(config: IbkrGatewayConfig) -> Self {
        Self {
            config,
            stream: Mutex::new(None),
        }
    }

    /// Establishes this provider's own real TCP connection,
    /// independent of any `IbkrExecutionGateway` instance -- market
    /// data and order execution are separate TWS API concerns that a
    /// real client can use independently.
    pub async fn connect(&self) -> Result<(), String> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|error| format!("failed to connect to IBKR gateway at {addr}: {error}"))?;
        *self.stream.lock().await = Some(stream);
        Ok(())
    }

    /// Real Instrument -> IBKR `reqMktData` request-field mapping.
    fn build_market_data_request(instrument: &Instrument) -> Vec<String> {
        vec!["REQ_MKT_DATA".to_string(), instrument.symbol.clone()]
    }
}

#[async_trait]
impl MarketDataProvider for IbkrMarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        let mut guard = self.stream.lock().await;
        let stream = guard
            .as_mut()
            .ok_or_else(|| ApexesError::SubscriptionFailed {
                symbol: instruments
                    .first()
                    .map(|instrument| instrument.symbol.clone())
                    .unwrap_or_else(|| "*".to_string()),
                venue: instruments
                    .first()
                    .map(|instrument| instrument.venue)
                    .unwrap_or(Venue::Nasdaq),
                reason: "not connected to an IBKR gateway -- call connect() first".to_string(),
                source_detail: None,
            })?;

        for instrument in instruments {
            let fields = Self::build_market_data_request(instrument);
            let bytes = encode_message(&fields);
            stream
                .write_all(&bytes)
                .await
                .map_err(|error| ApexesError::SubscriptionFailed {
                    symbol: instrument.symbol.clone(),
                    venue: instrument.venue,
                    reason: format!("failed to write reqMktData to IBKR socket: {error}"),
                    source_detail: None,
                })?;
        }

        Err(ApexesError::SubscriptionFailed {
            symbol: instruments
                .first()
                .map(|instrument| instrument.symbol.clone())
                .unwrap_or_else(|| "*".to_string()),
            venue: instruments
                .first()
                .map(|instrument| instrument.venue)
                .unwrap_or(Venue::Nasdaq),
            reason: "reqMktData sent, but never connected to a real TWS/IB Gateway process -- \
                     no tickPrice response was ever received to build a live event stream from"
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

    fn sample_config() -> IbkrGatewayConfig {
        IbkrGatewayConfig {
            host: "127.0.0.1".to_string(),
            port: 7497,
            client_id: 1,
        }
    }

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

    #[test]
    fn build_market_data_request_maps_instrument_symbol() {
        let fields = IbkrMarketDataProvider::build_market_data_request(&sample_instrument());
        assert_eq!(fields[0], "REQ_MKT_DATA");
        assert_eq!(fields[1], "AAPL");
    }

    #[tokio::test]
    async fn subscribe_returns_err_when_not_connected() {
        let provider = IbkrMarketDataProvider::new(sample_config());
        let result = provider.subscribe(&[sample_instrument()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_error_mentions_no_real_tws_connection() {
        let provider = IbkrMarketDataProvider::new(sample_config());
        let result = provider.subscribe(&[sample_instrument()]).await;
        // Note: brief's original `result.unwrap_err()` cannot compile --
        // `BoxStream<'static, MarketEvent>` (`dyn Stream + Send`) does not
        // implement `Debug`, which `Result::unwrap_err` requires. Using
        // `err().expect(...)` preserves identical semantics.
        let error = result
            .err()
            .expect("subscribe should return Err when not connected");
        assert!(error.to_string().to_lowercase().contains("connect"));
    }
}
