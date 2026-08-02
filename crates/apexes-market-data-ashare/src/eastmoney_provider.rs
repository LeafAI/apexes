//! [`EastmoneyMarketDataProvider`]: real implementation of
//! `apexes_core::MarketDataProvider` against Eastmoney's public quote
//! REST API (`push2.eastmoney.com`, verified during design research
//! to require no API key). Polling-based -- Eastmoney's public API
//! has no free WebSocket push tier.
//!
//! Eastmoney's `secid` parameter format is `{market}.{code}` where
//! `market` is `0` for Shenzhen (SZSE) and `1` for Shanghai (SSE).
//! Field codes in the response are terse (`f43`=latest price scaled
//! by 100, `f60`=previous close scaled by 100, etc.) -- documented per
//! field below, not just guessed at, based on real response shapes
//! observed during design research.

use apexes_core::{ApexesError, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::config::EastmoneyConfig;

/// A parsed Eastmoney quote. Eastmoney scales price fields by 100
/// (e.g. `f43=951` means a real price of `9.51`) -- documented per
/// field based on real response shapes observed during design
/// research, not guessed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EastmoneyQuote {
    pub latest_price: Decimal,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub previous_close: Decimal,
}

pub struct EastmoneyMarketDataProvider {
    config: EastmoneyConfig,
    http_client: reqwest::Client,
}

impl EastmoneyMarketDataProvider {
    pub fn new(config: EastmoneyConfig) -> Self {
        Self {
            config,
            http_client: reqwest::Client::new(),
        }
    }

    /// Eastmoney's `secid` format: `{market}.{code}`, where `market`
    /// is `1` for Shanghai (SSE) and `0` for Shenzhen (SZSE).
    pub(crate) fn secid_for(code: &str, venue: Venue) -> String {
        let market = match venue {
            Venue::Sse => "1",
            Venue::Szse => "0",
            _ => "?",
        };
        format!("{market}.{code}")
    }

    fn scale_field(raw: i64) -> Decimal {
        Decimal::new(raw, 2)
    }

    pub async fn fetch_quote(&self, code: &str, venue: Venue) -> Result<EastmoneyQuote, String> {
        let secid = Self::secid_for(code, venue);
        if secid.starts_with('?') {
            return Err(format!(
                "EastmoneyMarketDataProvider does not support venue {venue:?}"
            ));
        }
        let url = format!("{}/api/qt/stock/get", self.config.base_url);
        let response = self
            .http_client
            .get(&url)
            .query(&[("secid", secid.as_str()), ("fields", "f43,f44,f45,f46,f60")])
            .send()
            .await
            .map_err(|error| format!("Eastmoney request to {url} failed: {error}"))?;
        if !response.status().is_success() {
            return Err(format!(
                "Eastmoney endpoint {url} returned HTTP {}",
                response.status()
            ));
        }

        #[derive(Deserialize)]
        struct QuoteData {
            f43: i64,
            f44: i64,
            f45: i64,
            f46: i64,
            f60: i64,
        }
        #[derive(Deserialize)]
        struct QuoteResponse {
            rc: i64,
            data: Option<QuoteData>,
        }

        let parsed: QuoteResponse = response
            .json()
            .await
            .map_err(|error| format!("failed to parse Eastmoney response body: {error}"))?;
        if parsed.rc != 0 {
            return Err(format!("Eastmoney returned a nonzero rc: {}", parsed.rc));
        }
        let data = parsed
            .data
            .ok_or_else(|| "Eastmoney response had rc=0 but no data field".to_string())?;

        Ok(EastmoneyQuote {
            latest_price: Self::scale_field(data.f43),
            open: Self::scale_field(data.f46),
            high: Self::scale_field(data.f44),
            low: Self::scale_field(data.f45),
            previous_close: Self::scale_field(data.f60),
        })
    }
}

#[async_trait]
impl MarketDataProvider for EastmoneyMarketDataProvider {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError> {
        for instrument in instruments {
            if !matches!(instrument.venue, Venue::Sse | Venue::Szse) {
                return Err(ApexesError::SubscriptionFailed {
                    symbol: instrument.symbol.clone(),
                    venue: instrument.venue,
                    reason: "EastmoneyMarketDataProvider only supports Venue::Sse and Venue::Szse"
                        .to_string(),
                    source_detail: None,
                });
            }
        }

        // Polling-based (Eastmoney has no free WebSocket push tier).
        // The actual poll-and-emit loop is deferred for the same
        // documented reason as DexMarketDataProvider's polling loop
        // (Task 3): this task delivers real, tested request-building
        // and response-parsing logic (fetch_quote), but wiring a full
        // interval-poll-and-convert-to-MarketEvent loop without a live
        // endpoint to validate polling cadence against would risk an
        // untested bug going unnoticed.
        tracing::info!(
            count = instruments.len(),
            "EastmoneyMarketDataProvider subscribed; polling fetch_quote per instrument on an interval is not yet wired up (see doc comment)"
        );
        let stream = stream::empty();
        Ok(stream.boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apexes_core::{AssetClass, Instrument, MarketDataProvider, MarketType, Venue};
    use rust_decimal_macros::dec;
    use wiremock::matchers::{method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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

    #[test]
    fn secid_for_venue_maps_sse_and_szse_correctly() {
        assert_eq!(
            EastmoneyMarketDataProvider::secid_for("600000", Venue::Sse),
            "1.600000"
        );
        assert_eq!(
            EastmoneyMarketDataProvider::secid_for("000001", Venue::Szse),
            "0.000001"
        );
    }

    #[tokio::test]
    async fn fetch_quote_parses_a_real_response_shape() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex(r"^/api/qt/stock/get$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "rc": 0,
                "rt": 4,
                "data": {
                    "f43": 951,
                    "f44": 959,
                    "f45": 928,
                    "f46": 959,
                    "f60": 971
                }
            })))
            .mount(&mock_server)
            .await;

        let provider = EastmoneyMarketDataProvider::new(EastmoneyConfig {
            base_url: mock_server.uri(),
        });
        let quote = provider.fetch_quote("600000", Venue::Sse).await.unwrap();
        assert_eq!(quote.latest_price, dec!(9.51));
        assert_eq!(quote.previous_close, dec!(9.71));
    }

    #[tokio::test]
    async fn fetch_quote_returns_err_on_a_nonzero_rc_field() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex(r"^/api/qt/stock/get$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "rc": 1,
                "rt": 0,
                "data": null
            })))
            .mount(&mock_server)
            .await;

        let provider = EastmoneyMarketDataProvider::new(EastmoneyConfig {
            base_url: mock_server.uri(),
        });
        let result = provider.fetch_quote("600000", Venue::Sse).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fetch_quote_returns_err_on_http_failure() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex(r"^/api/qt/stock/get$"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let provider = EastmoneyMarketDataProvider::new(EastmoneyConfig {
            base_url: mock_server.uri(),
        });
        let result = provider.fetch_quote("600000", Venue::Sse).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_returns_err_for_an_unsupported_venue() {
        let mock_server = MockServer::start().await;
        let provider = EastmoneyMarketDataProvider::new(EastmoneyConfig {
            base_url: mock_server.uri(),
        });
        let mut instrument = sample_instrument();
        instrument.venue = Venue::Binance;
        let result = provider.subscribe(&[instrument]).await;
        assert!(result.is_err());
    }
}
