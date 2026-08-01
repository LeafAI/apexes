use apexes_core::{ApexesError, AssetClass, Instrument, MarketDataProvider, MarketEvent, Venue};
use async_trait::async_trait;
use futures::stream::BoxStream;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Which Binance market this instrument's data comes from -- Spot public
/// market data and USDS-M Perpetual Futures public market data are
/// separate `barter-data` exchange types (`BinanceSpot` vs.
/// `BinanceFuturesUsd`) with separate WebSocket hosts, so every
/// ingestion task takes this enum to know which one to use per
/// subscription batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceExchangeKind {
    Spot,
    Perpetual,
}

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

        let symbols: Vec<String> = filtered.iter().map(|i| i.symbol.clone()).collect();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        spawn_task_thread("trade_task", {
            let symbols = symbols.clone();
            let tx = tx.clone();
            move || crate::trade_task::run(symbols, BinanceExchangeKind::Spot, tx)
        });
        spawn_task_thread("ticker_task", {
            let symbols = symbols.clone();
            let tx = tx.clone();
            move || crate::ticker_task::run(symbols, BinanceExchangeKind::Spot, tx)
        });
        spawn_task_thread("depth_task", move || {
            crate::depth_task::run(symbols, BinanceExchangeKind::Spot, tx)
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }
}

/// Runs a market-data task on a dedicated OS thread with its own
/// current-thread tokio runtime. This is required because `barter-data`
/// 0.11's `StreamBuilder::init()` awaits `Pin<Box<dyn Future>>` (non-`Send`)
/// subscription futures, so the task futures cannot be `tokio::spawn`ed
/// onto a multi-threaded runtime. The task future is constructed inside the
/// thread (via `make_task`) to avoid moving a non-`Send` future across
/// threads. `Runtime::block_on` itself has no `Send` bound.
fn spawn_task_thread<F, Fut>(task_name: &'static str, make_task: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let spawned = std::thread::Builder::new()
        .name(task_name.to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        task_name,
                        "failed to build market-data task runtime, task will not run"
                    );
                    return;
                }
            };
            runtime.block_on(make_task());
        });

    if let Err(error) = spawned {
        tracing::warn!(
            ?error,
            task_name,
            "failed to spawn market-data task thread, task will not run"
        );
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
