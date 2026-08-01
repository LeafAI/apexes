use crate::depth_converter;
use crate::provider::BinanceExchangeKind;
use crate::symbol::{split_symbol, symbol_of};
use apexes_core::{MarketEvent, Venue};
use barter_data::{
    exchange::binance::{futures::BinanceFuturesUsd, spot::BinanceSpot},
    streams::Streams,
    subscription::book::OrderBooksL2,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures::StreamExt;
use tokio::sync::mpsc::UnboundedSender;

/// Subscribes to Binance L2 order book updates for `symbols` on `exchange`
/// (`BinanceExchangeKind::Spot` or `::Perpetual`; snapshot + incremental,
/// synchronized and gap-checked by `barter-data` itself) and converts each
/// event into `MarketEvent::Depth` via `depth_converter::convert`.
pub async fn run(
    symbols: Vec<String>,
    exchange: BinanceExchangeKind,
    tx: UnboundedSender<MarketEvent>,
) {
    let mut merged = match exchange {
        BinanceExchangeKind::Spot => {
            let subscriptions: Vec<_> = build_subscriptions(&symbols, |base, quote| {
                (
                    BinanceSpot::default(),
                    base,
                    quote,
                    MarketDataInstrumentKind::Spot,
                    OrderBooksL2,
                )
            });
            if subscriptions.is_empty() {
                tracing::warn!(
                    ?symbols,
                    "depth_task: no valid Binance spot subscriptions derived from symbols"
                );
                return;
            }
            match Streams::<OrderBooksL2>::builder()
                .subscribe(subscriptions)
                .init()
                .await
            {
                Ok(streams) => streams.select_all(),
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        "depth_task: failed to initialize barter-data OrderBooksL2 stream (spot)"
                    );
                    return;
                }
            }
        }
        BinanceExchangeKind::Perpetual => {
            let subscriptions: Vec<_> = build_subscriptions(&symbols, |base, quote| {
                (
                    BinanceFuturesUsd::default(),
                    base,
                    quote,
                    MarketDataInstrumentKind::Perpetual,
                    OrderBooksL2,
                )
            });
            if subscriptions.is_empty() {
                tracing::warn!(
                    ?symbols,
                    "depth_task: no valid Binance perpetual subscriptions derived from symbols"
                );
                return;
            }
            match Streams::<OrderBooksL2>::builder()
                .subscribe(subscriptions)
                .init()
                .await
            {
                Ok(streams) => streams.select_all(),
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        "depth_task: failed to initialize barter-data OrderBooksL2 stream (perpetual)"
                    );
                    return;
                }
            }
        }
    };

    while let Some(event) = merged.next().await {
        let market_event = match event {
            barter_data::streams::reconnect::Event::Item(Ok(market_event)) => market_event,
            barter_data::streams::reconnect::Event::Item(Err(error)) => {
                tracing::warn!(
                    ?error,
                    "depth_task: received error item from barter-data stream"
                );
                continue;
            }
            barter_data::streams::reconnect::Event::Reconnecting(exchange) => {
                tracing::info!(
                    ?exchange,
                    "depth_task: barter-data stream reconnecting (next update will be is_snapshot: true)"
                );
                continue;
            }
        };

        let symbol = symbol_of(&market_event.instrument);
        let depth_update = depth_converter::convert(&symbol, Venue::Binance, &market_event.kind);

        if tx.send(MarketEvent::Depth(depth_update)).is_err() {
            tracing::warn!("depth_task: output channel closed, stopping");
            return;
        }
    }
}

/// Builds one subscription tuple per valid symbol via `make_tuple`,
/// skipping (with a warning) any symbol whose quote currency isn't
/// recognized by `split_symbol`. Shared helper so the Spot/Perpetual
/// branches above don't duplicate this filter-and-log logic.
fn build_subscriptions<T>(symbols: &[String], make_tuple: impl Fn(String, String) -> T) -> Vec<T> {
    symbols
        .iter()
        .filter_map(|symbol| {
            let (base, quote) = match split_symbol(symbol) {
                Some(pair) => pair,
                None => {
                    tracing::warn!(
                        %symbol,
                        "depth_task: skipping symbol with unrecognized quote currency"
                    );
                    return None;
                }
            };
            Some(make_tuple(base, quote))
        })
        .collect()
}
