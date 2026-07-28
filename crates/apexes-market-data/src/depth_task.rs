use crate::depth_converter;
use crate::symbol::{split_symbol, symbol_of};
use apexes_core::{MarketEvent, Venue};
use barter_data::{
    exchange::binance::spot::BinanceSpot, streams::Streams, subscription::book::OrderBooksL2,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures::StreamExt;
use tokio::sync::mpsc::UnboundedSender;

/// Subscribes to Binance Spot L2 order book updates for `symbols`
/// (snapshot + incremental, synchronized and gap-checked by `barter-data`
/// itself) and converts each event into `MarketEvent::Depth` via
/// `depth_converter::convert`.
pub async fn run(symbols: Vec<String>, tx: UnboundedSender<MarketEvent>) {
    let subscriptions: Vec<_> = symbols
        .iter()
        .filter_map(|symbol| {
            let (base, quote) = split_symbol(symbol)?;
            Some((
                BinanceSpot::default(),
                base,
                quote,
                MarketDataInstrumentKind::Spot,
                OrderBooksL2,
            ))
        })
        .collect();

    if subscriptions.is_empty() {
        tracing::warn!(
            ?symbols,
            "depth_task: no valid Binance spot subscriptions derived from symbols"
        );
        return;
    }

    let streams = match Streams::<OrderBooksL2>::builder()
        .subscribe(subscriptions)
        .init()
        .await
    {
        Ok(streams) => streams,
        Err(error) => {
            tracing::warn!(
                ?error,
                "depth_task: failed to initialize barter-data OrderBooksL2 stream"
            );
            return;
        }
    };

    let mut merged = streams.select_all();
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
