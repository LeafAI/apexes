use crate::symbol::{split_symbol, symbol_of};
use apexes_core::{MarketEvent, Tick, Venue};
use barter_data::{
    exchange::binance::spot::BinanceSpot, streams::Streams, subscription::book::OrderBooksL1,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures::StreamExt;
use tokio::sync::mpsc::UnboundedSender;

/// Subscribes to Binance Spot bookTicker (best bid/ask) for `symbols` and
/// converts each update into `MarketEvent::Tick`. Updates where either side
/// of the book is empty (`best_bid`/`best_ask` is `None`) are skipped --
/// see design spec §3 "ticker_task 的空盘口处理".
pub async fn run(symbols: Vec<String>, tx: UnboundedSender<MarketEvent>) {
    let subscriptions: Vec<_> = symbols
        .iter()
        .filter_map(|symbol| {
            let (base, quote) = match split_symbol(symbol) {
                Some(pair) => pair,
                None => {
                    tracing::warn!(
                        %symbol,
                        "ticker_task: skipping symbol with unrecognized quote currency"
                    );
                    return None;
                }
            };
            Some((
                BinanceSpot::default(),
                base,
                quote,
                MarketDataInstrumentKind::Spot,
                OrderBooksL1,
            ))
        })
        .collect();

    if subscriptions.is_empty() {
        tracing::warn!(
            ?symbols,
            "ticker_task: no valid Binance spot subscriptions derived from symbols"
        );
        return;
    }

    let streams = match Streams::<OrderBooksL1>::builder()
        .subscribe(subscriptions)
        .init()
        .await
    {
        Ok(streams) => streams,
        Err(error) => {
            tracing::warn!(
                ?error,
                "ticker_task: failed to initialize barter-data OrderBooksL1 stream"
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
                    "ticker_task: received error item from barter-data stream"
                );
                continue;
            }
            barter_data::streams::reconnect::Event::Reconnecting(exchange) => {
                tracing::info!(?exchange, "ticker_task: barter-data stream reconnecting");
                continue;
            }
        };

        let symbol = symbol_of(&market_event.instrument);
        let l1: barter_data::subscription::book::OrderBookL1 = market_event.kind;

        let (Some(best_bid), Some(best_ask)) = (l1.best_bid, l1.best_ask) else {
            tracing::debug!(symbol = %symbol, "ticker_task: skipping update with empty book side");
            continue;
        };

        let tick = MarketEvent::Tick(Tick {
            symbol,
            venue: Venue::Binance,
            bid_price: best_bid.price,
            bid_size: best_bid.amount,
            ask_price: best_ask.price,
            ask_size: best_ask.amount,
            timestamp: market_event.time_exchange,
        });

        if tx.send(tick).is_err() {
            tracing::warn!("ticker_task: output channel closed, stopping");
            return;
        }
    }
}
