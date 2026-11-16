use crate::provider::BinanceExchangeKind;
use crate::supervised::{self, Flow, Timings};
use crate::symbol::{split_symbol, symbol_of};
use crate::testnet::BinanceSpotTestnet;
use apexes_core::binance_network::BinanceNetwork;
use apexes_core::{MarketEvent, Tick, Venue};
use barter_data::{
    exchange::binance::{futures::BinanceFuturesUsd, spot::BinanceSpot},
    streams::Streams,
    subscription::book::OrderBooksL1,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures::StreamExt;
use futures::stream::LocalBoxStream;
use tokio::sync::mpsc::UnboundedSender;

/// Subscribes to Binance bookTicker (best bid/ask) for `symbols` on
/// `exchange` (`BinanceExchangeKind::Spot` or `::Perpetual`) and converts
/// each update into `MarketEvent::Tick`. Updates where either side of the
/// book is empty (`best_bid`/`best_ask` is `None`) are skipped -- see
/// design spec §3 "ticker_task 的空盘口处理".
///
/// Runs until the output channel closes; the connection is supervised
/// (`crate::supervised`), so a socket that silently dies is replaced.
/// Blocks the calling thread.
pub fn run(
    symbols: Vec<String>,
    exchange: BinanceExchangeKind,
    network: BinanceNetwork,
    tx: UnboundedSender<MarketEvent>,
) {
    if !symbols.iter().any(|s| split_symbol(s).is_some()) {
        tracing::warn!(
            ?symbols,
            "ticker_task: no valid Binance subscriptions derived from symbols"
        );
        return;
    }
    supervised::run_isolated(
        "ticker_task",
        Timings::default(),
        move || connect(symbols.clone(), exchange, network),
        move |tick: MarketEvent| {
            if tx.send(tick).is_err() {
                tracing::warn!("ticker_task: output channel closed, stopping");
                Flow::Stop
            } else {
                Flow::Continue
            }
        },
    );
}

/// Opens the `barter-data` stream, yielding only complete ticks.
async fn connect(
    symbols: Vec<String>,
    exchange: BinanceExchangeKind,
    network: BinanceNetwork,
) -> Option<LocalBoxStream<'static, MarketEvent>> {
    let streams = match (exchange, network) {
        (BinanceExchangeKind::Spot, BinanceNetwork::Mainnet) => {
            let subs = build_subscriptions(&symbols, |base, quote| {
                (
                    BinanceSpot::default(),
                    base,
                    quote,
                    MarketDataInstrumentKind::Spot,
                    OrderBooksL1,
                )
            });
            Streams::<OrderBooksL1>::builder()
                .subscribe(subs)
                .init()
                .await
        }
        (BinanceExchangeKind::Spot, BinanceNetwork::Testnet) => {
            let subs = build_subscriptions(&symbols, |base, quote| {
                (
                    BinanceSpotTestnet::default(),
                    base,
                    quote,
                    MarketDataInstrumentKind::Spot,
                    OrderBooksL1,
                )
            });
            Streams::<OrderBooksL1>::builder()
                .subscribe(subs)
                .init()
                .await
        }
        (BinanceExchangeKind::Perpetual, _) => {
            let subs = build_subscriptions(&symbols, |base, quote| {
                (
                    BinanceFuturesUsd::default(),
                    base,
                    quote,
                    MarketDataInstrumentKind::Perpetual,
                    OrderBooksL1,
                )
            });
            Streams::<OrderBooksL1>::builder()
                .subscribe(subs)
                .init()
                .await
        }
    };
    match streams {
        Ok(streams) => Some(
            streams
                .select_all()
                .filter_map(|event| std::future::ready(to_tick(event)))
                .boxed_local(),
        ),
        Err(error) => {
            tracing::warn!(?error, %network, "ticker_task: failed to initialize the OrderBooksL1 stream");
            None
        }
    }
}

/// A tick, or `None` for reconnect notices, error items and updates with an
/// empty side of the book.
fn to_tick(
    event: barter_data::streams::reconnect::Event<
        barter_instrument::exchange::ExchangeId,
        Result<
            barter_data::event::MarketEvent<
                barter_instrument::instrument::market_data::MarketDataInstrument,
                barter_data::subscription::book::OrderBookL1,
            >,
            barter_data::error::DataError,
        >,
    >,
) -> Option<MarketEvent> {
    let market_event = match event {
        barter_data::streams::reconnect::Event::Item(Ok(market_event)) => market_event,
        barter_data::streams::reconnect::Event::Item(Err(error)) => {
            tracing::warn!(
                ?error,
                "ticker_task: received error item from barter-data stream"
            );
            return None;
        }
        barter_data::streams::reconnect::Event::Reconnecting(exchange) => {
            tracing::info!(?exchange, "ticker_task: barter-data stream reconnecting");
            return None;
        }
    };
    let symbol = symbol_of(&market_event.instrument);
    let l1 = market_event.kind;
    let (Some(best_bid), Some(best_ask)) = (l1.best_bid, l1.best_ask) else {
        tracing::debug!(symbol = %symbol, "ticker_task: skipping update with empty book side");
        return None;
    };
    Some(MarketEvent::Tick(Tick {
        symbol,
        venue: Venue::Binance,
        bid_price: best_bid.price,
        bid_size: best_bid.amount,
        ask_price: best_ask.price,
        ask_size: best_ask.amount,
        timestamp: market_event.time_exchange,
    }))
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
                        "ticker_task: skipping symbol with unrecognized quote currency"
                    );
                    return None;
                }
            };
            Some(make_tuple(base, quote))
        })
        .collect()
}
