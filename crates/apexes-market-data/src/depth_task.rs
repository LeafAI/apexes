use crate::depth_converter;
use crate::provider::BinanceExchangeKind;
use crate::supervised::{self, Flow, Timings};
use crate::symbol::{split_symbol, symbol_of};
use apexes_core::{MarketEvent, Venue};
use barter_data::{
    exchange::binance::{futures::BinanceFuturesUsd, spot::BinanceSpot},
    streams::Streams,
    subscription::book::OrderBooksL2,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures::StreamExt;
use futures::stream::LocalBoxStream;
use tokio::sync::mpsc::UnboundedSender;

/// Subscribes to Binance L2 order book updates for `symbols` on `exchange`
/// (`BinanceExchangeKind::Spot` or `::Perpetual`; snapshot + incremental,
/// synchronized and gap-checked by `barter-data` itself) and converts each
/// event into `MarketEvent::Depth` via `depth_converter::convert`.
///
/// Runs until the output channel closes; the connection is supervised
/// (`crate::supervised`), and a replaced connection starts from a fresh
/// snapshot. Blocks the calling thread.
pub fn run(symbols: Vec<String>, exchange: BinanceExchangeKind, tx: UnboundedSender<MarketEvent>) {
    if !symbols.iter().any(|s| split_symbol(s).is_some()) {
        tracing::warn!(
            ?symbols,
            "depth_task: no valid Binance subscriptions derived from symbols"
        );
        return;
    }
    supervised::run_isolated(
        "depth_task",
        Timings::default(),
        move || connect(symbols.clone(), exchange),
        move |depth: MarketEvent| {
            if tx.send(depth).is_err() {
                tracing::warn!("depth_task: output channel closed, stopping");
                Flow::Stop
            } else {
                Flow::Continue
            }
        },
    );
}

async fn connect(
    symbols: Vec<String>,
    exchange: BinanceExchangeKind,
) -> Option<LocalBoxStream<'static, MarketEvent>> {
    let streams = match exchange {
        BinanceExchangeKind::Spot => {
            let subs = build_subscriptions(&symbols, |base, quote| {
                (
                    BinanceSpot::default(),
                    base,
                    quote,
                    MarketDataInstrumentKind::Spot,
                    OrderBooksL2,
                )
            });
            Streams::<OrderBooksL2>::builder()
                .subscribe(subs)
                .init()
                .await
        }
        BinanceExchangeKind::Perpetual => {
            let subs = build_subscriptions(&symbols, |base, quote| {
                (
                    BinanceFuturesUsd::default(),
                    base,
                    quote,
                    MarketDataInstrumentKind::Perpetual,
                    OrderBooksL2,
                )
            });
            Streams::<OrderBooksL2>::builder()
                .subscribe(subs)
                .init()
                .await
        }
    };
    match streams {
        Ok(streams) => Some(
            streams
                .select_all()
                .filter_map(|event| std::future::ready(to_depth(event)))
                .boxed_local(),
        ),
        Err(error) => {
            tracing::warn!(
                ?error,
                "depth_task: failed to initialize the OrderBooksL2 stream"
            );
            None
        }
    }
}

fn to_depth(
    event: barter_data::streams::reconnect::Event<
        barter_instrument::exchange::ExchangeId,
        Result<
            barter_data::event::MarketEvent<
                barter_instrument::instrument::market_data::MarketDataInstrument,
                barter_data::subscription::book::OrderBookEvent,
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
                "depth_task: received error item from barter-data stream"
            );
            return None;
        }
        barter_data::streams::reconnect::Event::Reconnecting(exchange) => {
            tracing::info!(
                ?exchange,
                "depth_task: barter-data stream reconnecting (next update will be is_snapshot: true)"
            );
            return None;
        }
    };
    let symbol = symbol_of(&market_event.instrument);
    Some(MarketEvent::Depth(depth_converter::convert(
        &symbol,
        Venue::Binance,
        &market_event.kind,
    )))
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
