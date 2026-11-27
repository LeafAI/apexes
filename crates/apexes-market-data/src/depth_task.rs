use crate::depth_converter;
use crate::provider::BinanceExchangeKind;
use crate::supervised::{self, Flow, Timings};
use crate::symbol::{split_symbol, symbol_of};
use apexes_core::{DepthUpdate, MarketEvent, Venue};
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
    let invalidate_tx = tx.clone();
    let mut attempted = false;
    supervised::run_isolated(
        "depth_task",
        Timings::default(),
        move || {
            // Outer supervisor replaces half-open connections as well as
            // failed connections. Invalidate before attempting recovery.
            if attempted {
                for symbol in &symbols {
                    let _ = invalidate_tx.send(invalidate_book(symbol));
                }
            }
            attempted = true;
            connect(symbols.clone(), exchange, invalidate_tx.clone())
        },
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
    invalidate_tx: UnboundedSender<MarketEvent>,
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
        Ok(streams) => Some({
            let mut ready = std::collections::HashSet::new();
            streams
                .select_all()
                .scan((), move |_, event| {
                    use barter_data::streams::reconnect::Event;
                    let update = match event {
                        Event::Item(Ok(event)) => {
                            let symbol = symbol_of(&event.instrument);
                            let update =
                                depth_converter::convert(&symbol, Venue::Binance, &event.kind);
                            if update.is_snapshot {
                                ready.insert(symbol.clone());
                            }
                            Some(
                                ready
                                    .contains(&symbol)
                                    .then_some(MarketEvent::Depth(update)),
                            )
                        }
                        Event::Item(Err(error)) => {
                            tracing::warn!(
                                ?error,
                                "depth stream error; rebuild connection from snapshot"
                            );
                            for symbol in &symbols {
                                let _ = invalidate_tx.send(invalidate_book(symbol));
                            }
                            None
                        }
                        Event::Reconnecting(exchange) => {
                            tracing::info!(
                                ?exchange,
                                "depth stream reconnecting; rebuild connection from snapshot"
                            );
                            for symbol in &symbols {
                                let _ = invalidate_tx.send(invalidate_book(symbol));
                            }
                            None
                        }
                    };
                    std::future::ready(update)
                })
                .filter_map(std::future::ready)
                .boxed_local()
        }),
        Err(error) => {
            tracing::warn!(
                ?error,
                "depth_task: failed to initialize the OrderBooksL2 stream"
            );
            None
        }
    }
}

/// An empty snapshot without an exchange sequence explicitly invalidates
/// the previously reconstructed book. It is not a tradable quote.
fn invalidate_book(symbol: &str) -> MarketEvent {
    MarketEvent::Depth(DepthUpdate {
        symbol: symbol.to_string(),
        venue: Venue::Binance,
        bids: Vec::new(),
        asks: Vec::new(),
        sequence: None,
        source_range: None,
        is_snapshot: true,
        timestamp: chrono::Utc::now(),
    })
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
