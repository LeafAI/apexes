use crate::bar_aggregator::BarAggregator;
use crate::provider::BinanceExchangeKind;
use crate::supervised::{self, Flow, Timings};
use crate::symbol::{split_symbol, symbol_of};
use crate::testnet::BinanceSpotTestnet;
use apexes_core::binance_network::BinanceNetwork;
use apexes_core::{MarketEvent, Side as ApexesSide, TradeEvent, Venue};
use barter_data::{
    exchange::binance::{futures::BinanceFuturesUsd, spot::BinanceSpot},
    streams::Streams,
    subscription::trade::PublicTrades,
};
use barter_instrument::{
    Side as BarterSide, instrument::market_data::kind::MarketDataInstrumentKind,
};
use futures::StreamExt;
use futures::stream::LocalBoxStream;
use rust_decimal::Decimal;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

fn convert_side(side: BarterSide) -> ApexesSide {
    match side {
        BarterSide::Buy => ApexesSide::Buy,
        BarterSide::Sell => ApexesSide::Sell,
    }
}

/// Converts an `f64` price/amount from `barter-data` into `Decimal`. Falls
/// back to `Decimal::ZERO` with a warning on non-finite input (NaN/inf),
/// which should not occur for real exchange data but is handled explicitly
/// rather than panicking.
fn f64_to_decimal(value: f64, field_name: &str) -> Decimal {
    Decimal::from_f64_retain(value).unwrap_or_else(|| {
        tracing::warn!(
            value,
            field_name,
            "failed to convert f64 to Decimal (non-finite?), using zero"
        );
        Decimal::ZERO
    })
}

/// Subscribes to Binance public trades for `symbols` on `exchange`
/// (`BinanceExchangeKind::Spot` or `::Perpetual`), converts each trade
/// into `MarketEvent::Trade`, and drives a per-symbol `BarAggregator`
/// to also emit `MarketEvent::Bar` when intervals close.
///
/// Runs until the output channel closes. The connection is supervised
/// (`crate::supervised`): one that goes silent is replaced, because
/// `barter-data` only reconnects on an error or a close and a half-open
/// socket produces neither. Blocks the calling thread.
pub fn run(
    symbols: Vec<String>,
    exchange: BinanceExchangeKind,
    network: BinanceNetwork,
    tx: UnboundedSender<MarketEvent>,
) {
    if !symbols.iter().any(|s| split_symbol(s).is_some()) {
        tracing::warn!(
            ?symbols,
            "trade_task: no valid Binance subscriptions derived from symbols"
        );
        return;
    }
    let mut aggregators: HashMap<String, BarAggregator> = symbols
        .iter()
        .map(|symbol| {
            (
                symbol.clone(),
                BarAggregator::new(symbol.clone(), Venue::Binance),
            )
        })
        .collect();
    supervised::run_isolated(
        "trade_task",
        Timings::trades(),
        move || connect(symbols.clone(), exchange, network),
        move |trade: MarketEvent| forward(&tx, &mut aggregators, trade),
    );
}

/// Sends the trade, then any bars it closed.
fn forward(
    tx: &UnboundedSender<MarketEvent>,
    aggregators: &mut HashMap<String, BarAggregator>,
    trade: MarketEvent,
) -> Flow {
    let MarketEvent::Trade(t) = &trade else {
        return Flow::Continue;
    };
    let (symbol, price, quantity, timestamp) = (t.symbol.clone(), t.price, t.quantity, t.timestamp);
    if tx.send(trade).is_err() {
        tracing::warn!("trade_task: output channel closed, stopping");
        return Flow::Stop;
    }
    if let Some(aggregator) = aggregators.get_mut(&symbol) {
        for bar in aggregator.on_trade(price, quantity, timestamp) {
            if tx.send(MarketEvent::Bar(bar)).is_err() {
                tracing::warn!("trade_task: output channel closed, stopping");
                return Flow::Stop;
            }
        }
    }
    Flow::Continue
}

/// Opens the `barter-data` stream, yielding only real trades.
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
                    PublicTrades,
                )
            });
            Streams::<PublicTrades>::builder()
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
                    PublicTrades,
                )
            });
            Streams::<PublicTrades>::builder()
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
                    PublicTrades,
                )
            });
            Streams::<PublicTrades>::builder()
                .subscribe(subs)
                .init()
                .await
        }
    };
    match streams {
        Ok(streams) => Some(
            streams
                .select_all()
                .filter_map(|event| std::future::ready(to_trade(event)))
                .boxed_local(),
        ),
        Err(error) => {
            tracing::warn!(?error, %network, "trade_task: failed to initialize the PublicTrades stream");
            None
        }
    }
}

/// A trade event, or `None` for reconnect notices and error items (which
/// are logged; the stream itself keeps going).
fn to_trade(
    event: barter_data::streams::reconnect::Event<
        barter_instrument::exchange::ExchangeId,
        Result<
            barter_data::event::MarketEvent<
                barter_instrument::instrument::market_data::MarketDataInstrument,
                barter_data::subscription::trade::PublicTrade,
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
                "trade_task: received error item from barter-data stream"
            );
            return None;
        }
        barter_data::streams::reconnect::Event::Reconnecting(exchange) => {
            tracing::info!(?exchange, "trade_task: barter-data stream reconnecting");
            return None;
        }
    };
    let trade = market_event.kind;
    Some(MarketEvent::Trade(TradeEvent {
        symbol: symbol_of(&market_event.instrument),
        venue: Venue::Binance,
        price: f64_to_decimal(trade.price, "price"),
        quantity: f64_to_decimal(trade.amount, "amount"),
        side: Some(convert_side(trade.side)),
        trade_id: Some(trade.id.clone()),
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
                        "trade_task: skipping symbol with unrecognized quote currency"
                    );
                    return None;
                }
            };
            Some(make_tuple(base, quote))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f64_to_decimal_converts_normal_values() {
        assert_eq!(
            f64_to_decimal(65000.5, "price"),
            Decimal::try_from(65000.5).unwrap()
        );
    }

    #[test]
    fn f64_to_decimal_falls_back_to_zero_on_nan() {
        assert_eq!(f64_to_decimal(f64::NAN, "price"), Decimal::ZERO);
    }
}
