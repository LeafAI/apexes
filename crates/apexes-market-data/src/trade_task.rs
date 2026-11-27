use crate::bar_aggregator::BarAggregator;
use crate::provider::BinanceExchangeKind;
use crate::supervised::{self, Flow, Pulse, Timings};
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
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::LocalBoxStream;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::Duration;
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
    quiet_bars: bool,
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
    if quiet_bars {
        supervised::run_isolated_with_pulses(
            "trade_task",
            Timings::trades(),
            Some(Duration::from_millis(250)),
            move || connect(symbols.clone(), exchange, network, true),
            move |pulse| handle_pulse(&tx, &mut aggregators, pulse, Utc::now()),
        );
    } else {
        supervised::run_isolated(
            "trade_task",
            Timings::trades(),
            move || connect(symbols.clone(), exchange, network, false),
            move |trade| forward(&tx, &mut aggregators, trade),
        );
    }
}

// Allow two seconds of source lateness before closing a populated bucket.
const QUIET_BAR_GRACE: chrono::TimeDelta = chrono::TimeDelta::seconds(2);

fn handle_pulse(
    tx: &UnboundedSender<MarketEvent>,
    aggregators: &mut HashMap<String, BarAggregator>,
    pulse: Pulse<MarketEvent>,
    now: DateTime<Utc>,
) -> Flow {
    if tx.is_closed() {
        return Flow::Stop;
    }
    match pulse {
        Pulse::Item(trade) => {
            // Keep the source stream intact, but do not let a future
            // exchange timestamp prematurely close or poison a bucket.
            if matches!(&trade, MarketEvent::Trade(t) if t.timestamp > now) {
                tracing::warn!(
                    symbol = trade.symbol(),
                    "future trade excluded from quiet-bar aggregation"
                );
                return if tx.send(trade).is_ok() {
                    Flow::Continue
                } else {
                    Flow::Stop
                };
            }
            forward(tx, aggregators, trade)
        }
        Pulse::Tick => {
            let Some(watermark) = now.checked_sub_signed(QUIET_BAR_GRACE) else {
                return Flow::Continue;
            };
            let mut bars: Vec<_> = aggregators
                .values_mut()
                .flat_map(|agg| agg.advance_watermark(watermark))
                .collect();
            // Stable tie order by symbol; per-symbol equal closes retain
            // the fixed ALL_INTERVALS order.
            bars.sort_by(|a, b| {
                a.close_time
                    .cmp(&b.close_time)
                    .then_with(|| a.symbol.cmp(&b.symbol))
            });
            for bar in bars {
                if tx.send(MarketEvent::Bar(bar)).is_err() {
                    return Flow::Stop;
                }
            }
            Flow::Continue
        }
        Pulse::Disconnected => {
            for agg in aggregators.values_mut() {
                agg.discard_open_bars();
            }
            Flow::Continue
        }
    }
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

/// Opens the `barter-data` stream, yielding only real trades. Quiet-bar
/// mode treats explicit upstream reconnect/error notices as a gap.
async fn connect(
    symbols: Vec<String>,
    exchange: BinanceExchangeKind,
    network: BinanceNetwork,
    quiet_bars: bool,
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
                .take_while(move |event| {
                    let keep = !quiet_bars
                        || matches!(event, barter_data::streams::reconnect::Event::Item(Ok(_)));
                    if !keep {
                        tracing::warn!(
                            "trade_task: upstream reconnect/error; abandoning unfinished quiet bars"
                        );
                    }
                    std::future::ready(keep)
                })
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
    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).unwrap()
    }

    fn trade(symbol: &str, seconds: i64, price: i64) -> MarketEvent {
        MarketEvent::Trade(TradeEvent {
            symbol: symbol.into(),
            venue: Venue::Binance,
            price: Decimal::from(price),
            quantity: Decimal::ONE,
            side: None,
            trade_id: None,
            timestamp: at(seconds),
        })
    }

    #[test]
    fn quiet_pulses_emit_real_bars_after_grace_with_stable_order_and_keep_raw_late_trades() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut aggregators = HashMap::from([
            (
                "ETHUSDT".into(),
                BarAggregator::new("ETHUSDT", Venue::Binance),
            ),
            (
                "BTCUSDT".into(),
                BarAggregator::new("BTCUSDT", Venue::Binance),
            ),
        ]);
        for symbol in ["ETHUSDT", "BTCUSDT"] {
            let event = trade(symbol, 0, 100);
            assert_eq!(
                handle_pulse(&tx, &mut aggregators, Pulse::Item(event.clone()), at(0)),
                Flow::Continue
            );
            assert_eq!(rx.try_recv().unwrap(), event);
        }
        let before = at(3) - chrono::TimeDelta::nanoseconds(1);
        handle_pulse(&tx, &mut aggregators, Pulse::Tick, before);
        assert!(rx.try_recv().is_err());
        handle_pulse(&tx, &mut aggregators, Pulse::Tick, at(3));
        for symbol in ["BTCUSDT", "ETHUSDT"] {
            let MarketEvent::Bar(bar) = rx.try_recv().unwrap() else {
                panic!("expected closed bar")
            };
            assert_eq!(bar.symbol, symbol);
            assert_eq!(
                (bar.open_time, bar.close_time, bar.close, bar.volume),
                (at(0), at(1), Decimal::from(100), Decimal::ONE)
            );
        }
        handle_pulse(&tx, &mut aggregators, Pulse::Tick, at(3));
        let late = trade("BTCUSDT", 0, 999);
        handle_pulse(&tx, &mut aggregators, Pulse::Item(late.clone()), at(3));
        assert_eq!(rx.try_recv().unwrap(), late);
        let future = trade("BTCUSDT", 90, 999);
        handle_pulse(&tx, &mut aggregators, Pulse::Item(future.clone()), at(3));
        assert_eq!(rx.try_recv().unwrap(), future);
        handle_pulse(&tx, &mut aggregators, Pulse::Disconnected, at(3));
        handle_pulse(&tx, &mut aggregators, Pulse::Tick, at(62));
        assert!(rx.try_recv().is_err());
        let resumed = trade("BTCUSDT", 62, 200);
        handle_pulse(&tx, &mut aggregators, Pulse::Item(resumed.clone()), at(62));
        assert_eq!(rx.try_recv().unwrap(), resumed);
        handle_pulse(&tx, &mut aggregators, Pulse::Tick, at(65));
        let MarketEvent::Bar(bar) = rx.try_recv().unwrap() else {
            panic!("expected resumed bar")
        };
        assert_eq!(
            (bar.open_time, bar.close, bar.volume),
            (at(62), Decimal::from(200), Decimal::ONE)
        );
        assert!(rx.try_recv().is_err());
        drop(rx);
        assert_eq!(
            handle_pulse(&tx, &mut aggregators, Pulse::Tick, at(66)),
            Flow::Stop
        );
    }
}
