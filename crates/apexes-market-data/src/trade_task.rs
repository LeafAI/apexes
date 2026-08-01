use crate::bar_aggregator::BarAggregator;
use crate::provider::BinanceExchangeKind;
use crate::symbol::{split_symbol, symbol_of};
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
/// to also emit `MarketEvent::Bar` when intervals close. Runs until the
/// underlying `barter-data` stream ends (which, given `barter-data`'s
/// internal auto-reconnect, is effectively "forever" under normal
/// operation).
pub async fn run(
    symbols: Vec<String>,
    exchange: BinanceExchangeKind,
    tx: UnboundedSender<MarketEvent>,
) {
    let mut aggregators: HashMap<String, BarAggregator> = symbols
        .iter()
        .map(|symbol| {
            (
                symbol.clone(),
                BarAggregator::new(symbol.clone(), Venue::Binance),
            )
        })
        .collect();

    let mut merged = match exchange {
        BinanceExchangeKind::Spot => {
            let subscriptions: Vec<_> = build_subscriptions(&symbols, |base, quote| {
                (
                    BinanceSpot::default(),
                    base,
                    quote,
                    MarketDataInstrumentKind::Spot,
                    PublicTrades,
                )
            });
            if subscriptions.is_empty() {
                tracing::warn!(
                    ?symbols,
                    "trade_task: no valid Binance spot subscriptions derived from symbols"
                );
                return;
            }
            match Streams::<PublicTrades>::builder()
                .subscribe(subscriptions)
                .init()
                .await
            {
                Ok(streams) => streams.select_all(),
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        "trade_task: failed to initialize barter-data PublicTrades stream (spot)"
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
                    PublicTrades,
                )
            });
            if subscriptions.is_empty() {
                tracing::warn!(
                    ?symbols,
                    "trade_task: no valid Binance perpetual subscriptions derived from symbols"
                );
                return;
            }
            match Streams::<PublicTrades>::builder()
                .subscribe(subscriptions)
                .init()
                .await
            {
                Ok(streams) => streams.select_all(),
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        "trade_task: failed to initialize barter-data PublicTrades stream (perpetual)"
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
                    "trade_task: received error item from barter-data stream"
                );
                continue;
            }
            barter_data::streams::reconnect::Event::Reconnecting(exchange) => {
                tracing::info!(?exchange, "trade_task: barter-data stream reconnecting");
                continue;
            }
        };

        let symbol = symbol_of(&market_event.instrument);
        let trade: barter_data::subscription::trade::PublicTrade = market_event.kind;

        let price = f64_to_decimal(trade.price, "price");
        let quantity = f64_to_decimal(trade.amount, "amount");
        let timestamp = market_event.time_exchange;

        let apexes_trade = MarketEvent::Trade(TradeEvent {
            symbol: symbol.clone(),
            venue: Venue::Binance,
            price,
            quantity,
            side: Some(convert_side(trade.side)),
            trade_id: Some(trade.id.clone()),
            timestamp,
        });

        if tx.send(apexes_trade).is_err() {
            tracing::warn!("trade_task: output channel closed, stopping");
            return;
        }

        if let Some(aggregator) = aggregators.get_mut(&symbol) {
            for bar in aggregator.on_trade(price, quantity, timestamp) {
                if tx.send(MarketEvent::Bar(bar)).is_err() {
                    tracing::warn!("trade_task: output channel closed, stopping");
                    return;
                }
            }
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
