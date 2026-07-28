use crate::error::ApexesError;
use crate::instrument::{Instrument, Venue};
use crate::order::Side;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketEvent {
    Tick(Tick),
    Depth(DepthUpdate),
    Bar(Bar),
    Trade(TradeEvent),
}

/// A best bid/ask quote update for a symbol.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tick {
    pub symbol: String,
    pub venue: Venue,
    pub bid_price: Decimal,
    pub bid_size: Decimal,
    pub ask_price: Decimal,
    pub ask_size: Decimal,
    pub timestamp: DateTime<Utc>,
}

/// An order book depth update (full snapshot or incremental).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DepthUpdate {
    pub symbol: String,
    pub venue: Venue,
    /// Sorted best-to-worst (bid: highest price first).
    pub bids: Vec<PriceLevel>,
    /// Sorted best-to-worst (ask: lowest price first).
    pub asks: Vec<PriceLevel>,
    /// Sequence number for ordering incremental updates and detecting gaps (data-source dependent; not all sources provide one).
    pub sequence: Option<u64>,
    /// true if this is a full snapshot; false if this is an incremental update.
    pub is_snapshot: bool,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

/// An OHLCV candlestick bar over a fixed interval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bar {
    pub symbol: String,
    pub venue: Venue,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub interval: BarInterval,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BarInterval {
    OneSecond,
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
    OneDay,
}

/// A single executed trade (public market trade, not an own fill).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeEvent {
    pub symbol: String,
    pub venue: Venue,
    pub price: Decimal,
    pub quantity: Decimal,
    /// Aggressor (taker) side, if known. Not all data sources provide this.
    pub side: Option<Side>,
    pub trade_id: Option<String>,
    pub timestamp: DateTime<Utc>,
}

/// Streams normalized [`MarketEvent`]s for a set of [`Instrument`]s.
///
/// Implementations are expected to be registered behind `Box<dyn
/// MarketDataProvider>` so data sources can be swapped via configuration
/// (free vs. paid feeds) without touching strategy or risk code.
#[async_trait]
pub trait MarketDataProvider: Send + Sync {
    async fn subscribe(
        &self,
        instruments: &[Instrument],
    ) -> Result<BoxStream<'static, MarketEvent>, ApexesError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn tick_event_serde_round_trip() {
        let event = MarketEvent::Tick(Tick {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(64999.5),
            bid_size: dec!(0.8),
            ask_price: dec!(65000.5),
            ask_size: dec!(1.2),
            timestamp: Utc::now(),
        });
        let json = serde_json::to_string(&event).unwrap();
        let round_tripped: MarketEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, round_tripped);
    }

    #[test]
    fn depth_event_serde_round_trip() {
        let event = MarketEvent::Depth(DepthUpdate {
            symbol: "AAPL".to_string(),
            venue: Venue::Nasdaq,
            bids: vec![
                PriceLevel {
                    price: dec!(190.50),
                    quantity: dec!(100),
                },
                PriceLevel {
                    price: dec!(190.45),
                    quantity: dec!(200),
                },
            ],
            asks: vec![PriceLevel {
                price: dec!(190.55),
                quantity: dec!(150),
            }],
            sequence: Some(12345),
            is_snapshot: false,
            timestamp: Utc::now(),
        });
        let json = serde_json::to_string(&event).unwrap();
        let round_tripped: MarketEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, round_tripped);
    }

    #[test]
    fn depth_snapshot_without_sequence_serde_round_trip() {
        let event = MarketEvent::Depth(DepthUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            bids: vec![PriceLevel {
                price: dec!(64999.5),
                quantity: dec!(1.5),
            }],
            asks: vec![PriceLevel {
                price: dec!(65000.5),
                quantity: dec!(2.0),
            }],
            sequence: None,
            is_snapshot: true,
            timestamp: Utc::now(),
        });
        let json = serde_json::to_string(&event).unwrap();
        let round_tripped: MarketEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, round_tripped);
    }

    #[test]
    fn bar_event_serde_round_trip() {
        let now = Utc::now();
        let event = MarketEvent::Bar(Bar {
            symbol: "0700".to_string(),
            venue: Venue::Hkex,
            open: dec!(320.0),
            high: dec!(325.0),
            low: dec!(318.0),
            close: dec!(322.5),
            volume: dec!(1_500_000),
            interval: BarInterval::OneDay,
            open_time: now,
            close_time: now,
        });
        let json = serde_json::to_string(&event).unwrap();
        let round_tripped: MarketEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, round_tripped);
    }

    #[test]
    fn trade_event_serde_round_trip() {
        let event = MarketEvent::Trade(TradeEvent {
            symbol: "600519".to_string(),
            venue: Venue::Sse,
            price: dec!(1680.0),
            quantity: dec!(100),
            side: Some(Side::Sell),
            trade_id: Some("T123456".to_string()),
            timestamp: Utc::now(),
        });
        let json = serde_json::to_string(&event).unwrap();
        let round_tripped: MarketEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, round_tripped);
    }

    #[test]
    fn trade_event_side_is_optional() {
        let event = TradeEvent {
            symbol: "ETHUSDT".to_string(),
            venue: Venue::Okx,
            price: dec!(3200.0),
            quantity: dec!(2.0),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        };
        assert_eq!(event.side, None);
    }

    #[test]
    fn market_data_provider_is_object_safe() {
        // This function only needs to compile — it proves MarketDataProvider
        // can be used as a trait object, which #[async_trait] enables.
        fn _assert_object_safe(_providers: Vec<Box<dyn MarketDataProvider>>) {}
    }
}
