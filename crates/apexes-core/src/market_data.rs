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
    MarkPrice(MarkPriceUpdate),
}

impl MarketEvent {
    /// The symbol this event is about, whichever kind it is.
    pub fn symbol(&self) -> &str {
        match self {
            Self::Tick(e) => &e.symbol,
            Self::Depth(e) => &e.symbol,
            Self::Bar(e) => &e.symbol,
            Self::Trade(e) => &e.symbol,
            Self::MarkPrice(e) => &e.symbol,
        }
    }

    pub fn venue(&self) -> Venue {
        match self {
            Self::Tick(e) => e.venue,
            Self::Depth(e) => e.venue,
            Self::Bar(e) => e.venue,
            Self::Trade(e) => e.venue,
            Self::MarkPrice(e) => e.venue,
        }
    }
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
    /// Original source endpoints, when actually captured. Never inferred.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_range: Option<DepthSourceRange>,
    /// true if this is a full snapshot; false if this is an incremental update.
    pub is_snapshot: bool,
    pub timestamp: DateTime<Utc>,
}

/// Original incremental update identifiers. The last ID must equal
/// DepthUpdate.sequence; snapshots have no incremental source range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthSourceRange {
    pub first: u64,
    pub last: u64,
    pub previous_final: Option<u64>,
}

impl DepthUpdate {
    pub fn validate_source_range(&self) -> Result<(), String> {
        if let Some(r) = self.source_range
            && (self.is_snapshot
                || self.sequence != Some(r.last)
                || r.first > r.last
                || r.previous_final.is_some_and(|previous| previous >= r.last))
        {
            return Err("invalid original depth source range".into());
        }
        Ok(())
    }

    /// Explicit upstream book invalidation (F01.03). An empty snapshot
    /// without a sequence clears any previously synchronized depth.
    pub fn is_invalidation(&self) -> bool {
        self.is_snapshot && self.sequence.is_none() && self.bids.is_empty() && self.asks.is_empty()
    }
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

/// A perpetual/futures instrument's current mark price -- the
/// index-and-funding-basis-adjusted price real exchanges use for
/// margin ratio / liquidation / unrealized PnL calculations,
/// DELIBERATELY separate from `TradeEvent.price` (last trade) or
/// order-book mid, both of which are more easily manipulated/noisy
/// for that purpose. Meaningless for `AssetClass::Spot` instruments --
/// never emitted for them. See
/// `docs/superpowers/specs/2026-08-02-apexes-mark-price-pipeline-design.md`
/// for the full rationale.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkPriceUpdate {
    pub symbol: String,
    pub venue: Venue,
    pub mark_price: Decimal,
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
    fn legacy_depth_json_is_byte_compatible_and_native_range_round_trips() {
        let raw = r#"{"Depth":{"symbol":"BTCUSDT","venue":"Binance","bids":[],"asks":[],"sequence":103,"is_snapshot":false,"timestamp":"2026-01-01T00:00:00Z"}}"#;
        let mut event: MarketEvent = serde_json::from_str(raw).unwrap();
        assert_eq!(serde_json::to_string(&event).unwrap(), raw);
        let MarketEvent::Depth(d) = &mut event else {
            unreachable!()
        };
        assert_eq!(d.source_range, None);
        d.source_range = Some(DepthSourceRange {
            first: 99,
            last: 103,
            previous_final: Some(98),
        });
        assert!(d.validate_source_range().is_ok());
        let encoded = serde_json::to_string(&event).unwrap();
        assert_eq!(
            serde_json::from_str::<MarketEvent>(&encoded).unwrap(),
            event
        );
    }

    #[test]
    fn malformed_native_range_is_refused() {
        let mut d = DepthUpdate {
            symbol: "BTCUSDT".into(),
            venue: Venue::Binance,
            bids: vec![],
            asks: vec![],
            sequence: Some(103),
            source_range: Some(DepthSourceRange {
                first: 99,
                last: 103,
                previous_final: None,
            }),
            is_snapshot: false,
            timestamp: Utc::now(),
        };
        assert!(d.validate_source_range().is_ok());
        d.is_snapshot = true;
        assert!(d.validate_source_range().is_err());
        d.is_snapshot = false;
        d.sequence = None;
        assert!(d.validate_source_range().is_err());
        d.sequence = Some(103);
        d.source_range.as_mut().unwrap().first = 104;
        assert!(d.validate_source_range().is_err());
        d.source_range.as_mut().unwrap().first = 99;
        d.source_range.as_mut().unwrap().previous_final = Some(103);
        assert!(d.validate_source_range().is_err());
    }

    #[test]
    fn every_event_kind_reports_its_symbol_and_venue() {
        let tick = MarketEvent::Tick(Tick {
            symbol: "ETHUSDT".to_string(),
            venue: Venue::Binance,
            bid_price: dec!(1),
            bid_size: dec!(1),
            ask_price: dec!(2),
            ask_size: dec!(1),
            timestamp: Utc::now(),
        });
        assert_eq!(tick.symbol(), "ETHUSDT");
        assert_eq!(tick.venue(), Venue::Binance);
        let trade = MarketEvent::Trade(TradeEvent {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            price: dec!(1),
            quantity: dec!(1),
            side: None,
            trade_id: None,
            timestamp: Utc::now(),
        });
        assert_eq!(trade.symbol(), "BTCUSDT");
        let mark = MarketEvent::MarkPrice(MarkPriceUpdate {
            symbol: "SOLUSDT".to_string(),
            venue: Venue::Binance,
            mark_price: dec!(1),
            timestamp: Utc::now(),
        });
        assert_eq!(mark.symbol(), "SOLUSDT");
    }

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
            source_range: None,
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
            source_range: None,
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
    fn mark_price_event_serde_round_trip() {
        let event = MarketEvent::MarkPrice(MarkPriceUpdate {
            symbol: "BTCUSDT".to_string(),
            venue: Venue::Binance,
            mark_price: dec!(65012.34),
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
