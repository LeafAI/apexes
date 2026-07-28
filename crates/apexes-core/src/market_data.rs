use crate::instrument::Venue;
use crate::order::Side;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketEvent {
    Tick(Tick),
    Depth(DepthUpdate),
    Bar(Bar),
    Trade(TradeEvent),
}

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DepthUpdate {
    pub symbol: String,
    pub venue: Venue,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeEvent {
    pub symbol: String,
    pub venue: Venue,
    pub price: Decimal,
    pub quantity: Decimal,
    pub side: Option<Side>,
    pub trade_id: Option<String>,
    pub timestamp: DateTime<Utc>,
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
}
